//! The durable store: an in-memory map fronted by a write-ahead log.
//!
//! Every mutation is logged (and fsynced) *before* it touches memory, so the
//! map is always reconstructible from the WAL. `open` replays the log to
//! rebuild exactly the state that existed before shutdown or crash.
//!
//! This is still a single in-memory layer (a `HashMap`). Phase 2 swaps it for a
//! sorted MemTable that flushes to disk; the WAL contract here doesn't change.

use std::collections::HashMap;
use std::io;
use std::path::Path;

use crate::api::{Entry, EntryKind, Key, Value};
use crate::store::Store;
use crate::wal::Wal;

const WAL_FILE: &str = "wal.log";

pub struct Engine {
    wal: Wal,
    mem: HashMap<Key, Value>,
    /// Monotonic sequence counter. Persisted implicitly via logged entries and
    /// restored to the high-water mark on recovery, so seq never goes backward.
    seq: u64,
}

impl Engine {
    /// Opens (or creates) a store rooted at directory `dir`, replaying any
    /// existing WAL to rebuild state.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Engine> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let wal_path = dir.join(WAL_FILE);

        let mut mem = HashMap::new();
        let mut seq = 0;
        for entry in Wal::replay(&wal_path)? {
            seq = seq.max(entry.seq);
            apply(&mut mem, entry);
        }

        let wal = Wal::open(&wal_path)?;
        Ok(Engine { wal, mem, seq })
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    /// Logs `entry` durably, then applies it to memory. The order is the whole
    /// point: if the append fails, memory is left untouched and the caller sees
    /// the error.
    fn write(&mut self, entry: Entry) -> io::Result<()> {
        self.wal.append(&entry)?;
        apply(&mut self.mem, entry);
        Ok(())
    }
}

/// Replays one entry against the in-memory map.
fn apply(mem: &mut HashMap<Key, Value>, entry: Entry) {
    match entry.kind {
        EntryKind::Put => {
            mem.insert(entry.key, entry.value);
        }
        EntryKind::Delete => {
            mem.remove(&entry.key);
        }
    }
}

impl Store for Engine {
    fn get(&self, key: &[u8]) -> Option<Value> {
        self.mem.get(key).cloned()
    }

    fn set(&mut self, key: Key, value: Value) -> io::Result<()> {
        let seq = self.next_seq();
        self.write(Entry::put(key, value, seq))
    }

    fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        let seq = self.next_seq();
        self.write(Entry::delete(key.to_vec(), seq))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_dir(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!(
            "strata-engine-{}-{}-{}",
            tag,
            std::process::id(),
            nanos
        ));
        p
    }

    fn k(s: &str) -> Key {
        s.as_bytes().to_vec()
    }

    #[test]
    fn survives_reopen() {
        let dir = temp_dir("reopen");
        {
            let mut db = Engine::open(&dir).unwrap();
            db.set(k("name"), k("ada")).unwrap();
            db.set(k("lang"), k("rust")).unwrap();
            db.set(k("name"), k("grace")).unwrap(); // overwrite
            db.delete(b"lang").unwrap(); // tombstone
        } // db dropped: simulate shutdown

        let db = Engine::open(&dir).unwrap();
        assert_eq!(db.get(b"name"), Some(k("grace")));
        assert_eq!(db.get(b"lang"), None); // delete survived recovery
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn seq_continues_after_recovery() {
        let dir = temp_dir("seq");
        {
            let mut db = Engine::open(&dir).unwrap();
            db.set(k("a"), k("1")).unwrap();
            db.set(k("b"), k("2")).unwrap();
            assert_eq!(db.seq, 2);
        }

        let mut db = Engine::open(&dir).unwrap();
        assert_eq!(db.seq, 2); // restored high-water mark
        db.set(k("c"), k("3")).unwrap();
        assert_eq!(db.seq, 3); // and keeps climbing
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_open_is_clean() {
        let dir = temp_dir("empty");
        let db = Engine::open(&dir).unwrap();
        assert_eq!(db.get(b"anything"), None);
        std::fs::remove_dir_all(&dir).ok();
    }
}
