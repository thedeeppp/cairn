//! SSTable — a Sorted String Table: an immutable, key-ordered file of entries.
//!
//! Phase 2 keeps the format deliberately minimal: a flat sequence of
//! length-prefixed bincode records, written in ascending key order. That's
//! enough to flush a MemTable durably and to scan it back on recovery. Phase 3
//! adds a sparse index + footer so a single key can be found without scanning
//! the whole file.
//!
//! Writes are crash-safe: data goes to a temp file that is fsynced and then
//! atomically renamed into place, so a reader never sees a half-written SSTable.

use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

use crate::api::Entry;

/// A handle to an on-disk SSTable file.
pub struct SsTable {
    path: PathBuf,
}

fn invalid(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, e.to_string())
}

impl SsTable {
    /// References an existing SSTable file. Does not touch the disk.
    pub fn open(path: impl Into<PathBuf>) -> SsTable {
        SsTable { path: path.into() }
    }

    /// Writes `entries` (which **must** be in ascending key order) to a new
    /// SSTable at `path`, durably. Returns a handle to the finished file.
    pub fn create(
        path: impl AsRef<Path>,
        entries: impl IntoIterator<Item = Entry>,
    ) -> io::Result<SsTable> {
        let path = path.as_ref().to_path_buf();

        // Write to a sibling temp file first; a crash mid-write leaves only the
        // temp behind, never a torn ".sst".
        let mut tmp = path.clone();
        tmp.set_extension("sst.tmp");

        {
            let file = File::create(&tmp)?;
            let mut writer = BufWriter::new(file);
            for entry in entries {
                let bytes = bincode::serialize(&entry).map_err(invalid)?;
                let len = u32::try_from(bytes.len()).map_err(invalid)?;
                writer.write_all(&len.to_le_bytes())?;
                writer.write_all(&bytes)?;
            }
            writer.flush()?;
            writer.get_ref().sync_all()?; // temp contents durable...
        }

        fs::rename(&tmp, &path)?; // ...then atomically swap into place.
        fsync_dir(&path)?; // make the rename itself durable.
        Ok(SsTable { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Reads every entry, in stored (key) order. A complete SSTable has no torn
    /// tail — it was fsynced before the rename — so a short read here is real
    /// corruption and surfaces as an error.
    pub fn scan(&self) -> io::Result<Vec<Entry>> {
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;
            entries.push(bincode::deserialize(&buf).map_err(invalid)?);
        }

        Ok(entries)
    }
}

/// fsyncs the directory containing `path`, so a rename into it survives a crash.
fn fsync_dir(path: &Path) -> io::Result<()> {
    if let Some(dir) = path.parent() {
        // An empty parent means the current dir; "." is always openable.
        let dir = if dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir
        };
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_path(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!(
            "strata-sst-{}-{}-{}.sst",
            tag,
            std::process::id(),
            nanos
        ));
        p
    }

    fn put(key: &str, val: &str, seq: u64) -> Entry {
        Entry::put(key.as_bytes().to_vec(), val.as_bytes().to_vec(), seq)
    }

    #[test]
    fn create_then_scan_roundtrips_in_order() {
        let path = temp_path("roundtrip");
        let entries = vec![put("a", "1", 1), put("b", "2", 2), put("c", "3", 3)];
        SsTable::create(&path, entries.clone()).unwrap();

        let read = SsTable::open(&path).scan().unwrap();
        assert_eq!(read, entries);

        // Keys are ascending on disk.
        let keys: Vec<_> = read.iter().map(|e| &e.key).collect();
        assert!(keys.windows(2).all(|w| w[0] < w[1]));
        fs::remove_file(&path).ok();
    }

    #[test]
    fn tombstones_are_preserved() {
        let path = temp_path("tomb");
        let entries = vec![put("a", "1", 1), Entry::delete(b"b".to_vec(), 2)];
        SsTable::create(&path, entries).unwrap();

        let read = SsTable::open(&path).scan().unwrap();
        assert!(!read[0].is_tombstone());
        assert!(read[1].is_tombstone());
        fs::remove_file(&path).ok();
    }

    #[test]
    fn no_temp_file_left_behind() {
        let path = temp_path("clean");
        SsTable::create(&path, vec![put("a", "1", 1)]).unwrap();
        let mut tmp = path.clone();
        tmp.set_extension("sst.tmp");
        assert!(!tmp.exists());
        fs::remove_file(&path).ok();
    }
}
