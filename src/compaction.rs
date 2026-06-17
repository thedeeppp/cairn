//! Size-tiered compaction: merge several SSTables into one, keeping the newest
//! value per key and — when safe — dropping tombstones.
//!
//! The merge is the pure part of compaction; the [`engine`](crate::engine) runs
//! it on a background thread and atomically swaps the result in for its inputs.
//! The inputs are scanned in parallel with rayon, since the heavy cost is the
//! per-table disk reads and bincode decoding, which are independent.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use rayon::prelude::*;

use crate::api::{Entry, Key};
use crate::sstable::SsTable;

/// Merges `tables` (in any order) into a single sorted run, keeping only the
/// newest entry (highest `seq`) for each key.
///
/// When `drop_tombstones` is set, deletions are removed outright. That is only
/// safe when `tables` includes the oldest table on disk — otherwise an older
/// value could survive beneath the dropped tombstone and resurface. The engine
/// guarantees this by compacting *every* table at once and flagging the output
/// as superseding, so a crash mid-swap can't resurrect a dropped key.
pub fn merge_tables(tables: &[Arc<SsTable>], drop_tombstones: bool) -> io::Result<Vec<Entry>> {
    // Scan every input in parallel; the reads and decoding are independent. If
    // any scan fails, the whole merge fails.
    let runs: Vec<Vec<Entry>> = tables
        .par_iter()
        .map(|table| table.scan())
        .collect::<io::Result<Vec<_>>>()?;

    // Collapse to the newest version of each key. BTreeMap keeps the result
    // key-ordered, ready to hand to `SsTable::create`.
    let mut newest: BTreeMap<Key, Entry> = BTreeMap::new();
    for run in runs {
        for entry in run {
            match newest.get(&entry.key) {
                // Keep whichever version is newer.
                Some(existing) if existing.seq >= entry.seq => {}
                _ => {
                    newest.insert(entry.key.clone(), entry);
                }
            }
        }
    }

    let mut merged = Vec::with_capacity(newest.len());
    for (_, entry) in newest {
        if drop_tombstones && entry.is_tombstone() {
            continue;
        }
        merged.push(entry);
    }
    Ok(merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::Entry;
    use std::path::PathBuf;

    fn temp_path(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!(
            "cairn-compact-{}-{}-{}.sst",
            tag,
            std::process::id(),
            nanos
        ));
        p
    }

    fn sst(tag: &str, entries: Vec<Entry>) -> Arc<SsTable> {
        Arc::new(SsTable::create(temp_path(tag), entries).unwrap())
    }

    fn put(key: &str, val: &str, seq: u64) -> Entry {
        Entry::put(key.as_bytes().to_vec(), val.as_bytes().to_vec(), seq)
    }

    #[test]
    fn newest_seq_wins_across_tables_and_output_is_sorted() {
        let old = sst("old", vec![put("a", "1", 1), put("b", "1", 2)]);
        let new = sst("new", vec![put("a", "2", 5)]);

        let merged = merge_tables(&[old.clone(), new.clone()], false).unwrap();

        let keys: Vec<_> = merged.iter().map(|e| e.key.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]); // sorted
        let a = merged.iter().find(|e| e.key == b"a").unwrap();
        assert_eq!(a.value, b"2"); // higher seq wins

        std::fs::remove_file(old.path()).ok();
        std::fs::remove_file(new.path()).ok();
    }

    #[test]
    fn dropping_tombstones_removes_the_deleted_key() {
        let older = sst("older", vec![put("a", "1", 1), put("b", "old", 1)]);
        let newer = sst("newer", vec![Entry::delete(b"b".to_vec(), 3)]);
        let inputs = [older.clone(), newer.clone()];

        // Kept: the tombstone collapses the older value but survives.
        let kept = merge_tables(&inputs, false).unwrap();
        assert!(kept.iter().any(|e| e.key == b"b" && e.is_tombstone()));

        // Dropped: the key disappears entirely.
        let dropped = merge_tables(&inputs, true).unwrap();
        assert!(dropped.iter().all(|e| e.key != b"b"));
        assert!(dropped.iter().any(|e| e.key == b"a"));

        std::fs::remove_file(older.path()).ok();
        std::fs::remove_file(newer.path()).ok();
    }
}
