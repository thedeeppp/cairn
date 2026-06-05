//! The MemTable: an in-memory, **sorted** table of the most recent writes.
//!
//! A `BTreeMap` keeps entries in key order, which is what lets a flush produce a
//! sorted SSTable in a single pass. Each key maps to a full [`Entry`], so a
//! delete is stored as a *tombstone* (a `Delete` entry) rather than removing the
//! key — the tombstone must remain to shadow any older value for that key living
//! in an SSTable on disk.

use std::collections::BTreeMap;

use crate::api::{Entry, Key};

#[derive(Default)]
pub struct MemTable {
    map: BTreeMap<Key, Entry>,
    /// Approximate live size in bytes (keys + values), used to decide when to
    /// flush. Approximate is fine: it only drives a threshold.
    bytes: usize,
}

/// Bytes an entry contributes to the size estimate.
fn entry_size(entry: &Entry) -> usize {
    entry.key.len() + entry.value.len()
}

impl MemTable {
    pub fn new() -> Self {
        MemTable::default()
    }

    /// Inserts `entry`, overwriting any existing version of the same key
    /// (newest wins within a single table).
    pub fn put(&mut self, entry: Entry) {
        let added = entry_size(&entry);
        if let Some(old) = self.map.insert(entry.key.clone(), entry) {
            self.bytes -= entry_size(&old);
        }
        self.bytes += added;
    }

    /// Returns the stored entry for `key`, tombstone or not. Callers inspect
    /// `kind` to distinguish a live value from a deletion.
    pub fn get(&self, key: &[u8]) -> Option<&Entry> {
        self.map.get(key)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Current approximate size in bytes.
    pub fn size_bytes(&self) -> usize {
        self.bytes
    }

    /// Entries in ascending key order. A flush writes these straight to disk.
    pub fn iter(&self) -> impl Iterator<Item = &Entry> {
        self.map.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(table: &mut MemTable, key: &str, val: &str, seq: u64) {
        table.put(Entry::put(
            key.as_bytes().to_vec(),
            val.as_bytes().to_vec(),
            seq,
        ));
    }

    #[test]
    fn put_and_get() {
        let mut t = MemTable::new();
        put(&mut t, "a", "1", 1);
        assert_eq!(t.get(b"a").unwrap().value, b"1");
        assert!(t.get(b"missing").is_none());
    }

    #[test]
    fn overwrite_replaces_and_keeps_size_sane() {
        let mut t = MemTable::new();
        put(&mut t, "a", "11", 1);
        let after_first = t.size_bytes();
        put(&mut t, "a", "2222", 2);
        assert_eq!(t.len(), 1); // still one key
        assert_eq!(t.get(b"a").unwrap().value, b"2222");
        // size reflects the new value (a:11 -> a:2222), not the sum of both.
        assert_eq!(t.size_bytes(), after_first + 2);
    }

    #[test]
    fn delete_is_stored_as_tombstone() {
        let mut t = MemTable::new();
        put(&mut t, "a", "1", 1);
        t.put(Entry::delete(b"a".to_vec(), 2));
        let e = t.get(b"a").unwrap();
        assert!(e.is_tombstone()); // present, but marked deleted
    }

    #[test]
    fn iter_is_sorted_by_key() {
        let mut t = MemTable::new();
        put(&mut t, "c", "3", 1);
        put(&mut t, "a", "1", 2);
        put(&mut t, "b", "2", 3);
        let keys: Vec<_> = t.iter().map(|e| e.key.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }
}
