//! Public data model and command surface.
//!
//! These types are the vocabulary the whole engine speaks: `Entry` is the unit
//! that flows from the write path into the WAL, the MemTable, and eventually
//! SSTables (later phases). Keeping it here lets every module agree on the same
//! shape.

use serde::{Deserialize, Serialize};

/// Keys and values are opaque byte strings — the store never interprets them.
pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

/// Whether an `Entry` records a live value or a deletion.
///
/// A `Delete` is a *tombstone*: in an LSM tree we cannot mutate older on-disk
/// data in place, so a deletion is written as a new record that shadows any
/// earlier value for the same key. Tombstones are only physically dropped
/// during compaction (Phase 5).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum EntryKind {
    Put,
    Delete,
}

/// A single versioned record. Newest `seq` wins when the same key appears more
/// than once across the memtable and SSTables.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Entry {
    pub key: Key,
    pub value: Value,
    pub kind: EntryKind,
    pub seq: u64,
}

impl Entry {
    /// A live key→value record.
    pub fn put(key: Key, value: Value, seq: u64) -> Self {
        Entry {
            key,
            value,
            kind: EntryKind::Put,
            seq,
        }
    }

    /// A tombstone. Carries an empty value — readers must check `kind`, not the
    /// value, to decide presence.
    pub fn delete(key: Key, seq: u64) -> Self {
        Entry {
            key,
            value: Vec::new(),
            kind: EntryKind::Delete,
            seq,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.kind == EntryKind::Delete
    }
}

/// The public operations a client can ask the store to perform.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Request {
    Get(Key),
    Set(Key, Value),
    Delete(Key),
}

/// The result of a [`Request`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Response {
    /// Answer to a `Get`: `Some(value)` if present, `None` if absent or deleted.
    Value(Option<Value>),
    /// Acknowledgement of a successful mutation.
    Ok,
}
