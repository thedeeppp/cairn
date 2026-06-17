//! Strata — a write-optimized, persistent key-value store built on an
//! LSM-tree. Built strictly phase by phase; see `CLAUDE.md`.
//!
//! - Phase 0: data model ([`api`]) and an in-memory store ([`store`]).
//! - Phase 1: durable [`engine::Engine`] backed by a write-ahead log ([`wal`]),
//!   replayed on startup.
//! - Phase 2: sorted [`memtable::MemTable`] that flushes to immutable, sorted
//!   [`sstable::SsTable`] files; the WAL is truncated after each flush.
//! - Phase 3: SSTable read path — sparse index + footer, reads from disk.
//! - Phase 4: a per-SSTable [`bloom::Bloom`] filter skips tables that can't hold
//!   a key, with no disk read.
//! - Phase 5: background size-tiered [`compaction`] merges SSTables, collapsing
//!   to the newest value per key and dropping tombstones (crash-safely).
//! - Phase 6: compaction scans its inputs in parallel with rayon; a hand-rolled
//!   `cargo bench` target reports write/read/compaction throughput.

pub mod api;
pub mod bloom;
pub mod compaction;
pub mod engine;
pub mod memtable;
pub mod sstable;
pub mod store;
pub mod wal;

pub use api::{Entry, EntryKind, Key, Request, Response, Value};
pub use bloom::Bloom;
pub use engine::Engine;
pub use memtable::MemTable;
pub use sstable::SsTable;
pub use store::{MemStore, Store};
pub use wal::Wal;
