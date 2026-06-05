//! Strata — a write-optimized, persistent key-value store built on an
//! LSM-tree. Built strictly phase by phase; see `CLAUDE.md`.
//!
//! - Phase 0: data model ([`api`]) and an in-memory store ([`store`]).
//! - Phase 1: durable [`engine::Engine`] backed by a write-ahead log ([`wal`]),
//!   replayed on startup.

pub mod api;
pub mod engine;
pub mod store;
pub mod wal;

pub use api::{Entry, EntryKind, Key, Request, Response, Value};
pub use engine::Engine;
pub use store::{MemStore, Store};
pub use wal::Wal;
