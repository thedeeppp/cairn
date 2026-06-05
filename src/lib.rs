//! Strata — a write-optimized, persistent key-value store built on an
//! LSM-tree. Built strictly phase by phase; see `CLAUDE.md`.
//!
//! Phase 0 ships the data model ([`api`]) and an in-memory store ([`store`]).

pub mod api;
pub mod store;

pub use api::{Entry, EntryKind, Key, Request, Response, Value};
pub use store::{MemStore, Store};
