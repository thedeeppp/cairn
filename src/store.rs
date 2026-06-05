//! Phase 0 storage: an in-memory, `HashMap`-backed store behind a `Store` trait.
//!
//! This is deliberately the simplest thing that works. With a single mutable map
//! there is no older layer to shadow, so `delete` just removes the key — a
//! reader can't tell "deleted" from "never set", and shouldn't need to. The
//! tombstone marker in [`crate::api::Entry`] becomes load-bearing later, when
//! the WAL must *log* deletes and immutable SSTables can't be edited in place.
//! The `Store` interface stays stable across all of that so callers don't change.

use std::collections::HashMap;

use crate::api::{Key, Request, Response, Value};

/// The operations every storage backend must support.
pub trait Store {
    /// Returns the value for `key`, or `None` if it was never set or has been
    /// deleted.
    fn get(&self, key: &[u8]) -> Option<Value>;

    /// Inserts or overwrites `key` with `value`.
    fn set(&mut self, key: Key, value: Value);

    /// Removes `key`. A subsequent `get` returns `None`.
    fn delete(&mut self, key: &[u8]);
}

/// In-memory store: a plain key→value map.
#[derive(Default)]
pub struct MemStore {
    data: HashMap<Key, Value>,
}

impl MemStore {
    pub fn new() -> Self {
        MemStore::default()
    }

    /// Convenience dispatch over the public command surface, mostly so the
    /// `Request`/`Response` API types have a real user.
    pub fn execute(&mut self, req: Request) -> Response {
        match req {
            Request::Get(key) => Response::Value(self.get(&key)),
            Request::Set(key, value) => {
                self.set(key, value);
                Response::Ok
            }
            Request::Delete(key) => {
                self.delete(&key);
                Response::Ok
            }
        }
    }
}

impl Store for MemStore {
    fn get(&self, key: &[u8]) -> Option<Value> {
        self.data.get(key).cloned()
    }

    fn set(&mut self, key: Key, value: Value) {
        self.data.insert(key, value);
    }

    fn delete(&mut self, key: &[u8]) {
        self.data.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(s: &str) -> Key {
        s.as_bytes().to_vec()
    }

    #[test]
    fn put_then_get() {
        let mut s = MemStore::new();
        s.set(k("a"), k("1"));
        assert_eq!(s.get(b"a"), Some(k("1")));
    }

    #[test]
    fn get_missing_is_none() {
        let s = MemStore::new();
        assert_eq!(s.get(b"nope"), None);
    }

    #[test]
    fn overwrite_keeps_newest() {
        let mut s = MemStore::new();
        s.set(k("a"), k("1"));
        s.set(k("a"), k("2"));
        assert_eq!(s.get(b"a"), Some(k("2")));
    }

    #[test]
    fn delete_hides_value() {
        let mut s = MemStore::new();
        s.set(k("a"), k("1"));
        s.delete(b"a");
        assert_eq!(s.get(b"a"), None);
    }

    #[test]
    fn delete_missing_is_noop() {
        let mut s = MemStore::new();
        s.delete(b"ghost");
        assert_eq!(s.get(b"ghost"), None);
    }

    #[test]
    fn delete_then_reput() {
        let mut s = MemStore::new();
        s.set(k("a"), k("1"));
        s.delete(b"a");
        s.set(k("a"), k("2"));
        assert_eq!(s.get(b"a"), Some(k("2")));
    }

    #[test]
    fn execute_dispatches_requests() {
        let mut s = MemStore::new();
        assert_eq!(s.execute(Request::Set(k("a"), k("1"))), Response::Ok);
        assert_eq!(
            s.execute(Request::Get(k("a"))),
            Response::Value(Some(k("1")))
        );
        assert_eq!(s.execute(Request::Delete(k("a"))), Response::Ok);
        assert_eq!(s.execute(Request::Get(k("a"))), Response::Value(None));
    }
}
