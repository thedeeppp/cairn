//! Phase 0 storage: an in-memory, `HashMap`-backed store behind a `Store` trait.
//!
//! This is deliberately the simplest thing that works. With a single mutable map
//! there is no older layer to shadow, so `delete` just removes the key — a
//! reader can't tell "deleted" from "never set", and shouldn't need to. The
//! tombstone marker in [`crate::api::Entry`] becomes load-bearing later, when
//! the WAL must *log* deletes and immutable SSTables can't be edited in place.
//! The `Store` interface stays stable across all of that so callers don't change.

use std::collections::HashMap;
use std::io;

use crate::api::{Key, Request, Response, Value};

/// The operations every storage backend must support.
///
/// All three return [`io::Result`] because a durable store touches the disk:
/// mutations must persist before reporting success, and as of Phase 3 a `get`
/// may have to read on-disk SSTables.
pub trait Store {
    /// Returns the value for `key`, or `None` if it was never set or has been
    /// deleted.
    fn get(&self, key: &[u8]) -> io::Result<Option<Value>>;

    /// Inserts or overwrites `key` with `value`.
    fn set(&mut self, key: Key, value: Value) -> io::Result<()>;

    /// Removes `key`. A subsequent `get` returns `None`.
    fn delete(&mut self, key: &[u8]) -> io::Result<()>;

    /// Convenience dispatch over the public command surface, so the
    /// `Request`/`Response` API types have a real user. Provided once for every
    /// backend.
    fn execute(&mut self, req: Request) -> io::Result<Response> {
        Ok(match req {
            Request::Get(key) => Response::Value(self.get(&key)?),
            Request::Set(key, value) => {
                self.set(key, value)?;
                Response::Ok
            }
            Request::Delete(key) => {
                self.delete(&key)?;
                Response::Ok
            }
        })
    }
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
}

impl Store for MemStore {
    fn get(&self, key: &[u8]) -> io::Result<Option<Value>> {
        Ok(self.data.get(key).cloned())
    }

    fn set(&mut self, key: Key, value: Value) -> io::Result<()> {
        self.data.insert(key, value);
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        self.data.remove(key);
        Ok(())
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
        s.set(k("a"), k("1")).unwrap();
        assert_eq!(s.get(b"a").unwrap(), Some(k("1")));
    }

    #[test]
    fn get_missing_is_none() {
        let s = MemStore::new();
        assert_eq!(s.get(b"nope").unwrap(), None);
    }

    #[test]
    fn overwrite_keeps_newest() {
        let mut s = MemStore::new();
        s.set(k("a"), k("1")).unwrap();
        s.set(k("a"), k("2")).unwrap();
        assert_eq!(s.get(b"a").unwrap(), Some(k("2")));
    }

    #[test]
    fn delete_hides_value() {
        let mut s = MemStore::new();
        s.set(k("a"), k("1")).unwrap();
        s.delete(b"a").unwrap();
        assert_eq!(s.get(b"a").unwrap(), None);
    }

    #[test]
    fn delete_missing_is_noop() {
        let mut s = MemStore::new();
        s.delete(b"ghost").unwrap();
        assert_eq!(s.get(b"ghost").unwrap(), None);
    }

    #[test]
    fn delete_then_reput() {
        let mut s = MemStore::new();
        s.set(k("a"), k("1")).unwrap();
        s.delete(b"a").unwrap();
        s.set(k("a"), k("2")).unwrap();
        assert_eq!(s.get(b"a").unwrap(), Some(k("2")));
    }

    #[test]
    fn execute_dispatches_requests() {
        let mut s = MemStore::new();
        assert_eq!(
            s.execute(Request::Set(k("a"), k("1"))).unwrap(),
            Response::Ok
        );
        assert_eq!(
            s.execute(Request::Get(k("a"))).unwrap(),
            Response::Value(Some(k("1")))
        );
        assert_eq!(s.execute(Request::Delete(k("a"))).unwrap(), Response::Ok);
        assert_eq!(
            s.execute(Request::Get(k("a"))).unwrap(),
            Response::Value(None)
        );
    }
}
