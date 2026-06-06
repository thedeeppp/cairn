//! Phase 1 integration test: durability across a full close/reopen cycle.
//!
//! Write a batch of mutations (puts, an overwrite, deletes), drop the store to
//! simulate shutdown, then reopen purely from the WAL and confirm every effect
//! — including deletions — survived.

use std::path::PathBuf;

use strata::{Engine, Store};

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut p = std::env::temp_dir();
    p.push(format!(
        "strata-it-{}-{}-{}",
        tag,
        std::process::id(),
        nanos
    ));
    p
}

#[test]
fn data_survives_drop_and_reopen() {
    let dir = temp_dir("recovery");

    {
        let mut db = Engine::open(&dir).unwrap();
        for i in 0..100 {
            db.set(
                format!("key{i}").into_bytes(),
                format!("val{i}").into_bytes(),
            )
            .unwrap();
        }
        db.set(b"key0".to_vec(), b"overwritten".to_vec()).unwrap();
        db.delete(b"key50").unwrap();
        db.delete(b"key99").unwrap();
    } // shutdown

    let db = Engine::open(&dir).unwrap();

    assert_eq!(db.get(b"key0").unwrap(), Some(b"overwritten".to_vec())); // overwrite kept
    assert_eq!(db.get(b"key1").unwrap(), Some(b"val1".to_vec()));
    assert_eq!(db.get(b"key50").unwrap(), None); // delete kept
    assert_eq!(db.get(b"key99").unwrap(), None); // delete kept
    assert_eq!(db.get(b"key98").unwrap(), Some(b"val98".to_vec()));
    assert_eq!(db.get(b"missing").unwrap(), None);

    std::fs::remove_dir_all(&dir).ok();
}
