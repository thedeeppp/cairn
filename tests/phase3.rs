//! Phase 3 integration test: the on-disk read path. Get must find keys whether
//! they live in the active MemTable or in any SSTable (via the sparse index),
//! and a tombstone must win over an older on-disk value.

use std::path::PathBuf;

use cairn::{Engine, Store};

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut p = std::env::temp_dir();
    p.push(format!(
        "cairn-it3-{}-{}-{}",
        tag,
        std::process::id(),
        nanos
    ));
    p
}

#[test]
fn reads_span_memtable_and_many_sstables() {
    let dir = temp_dir("readpath");

    // Small threshold => the 2000 keys spread across many SSTables, with the
    // most recent writes still in the active MemTable.
    let mut db = Engine::open_with_threshold(&dir, 512).unwrap();
    for i in 0..2000 {
        db.set(
            format!("k{i:05}").into_bytes(),
            format!("v{i:05}").into_bytes(),
        )
        .unwrap();
    }
    assert!(db.sstable_count() > 5, "expected several SSTables on disk");

    // A spread of keys: earliest (deep SSTable), middle, and latest (memtable).
    for i in [0usize, 1, 17, 500, 1234, 1999] {
        assert_eq!(
            db.get(format!("k{i:05}").as_bytes()).unwrap(),
            Some(format!("v{i:05}").into_bytes()),
            "key k{i:05} should be found via the read path"
        );
    }
    assert_eq!(db.get(b"nope").unwrap(), None);

    // Overwrite a key that lives in an old SSTable; the newer write wins.
    db.set(b"k00000".to_vec(), b"updated".to_vec()).unwrap();
    assert_eq!(db.get(b"k00000").unwrap(), Some(b"updated".to_vec()));

    // Delete a key that lives in an old SSTable; the tombstone shadows it.
    db.delete(b"k00777").unwrap();
    assert_eq!(db.get(b"k00777").unwrap(), None);

    drop(db);

    // Reopen: frozen MemTables are gone, so every read now comes from disk
    // (SSTables) or the replayed WAL. Everything still resolves correctly.
    let db = Engine::open_with_threshold(&dir, 512).unwrap();
    assert_eq!(db.get(b"k00000").unwrap(), Some(b"updated".to_vec()));
    assert_eq!(db.get(b"k00777").unwrap(), None);
    assert_eq!(db.get(b"k01500").unwrap(), Some(b"v01500".to_vec()));

    std::fs::remove_dir_all(&dir).ok();
}
