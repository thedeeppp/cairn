//! Phase 2 integration test: many writes force several flushes; verify the
//! store stays correct across flushes and a reopen, and that the SSTable files
//! on disk are sorted.

use std::path::PathBuf;

use cairn::{Engine, SsTable, Store};

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut p = std::env::temp_dir();
    p.push(format!(
        "cairn-it2-{}-{}-{}",
        tag,
        std::process::id(),
        nanos
    ));
    p
}

#[test]
fn flushes_then_reopen_preserve_everything() {
    let dir = temp_dir("lsm");
    let n = 1_000;

    {
        // ~30-byte values, small threshold => many SSTables.
        let mut db = Engine::open_with_threshold(&dir, 1024).unwrap();
        for i in 0..n {
            db.set(
                format!("k{i:05}").into_bytes(),
                format!("value-{i:05}").into_bytes(),
            )
            .unwrap();
        }
        // Mutate keys that already live in flushed SSTables.
        db.set(b"k00000".to_vec(), b"NEW".to_vec()).unwrap();
        db.delete(b"k00042").unwrap();

        assert!(db.sstable_count() > 1, "expected multiple flushes");
    } // shutdown

    // Reopen purely from SSTables + WAL.
    let db = Engine::open_with_threshold(&dir, 1024).unwrap();
    assert_eq!(db.get(b"k00000").unwrap(), Some(b"NEW".to_vec())); // overwrite survived
    assert_eq!(db.get(b"k00042").unwrap(), None); // delete survived
    assert_eq!(db.get(b"k00500").unwrap(), Some(b"value-00500".to_vec()));
    assert_eq!(db.get(b"k00999").unwrap(), Some(b"value-00999".to_vec()));
    assert_eq!(db.get(b"missing").unwrap(), None);

    // Every SSTable on disk is independently key-sorted.
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) == Some("sst") {
            let entries = SsTable::open(path).unwrap().scan().unwrap();
            let keys: Vec<_> = entries.iter().map(|e| e.key.clone()).collect();
            let mut sorted = keys.clone();
            sorted.sort();
            assert_eq!(keys, sorted);
        }
    }

    std::fs::remove_dir_all(&dir).ok();
}
