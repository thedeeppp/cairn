//! Phase 4 integration test: Bloom filters make absent-key lookups cheap by
//! skipping SSTables that can't hold the key, while never hiding a present key.

use std::path::PathBuf;

use strata::{Engine, Store};

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut p = std::env::temp_dir();
    p.push(format!(
        "strata-it4-{}-{}-{}",
        tag,
        std::process::id(),
        nanos
    ));
    p
}

#[test]
fn absent_lookups_skip_sstables_present_keys_survive() {
    let dir = temp_dir("bloom");

    let mut db = Engine::open_with_threshold(&dir, 512).unwrap();
    for i in 0..1500 {
        db.set(
            format!("k{i:05}").into_bytes(),
            format!("v{i:05}").into_bytes(),
        )
        .unwrap();
    }
    assert!(
        db.sstable_count() > 4,
        "want several SSTables to skip across"
    );

    // 1000 keys that exist nowhere: the Bloom filter should reject them from
    // (almost) every table without a block read.
    for i in 0..1000 {
        assert_eq!(db.get(format!("missing{i}").as_bytes()).unwrap(), None);
    }
    let rate = db.bloom_skip_rate();
    assert!(rate > 0.9, "expected most probes skipped, got {rate}");

    // The filter must never produce a false negative: every key is still found.
    for i in 0..1500 {
        assert_eq!(
            db.get(format!("k{i:05}").as_bytes()).unwrap(),
            Some(format!("v{i:05}").into_bytes())
        );
    }

    // Survives a reopen (filters are persisted in each SSTable).
    drop(db);
    let db = Engine::open_with_threshold(&dir, 512).unwrap();
    assert_eq!(db.get(b"k00500").unwrap(), Some(b"v00500".to_vec()));
    assert_eq!(db.get(b"missing-after-reopen").unwrap(), None);
    assert!(
        db.bloom_skip_rate() > 0.0,
        "filter should work after reopen"
    );

    std::fs::remove_dir_all(&dir).ok();
}
