//! Phase 5 integration test: size-tiered compaction merges SSTables into one,
//! collapsing each key to its newest value and dropping tombstones — preserving
//! correctness while reclaiming on-disk space, and surviving a reopen.

use std::path::{Path, PathBuf};

use strata::{Engine, Store};

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut p = std::env::temp_dir();
    p.push(format!(
        "strata-it5-{}-{}-{}",
        tag,
        std::process::id(),
        nanos
    ));
    p
}

/// Total size of all `*.sst` files in `dir`.
fn sst_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) == Some("sst") {
            total += std::fs::metadata(&path).unwrap().len();
        }
    }
    total
}

fn sst_count(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sst"))
        .count()
}

#[test]
fn compaction_reclaims_space_and_preserves_correctness() {
    let dir = temp_dir("compact");

    // Auto-compaction off (open_with_threshold) so the table set is predictable;
    // we trigger one full compaction explicitly with `compact_now`.
    let mut db = Engine::open_with_threshold(&dir, 256).unwrap();

    // 400 keys, each later overwritten with a *larger* value — so the original
    // small versions become dead weight spread across many SSTables.
    for i in 0..400 {
        db.set(format!("k{i:04}").into_bytes(), b"v".to_vec())
            .unwrap();
    }
    for i in 0..400 {
        db.set(format!("k{i:04}").into_bytes(), vec![b'x'; 64])
            .unwrap();
    }
    // Delete a quarter of them outright.
    for i in 0..100 {
        db.delete(format!("k{i:04}").as_bytes()).unwrap();
    }

    let count_before = sst_count(&dir);
    let bytes_before = sst_bytes(&dir);
    assert!(count_before > 1, "want several tables to merge");

    db.compact_now().unwrap();

    // Everything collapsed into a single superseding table, smaller than before.
    assert_eq!(sst_count(&dir), 1, "full compaction yields one table");
    let bytes_after = sst_bytes(&dir);
    assert!(
        bytes_after < bytes_before,
        "on-disk size should drop: {bytes_before} -> {bytes_after}"
    );

    // Correctness preserved: deleted keys gone, survivors at their newest value.
    for i in 0..100 {
        assert_eq!(db.get(format!("k{i:04}").as_bytes()).unwrap(), None);
    }
    for i in 100..400 {
        assert_eq!(
            db.get(format!("k{i:04}").as_bytes()).unwrap(),
            Some(vec![b'x'; 64])
        );
    }

    // Survives a reopen — and a crash that left a superseded input behind cannot
    // resurrect a deleted key.
    drop(db);
    let db = Engine::open_with_threshold(&dir, 256).unwrap();
    assert_eq!(db.get(b"k0000").unwrap(), None); // stayed deleted
    assert_eq!(db.get(b"k0399").unwrap(), Some(vec![b'x'; 64]));

    std::fs::remove_dir_all(&dir).ok();
}
