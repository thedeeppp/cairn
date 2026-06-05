//! Phase 0 integration test: drive the public store through a realistic
//! sequence of operations from outside the crate.

use strata::{MemStore, Store};

#[test]
fn put_overwrite_delete_lifecycle() {
    let mut store = MemStore::new();

    store.set(b"name".to_vec(), b"ada".to_vec());
    store.set(b"lang".to_vec(), b"rust".to_vec());
    assert_eq!(store.get(b"name"), Some(b"ada".to_vec()));
    assert_eq!(store.get(b"lang"), Some(b"rust".to_vec()));

    // Overwrite wins.
    store.set(b"name".to_vec(), b"grace".to_vec());
    assert_eq!(store.get(b"name"), Some(b"grace".to_vec()));

    // Delete hides the value.
    store.delete(b"name");
    assert_eq!(store.get(b"name"), None);

    // Unrelated key is untouched.
    assert_eq!(store.get(b"lang"), Some(b"rust".to_vec()));

    // Re-inserting after delete works.
    store.set(b"name".to_vec(), b"linus".to_vec());
    assert_eq!(store.get(b"name"), Some(b"linus".to_vec()));
}
