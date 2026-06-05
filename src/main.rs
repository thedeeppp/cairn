//! Tiny smoke-test binary. The real interface is the library crate; this just
//! proves the store wires together and gives `cargo run` something to do.

use strata::{MemStore, Store};

fn main() {
    let mut store = MemStore::new();
    store.set(b"hello".to_vec(), b"world".to_vec());
    match store.get(b"hello") {
        Some(v) => println!("hello = {}", String::from_utf8_lossy(&v)),
        None => println!("hello is absent"),
    }
}
