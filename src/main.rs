//! Tiny smoke-test binary. The real interface is the library crate; this just
//! proves the durable engine wires together and survives a reopen.

use strata::{Engine, Store};

fn main() -> std::io::Result<()> {
    let dir = std::env::temp_dir().join("strata-demo");

    {
        let mut db = Engine::open(&dir)?;
        db.set(b"hello".to_vec(), b"world".to_vec())?;
    } // drop: flush + close, like a shutdown

    // Reopen from the WAL alone.
    let db = Engine::open(&dir)?;
    match db.get(b"hello") {
        Some(v) => println!("recovered hello = {}", String::from_utf8_lossy(&v)),
        None => println!("hello is absent"),
    }

    std::fs::remove_dir_all(&dir).ok();
    Ok(())
}
