//! Hand-rolled benchmarks for the engine — run with `cargo bench`.
//!
//! Criterion is intentionally avoided: the project keeps its dependency list
//! tiny, and `std::time::Instant` is enough to report throughput and latency.
//! Each section builds a fresh store under a temp directory and cleans up after
//! itself. Numbers are indicative, not statistically rigorous.

use std::path::PathBuf;
use std::time::Instant;

use cairn::{Engine, Store};

const VALUE_LEN: usize = 100;

// Every write fsyncs the WAL, so wall time is dominated by durable writes, not
// CPU. These counts keep `cargo bench` to a few seconds while still triggering
// dozens of flushes and a real multi-table compaction.
const N: u64 = 20_000;
const READS: u64 = 100_000;

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut p = std::env::temp_dir();
    p.push(format!(
        "cairn-bench-{}-{}-{}",
        tag,
        std::process::id(),
        nanos
    ));
    p
}

fn key(i: u64) -> Vec<u8> {
    format!("key{i:012}").into_bytes()
}

/// A tiny xorshift PRNG — enough to pick pseudo-random keys without a dep.
fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

fn report(label: &str, ops: u64, bytes: u64, elapsed: std::time::Duration) {
    let secs = elapsed.as_secs_f64();
    let per_sec = ops as f64 / secs;
    let mib = bytes as f64 / (1024.0 * 1024.0) / secs;
    let latency_us = secs * 1e6 / ops as f64;
    println!(
        "{label:<28} {ops:>9} ops  {secs:>7.3}s  {per_sec:>12.0} ops/s  \
         {mib:>7.1} MiB/s  {latency_us:>7.2} µs/op"
    );
}

fn bench_writes() {
    let dir = temp_dir("writes");
    // Large flush threshold so we measure write throughput, not flush churn.
    let mut db = Engine::open_with_threshold(&dir, 64 << 20).unwrap();

    let n: u64 = N;
    let value = vec![b'x'; VALUE_LEN];
    let start = Instant::now();
    for i in 0..n {
        db.set(key(i), value.clone()).unwrap();
    }
    let elapsed = start.elapsed();
    let bytes = n * (15 + VALUE_LEN as u64);
    report("write (memtable)", n, bytes, elapsed);

    drop(db);
    std::fs::remove_dir_all(&dir).ok();
}

fn bench_reads() {
    let dir = temp_dir("reads");
    let mut db = Engine::open_with_threshold(&dir, 64 << 10).unwrap();

    let n: u64 = N;
    let value = vec![b'x'; VALUE_LEN];
    for i in 0..n {
        db.set(key(i), value.clone()).unwrap();
    }
    // Compact to a single indexed table so reads hit the steady-state path.
    db.compact_now().unwrap();

    let m: u64 = READS;
    let mut rng = 0x9e37_79b9_7f4a_7c15u64;

    // Present keys.
    let start = Instant::now();
    let mut hits = 0u64;
    for _ in 0..m {
        let i = xorshift(&mut rng) % n;
        if db.get(&key(i)).unwrap().is_some() {
            hits += 1;
        }
    }
    let elapsed = start.elapsed();
    assert_eq!(hits, m, "every present key must be found");
    report("read (present)", m, m * VALUE_LEN as u64, elapsed);

    // Absent keys — the Bloom filter should make these cheap.
    let start = Instant::now();
    let mut misses = 0u64;
    for _ in 0..m {
        let i = n + (xorshift(&mut rng) % n);
        if db.get(&key(i)).unwrap().is_none() {
            misses += 1;
        }
    }
    let elapsed = start.elapsed();
    assert_eq!(misses, m, "absent keys must read as None");
    report("read (absent, bloom)", m, 0, elapsed);

    drop(db);
    std::fs::remove_dir_all(&dir).ok();
}

fn bench_compaction() {
    let dir = temp_dir("compaction");
    // Small threshold + auto-compaction off => many SSTables to merge at once.
    let mut db = Engine::open_with_threshold(&dir, 64 << 10).unwrap();

    let n: u64 = N;
    let value = vec![b'x'; VALUE_LEN];
    for i in 0..n {
        db.set(key(i), value.clone()).unwrap();
    }
    // Overwrite half, so compaction has real work collapsing versions.
    for i in 0..n / 2 {
        db.set(key(i), value.clone()).unwrap();
    }
    let tables = db.sstable_count();

    let start = Instant::now();
    db.compact_now().unwrap();
    let elapsed = start.elapsed();
    assert_eq!(db.sstable_count(), 1);

    let bytes = (n + n / 2) * (15 + VALUE_LEN as u64);
    println!("compaction merged {tables} tables:");
    report("compaction (rayon scan)", n + n / 2, bytes, elapsed);

    drop(db);
    std::fs::remove_dir_all(&dir).ok();
}

fn main() {
    println!(
        "{:<28} {:>9}      {:>7}   {:>12}   {:>7}    {:>7}",
        "benchmark", "ops", "time", "throughput", "rate", "latency"
    );
    bench_writes();
    bench_reads();
    bench_compaction();
}
