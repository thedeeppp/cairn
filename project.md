# Cairn — Project Reference

A complete technical reference for the Cairn codebase: what it is, how it's
built, how every module fits together, how to run and demo it, and where its
sharp edges are. For the narrative overview and changelog, see `README.md`; for
the phase-by-phase build rules, see `CLAUDE.md`.

---

## 1. What Cairn is

Cairn is a **persistent, embeddable, write-optimized key–value store** written
from scratch in Rust on top of a **Log-Structured Merge (LSM) tree**. It stores
opaque `bytes → bytes` pairs durably on disk, with these guarantees:

- **Durability** — once `set`/`delete` returns, the mutation has been `fsync`ed
  and survives a crash.
- **Read-your-writes** — a `get` always sees the most recent value, or `None`
  after a delete.
- **Crash safety** — `open` recovers exact state after a crash at any point,
  including mid-flush and mid-compaction.
- **Bounded space** — background compaction collapses overwrites and deletions.

It is deliberately small (~1,900 lines of `src`) and dependency-light. It is the
same engine architecture behind LevelDB / RocksDB / Cassandra, implemented at a
size you can read end to end.

**Scope / non-goals:** single process, single node, embedded as a library. No
network server, no transactions/MVCC, no range-scan API, no concurrent multi-
writer access. See §9 for the full list of limitations.

---

## 2. Tech stack

| Concern        | Choice                                                              |
|----------------|--------------------------------------------------------------------|
| Language       | Rust, edition 2024                                                  |
| Serialization  | `serde` + `bincode` (length-prefixed records)                      |
| Parallelism    | `rayon` (parallel SSTable scan during compaction)                  |
| Benchmarks     | hand-rolled with `std::time::Instant` (no criterion)               |
| Tests          | `cargo test` — unit tests per module + one integration test/phase  |
| Lint/format    | `cargo clippy --all-targets -- -D warnings`, `cargo fmt`           |

The dependency list is intentionally tiny: `serde`, `bincode`, `rayon`.

---

## 3. Repository layout

```
cairn/
├── Cargo.toml          # crate "cairn", deps, [[bench]] target
├── Cargo.lock
├── README.md           # narrative overview + changelog
├── CLAUDE.md           # phase-by-phase build rules
├── PROJECT.md          # this file
├── src/
│   ├── lib.rs          # crate root: module decls + public re-exports
│   ├── main.rs         # smoke-test binary (write → reopen → read)
│   ├── api.rs          # data model: Entry, EntryKind, Request/Response
│   ├── store.rs        # Store trait + in-memory MemStore (Phase 0 reference)
│   ├── wal.rs          # write-ahead log: append, replay, truncate
│   ├── memtable.rs     # sorted in-memory write buffer (BTreeMap)
│   ├── sstable.rs      # immutable on-disk Sorted String Table + reader
│   ├── bloom.rs        # Bloom filter (FNV-1a double hashing)
│   ├── compaction.rs   # pure merge of several SSTables → one
│   └── engine.rs       # the durable Engine that ties it all together
├── tests/
│   └── phase0..5.rs    # one black-box integration test per phase
└── benches/
    └── engine_bench.rs # write / read / compaction throughput + latency
```

Approximate sizes: `engine.rs` ~650 lines, `sstable.rs` ~395, the rest 80–190.

---

## 4. The data model (`api.rs`)

Everything flows as an **`Entry`** — a single versioned record:

```rust
pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

pub enum EntryKind { Put, Delete }

pub struct Entry {
    pub key: Key,
    pub value: Value,     // empty for a Delete
    pub kind: EntryKind,
    pub seq: u64,         // monotonic; newest seq wins for a key
}
```

- Keys and values are **opaque bytes** — the store never interprets them.
- A **`Delete` is a tombstone**: because on-disk data is immutable, a deletion is
  a new record that *shadows* older values for the same key. Tombstones are only
  physically removed during compaction.
- **`seq`** is a global monotonic counter. When the same key appears in multiple
  layers, the highest `seq` is the live one.

The public command surface is `Request::{Get,Set,Delete}` → `Response::{Value,Ok}`,
dispatched by `Store::execute`.

---

## 5. The storage interface (`store.rs`)

```rust
pub trait Store {
    fn get(&self, key: &[u8]) -> io::Result<Option<Value>>;
    fn set(&mut self, key: Key, value: Value) -> io::Result<()>;
    fn delete(&mut self, key: &[u8]) -> io::Result<()>;
    fn execute(&mut self, req: Request) -> io::Result<Response>; // provided
}
```

All three return `io::Result` because the durable backend touches disk on both
the write and read path. Two implementations:

- **`MemStore`** — a plain `HashMap`, the Phase 0 reference. `delete` just removes
  the key (no tombstone needed: there's no older layer to shadow).
- **`Engine`** — the durable LSM tree (§7). The trait is identical, so callers
  don't change as the backend gains durability.

---

## 6. Architecture overview

Data is organized in layers, **newest at the top**. A write enters at the top; a
read walks down and stops at the first layer holding the key — so a newer value
(or a tombstone) naturally shadows everything older.

```text
                           writes: set(k,v) / delete(k)
                                       │
        ┌──────────────────────────────┼───────────────────────────────┐
        │                              ▼                                 │
        │   ① append + fsync   ┌───────────────────┐                     │
        │   ──────────────────▶│   WAL (wal.log)   │  durability journal │
        │                      └───────────────────┘                     │
        │   ② apply in memory          │                                 │
        │                              ▼                                 │
   IN   │                      ┌───────────────────┐   reads start here  │
  MEMORY│   get(k) ───────────▶│  active MemTable  │ ◀── newest          │
        │                      │  (sorted BTreeMap)│                     │
        │                      └───────────────────┘                     │
        └──────────────────────────────┼───────────────────────────────┘
                       ③ flush when size ≥ threshold
                          (then ④ truncate the WAL)
        ┌──────────────────────────────▼───────────────────────────────┐
        │                      ┌───────────────────┐                     │
   ON   │   reads fall through │  SSTable (newest) │   each table:       │
  DISK  │   newest → oldest,   ├───────────────────┤   • Bloom filter    │
        │   Bloom-gated        │  SSTable …        │   • sparse index    │
        │                      ├───────────────────┤   • sorted records  │
        │                      │  SSTable (oldest) │   immutable         │
        │                      └─────────┬─────────┘                     │
        │       size-tiered compaction   │ (background thread, rayon)    │
        │                                ▼                               │
        │                      ┌───────────────────┐                     │
        │                      │ one merged SSTable│  newest seq per key,│
        │                      │   (superseding)   │  tombstones dropped │ ◀── oldest
        │                      └───────────────────┘                     │
        └────────────────────────────────────────────────────────────────┘
```

### Component responsibilities

| Module       | Responsibility                                                                 |
|--------------|--------------------------------------------------------------------------------|
| `wal`        | Append-only durability journal; fsync-per-write; replayed on `open`.           |
| `memtable`   | Active write buffer — a sorted `BTreeMap<Key, Entry>` with a byte-size estimate. |
| `sstable`    | Immutable, sorted on-disk file: data + sparse index + Bloom filter + footer.   |
| `bloom`      | Per-table membership filter; answers "definitely absent?" with no disk read.   |
| `compaction` | The *pure* merge function: parallel-scan inputs, keep newest seq, drop tombstones. |
| `engine`     | Orchestrates write path, flush, read path, background compaction, recovery.    |

---

## 7. The Engine (`engine.rs`)

`Engine` owns the directory and holds: the open WAL, the active MemTable, a
`Vec<Arc<SsTable>>` (oldest → newest), the `seq` counter, the next SSTable number,
the flush/compaction thresholds, an optional in-flight `CompactionJob`, and Bloom
stats.

### Opening / tuning

```rust
Engine::open(dir)                                  // 1 MiB flush, auto-compact at 4 tables
Engine::open_with_threshold(dir, bytes)            // custom flush, auto-compaction OFF
Engine::open_with_thresholds(dir, bytes, trigger)  // custom flush + compaction trigger
```

Defaults: `DEFAULT_MEMTABLE_BYTES = 1 MiB`, `DEFAULT_COMPACTION_TRIGGER = 4`.
A `compaction_threshold` of `0` disables auto-compaction (you can still force it
with `compact_now`).

### Write path (`set` / `delete` → `write`)

1. Assign the next `seq`, build an `Entry`.
2. **`wal.append(&entry)`** — serialize, write length + bytes, `flush` (BufWriter
   → OS), `sync_all` (OS → disk). Durable before returning.
3. **`active.put(entry)`** — insert into the sorted MemTable.
4. If `active.size_bytes() >= threshold` → **`flush()`**.
5. `poll_compaction()` (reap a finished background merge) then
   `maybe_start_compaction()` (spawn one if the table count crossed the trigger).

### Flush (`flush`)

1. `std::mem::take` the active MemTable (leaving an empty one).
2. Write it to `sst-NNNNNN.sst` via `SsTable::create` — temp file → fsync →
   atomic rename → fsync dir. The entries are already key-sorted (BTreeMap).
3. **`wal.truncate()`** — only *after* the SSTable is durable.
4. Push the new `Arc<SsTable>`; the frozen MemTable drops, freeing its memory.

> **Core invariant:** every acknowledged mutation lives in **exactly one** of the
> WAL or an SSTable — never neither (no data loss), never duplicated in a way that
> matters (newest seq wins).

### Read path (`get`)

1. Check the **active MemTable** (in-memory). First hit wins, tombstone → `None`.
2. Otherwise iterate SSTables **newest → oldest**. For each:
   - `bloom.may_contain(key)` → if `false`, **skip with no disk read** (count a
     "skip"); a `true` means probe for real.
   - `sst.get(key)` binary-searches the in-memory sparse index for the block that
     could hold the key, then scans forward ≤ `INDEX_INTERVAL` (16) records.
   - First hit wins; a tombstone resolves to `None` and shadows older layers.

`bloom_skip_rate()` reports the fraction of probes the filter let us skip.

### Compaction (background)

- Triggered when `sstables.len() >= compaction_threshold`, or forced via
  `compact_now()`. No-op below 2 tables.
- `start_compaction` clones the current tables, allocates the **output's number**
  (always greater than every input), and **spawns a thread** running
  `compaction::merge_tables(inputs, drop_tombstones = true)` →
  `SsTable::create_compaction(out_path, …)`.
- `merge_tables` (`compaction.rs`): **scans every input in parallel with rayon**
  (independent disk reads + bincode decode), collapses to the newest `seq` per key
  in a `BTreeMap`, and drops tombstones. Output is sorted, ready for `create`.
- `install_compaction` joins the thread, removes the merged-away inputs from the
  vec (by `Arc::ptr_eq`), inserts the merged table at index 0 (it holds the oldest
  data), deletes the old input files, and fsyncs the dir. Tables flushed *during*
  the compaction have higher numbers and are kept.

The merged table is flagged **superseding** (`kind = 1`) in its footer.

### Recovery (`open_tuned`)

On `open`, in order:

1. **Remove stale `*.sst.tmp`** from a flush/compaction interrupted mid-write.
2. **Open every SSTable**, read its footer.
3. **Superseding cleanup:** let `cutoff` = the highest number among
   compaction-flagged tables. Delete every table numbered `< cutoff` — it was
   already merged into the newest compaction output, so keeping it could resurrect
   a dropped tombstone. (Numbers are monotonic, so a compaction output's number is
   always greater than everything it subsumed and less than anything flushed
   after it started — which makes this safe.)
4. **Restore `seq`** from the max of surviving footers' `max_seq` (no data scan).
5. **Replay the WAL** into a fresh active MemTable (everything since the last
   flush), advancing `seq`.

`Drop for Engine` calls `wait_for_compaction()` so any in-flight merge finishes
and leaves files consistent.

---

## 8. On-disk formats

### SSTable — `sst-NNNNNN.sst` (immutable)

```text
[ data:   record* ]                        each record: [u32 len LE][bincode(Entry)], keys ascending
[ index:  bincode(Vec<(Key, u64 offset)>) ]   one entry every INDEX_INTERVAL (16) records
[ bloom:  bincode(Bloom) ]                     membership filter over all keys
[ footer: 7 × u64 LE = 56 bytes ]
          index_offset | index_len | bloom_offset | bloom_len | max_seq | kind | magic
```

- **Read order:** footer first (located via fixed 56-byte size from EOF) → it
  gives the index and bloom offsets, restores `max_seq`, identifies `kind`
  (1 = compaction output), and validates `magic` (`0x5354_5241_5441_0005`).
- The index and Bloom filter are loaded into memory on `open`; the data section
  stays on disk and is read a block at a time on demand.
- **Crash safety:** written to `*.sst.tmp`, fsynced, atomically renamed, then the
  directory is fsynced — a reader never sees a half-written table.

### WAL — `wal.log` (append-only)

```text
[ u32 len LE ][ len bytes of bincode(Entry) ]   ...repeated
```

- `append` fsyncs after every record.
- `replay` stops at a short read (`UnexpectedEof`), discarding a **torn trailing
  record** from a crash mid-append — which was never acknowledged anyway.
- `truncate` (`set_len(0)` + fsync) empties the log after a flush; the open handle
  keeps appending at offset 0.

### Bloom filter (`bloom.rs`)

- Standard optimal sizing: `m = ceil(-n·ln(fp) / ln2²)` bits, `k = round(m/n·ln2)`
  hashes, target `fp = 0.01`.
- Hashing is **hand-rolled FNV-1a** with two independent offset bases, combined by
  double hashing (`h1 + i·h2`). FNV (not the stdlib hasher) is used deliberately:
  the filter is **persisted**, so the same bytes must hash identically across runs
  and Rust versions.
- Never a false negative; ~1% false positives. An empty filter contains nothing.

---

## 9. Known limitations & sharp edges ("potholes")

Cairn is a clean, correct teaching-grade engine, not a production database. The
gaps below are by design or simply not yet built — listed so they're explicit.

### Scalability

- **Compaction loads the entire dataset into RAM.** `merge_tables` collects every
  entry of every input into a `BTreeMap` before writing. There is no streaming /
  external k-way merge, so peak memory during compaction ≈ total live data size.
- **"Compact everything into one" — not truly tiered.** Each compaction rewrites
  the *whole* dataset, so write amplification grows with data size; there are no
  size tiers or levels limiting how much is merged at once.
- **One `File::open` per `get`.** Point lookups open the SSTable file on every
  call — no file-descriptor cache and no mmap, so read-heavy workloads pay a
  syscall per probed table.
- **Whole-value clones on read.** `get` clones the value `Vec<u8>` out; no
  zero-copy / `Arc<[u8]>` sharing.

### Robustness

- **No per-record checksums.** Torn *trailing* WAL records are detected by a short
  read, but **corruption in the middle** of a WAL or SSTable record is not caught
  by a CRC — it either fails to deserialize (aborting `open`) or, worst case,
  deserializes into wrong data.
- **A single corrupt WAL record fails `open` entirely.** Replay has no "skip the
  bad record and continue" path, so one bad record makes the whole store
  unavailable.
- **No process lock file.** Two `Engine::open` calls on the same directory will
  corrupt each other; nothing prevents concurrent opens.

### Concurrency

- **Single-writer, not shareable across threads.** `set` needs `&mut self`; there
  is no internal locking or `Arc<Engine>` story for concurrent readers + a writer.
- **Finished compactions are only reaped on the next `write` (or `drop` /
  `wait_for_compaction`).** On a read-only workload the merged output isn't
  installed and the old input files linger, keeping the table count (and read
  fan-out) high until the next write.

### Functional gaps

- **No range scans / iteration API**, despite SSTables being sorted — only point
  `get`.
- **No explicit `flush()` / checkpoint** on the public API; flushing happens at
  the size threshold or on `drop`. `compact_now()` merges existing SSTables but
  does **not** flush the active MemTable first.
- **No transactions, atomic batches, snapshots, or TTL.**
- **`u32` length prefix** caps a single serialized record (≈ value size) at ~4 GiB.

### Cleanups

- **Stale comment in `wal.rs`:** the `append` doc says "Phase 6 revisits
  batching," but Phase 6 delivered rayon + benchmarks, not group commit. fsync is
  still per-write. (Group commit / batched fsync is the natural next step.)
- **MemTable size estimate undercounts:** `entry_size` counts only
  `key.len() + value.len()`, ignoring `seq`, `kind`, bincode framing, and
  `BTreeMap` node overhead — so the real footprint exceeds the configured
  threshold somewhat. (Documented as approximate; it only drives a threshold.)

### Natural next steps

Group-commit / batched fsync; streaming external-merge compaction with real size
tiers or levels; an SSTable handle/block cache; per-record CRCs; a range-scan
iterator; a single-writer lock file.

---

## 10. How to build, test, run, and demo

Prerequisite: a Rust toolchain (`rustup`, stable). Everything is via `cargo`.

### Build & quality gate

```sh
cargo build                                 # debug build
cargo build --release                       # optimized build
cargo test                                  # unit + per-phase integration tests
cargo clippy --all-targets -- -D warnings   # lint (warnings are errors)
cargo fmt --check                           # formatting
```

`cargo test` runs ~37 unit tests plus one black-box integration test per phase
(`tests/phase0..5.rs`).

### Run the demo binary

`src/main.rs` is a tiny smoke test: it opens a store, writes a key, drops the
engine (a clean shutdown), reopens **from disk alone**, and reads the key back —
proving durability and recovery end to end.

```sh
cargo run
# → recovered hello = world
```

### Benchmarks

```sh
cargo bench
```

Reports write / read(present) / read(absent) / compaction throughput and latency.
Writes are `fsync`-bound, so the write number reflects your disk's sync latency
(~4 ms/op on a typical SSD here); reads and the Bloom-gated absent path are
CPU-bound and fast.

### Demoing to someone (≈ 2 minutes)

A good live walkthrough that shows the whole system working:

1. **Show it builds clean and passes everything:**
   ```sh
   cargo test && cargo clippy --all-targets -- -D warnings
   ```
   Point out: one integration test per phase, each exercising real on-disk files.

2. **Show durability + recovery with the demo binary:**
   ```sh
   cargo run
   ```
   Explain that the second `Engine::open` reads only from disk (WAL + SSTables) —
   the in-memory state from the first open is gone, yet the value comes back.

3. **Show the engine in a tiny REPL-style snippet** (drop into a scratch example
   or `cargo run` a small `main`):
   ```rust
   use cairn::{Engine, Store};

   let mut db = Engine::open("/tmp/cairn-demo")?;
   db.set(b"name".to_vec(), b"cairn".to_vec())?;
   db.delete(b"name")?;                     // writes a tombstone
   assert_eq!(db.get(b"name")?, None);      // shadowed → absent
   ```

4. **Show the disk artifacts** so the LSM structure is tangible:
   ```sh
   ls -la /tmp/cairn-demo        # wal.log + sst-NNNNNN.sst files
   ```
   With enough writes to cross the flush threshold you'll see multiple
   `sst-*.sst` files appear, then collapse to one after a compaction.

5. **Show the numbers:**
   ```sh
   cargo bench
   ```
   Call out the absent-key read being ~10× faster than a present read — that's the
   Bloom filter skipping tables with zero disk reads.

---

## 11. Build history (phases)

Cairn was built strictly phase by phase; each phase compiles, is tested, and
passes the full quality gate. See `README.md` for the detailed changelog.

| Phase | Theme                              | Lands                                             |
|-------|------------------------------------|---------------------------------------------------|
| 0     | Bootstrap                          | data model + in-memory `MemStore`                 |
| 1     | Write-ahead log + recovery         | durable `Engine`, fsync-per-write, WAL replay     |
| 2     | MemTable + flush to SSTable        | sorted `BTreeMap`, threshold flush, tombstones    |
| 3     | SSTable read path                  | sparse index + footer, disk reads, seq recovery   |
| 4     | Bloom filters                      | per-table FNV-1a filter, skip-rate tracking       |
| 5     | Size-tiered compaction             | background merge, drop tombstones, crash-safe     |
| 6     | Parallelism + benchmarks           | rayon parallel scan, `cargo bench` harness        |
