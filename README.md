# Cairn

A persistent, write-optimized key-value store built on a Log-Structured Merge
(LSM) tree, in Rust. Built strictly phase by phase — see `CLAUDE.md`.

Cairn stores opaque `bytes → bytes` pairs durably on disk. Every acknowledged
write survives a crash, reads see the most recent value (or `None` after a
delete), and the on-disk footprint is kept in check by background compaction.
It is a from-scratch, dependency-light implementation of the same storage engine
that powers LevelDB, RocksDB, Cassandra, and friends — small enough to read in
one sitting, but with the load-bearing parts (durability, crash recovery, Bloom
filters, compaction) actually implemented rather than hand-waved.

## What it does

```rust
use cairn::{Engine, Store};

let mut db = Engine::open("/var/lib/mydb")?;   // creates dir + recovers if present

db.set(b"user:42".to_vec(), b"alice".to_vec())?;  // durable before it returns
assert_eq!(db.get(b"user:42")?, Some(b"alice".to_vec()));

db.delete(b"user:42")?;                            // writes a tombstone
assert_eq!(db.get(b"user:42")?, None);
// `db` flushes and shuts down cleanly when dropped.
```

The public surface is the `Store` trait — `get`, `set`, `delete` (plus an
`execute(Request) -> Response` dispatch). Two backends implement it: `MemStore`
(an in-memory `HashMap`, the Phase 0 reference) and `Engine` (the durable
LSM-tree). All three operations return `io::Result` because the durable engine
touches disk on both the write and the read path.

## Why an LSM-tree?

A B-tree updates data *in place*, turning every write into a random disk seek. An
LSM-tree instead **only ever appends**: writes accumulate in memory and are
flushed to disk in large, sequential, immutable batches. This trades a little
read amplification (a key might live in any of several files) for dramatically
cheaper writes — the right call for write-heavy and ingest-heavy workloads.

Cairn buys back the read cost with three mechanisms: an in-memory sparse index
per file (no full scans), a per-file Bloom filter (skip files that can't hold the
key, with zero disk reads), and background compaction (merge many files back into
one, collapsing overwrites and deletions).

## Architecture

Data flows top-to-bottom; it is always newest at the top. A read walks down until
the first layer that holds the key, so a newer value (or a tombstone) naturally
shadows everything older.

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

### Components

| Module        | Role                                                                       |
|---------------|----------------------------------------------------------------------------|
| `api`         | Data model: `Key`/`Value` (opaque bytes), `Entry` (versioned record + `seq`), `EntryKind::{Put,Delete}`, and the `Request`/`Response` command surface. |
| `store`       | The `Store` trait and `MemStore`, the in-memory reference backend.         |
| `wal`         | Write-ahead log — append-only, fsync-per-write, replayed on startup.       |
| `memtable`    | The active write buffer: a sorted `BTreeMap` with a byte-size estimate.    |
| `sstable`     | Immutable Sorted String Table: data + sparse index + Bloom filter + footer.|
| `bloom`       | A Bloom filter (FNV-1a double hashing) persisted inside each SSTable.       |
| `compaction`  | The pure merge: scan inputs in parallel, keep newest `seq` per key, drop tombstones. |
| `engine`      | Ties it all together — write path, flush, read path, background compaction, recovery. |

## How a write becomes durable

1. **Log it.** The `Entry` is appended to the WAL and `fsync`ed *before* anything
   else. Once `set`/`delete` returns, the write has survived to stable storage.
2. **Apply it.** The entry goes into the active MemTable (a sorted `BTreeMap`),
   tagged with a monotonically increasing sequence number.
3. **Flush.** When the MemTable's estimated size crosses the threshold
   (`DEFAULT_MEMTABLE_BYTES`, 1 MiB), it freezes and is written out as a new,
   sorted, immutable SSTable via a temp file → fsync → atomic rename.
4. **Truncate.** Only after the SSTable is durable is the WAL truncated. This is
   the core invariant: **every acknowledged mutation lives in exactly one of the
   WAL or an SSTable — never neither, never lost.**

## How a read finds a value

`get` consults layers newest → oldest and returns at the first one that holds the
key:

1. The **active MemTable** (an in-memory lookup).
2. Each **SSTable**, newest to oldest. For each: check the **Bloom filter** first
   — if it says "absent", skip the file with no disk read; otherwise binary-search
   the in-memory **sparse index** for the block that could contain the key and
   scan forward at most `INDEX_INTERVAL` (16) records.

A `Delete` is a **tombstone**: the first layer that holds the key wins even if
that record is a tombstone, so a deletion correctly shadows older on-disk values
and reads as `None`.

## On-disk layout

**SSTable** (`sst-NNNNNN.sst`) — written once, never modified:

```text
[ data:   record* ]                        each: [u32 len][bincode(Entry)], keys ascending
[ index:  bincode(Vec<(Key, u64 offset)>) ]   one entry every 16th data record
[ bloom:  bincode(Bloom) ]                     membership filter over all keys
[ footer: index_offset | index_len | bloom_offset | bloom_len | max_seq | kind | magic ]
                                               (seven little-endian u64s = 56 bytes)
```

The `footer` is read first: it locates the index and Bloom sections, restores the
sequence counter from `max_seq` without scanning data, identifies the format via
`magic`, and uses `kind` to mark a *compaction output* (see below).

**WAL** (`wal.log`) — a flat append-only sequence:

```text
[ u32 length ][ length bytes of bincode(Entry) ]  ...repeated
```

A crash can leave a half-written record at the tail; replay detects the short read
and discards exactly that torn record (which was never acknowledged).

## Compaction

As flushes pile up, reads have more SSTables to walk and overwritten/deleted keys
waste space. Once the table count crosses `DEFAULT_COMPACTION_TRIGGER` (4), the
engine spawns a **background thread** that merges *all* current SSTables into one:

- Inputs are **scanned in parallel** with rayon — the per-table disk reads and
  bincode decoding are independent, and the merge is newest-`seq`-wins so order
  doesn't matter.
- For each key, only the highest-`seq` entry is kept; **tombstones are dropped
  entirely**, reclaiming the space of both the deletion and the value it hid.
- The result is written as a single SSTable flagged **superseding** (`kind = 1`)
  in its footer, then atomically swapped in for its inputs, which are deleted.

Compaction runs off the write path; new flushes can land while it works and are
simply kept (they're newer than everything being merged).

## Crash safety & recovery

`Engine::open` rebuilds exact state after any crash:

- **Torn temp files** (`*.sst.tmp`) from a flush interrupted mid-write are removed.
- **Superseded inputs** left behind by a crash *during* compaction cleanup are
  deleted: any table numbered below the newest *superseding* table was already
  merged into it, so keeping it could resurrect a dropped tombstone. Removing it
  is what makes tombstone-dropping safe.
- **SSTable footers** restore the sequence counter (no data scan needed).
- **The WAL is replayed** to recover every mutation written since the last flush.

Dropping an `Engine` flushes cleanly and joins any in-flight compaction, leaving
files consistent.

## Performance

Reads and the Bloom-gated absent path are CPU-bound and fast; writes are
deliberately `fsync`-per-mutation, so durable-write latency is dominated by disk
sync time. Run the hand-rolled benchmarks (no criterion — the dependency list is
kept tiny) with:

```sh
cargo bench
```

Representative figures from a single run (numbers are indicative, not rigorous;
write latency tracks your disk's fsync cost):

```text
benchmark                          ops         time     throughput      rate      latency
write (memtable)                 20000 ops   ~fsync-bound      ~240 ops/s            ~4 ms/op
read (present)                  100000 ops      0.40s     ~251k ops/s   24 MiB/s    ~4 µs/op
read (absent, bloom)            100000 ops      0.03s    ~3.4M ops/s                ~0.3 µs/op
compaction (rayon scan)          30000 ops      0.05s     ~603k ops/s   66 MiB/s    ~1.7 µs/op
```

The absent-key path is ~13× faster than a present lookup — the Bloom filter doing
its job, skipping tables with no disk read.

## Build & test

```sh
cargo build
cargo test                              # unit + per-phase integration tests
cargo clippy --all-targets -- -D warnings
cargo bench                             # throughput / latency report
cargo run                               # tiny smoke-test binary (write, reopen, read)
```

## Changelog

- **Phase 0 — Bootstrap:** data model (`Entry`, `EntryKind`, `Request`/`Response`)
  and a `HashMap`-backed in-memory `Store` (get/set/delete), where `delete`
  simply removes the key. Tombstones/sequence numbers arrive in later phases
  where they're load-bearing (WAL, SSTables).
- **Phase 1 — Write-ahead log + recovery:** durable `Engine` that fsyncs everyg
  mutation to a length-prefixed bincode WAL *before* applying it in memory, and
  replays the log on `open` to rebuild state. Recovery tolerates a torn tail
  record from a crash mid-write; deletes (tombstones) survive recovery.
- **Phase 2 — MemTable + flush to SSTable:** writes land in a sorted `BTreeMap`
  MemTable; past a byte threshold it freezes and flushes to an immutable, sorted
  SSTable (temp-file + atomic rename + fsync), then the WAL is truncated. `delete`
  now writes a tombstone so it can shadow older on-disk values. Reads check the
  active table then frozen tables newest→oldest.
- **Phase 3 — SSTable read path:** SSTables gain a sparse index (offset of every
  16th key) + a footer (index location, max-seq, magic). Reads `get` from disk by
  binary-searching the in-memory index and scanning a single block; flushed
  MemTables are dropped, reclaiming memory. `get` is now `io::Result` and the read
  order is active MemTable → SSTables newest→oldest, honoring tombstones. Recovery
  restores the sequence counter from the footer without rescanning data.
- **Phase 4 — Bloom filters:** each SSTable carries a persisted Bloom filter
  (FNV-1a double hashing, ~1% false-positive rate) checked before the index, so a
  Get for an absent key skips the table with no disk read — and never a false
  negative. The engine tracks the skip rate (`bloom_skip_rate()`).
- **Phase 5 — Size-tiered compaction:** once enough SSTables pile up, a
  background thread merges them all into one, keeping the newest value per key
  and dropping tombstones — collapsing overwrites and deletions to reclaim disk
  space. The merged table is flagged *superseding* in its footer, so recovery
  deletes any lower-numbered input a crash left behind, ensuring a dropped
  tombstone can never resurrect a deleted key.
- **Phase 6 — Parallelism + benchmarks:** compaction now scans its input
  SSTables in parallel with rayon (the per-table disk reads and bincode decoding
  are independent), then merges newest-seq-wins. A hand-rolled `cargo bench`
  target (`benches/engine_bench.rs`, no criterion) reports write, read
  (present/absent), and compaction throughput and latency.
