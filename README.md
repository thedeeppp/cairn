# Strata

A persistent, write-optimized key-value store built on a Log-Structured Merge
(LSM) tree, in Rust. Built strictly phase by phase — see `CLAUDE.md`.

## Changelog

- **Phase 0 — Bootstrap:** data model (`Entry`, `EntryKind`, `Request`/`Response`)
  and a `HashMap`-backed in-memory `Store` (get/set/delete), where `delete`
  simply removes the key. Tombstones/sequence numbers arrive in later phases
  where they're load-bearing (WAL, SSTables).
- **Phase 1 — Write-ahead log + recovery:** durable `Engine` that fsyncs every
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
