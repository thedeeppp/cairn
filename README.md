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
