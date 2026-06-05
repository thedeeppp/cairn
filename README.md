# Strata

A persistent, write-optimized key-value store built on a Log-Structured Merge
(LSM) tree, in Rust. Built strictly phase by phase — see `CLAUDE.md`.

## Changelog

- **Phase 0 — Bootstrap:** data model (`Entry`, `EntryKind`, `Request`/`Response`)
  and a `HashMap`-backed in-memory `Store` (get/set/delete), where `delete`
  simply removes the key. Tombstones/sequence numbers arrive in later phases
  where they're load-bearing (WAL, SSTables).
