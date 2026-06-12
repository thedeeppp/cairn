//! The durable store: a sorted MemTable fronted by a WAL, flushing to immutable
//! SSTables on disk.
//!
//! Write path: append to the WAL (durable), then apply to the active MemTable.
//! When the MemTable grows past a byte threshold it is *frozen* and flushed to a
//! new sorted SSTable; once that SSTable is durable, the WAL is truncated —
//! every mutation lives in exactly one of WAL-or-SSTable, never neither.
//!
//! Read path: the active MemTable, then SSTables newest → oldest, each found via
//! its in-memory sparse index + a single block scan. The first layer holding the
//! key wins — a tombstone there reads as absent and shadows older values.

use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, JoinHandle};

use crate::api::{Entry, Key, Value};
use crate::compaction;
use crate::memtable::MemTable;
use crate::sstable::SsTable;
use crate::store::Store;
use crate::wal::Wal;

/// Counters for how often the Bloom filter spared us an SSTable lookup. Atomic
/// so they update behind a shared `&self` read path.
#[derive(Default)]
struct BloomStats {
    /// Times an SSTable was considered for a `get`.
    probes: AtomicU64,
    /// Of those, times the Bloom filter rejected the key (no block read).
    skips: AtomicU64,
}

const WAL_FILE: &str = "wal.log";
const SST_EXT: &str = "sst";

/// Default MemTable flush threshold: 1 MiB.
pub const DEFAULT_MEMTABLE_BYTES: usize = 1 << 20;

/// Default number of SSTables that triggers a background compaction.
pub const DEFAULT_COMPACTION_TRIGGER: usize = 4;

pub struct Engine {
    dir: PathBuf,
    wal: Wal,
    /// Receives current writes.
    active: MemTable,
    /// On-disk SSTables (with loaded indexes), oldest first / newest last.
    sstables: Vec<Arc<SsTable>>,
    /// Monotonic write sequence; restored to its high-water mark on recovery.
    seq: u64,
    /// Highest SSTable number used; the next flush takes `next_sst + 1`.
    next_sst: u64,
    /// Flush when the active table reaches this many bytes.
    threshold: usize,
    /// Compact once this many SSTables pile up; `0` disables auto-compaction.
    compaction_threshold: usize,
    /// In-flight background compaction, if any.
    compaction: Option<CompactionJob>,
    /// Bloom-filter effectiveness counters.
    bloom_stats: BloomStats,
}

/// A compaction running on a background thread: the handle yielding the merged
/// table, plus the input tables it will replace once it finishes.
struct CompactionJob {
    handle: JoinHandle<io::Result<SsTable>>,
    inputs: Vec<Arc<SsTable>>,
}

impl Engine {
    /// Opens (or creates) a store at `dir` with default thresholds — including
    /// background auto-compaction.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Engine> {
        Engine::open_tuned(dir, DEFAULT_MEMTABLE_BYTES, DEFAULT_COMPACTION_TRIGGER)
    }

    /// Opens with an explicit flush threshold and **no** auto-compaction — handy
    /// for tests that want a predictable set of SSTables. (Compaction can still
    /// be forced with [`Engine::compact_now`].)
    pub fn open_with_threshold(dir: impl AsRef<Path>, threshold: usize) -> io::Result<Engine> {
        Engine::open_tuned(dir, threshold, 0)
    }

    /// Opens with explicit flush and compaction thresholds.
    pub fn open_with_thresholds(
        dir: impl AsRef<Path>,
        threshold: usize,
        compaction_threshold: usize,
    ) -> io::Result<Engine> {
        Engine::open_tuned(dir, threshold, compaction_threshold)
    }

    fn open_tuned(
        dir: impl AsRef<Path>,
        threshold: usize,
        compaction_threshold: usize,
    ) -> io::Result<Engine> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        remove_stale_temps(&dir)?;

        // Open every SSTable so we can read each footer's compaction flag.
        let mut loaded = Vec::new();
        let mut next_sst = 0;
        for (num, path) in sstable_files(&dir)? {
            next_sst = next_sst.max(num);
            loaded.push((num, SsTable::open(path)?));
        }

        // Superseding rule: the newest compaction output makes every
        // lower-numbered table obsolete (they were merged into it). A crash mid
        // cleanup can leave those inputs behind; delete them now so a dropped
        // tombstone can never resurrect a key.
        let cutoff = loaded
            .iter()
            .filter(|(_, sst)| sst.is_compaction())
            .map(|(num, _)| *num)
            .max();

        let mut seq = 0;
        let mut sstables = Vec::new();
        let mut deleted_any = false;
        for (num, sst) in loaded {
            if cutoff.is_some_and(|c| num < c) {
                fs::remove_file(sst.path()).ok();
                deleted_any = true;
                continue;
            }
            // The footer's max_seq restores the sequence counter without
            // scanning any data.
            seq = seq.max(sst.max_seq());
            sstables.push(Arc::new(sst));
        }
        if deleted_any {
            fsync_dir(&dir)?;
        }

        // Replay the WAL (everything written since the last flush) into active.
        let wal_path = dir.join(WAL_FILE);
        let mut active = MemTable::new();
        for entry in Wal::replay(&wal_path)? {
            seq = seq.max(entry.seq);
            active.put(entry);
        }

        let wal = Wal::open(&wal_path)?;
        Ok(Engine {
            dir,
            wal,
            active,
            sstables,
            seq,
            next_sst,
            threshold,
            compaction_threshold,
            compaction: None,
            bloom_stats: BloomStats::default(),
        })
    }

    /// Number of SSTables on disk.
    pub fn sstable_count(&self) -> usize {
        self.sstables.len()
    }

    /// Entry count in the active MemTable.
    pub fn active_len(&self) -> usize {
        self.active.len()
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Fraction of SSTable probes that the Bloom filter let us skip without a
    /// disk read, over the engine's lifetime. `0.0` if nothing has been probed.
    pub fn bloom_skip_rate(&self) -> f64 {
        let probes = self.bloom_stats.probes.load(Ordering::Relaxed);
        if probes == 0 {
            return 0.0;
        }
        self.bloom_stats.skips.load(Ordering::Relaxed) as f64 / probes as f64
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    /// Logs `entry` durably, applies it to the active table, then flushes if the
    /// table has grown past the threshold.
    fn write(&mut self, entry: Entry) -> io::Result<()> {
        self.wal.append(&entry)?;
        self.active.put(entry);
        if self.active.size_bytes() >= self.threshold {
            self.flush()?;
        }
        // Reap a finished background compaction, then maybe start a new one.
        self.poll_compaction()?;
        self.maybe_start_compaction();
        Ok(())
    }

    /// Freezes the active table, flushes it to a new SSTable, opens it for
    /// reading, and — once it is durable — truncates the WAL. The frozen table's
    /// memory is reclaimed; reads now come from the SSTable on disk.
    fn flush(&mut self) -> io::Result<()> {
        if self.active.is_empty() {
            return Ok(());
        }

        let frozen = std::mem::take(&mut self.active);

        self.next_sst += 1;
        let path = self.dir.join(format!("sst-{:06}.{SST_EXT}", self.next_sst));
        let sst = SsTable::create(&path, frozen.iter().cloned())?; // sorted: BTreeMap order

        // The data is now durable on disk, so the WAL no longer needs it.
        self.wal.truncate()?;

        self.sstables.push(Arc::new(sst));
        // `frozen` drops here, freeing its memory.
        Ok(())
    }

    /// Forces a full compaction (ignoring the count trigger) and waits for it.
    /// A no-op when fewer than two SSTables exist.
    pub fn compact_now(&mut self) -> io::Result<()> {
        self.start_compaction();
        self.wait_for_compaction()
    }

    /// Blocks until any in-flight compaction finishes and installs its result.
    pub fn wait_for_compaction(&mut self) -> io::Result<()> {
        if self.compaction.is_some() {
            self.install_compaction()?;
        }
        Ok(())
    }

    /// Starts a background compaction once the SSTable count crosses the trigger.
    fn maybe_start_compaction(&mut self) {
        if self.compaction_threshold == 0 || self.sstables.len() < self.compaction_threshold {
            return;
        }
        self.start_compaction();
    }

    /// Spawns a background thread that merges *all* current SSTables into one,
    /// keeping the newest value per key and dropping tombstones — safe because
    /// the merge includes the oldest table and the output is flagged superseding.
    fn start_compaction(&mut self) {
        if self.compaction.is_some() || self.sstables.len() < 2 {
            return;
        }
        let inputs: Vec<Arc<SsTable>> = self.sstables.clone();
        let merge_inputs = inputs.clone();
        self.next_sst += 1;
        let out_path = self.dir.join(format!("sst-{:06}.{SST_EXT}", self.next_sst));
        let handle = thread::spawn(move || {
            let entries = compaction::merge_tables(&merge_inputs, true)?;
            SsTable::create_compaction(&out_path, entries)
        });
        self.compaction = Some(CompactionJob { handle, inputs });
    }

    /// Installs a finished compaction if its thread has completed (non-blocking).
    fn poll_compaction(&mut self) -> io::Result<()> {
        if self
            .compaction
            .as_ref()
            .is_some_and(|job| job.handle.is_finished())
        {
            self.install_compaction()?;
        }
        Ok(())
    }

    /// Joins the compaction thread and swaps its merged table in for the inputs,
    /// then deletes the now-obsolete input files.
    fn install_compaction(&mut self) -> io::Result<()> {
        let Some(job) = self.compaction.take() else {
            return Ok(());
        };
        let merged = match job.handle.join() {
            Ok(result) => result?,
            Err(_) => return Err(io::Error::other("compaction thread panicked")),
        };

        // Drop the merged-away inputs; any tables flushed while the compaction
        // ran are newer and stay in place.
        let inputs = job.inputs;
        self.sstables
            .retain(|table| !inputs.iter().any(|input| Arc::ptr_eq(input, table)));
        // The merged table holds the oldest data, so it sorts before those
        // newer flushes.
        self.sstables.insert(0, Arc::new(merged));

        for input in &inputs {
            fs::remove_file(input.path()).ok();
        }
        fsync_dir(&self.dir)?;
        Ok(())
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        // Finish any background compaction so its files are left consistent.
        let _ = self.wait_for_compaction();
    }
}

/// Resolves a found entry to a read result: a tombstone reads as absent.
fn resolve(entry: &Entry) -> Option<Value> {
    if entry.is_tombstone() {
        None
    } else {
        Some(entry.value.clone())
    }
}

impl Store for Engine {
    fn get(&self, key: &[u8]) -> io::Result<Option<Value>> {
        // Newest layer wins; the first layer holding the key is authoritative,
        // even if that entry is a tombstone (which shadows older values).
        if let Some(entry) = self.active.get(key) {
            return Ok(resolve(entry));
        }
        for sst in self.sstables.iter().rev() {
            self.bloom_stats.probes.fetch_add(1, Ordering::Relaxed);
            if !sst.may_contain(key) {
                // Definitely absent: skip the index search and block read.
                self.bloom_stats.skips.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            if let Some(entry) = sst.get(key)? {
                return Ok(resolve(&entry));
            }
        }
        Ok(None)
    }

    fn set(&mut self, key: Key, value: Value) -> io::Result<()> {
        let seq = self.next_seq();
        self.write(Entry::put(key, value, seq))
    }

    fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        let seq = self.next_seq();
        self.write(Entry::delete(key.to_vec(), seq))
    }
}

/// Deletes leftover `*.sst.tmp` files from a crash during a previous flush.
fn remove_stale_temps(dir: &Path) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) == Some("tmp") {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

/// fsyncs `dir` so file creations and deletions within it survive a crash.
fn fsync_dir(dir: &Path) -> io::Result<()> {
    let dir = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    File::open(dir)?.sync_all()
}

/// Lists `sst-NNNNNN.sst` files in `dir`, parsed and sorted by number ascending.
fn sstable_files(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some(SST_EXT) {
            continue;
        }
        if let Some(num) = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.strip_prefix("sst-"))
            .and_then(|n| n.parse::<u64>().ok())
        {
            files.push((num, path));
        }
    }
    files.sort_by_key(|(num, _)| *num);
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_dir(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!(
            "strata-engine-{}-{}-{}",
            tag,
            std::process::id(),
            nanos
        ));
        p
    }

    fn k(s: &str) -> Key {
        s.as_bytes().to_vec()
    }

    /// Writes `n` keys with ~20-byte values; small threshold forces flushes.
    fn fill(db: &mut Engine, n: usize) {
        fill_range(db, 0, n);
    }

    /// Writes keys `key{lo..hi}` with ~20-byte values.
    fn fill_range(db: &mut Engine, lo: usize, hi: usize) {
        for i in lo..hi {
            db.set(format!("key{i:04}").into_bytes(), vec![b'x'; 20])
                .unwrap();
        }
    }

    #[test]
    fn flush_triggers_and_data_stays_readable() {
        let dir = temp_dir("flush");
        let mut db = Engine::open_with_threshold(&dir, 200).unwrap();
        fill(&mut db, 100);

        assert!(
            db.sstable_count() >= 1,
            "threshold should have forced a flush"
        );
        for i in 0..100 {
            assert_eq!(
                db.get(format!("key{i:04}").as_bytes()).unwrap(),
                Some(vec![b'x'; 20])
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn flushed_sstable_is_sorted() {
        let dir = temp_dir("sorted");
        let mut db = Engine::open_with_threshold(&dir, 200).unwrap();
        fill(&mut db, 100);

        let files = sstable_files(&dir).unwrap();
        assert!(!files.is_empty());
        for (_, path) in files {
            let entries = SsTable::open(path).unwrap().scan().unwrap();
            let keys: Vec<_> = entries.iter().map(|e| e.key.clone()).collect();
            let mut sorted = keys.clone();
            sorted.sort();
            assert_eq!(keys, sorted, "SSTable must be key-ordered");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn wal_is_truncated_after_flush() {
        let dir = temp_dir("truncate");
        let mut db = Engine::open_with_threshold(&dir, 200).unwrap();

        // Write until the first flush; the triggering write leaves active empty.
        let mut i = 0;
        while db.sstable_count() == 0 {
            db.set(format!("k{i:04}").into_bytes(), vec![b'x'; 20])
                .unwrap();
            i += 1;
        }
        assert_eq!(db.active_len(), 0);

        let wal_len = std::fs::metadata(dir.join(WAL_FILE)).unwrap().len();
        assert_eq!(wal_len, 0, "WAL should be empty right after a clean flush");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn delete_shadows_flushed_value() {
        let dir = temp_dir("shadow");
        let mut db = Engine::open_with_threshold(&dir, 200).unwrap();
        fill(&mut db, 100); // key0000 lands in an early flushed SSTable
        assert_eq!(db.get(b"key0000").unwrap(), Some(vec![b'x'; 20]));

        db.delete(b"key0000").unwrap(); // tombstone in a newer layer
        assert_eq!(db.get(b"key0000").unwrap(), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recovery_rebuilds_from_sstables_and_wal() {
        let dir = temp_dir("recovery");
        {
            let mut db = Engine::open_with_threshold(&dir, 200).unwrap();
            fill(&mut db, 100); // multiple flushes
            db.set(b"key0001".to_vec(), b"overwritten".to_vec())
                .unwrap();
            db.delete(b"key0050").unwrap();
        } // shutdown

        let db = Engine::open_with_threshold(&dir, 200).unwrap();
        assert_eq!(db.get(b"key0001").unwrap(), Some(b"overwritten".to_vec()));
        assert_eq!(db.get(b"key0050").unwrap(), None); // deletion survived
        assert_eq!(db.get(b"key0099").unwrap(), Some(vec![b'x'; 20]));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn bloom_filter_skips_absent_keys() {
        let dir = temp_dir("bloom");
        let mut db = Engine::open_with_threshold(&dir, 256).unwrap();
        fill(&mut db, 300); // many SSTables on disk

        assert!(db.sstable_count() > 3);

        // Look up keys that exist in no table at all.
        for i in 0..300 {
            assert_eq!(db.get(format!("absent{i:04}").as_bytes()).unwrap(), None);
        }

        // Almost every SSTable probe should have been skipped by the filter
        // (only the ~1% false-positive probes fall through to a real lookup).
        let rate = db.bloom_skip_rate();
        assert!(rate > 0.9, "bloom skip rate too low: {rate}");

        // And present keys are still always found (no false negatives).
        for i in 0..300 {
            assert_eq!(
                db.get(format!("key{i:04}").as_bytes()).unwrap(),
                Some(vec![b'x'; 20])
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn compaction_collapses_versions_and_drops_tombstones() {
        let dir = temp_dir("compact");
        // Auto-compaction off: we drive it explicitly for a deterministic result.
        let mut db = Engine::open_with_threshold(&dir, 200).unwrap();
        fill(&mut db, 80); // key0000..key0079 across many SSTables
        db.delete(b"key0000").unwrap(); // tombstone, newer seq
        db.set(b"key0001".to_vec(), b"v2".to_vec()).unwrap(); // overwrite
        fill_range(&mut db, 80, 160); // flush the tombstone + overwrite into SSTables

        assert!(db.sstable_count() > 1, "want several tables to merge");
        db.compact_now().unwrap();
        assert_eq!(db.sstable_count(), 1, "full compaction yields one table");

        // Observable correctness is preserved.
        assert_eq!(db.get(b"key0000").unwrap(), None); // deleted
        assert_eq!(db.get(b"key0001").unwrap(), Some(b"v2".to_vec())); // newest value
        assert_eq!(db.get(b"key0079").unwrap(), Some(vec![b'x'; 20]));

        // The merged table is physically tombstone-free, has dropped the deleted
        // key, and holds the overwritten key exactly once.
        let files = sstable_files(&dir).unwrap();
        assert_eq!(files.len(), 1);
        let entries = SsTable::open(&files[0].1).unwrap().scan().unwrap();
        assert!(entries.iter().all(|e| !e.is_tombstone()));
        assert!(entries.iter().all(|e| e.key != b"key0000"));
        let dups: Vec<_> = entries.iter().filter(|e| e.key == b"key0001").collect();
        assert_eq!(dups.len(), 1);
        assert_eq!(dups[0].value, b"v2");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recovery_deletes_superseded_inputs() {
        let dir = temp_dir("supersede");
        let mut db = Engine::open_with_threshold(&dir, 200).unwrap();
        fill(&mut db, 60);
        db.delete(b"key0000").unwrap();
        fill_range(&mut db, 60, 120); // flush the tombstone into an SSTable
        db.compact_now().unwrap(); // one superseding table; key0000 dropped
        assert_eq!(db.get(b"key0000").unwrap(), None);

        let merged = sstable_files(&dir).unwrap();
        assert_eq!(merged.len(), 1);
        assert!(merged[0].0 > 1, "merged table outnumbers any stale input");
        drop(db);

        // Simulate a crash mid-cleanup: a lower-numbered input lingers, still
        // holding key0000's value.
        let stale = dir.join("sst-000001.sst");
        SsTable::create(
            &stale,
            vec![Entry::put(b"key0000".to_vec(), b"zombie".to_vec(), 1)],
        )
        .unwrap();

        // Recovery deletes the superseded table — no resurrection.
        let db = Engine::open_with_threshold(&dir, 200).unwrap();
        assert_eq!(db.get(b"key0000").unwrap(), None);
        assert!(!stale.exists(), "superseded input must be deleted on open");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn auto_compaction_bounds_table_count() {
        let dir = temp_dir("auto");
        let mut db = Engine::open_with_thresholds(&dir, 200, 4).unwrap();
        fill(&mut db, 300);
        db.wait_for_compaction().unwrap();

        // The trigger keeps the table count from growing without bound.
        assert!(
            db.sstable_count() <= 8,
            "auto-compaction should bound table count, got {}",
            db.sstable_count()
        );
        // ...without losing data.
        for i in 0..300 {
            assert_eq!(
                db.get(format!("key{i:04}").as_bytes()).unwrap(),
                Some(vec![b'x'; 20])
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn seq_continues_after_recovery() {
        let dir = temp_dir("seq");
        {
            let mut db = Engine::open(&dir).unwrap();
            db.set(k("a"), k("1")).unwrap();
            db.set(k("b"), k("2")).unwrap();
        }
        let mut db = Engine::open(&dir).unwrap();
        assert_eq!(db.seq, 2);
        db.set(k("c"), k("3")).unwrap();
        assert_eq!(db.seq, 3);
        std::fs::remove_dir_all(&dir).ok();
    }
}
