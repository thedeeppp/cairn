//! SSTable — a Sorted String Table: an immutable, key-ordered file with a sparse
//! index for point lookups without scanning the whole file.
//!
//! Layout:
//!
//! ```text
//! [ data:   record* ]                      each: [u32 len][bincode(Entry)], keys ascending
//! [ index:  bincode(Vec<(Key, u64 offset)>) ]   one entry per Nth data record
//! [ bloom:  bincode(Bloom) ]                     membership filter over all keys
//! [ footer: u64 index_offset | u64 index_len | u64 bloom_offset | u64 bloom_len
//!           | u64 max_seq | u64 magic ]
//! ```
//!
//! Lookup: a Bloom filter check first rejects keys that are definitely absent
//! with no disk read; otherwise binary-search the (small, in-memory) index for
//! the largest indexed key ≤ target to get a starting offset, then scan forward
//! at most `INDEX_INTERVAL` records — keys are sorted, so we stop as soon as we
//! meet or pass the target.
//!
//! Writes are crash-safe: a temp file is fsynced and atomically renamed into
//! place, so a reader never sees a half-written SSTable.

use std::cmp::Ordering;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::api::{Entry, Key};
use crate::bloom::Bloom;

/// One index entry every this many data records. Smaller = bigger index but
/// shorter scans; 16 is a reasonable middle for now (tunable later).
const INDEX_INTERVAL: usize = 16;

/// Target false-positive rate for each table's Bloom filter.
const BLOOM_FP_RATE: f64 = 0.01;

/// Fixed-size footer: index_offset, index_len, bloom_offset, bloom_len,
/// max_seq, magic — six u64s.
const FOOTER_SIZE: u64 = 48;

/// Identifies the file format (and version) in the footer.
const MAGIC: u64 = 0x5354_5241_5441_0004;

fn invalid(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, e.to_string())
}

/// An open SSTable: a path plus its loaded sparse index and Bloom filter.
pub struct SsTable {
    path: PathBuf,
    /// `(key, byte offset)` for every `INDEX_INTERVAL`-th record. Ascending.
    index: Vec<(Key, u64)>,
    /// Membership filter over every key in the table — checked before any read.
    bloom: Bloom,
    /// Byte offset where the data section ends (and the index begins).
    data_end: u64,
    /// Largest sequence number stored, for cheap seq recovery without scanning.
    max_seq: u64,
}

impl SsTable {
    /// Writes `entries` (which **must** be in ascending key order) to a new
    /// SSTable at `path`, durably, and returns it opened (index loaded).
    pub fn create(
        path: impl AsRef<Path>,
        entries: impl IntoIterator<Item = Entry>,
    ) -> io::Result<SsTable> {
        let path = path.as_ref().to_path_buf();
        let mut tmp = path.clone();
        tmp.set_extension("sst.tmp");

        // Buffer the entries so we know the key count up front (to size the
        // Bloom filter). The set is bounded by the MemTable flush threshold.
        let entries: Vec<Entry> = entries.into_iter().collect();

        {
            let file = File::create(&tmp)?;
            let mut writer = BufWriter::new(file);
            let mut index: Vec<(Key, u64)> = Vec::new();
            let key_refs: Vec<&[u8]> = entries.iter().map(|e| e.key.as_slice()).collect();
            let bloom = Bloom::build(&key_refs, BLOOM_FP_RATE);
            let mut offset: u64 = 0;
            let mut max_seq: u64 = 0;

            for (i, entry) in entries.iter().enumerate() {
                if i % INDEX_INTERVAL == 0 {
                    index.push((entry.key.clone(), offset));
                }
                max_seq = max_seq.max(entry.seq);

                let bytes = bincode::serialize(entry).map_err(invalid)?;
                let len = u32::try_from(bytes.len()).map_err(invalid)?;
                writer.write_all(&len.to_le_bytes())?;
                writer.write_all(&bytes)?;
                offset += 4 + bytes.len() as u64;
            }

            let index_offset = offset;
            let index_bytes = bincode::serialize(&index).map_err(invalid)?;
            let index_len = index_bytes.len() as u64;
            writer.write_all(&index_bytes)?;

            let bloom_offset = index_offset + index_len;
            let bloom_bytes = bincode::serialize(&bloom).map_err(invalid)?;
            let bloom_len = bloom_bytes.len() as u64;
            writer.write_all(&bloom_bytes)?;

            writer.write_all(&index_offset.to_le_bytes())?;
            writer.write_all(&index_len.to_le_bytes())?;
            writer.write_all(&bloom_offset.to_le_bytes())?;
            writer.write_all(&bloom_len.to_le_bytes())?;
            writer.write_all(&max_seq.to_le_bytes())?;
            writer.write_all(&MAGIC.to_le_bytes())?;

            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        fs::rename(&tmp, &path)?;
        fsync_dir(&path)?;
        SsTable::open(path)
    }

    /// Opens an existing SSTable, reading its footer and loading its index.
    pub fn open(path: impl Into<PathBuf>) -> io::Result<SsTable> {
        let path = path.into();
        let mut file = File::open(&path)?;

        let file_len = file.metadata()?.len();
        if file_len < FOOTER_SIZE {
            return Err(invalid("sstable smaller than its footer"));
        }

        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut footer = [0u8; FOOTER_SIZE as usize];
        file.read_exact(&mut footer)?;
        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let index_len = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let bloom_offset = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        let bloom_len = u64::from_le_bytes(footer[24..32].try_into().unwrap());
        let max_seq = u64::from_le_bytes(footer[32..40].try_into().unwrap());
        let magic = u64::from_le_bytes(footer[40..48].try_into().unwrap());
        if magic != MAGIC {
            return Err(invalid("bad sstable magic"));
        }

        file.seek(SeekFrom::Start(index_offset))?;
        let mut index_bytes = vec![0u8; index_len as usize];
        file.read_exact(&mut index_bytes)?;
        let index = bincode::deserialize(&index_bytes).map_err(invalid)?;

        file.seek(SeekFrom::Start(bloom_offset))?;
        let mut bloom_bytes = vec![0u8; bloom_len as usize];
        file.read_exact(&mut bloom_bytes)?;
        let bloom = bincode::deserialize(&bloom_bytes).map_err(invalid)?;

        Ok(SsTable {
            path,
            index,
            bloom,
            data_end: index_offset,
            max_seq,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Largest sequence number stored in this table.
    pub fn max_seq(&self) -> u64 {
        self.max_seq
    }

    /// `false` means `key` is definitely not in this table (no disk read needed).
    /// `true` means it might be — the caller should `get` to be sure.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.bloom.contains(key)
    }

    /// Point lookup. Returns the stored entry (tombstone or value) for `key`, or
    /// `None` if this table doesn't contain it. Reads at most one block, and
    /// none at all when the Bloom filter rejects the key.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Entry>> {
        if !self.bloom.contains(key) {
            return Ok(None);
        }
        let start = match self.index.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
            Ok(i) => self.index[i].1,      // target is an indexed key
            Err(0) => return Ok(None),     // target precedes the first key
            Err(i) => self.index[i - 1].1, // floor: start of the block that may hold it
        };

        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(start))?;
        let mut pos = start;

        while pos < self.data_end {
            let entry = read_record(&mut reader)?;
            pos += record_len(&entry)?;
            match entry.key.as_slice().cmp(key) {
                Ordering::Equal => return Ok(Some(entry)),
                Ordering::Greater => return Ok(None), // passed it; keys ascending
                Ordering::Less => {}
            }
        }
        Ok(None)
    }

    /// Reads every entry in the data section, in key order. Used by recovery and
    /// (later) compaction.
    pub fn scan(&self) -> io::Result<Vec<Entry>> {
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut pos = 0u64;
        let mut entries = Vec::new();
        while pos < self.data_end {
            let entry = read_record(&mut reader)?;
            pos += record_len(&entry)?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

/// Reads one `[u32 len][bincode(Entry)]` record from the current position.
fn read_record(reader: &mut impl Read) -> io::Result<Entry> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    bincode::deserialize(&buf).map_err(invalid)
}

/// On-disk byte length of an entry's record (length prefix + payload).
fn record_len(entry: &Entry) -> io::Result<u64> {
    let len = bincode::serialized_size(entry).map_err(invalid)?;
    Ok(4 + len)
}

/// fsyncs the directory containing `path`, so a rename into it survives a crash.
fn fsync_dir(path: &Path) -> io::Result<()> {
    if let Some(dir) = path.parent() {
        let dir = if dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir
        };
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_path(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!(
            "strata-sst-{}-{}-{}.sst",
            tag,
            std::process::id(),
            nanos
        ));
        p
    }

    fn put(key: &str, val: &str, seq: u64) -> Entry {
        Entry::put(key.as_bytes().to_vec(), val.as_bytes().to_vec(), seq)
    }

    #[test]
    fn create_then_scan_roundtrips_in_order() {
        let path = temp_path("roundtrip");
        let entries = vec![put("a", "1", 1), put("b", "2", 2), put("c", "3", 3)];
        SsTable::create(&path, entries.clone()).unwrap();

        let read = SsTable::open(&path).unwrap().scan().unwrap();
        assert_eq!(read, entries);
        fs::remove_file(&path).ok();
    }

    #[test]
    fn point_lookup_across_many_blocks() {
        let path = temp_path("lookup");
        // 500 keys k000..k499 -> spans ~31 index blocks (INTERVAL=16).
        let entries: Vec<_> = (0..500)
            .map(|i| put(&format!("k{i:03}"), &format!("v{i}"), i as u64 + 1))
            .collect();
        let sst = SsTable::create(&path, entries).unwrap();

        // Hits at block boundaries and mid-block.
        assert_eq!(sst.get(b"k000").unwrap().unwrap().value, b"v0");
        assert_eq!(sst.get(b"k016").unwrap().unwrap().value, b"v16");
        assert_eq!(sst.get(b"k255").unwrap().unwrap().value, b"v255");
        assert_eq!(sst.get(b"k499").unwrap().unwrap().value, b"v499");

        // Misses: before first, after last, and a gap.
        assert!(sst.get(b"k500").unwrap().is_none());
        assert!(sst.get(b"aaa").unwrap().is_none()); // precedes everything
        assert!(sst.get(b"k0005").unwrap().is_none()); // between k000 and k001
        fs::remove_file(&path).ok();
    }

    #[test]
    fn tombstones_are_retrievable() {
        let path = temp_path("tomb");
        let entries = vec![put("a", "1", 1), Entry::delete(b"b".to_vec(), 2)];
        let sst = SsTable::create(&path, entries).unwrap();
        assert!(!sst.get(b"a").unwrap().unwrap().is_tombstone());
        assert!(sst.get(b"b").unwrap().unwrap().is_tombstone());
        fs::remove_file(&path).ok();
    }

    #[test]
    fn footer_carries_max_seq() {
        let path = temp_path("seq");
        let entries = vec![put("a", "1", 7), put("b", "2", 42), put("c", "3", 13)];
        let sst = SsTable::create(&path, entries).unwrap();
        assert_eq!(sst.max_seq(), 42);
        // Survives a fresh open (read from footer, not recomputed).
        assert_eq!(SsTable::open(&path).unwrap().max_seq(), 42);
        fs::remove_file(&path).ok();
    }

    #[test]
    fn no_temp_file_left_behind() {
        let path = temp_path("clean");
        SsTable::create(&path, vec![put("a", "1", 1)]).unwrap();
        let mut tmp = path.clone();
        tmp.set_extension("sst.tmp");
        assert!(!tmp.exists());
        fs::remove_file(&path).ok();
    }
}
