//! Write-ahead log: an append-only file of mutations, replayed on startup.
//!
//! Durability rule: a mutation is written here *and flushed to disk* before it
//! is applied in memory. If the process dies, replaying the log rebuilds the
//! exact in-memory state — nothing acknowledged is lost.
//!
//! On-disk format is a flat sequence of records, each:
//!
//! ```text
//! [ u32 length (little-endian) ][ length bytes of bincode(Entry) ]
//! ```
//!
//! A crash can leave a half-written record at the tail. Replay detects the
//! short read and stops there, discarding only the torn record (which was never
//! acknowledged to the caller).

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::Path;

use crate::api::Entry;

/// An open WAL, positioned at the end of the file for appending.
pub struct Wal {
    writer: BufWriter<File>,
}

impl Wal {
    /// Opens the log at `path`, creating it if absent. New writes append.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Wal> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Wal {
            writer: BufWriter::new(file),
        })
    }

    /// Durably appends one entry. Returns only after the bytes have been
    /// `fsync`ed, so a crash immediately after this call still recovers the
    /// entry. (fsync-per-write is the correct-but-slow baseline; Phase 6
    /// revisits batching.)
    pub fn append(&mut self, entry: &Entry) -> io::Result<()> {
        let bytes =
            bincode::serialize(entry).map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
        let len = u32::try_from(bytes.len())
            .map_err(|_| io::Error::new(ErrorKind::InvalidData, "record too large"))?;

        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&bytes)?;
        self.writer.flush()?; // BufWriter -> OS
        self.writer.get_ref().sync_all()?; // OS -> disk
        Ok(())
    }

    /// Empties the log. Called after a flush has made the logged mutations
    /// durable in an SSTable, so replaying them again would be wasted work. The
    /// open file handle keeps appending — at offset 0 now.
    pub fn truncate(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        let file = self.writer.get_ref();
        file.set_len(0)?;
        file.sync_all()?;
        Ok(())
    }

    /// Reads every entry in the log at `path`, in write order. A missing file
    /// is an empty log (first start). A torn trailing record is ignored.
    pub fn replay(path: impl AsRef<Path>) -> io::Result<Vec<Entry>> {
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                // Clean end of log, or a crash between records.
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            match reader.read_exact(&mut buf) {
                Ok(()) => {}
                // Crash mid-record: the length promised more bytes than exist.
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let entry = bincode::deserialize(&buf)
                .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
            entries.push(entry);
        }

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::Entry;
    use std::path::PathBuf;

    /// Unique temp path; avoids a `tempfile` dependency for a couple of tests.
    fn temp_path(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!(
            "strata-wal-{}-{}-{}.log",
            tag,
            std::process::id(),
            nanos
        ));
        p
    }

    #[test]
    fn append_then_replay_roundtrips_in_order() {
        let path = temp_path("roundtrip");
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&Entry::put(b"a".to_vec(), b"1".to_vec(), 1))
                .unwrap();
            wal.append(&Entry::delete(b"a".to_vec(), 2)).unwrap();
            wal.append(&Entry::put(b"b".to_vec(), b"2".to_vec(), 3))
                .unwrap();
        }

        let entries = Wal::replay(&path).unwrap();
        assert_eq!(
            entries,
            vec![
                Entry::put(b"a".to_vec(), b"1".to_vec(), 1),
                Entry::delete(b"a".to_vec(), 2),
                Entry::put(b"b".to_vec(), b"2".to_vec(), 3),
            ]
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn truncate_empties_the_log_then_appends_continue() {
        let path = temp_path("truncate");
        let mut wal = Wal::open(&path).unwrap();
        wal.append(&Entry::put(b"a".to_vec(), b"1".to_vec(), 1))
            .unwrap();
        wal.truncate().unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);

        // The handle keeps working after truncation.
        wal.append(&Entry::put(b"b".to_vec(), b"2".to_vec(), 2))
            .unwrap();
        let entries = Wal::replay(&path).unwrap();
        assert_eq!(entries, vec![Entry::put(b"b".to_vec(), b"2".to_vec(), 2)]);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn replay_missing_file_is_empty() {
        let path = temp_path("missing");
        assert!(Wal::replay(&path).unwrap().is_empty());
    }

    #[test]
    fn torn_tail_record_is_discarded() {
        let path = temp_path("torn");
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&Entry::put(b"a".to_vec(), b"1".to_vec(), 1))
                .unwrap();
            wal.append(&Entry::put(b"b".to_vec(), b"2".to_vec(), 2))
                .unwrap();
        }
        // Simulate a crash mid-append by lopping bytes off the end.
        let len = std::fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len - 3).unwrap();

        // The first (intact) record survives; the torn second one is dropped.
        let entries = Wal::replay(&path).unwrap();
        assert_eq!(entries, vec![Entry::put(b"a".to_vec(), b"1".to_vec(), 1)]);
        std::fs::remove_file(&path).ok();
    }
}
