//! Write-ahead log.
//!
//! Every mutation is appended to the active log before it is applied to the memtable, so
//! the memtable can be rebuilt after a crash. Each record is framed as
//!
//! ```text
//! +-----------+-----------+------------------+
//! | crc32 u32 | len u32   | payload[len]     |
//! +-----------+-----------+------------------+
//! payload = tag u8 | key_len varint | key | value_len varint | value
//! ```
//!
//! The CRC covers the length field and the payload. On replay, reading stops at the first
//! record that is truncated or fails its checksum: a crash in the middle of an append can
//! only damage the final record, so everything before it is intact.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

use crate::coding::{put_bytes, read_u32_le, Decoder};
use crate::error::{Error, Result};
use crate::types::Value;

const HEADER_LEN: usize = 8;

/// Appends records to a log file.
pub struct WalWriter {
    file: File,
    sync: bool,
    buf: Vec<u8>,
}

impl WalWriter {
    /// Creates (or truncates) the log at `path`.
    pub fn create(path: &Path, sync: bool) -> Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(WalWriter {
            file,
            sync,
            buf: Vec::with_capacity(256),
        })
    }

    /// Appends one record and returns the number of bytes written. The record is handed to
    /// the OS in a single `write` call; with `sync` enabled it is also fsynced.
    pub fn append(&mut self, key: &[u8], value: &Value) -> Result<usize> {
        if key.len() > u32::MAX as usize / 2 || value.payload().len() > u32::MAX as usize / 2 {
            return Err(Error::InvalidArgument("key or value too large".into()));
        }
        self.buf.clear();
        self.buf.extend_from_slice(&[0u8; HEADER_LEN]);
        self.buf.push(value.tag());
        put_bytes(&mut self.buf, key);
        put_bytes(&mut self.buf, value.payload());

        let len = (self.buf.len() - HEADER_LEN) as u32;
        self.buf[4..8].copy_from_slice(&len.to_le_bytes());
        let crc = crc32fast::hash(&self.buf[4..]);
        self.buf[0..4].copy_from_slice(&crc.to_le_bytes());

        self.file.write_all(&self.buf)?;
        if self.sync {
            self.file.sync_data()?;
        }
        Ok(self.buf.len())
    }

    /// Forces all appended records to stable storage.
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }
}

/// Outcome of replaying a log file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayStats {
    /// Number of intact records applied.
    pub records: usize,
    /// Length of the valid prefix of the file, in bytes.
    pub valid_bytes: u64,
    /// Total file length. Greater than `valid_bytes` if the tail was torn or corrupt.
    pub total_bytes: u64,
}

/// Replays the log at `path`, calling `apply` for each intact record in order.
///
/// A truncated or checksum-failing record ends the replay without error: it is the
/// expected signature of a crash during an append. A record whose checksum is valid but
/// whose payload cannot be decoded indicates a bug or deliberate tampering and is
/// reported as corruption.
pub fn replay(path: &Path, mut apply: impl FnMut(Vec<u8>, Value)) -> Result<ReplayStats> {
    let data = std::fs::read(path)?;
    let mut off = 0usize;
    let mut records = 0usize;

    while off + HEADER_LEN <= data.len() {
        let crc = read_u32_le(&data, off);
        let len = read_u32_le(&data, off + 4) as usize;
        let Some(end) = (off + HEADER_LEN).checked_add(len) else {
            break;
        };
        if end > data.len() {
            break; // torn write: header made it to disk but the payload did not
        }
        if crc32fast::hash(&data[off + 4..end]) != crc {
            break; // torn or corrupt record
        }
        let (key, value) = decode_payload(&data[off + HEADER_LEN..end])?;
        apply(key, value);
        records += 1;
        off = end;
    }

    Ok(ReplayStats {
        records,
        valid_bytes: off as u64,
        total_bytes: data.len() as u64,
    })
}

fn decode_payload(payload: &[u8]) -> Result<(Vec<u8>, Value)> {
    let mut d = Decoder::new(payload, "wal record");
    let tag = d.u8()?;
    let key = d.bytes()?.to_vec();
    let value = d.bytes()?;
    if !d.is_empty() {
        return Err(Error::corruption("wal record has trailing bytes"));
    }
    let value = match tag {
        Value::TAG_PUT => Value::Put(value.to_vec()),
        Value::TAG_DELETE if value.is_empty() => Value::Delete,
        _ => return Err(Error::corruption(format!("wal record has bad tag {tag}"))),
    };
    Ok((key, value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write_records(path: &Path, n: usize) -> Vec<usize> {
        let mut w = WalWriter::create(path, false).unwrap();
        (0..n)
            .map(|i| {
                let key = format!("key{i:04}");
                if i % 3 == 2 {
                    w.append(key.as_bytes(), &Value::Delete).unwrap()
                } else {
                    let value = vec![b'v'; i % 17];
                    w.append(key.as_bytes(), &Value::Put(value)).unwrap()
                }
            })
            .collect()
    }

    fn collect(path: &Path) -> (Vec<(Vec<u8>, Value)>, ReplayStats) {
        let mut out = Vec::new();
        let stats = replay(path, |k, v| out.push((k, v))).unwrap();
        (out, stats)
    }

    #[test]
    fn roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("000001.log");
        write_records(&path, 50);
        let (entries, stats) = collect(&path);
        assert_eq!(entries.len(), 50);
        assert_eq!(stats.valid_bytes, stats.total_bytes);
        assert_eq!(entries[0], (b"key0000".to_vec(), Value::Put(vec![])));
        assert_eq!(entries[2], (b"key0002".to_vec(), Value::Delete));
        assert_eq!(entries[4], (b"key0004".to_vec(), Value::Put(vec![b'v'; 4])));
    }

    #[test]
    fn truncation_at_every_offset_recovers_exact_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("000001.log");
        let sizes = write_records(&path, 20);
        let full = fs::read(&path).unwrap();
        let torn = dir.path().join("torn.log");

        for cut in 0..=full.len() {
            fs::write(&torn, &full[..cut]).unwrap();
            let (entries, stats) = collect(&torn);
            // Number of records that lie entirely within the first `cut` bytes.
            let mut expected = 0;
            let mut end = 0;
            for &s in &sizes {
                if end + s <= cut {
                    end += s;
                    expected += 1;
                } else {
                    break;
                }
            }
            assert_eq!(entries.len(), expected, "cut at {cut}");
            assert_eq!(stats.valid_bytes as usize, end, "cut at {cut}");
        }
    }

    #[test]
    fn bit_flip_stops_replay_at_damaged_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("000001.log");
        let sizes = write_records(&path, 10);
        let mut data = fs::read(&path).unwrap();
        // Corrupt a payload byte inside record 5.
        let start: usize = sizes[..5].iter().sum();
        data[start + HEADER_LEN + 2] ^= 0x40;
        fs::write(&path, &data).unwrap();
        let (entries, stats) = collect(&path);
        assert_eq!(entries.len(), 5);
        assert_eq!(stats.valid_bytes as usize, start);
    }

    #[test]
    fn zero_filled_tail_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("000001.log");
        write_records(&path, 3);
        let mut data = fs::read(&path).unwrap();
        data.extend_from_slice(&[0u8; 64]);
        fs::write(&path, &data).unwrap();
        let (entries, _) = collect(&path);
        assert_eq!(entries.len(), 3);
    }
}
