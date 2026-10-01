//! Persistent record of which SSTables make up the current version.
//!
//! The manifest is rewritten in full on every version change and installed atomically:
//! write `MANIFEST.tmp`, fsync it, rename it over `MANIFEST`, then fsync the directory.
//! A crash at any point leaves either the old or the new manifest in place, never a mix.

use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use crate::coding::{put_bytes, put_varint, read_u32_le, Decoder};
use crate::error::{Error, Result};

const MAGIC: &[u8; 8] = b"EMBMANI1";
pub const MANIFEST_FILE: &str = "MANIFEST";
pub const MANIFEST_TMP_FILE: &str = "MANIFEST.tmp";

/// Metadata for one SSTable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileMeta {
    pub number: u64,
    pub file_size: u64,
    pub num_entries: u64,
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
}

/// The persisted state of the tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestState {
    /// Lower bound for the next file number to allocate.
    pub next_file: u64,
    /// Logs numbered `>= log_number` must be replayed on recovery.
    pub log_number: u64,
    /// Files per level. L0 is newest-first; deeper levels are sorted by smallest key.
    pub levels: Vec<Vec<FileMeta>>,
}

impl ManifestState {
    pub fn empty(num_levels: usize) -> Self {
        ManifestState {
            next_file: 1,
            log_number: 0,
            levels: vec![Vec::new(); num_levels],
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        put_varint(&mut body, self.next_file);
        put_varint(&mut body, self.log_number);
        put_varint(&mut body, self.levels.len() as u64);
        for level in &self.levels {
            put_varint(&mut body, level.len() as u64);
            for f in level {
                put_varint(&mut body, f.number);
                put_varint(&mut body, f.file_size);
                put_varint(&mut body, f.num_entries);
                put_bytes(&mut body, &f.smallest);
                put_bytes(&mut body, &f.largest);
            }
        }
        let mut out = Vec::with_capacity(16 + body.len());
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&(body.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
        out.extend_from_slice(&body);
        out
    }

    fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 16 || &data[..8] != MAGIC {
            return Err(Error::corruption("manifest: bad magic"));
        }
        let body_len = read_u32_le(data, 8) as usize;
        let crc = read_u32_le(data, 12);
        let body = data
            .get(16..16 + body_len)
            .filter(|_| data.len() == 16 + body_len)
            .ok_or_else(|| Error::corruption("manifest: length mismatch"))?;
        if crc32fast::hash(body) != crc {
            return Err(Error::corruption("manifest: checksum mismatch"));
        }

        let mut d = Decoder::new(body, "manifest");
        let next_file = d.varint()?;
        let log_number = d.varint()?;
        let num_levels = d.varint_usize()?;
        if num_levels > 64 {
            return Err(Error::corruption("manifest: implausible level count"));
        }
        let mut levels = Vec::with_capacity(num_levels);
        for _ in 0..num_levels {
            let n = d.varint_usize()?;
            let mut files = Vec::with_capacity(n.min(4096));
            for _ in 0..n {
                files.push(FileMeta {
                    number: d.varint()?,
                    file_size: d.varint()?,
                    num_entries: d.varint()?,
                    smallest: d.bytes()?.to_vec(),
                    largest: d.bytes()?.to_vec(),
                });
            }
            levels.push(files);
        }
        if !d.is_empty() {
            return Err(Error::corruption("manifest: trailing bytes"));
        }
        Ok(ManifestState {
            next_file,
            log_number,
            levels,
        })
    }
}

/// Atomically replaces the manifest in `dir` with `state`.
pub fn write_manifest(dir: &Path, state: &ManifestState) -> Result<()> {
    let tmp = dir.join(MANIFEST_TMP_FILE);
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&state.encode())?;
        f.sync_all()?;
    }
    fs::rename(&tmp, dir.join(MANIFEST_FILE))?;
    sync_dir(dir)
}

/// Reads the manifest in `dir`, or `None` if the database has never been initialised.
pub fn read_manifest(dir: &Path) -> Result<Option<ManifestState>> {
    match fs::read(dir.join(MANIFEST_FILE)) {
        Ok(data) => ManifestState::decode(&data).map(Some),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Makes directory entry changes (creates, renames) durable.
pub fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ManifestState {
        let mut s = ManifestState::empty(3);
        s.next_file = 42;
        s.log_number = 40;
        s.levels[0].push(FileMeta {
            number: 39,
            file_size: 1234,
            num_entries: 10,
            smallest: b"a".to_vec(),
            largest: b"m".to_vec(),
        });
        s.levels[2].push(FileMeta {
            number: 7,
            file_size: 99_999,
            num_entries: 5000,
            smallest: vec![],
            largest: vec![0xff; 20],
        });
        s
    }

    #[test]
    fn roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_manifest(dir.path()).unwrap(), None);
        write_manifest(dir.path(), &sample()).unwrap();
        assert_eq!(read_manifest(dir.path()).unwrap(), Some(sample()));
        assert!(!dir.path().join(MANIFEST_TMP_FILE).exists());

        let mut next = sample();
        let moved = next.levels[0].pop().unwrap();
        next.levels[1].push(moved);
        write_manifest(dir.path(), &next).unwrap();
        assert_eq!(read_manifest(dir.path()).unwrap(), Some(next));
    }

    #[test]
    fn leftover_tmp_does_not_affect_current_manifest() {
        let dir = tempfile::tempdir().unwrap();
        write_manifest(dir.path(), &sample()).unwrap();
        // Simulate a crash after writing MANIFEST.tmp but before the rename.
        fs::write(dir.path().join(MANIFEST_TMP_FILE), b"half-written").unwrap();
        assert_eq!(read_manifest(dir.path()).unwrap(), Some(sample()));
    }

    #[test]
    fn detects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        write_manifest(dir.path(), &sample()).unwrap();
        let path = dir.path().join(MANIFEST_FILE);
        let good = fs::read(&path).unwrap();
        for at in [0, 9, 13, 20, good.len() - 1] {
            let mut bad = good.clone();
            bad[at] ^= 0x10;
            fs::write(&path, &bad).unwrap();
            assert!(read_manifest(dir.path()).is_err(), "flip at {at}");
        }
        fs::write(&path, &good[..good.len() - 1]).unwrap();
        assert!(read_manifest(dir.path()).is_err());
    }
}
