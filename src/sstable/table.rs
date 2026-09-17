//! SSTable reader.

use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::Arc;

use super::block::{Block, BlockIter};
use super::bloom::BloomFilter;
use super::{BlockHandle, BLOCK_TRAILER_LEN, FOOTER_LEN, FORMAT_VERSION, TABLE_MAGIC};
use crate::coding::{read_u32_le, read_u64_le};
use crate::error::{Error, Result};
use crate::types::{Entry, Value};

/// An open SSTable. The index block and bloom filter are held in memory; data blocks are
/// read on demand with positional reads, so a `Table` can be shared across threads.
pub struct Table {
    file: File,
    index: Arc<Block>,
    filter: BloomFilter,
    file_size: u64,
}

impl Table {
    pub fn open(path: &Path) -> Result<Table> {
        let file = File::open(path)?;
        let file_size = file.metadata()?.len();
        if file_size < FOOTER_LEN as u64 {
            return Err(Error::corruption(format!(
                "{}: file too short to be an sstable",
                path.display()
            )));
        }
        let mut footer = [0u8; FOOTER_LEN];
        read_exact_at(&file, &mut footer, file_size - FOOTER_LEN as u64)?;
        if &footer[48..56] != TABLE_MAGIC {
            return Err(Error::corruption(format!("{}: bad magic", path.display())));
        }
        if crc32fast::hash(&footer[..44]) != read_u32_le(&footer, 44) {
            return Err(Error::corruption(format!(
                "{}: footer checksum mismatch",
                path.display()
            )));
        }
        let version = read_u32_le(&footer, 40);
        if version != FORMAT_VERSION {
            return Err(Error::corruption(format!(
                "{}: unsupported format version {version}",
                path.display()
            )));
        }
        let index_handle = BlockHandle {
            offset: read_u64_le(&footer, 0),
            len: read_u64_le(&footer, 8),
        };
        let filter_handle = BlockHandle {
            offset: read_u64_le(&footer, 16),
            len: read_u64_le(&footer, 24),
        };

        let index = Arc::new(Block::new(read_block(&file, file_size, index_handle)?)?);
        let filter = BloomFilter::new(read_block(&file, file_size, filter_handle)?);
        Ok(Table {
            file,
            index,
            filter,
            file_size,
        })
    }

    #[cfg(test)]
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    fn read_data_block(&self, handle_bytes: &Value) -> Result<Arc<Block>> {
        let Value::Put(bytes) = handle_bytes else {
            return Err(Error::corruption("index entry is a tombstone"));
        };
        let handle = BlockHandle::decode(bytes)?;
        Ok(Arc::new(Block::new(read_block(
            &self.file,
            self.file_size,
            handle,
        )?)?))
    }

    /// Point lookup. Returns the stored value (possibly a tombstone) or `None` if this
    /// table has no entry for `key`.
    pub fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        if !self.filter.may_contain(key) {
            return Ok(None);
        }
        let mut index = BlockIter::new(self.index.clone());
        index.seek(key)?;
        let Some((_, handle)) = index.next_entry()? else {
            return Ok(None); // key is past the last key in the table
        };
        let mut it = BlockIter::new(self.read_data_block(&handle)?);
        it.seek(key)?;
        match it.next_entry()? {
            Some((k, v)) if k == key => Ok(Some(v)),
            _ => Ok(None),
        }
    }

    /// Returns `false` if the bloom filter proves `key` is absent.
    #[cfg(test)]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.filter.may_contain(key)
    }

    /// Iterates entries with key >= `start` (or all entries if `start` is `None`).
    pub fn iter(self: &Arc<Self>, start: Option<Vec<u8>>) -> TableIter {
        TableIter {
            index: BlockIter::new(self.index.clone()),
            table: self.clone(),
            data: None,
            start,
            seeked_index: false,
            done: false,
        }
    }
}

/// Reads a block and verifies its CRC trailer.
fn read_block(file: &File, file_size: u64, handle: BlockHandle) -> Result<Vec<u8>> {
    let end = handle
        .offset
        .checked_add(handle.len)
        .and_then(|e| e.checked_add(BLOCK_TRAILER_LEN as u64))
        .filter(|&e| e <= file_size - FOOTER_LEN as u64)
        .ok_or_else(|| Error::corruption("block handle points outside the file"))?;
    let mut buf = vec![0u8; (end - handle.offset) as usize];
    read_exact_at(file, &mut buf, handle.offset)?;
    let crc_at = buf.len() - BLOCK_TRAILER_LEN;
    if crc32fast::hash(&buf[..crc_at]) != read_u32_le(&buf, crc_at) {
        return Err(Error::corruption(format!(
            "block checksum mismatch at offset {}",
            handle.offset
        )));
    }
    buf.truncate(crc_at);
    Ok(buf)
}

/// Iterator over a table's entries in key order. I/O errors are yielded once, after
/// which the iterator is exhausted.
pub struct TableIter {
    table: Arc<Table>,
    index: BlockIter,
    data: Option<BlockIter>,
    start: Option<Vec<u8>>,
    seeked_index: bool,
    done: bool,
}

impl TableIter {
    fn advance(&mut self) -> Result<Option<Entry>> {
        if !self.seeked_index {
            self.seeked_index = true;
            if let Some(start) = &self.start {
                self.index.seek(start)?;
            }
        }
        loop {
            if let Some(data) = &mut self.data {
                if let Some(entry) = data.next_entry()? {
                    return Ok(Some(entry));
                }
            }
            let Some((_, handle)) = self.index.next_entry()? else {
                return Ok(None);
            };
            let mut it = BlockIter::new(self.table.read_data_block(&handle)?);
            // Only the first block visited can contain keys below `start`.
            if let Some(start) = self.start.take() {
                it.seek(&start)?;
            }
            self.data = Some(it);
        }
    }
}

impl Iterator for TableIter {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.advance() {
            Ok(Some(e)) => Some(Ok(e)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, buf, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_read(buf, offset) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => {
                buf = &mut buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
