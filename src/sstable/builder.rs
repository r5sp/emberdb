//! Streaming SSTable writer.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use super::block::BlockBuilder;
use super::bloom::BloomBuilder;
use super::{BlockHandle, FORMAT_VERSION, TABLE_MAGIC};
use crate::error::{Error, Result};
use crate::options::Options;
use crate::types::Value;

/// Summary of a finished table, recorded in the MANIFEST.
#[derive(Clone, Debug)]
pub struct TableInfo {
    pub file_size: u64,
    pub num_entries: u64,
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
}

/// Writes an SSTable from entries supplied in strictly increasing key order.
pub struct TableBuilder {
    out: BufWriter<File>,
    offset: u64,
    block_size: usize,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    bloom: BloomBuilder,
    num_entries: u64,
    smallest: Option<Vec<u8>>,
}

impl TableBuilder {
    pub fn create(path: &Path, opts: &Options) -> Result<Self> {
        let file = File::create(path)?;
        Ok(TableBuilder {
            out: BufWriter::with_capacity(64 << 10, file),
            offset: 0,
            block_size: opts.block_size,
            data_block: BlockBuilder::new(opts.block_restart_interval),
            // Index entries are looked up by binary search on restart points; a restart at
            // every entry keeps each probe to a single decode.
            index_block: BlockBuilder::new(1),
            bloom: BloomBuilder::new(opts.bloom_bits_per_key),
            num_entries: 0,
            smallest: None,
        })
    }

    pub fn add(&mut self, key: &[u8], value: &Value) -> Result<()> {
        if self.num_entries > 0 && key <= self.data_block_last_key() {
            return Err(Error::InvalidArgument(
                "table keys must be added in strictly increasing order".into(),
            ));
        }
        if self.smallest.is_none() {
            self.smallest = Some(key.to_vec());
        }
        self.data_block.add(key, value);
        self.bloom.add_key(key);
        self.num_entries += 1;
        if self.data_block.estimated_size() >= self.block_size {
            self.flush_data_block()?;
        }
        Ok(())
    }

    fn data_block_last_key(&self) -> &[u8] {
        // After a block flush the data builder is empty; the index holds the last key.
        if self.data_block.is_empty() {
            self.index_block.last_key()
        } else {
            self.data_block.last_key()
        }
    }

    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }

    /// Approximate size of the file if it were finished now.
    pub fn estimated_size(&self) -> u64 {
        self.offset + self.data_block.estimated_size() as u64
    }

    fn write_block(&mut self, contents: &[u8]) -> Result<BlockHandle> {
        let handle = BlockHandle {
            offset: self.offset,
            len: contents.len() as u64,
        };
        self.out.write_all(contents)?;
        self.out
            .write_all(&crc32fast::hash(contents).to_le_bytes())?;
        self.offset += contents.len() as u64 + 4;
        Ok(handle)
    }

    fn flush_data_block(&mut self) -> Result<()> {
        if self.data_block.is_empty() {
            return Ok(());
        }
        let last_key = self.data_block.last_key().to_vec();
        let contents = self.data_block.finish();
        let handle = self.write_block(&contents)?;
        // The index maps each block's last key to its location: the first index entry
        // whose key is >= a lookup key identifies the only block that may contain it.
        self.index_block
            .add(&last_key, &Value::Put(handle.encode()));
        Ok(())
    }

    /// Writes the filter, index and footer and fsyncs the file.
    pub fn finish(mut self) -> Result<TableInfo> {
        if self.num_entries == 0 {
            return Err(Error::InvalidArgument(
                "cannot finish an empty table".into(),
            ));
        }
        self.flush_data_block()?;
        let largest = self.index_block.last_key().to_vec();

        let filter = self.bloom.finish();
        let filter_handle = self.write_block(&filter)?;
        let index = self.index_block.finish();
        let index_handle = self.write_block(&index)?;

        let mut footer = Vec::with_capacity(super::FOOTER_LEN);
        footer.extend_from_slice(&index_handle.offset.to_le_bytes());
        footer.extend_from_slice(&index_handle.len.to_le_bytes());
        footer.extend_from_slice(&filter_handle.offset.to_le_bytes());
        footer.extend_from_slice(&filter_handle.len.to_le_bytes());
        footer.extend_from_slice(&self.num_entries.to_le_bytes());
        footer.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        let crc = crc32fast::hash(&footer);
        footer.extend_from_slice(&crc.to_le_bytes());
        footer.extend_from_slice(TABLE_MAGIC);
        debug_assert_eq!(footer.len(), super::FOOTER_LEN);
        self.out.write_all(&footer)?;
        self.offset += footer.len() as u64;

        let file = self.out.into_inner().map_err(|e| e.into_error())?;
        file.sync_all()?;

        Ok(TableInfo {
            file_size: self.offset,
            num_entries: self.num_entries,
            smallest: self.smallest.expect("non-empty table has a smallest key"),
            largest,
        })
    }
}
