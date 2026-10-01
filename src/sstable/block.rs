//! Sorted, prefix-compressed blocks.
//!
//! A block stores a run of strictly increasing keys. Each key is encoded relative to its
//! predecessor (the shared prefix length plus the differing suffix). Every
//! `restart_interval` entries the full key is stored and its offset recorded as a
//! *restart point*, which lets a reader binary-search the restart array and then scan at
//! most `restart_interval` entries.
//!
//! ```text
//! entry   = shared varint | unshared varint | tag u8 | value_len varint
//!           | key_suffix[unshared] | value[value_len]
//! block   = entry* | restart_offset u32 * num_restarts | num_restarts u32
//! ```

use std::sync::Arc;

use crate::coding::{put_varint, read_u32_le, Decoder};
use crate::error::{Error, Result};
use crate::types::{Entry, Value};

/// Builds a single block.
pub struct BlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    restart_interval: usize,
    counter: usize,
    last_key: Vec<u8>,
    entries: usize,
}

impl BlockBuilder {
    pub fn new(restart_interval: usize) -> Self {
        BlockBuilder {
            buf: Vec::new(),
            restarts: vec![0],
            restart_interval: restart_interval.max(1),
            counter: 0,
            last_key: Vec::new(),
            entries: 0,
        }
    }

    /// Appends an entry. Keys must be added in strictly increasing order.
    pub fn add(&mut self, key: &[u8], value: &Value) {
        debug_assert!(
            self.entries == 0 || key > self.last_key.as_slice(),
            "keys must be strictly increasing"
        );
        let shared = if self.counter < self.restart_interval {
            common_prefix(&self.last_key, key)
        } else {
            self.restarts.push(self.buf.len() as u32);
            self.counter = 0;
            0
        };
        let payload = value.payload();
        put_varint(&mut self.buf, shared as u64);
        put_varint(&mut self.buf, (key.len() - shared) as u64);
        self.buf.push(value.tag());
        put_varint(&mut self.buf, payload.len() as u64);
        self.buf.extend_from_slice(&key[shared..]);
        self.buf.extend_from_slice(payload);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1;
        self.entries += 1;
    }

    pub fn is_empty(&self) -> bool {
        self.entries == 0
    }

    pub fn last_key(&self) -> &[u8] {
        &self.last_key
    }

    /// Size the block would have if finished now.
    pub fn estimated_size(&self) -> usize {
        self.buf.len() + self.restarts.len() * 4 + 4
    }

    /// Finishes the block and resets the builder for reuse.
    pub fn finish(&mut self) -> Vec<u8> {
        let mut out = std::mem::take(&mut self.buf);
        for r in &self.restarts {
            out.extend_from_slice(&r.to_le_bytes());
        }
        out.extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        self.restarts.clear();
        self.restarts.push(0);
        self.counter = 0;
        self.last_key.clear();
        self.entries = 0;
        out
    }
}

fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// A decoded, validated block.
pub struct Block {
    data: Vec<u8>,
    restarts_off: usize,
    num_restarts: usize,
}

impl Block {
    pub fn new(data: Vec<u8>) -> Result<Block> {
        if data.len() < 4 {
            return Err(Error::corruption("block too short"));
        }
        let num_restarts = read_u32_le(&data, data.len() - 4) as usize;
        let restarts_off = num_restarts
            .checked_mul(4)
            .and_then(|n| data.len().checked_sub(4 + n))
            .ok_or_else(|| Error::corruption("block restart array out of bounds"))?;
        if num_restarts == 0 {
            return Err(Error::corruption("block has no restart points"));
        }
        Ok(Block {
            data,
            restarts_off,
            num_restarts,
        })
    }

    fn restart_point(&self, i: usize) -> usize {
        read_u32_le(&self.data, self.restarts_off + 4 * i) as usize
    }

    /// Decodes the entry at `offset`, given the previous key, returning it and the offset
    /// of the following entry.
    fn decode_at(&self, offset: usize, prev_key: &[u8]) -> Result<(Entry, usize)> {
        if offset > self.restarts_off {
            return Err(Error::corruption("block entry offset out of bounds"));
        }
        let mut d = Decoder::new(&self.data[offset..self.restarts_off], "block entry");
        let shared = d.varint_usize()?;
        let unshared = d.varint_usize()?;
        let tag = d.u8()?;
        let value_len = d.varint_usize()?;
        if shared > prev_key.len() {
            return Err(Error::corruption("block entry shares more than previous key"));
        }
        let suffix = d.slice(unshared)?;
        let value = d.slice(value_len)?;
        let mut key = Vec::with_capacity(shared + unshared);
        key.extend_from_slice(&prev_key[..shared]);
        key.extend_from_slice(suffix);
        let value = match tag {
            Value::TAG_PUT => Value::Put(value.to_vec()),
            Value::TAG_DELETE if value.is_empty() => Value::Delete,
            _ => return Err(Error::corruption(format!("block entry has bad tag {tag}"))),
        };
        Ok(((key, value), offset + d.position()))
    }
}

/// Forward iterator over a block with support for seeking.
pub struct BlockIter {
    block: Arc<Block>,
    /// Offset of the next entry to decode.
    offset: usize,
    /// Key of the most recently decoded entry (the base for prefix decoding).
    last_key: Vec<u8>,
    /// An entry decoded during `seek` that has not been returned yet.
    pending: Option<Entry>,
}

impl BlockIter {
    pub fn new(block: Arc<Block>) -> Self {
        BlockIter {
            block,
            offset: 0,
            last_key: Vec::new(),
            pending: None,
        }
    }

    /// Positions the iterator so the next entry returned is the first with key >= `target`.
    pub fn seek(&mut self, target: &[u8]) -> Result<()> {
        // Binary search for the last restart point whose key is < target.
        let (mut left, mut right) = (0, self.block.num_restarts - 1);
        while left < right {
            let mid = (left + right).div_ceil(2);
            let ((key, _), _) = self.block.decode_at(self.block.restart_point(mid), &[])?;
            if key.as_slice() < target {
                left = mid;
            } else {
                right = mid - 1;
            }
        }
        self.offset = self.block.restart_point(left);
        self.last_key.clear();
        self.pending = None;
        // Linear scan within the restart interval.
        while let Some(entry) = self.next_entry()? {
            if entry.0.as_slice() >= target {
                self.pending = Some(entry);
                break;
            }
        }
        Ok(())
    }

    pub fn next_entry(&mut self) -> Result<Option<Entry>> {
        if let Some(e) = self.pending.take() {
            return Ok(Some(e));
        }
        if self.offset >= self.block.restarts_off {
            return Ok(None);
        }
        let ((key, value), next) = self.block.decode_at(self.offset, &self.last_key)?;
        self.offset = next;
        self.last_key.clone_from(&key);
        Ok(Some((key, value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(n: usize, interval: usize) -> (Arc<Block>, Vec<Entry>) {
        let mut b = BlockBuilder::new(interval);
        let entries: Vec<Entry> = (0..n)
            .map(|i| {
                let key = format!("user:{:05}", i * 2).into_bytes();
                let value = if i % 5 == 0 {
                    Value::Delete
                } else {
                    Value::Put(format!("value-{i}").into_bytes())
                };
                (key, value)
            })
            .collect();
        for (k, v) in &entries {
            b.add(k, v);
        }
        (Arc::new(Block::new(b.finish()).unwrap()), entries)
    }

    fn drain(it: &mut BlockIter) -> Vec<Entry> {
        let mut out = Vec::new();
        while let Some(e) = it.next_entry().unwrap() {
            out.push(e);
        }
        out
    }

    #[test]
    fn roundtrip_various_restart_intervals() {
        for interval in [1, 2, 16, 1000] {
            let (block, entries) = build(100, interval);
            assert_eq!(drain(&mut BlockIter::new(block)), entries);
        }
    }

    #[test]
    fn prefix_compression_shrinks_block() {
        let mut compressed = BlockBuilder::new(16);
        let mut uncompressed = BlockBuilder::new(1);
        for i in 0..100 {
            let k = format!("a-long-common-prefix/{i:04}").into_bytes();
            compressed.add(&k, &Value::Put(vec![]));
            uncompressed.add(&k, &Value::Put(vec![]));
        }
        assert!(compressed.finish().len() * 2 < uncompressed.finish().len());
    }

    #[test]
    fn seek_finds_exact_and_successor_keys() {
        for interval in [1, 3, 16] {
            let (block, entries) = build(64, interval);
            let mut it = BlockIter::new(block.clone());
            for (i, (k, _)) in entries.iter().enumerate() {
                // Exact match.
                it.seek(k).unwrap();
                assert_eq!(drain(&mut it), entries[i..].to_vec());
                // A key just after k (odd number) seeks to its successor.
                let mut after = k.clone();
                after.push(b'!');
                it.seek(&after).unwrap();
                assert_eq!(drain(&mut it), entries[i + 1..].to_vec());
            }
            it.seek(b"").unwrap();
            assert_eq!(drain(&mut it).len(), entries.len());
            it.seek(b"zzz").unwrap();
            assert!(drain(&mut it).is_empty());
        }
    }

    #[test]
    fn empty_block() {
        let mut b = BlockBuilder::new(16);
        assert!(b.is_empty());
        let block = Arc::new(Block::new(b.finish()).unwrap());
        let mut it = BlockIter::new(block);
        it.seek(b"a").unwrap();
        assert!(it.next_entry().unwrap().is_none());
    }

    #[test]
    fn rejects_malformed_blocks() {
        assert!(Block::new(vec![1, 2]).is_err());
        assert!(Block::new(vec![0, 0, 0, 0]).is_err()); // zero restarts
        assert!(Block::new(vec![9, 0, 0, 0]).is_err()); // restart array overflows
        // Valid framing, garbage entry: iteration must error, not panic.
        let mut data = vec![0xff, 0xff, 0xff];
        data.extend_from_slice(&0u32.to_le_bytes());
        data.extend_from_slice(&1u32.to_le_bytes());
        let mut it = BlockIter::new(Arc::new(Block::new(data).unwrap()));
        assert!(it.next_entry().is_err());
    }
}
