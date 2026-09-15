//! In-memory write buffer.
//!
//! The memtable is an ordered map from key to the most recent [`Value`] written for it.
//! Deletes are stored as tombstones so they can shadow older versions on disk once the
//! memtable is flushed. Ordering lets a flush emit an SSTable with a single sequential
//! pass.

use std::collections::BTreeMap;
use std::ops::Bound;

use crate::types::{Entry, Value};

/// Fixed per-entry overhead used in the size estimate (tree node + allocation headers).
const ENTRY_OVERHEAD: usize = 32;

/// An ordered in-memory table of the most recent writes.
#[derive(Default, Debug)]
pub struct Memtable {
    map: BTreeMap<Vec<u8>, Value>,
    approx_bytes: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts or overwrites `key`.
    pub fn insert(&mut self, key: Vec<u8>, value: Value) {
        let key_len = key.len();
        let value_len = value.payload().len();
        match self.map.insert(key, value) {
            Some(old) => {
                self.approx_bytes = self.approx_bytes - old.payload().len() + value_len;
            }
            None => self.approx_bytes += key_len + value_len + ENTRY_OVERHEAD,
        }
    }

    /// Returns the latest value for `key`, which may be a tombstone.
    pub fn get(&self, key: &[u8]) -> Option<&Value> {
        self.map.get(key)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Approximate heap footprint in bytes; compared against `Options::memtable_size`.
    pub fn approximate_size(&self) -> usize {
        self.approx_bytes
    }

    /// Iterates all entries in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Value)> {
        self.map.iter()
    }

    /// Copies out the entries within `(start, end)`. The caller must ensure the range is
    /// non-empty (start <= end), as `BTreeMap::range` panics otherwise.
    pub fn range_entries(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Vec<Entry> {
        self.map
            .range::<[u8], _>((start, end))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get_and_overwrite() {
        let mut m = Memtable::new();
        m.insert(b"b".to_vec(), Value::Put(b"1".to_vec()));
        m.insert(b"a".to_vec(), Value::Put(b"2".to_vec()));
        m.insert(b"b".to_vec(), Value::Delete);
        assert_eq!(m.len(), 2);
        assert_eq!(m.get(b"a"), Some(&Value::Put(b"2".to_vec())));
        assert_eq!(m.get(b"b"), Some(&Value::Delete));
        assert_eq!(m.get(b"c"), None);
        let keys: Vec<_> = m.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn size_tracks_overwrites() {
        let mut m = Memtable::new();
        m.insert(b"key".to_vec(), Value::Put(vec![0; 100]));
        let first = m.approximate_size();
        assert_eq!(first, 3 + 100 + ENTRY_OVERHEAD);
        m.insert(b"key".to_vec(), Value::Put(vec![0; 10]));
        assert_eq!(m.approximate_size(), first - 90);
        m.insert(b"key".to_vec(), Value::Delete);
        assert_eq!(m.approximate_size(), 3 + ENTRY_OVERHEAD);
    }

    #[test]
    fn range_respects_bounds() {
        let mut m = Memtable::new();
        for k in [b"a", b"b", b"c", b"d"] {
            m.insert(k.to_vec(), Value::Put(k.to_vec()));
        }
        let keys = |v: Vec<Entry>| v.into_iter().map(|(k, _)| k).collect::<Vec<_>>();
        assert_eq!(
            keys(m.range_entries(Bound::Included(b"b"), Bound::Excluded(b"d"))),
            vec![b"b".to_vec(), b"c".to_vec()]
        );
        assert_eq!(
            keys(m.range_entries(Bound::Excluded(b"b"), Bound::Unbounded)),
            vec![b"c".to_vec(), b"d".to_vec()]
        );
    }
}
