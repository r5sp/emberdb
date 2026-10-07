//! In-memory write buffer.
//!
//! The memtable is a skiplist from key to the most recent [`Value`] written for it.
//! Deletes are stored as tombstones so they can shadow older versions on disk once the
//! memtable is flushed. Ordering lets a flush emit an SSTable with a single sequential
//! pass.
//!
//! Nodes live in one `Vec` and link to each other by index, so the whole list is safe
//! Rust and frees in one deallocation when the memtable is dropped after a flush. Writes
//! already go through the `Db` write lock, so the list only needs a single writer.

use std::cmp::Ordering;
use std::ops::Bound;

use crate::types::{Entry, Value};

/// Fixed per-entry overhead used in the size estimate (node + link vector + headers).
const ENTRY_OVERHEAD: usize = 32;

/// Tallest tower a node can get. With p = 1/4 this stays balanced well past 4^12 keys.
const MAX_HEIGHT: usize = 12;

/// Index of the sentinel head node, which has no key and a full-height tower.
const HEAD: usize = 0;

/// Marks the end of a level.
const NIL: u32 = u32::MAX;

#[derive(Debug)]
struct Node {
    key: Vec<u8>,
    value: Value,
    /// `next[i]` is the index of the following node on level `i`.
    next: Vec<u32>,
}

/// An ordered in-memory table of the most recent writes.
#[derive(Debug)]
pub struct Memtable {
    nodes: Vec<Node>,
    /// Number of levels currently in use (1..=MAX_HEIGHT).
    height: usize,
    rng: u64,
    approx_bytes: usize,
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

impl Memtable {
    pub fn new() -> Self {
        let head = Node {
            key: Vec::new(),
            value: Value::Delete,
            next: vec![NIL; MAX_HEIGHT],
        };
        Self {
            nodes: vec![head],
            height: 1,
            rng: 0x9E37_79B9_7F4A_7C15,
            approx_bytes: 0,
        }
    }

    /// Inserts or overwrites `key`.
    pub fn insert(&mut self, key: Vec<u8>, value: Value) {
        let mut prev = [HEAD as u32; MAX_HEIGHT];
        let found = self.find_ge(&key, Some(&mut prev));
        if found != NIL && self.nodes[found as usize].key == key {
            let node = &mut self.nodes[found as usize];
            self.approx_bytes =
                self.approx_bytes - node.value.payload().len() + value.payload().len();
            node.value = value;
            return;
        }

        let height = self.random_height();
        if height > self.height {
            // prev[] already holds HEAD for the new levels.
            self.height = height;
        }
        self.approx_bytes += key.len() + value.payload().len() + ENTRY_OVERHEAD;

        let idx = u32::try_from(self.nodes.len()).expect("memtable exceeds u32::MAX entries");
        let mut next = Vec::with_capacity(height);
        for (level, &p) in prev.iter().enumerate().take(height) {
            next.push(self.nodes[p as usize].next[level]);
        }
        self.nodes.push(Node { key, value, next });
        for (level, &p) in prev.iter().enumerate().take(height) {
            self.nodes[p as usize].next[level] = idx;
        }
    }

    /// Returns the latest value for `key`, which may be a tombstone.
    pub fn get(&self, key: &[u8]) -> Option<&Value> {
        let found = self.find_ge(key, None);
        if found == NIL {
            return None;
        }
        let node = &self.nodes[found as usize];
        (node.key == key).then_some(&node.value)
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.nodes.len() - 1
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.len() == 1
    }

    /// Approximate heap footprint in bytes; compared against `Options::memtable_size`.
    pub fn approximate_size(&self) -> usize {
        self.approx_bytes
    }

    /// Iterates all entries in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Value)> {
        self.walk_from(self.nodes[HEAD].next[0])
    }

    /// Copies out the entries within `(start, end)`. The caller must ensure the range is
    /// non-empty (start <= end).
    pub fn range_entries(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Vec<Entry> {
        let first = match start {
            Bound::Unbounded => self.nodes[HEAD].next[0],
            Bound::Included(k) => self.find_ge(k, None),
            Bound::Excluded(k) => {
                let n = self.find_ge(k, None);
                if n != NIL && self.nodes[n as usize].key == k {
                    self.nodes[n as usize].next[0]
                } else {
                    n
                }
            }
        };
        self.walk_from(first)
            .take_while(|(k, _)| match end {
                Bound::Unbounded => true,
                Bound::Included(e) => k.as_slice() <= e,
                Bound::Excluded(e) => k.as_slice() < e,
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Returns the first node whose key is >= `key`, or NIL. When `prev` is given, it is
    /// filled with the last node before that position on every level in use.
    fn find_ge(&self, key: &[u8], mut prev: Option<&mut [u32; MAX_HEIGHT]>) -> u32 {
        let mut cur = HEAD;
        for level in (0..self.height).rev() {
            loop {
                let next = self.nodes[cur].next[level];
                if next != NIL
                    && self.nodes[next as usize].key.as_slice().cmp(key) == Ordering::Less
                {
                    cur = next as usize;
                } else {
                    break;
                }
            }
            if let Some(p) = prev.as_deref_mut() {
                p[level] = cur as u32;
            }
        }
        self.nodes[cur].next[0]
    }

    fn walk_from(&self, start: u32) -> impl Iterator<Item = (&Vec<u8>, &Value)> {
        let mut cur = start;
        std::iter::from_fn(move || {
            if cur == NIL {
                return None;
            }
            let node = &self.nodes[cur as usize];
            cur = node.next[0];
            Some((&node.key, &node.value))
        })
    }

    /// Geometric height with p = 1/4, from an xorshift64 generator.
    fn random_height(&mut self) -> usize {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        // Two random bits per level: each level is kept with probability 1/4.
        let h = 1 + (x.trailing_zeros() as usize / 2);
        h.min(MAX_HEIGHT)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

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
        assert_eq!(
            keys(m.range_entries(Bound::Excluded(b"bb"), Bound::Included(b"d"))),
            vec![b"c".to_vec(), b"d".to_vec()]
        );
        assert!(m
            .range_entries(Bound::Included(b"x"), Bound::Unbounded)
            .is_empty());
    }

    #[test]
    fn empty_table() {
        let m = Memtable::new();
        assert!(m.is_empty());
        assert_eq!(m.get(b"a"), None);
        assert_eq!(m.iter().count(), 0);
        assert!(m
            .range_entries(Bound::Unbounded, Bound::Unbounded)
            .is_empty());
    }

    #[test]
    fn towers_grow_past_one_level() {
        let mut m = Memtable::new();
        for i in 0u32..5_000 {
            m.insert(i.to_be_bytes().to_vec(), Value::Delete);
        }
        assert!(m.height > 3, "height stayed at {}", m.height);
        assert!(m.nodes.iter().skip(1).all(|n| n.next.len() <= MAX_HEIGHT));
    }

    /// Random inserts, overwrites and range reads checked against a BTreeMap.
    #[test]
    fn matches_btreemap_model() {
        let mut m = Memtable::new();
        let mut model = BTreeMap::new();
        let mut x: u64 = 42;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..20_000 {
            let r = next();
            let key = format!("k{:04}", r % 3_000).into_bytes();
            let value = if r % 7 == 0 {
                Value::Delete
            } else {
                Value::Put((r % 1_000).to_string().into_bytes())
            };
            m.insert(key.clone(), value.clone());
            model.insert(key, value);
        }
        assert_eq!(m.len(), model.len());
        let ours: Vec<_> = m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let theirs: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(ours, theirs);

        for _ in 0..500 {
            let a = format!("k{:04}", next() % 3_100).into_bytes();
            let b = format!("k{:04}", next() % 3_100).into_bytes();
            let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
            let got = m.range_entries(Bound::Included(&lo), Bound::Excluded(&hi));
            let want: Vec<_> = model
                .range::<[u8], _>((
                    Bound::Included(lo.as_slice()),
                    Bound::Excluded(hi.as_slice()),
                ))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            assert_eq!(got, want);
            let probe = format!("k{:04}", next() % 3_100).into_bytes();
            assert_eq!(m.get(&probe), model.get(&probe));
        }
    }
}
