//! K-way merge over sorted sources.
//!
//! Reads and compactions both need a single sorted view over several sorted inputs (the
//! memtables and many SSTables) where the same key may appear in more than one input.
//! Sources are supplied newest-first; when several sources hold the same key, only the
//! entry from the newest source is emitted and the shadowed versions are skipped.
//!
//! A binary heap ordered by `(key, source index)` makes each step `O(log k)`.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::error::Result;
use crate::types::{Entry, Value};

/// A boxed sorted source of entries.
pub type BoxedIter = Box<dyn Iterator<Item = Result<Entry>> + Send>;

struct HeapItem {
    key: Vec<u8>,
    value: Value,
    source: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.source == other.source
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; reverse so the smallest (key, source) is on top.
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.source.cmp(&self.source))
    }
}

/// Merges sorted sources into one sorted, de-duplicated stream. Tombstones are passed
/// through: callers decide whether to hide them (reads) or keep them (compactions that
/// are not at the bottom level).
pub struct MergeIterator {
    sources: Vec<BoxedIter>,
    heap: BinaryHeap<HeapItem>,
    initialized: bool,
    failed: bool,
}

impl MergeIterator {
    /// `sources` must be ordered newest first, and each must yield strictly increasing keys.
    pub fn new(sources: Vec<BoxedIter>) -> Self {
        MergeIterator {
            heap: BinaryHeap::with_capacity(sources.len()),
            sources,
            initialized: false,
            failed: false,
        }
    }

    /// Pulls the next entry from `source` into the heap.
    fn refill(&mut self, source: usize) -> Result<()> {
        if let Some(item) = self.sources[source].next() {
            let (key, value) = item?;
            self.heap.push(HeapItem { key, value, source });
        }
        Ok(())
    }

    fn step(&mut self) -> Result<Option<Entry>> {
        if !self.initialized {
            self.initialized = true;
            for i in 0..self.sources.len() {
                self.refill(i)?;
            }
        }
        let Some(top) = self.heap.pop() else {
            return Ok(None);
        };
        self.refill(top.source)?;
        // Skip older versions of the same key held by other sources.
        while self.heap.peek().is_some_and(|next| next.key == top.key) {
            let shadowed = self.heap.pop().expect("peeked");
            self.refill(shadowed.source)?;
        }
        Ok(Some((top.key, top.value)))
    }
}

impl Iterator for MergeIterator {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.step() {
            Ok(entry) => entry.map(Ok),
            Err(e) => {
                self.failed = true;
                Some(Err(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    fn source(entries: &[(&str, Option<&str>)]) -> BoxedIter {
        let v: Vec<Result<Entry>> = entries
            .iter()
            .map(|(k, v)| {
                let value = match v {
                    Some(v) => Value::Put(v.as_bytes().to_vec()),
                    None => Value::Delete,
                };
                Ok((k.as_bytes().to_vec(), value))
            })
            .collect();
        Box::new(v.into_iter())
    }

    fn collect(it: MergeIterator) -> Vec<(String, Option<String>)> {
        it.map(|r| {
            let (k, v) = r.unwrap();
            (
                String::from_utf8(k).unwrap(),
                v.into_option().map(|v| String::from_utf8(v).unwrap()),
            )
        })
        .collect()
    }

    #[test]
    fn newest_source_wins() {
        let newest = source(&[("a", Some("a3")), ("c", None)]);
        let middle = source(&[("a", Some("a2")), ("b", Some("b2")), ("c", Some("c2"))]);
        let oldest = source(&[("a", Some("a1")), ("d", Some("d1"))]);
        let got = collect(MergeIterator::new(vec![newest, middle, oldest]));
        let expected = vec![
            ("a".to_string(), Some("a3".to_string())),
            ("b".to_string(), Some("b2".to_string())),
            ("c".to_string(), None),
            ("d".to_string(), Some("d1".to_string())),
        ];
        assert_eq!(got, expected);
    }

    #[test]
    fn handles_empty_and_no_sources() {
        assert!(collect(MergeIterator::new(vec![])).is_empty());
        let got = collect(MergeIterator::new(vec![
            source(&[]),
            source(&[("x", Some("1"))]),
        ]));
        assert_eq!(got, vec![("x".to_string(), Some("1".to_string()))]);
    }

    #[test]
    fn propagates_errors_once() {
        let failing: BoxedIter = Box::new(
            vec![
                Ok((b"a".to_vec(), Value::Delete)),
                Err(Error::corruption("boom")),
            ]
            .into_iter(),
        );
        let mut it = MergeIterator::new(vec![failing]);
        // The error surfaces when the source is refilled after yielding "a".
        assert!(it.next().unwrap().is_err());
        assert!(it.next().is_none());
    }

    #[test]
    fn matches_naive_merge() {
        // Pseudo-random sources checked against a BTreeMap built oldest-to-newest.
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut rand = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut model = std::collections::BTreeMap::new();
        let mut sources_oldest_first = Vec::new();
        for s in 0..6 {
            let mut keys: Vec<u64> = (0..200).map(|_| rand() % 300).collect();
            keys.sort_unstable();
            keys.dedup();
            let entries: Vec<Result<Entry>> = keys
                .iter()
                .map(|k| {
                    let key = format!("{k:04}").into_bytes();
                    let value = Value::Put(format!("{s}").into_bytes());
                    model.insert(key.clone(), value.clone());
                    Ok((key, value))
                })
                .collect();
            sources_oldest_first.push(entries);
        }
        let sources: Vec<BoxedIter> = sources_oldest_first
            .into_iter()
            .rev()
            .map(|v| Box::new(v.into_iter()) as BoxedIter)
            .collect();
        let got: Vec<Entry> = MergeIterator::new(sources).map(Result::unwrap).collect();
        let expected: Vec<Entry> = model.into_iter().collect();
        assert_eq!(got, expected);
    }
}
