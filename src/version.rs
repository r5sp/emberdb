//! An immutable snapshot of the SSTables that make up the tree.
//!
//! Readers clone an `Arc<Version>` and can then search it without holding any lock.
//! Flushes and compactions build a new `Version` and swap it in; tables referenced by an
//! old version stay readable for as long as someone holds it.

use std::collections::HashSet;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use crate::error::Result;
use crate::manifest::FileMeta;
use crate::sstable::Table;
use crate::types::Value;

/// An open SSTable together with its manifest metadata.
pub struct TableHandle {
    pub meta: FileMeta,
    pub table: Arc<Table>,
}

impl TableHandle {
    pub fn open(dir: &Path, meta: FileMeta) -> Result<Arc<Self>> {
        let table = Arc::new(Table::open(&table_path(dir, meta.number))?);
        Ok(Arc::new(TableHandle { meta, table }))
    }

    /// Whether this table's key range intersects `(start, end)`.
    pub fn overlaps(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
        range_overlaps(&self.meta.smallest, &self.meta.largest, start, end)
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.meta.smallest.as_slice() <= key && key <= self.meta.largest.as_slice()
    }
}

/// Whether `[smallest, largest]` intersects `(start, end)`.
pub fn range_overlaps(
    smallest: &[u8],
    largest: &[u8],
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
) -> bool {
    let before_start = match start {
        Bound::Included(s) => largest < s,
        Bound::Excluded(s) => largest <= s,
        Bound::Unbounded => false,
    };
    let after_end = match end {
        Bound::Included(e) => smallest > e,
        Bound::Excluded(e) => smallest >= e,
        Bound::Unbounded => false,
    };
    !before_start && !after_end
}

pub fn table_path(dir: &Path, number: u64) -> std::path::PathBuf {
    dir.join(format!("{number:06}.sst"))
}

pub fn log_path(dir: &Path, number: u64) -> std::path::PathBuf {
    dir.join(format!("{number:06}.log"))
}

/// The set of live tables, by level.
#[derive(Clone)]
pub struct Version {
    pub levels: Vec<Vec<Arc<TableHandle>>>,
}

impl Version {
    pub fn new(num_levels: usize) -> Self {
        Version {
            levels: vec![Vec::new(); num_levels],
        }
    }

    /// Looks `key` up level by level, returning the newest stored value (which may be a
    /// tombstone).
    pub fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        // L0 tables may overlap, so each candidate is checked newest-first.
        for h in &self.levels[0] {
            if h.contains_key(key) {
                if let Some(v) = h.table.get(key)? {
                    return Ok(Some(v));
                }
            }
        }
        // Deeper levels are sorted and disjoint: binary search for the one candidate.
        for level in &self.levels[1..] {
            let idx = level.partition_point(|h| h.meta.largest.as_slice() < key);
            if let Some(h) = level.get(idx) {
                if h.contains_key(key) {
                    if let Some(v) = h.table.get(key)? {
                        return Ok(Some(v));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Tables in `level` that overlap `(start, end)`, in level order.
    pub fn overlapping(
        &self,
        level: usize,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Vec<Arc<TableHandle>> {
        self.levels[level]
            .iter()
            .filter(|h| h.overlaps(start, end))
            .cloned()
            .collect()
    }

    /// Returns a new version with the tables numbered in `removed` dropped and `added`
    /// inserted at their levels, keeping each level correctly ordered.
    pub fn apply(&self, removed: &HashSet<u64>, added: Vec<(usize, Arc<TableHandle>)>) -> Version {
        let mut levels: Vec<Vec<Arc<TableHandle>>> = self
            .levels
            .iter()
            .map(|l| {
                l.iter()
                    .filter(|h| !removed.contains(&h.meta.number))
                    .cloned()
                    .collect()
            })
            .collect();
        for (level, h) in added {
            levels[level].push(h);
        }
        levels[0].sort_by_key(|t| std::cmp::Reverse(t.meta.number));
        for level in &mut levels[1..] {
            level.sort_by(|a, b| a.meta.smallest.cmp(&b.meta.smallest));
            debug_assert!(
                level
                    .windows(2)
                    .all(|w| w[0].meta.largest < w[1].meta.smallest),
                "tables in L1+ must not overlap"
            );
        }
        Version { levels }
    }

    pub fn level_bytes(&self, level: usize) -> u64 {
        self.levels[level].iter().map(|h| h.meta.file_size).sum()
    }

    pub fn manifest_levels(&self) -> Vec<Vec<FileMeta>> {
        self.levels
            .iter()
            .map(|l| l.iter().map(|h| h.meta.clone()).collect())
            .collect()
    }

    pub fn live_files(&self) -> HashSet<u64> {
        self.levels
            .iter()
            .flatten()
            .map(|h| h.meta.number)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlap_respects_bound_kinds() {
        let (s, l) = (&b"c"[..], &b"f"[..]);
        use Bound::*;
        assert!(range_overlaps(s, l, Unbounded, Unbounded));
        assert!(range_overlaps(s, l, Included(b"f"), Unbounded));
        assert!(!range_overlaps(s, l, Excluded(b"f"), Unbounded));
        assert!(!range_overlaps(s, l, Included(b"g"), Unbounded));
        assert!(range_overlaps(s, l, Unbounded, Included(b"c")));
        assert!(!range_overlaps(s, l, Unbounded, Excluded(b"c")));
        assert!(!range_overlaps(s, l, Unbounded, Included(b"b")));
        assert!(range_overlaps(s, l, Included(b"a"), Included(b"z")));
        assert!(range_overlaps(s, l, Included(b"d"), Excluded(b"e")));
    }
}
