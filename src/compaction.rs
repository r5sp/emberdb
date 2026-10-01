//! Leveled compaction: choosing what to compact and producing the merged output.
//!
//! Invariants maintained across levels:
//!
//! * L0 holds whole memtable flushes. Its tables may overlap one another.
//! * Every level `L >= 1` is a single sorted run: its tables are disjoint in key range.
//! * For any key, a version in level `L` is newer than any version in level `L + 1`.
//!
//! When L0 accumulates `l0_compaction_trigger` tables, *all* of them are merged with the
//! overlapping L1 tables (taking only some would let an older L0 version stay above a
//! newer one that moved down). When a level `L >= 1` exceeds its byte budget, one table is
//! chosen round-robin and merged with the overlapping tables of `L + 1`.

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::Result;
use crate::iterator::{BoxedIter, MergeIterator};
use crate::manifest::FileMeta;
use crate::options::Options;
use crate::sstable::TableBuilder;
use crate::version::{table_path, TableHandle, Version};

/// A unit of compaction work.
pub struct Compaction {
    /// Level the compaction was triggered for (`None` for a manual full compaction).
    pub level: Option<usize>,
    /// Level the merged tables are written to.
    pub output_level: usize,
    /// Input runs ordered newest first. Each run is sorted and internally disjoint, so it
    /// can be read as a single concatenated stream.
    pub runs: Vec<Vec<Arc<TableHandle>>>,
    /// True when no level below `output_level` overlaps the inputs, so tombstones have
    /// nothing left to shadow and can be discarded.
    pub drop_tombstones: bool,
}

impl Compaction {
    pub fn inputs(&self) -> impl Iterator<Item = &Arc<TableHandle>> {
        self.runs.iter().flatten()
    }

    pub fn num_inputs(&self) -> usize {
        self.runs.iter().map(Vec::len).sum()
    }

    /// A single input table with nothing to merge against can be moved to the next level
    /// by a metadata-only edit, avoiding a rewrite.
    pub fn is_trivial_move(&self) -> bool {
        self.level.is_some() && self.num_inputs() == 1
    }
}

/// Smallest and largest key across `tables`.
fn key_span<'a>(tables: impl Iterator<Item = &'a Arc<TableHandle>>) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut span: Option<(&[u8], &[u8])> = None;
    for t in tables {
        let (s, l) = (t.meta.smallest.as_slice(), t.meta.largest.as_slice());
        span = Some(match span {
            None => (s, l),
            Some((cs, cl)) => (cs.min(s), cl.max(l)),
        });
    }
    span.map(|(s, l)| (s.to_vec(), l.to_vec()))
}

fn no_deeper_overlap(
    version: &Version,
    output_level: usize,
    smallest: &[u8],
    largest: &[u8],
) -> bool {
    version.levels[output_level + 1..]
        .iter()
        .flatten()
        .all(|t| !t.overlaps(Bound::Included(smallest), Bound::Included(largest)))
}

/// Scores each level and returns the most urgent compaction, if any level is over budget.
/// `pointers[level]` remembers where the last compaction of that level ended so tables
/// are chosen round-robin across the key space.
pub fn pick(version: &Version, opts: &Options, pointers: &mut [Vec<u8>]) -> Option<Compaction> {
    let mut best: Option<(f64, usize)> = None;
    let l0_score = version.levels[0].len() as f64 / opts.l0_compaction_trigger as f64;
    if l0_score >= 1.0 {
        best = Some((l0_score, 0));
    }
    // The last level has no level below it to compact into.
    for level in 1..opts.num_levels - 1 {
        let score = version.level_bytes(level) as f64 / opts.max_bytes_for_level(level) as f64;
        if score >= 1.0 && best.is_none_or(|(b, _)| score > b) {
            best = Some((score, level));
        }
    }
    let (_, level) = best?;

    let upper: Vec<Arc<TableHandle>> = if level == 0 {
        version.levels[0].clone()
    } else {
        let files = &version.levels[level];
        let ptr = &pointers[level];
        let chosen = files
            .iter()
            .find(|t| ptr.is_empty() || t.meta.smallest > *ptr)
            .unwrap_or(&files[0]);
        pointers[level] = chosen.meta.largest.clone();
        vec![chosen.clone()]
    };

    let (smallest, largest) = key_span(upper.iter())?;
    let lower = version.overlapping(
        level + 1,
        Bound::Included(&smallest),
        Bound::Included(&largest),
    );
    let (smallest, largest) = key_span(upper.iter().chain(lower.iter()))?;

    // L0 tables overlap, so each is its own run (already newest-first).
    let mut runs: Vec<Vec<Arc<TableHandle>>> = if level == 0 {
        upper.into_iter().map(|t| vec![t]).collect()
    } else {
        vec![upper]
    };
    if !lower.is_empty() {
        runs.push(lower);
    }
    Some(Compaction {
        level: Some(level),
        output_level: level + 1,
        drop_tombstones: no_deeper_overlap(version, level + 1, &smallest, &largest),
        runs,
    })
}

/// A manual compaction that merges every table into the deepest non-empty level (at
/// least L1), discarding all shadowed versions and tombstones.
pub fn full(version: &Version) -> Option<Compaction> {
    let deepest = version.levels.iter().rposition(|l| !l.is_empty())?;
    let output_level = deepest.max(1);
    let mut runs: Vec<Vec<Arc<TableHandle>>> =
        version.levels[0].iter().map(|t| vec![t.clone()]).collect();
    for level in &version.levels[1..] {
        if !level.is_empty() {
            runs.push(level.clone());
        }
    }
    Some(Compaction {
        level: None,
        output_level,
        runs,
        drop_tombstones: true,
    })
}

/// Merges the compaction inputs into new tables of roughly `target_file_size` bytes.
/// Output tables are fsynced but not yet referenced by any manifest.
pub fn execute(
    c: &Compaction,
    dir: &Path,
    opts: &Options,
    next_file: &AtomicU64,
) -> Result<Vec<Arc<TableHandle>>> {
    let sources: Vec<BoxedIter> = c
        .runs
        .iter()
        .map(|run| {
            let run = run.clone();
            Box::new(run.into_iter().flat_map(|t| t.table.iter(None))) as BoxedIter
        })
        .collect();

    let mut outputs = Vec::new();
    let mut current: Option<(u64, TableBuilder)> = None;

    let finish =
        |number: u64, builder: TableBuilder, outputs: &mut Vec<Arc<TableHandle>>| -> Result<()> {
            let info = builder.finish()?;
            let meta = FileMeta {
                number,
                file_size: info.file_size,
                num_entries: info.num_entries,
                smallest: info.smallest,
                largest: info.largest,
            };
            outputs.push(TableHandle::open(dir, meta)?);
            Ok(())
        };

    for entry in MergeIterator::new(sources) {
        let (key, value) = entry?;
        if c.drop_tombstones && value.is_delete() {
            continue;
        }
        if current.is_none() {
            let number = next_file.fetch_add(1, Ordering::SeqCst);
            current = Some((
                number,
                TableBuilder::create(&table_path(dir, number), opts)?,
            ));
        }
        let (_, builder) = current.as_mut().expect("just created");
        builder.add(&key, &value)?;
        if builder.estimated_size() >= opts.target_file_size {
            let (number, builder) = current.take().expect("present");
            finish(number, builder, &mut outputs)?;
        }
    }
    if let Some((number, builder)) = current.take() {
        finish(number, builder, &mut outputs)?;
    }
    Ok(outputs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    struct Fixture {
        dir: tempfile::TempDir,
        next: AtomicU64,
        opts: Options,
    }

    impl Fixture {
        fn new() -> Self {
            Fixture {
                dir: tempfile::tempdir().unwrap(),
                next: AtomicU64::new(1),
                opts: Options {
                    block_size: 256,
                    l0_compaction_trigger: 2,
                    level1_max_bytes: 4 << 10,
                    target_file_size: 2 << 10,
                    num_levels: 4,
                    ..Options::default()
                },
            }
        }

        fn table(&self, entries: &[(&str, Option<&str>)]) -> Arc<TableHandle> {
            let number = self.next.fetch_add(1, Ordering::SeqCst);
            let path = table_path(self.dir.path(), number);
            let mut b = TableBuilder::create(&path, &self.opts).unwrap();
            for (k, v) in entries {
                let v = v.map_or(Value::Delete, |v| Value::Put(v.as_bytes().to_vec()));
                b.add(k.as_bytes(), &v).unwrap();
            }
            let info = b.finish().unwrap();
            TableHandle::open(
                self.dir.path(),
                FileMeta {
                    number,
                    file_size: info.file_size,
                    num_entries: info.num_entries,
                    smallest: info.smallest,
                    largest: info.largest,
                },
            )
            .unwrap()
        }

        fn contents(&self, tables: &[Arc<TableHandle>]) -> Vec<(String, Option<String>)> {
            tables
                .iter()
                .flat_map(|t| t.table.iter(None))
                .map(|r| {
                    let (k, v) = r.unwrap();
                    (
                        String::from_utf8(k).unwrap(),
                        v.into_option().map(|v| String::from_utf8(v).unwrap()),
                    )
                })
                .collect()
        }
    }

    fn version(levels: Vec<Vec<Arc<TableHandle>>>) -> Version {
        Version { levels }
    }

    #[test]
    fn l0_compaction_merges_all_l0_and_overlapping_l1() {
        let f = Fixture::new();
        let l1_a = f.table(&[("a", Some("1")), ("c", Some("1"))]);
        let l1_z = f.table(&[("x", Some("1")), ("z", Some("1"))]);
        let old = f.table(&[("b", Some("old")), ("c", Some("old"))]);
        let new = f.table(&[("c", None), ("d", Some("new"))]);
        let v = version(vec![
            vec![new.clone(), old.clone()],
            vec![l1_a.clone(), l1_z.clone()],
            vec![],
            vec![],
        ]);
        let mut ptrs = vec![Vec::new(); 4];
        let c = pick(&v, &f.opts, &mut ptrs).expect("L0 is at its trigger");
        assert_eq!(c.level, Some(0));
        assert_eq!(c.output_level, 1);
        assert_eq!(c.num_inputs(), 3, "l1_z does not overlap [b, d]");
        assert!(c.drop_tombstones, "nothing below L1");

        let out = execute(&c, f.dir.path(), &f.opts, &f.next).unwrap();
        let expected = vec![
            ("a".to_string(), Some("1".to_string())),
            ("b".to_string(), Some("old".to_string())),
            ("d".to_string(), Some("new".to_string())),
        ];
        assert_eq!(
            f.contents(&out),
            expected,
            "c's tombstone and old versions are gone"
        );
    }

    #[test]
    fn tombstones_survive_when_deeper_levels_overlap() {
        let f = Fixture::new();
        let deep = f.table(&[("k", Some("ancient"))]);
        let l0a = f.table(&[("k", None)]);
        let l0b = f.table(&[("m", Some("1"))]);
        let v = version(vec![vec![l0b, l0a], vec![], vec![deep], vec![]]);
        let c = pick(&v, &f.opts, &mut [vec![], vec![], vec![], vec![]]).unwrap();
        assert!(!c.drop_tombstones);
        let out = execute(&c, f.dir.path(), &f.opts, &f.next).unwrap();
        assert_eq!(
            f.contents(&out),
            vec![
                ("k".to_string(), None),
                ("m".to_string(), Some("1".to_string()))
            ]
        );
    }

    #[test]
    fn level_compaction_is_round_robin_and_detects_trivial_moves() {
        let f = Fixture::new();
        let big = "v".repeat(1500);
        let t1 = f.table(&[("a", Some(&big)), ("b", Some(&big))]);
        let t2 = f.table(&[("c", Some(&big)), ("d", Some(&big))]);
        let below = f.table(&[("c", Some("x"))]);
        let v = version(vec![
            vec![],
            vec![t1.clone(), t2.clone()],
            vec![below],
            vec![],
        ]);
        assert!(v.level_bytes(1) > f.opts.max_bytes_for_level(1));

        let mut ptrs = vec![Vec::new(); 4];
        let first = pick(&v, &f.opts, &mut ptrs).unwrap();
        assert_eq!(first.level, Some(1));
        assert_eq!(first.runs[0][0].meta.number, t1.meta.number);
        assert!(first.is_trivial_move(), "nothing in L2 overlaps [a, b]");

        let second = pick(&v, &f.opts, &mut ptrs).unwrap();
        assert_eq!(second.runs[0][0].meta.number, t2.meta.number);
        assert_eq!(second.num_inputs(), 2);
        assert!(!second.is_trivial_move());

        let third = pick(&v, &f.opts, &mut ptrs).unwrap();
        assert_eq!(third.runs[0][0].meta.number, t1.meta.number, "wraps around");
    }

    #[test]
    fn outputs_are_split_at_target_size_and_disjoint() {
        let f = Fixture::new();
        let entries: Vec<(String, String)> = (0..400)
            .map(|i| (format!("key{i:05}"), "x".repeat(40)))
            .collect();
        let refs: Vec<(&str, Option<&str>)> = entries
            .iter()
            .map(|(k, v)| (k.as_str(), Some(v.as_str())))
            .collect();
        let t = f.table(&refs);
        let v = version(vec![vec![t], vec![], vec![], vec![]]);
        let c = full(&v).unwrap();
        assert_eq!(c.output_level, 1);
        let out = execute(&c, f.dir.path(), &f.opts, &f.next).unwrap();
        assert!(
            out.len() > 3,
            "expected several output tables, got {}",
            out.len()
        );
        for w in out.windows(2) {
            assert!(w[0].meta.largest < w[1].meta.smallest);
        }
        assert_eq!(f.contents(&out).len(), 400);
    }

    #[test]
    fn nothing_to_do_when_under_budget() {
        let f = Fixture::new();
        let v = version(vec![
            vec![f.table(&[("a", Some("1"))])],
            vec![],
            vec![],
            vec![],
        ]);
        assert!(pick(&v, &f.opts, &mut [vec![], vec![], vec![], vec![]]).is_none());
        assert!(full(&Version::new(4)).is_none());
    }
}
