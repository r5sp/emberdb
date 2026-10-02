//! Crash-recovery tests.
//!
//! A "crash" is simulated by dropping the database without flushing (so the latest writes
//! exist only in the write-ahead log) and then truncating or corrupting that log, as a
//! power failure part-way through an append would. On reopen the database must contain
//! exactly the writes whose WAL records are intact, and nothing else.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use emberdb::{Db, Options};
use proptest::prelude::*;

/// Large memtable so nothing is flushed: every write lives only in the WAL.
fn wal_only_opts() -> Options {
    Options {
        memtable_size: 64 << 20,
        background_compaction: false,
        ..Options::default()
    }
}

enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

struct CrashImage {
    /// Directory holding the database files as they were at the "crash".
    _dir: tempfile::TempDir,
    files: Vec<(String, Vec<u8>)>,
    log_name: String,
    log: Vec<u8>,
    /// WAL length after each operation: op `i`'s record ends at `boundaries[i]`.
    boundaries: Vec<u64>,
    ops: Vec<Op>,
}

fn single_log(dir: &Path) -> PathBuf {
    let logs: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "log"))
        .collect();
    assert_eq!(logs.len(), 1, "expected exactly one WAL file");
    logs.into_iter().next().unwrap()
}

/// Writes `n` operations (puts, overwrites and deletes) and captures the files on disk.
fn build_crash_image(n: usize, seed_data: bool) -> CrashImage {
    let dir = tempfile::tempdir().unwrap();
    if seed_data {
        // Some pre-existing data in SSTables that the WAL operations shadow.
        let db = Db::open(dir.path(), wal_only_opts()).unwrap();
        for i in 0..50 {
            db.put(format!("key{i:03}"), "from-sstable").unwrap();
        }
        db.flush().unwrap();
    }
    let db = Db::open(dir.path(), wal_only_opts()).unwrap();
    let log_path = single_log(dir.path());
    let mut ops = Vec::new();
    let mut boundaries = Vec::new();
    for i in 0..n {
        let k = format!("key{:03}", (i * 7) % 60).into_bytes();
        let op = if i % 5 == 4 {
            db.delete(&k).unwrap();
            Op::Delete(k)
        } else {
            let v = format!("value-{i}-{}", "#".repeat(i % 23)).into_bytes();
            db.put(&k, &v).unwrap();
            Op::Put(k, v)
        };
        ops.push(op);
        // Each write reaches the file in one write(2) call, so the length is a boundary.
        boundaries.push(fs::metadata(&log_path).unwrap().len());
    }
    drop(db);

    let log_name = log_path.file_name().unwrap().to_str().unwrap().to_string();
    let log = fs::read(&log_path).unwrap();
    assert_eq!(log.len() as u64, *boundaries.last().unwrap());
    let files = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.file_name() != log_name.as_str() && e.file_name() != "LOCK")
        .map(|e| {
            (
                e.file_name().to_str().unwrap().to_string(),
                fs::read(e.path()).unwrap(),
            )
        })
        .collect();
    CrashImage {
        _dir: dir,
        files,
        log_name,
        log,
        boundaries,
        ops,
    }
}

impl CrashImage {
    /// Materialises the image with the WAL replaced by `log` into a fresh directory.
    fn restore(&self, log: &[u8]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for (name, data) in &self.files {
            fs::write(dir.path().join(name), data).unwrap();
        }
        fs::write(dir.path().join(&self.log_name), log).unwrap();
        dir
    }

    /// Expected contents after applying the first `n` operations.
    fn model_after(&self, n: usize, seed_data: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut model = BTreeMap::new();
        if seed_data {
            for i in 0..50 {
                model.insert(format!("key{i:03}").into_bytes(), b"from-sstable".to_vec());
            }
        }
        for op in &self.ops[..n] {
            match op {
                Op::Put(k, v) => {
                    model.insert(k.clone(), v.clone());
                }
                Op::Delete(k) => {
                    model.remove(k);
                }
            }
        }
        model.into_iter().collect()
    }

    /// Number of operations whose records lie entirely within the first `cut` bytes.
    fn intact_ops(&self, cut: usize) -> usize {
        self.boundaries.partition_point(|&b| b <= cut as u64)
    }

    fn check_cut(&self, cut: usize, seed_data: bool) {
        let dir = self.restore(&self.log[..cut]);
        let expected = self.model_after(self.intact_ops(cut), seed_data);
        {
            let db = Db::open(dir.path(), wal_only_opts()).unwrap();
            let got: Vec<_> = db.iter().unwrap().map(Result::unwrap).collect();
            assert_eq!(got, expected, "WAL truncated at byte {cut}");
            // The recovered database must accept new writes...
            db.put("after-crash", "ok").unwrap();
        }
        // ...and must not resurrect torn bytes or lose data on a second restart.
        let db = Db::open(dir.path(), wal_only_opts()).unwrap();
        assert_eq!(db.get("after-crash").unwrap(), Some(b"ok".to_vec()));
        let mut got: Vec<_> = db.iter().unwrap().map(Result::unwrap).collect();
        got.retain(|(k, _)| k != b"after-crash");
        assert_eq!(got, expected, "after second reopen, cut {cut}");
    }
}

#[test]
fn truncation_at_every_record_boundary() {
    let image = build_crash_image(60, false);
    let mut cuts = vec![0usize];
    for &b in &image.boundaries {
        let b = b as usize;
        cuts.extend([b - 1, b, (b + 1).min(image.log.len())]);
    }
    cuts.sort_unstable();
    cuts.dedup();
    for cut in cuts {
        image.check_cut(cut, false);
    }
}

#[test]
fn truncation_with_older_data_in_sstables() {
    let image = build_crash_image(40, true);
    for &b in image.boundaries.iter().step_by(3) {
        image.check_cut(b as usize - 2, true);
        image.check_cut(b as usize, true);
    }
}

#[test]
fn corrupted_record_discards_it_and_everything_after() {
    let image = build_crash_image(30, false);
    let victim = 17;
    let start = image.boundaries[victim - 1] as usize;
    let end = image.boundaries[victim] as usize;
    let mut log = image.log.clone();
    log[(start + end) / 2] ^= 0xa5;
    let dir = image.restore(&log);
    let db = Db::open(dir.path(), wal_only_opts()).unwrap();
    let got: Vec<_> = db.iter().unwrap().map(Result::unwrap).collect();
    assert_eq!(got, image.model_after(victim, false));
}

#[test]
fn sync_writes_mode_recovers_everything() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        sync_writes: true,
        ..wal_only_opts()
    };
    {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        for i in 0..100 {
            db.put(format!("k{i}"), format!("v{i}")).unwrap();
        }
    }
    let db = Db::open(dir.path(), opts).unwrap();
    assert_eq!(db.iter().unwrap().count(), 100);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Random truncation points, mostly landing inside records.
    #[test]
    fn truncation_at_random_offsets(fraction in 0.0f64..=1.0) {
        thread_local! {
            static IMAGE: CrashImage = build_crash_image(80, true);
        }
        IMAGE.with(|image| {
            let cut = ((image.log.len() as f64) * fraction) as usize;
            image.check_cut(cut.min(image.log.len()), true);
        });
    }
}
