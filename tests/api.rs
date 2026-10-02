//! End-to-end tests of the public API.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;

use emberdb::{Db, Error, Options};

/// Tiny sizes so a few thousand writes exercise flushes and multi-level compaction.
fn small_opts(background: bool) -> Options {
    Options {
        memtable_size: 4 << 10,
        block_size: 256,
        l0_compaction_trigger: 2,
        level1_max_bytes: 16 << 10,
        level_size_multiplier: 4,
        target_file_size: 8 << 10,
        num_levels: 5,
        background_compaction: background,
        ..Options::default()
    }
}

fn key(i: usize) -> String {
    format!("key{i:06}")
}

fn collect(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.iter().unwrap().map(Result::unwrap).collect()
}

#[test]
fn put_get_delete_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db.get("missing").unwrap(), None);
    db.put("a", "1").unwrap();
    db.put("b", "2").unwrap();
    db.put("a", "3").unwrap();
    assert_eq!(db.get("a").unwrap(), Some(b"3".to_vec()));
    db.delete("b").unwrap();
    db.delete("never-existed").unwrap();
    assert_eq!(db.get("b").unwrap(), None);
    db.put("", "empty key is allowed").unwrap();
    db.put("empty-value", "").unwrap();
    assert_eq!(db.get("").unwrap(), Some(b"empty key is allowed".to_vec()));
    assert_eq!(db.get("empty-value").unwrap(), Some(vec![]));
}

#[test]
fn data_survives_reopen_with_and_without_flush() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), small_opts(false)).unwrap();
        for i in 0..2_000 {
            db.put(key(i), format!("v{i}")).unwrap();
        }
        for i in (0..2_000).step_by(3) {
            db.delete(key(i)).unwrap();
        }
        // Dropped without an explicit flush: the tail lives only in the WAL.
    }
    let db = Db::open(dir.path(), small_opts(false)).unwrap();
    for i in 0..2_000 {
        let expected = (i % 3 != 0).then(|| format!("v{i}").into_bytes());
        assert_eq!(db.get(key(i)).unwrap(), expected, "key {i}");
    }
    assert!(db.stats().levels.iter().skip(1).any(|l| l.files > 0));
}

#[test]
fn flush_and_compact_preserve_contents_and_drop_garbage() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts(false)).unwrap();
    let mut model = BTreeMap::new();
    for round in 0..5 {
        for i in 0..500 {
            let v = format!("round{round}-{i}");
            db.put(key(i), &v).unwrap();
            model.insert(key(i).into_bytes(), v.into_bytes());
        }
    }
    for i in 0..250 {
        db.delete(key(i)).unwrap();
        model.remove(key(i).as_bytes());
    }
    db.flush().unwrap();
    assert_eq!(db.stats().memtable_bytes, 0);

    db.compact().unwrap();
    let stats = db.stats();
    assert_eq!(stats.levels[0].files, 0, "full compaction empties L0");
    let populated: Vec<usize> = (0..stats.levels.len())
        .filter(|&l| stats.levels[l].files > 0)
        .collect();
    assert_eq!(populated.len(), 1, "everything lands in one level: {stats}");

    let expected: Vec<(Vec<u8>, Vec<u8>)> = model.into_iter().collect();
    assert_eq!(collect(&db), expected);

    // Only 250 live keys of ~30 bytes remain; overwritten values and tombstones are gone.
    assert!(stats.levels[populated[0]].bytes < 16 << 10, "{stats}");
}

#[test]
fn scan_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts(false)).unwrap();
    for i in 0..1_000 {
        db.put(key(i), format!("{i}")).unwrap();
    }
    db.flush().unwrap();
    // Some data in memtable, some deleted, some overwritten after the flush.
    for i in 1_000..1_100 {
        db.put(key(i), format!("{i}")).unwrap();
    }
    db.delete(key(10)).unwrap();
    db.put(key(11), "new").unwrap();

    let keys = |it: emberdb::DbIterator| -> Vec<String> {
        it.map(|r| String::from_utf8(r.unwrap().0).unwrap())
            .collect()
    };
    let got = keys(db.scan(key(5)..key(13)).unwrap());
    let want: Vec<String> = [5, 6, 7, 8, 9, 11, 12].iter().map(|&i| key(i)).collect();
    assert_eq!(got, want);

    let got = keys(db.scan(key(5)..=key(13)).unwrap());
    assert_eq!(got.last().unwrap(), &key(13));

    assert_eq!(keys(db.scan(key(1_095)..).unwrap()).len(), 5);
    assert_eq!(
        keys(db.scan(..key(3)).unwrap()),
        vec![key(0), key(1), key(2)]
    );
    assert_eq!(db.iter().unwrap().count(), 1_099);
    assert_eq!(db.scan("zzz"..).unwrap().count(), 0);
    assert_eq!(db.scan(key(9)..key(9)).unwrap().count(), 0);
    #[allow(clippy::reversed_empty_ranges)]
    let reversed = db.scan(key(9)..key(1)).unwrap().count();
    assert_eq!(reversed, 0);

    let (k, v) = db.scan(key(11)..key(12)).unwrap().next().unwrap().unwrap();
    assert_eq!((k, v), (key(11).into_bytes(), b"new".to_vec()));
}

#[test]
fn scan_is_a_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts(false)).unwrap();
    for i in 0..100 {
        db.put(key(i), "old").unwrap();
    }
    let it = db.iter().unwrap();
    for i in 0..100 {
        db.put(key(i), "new").unwrap();
    }
    db.put("zzz", "late").unwrap();
    let seen: Vec<_> = it.map(Result::unwrap).collect();
    assert_eq!(seen.len(), 100);
    assert!(seen.iter().all(|(_, v)| v == b"old"));
}

#[test]
fn scan_survives_concurrent_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts(false)).unwrap();
    for i in 0..3_000 {
        db.put(key(i), format!("{i}")).unwrap();
    }
    db.flush().unwrap();
    let mut it = db.iter().unwrap();
    let first = it.next().unwrap().unwrap();
    // Compaction deletes the table files the iterator is reading from.
    db.compact().unwrap();
    let rest: Vec<_> = it.map(Result::unwrap).collect();
    assert_eq!(first.0, key(0).into_bytes());
    assert_eq!(rest.len(), 2_999);
}

#[test]
fn second_open_of_same_directory_fails() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    match Db::open(dir.path(), Options::default()) {
        Err(Error::InvalidArgument(msg)) => assert!(msg.contains("already open")),
        Err(e) => panic!("unexpected error {e}"),
        Ok(_) => panic!("second open should fail"),
    }
    drop(db);
    Db::open(dir.path(), Options::default()).unwrap();
}

#[test]
fn create_if_missing_false() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        create_if_missing: false,
        ..Options::default()
    };
    assert!(Db::open(dir.path().join("nope"), opts).is_err());
}

#[test]
fn orphaned_files_are_removed_on_open() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), small_opts(false)).unwrap();
        db.put("k", "v").unwrap();
        db.flush().unwrap();
    }
    // Simulate a crash mid-compaction and mid-manifest-write.
    std::fs::write(dir.path().join("999999.sst"), b"partial table").unwrap();
    std::fs::write(dir.path().join("MANIFEST.tmp"), b"partial manifest").unwrap();
    let db = Db::open(dir.path(), small_opts(false)).unwrap();
    assert!(!dir.path().join("999999.sst").exists());
    assert!(!dir.path().join("MANIFEST.tmp").exists());
    assert_eq!(db.get("k").unwrap(), Some(b"v".to_vec()));
    // New files never reuse the orphan's number.
    db.put("k2", "v2").unwrap();
    db.flush().unwrap();
    assert_eq!(db.get("k2").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn compaction_reports_write_amplification() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts(false)).unwrap();
    for i in 0..5_000 {
        db.put(key(i * 7919 % 5_000), [b'x'; 64]).unwrap();
    }
    let stats = db.stats();
    assert!(stats.flushes > 10, "{stats}");
    assert!(stats.compactions > 0, "{stats}");
    assert!(stats.write_amplification() > 1.0, "{stats}");
    // L1+ budgets are respected once compaction has settled.
    for (level, l) in stats.levels.iter().enumerate().skip(1).take(3) {
        let budget = (16u64 << 10) * 4u64.pow(level as u32 - 1);
        assert!(l.bytes <= budget * 2, "L{level} over budget: {stats}");
    }
}

#[test]
fn concurrent_readers_and_writers_with_background_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(dir.path(), small_opts(true)).unwrap());
    let writers: Vec<_> = (0..4)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                for i in 0..2_000 {
                    db.put(format!("t{t}-{i:05}"), format!("{t}:{i}")).unwrap();
                }
            })
        })
        .collect();
    let readers: Vec<_> = (0..2)
        .map(|_| {
            let db = db.clone();
            thread::spawn(move || {
                for _ in 0..20 {
                    // Every key a reader sees must carry the value its writer wrote.
                    for kv in db.scan("t0".."t9").unwrap() {
                        let (k, v) = kv.unwrap();
                        let k = String::from_utf8(k).unwrap();
                        let (t, i) = k[1..].split_once('-').unwrap();
                        let i: usize = i.parse().unwrap();
                        assert_eq!(v, format!("{t}:{i}").into_bytes());
                    }
                }
            })
        })
        .collect();
    for h in writers.into_iter().chain(readers) {
        h.join().unwrap();
    }
    for t in 0..4 {
        for i in (0..2_000).step_by(97) {
            assert_eq!(
                db.get(format!("t{t}-{i:05}")).unwrap(),
                Some(format!("{t}:{i}").into_bytes())
            );
        }
    }
    assert_eq!(db.iter().unwrap().count(), 8_000);

    db.wait_for_compactions().unwrap();
    let stats = db.stats();
    assert!(
        stats.levels[0].files < 2,
        "L0 drained below its trigger: {stats}"
    );
    drop(db);

    // Reopen after background work was interrupted by the drop.
    let db = Db::open(dir.path(), small_opts(true)).unwrap();
    assert_eq!(db.iter().unwrap().count(), 8_000);
}

#[test]
fn reopen_with_different_level_count_is_rejected_if_data_would_be_lost() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), small_opts(false)).unwrap();
        for i in 0..3_000 {
            db.put(key(i), [0u8; 32]).unwrap();
        }
        db.compact().unwrap();
    }
    let opts = Options {
        num_levels: 2,
        ..small_opts(false)
    };
    match Db::open(dir.path(), opts) {
        Err(Error::InvalidArgument(_)) => {}
        Err(e) => panic!("unexpected error {e}"),
        Ok(db) => {
            // Acceptable only if all data happened to live in L0/L1.
            assert_eq!(db.iter().unwrap().count(), 3_000);
        }
    }
}
