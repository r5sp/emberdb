//! Model-based property tests: random operation sequences are applied both to emberdb
//! and to a `BTreeMap`, and every observable result must agree, including across
//! flushes, compactions and reopens.

use std::collections::BTreeMap;
use std::ops::Bound;

use emberdb::{Db, Options};
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum Op {
    Put(u8, Vec<u8>),
    Delete(u8),
    Get(u8),
    Scan(Bound<u8>, Bound<u8>),
    Flush,
    Compact,
    Reopen,
}

fn key(k: u8) -> Vec<u8> {
    format!("key-{k:03}").into_bytes()
}

fn bound_strategy() -> impl Strategy<Value = Bound<u8>> {
    prop_oneof![
        1 => Just(Bound::Unbounded),
        3 => (0u8..80).prop_map(Bound::Included),
        3 => (0u8..80).prop_map(Bound::Excluded),
    ]
}

fn op_strategy() -> impl Strategy<Value = Op> {
    // A small key space makes overwrites, deletes of live keys and shadowing common.
    let k = 0u8..80;
    prop_oneof![
        40 => (k.clone(), prop::collection::vec(any::<u8>(), 0..48)).prop_map(|(k, v)| Op::Put(k, v)),
        15 => k.clone().prop_map(Op::Delete),
        20 => k.prop_map(Op::Get),
        10 => (bound_strategy(), bound_strategy()).prop_map(|(s, e)| Op::Scan(s, e)),
        4 => Just(Op::Flush),
        2 => Just(Op::Compact),
        4 => Just(Op::Reopen),
    ]
}

fn tiny_opts(background: bool) -> Options {
    Options {
        memtable_size: 512,
        block_size: 64,
        block_restart_interval: 4,
        bloom_bits_per_key: 8,
        l0_compaction_trigger: 2,
        level1_max_bytes: 1024,
        level_size_multiplier: 2,
        target_file_size: 512,
        num_levels: 4,
        background_compaction: background,
        ..Options::default()
    }
}

fn map_bound(b: &Bound<u8>) -> Bound<Vec<u8>> {
    match b {
        Bound::Included(k) => Bound::Included(key(*k)),
        Bound::Excluded(k) => Bound::Excluded(key(*k)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn model_scan(
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    // BTreeMap::range panics on inverted ranges; the engine returns nothing.
    let empty = match (&start, &end) {
        (Bound::Included(s), Bound::Included(e)) => s > e,
        (Bound::Included(s) | Bound::Excluded(s), Bound::Included(e) | Bound::Excluded(e)) => {
            s >= e
        }
        _ => false,
    };
    if empty {
        return Vec::new();
    }
    model
        .range((start, end))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn run(ops: Vec<Op>, background: bool) -> Result<(), TestCaseError> {
    let dir = tempfile::tempdir().unwrap();
    let opts = tiny_opts(background);
    let mut db = Some(Db::open(dir.path(), opts.clone()).unwrap());
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    for (step, op) in ops.into_iter().enumerate() {
        let d = db.as_ref().unwrap();
        match op {
            Op::Put(k, v) => {
                d.put(key(k), &v).unwrap();
                model.insert(key(k), v);
            }
            Op::Delete(k) => {
                d.delete(key(k)).unwrap();
                model.remove(&key(k));
            }
            Op::Get(k) => {
                prop_assert_eq!(
                    d.get(key(k)).unwrap(),
                    model.get(&key(k)).cloned(),
                    "step {}",
                    step
                );
            }
            Op::Scan(s, e) => {
                let (s, e) = (map_bound(&s), map_bound(&e));
                let got: Vec<_> = d
                    .scan((s.clone(), e.clone()))
                    .unwrap()
                    .map(Result::unwrap)
                    .collect();
                prop_assert_eq!(got, model_scan(&model, s, e), "step {}", step);
            }
            Op::Flush => d.flush().unwrap(),
            Op::Compact => d.compact().unwrap(),
            Op::Reopen => {
                drop(db.take());
                db = Some(Db::open(dir.path(), opts.clone()).unwrap());
            }
        }
    }

    // Final full comparison, before and after one more reopen.
    for _ in 0..2 {
        let d = db.as_ref().unwrap();
        let all: Vec<_> = d.iter().unwrap().map(Result::unwrap).collect();
        let expected: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        prop_assert_eq!(all, expected);
        drop(db.take());
        db = Some(Db::open(dir.path(), opts.clone()).unwrap());
    }
    Ok(())
}

fn cases(default: u32) -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases(32)))]

    #[test]
    fn matches_btreemap_model(ops in prop::collection::vec(op_strategy(), 1..300)) {
        run(ops, false)?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases(8)))]

    #[test]
    fn matches_btreemap_model_with_background_compaction(
        ops in prop::collection::vec(op_strategy(), 1..300)
    ) {
        run(ops, true)?;
    }
}
