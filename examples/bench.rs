//! Throughput benchmark modelled on LevelDB's `db_bench`.
//!
//! ```text
//! cargo run --release --example bench -- [--num N] [--reads N] [--value-size BYTES]
//!     [--sync-ops N] [--bloom-bits N] [--dir PATH]
//! ```
//!
//! Keys are 16-byte zero-padded decimal integers; values are pseudo-random bytes.
//! Every phase runs single-threaded with default `Options` (4 MiB memtable, 4 KiB blocks,
//! 10 bloom bits per key, background compaction on, no fsync per write).

use std::path::PathBuf;
use std::time::{Duration, Instant};

use emberdb::{Db, Options};

struct Config {
    num: u64,
    reads: u64,
    value_size: usize,
    sync_ops: u64,
    bloom_bits: usize,
    dir: Option<PathBuf>,
}

fn parse_args() -> Config {
    let mut cfg = Config {
        num: 1_000_000,
        reads: 1_000_000,
        value_size: 100,
        sync_ops: 1_000,
        bloom_bits: 10,
        dir: None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || args.next().unwrap_or_else(|| usage(&arg));
        match arg.as_str() {
            "--num" => cfg.num = value().parse().unwrap_or_else(|_| usage("--num")),
            "--reads" => cfg.reads = value().parse().unwrap_or_else(|_| usage("--reads")),
            "--value-size" => {
                cfg.value_size = value().parse().unwrap_or_else(|_| usage("--value-size"))
            }
            "--sync-ops" => cfg.sync_ops = value().parse().unwrap_or_else(|_| usage("--sync-ops")),
            "--bloom-bits" => {
                cfg.bloom_bits = value().parse().unwrap_or_else(|_| usage("--bloom-bits"))
            }
            "--dir" => cfg.dir = Some(PathBuf::from(value())),
            _ => usage(&arg),
        }
    }
    cfg
}

fn usage(bad: &str) -> ! {
    eprintln!("unrecognised or incomplete argument: {bad}");
    eprintln!(
        "usage: bench [--num N] [--reads N] [--value-size BYTES] [--sync-ops N] [--bloom-bits N] [--dir PATH]"
    );
    std::process::exit(2);
}

/// SplitMix64: tiny, fast, and good enough to scatter keys.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

fn key(i: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k.copy_from_slice(format!("{i:016}").as_bytes());
    k
}

fn value(rng: &mut Rng, size: usize) -> Vec<u8> {
    (0..size).map(|_| rng.next() as u8).collect()
}

fn report(name: &str, ops: u64, bytes: u64, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();
    let mbps = if bytes > 0 {
        format!("{:8.1} MB/s", bytes as f64 / (1024.0 * 1024.0) / secs)
    } else {
        String::from("           ")
    };
    println!(
        "{name:<22} {:>12.0} ops/s  {:>8.3} us/op  {mbps}  ({ops} ops in {secs:.2}s)",
        ops as f64 / secs,
        secs * 1e6 / ops as f64,
    );
}

fn fresh_dir(base: &Option<PathBuf>, name: &str) -> (Option<tempfile::TempDir>, PathBuf) {
    match base {
        Some(base) => {
            let path = base.join(name);
            let _ = std::fs::remove_dir_all(&path);
            (None, path)
        }
        None => {
            let tmp = tempfile::tempdir().expect("create temp dir");
            let path = tmp.path().join(name);
            (Some(tmp), path)
        }
    }
}

fn main() -> emberdb::Result<()> {
    let cfg = parse_args();
    let entry_bytes = 16 + cfg.value_size as u64;
    let opts = Options {
        bloom_bits_per_key: cfg.bloom_bits,
        ..Options::default()
    };
    println!(
        "emberdb bench: {} entries, 16-byte keys, {}-byte values, {} reads, {} bloom bits/key",
        cfg.num, cfg.value_size, cfg.reads, cfg.bloom_bits
    );
    println!("{}", "-".repeat(96));

    let mut rng = Rng(0x00C0_FFEE);
    let values: Vec<Vec<u8>> = (0..1024).map(|_| value(&mut rng, cfg.value_size)).collect();

    // fillseq
    let (_seq_tmp, seq_path) = fresh_dir(&cfg.dir, "seq");
    let seq = Db::open(&seq_path, opts.clone())?;
    let start = Instant::now();
    for i in 0..cfg.num {
        seq.put(key(i), &values[(i % 1024) as usize])?;
    }
    report("fillseq", cfg.num, cfg.num * entry_bytes, start.elapsed());

    // fillrandom
    let (_rand_tmp, rand_path) = fresh_dir(&cfg.dir, "random");
    let random = Db::open(&rand_path, opts.clone())?;
    let start = Instant::now();
    for i in 0..cfg.num {
        random.put(key(rng.next() % cfg.num), &values[(i % 1024) as usize])?;
    }
    report(
        "fillrandom",
        cfg.num,
        cfg.num * entry_bytes,
        start.elapsed(),
    );

    // fillsync: every write fsyncs the WAL.
    if cfg.sync_ops > 0 {
        let (_sync_tmp, sync_path) = fresh_dir(&cfg.dir, "sync");
        let sync = Db::open(
            &sync_path,
            Options {
                sync_writes: true,
                ..opts.clone()
            },
        )?;
        let start = Instant::now();
        for i in 0..cfg.sync_ops {
            sync.put(key(rng.next() % cfg.num), &values[(i % 1024) as usize])?;
        }
        report(
            "fillsync",
            cfg.sync_ops,
            cfg.sync_ops * entry_bytes,
            start.elapsed(),
        );
    }

    // Reads against the sequentially loaded database (every key present). Let background
    // compaction settle first so it does not compete with the read phases.
    seq.flush()?;
    seq.wait_for_compactions()?;
    let start = Instant::now();
    let mut found = 0u64;
    for _ in 0..cfg.reads {
        if seq.get(key(rng.next() % cfg.num))?.is_some() {
            found += 1;
        }
    }
    report("readrandom", cfg.reads, 0, start.elapsed());
    assert_eq!(found, cfg.reads, "every key was written");

    let start = Instant::now();
    for _ in 0..cfg.reads {
        // A 17-byte key sorts between two stored keys, so it falls inside table key ranges
        // and only the bloom filter can rule each table out without a block read.
        let mut k = key(rng.next() % cfg.num).to_vec();
        k.push(b'x');
        assert!(seq.get(&k)?.is_none());
    }
    report("readmissing", cfg.reads, 0, start.elapsed());

    let start = Instant::now();
    let mut n = 0u64;
    for kv in seq.iter()? {
        kv?;
        n += 1;
    }
    report("readseq (scan)", n, n * entry_bytes, start.elapsed());

    // Reads against the randomly loaded database, which has overlapping L0 files and
    // data spread across levels.
    random.flush()?;
    random.wait_for_compactions()?;
    let start = Instant::now();
    for _ in 0..cfg.reads {
        random.get(key(rng.next() % cfg.num))?;
    }
    report("readrandom (random db)", cfg.reads, 0, start.elapsed());

    let start = Instant::now();
    random.compact()?;
    println!(
        "{:<22} {:>12.2} s",
        "compact (random db)",
        start.elapsed().as_secs_f64()
    );
    let start = Instant::now();
    for _ in 0..cfg.reads {
        random.get(key(rng.next() % cfg.num))?;
    }
    report("readrandom (compacted)", cfg.reads, 0, start.elapsed());

    println!("{}", "-".repeat(96));
    println!("sequential-load db:\n{}\n", seq.stats());
    println!(
        "random-load db (after manual compaction):\n{}",
        random.stats()
    );
    Ok(())
}
