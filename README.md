# emberdb

[![CI](https://github.com/r5sp/emberdb/actions/workflows/ci.yml/badge.svg)](https://github.com/r5sp/emberdb/actions/workflows/ci.yml)

An LSM-tree key-value store in Rust, written from scratch. WAL, memtable, SSTables with
bloom filters, leveled compaction, and a manifest that gets swapped atomically.
Only runtime dep is `crc32fast`, everything else lives in this crate.

```rust
use emberdb::{Db, Options};

fn main() -> emberdb::Result<()> {
    let db = Db::open("/tmp/ember", Options::default())?;
    db.put("user:1", "ada")?;
    db.put("user:2", "grace")?;
    db.delete("user:1")?;

    assert_eq!(db.get("user:2")?, Some(b"grace".to_vec()));
    for kv in db.scan("user:".."user;")? {
        let (key, value) = kv?;
        println!("{} = {}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
    }
    db.flush()?;    // memtable -> L0 SSTable
    db.compact()?;  // merge everything into the bottom level
    Ok(())
}
```

## how it works

```
put/delete -> WAL -> memtable --(full: freeze, new WAL)--> immutable --(bg flush)--> L0

L0    whole flushes, can overlap
L1    one sorted run, 16 MiB
L2    one sorted run, 160 MiB
...   down to L6

get: memtable -> immutable -> L0 newest first -> one table per level
```

Memtable hits `memtable_size` (4 MiB default), it gets frozen and the background thread
flushes it. If a second one fills before the first is flushed, the writer stalls and
helps finish the flush. That's what keeps memory bounded.

Point lookups check each candidate table's key range, then its bloom filter, then the
sparse index (one entry per 4 KiB block), and only then read one block. So at most one
block read per level, usually none. Scans are a k-way heap merge where the newest source
wins, and they hold `Arc`s to the table set they started with, so compactions deleting
files underneath them is fine.

Byte layout for logs, SSTables and the manifest is in [docs/FORMAT.md](docs/FORMAT.md).

## why leveled

I care more about predictable reads than write amp. Every level below L0 is one sorted
run, 10x the one above, so a lookup touches at most one table per level. The cost is
write amp: measured **2.97x** for sequential inserts and **5.15x** for uniformly random
ones (WAL + flush + compaction bytes). Size-tiered would probably get the random number
down but make lookups slower.

A couple of correctness things. L0 compaction always takes all L0 tables, otherwise an
older L0 version of a key could sit above a newer one in L1. And tombstones only get
dropped when nothing deeper overlaps, or an old value comes back. One table with no
overlap below is just a manifest edit (the sequential bench did 23 of those).

## bloom filters

My own. One 64-bit hash per key, `k = floor(bits_per_key * ln 2)` probes via
Kirsch-Mitzenmacher double hashing. Measured FP rate: 5.6% at 6 bits/key, 0.84% at 10,
0.05% at 16. About 1.25 MB per million keys at 10 bits. With vs without (1M keys,
`--bloom-bits 0`):

| workload | 10 bits/key | filters off | speedup |
|---|--:|--:|--:|
| `readmissing` (absent keys inside table ranges) | 4,795,004 ops/s | 425,131 ops/s | 11.3x |
| `readrandom` on the randomly loaded DB | 569,672 ops/s | 97,381 ops/s | 5.8x |

The random DB gains more because overlapping L0 tables plus several levels all cover
each key, and ~37% of its lookups are for keys never written.

## durability

Default `sync_writes: false` survives a process crash but not power loss. `true` fsyncs
every write. WAL replay stops at the first truncated or bad-CRC record. A crash
mid-append can only hurt the last record, so you keep exactly what was acknowledged. A
record that passes CRC but won't decode is reported as corruption, not skipped.

The MANIFEST is a full snapshot rewritten on every change (tmp, fsync, rename, fsync dir).
LevelDB appends edits instead. Snapshot is simpler and cheap at this size. Inputs only get
deleted once the manifest dropping them is durable, and a `LOCK` file stops two handles
opening the same dir.

## concurrency

`Db` is `Send + Sync`. One `RwLock` over the memtables, WAL and current `Arc<Version>`,
held briefly. SSTable reads happen outside it with `pread`. Flush and compaction install
add/remove edits, so they can run at the same time. No per-key sequence numbers, which
keeps the format simple but means no multi-version snapshot reads.

## tests

```sh
cargo test                       # 65 tests: unit, integration, property, recovery, doc
PROPTEST_CASES=500 cargo test --release --test model   # longer randomized run
cargo clippy --all-targets -- -D warnings
```

`tests/model.rs` runs random sequences of up to 300 ops (put/delete/get/scan/flush/
compact/reopen) against emberdb and a `BTreeMap`, with tiny memtable and level sizes so
it hits lots of compactions. To check it catches real bugs I injected one (always dropping
tombstones in compaction) and it found it and shrank it to a minimal sequence.
`tests/recovery.rs` truncates the WAL at every record boundary and one byte either side,
at 64 random offsets, and with a flipped byte, and checks the result against a model.

## numbers

```sh
cargo run --release --example bench                     # defaults below
cargo run --release --example bench -- --bloom-bits 0   # filter ablation
```

1,000,000 entries, 16-byte keys, 100-byte values, default `Options`, single-threaded
client, no fsync except `fillsync`. Apple M5 (10 cores, 16 GB RAM, internal SSD, APFS,
macOS 26.4), Rust 1.98.1.

| benchmark | throughput | latency | notes |
|---|--:|--:|---|
| `fillseq` | 474,204 ops/s | 2.11 us/op | 52.5 MB/s of user data |
| `fillrandom` | 408,358 ops/s | 2.45 us/op | 45.2 MB/s of user data |
| `fillsync` | 276 ops/s | 3,626 us/op | fsync per write |
| `readrandom` | 418,816 ops/s | 2.39 us/op | sequential-load DB, every key present |
| `readmissing` | 4,795,004 ops/s | 0.21 us/op | absent keys inside table ranges |
| `readseq` (full scan) | 9,985,957 ops/s | 0.10 us/op | 1,105 MB/s |
| `readrandom` (random DB) | 569,672 ops/s | 1.76 us/op | ~37% misses |
| `compact` (random DB) | 0.21 s | | full merge of ~69 MB |
| `readrandom` (compacted) | 631,428 ops/s | 1.58 us/op | single level |

| write amp | user | WAL | flush | compaction | total/user |
|---|--:|--:|--:|--:|--:|
| sequential load | 116 MB | 127 MB | 109 MB | 109 MB | 2.97x |
| random load + full compaction | 116 MB | 127 MB | 109 MB | 362 MB | 5.15x |

Runs varied about 5-10%. The ~110 MB dataset fits in page cache, so reads measure CPU
and syscalls, not the SSD (and there's no block cache). `fillsync` is slow because
`File::sync_data` on macOS does `F_FULLFSYNC`, about 3.6 ms here.

## stuff I'd do next

- block cache
- per-block LZ4 or Snappy
- sequence numbers, for real snapshots and `WriteBatch`
- group commit
- move non-overlapping L0 tables down without rewriting them

Tested on Linux and macOS. Windows would need ref-counted deletion, since compaction
unlinks files open iterators might still be reading.

## cli

```sh
cargo run --example cli -- /tmp/ember put hello world
cargo run --example cli -- /tmp/ember get hello
cargo run --example cli -- /tmp/ember load 100000
cargo run --example cli -- /tmp/ember scan key00000010 key00000020
cargo run --example cli -- /tmp/ember compact
```

MIT, see [LICENSE](LICENSE).
