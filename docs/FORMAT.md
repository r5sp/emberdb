# emberdb on-disk format

All multi-byte integers are little-endian. `varint` is unsigned LEB128 (7 bits per
byte, high bit set on every byte except the last). CRC32 is the IEEE polynomial
(`crc32fast`).

A database directory contains:

| File            | Purpose                                                        |
|-----------------|----------------------------------------------------------------|
| `NNNNNN.log`    | Write-ahead log for one memtable generation                    |
| `NNNNNN.sst`    | Immutable sorted string table                                  |
| `MANIFEST`      | Current set of live SSTables and their levels                  |
| `MANIFEST.tmp`  | Transient; a manifest being written. Deleted on open if found. |
| `LOCK`          | Holds an advisory exclusive lock while the database is open   |

`NNNNNN` is a decimal file number, zero-padded to six digits. Log and table numbers
share a single monotonically increasing counter, so a higher number is always newer.

---

## Write-ahead log (`*.log`)

A log is a sequence of records with no file header:

```
+------------+------------+------------------------+
| crc32: u32 | len: u32   | payload: [u8; len]     |
+------------+------------+------------------------+

payload := tag: u8          0 = delete (tombstone), 1 = put
           key_len: varint
           key: [u8; key_len]
           value_len: varint  (0 for a delete)
           value: [u8; value_len]
```

The CRC covers the 4-byte `len` field followed by the payload, so a corrupted length
cannot cause a valid-looking read of the wrong span.

**Recovery semantics.** Replay applies records in order and stops at the first record
that is incomplete (the file ends inside its header or payload) or whose CRC does not
match. That record and everything after it are discarded. A crash during `write(2)`
can only damage the record being appended, so this recovers every write that was
acknowledged before the crash (all of them when `sync_writes` is enabled; all writes
that reached the OS otherwise). A record that passes its CRC but cannot be decoded is
reported as `Error::Corruption` rather than silently skipped.

After replay on open, the recovered memtable is immediately flushed to an L0 table and
the old logs are deleted, so torn bytes never persist past one restart.

---

## SSTable (`*.sst`)

```
+------------------+-------+
| data block 0     | crc32 |
+------------------+-------+
| ...              |       |
+------------------+-------+
| data block N     | crc32 |
+------------------+-------+
| filter block     | crc32 |
+------------------+-------+
| index block      | crc32 |
+------------------+-------+
| footer (56 bytes)        |
+--------------------------+
```

Every block is followed by a 4-byte CRC32 of its contents. Block handles (offset,
length) refer to the contents only; the CRC immediately follows.

### Data and index blocks

Both use the same prefix-compressed sorted block encoding. Keys within a block are
strictly increasing.

```
block   := entry* restart: u32 * num_restarts  num_restarts: u32

entry   := shared: varint        bytes shared with the previous key
           unshared: varint      length of the key suffix stored here
           tag: u8               0 = tombstone, 1 = value
           value_len: varint
           key_suffix: [u8; unshared]
           value: [u8; value_len]
```

Every `block_restart_interval` entries (default 16) an entry is written with
`shared = 0` and its byte offset is appended to the restart array. A lookup
binary-searches the restart array for the last restart key `< target`, then scans
forward at most one interval. Restart offset 0 is always present, so `num_restarts >= 1`.

Data blocks are cut when their estimated encoded size reaches `block_size`
(default 4 KiB).

The **index block** has one entry per data block (a *sparse* index). Its key is the
last key in that data block and its value is a 16-byte block handle:

```
handle := offset: u64  len: u64
```

The data block that may contain key `k` is the one referenced by the first index entry
whose key is `>= k`. If no such entry exists, `k` is greater than every key in the
table. The index uses a restart interval of 1 so each binary-search probe decodes a
single entry.

### Filter block

A single bloom filter over every key in the table:

```
filter := bits: [u8; m/8]  k: u8
```

- `m = max(64, num_keys * bits_per_key)`, rounded up to a whole byte.
- `k = clamp(floor(bits_per_key * ln 2), 1, 30)`.
- Bit positions for a key with 64-bit hash `h` are
  `(h + i * (rotl(h, 32) | 1)) mod m` for `i in 0..k` (Kirsch-Mitzenmacher double
  hashing). Bit `p` is bit `p % 8` of byte `p / 8`.
- `h` is emberdb's own 64-bit hash (`sstable::bloom::hash64`): 8-byte chunks mixed with
  the SplitMix64 finalizer, seeded with the key length.

An empty filter block (filters disabled with `bloom_bits_per_key = 0`) matches every
key, as does a filter whose `k` byte is 0 or greater than 30.

### Footer

Fixed 56 bytes at the end of the file:

| Offset | Size | Field                                         |
|-------:|-----:|-----------------------------------------------|
|      0 |    8 | index block offset                            |
|      8 |    8 | index block length                            |
|     16 |    8 | filter block offset                           |
|     24 |    8 | filter block length                           |
|     32 |    8 | number of entries (values + tombstones)       |
|     40 |    4 | format version (currently `1`)                |
|     44 |    4 | CRC32 of bytes `0..44`                        |
|     48 |    8 | magic: ASCII `EMBERDB1`                       |

A reader validates magic, footer CRC and version before trusting any offset, and
bounds-checks every block handle against the file size.

---

## MANIFEST

The manifest is a full snapshot of the current version, rewritten on every change:

```
manifest := magic: "EMBMANI1"  body_len: u32  body_crc32: u32  body

body     := next_file_number: varint
            log_number: varint
            num_levels: varint
            level * num_levels

level    := num_files: varint  file_meta * num_files

file_meta := number: varint
             file_size: varint
             num_entries: varint
             smallest_key_len: varint  smallest_key
             largest_key_len: varint   largest_key
```

- `log_number`: logs with a number `>= log_number` may contain writes not yet in any
  SSTable and must be replayed on open. Older logs are obsolete.
- `next_file_number`: a lower bound for the next file number to allocate. On open the
  engine also takes the maximum with every file number found on disk.
- L0 files may overlap and are ordered newest (highest number) first. Files in L1 and
  deeper are non-overlapping and ordered by smallest key.

**Atomic update.** A new manifest is written to `MANIFEST.tmp`, fsynced, renamed over
`MANIFEST`, and then the directory is fsynced so the rename itself is durable. `rename`
is atomic on POSIX filesystems, so a reader observes either the complete old manifest
or the complete new one. SSTables are fsynced before the manifest that references them
is written, and obsolete files are deleted only after the manifest that drops them is
durable.

**Orphans.** A crash can leave an SSTable that was written but never referenced (for
example, mid-compaction). On open, any `*.sst` not listed in the manifest, any log
older than `log_number`, and any `MANIFEST.tmp` is deleted.
