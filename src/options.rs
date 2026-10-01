//! Tuning knobs for a database instance.

use crate::error::{Error, Result};

/// Configuration for [`Db::open`](crate::Db::open).
///
/// The defaults are reasonable for a general-purpose workload. Tests use much smaller
/// sizes to force frequent flushes and compactions.
#[derive(Clone, Debug)]
pub struct Options {
    /// Create the database directory if it does not exist.
    pub create_if_missing: bool,
    /// `fsync` the write-ahead log after every write. When `false`, each write is still
    /// handed to the OS before `put`/`delete` returns, so it survives a process crash but
    /// may be lost on power failure or kernel panic.
    pub sync_writes: bool,
    /// Approximate memtable size in bytes at which it is frozen and flushed to an L0 SSTable.
    pub memtable_size: usize,
    /// Target uncompressed size of an SSTable data block.
    pub block_size: usize,
    /// Number of entries between prefix-compression restart points in a block.
    pub block_restart_interval: usize,
    /// Bloom filter bits per key. `0` disables filters. 10 bits gives roughly a 1%
    /// false-positive rate.
    pub bloom_bits_per_key: usize,
    /// Number of L0 files that triggers an L0 -> L1 compaction.
    pub l0_compaction_trigger: usize,
    /// Maximum total size of level 1, in bytes.
    pub level1_max_bytes: u64,
    /// Each level below L1 may hold this many times more bytes than the level above it.
    pub level_size_multiplier: u64,
    /// Target size of SSTables produced by compaction.
    pub target_file_size: u64,
    /// Total number of levels, including L0.
    pub num_levels: usize,
    /// Run flushes and compactions on a dedicated background thread. When `false` they
    /// run inline on the writing thread, which makes behaviour fully deterministic.
    pub background_compaction: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            create_if_missing: true,
            sync_writes: false,
            memtable_size: 4 << 20,
            block_size: 4 << 10,
            block_restart_interval: 16,
            bloom_bits_per_key: 10,
            l0_compaction_trigger: 4,
            level1_max_bytes: 16 << 20,
            level_size_multiplier: 10,
            target_file_size: 4 << 20,
            num_levels: 7,
            background_compaction: true,
        }
    }
}

impl Options {
    pub(crate) fn validate(&self) -> Result<()> {
        let check = |ok: bool, msg: &str| {
            if ok {
                Ok(())
            } else {
                Err(Error::InvalidArgument(msg.to_string()))
            }
        };
        check(self.memtable_size > 0, "memtable_size must be > 0")?;
        check(self.block_size >= 64, "block_size must be >= 64")?;
        check(
            self.block_restart_interval >= 1,
            "block_restart_interval must be >= 1",
        )?;
        check(
            self.bloom_bits_per_key <= 64,
            "bloom_bits_per_key must be <= 64",
        )?;
        check(
            self.l0_compaction_trigger >= 1,
            "l0_compaction_trigger must be >= 1",
        )?;
        check(self.level1_max_bytes > 0, "level1_max_bytes must be > 0")?;
        check(
            self.level_size_multiplier >= 2,
            "level_size_multiplier must be >= 2",
        )?;
        check(self.target_file_size > 0, "target_file_size must be > 0")?;
        check(
            (2..=32).contains(&self.num_levels),
            "num_levels must be between 2 and 32",
        )?;
        Ok(())
    }

    /// Maximum number of bytes level `level` (>= 1) may hold before it is compacted.
    pub(crate) fn max_bytes_for_level(&self, level: usize) -> u64 {
        debug_assert!(level >= 1);
        let mut bytes = self.level1_max_bytes;
        for _ in 1..level {
            bytes = bytes.saturating_mul(self.level_size_multiplier);
        }
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        Options::default().validate().unwrap();
    }

    #[test]
    fn rejects_bad_options() {
        let opts = Options {
            num_levels: 1,
            ..Options::default()
        };
        assert!(opts.validate().is_err());
        let opts = Options {
            block_size: 8,
            ..Options::default()
        };
        assert!(opts.validate().is_err());
    }

    #[test]
    fn level_sizes_grow_geometrically() {
        let opts = Options::default();
        assert_eq!(opts.max_bytes_for_level(1), 16 << 20);
        assert_eq!(opts.max_bytes_for_level(2), 160 << 20);
        assert_eq!(opts.max_bytes_for_level(3), 1600 << 20);
    }
}
