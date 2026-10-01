//! emberdb is an LSM-tree key-value storage engine.
//!
//! Writes go to a checksummed write-ahead log and an in-memory memtable. Full memtables
//! are flushed to immutable, bloom-filtered SSTables, which a leveled compaction process
//! merges into progressively larger sorted runs. See the README for an architecture
//! overview and `docs/FORMAT.md` for the on-disk format.

#![warn(missing_docs)]

mod coding;
mod error;
mod options;
mod types;
mod wal;

pub use error::{Error, Result};
pub use options::Options;
