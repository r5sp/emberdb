//! emberdb is an LSM-tree key-value storage engine.
//!
//! Writes go to a checksummed write-ahead log and an in-memory memtable. Full memtables
//! are flushed to immutable, bloom-filtered SSTables, which a leveled compaction process
//! merges into progressively larger sorted runs. See the README for an architecture
//! overview and `docs/FORMAT.md` for the on-disk format.
//!
//! ```
//! use emberdb::{Db, Options};
//!
//! # let dir = tempfile::tempdir().unwrap();
//! let db = Db::open(dir.path(), Options::default())?;
//! db.put("k1", "v1")?;
//! db.put("k2", "v2")?;
//! db.delete("k1")?;
//! assert_eq!(db.get("k1")?, None);
//! assert_eq!(db.get("k2")?, Some(b"v2".to_vec()));
//!
//! db.flush()?;
//! drop(db);
//! let db = Db::open(dir.path(), Options::default())?;
//! assert_eq!(db.iter()?.count(), 1);
//! # Ok::<(), emberdb::Error>(())
//! ```

#![warn(missing_docs)]

mod coding;
mod compaction;
mod db;
mod error;
mod iterator;
mod manifest;
mod memtable;
mod options;
mod sstable;
mod types;
mod version;
mod wal;

pub use db::{Db, DbIterator, LevelStats, Stats};
pub use error::{Error, Result};
pub use options::Options;
