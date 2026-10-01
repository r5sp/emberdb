//! Core value representation shared by the memtable, WAL and SSTables.

/// A stored entry: either a live value or a tombstone recording a delete.
///
/// Tombstones must be persisted (rather than simply removing the key) because an older
/// version of the key may still live in a lower level of the tree. They are only dropped
/// by a compaction whose output is the bottom-most level containing the key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Put(Vec<u8>),
    Delete,
}

impl Value {
    /// Encoded kind tag used by the WAL and SSTable block formats.
    pub const TAG_DELETE: u8 = 0;
    pub const TAG_PUT: u8 = 1;

    pub fn tag(&self) -> u8 {
        match self {
            Value::Put(_) => Self::TAG_PUT,
            Value::Delete => Self::TAG_DELETE,
        }
    }

    pub fn payload(&self) -> &[u8] {
        match self {
            Value::Put(v) => v,
            Value::Delete => &[],
        }
    }

    pub fn into_option(self) -> Option<Vec<u8>> {
        match self {
            Value::Put(v) => Some(v),
            Value::Delete => None,
        }
    }

    pub fn is_delete(&self) -> bool {
        matches!(self, Value::Delete)
    }
}

/// A key/value pair as produced by internal iterators.
pub type Entry = (Vec<u8>, Value);
