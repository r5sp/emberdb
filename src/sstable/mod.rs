//! Immutable sorted string tables.
//!
//! An SSTable is written once, sequentially, by [`TableBuilder`] and is read thereafter by
//! [`Table`]. Layout (see `docs/FORMAT.md` for byte-level detail):
//!
//! ```text
//! [data block 0][crc] ... [data block N][crc] [filter][crc] [index block][crc] [footer]
//! ```

mod block;
mod bloom;
mod builder;
mod table;

pub use builder::TableBuilder;
pub use table::Table;

/// Magic bytes at the end of every SSTable.
pub const TABLE_MAGIC: &[u8; 8] = b"EMBERDB1";
/// Format version recorded in the footer.
pub const FORMAT_VERSION: u32 = 1;
/// Fixed footer size in bytes.
pub const FOOTER_LEN: usize = 56;
/// Every block is followed by a CRC32 of its contents.
pub const BLOCK_TRAILER_LEN: usize = 4;

/// Location of a block within a table file (excluding its CRC trailer).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockHandle {
    pub offset: u64,
    pub len: u64,
}

impl BlockHandle {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16);
        out.extend_from_slice(&self.offset.to_le_bytes());
        out.extend_from_slice(&self.len.to_le_bytes());
        out
    }

    pub fn decode(buf: &[u8]) -> crate::Result<Self> {
        if buf.len() != 16 {
            return Err(crate::Error::corruption("bad block handle length"));
        }
        Ok(BlockHandle {
            offset: crate::coding::read_u64_le(buf, 0),
            len: crate::coding::read_u64_le(buf, 8),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::builder::TableInfo;
    use super::*;
    use crate::options::Options;
    use crate::types::{Entry, Value};
    use std::path::Path;
    use std::sync::Arc;

    fn small_opts() -> Options {
        Options {
            block_size: 256,
            ..Options::default()
        }
    }

    fn entries(n: usize) -> Vec<Entry> {
        (0..n)
            .map(|i| {
                let key = format!("k{:06}", i * 10).into_bytes();
                let value = if i % 7 == 3 {
                    Value::Delete
                } else {
                    Value::Put(format!("value-{i}-{}", "x".repeat(i % 50)).into_bytes())
                };
                (key, value)
            })
            .collect()
    }

    fn write_table(path: &Path, data: &[Entry], opts: &Options) -> TableInfo {
        let mut b = TableBuilder::create(path, opts).unwrap();
        for (k, v) in data {
            b.add(k, v).unwrap();
        }
        b.finish().unwrap()
    }

    #[test]
    fn point_lookups() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("1.sst");
        let data = entries(2_000);
        let info = write_table(&path, &data, &small_opts());
        assert_eq!(info.num_entries, 2_000);
        assert_eq!(info.smallest, data[0].0);
        assert_eq!(info.largest, data.last().unwrap().0);

        let table = Table::open(&path).unwrap();
        assert_eq!(table.file_size(), info.file_size);
        for (k, v) in &data {
            assert_eq!(table.get(k).unwrap().as_ref(), Some(v));
        }
        // Keys between, before and after the stored keys.
        for missing in [&b"k000005"[..], b"a", b"k", b"zzz", b"k019991"] {
            assert_eq!(table.get(missing).unwrap(), None);
        }
    }

    #[test]
    fn iteration_with_and_without_start_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("1.sst");
        let data = entries(1_000);
        write_table(&path, &data, &small_opts());
        let table = Arc::new(Table::open(&path).unwrap());

        let all: Vec<Entry> = table.iter(None).map(Result::unwrap).collect();
        assert_eq!(all, data);

        for start_idx in [0usize, 1, 37, 500, 999] {
            let start = data[start_idx].0.clone();
            let got: Vec<Entry> = table.iter(Some(start)).map(Result::unwrap).collect();
            assert_eq!(got, data[start_idx..].to_vec());
            // A start key between two stored keys begins at the successor.
            let mut between = data[start_idx].0.clone();
            between.push(b'5');
            let got: Vec<Entry> = table.iter(Some(between)).map(Result::unwrap).collect();
            assert_eq!(got, data[start_idx + 1..].to_vec());
        }
        assert_eq!(table.iter(Some(b"zzz".to_vec())).count(), 0);
    }

    #[test]
    fn bloom_filter_skips_absent_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("1.sst");
        write_table(&path, &entries(1_000), &small_opts());
        let table = Table::open(&path).unwrap();
        let rejected = (0..10_000)
            .filter(|i| !table.may_contain(format!("absent{i}").as_bytes()))
            .count();
        assert!(
            rejected > 9_700,
            "only {rejected} of 10000 absent keys filtered"
        );
    }

    #[test]
    fn works_without_bloom_filter() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("1.sst");
        let opts = Options {
            bloom_bits_per_key: 0,
            ..small_opts()
        };
        let data = entries(300);
        write_table(&path, &data, &opts);
        let table = Table::open(&path).unwrap();
        assert!(table.may_contain(b"absent"));
        assert_eq!(table.get(&data[42].0).unwrap().as_ref(), Some(&data[42].1));
    }

    #[test]
    fn rejects_out_of_order_and_empty_tables() {
        let dir = tempfile::tempdir().unwrap();
        let mut b = TableBuilder::create(&dir.path().join("1.sst"), &small_opts()).unwrap();
        b.add(b"b", &Value::Delete).unwrap();
        assert!(b.add(b"a", &Value::Delete).is_err());
        assert!(b.add(b"b", &Value::Delete).is_err());
        let b = TableBuilder::create(&dir.path().join("2.sst"), &small_opts()).unwrap();
        assert!(b.finish().is_err());
    }

    #[test]
    fn detects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("1.sst");
        let data = entries(500);
        write_table(&path, &data, &small_opts());
        let pristine = std::fs::read(&path).unwrap();

        // Flip a byte in the first data block: lookups in that block must fail loudly.
        let mut bytes = pristine.clone();
        bytes[10] ^= 0x01;
        std::fs::write(&path, &bytes).unwrap();
        let table = Table::open(&path).unwrap();
        assert!(matches!(
            table.get(&data[0].0),
            Err(crate::Error::Corruption(_))
        ));

        // Damaged magic or footer: open must fail.
        for at in [pristine.len() - 1, pristine.len() - FOOTER_LEN + 3] {
            let mut bytes = pristine.clone();
            bytes[at] ^= 0x01;
            std::fs::write(&path, &bytes).unwrap();
            assert!(matches!(
                Table::open(&path),
                Err(crate::Error::Corruption(_))
            ));
        }

        // Truncated file.
        std::fs::write(&path, &pristine[..20]).unwrap();
        assert!(Table::open(&path).is_err());
    }
}
