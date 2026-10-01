//! Little-endian fixed-width and LEB128 varint encoding helpers.

use crate::error::{Error, Result};

/// Appends `v` as an unsigned LEB128 varint (1..=10 bytes).
pub fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

/// Decodes a varint from the front of `buf`, returning the value and the number of
/// bytes consumed, or `None` if the input is truncated or overlong.
pub fn get_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut result = 0u64;
    let mut shift = 0u32;
    for (i, &b) in buf.iter().enumerate() {
        if shift > 63 {
            return None;
        }
        result |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Some((result, i + 1));
        }
        shift += 7;
    }
    None
}

/// Appends a length-prefixed byte string.
pub fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    put_varint(buf, bytes.len() as u64);
    buf.extend_from_slice(bytes);
}

/// A bounds-checked cursor for decoding the formats in this crate. Every method returns
/// `Error::Corruption` rather than panicking on malformed input.
pub struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
    what: &'static str,
}

impl<'a> Decoder<'a> {
    pub fn new(buf: &'a [u8], what: &'static str) -> Self {
        Decoder { buf, pos: 0, what }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    fn err(&self, detail: &str) -> Error {
        Error::corruption(format!("{}: {} at offset {}", self.what, detail, self.pos))
    }

    pub fn varint(&mut self) -> Result<u64> {
        let (v, n) = get_varint(&self.buf[self.pos..]).ok_or_else(|| self.err("bad varint"))?;
        self.pos += n;
        Ok(v)
    }

    pub fn varint_usize(&mut self) -> Result<usize> {
        let v = self.varint()?;
        usize::try_from(v).map_err(|_| self.err("length overflows usize"))
    }

    pub fn u8(&mut self) -> Result<u8> {
        let b = *self
            .buf
            .get(self.pos)
            .ok_or_else(|| self.err("unexpected end of input"))?;
        self.pos += 1;
        Ok(b)
    }

    pub fn u64_le(&mut self) -> Result<u64> {
        let bytes = self.slice(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().expect("slice of len 8")))
    }

    pub fn slice(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .filter(|&end| end <= self.buf.len())
            .ok_or_else(|| self.err("length exceeds input"))?;
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.varint_usize()?;
        self.slice(len)
    }
}

pub fn read_u32_le(buf: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(buf[at..at + 4].try_into().expect("slice of len 4"))
}

pub fn read_u64_le(buf: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(buf[at..at + 8].try_into().expect("slice of len 8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        let values = [0u64, 1, 127, 128, 300, 16_383, 16_384, u32::MAX as u64, u64::MAX];
        for &v in &values {
            let mut buf = Vec::new();
            put_varint(&mut buf, v);
            assert_eq!(get_varint(&buf), Some((v, buf.len())), "value {v}");
        }
    }

    #[test]
    fn varint_rejects_truncated_and_overlong() {
        let mut buf = Vec::new();
        put_varint(&mut buf, u64::MAX);
        assert_eq!(get_varint(&buf[..buf.len() - 1]), None);
        assert_eq!(get_varint(&[0xff; 11]), None);
    }

    #[test]
    fn decoder_reports_corruption_instead_of_panicking() {
        let mut buf = Vec::new();
        put_varint(&mut buf, 100); // claims 100 bytes follow
        buf.extend_from_slice(b"short");
        let mut d = Decoder::new(&buf, "test");
        assert!(matches!(d.bytes(), Err(Error::Corruption(_))));
        let mut d = Decoder::new(&[], "test");
        assert!(d.u8().is_err());
        assert!(d.is_empty());
    }
}
