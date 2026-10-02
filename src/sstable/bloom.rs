//! Bloom filter used to skip SSTables that cannot contain a key.
//!
//! A filter with `m` bits, `n` keys and `k` probes has a false-positive rate of roughly
//! `(1 - e^(-k*n/m))^k`, minimised at `k = (m/n) * ln 2`. Probe positions use the
//! Kirsch-Mitzenmacher double-hashing scheme, `g_i(x) = h1(x) + i * h2(x)`, so only one
//! 64-bit hash is computed per key.
//!
//! Serialized form: `bit array | k u8`. An empty buffer means "no filter" and matches
//! every key.

/// Accumulates key hashes and serializes a filter for them.
pub struct BloomBuilder {
    bits_per_key: usize,
    hashes: Vec<u64>,
}

impl BloomBuilder {
    pub fn new(bits_per_key: usize) -> Self {
        BloomBuilder {
            bits_per_key,
            hashes: Vec::new(),
        }
    }

    pub fn add_key(&mut self, key: &[u8]) {
        if self.bits_per_key > 0 {
            self.hashes.push(hash64(key));
        }
    }

    /// Serializes the filter. Returns an empty buffer if filters are disabled.
    pub fn finish(&self) -> Vec<u8> {
        if self.bits_per_key == 0 {
            return Vec::new();
        }
        let k = num_probes(self.bits_per_key);
        // A small floor keeps the false-positive rate sane for tiny tables.
        let nbits = (self.hashes.len() * self.bits_per_key).max(64);
        let nbytes = nbits.div_ceil(8);
        let nbits = (nbytes * 8) as u64;

        let mut out = vec![0u8; nbytes + 1];
        for &h in &self.hashes {
            for pos in probes(h, k, nbits) {
                out[(pos / 8) as usize] |= 1 << (pos % 8);
            }
        }
        out[nbytes] = k as u8;
        out
    }
}

/// A read-only view of a serialized filter.
pub struct BloomFilter {
    data: Vec<u8>,
}

impl BloomFilter {
    pub fn new(data: Vec<u8>) -> Self {
        BloomFilter { data }
    }

    /// Returns `false` only if `key` is definitely not in the set.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.data.len() < 2 {
            return true;
        }
        let nbytes = self.data.len() - 1;
        let k = u32::from(self.data[nbytes]);
        if k == 0 || k > 30 {
            // Unknown encoding: err on the side of reading the table.
            return true;
        }
        let nbits = (nbytes * 8) as u64;
        probes(hash64(key), k, nbits)
            .all(|pos| self.data[(pos / 8) as usize] & (1 << (pos % 8)) != 0)
    }
}

fn num_probes(bits_per_key: usize) -> u32 {
    ((bits_per_key as f64 * std::f64::consts::LN_2) as u32).clamp(1, 30)
}

fn probes(h: u64, k: u32, nbits: u64) -> impl Iterator<Item = u64> {
    let h1 = h;
    // Force h2 odd so successive probes never collapse onto the same bit cycle.
    let h2 = h.rotate_left(32) | 1;
    (0..u64::from(k)).map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) % nbits)
}

/// A fast, well-mixed 64-bit hash (not cryptographic). Processes eight bytes at a time
/// and finishes with the SplitMix64 avalanche step.
pub fn hash64(data: &[u8]) -> u64 {
    const P: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut h: u64 = 0xcbf2_9ce4_8422_2325 ^ (data.len() as u64).wrapping_mul(P);
    let (chunks, rem) = data.as_chunks::<8>();
    for c in chunks {
        h = (h ^ mix(u64::from_le_bytes(*c)))
            .rotate_left(27)
            .wrapping_mul(P);
    }
    if !rem.is_empty() {
        let mut buf = [0u8; 8];
        buf[..rem.len()].copy_from_slice(rem);
        h = (h ^ mix(u64::from_le_bytes(buf)))
            .rotate_left(27)
            .wrapping_mul(P);
    }
    mix(h)
}

fn mix(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(keys: impl Iterator<Item = Vec<u8>>, bits_per_key: usize) -> BloomFilter {
        let mut b = BloomBuilder::new(bits_per_key);
        for k in keys {
            b.add_key(&k);
        }
        BloomFilter::new(b.finish())
    }

    #[test]
    fn no_false_negatives() {
        let keys: Vec<Vec<u8>> = (0..10_000)
            .map(|i| format!("key-{i}").into_bytes())
            .collect();
        let f = build(keys.iter().cloned(), 10);
        assert!(keys.iter().all(|k| f.may_contain(k)));
    }

    fn false_positive_rate(bits_per_key: usize) -> f64 {
        let n = 10_000;
        let f = build(
            (0..n).map(|i| format!("present-{i}").into_bytes()),
            bits_per_key,
        );
        let trials = 100_000;
        let hits = (0..trials)
            .filter(|i| f.may_contain(format!("absent-{i}").as_bytes()))
            .count();
        hits as f64 / trials as f64
    }

    #[test]
    fn false_positive_rate_matches_theory() {
        // Theoretical rates with k = floor(b * ln 2): b=10 -> ~0.84%, b=6 -> ~5.6%,
        // b=16 -> ~0.05%. Allow generous headroom for hash variance.
        let fpr10 = false_positive_rate(10);
        assert!(fpr10 < 0.015, "10 bits/key fpr = {fpr10}");
        assert!(fpr10 > 0.002, "10 bits/key fpr suspiciously low = {fpr10}");
        let fpr6 = false_positive_rate(6);
        assert!(fpr6 < 0.08, "6 bits/key fpr = {fpr6}");
        let fpr16 = false_positive_rate(16);
        assert!(fpr16 < 0.002, "16 bits/key fpr = {fpr16}");
        assert!(fpr16 < fpr10 && fpr10 < fpr6);
    }

    #[test]
    fn disabled_and_empty_filters_match_everything() {
        let f = build(std::iter::empty(), 0);
        assert!(f.may_contain(b"anything"));
        let f = BloomFilter::new(vec![0, 99]); // unknown probe count
        assert!(f.may_contain(b"anything"));
    }

    #[test]
    fn hash_distinguishes_trailing_zeros() {
        assert_ne!(hash64(b"ab"), hash64(b"ab\0"));
        assert_ne!(hash64(b""), hash64(b"\0"));
    }
}
