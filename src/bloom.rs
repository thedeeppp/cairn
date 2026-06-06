//! A Bloom filter: a compact, probabilistic set used to answer "is this key
//! *definitely not* here?" cheaply.
//!
//! It never reports a false negative — if a key was inserted, `contains` always
//! returns true — but it may report a false positive. That asymmetry is exactly
//! what an SSTable wants: a `false` lets a Get skip the file with zero disk
//! reads; a `true` just means "go check for real".
//!
//! Hashing is a hand-rolled FNV-1a with two independent bases, combined by
//! double hashing (`h1 + i·h2`). FNV is used instead of the stdlib hasher
//! because the filter is persisted to disk: the same bytes must hash the same
//! way on every future run and Rust version.

use serde::{Deserialize, Serialize};

const FNV_OFFSET_A: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_OFFSET_B: u64 = 0x9e37_79b9_7f4a_7c15;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Serialize, Deserialize)]
pub struct Bloom {
    bits: Vec<u8>,
    num_bits: u64,
    k: u32,
}

fn fnv1a(data: &[u8], mut hash: u64) -> u64 {
    for &b in data {
        hash ^= b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

impl Bloom {
    /// Sizes a filter for roughly `expected_items` keys at false-positive rate
    /// `fp_rate` (e.g. 0.01), using the standard optimal m (bits) and k (hashes).
    pub fn new(expected_items: usize, fp_rate: f64) -> Bloom {
        let n = expected_items.max(1) as f64;
        let ln2 = std::f64::consts::LN_2;
        let num_bits = (-n * fp_rate.ln() / (ln2 * ln2)).ceil().max(1.0) as u64;
        let k = ((num_bits as f64 / n) * ln2).round().max(1.0) as u32;
        Bloom {
            bits: vec![0; num_bits.div_ceil(8) as usize],
            num_bits,
            k,
        }
    }

    /// Builds a filter already populated with `keys`.
    pub fn build(keys: &[&[u8]], fp_rate: f64) -> Bloom {
        let mut bloom = Bloom::new(keys.len(), fp_rate);
        for key in keys {
            bloom.insert(key);
        }
        bloom
    }

    /// The two derived hashes used for double hashing.
    fn hashes(key: &[u8]) -> (u64, u64) {
        (fnv1a(key, FNV_OFFSET_A), fnv1a(key, FNV_OFFSET_B))
    }

    fn bit_index(&self, h1: u64, h2: u64, i: u32) -> u64 {
        h1.wrapping_add((i as u64).wrapping_mul(h2)) % self.num_bits
    }

    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = Bloom::hashes(key);
        for i in 0..self.k {
            let idx = self.bit_index(h1, h2, i);
            self.bits[(idx / 8) as usize] |= 1 << (idx % 8);
        }
    }

    /// Returns `false` only if `key` was definitely never inserted. A `true`
    /// may be a false positive.
    pub fn contains(&self, key: &[u8]) -> bool {
        let (h1, h2) = Bloom::hashes(key);
        (0..self.k).all(|i| {
            let idx = self.bit_index(h1, h2, i);
            self.bits[(idx / 8) as usize] & (1 << (idx % 8)) != 0
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn never_false_negative() {
        let keys: Vec<Vec<u8>> = (0..1000).map(|i| format!("key-{i}").into_bytes()).collect();
        let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let bloom = Bloom::build(&refs, 0.01);
        // Every inserted key MUST be reported present.
        for key in &keys {
            assert!(bloom.contains(key), "false negative for {key:?}");
        }
    }

    #[test]
    fn empty_filter_contains_nothing() {
        let bloom = Bloom::build(&[], 0.01);
        assert!(!bloom.contains(b"anything"));
    }

    #[test]
    fn false_positive_rate_is_roughly_bounded() {
        let keys: Vec<Vec<u8>> = (0..1000)
            .map(|i| format!("present-{i}").into_bytes())
            .collect();
        let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let bloom = Bloom::build(&refs, 0.01);

        let mut fps = 0;
        let trials = 10_000;
        for i in 0..trials {
            if bloom.contains(format!("absent-{i}").as_bytes()) {
                fps += 1;
            }
        }
        // Target is 1%; allow generous slack so the test isn't flaky.
        let rate = fps as f64 / trials as f64;
        assert!(rate < 0.05, "false-positive rate too high: {rate}");
    }

    #[test]
    fn survives_serialization() {
        let keys: Vec<Vec<u8>> = (0..200).map(|i| format!("k{i}").into_bytes()).collect();
        let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let bloom = Bloom::build(&refs, 0.01);

        let bytes = bincode::serialize(&bloom).unwrap();
        let restored: Bloom = bincode::deserialize(&bytes).unwrap();
        for key in &keys {
            assert!(restored.contains(key));
        }
    }
}
