//! Simple Bloom filter implementation used by SSTables to avoid unnecessary disk reads.

use std::f64;
use xxhash_rust::xxh64::xxh64;

#[derive(Clone)]
pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: usize,
    num_hashes: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter sized for `num_items` with the given false-positive rate.
    pub fn new(num_items: usize, false_positive_rate: f64) -> Self {
        // Optimal number of bits (m)
        let num_bits = -((num_items as f64 * false_positive_rate.ln())
            / (f64::consts::LN_2 * f64::consts::LN_2))
            .ceil() as usize;

        // Optimal number of hash functions (k)
        let num_hashes = ((num_bits as f64 / num_items as f64) * f64::consts::LN_2).ceil() as usize;

        let num_bytes = num_bits.div_ceil(8);

        Self {
            bits: vec![0; num_bytes],
            num_bits,
            num_hashes,
        }
    }

    /// Add a key to the filter.
    pub fn add(&mut self, key: &[u8]) {
        let hash_indices: Vec<usize> = self.get_hashes(key).collect();

        for hash_index in hash_indices {
            let byte_index = hash_index / 8;
            let bit_in_byte_index = hash_index % 8;

            self.bits[byte_index] |= 1 << bit_in_byte_index;
        }
    }

    /// Returns false if the key is definitely not present; true otherwise (possible false positive).
    pub fn might_contain(&self, key: &[u8]) -> bool {
        for hash_index in self.get_hashes(key) {
            let byte_index = hash_index / 8;
            let bit_in_byte_index = hash_index % 8;

            if (self.bits[byte_index] & (1 << bit_in_byte_index)) == 0 {
                return false;
            }
        }

        true // Possibly present
    }

    // Double-hashing to derive `k` hash indices from two base hashes.
    fn get_hashes(&self, key: &[u8]) -> impl Iterator<Item = usize> {
        let h1 = xxh64(key, 0) as usize;
        let mut reader = std::io::Cursor::new(key);
        let h2 = murmur3::murmur3_32(&mut reader, 0).unwrap() as usize;

        (0..self.num_hashes).map(move |i| {
            // Formula: (h1 + i * h2) % num_bits
            (h1.wrapping_add(i.wrapping_mul(h2))) % self.num_bits
        })
    }

    /// Serialize the bloom filter to bytes for persistence.
    /// Format: [num_bits: u64][num_hashes: u64][bits_len: u64][bits...]
    #[allow(dead_code)]
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(24 + self.bits.len());
        out.extend_from_slice(&(self.num_bits as u64).to_le_bytes());
        out.extend_from_slice(&(self.num_hashes as u64).to_le_bytes());
        out.extend_from_slice(&(self.bits.len() as u64).to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    /// Deserialize a bloom filter from bytes.
    #[allow(dead_code)]
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 24 {
            return None;
        }
        let num_bits = u64::from_le_bytes(data[0..8].try_into().ok()?) as usize;
        let num_hashes = u64::from_le_bytes(data[8..16].try_into().ok()?) as usize;
        let bits_len = u64::from_le_bytes(data[16..24].try_into().ok()?) as usize;

        if data.len() < 24 + bits_len {
            return None;
        }
        let bits = data[24..24 + bits_len].to_vec();

        Some(Self {
            bits,
            num_bits,
            num_hashes,
        })
    }
}
