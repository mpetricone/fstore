//Copyright 2021 Matthew Petricone
use blake3;

/// Generate a hash from arbitrary amount of input data
///
/// Used by DataBlock to verify data integrity
pub trait BlockHasher<T: Eq + PartialEq + Copy> {
    
    /// Generate hash from input
    fn hash(&mut self, input: &[u8]) -> &T;

    /// Size of hash
    fn hash_size() -> usize;
}

/// Blake3 Hasher
#[derive(Default)]
pub struct B3BlockHasher {
    /// Stores the value of hash as bytes, not aligned.
    hash_value: [u8;  32],
}

impl BlockHasher<[u8; 32]> for B3BlockHasher {

    fn hash(&mut self, input: &[u8]) -> &[u8; 32] {
        self.hash_value = *blake3::hash(input).as_bytes();
        &self.hash_value
    }

    fn hash_size() -> usize {
        256
    }
}
