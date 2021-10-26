//Copyright 2021 Matthew Petricone
use blake3;

/// Generate a hash from arbitrary amount of input data
///
/// Used by DataBlock to verify data integrity
pub trait BlockHasher<T: Eq + PartialEq + Copy> {
    
    /// Generate hash from input
    fn hash(&mut self, input: &[u8]) -> &[u8];
    /// Size of hash
    fn size() -> usize;
}

/// Blake3 Hasher
#[derive(Default)]
pub struct B3BlockHasher {
    /// Stores the value of hash as bytes, not aligned.
    pub hash_value: [u8;  32],
}

impl BlockHasher<&[u8]> for B3BlockHasher {

    fn hash(&mut self, input: &[u8]) -> &[u8] {
        self.hash_value = *blake3::hash(input).as_bytes();
        &self.hash_value
    }

    fn size() -> usize {
        256
    }
}

#[derive(Default)]
pub struct NullBlockHasher {
}

impl BlockHasher<u8> for NullBlockHasher {
    fn hash(&mut self, _input: &[u8]) -> &[u8] { &[0] }
    fn size() -> usize { 0 }
}
