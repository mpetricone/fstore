//Copyright 2021 Matthew Petricone
use blake3;

/// Generate a hash from arbitrary amount of input data
///
/// Used by DataBlock to verify data integrity
pub trait BlockHasher {
    
    /// Create an instance
    fn create() -> Self;
    /// Generate hash from input
    fn hash(&mut self, input: &[u8]) -> &[u8];
    /// Size of hash
    fn size() -> usize;
}

/// Blake3 Hasher
#[derive(Default, Debug, PartialEq)]
pub struct B3BlockHasher {
    /// Stores the value of hash as bytes, not aligned.
    pub hash_value: [u8;  32],
}

impl BlockHasher for B3BlockHasher {

    fn create() -> Self {
        B3BlockHasher { hash_value: [0; 32] }
    }
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

impl BlockHasher for NullBlockHasher {
    fn create() -> Self { NullBlockHasher {} }
    fn hash(&mut self, _input: &[u8]) -> &[u8] { &[0] }
    fn size() -> usize { 0 }
}
