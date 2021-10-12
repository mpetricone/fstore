//Copyright 2021 Matthew Petricone
use blake3;

pub trait BlockHasher<T: Eq + PartialEq + Copy> {
    fn hash(&mut self, input: &[u8]) -> &T;
}

#[derive(Default)]
pub struct B3BlockHasher {
    hash_value: [u8;  32],
}

impl BlockHasher<[u8; 32]> for B3BlockHasher {

    fn hash(&mut self, input: &[u8]) -> &[u8; 32] {
        self.hash_value = *blake3::hash(input).as_bytes();
        &self.hash_value
    }
}
