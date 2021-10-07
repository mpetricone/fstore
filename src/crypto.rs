//Copyright 2021 Matthew Petricone
pub trait BlockHasher<T: Eq + PartialEq + Copy> {
    fn hash(&self, input: &[u8]) -> T;
}
