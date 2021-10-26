//Copyright 2021 Matthew Petricone
use std::convert::TryFrom;
use std::convert::TryInto;
use std::error::Error;
use std::mem::size_of;
use std::marker::PhantomData;
use crate::crypto::BlockHasher;

const STATE_FLAG_ALLOC: u32 = 0b0;
const STATE_FLAG_DELETE: u32 = 0b1;
const DEFAULT_ADDR_NEXT: u64 = 0;

/// Trait for preparing a DataHeader for writing to stream
pub trait BlockSerializer {
    /// Create a vector of data ready to be written
    ///
    fn serialize(&mut self, data: &[u8]) -> &Vec<u8>;

    fn deserialize(&mut self, data: &Vec<u8>) -> Result<(), Box<dyn Error>>;

    /// size in bytes of the serialized data
    fn size(&self) -> usize;

    /// Minimum size of data needed to read ahead to next block
    fn read_ahead_size() -> usize;

    fn delete_offset() -> usize;

    /// gets the amount to seek to next DataHeader
    fn read_ahead(size: &Vec<u8>) -> Result<i64, Box<dyn Error>>;
}

/// interface with block flags
pub trait BlockFlags {
    /// Get the positive flag value
    fn delete_flag() -> u32;
    fn set_delete_flag(value: bool, flags: u32) -> u32;
}

/// A DataHeader, minus the data.debuggers
///
/// It should probably be renamed DataHeader
#[derive(PartialEq, Debug)]
pub struct DataHeader<U: Eq + PartialEq + Copy, T: BlockHasher<U>> {
    /// size of data in this block
    size_data: u64,
    /// state of block.
    /// usually a 1 for allocated
    pub state_flag: u32,
    /// address of next DataHeader in file containing appended data
    address_next: u64,
    /// Vector of DataHeader header
    header: Vec<u8>,
    hasher: T,
    phantom: PhantomData<U>,
}

impl<U:Eq + PartialEq + Copy ,T: BlockHasher<U>> DataHeader<U, T> {
    /// create Data block, get size (& eventually checksum from data)
    pub fn new(
        data: &[u8],
        hasher: T
    ) -> Result<DataHeader<U, T>, Box<dyn Error>> {
        let mut cs = 0;
        Ok(DataHeader {
            size_data: u64::try_from(data.len())?,
            state_flag: STATE_FLAG_ALLOC,
            address_next: DEFAULT_ADDR_NEXT,
            header: vec![0],
            hasher,
            phantom: PhantomData,
        })
    }

    pub fn data_size(&self) -> Result<usize, Box<dyn std::error::Error>> {
        Ok(usize::try_from(self.size_data)?)
    }
}

impl<U: Eq + PartialEq + Copy,T: BlockHasher<U>> BlockFlags for DataHeader<U, T> {
    #[inline]
    fn delete_flag() -> u32 {
        STATE_FLAG_DELETE
    }

    fn set_delete_flag(value: bool,mut  flags: u32 ) -> u32 {
        flags = flags|STATE_FLAG_DELETE;
        if !value {
            flags = flags^STATE_FLAG_DELETE;
        }
        flags
    }
}

impl<U: Eq + PartialEq + Copy, T: BlockHasher<U>> BlockSerializer for DataHeader<U, T> {
    /// Return vector serialized DataHeader
    fn serialize(&mut self, data: &[u8]) -> &Vec<u8> {
        self.header.clear();
        self.header
            .append(&mut self.size_data.to_le_bytes().to_vec());
        self.header
            .append(&mut self.state_flag.to_le_bytes().to_vec());
        self.header
            .append(&mut self.address_next.to_le_bytes().to_vec());
        self.header
            .append(&mut self.hasher.hash(data).to_vec());
        &self.header
    }

    /// Fill struct from binary data
    ///
    /// Assumes correct size of data for the Block
    fn deserialize(&mut self, data: &Vec<u8>) -> Result<(), Box<dyn Error>> {
        self.size_data = u64::from_le_bytes(data[0..8].try_into()?);
        self.state_flag = u32::from_le_bytes(data[8..12].try_into()?);
        self.address_next = u64::from_le_bytes(data[12..20].try_into()?);
        if self.hasher.hash(data) != &data[20..] {
            return Err(
                Box::new(
                    std::io::Error::new(std::io::ErrorKind::InvalidData, 
                        "Block Hashes do not match.")))
        }
        Ok(())
    }

    #[inline]
    fn size(&self) -> usize {
        (size_of::<u64>() * 2) + size_of::<u32>() + T::size()
    }

    #[inline]
    fn read_ahead_size() -> usize {
        size_of::<u64>()
    }

    fn read_ahead(size: &Vec<u8>) -> Result<i64, Box<dyn Error>> {
        let mds = i64::try_from(size_of::<u64>() + size_of::<u32>() + T::size() )?;
        Ok(mds)
    }

    #[inline]
    fn delete_offset() -> usize {
        size_of::<u64>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_create_data_block() {
        let _db = DataHeader::new(&vec![0u8; 8], None).unwrap();
    }

    #[test]
    fn can_serialize_data_block() {
        println!(
            "{:?}",
            DataHeader::new(&vec!(0u8; 16), None).unwrap().serialize()
        );
    }

    #[test]
    fn can_deserialize_data_block() {
        let mut serialized = DataHeader::new(&vec![50, 24, 24, 100], None).unwrap();
        let mut db2 = DataHeader::new(&Vec::<u8>::new(), None).unwrap();
        db2.deserialize(serialized.serialize()).unwrap();
        // This is to make sure the db2.header matches serialized.header otherwise we'll fail the
        // assert
        db2.serialize();
        assert_eq!(db2, serialized);
    }

    #[test]
    fn can_set_delet_flag() {
        let mut tflag = 0b0;
        assert_eq!(DataHeader::set_delete_flag(false, tflag), 0);
        assert_eq!(DataHeader::set_delete_flag(true, tflag), 1);
        tflag = 0b1;
        assert_eq!(DataHeader::set_delete_flag(false, tflag), 0);
        assert_eq!(DataHeader::set_delete_flag(true, tflag), 1);
    }
}
