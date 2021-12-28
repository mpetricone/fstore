//Copyright 2021 Matthew Petricone
use std::convert::TryFrom;
use std::convert::TryInto;
use std::error::Error;
use std::mem::size_of;
use std::marker::PhantomData;
use crate::crypto::{BlockHasher};


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
    fn size() -> usize;

    /// Minimum size of data needed to read ahead to next block
    fn read_ahead_size() -> usize;

    fn delete_offset() -> usize;

    /// gets the amount to seek to next DataHeader
    fn read_ahead(_buffer: &Vec<u8>) -> Result<i64, Box<dyn Error>>;
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
pub struct DataHeader<T: BlockHasher> {
    /// size of data in this block
    size_data: u64,
    /// state of block.
    /// usually a 1 for allocated
    pub state_flag: u32,
    /// address of next DataHeader in file containing appended data
    address_next: u64,
    /// Vector of DataHeader header
    header: Vec<u8>,
    phantom: PhantomData<T>,
}

impl<T: BlockHasher > DataHeader<T> {
    /// create Data block, get size (& eventually checksum from data)
    pub fn new( ) -> Result<DataHeader<T>, Box<dyn Error>> {
        Ok(DataHeader::<T> {
            size_data: 0,
            state_flag: STATE_FLAG_ALLOC,
            address_next: DEFAULT_ADDR_NEXT,
            header: vec![0],
            phantom: PhantomData,
        })
    }

    pub fn data_size(&self) -> Result<usize, Box<dyn std::error::Error>> {
        Ok(usize::try_from(self.size_data)?)
    }
}

impl<T: BlockHasher> BlockFlags for DataHeader<T> {
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

impl<T: BlockHasher> BlockSerializer for DataHeader<T> {
    /// Return vector serialized DataHeader
    fn serialize(&mut self, data: &[u8] ) -> &Vec<u8> {
        self.header.clear();
        self.header
            .append(&mut self.size_data.to_le_bytes().to_vec());
        self.header
            .append(&mut self.state_flag.to_le_bytes().to_vec());
        self.header
            .append(&mut self.address_next.to_le_bytes().to_vec());
        let mut hasher = T::create();
        self.header
            .append(&mut hasher.hash(data).to_vec());
        &self.header
    }

    /// Fill struct from binary data
    ///
    /// Assumes correct size of data for the Block
    fn deserialize(&mut self, data: &Vec<u8>) -> Result<(), Box<dyn Error>> {
        self.size_data = u64::from_le_bytes(data[0..8].try_into()?);
        self.state_flag = u32::from_le_bytes(data[8..12].try_into()?);
        self.address_next = u64::from_le_bytes(data[12..20].try_into()?);
        if T::create().hash(data) != &data[20..] {
            return Err(
                Box::new(
                    std::io::Error::new(std::io::ErrorKind::InvalidData, 
                        "Block Hashes do not match.")))
        }
        Ok(())
    }

    #[inline]
    fn size() -> usize {
        (size_of::<u64>() * 2) + size_of::<u32>() + T::size()
    }

    #[inline]
    fn read_ahead_size() -> usize {
        size_of::<u64>()
    }

    fn read_ahead(_buffer: &Vec<u8>) -> Result<i64, Box<dyn Error>> {
        //TODO: WTF was supposed to happen here?
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
    use crate::crypto::{ NullBlockHasher, B3BlockHasher};

    #[test]
    fn can_create_data_block() {
        let _db = DataHeader::<B3BlockHasher>::new();
    }

    #[test]
    fn can_serialize_data_block() {
        let data = [0, 0, 1, 0];
        println!(
            "{:?}",
            DataHeader::<NullBlockHasher>::new().unwrap().serialize(&data)
        );
    }

    #[test]
    fn can_deserialize_data_block() {
        let data = [0u8];
        let mut serialized = DataHeader::<B3BlockHasher>::new().unwrap();
        let mut db2 = DataHeader::<B3BlockHasher>::new().unwrap();
        db2.deserialize(serialized.serialize(&data)).unwrap();
        // This is to make sure the db2.header matches serialized.header otherwise we'll fail the
        // assert
        db2.serialize(&data);
        assert_eq!(db2, serialized);
    }

    #[test]
    fn can_set_delet_flag() {
        let mut tflag = 0b0;
        assert_eq!(DataHeader::<B3BlockHasher>::set_delete_flag(false, tflag), 0);
        assert_eq!(DataHeader::<B3BlockHasher>::set_delete_flag(true, tflag), 1);
        tflag = 0b1;
        assert_eq!(DataHeader::<B3BlockHasher>::set_delete_flag(false, tflag), 0);
        assert_eq!(DataHeader::<B3BlockHasher>::set_delete_flag(true, tflag), 1);
    }
}
