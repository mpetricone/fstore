//Copyright 2021 Matthew Petricone
use std::convert::TryFrom;
use std::convert::TryInto;
use std::error::Error;
use std::mem::size_of;

const STATE_FLAG_ALLOC: u32 = 0b0;
const STATE_FLAG_DELETE: u32 = 0b1;
const DEFAULT_ADDR_NEXT: u64 = 0;

/// Trait for preparing a Datablock for writing to stream
pub trait BlockSerializer {
    /// Create a vector of data ready to be written
    ///
    fn serialize(&mut self) -> &Vec<u8>;

    fn deserialize(&mut self, data: &Vec<u8>) -> Result<(), Box<dyn Error>>;

    /// size in bytes of the serialized data
    fn size() -> usize;

    /// Minimum size of data needed to read ahead to next block
    fn read_ahead_size() -> usize;

    fn delete_offset() -> usize;

    /// gets the amount to seek to next DataBlock
    fn read_ahead(size: &Vec<u8>) -> Result<i64, Box<dyn Error>>;
}

/// Trait for checksum calculation
pub trait BlockChecksum {
    fn calculate(&self, data: &[u8]) -> u32;
}

/// interface with block flags
pub trait BlockFlags {
    /// Get the positive flag value
    fn delete_flag() -> u32;
    fn set_delete_flag(value: bool, flags: u32) -> u32;
}

/// A Datablock, minus the data.
///
/// It should probably be renamed DataHeader
#[derive(PartialEq, Debug)]
pub struct DataBlock {
    /// size of data in this block
    size_data: u64,
    /// state of block.
    /// usually a 1 for allocated
    pub state_flag: u32,
    /// address of next DataBlock in file containing appended data
    address_next: u64,
    /// checksum of data in this block. 0 if not used.
    checksum: [u8],
    /// Vector of DataBlock header
    header: Vec<u8>,
}

impl DataBlock {
    /// create Data block, get size (& eventually checksum from data)
    pub fn new(
        data: &[u8],
        checksum: Option<&dyn BlockChecksum>,
    ) -> Result<DataBlock, Box<dyn Error>> {
        let mut cs = 0;
        if let Some(check) = checksum {
            cs = check.calculate(data);
        }
        Ok(DataBlock {
            size_data: u64::try_from(data.len())?,
            state_flag: STATE_FLAG_ALLOC,
            address_next: DEFAULT_ADDR_NEXT,
            checksum: cs,
            header: vec![0],
        })
    }

    pub fn data_size(&self) -> Result<usize, Box<dyn std::error::Error>> {
        Ok(usize::try_from(self.size_data)?)
    }
}

impl BlockFlags for DataBlock {
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

impl BlockSerializer for DataBlock {
    /// Return vector serialized DataBlock
    fn serialize(&mut self) -> &Vec<u8> {
        self.header.clear();
        self.header
            .append(&mut self.size_data.to_le_bytes().to_vec());
        self.header
            .append(&mut self.state_flag.to_le_bytes().to_vec());
        self.header
            .append(&mut self.address_next.to_le_bytes().to_vec());
        self.header
            .append(&mut self.checksum.to_le_bytes().to_vec());
        &self.header
    }

    /// Fill struct from binary data
    ///
    /// Assumes correct size of data for the Block
    fn deserialize(&mut self, data: &Vec<u8>) -> Result<(), Box<dyn Error>> {
        self.size_data = u64::from_le_bytes(data[0..8].try_into()?);
        self.state_flag = u32::from_le_bytes(data[8..12].try_into()?);
        self.address_next = u64::from_le_bytes(data[12..20].try_into()?);
        self.checksum = u32::from_le_bytes(data[20..24].try_into()?);
        Ok(())
    }

    #[inline]
    fn size() -> usize {
        (size_of::<u64>() * 2) + (size_of::<u32>() * 2)
    }

    #[inline]
    fn read_ahead_size() -> usize {
        size_of::<u64>()
    }

    fn read_ahead(size: &Vec<u8>) -> Result<i64, Box<dyn Error>> {
        let mds = i64::try_from(size_of::<u64>() + (size_of::<u32>() * 2))?;
        Ok(i64::from_le_bytes(size[0..8].try_into()?) + mds)
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
        let _db = DataBlock::new(&vec![0u8; 8], None).unwrap();
    }

    #[test]
    fn can_serialize_data_block() {
        println!(
            "{:?}",
            DataBlock::new(&vec!(0u8; 16), None).unwrap().serialize()
        );
    }

    #[test]
    fn can_deserialize_data_block() {
        let mut serialized = DataBlock::new(&vec![50, 24, 24, 100], None).unwrap();
        let mut db2 = DataBlock::new(&Vec::<u8>::new(), None).unwrap();
        db2.deserialize(serialized.serialize()).unwrap();
        // This is to make sure the db2.header matches serialized.header otherwise we'll fail the
        // assert
        db2.serialize();
        assert_eq!(db2, serialized);
    }

    #[test]
    fn can_set_delet_flag() {
        let mut tflag = 0b0;
        assert_eq!(DataBlock::set_delete_flag(false, tflag), 0);
        assert_eq!(DataBlock::set_delete_flag(true, tflag), 1);
        tflag = 0b1;
        assert_eq!(DataBlock::set_delete_flag(false, tflag), 0);
        assert_eq!(DataBlock::set_delete_flag(true, tflag), 1);
    }
}
