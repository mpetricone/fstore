// Coyright 2021 Matthew Petricone
use crate::data_header::DataHeader;
use crate::data_header::{BlockFlags, BlockSerializer};
use crate::crypto::BlockHasher;
use std::convert::TryFrom;
use std::fmt;
use std::fs::{ File, OpenOptions };
use std::io::{Error, ErrorKind};
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;

// TODO: is there a better way in rust?
static STORE_VERSIONTAG: &str = "FSTOREV.01BINARYR01";
static STORE_VERSIONNUM: u32 = 1;

// TODO: should these be static?
static ERROR_FSTORE_VERSION: &str = "Unexpected version info.";
static ERROR_FSTORE_INVALID: &str = "Invalid file descriptor.";
static ERROR_FSTORE_INVSIZE: &str = "Unexpected data size encountered.";
static ERROR_OUTOFBOUNDS: &str = "Value out of bounds.";


/// Used by some fstore methods
#[derive(Debug)]
pub struct StoreError {
    error: String,
}

impl StoreError {
    /// Create new StoreError
    fn new(error: String) -> StoreError {
        StoreError { error }
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for StoreError {}

/// Store manages a file store.
///
/// Data is written in blocks of arbitrary size.
///
/// Consult DataHeader for block details.
///
/// There is a 32bit checksum availible for each block.
///
pub struct Store<'a, U: Eq + PartialEq + Copy, T: BlockHasher<U>> {
    /// File data resides in
    file: File,
    /// the last stream position
    data_start_address: u64,
    /// Vector of written block addresses
    block_addresses: Vec<u64>,
    hasher: &'a mut T,
    phantom: PhantomData<U>,
}

/// Utilities for a Store
pub trait StoreIO<'a, U: Eq + PartialEq + Copy, T: 'a + BlockHasher<U>> where &'a mut T: BlockHasher<U> {
    /// Delete block at index
    fn delete_block(&mut self, index: usize) -> Result<(), Box<dyn std::error::Error>>;
    /// Should return the number of blocks availible for access
    fn len(&self) -> usize;
    /// Get the address of the block at index
    fn block_address(&self, index: usize) -> Option<&u64>;

    fn read_data_header(
        &mut self,
        data_header: &mut DataHeader<'a, U, T>,
    ) -> Result<(), Box<dyn std::error::Error>>;
    fn read(&mut self, data: &mut Vec<u8>) -> Result<usize, Error>;
    fn read_at_index(&mut self, index: usize, data: &mut Vec<u8>) -> Result<usize,Box<dyn std::error::Error>>;

    fn seek(&mut self, index: usize) -> Result<u64, Box<dyn std::error::Error>>;
}

impl<'a, U: Eq + PartialEq + Copy, T: BlockHasher<U>> Store<'a, U, T> where &'a mut T: BlockHasher<U> {
    /// Open existing Store file
    ///
    /// Will return an error if the file is not a Store file
    pub fn new(filename: String, hasher: &'a mut T) -> Result<Store<'a, U,&'a mut T>, Box<dyn std::error::Error>> {
        let v = File::open(filename)?;
        let mut st = Store::<U, &'a mut T> {
            file: v,
            data_start_address: 0,
            block_addresses: Vec::new(),
            hasher,
            phantom: PhantomData,
        };
        let fd = st.read_file_descriptor()?;
        if !Store::<U,T>::validate_file_descriptor(fd) {
            return Err(Box::new(Error::new(
                ErrorKind::InvalidData,
                ERROR_FSTORE_INVALID,
            )));
        }
        st.index_blocks(0)?;
        Ok(st)
    }

    ///Create new Store file
    ///
    ///Will overwrite an existing store.
    pub fn create(filename: String, hasher: &'a mut T) -> Result<Store<'a, U, T>, Error> {
        let mut f = OpenOptions::new().write(true).read(true).create(true).open(filename)?;
        Store::<'a, U, T>::write_file_descriptor(&mut f)?;
        Ok(Store::<'a, U, T> {
            file: f,
            data_start_address: 0,
            block_addresses: Vec::new(),
            hasher,
            phantom: PhantomData,
        })
    }

    /// Writes the file descriptor (should be at the start of the file)
    fn write_file_descriptor(file: &mut File) -> Result<(), Error> {
        file.write(&STORE_VERSIONNUM.to_le_bytes())?;
        // Panic here, there is no way this should fail unless we've typo'd
        let sz = u64::try_from(STORE_VERSIONTAG.as_bytes().len()).unwrap();
        file.write(&sz.to_le_bytes())?;
        file.write(&STORE_VERSIONTAG.as_bytes())?;
        Ok(())
    }

    /// reads the file descriptor
    /// returns a tuple
    fn read_file_descriptor(&mut self) -> Result<(u32, String), Error> {
        // it's only at the start of the file
        self.file.seek(SeekFrom::Start(0))?;
        let mut buff = [0u8; 4];
        let mut sz_buff = [0u8; 8];
        self.file.read(&mut buff)?;
        self.file.read(&mut sz_buff)?;
        let mut str_buff = vec![0u8; usize::try_from(u64::from_le_bytes(sz_buff)).unwrap()];
        self.file.read(&mut str_buff)?;
        self.data_start_address = self.file.seek(SeekFrom::Current(0))?;
        //Convert this error into a somewhat relevant io::Error
        if let Ok(s) = String::from_utf8(str_buff) {
            Ok((u32::from_le_bytes(buff), s))
        } else {
            return Err(Error::new(ErrorKind::InvalidData, ERROR_FSTORE_VERSION));
        }
    }

    /// checks value to see if it's a valid file descriptor
    pub fn validate_file_descriptor(value: (u32, String)) -> bool {
        //NOTE: this should get more complicated when there are more versions;
        if value == (STORE_VERSIONNUM, STORE_VERSIONTAG.to_string()) {
            return true;
        }
        false
    }

    /// Read address of blocks for index
    fn index_blocks(&mut self, startpos: u64) -> Result<(), Box<dyn std::error::Error>> {
        // if startpos is 0, set it to the first block, otherwise it's a valid block start
        // at this point, i'm failry sure an incorrect block location will still fill up a block
        // albeit with incorect info if  there is enough data in the file
        self.block_addresses.clear();
        let mut curpos = if startpos == 0 {
            self.data_start_address
        } else {
            startpos
        };
        // size of read ahead data
        let buffsize = DataHeader::<U, T>::read_ahead_size();
        // get metadata for file once
        let md = self.file.metadata()?;
        // Insert the first block address
        self.block_addresses.push(curpos);
        // We are assuming the file will not change size during this loop
        while curpos < md.len() {
            //TODO: is it faster to reuse a buffer?
            let mut buffer = vec![0u8; buffsize];
            // read the data, then pass it to dataBlock::read_ahead
            self.file.read(&mut buffer)?;
            // TODO: I think this logic is wrong, we want a more generic way to do this.
            let tbs = DataHeader::<U, T>::read_ahead(&buffer)?;
            // update curpos with next DataHeader addess, then push that onto the list
            curpos = self.file.seek(SeekFrom::Current(tbs))?;
            self.block_addresses.push(curpos);
        }
        self.file.seek(SeekFrom::Start(self.data_start_address))?;
        Ok(())
    }
}

impl<'a, U: Eq + PartialEq + Copy, T: BlockHasher<U>> Write for Store<'a, U, T> {
    /// Writes data in buf to file, encapsulated in a DataHeader
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        if let Ok(mut bd) = DataHeader::<U,T>::new(buf, &self.hasher) {
            self.file.write(bd.serialize(buf))?;
            let retval = self.file.write(&buf);
            self.block_addresses.push(self.file.seek(SeekFrom::Current(0))?);
            retval
        } else {
            return Err(Error::new(ErrorKind::InvalidInput, ERROR_FSTORE_INVSIZE));
        }
    }

    /// Calls flush on self.file
    fn flush(&mut self) -> Result<(), Error> {
        self.file.flush()
    }
}

impl<'a, U: Eq + PartialEq + Copy, T: BlockHasher<U>> StoreIO<'a,U, T> for Store<'a, U, T> where &'a mut T: BlockHasher<U> {
    fn delete_block(&mut self, index: usize) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(address) = self.block_addresses.get(index) {
            self.file.seek(SeekFrom::Start(
                *address + u64::try_from(DataHeader::<U,T>::delete_offset())?,
            ))?;
            self.file.write(&DataHeader::<U, T>::delete_flag().to_le_bytes())?;
            self.file.seek(SeekFrom::Start(0))?;
        } else {
            return Err(Box::new(StoreError::new(ERROR_OUTOFBOUNDS.to_string())));
        }
        Ok(())
    }

    fn block_address(&self, index: usize) -> Option<&u64> {
        self.block_addresses.get(index)
    }

    fn len(&self) -> usize {
        self.block_addresses.len()
    }
    
    fn seek(&mut self, index: usize) -> Result<u64, Box<dyn std::error::Error>> {
        if let Some(a) = self.block_addresses.get(index) {
            Ok(self.file.seek(SeekFrom::Start(*a))?)
        } else {
            return Err(Box::new(StoreError::new(ERROR_OUTOFBOUNDS.to_string())));
        }
    }

    /// Reads data into buf according to surrounding DataHeader
    fn read_data_header(
        &mut self,
        data_header: &mut DataHeader<'a, U, T>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut db_buf = vec![0u8; DataHeader::<U, T>::size()];
        self.file.read(&mut db_buf)?;
        data_header.deserialize(&db_buf)?;
        Ok(())
    }

    fn read(&mut self, data: &mut Vec<u8>) -> Result<usize, Error> {
        self.file.read(data)
    }

    fn read_at_index(&mut self,index: usize, data: &mut Vec<u8>) -> Result<usize, Box<dyn std::error::Error>> {
        if let Some(a) = self.block_addresses.get(index) {
            self.file.seek(SeekFrom::Start(*a))?;
            Ok(self.read(data)?)
        } else {
            return Err(Box::new(StoreError::new(ERROR_OUTOFBOUNDS.to_string())));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_header::DataHeader;
    use crate::store::Store;
    use crate::crypto::B3BlockHasher;
    use std::io::Write;

    fn fill_test_vector(data: &mut Vec<u8>) {
        data.append(&mut vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 255]);
    }
    #[test]
    fn can_write_to_store() {
        let mut s = Store::<&[u8], B3BlockHasher>::create("testout/store.st".to_string(), B3BlockHasher::default()).unwrap();
        let mut buf = vec![0, 1, 3, 4, 5, 11, 33, 0];
        s.write(&mut buf).unwrap();
        s.write(&mut buf).unwrap();
    }

    #[test]
    fn can_read_from_store() {
        let mut testval = Vec::new();
        fill_test_vector(&mut testval);
        {
            let mut s = Store::<&[u8], B3BlockHasher>::create("testout/store.test.st".to_string(), B3BlockHasher::default()).unwrap();
            for _i in 1..10 {
                s.write(&testval).unwrap();
                s.write(&testval).unwrap();
            }
        }

        let mut db = DataHeader::<&[u8], B3BlockHasher>::new(&[0u8],B3BlockHasher::default()).unwrap();
        let mut s = Store::<&[u8], B3BlockHasher>::new("testout/store.test.st".to_string(), B3BlockHasher::default()).unwrap();
        s.read_data_header(&mut db).unwrap();
        let mut data = vec![0u8; db.data_size().unwrap()];
        s.read(&mut data).unwrap();
        assert_eq!(testval, data);
    }

    #[test]
    fn can_delete_block() {
        let v = [
            vec!(1, 244, 231,13,42,1,2,3,4,5,6,7),
            vec!(1,2,3,4,5,6,7,8,9,0),
            vec!(11,12,13,14,15,16,17,18,19,20),
        ];
        let mut s = Store::<&[u8], B3BlockHasher>::create("testout/delete.tst".to_string(), B3BlockHasher::default()).unwrap();
        for i in v {
            s.write(&i).unwrap();
        }
        s.delete_block(2).unwrap();
        let mut db = DataHeader::<&[u8], B3BlockHasher>::new(&[0u8], B3BlockHasher::default()).unwrap();
        s.seek(2).unwrap();
        s.read_data_header(&mut db).unwrap();
        assert_eq!(DataHeader::<&[u8], B3BlockHasher>::delete_flag(),db.state_flag );
    }
}
