// Details of fs in excel in root
// for now, convert all usize to u64.
// This should ensure all architechures will read files ok.
// I don't think file size will be an issuee
pub mod data_block;
use std::fs::File;
use std::io::{Error, ErrorKind};
use std::convert::TryFrom;
use std::io::{ Seek, Write, Read, SeekFrom };
use crate::fstore::data_block::DataBlock;
use crate::fstore::data_block::BlockSerializer;

// TODO: is there a better way in rust?
static STORE_VERSIONTAG: &str = "FSTOREV.01BINARYR01";
static STORE_VERSIONNUM: u32 = 1;

// TODO: should these be static?
static ERROR_FSTORE_VERSION: &str = "Unexpected version info.";
static ERROR_FSTORE_INVALID: &str = "Invalid file descriptor.";
static ERROR_FSTORE_INVSIZE: &str = "Unexpected data size encountered.";

/// Store manages a file store.
///
/// Data is written in blocks of arbitrary size.
///
/// Consult DataBlock for block details.
///
/// There is a 32bit checksum availible for each block.
///
pub struct Store  {
    /// File data resides in
    file: File,
    /// next read location
    //read_size: usize,
    /// the last stream position
    data_start_address: u64,
    block_addresses: Vec<u64>,
}

trait StoreReader {
    fn read_data_block(&mut self, data_block: &mut DataBlock) -> Result<(), Box<dyn std::error::Error>>;
    fn read(&mut self, data: &mut Vec<u8>) -> Result<usize, Error>;
}

impl Store {

    /// Open existing Store file
    ///
    /// Will return an error if the file is not a Store file
    pub fn new(filename: String) -> Result<Store, Box<dyn std::error::Error>> {
        let v =  File::open(filename)?;
        let mut st = Store { file: v, data_start_address: 0, block_addresses: Vec::new() };
        let fd = st.read_file_descriptor()?;
        if !Store::validate_file_descriptor(fd) {
            return Err(Box::new(Error::new(ErrorKind::InvalidData, ERROR_FSTORE_INVALID)))
        }
        st.index_blocks(0)?;
        Ok(st)
    }

    ///Create new Store file
    ///
    ///Will overwrite an existing store.
    pub fn create(filename: String) -> Result<Store, Error> {
        let mut f = File::create(filename)?;
        Store::write_file_descriptor(&mut f)?;
        Ok(Store { file: f, data_start_address: 0, block_addresses: Vec::new()})
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
        let mut buff = [0u8;4];
        let mut sz_buff = [0u8;8];
        self.file.read(&mut buff)?;
        self.file.read(&mut sz_buff)?;
        let mut str_buff= vec!(0u8; usize::try_from(u64::from_le_bytes(sz_buff)).unwrap());
        self.file.read(&mut str_buff)?;
        self.data_start_address = self.file.seek(SeekFrom::Current(0))?;
        //Convert this error into a somewhat relevant io::Error
        if let Ok(s) = String::from_utf8(str_buff) {
            Ok((u32::from_le_bytes(buff), s))
        } else {
            return Err(Error::new(ErrorKind::InvalidData,ERROR_FSTORE_VERSION))
        }
    }

    /// checks value to see if it's a valid file descriptor
    pub fn validate_file_descriptor(value: (u32, String)) -> bool {
        //NOTE: this should get more complicated when there are more versions;
        if value == (STORE_VERSIONNUM, STORE_VERSIONTAG.to_string()) {
            return true
        }
        false
    }

    /// Read address of blocks for index
    fn index_blocks(&mut self, startpos: u64) -> Result<(), Box<dyn std::error::Error>> {
        // if startpos is 0, set it to the first block, otherwise it's a valid block start
        // at this point, i'm failry sure an incorrect block location will still fill up a block
        // albeit with incorect info if  there is enough data in the file
        self.block_addresses.clear();
        let mut curpos = if startpos == 0 { self.data_start_address } else {startpos};
        // size of read ahead data
        let buffsize = DataBlock::read_ahead_size();
        // get metadata for file once
        let md = self.file.metadata()?;
        // Insert the first block address
        self.block_addresses.push(curpos);
        // We are assuming the file will not change size during this loop
        while curpos < md.len() {
            //TODO: is it faster to reuse a buffer?
            let mut buffer = vec!(0u8; buffsize);
            // read the data, then pass it to dataBlock::read_ahead 
            self.file.read(&mut buffer)?;
            // TODO: I think this logic is wrong, we want a more generic way to do this.
            let tbs = DataBlock::read_ahead(&buffer)?;
            // update curpos with next DataBlock addess, then push that onto the list
            curpos = self.file.seek(SeekFrom::Current(tbs))?;
            self.block_addresses.push(curpos);
        }
        self.file.seek(SeekFrom::Start(self.data_start_address))?;
        Ok(())
    }
}

impl Write for Store {
    /// Writes data in buf to file, encapsulated in a DataBlock
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        if let Ok(mut bd) = DataBlock::new(buf,None) {
            self.file.write(bd.serialize())?;
            return self.file.write(&buf)
        } else {
            return Err(Error::new(ErrorKind::InvalidInput,ERROR_FSTORE_INVSIZE));
        }
    }

    /// Calls flush on self.file
    fn flush(&mut self) -> Result<(), Error> {
        self.file.flush()
    }
}

impl StoreReader for Store {

    /// Reads data into buf according to surrounding DataBlock
    fn read_data_block(&mut self, data_block: &mut DataBlock) -> Result<(), Box<dyn std::error::Error>> {
        let mut db_buf = vec![0u8; DataBlock::size()];
        self.file.read(&mut db_buf)?;
        data_block.deserialize(&db_buf)?;
        Ok(())
    }

    fn read(&mut self, data: &mut Vec<u8>) -> Result<usize ,Error> {
        self.file.read(data)
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::fstore::Store;
    use std::io::{ Write};
    use crate::fstore::data_block::DataBlock;

    fn fill_test_vector(data: &mut Vec<u8>) {
        data.append(&mut vec!(1,2,3,4,5,6,7,8,9,10,11,12,13,255));
    }
    #[test]
    fn can_write_to_store() {
        let mut s = Store::create("testout/store.st".to_string()).unwrap();
        let mut buf = vec!(0, 1, 3, 4, 5, 11, 33, 0);
        s.write(&mut buf).unwrap();
        s.write(&mut buf).unwrap();
    }

    #[test]
    fn can_read_from_store() {
        let mut testval = Vec::new();
        fill_test_vector(&mut testval);
        {
            let mut s = Store::create("testout/store.test.st".to_string()).unwrap();
            for _i in 1..10   {
            s.write(&testval).unwrap();
            s.write(&testval).unwrap();
            }
            
        }

        let mut db = DataBlock::new(&[0u8], None).unwrap();
        let mut s = Store::new("testout/store.test.st".to_string()).unwrap();
        s.read_data_block(&mut db).unwrap();
        let mut data = vec!(0u8; db.data_size().unwrap());
        s.read(&mut data).unwrap();
        assert_eq!(testval, data);
    }
}
