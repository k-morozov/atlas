use crate::core::{
    disk_table::local::file_handle::ReadSeek,
    field::{Field, FlexibleField},
    marshal::{read_u32, write_u32},
    storage::config,
};

use crate::errors::Result;

use super::block;

pub type Offsets = Vec<Offset>;

pub const INDEX_BLOCK_OFFSET: usize = size_of::<u32>();
pub const INDEX_BLOCK_SIZE: usize = size_of::<u32>();
pub const INDEX_BLOCK_KEY_SIZE: usize = size_of::<u32>();
pub const INDEX_BLOCKS_OFFSET_SIZE: usize =
    INDEX_BLOCK_OFFSET + INDEX_BLOCK_SIZE + INDEX_BLOCK_KEY_SIZE;
pub const INDEX_BLOCKS_COUNT_SIZE: usize = size_of::<u32>();
pub const INDEX_BLOCKS_BASE: usize = size_of::<u32>();

pub const INDEX_ENTRIES_OFFSET_SIZE: usize = size_of::<u32>();
pub const INDEX_ENTRIES_LEN_SIZE: usize = size_of::<u32>();
pub const INDEX_ENTRIES_SIZE: usize = INDEX_ENTRIES_OFFSET_SIZE + INDEX_ENTRIES_LEN_SIZE;
pub const INDEX_ENTRIES_COUNT_SIZE: usize = size_of::<u32>();

pub struct Offset {
    pub pos: u32,
    pub size: u32,
}

// todo result
pub fn metadata_index_blocks(base: i64, fd: &mut Box<dyn ReadSeek>) -> (i64, u32) {
    fd.seek(std::io::SeekFrom::End(
        -(base + INDEX_BLOCKS_COUNT_SIZE as i64),
    ))
    .unwrap();

    let mut buffer = [0u8; INDEX_BLOCKS_COUNT_SIZE];

    let Ok(bytes) = fd.read(&mut buffer) else {
        panic!("Failed read count index blocks from disk")
    };
    assert_eq!(bytes, INDEX_BLOCKS_COUNT_SIZE);

    let count_blocks = u32::from_le_bytes(buffer);

    // read count
    fd.seek(std::io::SeekFrom::End(
        -(base + INDEX_BLOCKS_COUNT_SIZE as i64 + INDEX_BLOCKS_BASE as i64),
    ))
    .unwrap();

    let mut buffer = [0u8; INDEX_BLOCKS_BASE];
    let Ok(bytes) = fd.read(&mut buffer) else {
        panic!("Failed read size of index blocks from disk")
    };
    assert_eq!(bytes, INDEX_BLOCKS_BASE);
    let size_blocks = u32::from_le_bytes(buffer);

    let offset_index_blocks =
        base + (INDEX_BLOCKS_COUNT_SIZE + INDEX_BLOCKS_BASE + size_blocks as usize) as i64;

    (offset_index_blocks, count_blocks)
}

pub struct IndexBlocks {
    data: Vec<IndexBlock>,
}

impl IndexBlocks {
    pub fn new() -> Self {
        Self {
            data: Vec::<IndexBlock>::new(),
        }
    }

    pub fn with_capacity(n: usize) -> Self {
        assert_ne!(n, 0);
        Self {
            data: Vec::<IndexBlock>::with_capacity(n),
        }
    }

    pub fn size(&self) -> u32 {
        let mut result = 0;
        for v in &self.data {
            result += v.size();
        }
        result
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn append(&mut self, index_to_block: IndexBlock) {
        self.data.push(index_to_block);
    }

    // @todo []
    pub fn get_by_index(&self, index: usize) -> &IndexBlock {
        &self.data[index]
    }
}

impl block::WriteToTable for IndexBlocks {
    fn write_to(&self, ptr: &mut Box<dyn std::io::Write>) -> Result<()> {
        assert_ne!(0, self.data.len());

        for index_block in &self.data {
            index_block.write_to(ptr)?;
        }

        ptr.write(&(self.size()).to_le_bytes())?;
        ptr.write(&(self.data.len() as u32).to_le_bytes())?;

        Ok(())
    }
}

pub struct IndexBlock {
    pub block_offset: u32,
    pub block_size: u32,
    pub key_size: u32,
    pub first_key: FlexibleField,
}

impl IndexBlock {
    // @todo depends on seek position
    pub fn from(fd: &mut Box<dyn ReadSeek>) -> Result<Self> {
        let mut buffer = [0u8; INDEX_BLOCKS_OFFSET_SIZE];

        let bytes = fd.read(&mut buffer)?;

        assert_eq!(bytes, INDEX_BLOCKS_OFFSET_SIZE);

        let block_offset = read_u32(&buffer[..INDEX_BLOCK_OFFSET])?;
        let block_size = read_u32(&buffer[INDEX_BLOCK_OFFSET..])?;
        let key_size = read_u32(&buffer[INDEX_BLOCK_OFFSET + INDEX_BLOCK_SIZE..])?;

        let mut buffer = vec![0u8; key_size as usize];

        let bytes = fd.read(&mut buffer)?;
        assert_eq!(bytes, key_size as usize);

        let first_key = FlexibleField::new(buffer);

        assert_eq!(block_size, config::DEFAULT_DATA_BLOCK_SIZE as u32);

        Ok(IndexBlock {
            block_offset,
            block_size,
            key_size,
            first_key,
        })
    }

    pub fn size(&self) -> u32 {
        (3 * size_of::<u32>()) as u32 + self.key_size
    }
}

impl block::WriteToTable for IndexBlock {
    fn write_to(&self, ptr: &mut Box<dyn std::io::Write>) -> Result<()> {
        let mut tmp = [0; INDEX_BLOCKS_OFFSET_SIZE];

        write_u32(&mut tmp[0..INDEX_BLOCK_OFFSET], self.block_offset)?;
        write_u32(&mut tmp[INDEX_BLOCK_OFFSET..], self.block_size)?;
        write_u32(
            &mut tmp[INDEX_BLOCK_OFFSET + INDEX_BLOCK_SIZE..],
            self.key_size,
        )?;
        ptr.write(&tmp)?;

        assert_eq!(self.block_size, config::DEFAULT_DATA_BLOCK_SIZE as u32);

        ptr.write(self.first_key.data())?;

        Ok(())
    }
}
