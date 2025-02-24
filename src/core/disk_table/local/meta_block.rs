use std::fs;
use std::io::Read;

use crate::core::{
    field::{Field, FlexibleField},
    marshal::{read_u32, write_u32},
    storage::config,
};

use super::writer_disk_table::WriterFlexibleDiskTablePtr;
use crate::errors::Result;

pub type Offsets = Vec<Offset>;

pub(super) const INDEX_BLOCK_OFFSET: usize = size_of::<u32>();
pub(super) const INDEX_BLOCK_SIZE: usize = size_of::<u32>();
pub(super) const INDEX_BLOCK_KEY_SIZE: usize = size_of::<u32>();
pub(super) const INDEX_BLOCKS_OFFSET_SIZE: usize =
    INDEX_BLOCK_OFFSET + INDEX_BLOCK_SIZE + INDEX_BLOCK_KEY_SIZE;
pub(super) const INDEX_BLOCKS_COUNT_SIZE: usize = size_of::<u32>();
pub(super) const INDEX_BLOCKS_BASE: usize = size_of::<u32>();

pub(super) const INDEX_ENTRIES_OFFSET_SIZE: usize = size_of::<u32>();
pub(super) const INDEX_ENTRIES_LEN_SIZE: usize = size_of::<u32>();
pub(super) const INDEX_ENTRIES_SIZE: usize = INDEX_ENTRIES_OFFSET_SIZE + INDEX_ENTRIES_LEN_SIZE;
pub(super) const INDEX_ENTRIES_COUNT_SIZE: usize = size_of::<u32>();

pub struct Offset {
    pub pos: u32,
    pub size: u32,
}

pub type IndexBlocks = Vec<IndexBlock>;

pub fn index_blocks_sizes(value: &IndexBlocks) -> u32 {
    let mut result = 0;
    for v in value {
        result += v.size();
    }
    result
}

pub struct IndexBlock {
    pub block_offset: u32,
    pub block_size: u32,
    pub key_size: u32,
    pub key: FlexibleField,
}

impl IndexBlock {
    // @todo depends on seek position
    pub fn from(fd: &mut fs::File) -> Result<Self> {
        let mut buffer = [0u8; INDEX_BLOCKS_OFFSET_SIZE];

        let bytes = fd.read(&mut buffer)?;
        assert_eq!(bytes, INDEX_BLOCKS_OFFSET_SIZE);

        let block_offset = read_u32(&buffer[..INDEX_BLOCK_OFFSET])?;
        let block_size = read_u32(&buffer[INDEX_BLOCK_OFFSET..])?;
        let key_size = read_u32(&buffer[INDEX_BLOCK_OFFSET + INDEX_BLOCK_SIZE..])?;

        let mut buffer = vec![0u8; key_size as usize];

        let bytes = fd.read(&mut buffer)?;
        assert_eq!(bytes, key_size as usize);

        let key = FlexibleField::new(buffer);

        assert_eq!(block_size, config::DEFAULT_DATA_BLOCK_SIZE as u32);

        Ok(IndexBlock {
            block_offset,
            block_size,
            key_size,
            key,
        })
    }

    pub fn size(&self) -> u32 {
        (3 * size_of::<u32>()) as u32 + self.key_size
    }

    pub fn write_to(&self, ptr: &mut WriterFlexibleDiskTablePtr) -> Result<()> {
        let mut tmp = [0; INDEX_BLOCKS_OFFSET_SIZE];

        write_u32(&mut tmp[0..INDEX_BLOCK_OFFSET], self.block_offset)?;
        write_u32(&mut tmp[INDEX_BLOCK_OFFSET..], self.block_size)?;
        write_u32(
            &mut tmp[INDEX_BLOCK_OFFSET + INDEX_BLOCK_SIZE..],
            self.key_size,
        )?;
        ptr.write(&tmp)?;

        assert_eq!(self.block_size, config::DEFAULT_DATA_BLOCK_SIZE as u32);

        ptr.write(self.key.data())?;

        Ok(())
    }
}
