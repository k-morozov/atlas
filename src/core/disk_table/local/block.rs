use crate::core::{
    entry::flexible_entry::FlexibleEntry, storage::config::DEFAULT_DATA_BLOCK_SIZE,
};

pub const ENTRY_METADATA_OFFSET: u32 = 2 * size_of::<u32>() as u32;

pub(super) const INDEX_BLOCK_OFFSET: usize = size_of::<u32>();
pub(super) const INDEX_BLOCK_SIZE: usize = size_of::<u32>();
pub(super) const INDEX_BLOCKS_OFFSET_SIZE: usize = INDEX_BLOCK_OFFSET + INDEX_BLOCK_SIZE;
pub(super) const INDEX_BLOCKS_COUNT_SIZE: usize = size_of::<u32>();

pub(super) const INDEX_ENTRIES_OFFSET_SIZE: usize = size_of::<u32>();
pub(super) const INDEX_ENTRIES_LEN_SIZE: usize = size_of::<u32>();
pub(super) const INDEX_ENTRIES_SIZE: usize = INDEX_ENTRIES_OFFSET_SIZE + INDEX_ENTRIES_LEN_SIZE;
pub(super) const INDEX_ENTRIES_COUNT_SIZE: usize = size_of::<u32>();

pub struct DataBlock {
    block_data: Vec<u8>,
    max_size: usize,
    current_pos: usize,
}

impl DataBlock {
    pub fn new() -> Self {
        Self {
            // @todo change
            block_data: vec![0u8; DEFAULT_DATA_BLOCK_SIZE as usize],
            max_size: DEFAULT_DATA_BLOCK_SIZE,
            current_pos: 0,
        }
    }

    pub fn data(&self) -> &[u8] {
        &self.block_data
    }

    pub fn empty(&self) -> bool {
        self.current_pos == 0
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }

    pub fn current_size(&self) -> usize {
        self.current_pos
    }

    pub fn remaining_size(&self) -> usize {
        self.max_size - self.current_pos
    }

    fn possible_append(&self, entry: &FlexibleEntry) -> bool {
        self.current_pos + ENTRY_METADATA_OFFSET as usize + entry.size() < self.max_size
    }

    pub fn append(&mut self, entry: &FlexibleEntry) -> crate::errors::Result<usize> {
        if !self.possible_append(entry) {
            return Ok(0);
        }

        let dst = self.block_data.as_mut_slice();
        let entry_bytes = entry.serialize_to(&mut dst[self.current_pos..])? as usize;
        self.current_pos += entry_bytes;

        Ok(entry_bytes)
    }

    pub fn reset(&mut self) {
        self.block_data.clear();
        self.block_data = vec![0u8; DEFAULT_DATA_BLOCK_SIZE as usize];

        self.current_pos = 0;
    }
}
