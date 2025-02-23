use crate::core::storage::config::DEFAULT_DISK_ERASE_BLOCK_SIZE;
use std::ptr;

pub const ENTRY_METADATA_OFFSET: u32 = 2 * size_of::<u32>() as u32;

pub(super) const INDEX_ENTRIES_OFFSET_SIZE: usize = size_of::<u32>();
pub(super) const INDEX_ENTRIES_LEN_SIZE: usize = size_of::<u32>();
pub(super) const INDEX_ENTRIES_SIZE: usize = INDEX_ENTRIES_OFFSET_SIZE + INDEX_ENTRIES_LEN_SIZE;

pub(super) const INDEX_COUNT_SIZE: usize = size_of::<u32>();

pub enum BlockAppendStatus {
    Appended,
    NotEnoughSpace,
}

pub struct DataBlock {
    block_data: Vec<u8>,
    max_size: usize,
    current_size: usize,
}

impl DataBlock {
    pub fn new() -> Self {
        Self {
            block_data: vec![0u8; DEFAULT_DISK_ERASE_BLOCK_SIZE as usize],
            max_size: DEFAULT_DISK_ERASE_BLOCK_SIZE,
            current_size: 0,
        }
    }

    pub fn data(&self) -> &[u8] {
        &self.block_data
    }

    pub fn current_size(&self) -> usize {
        self.current_size
    }

    pub fn remaining_size(&self) -> usize {
        self.max_size - self.current_size
    }

    pub fn possible_append(&self, bytes: usize) -> bool {
        self.current_size + bytes < self.max_size
    }

    pub fn append(&mut self, data: Vec<u8>) {
        assert!(self.possible_append(data.len()));

        let dst = self.block_data.as_mut_ptr();
        unsafe {
            ptr::copy_nonoverlapping(data.as_ptr(), dst.byte_add(self.current_size), data.len());
        }
        self.current_size += data.len();
    }

    pub fn reset(&mut self) {
        self.block_data.clear();
        self.block_data = vec![0u8; DEFAULT_DISK_ERASE_BLOCK_SIZE as usize];

        self.current_size = 0;
    }
}
