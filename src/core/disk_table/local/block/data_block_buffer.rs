use std::cell::RefCell;

use super::block;
use crate::core::disk_table::local::writer_local_disk_table::WriterFlexibleDiskTablePtr;
use crate::core::{
    entry::flexible_user_entry::FlexibleUserEntry, marshal::write_u32,
    storage::config::DEFAULT_DATA_BLOCK_SIZE,
};
use crate::errors::Result;

pub const ENTRY_METADATA_SIZE: u32 = 2 * size_of::<u32>() as u32;

fn block_entry_size(entry: &FlexibleUserEntry) -> usize {
    ENTRY_METADATA_SIZE as usize + entry.size()
}

struct Metadata {
    index_entries: Vec<u32>,
    count_entries: u32,
}

impl Metadata {
    fn new() -> Self {
        Self {
            index_entries: Vec::new(),
            count_entries: 0,
        }
    }

    fn size(&self) -> usize {
        size_of::<u32>() + size_of::<u32>() * self.index_entries.len()
    }

    fn size_with_entry(&self) -> usize {
        self.size() + size_of::<u32>()
    }

    fn append(&mut self, offset: u32) {
        self.count_entries += 1;
        self.index_entries.push(offset);
    }

    fn reset(&mut self) {
        self.count_entries = 0;
        self.index_entries.clear();
    }

    fn serialize_to(&self, buffer: &mut [u8]) -> Result<()> {
        let mut offset = 0;
        for index in &self.index_entries {
            write_u32(&mut buffer[offset..], *index)?;
            offset += size_of::<u32>();
        }
        write_u32(&mut buffer[offset..], self.count_entries)?;
        Ok(())
    }
}

pub struct DataBlockBuffer {
    block_data: RefCell<Vec<u8>>,
    max_size: usize,
    current_pos: usize,

    meta: Metadata,
}

impl DataBlockBuffer {
    pub fn new() -> Self {
        Self {
            // @todo change
            block_data: RefCell::new(vec![0u8; DEFAULT_DATA_BLOCK_SIZE as usize]),
            max_size: DEFAULT_DATA_BLOCK_SIZE,
            current_pos: 0,
            meta: Metadata::new(),
        }
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

    fn possible_append(&self, entry: &FlexibleUserEntry) -> bool {
        self.current_pos + block_entry_size(entry) < self.max_size - self.meta.size_with_entry()
    }

    pub fn append(&mut self, entry: &FlexibleUserEntry) -> crate::errors::Result<usize> {
        if !self.possible_append(entry) {
            return Ok(0);
        }

        let mut dst = self.block_data.borrow_mut();
        let entry_bytes = entry.serialize_to(&mut dst[self.current_pos..])? as usize;

        assert_eq!(block_entry_size(entry), entry_bytes);

        self.meta.append(self.current_pos as u32);
        self.current_pos += entry_bytes;

        Ok(entry_bytes)
    }

    pub fn reset(&mut self) {
        let mut old = self
            .block_data
            .replace(vec![0u8; DEFAULT_DATA_BLOCK_SIZE as usize]);
        old.clear();

        self.current_pos = 0;

        self.meta.reset();
    }
}

impl block::WriteToTable for DataBlockBuffer {
    fn write_to(&self, ptr: &mut WriterFlexibleDiskTablePtr) -> Result<()> {
        let offset = self.max_size - self.meta.size();

        let mut dst = self.block_data.borrow_mut();
        self.meta.serialize_to(&mut dst[offset..])?;
        ptr.write(dst.as_slice())?;

        Ok(())
    }
}
