use std::{
    cell::RefCell,
    fs,
    io::{Read, Seek, SeekFrom},
};

use crate::core::{
    entry::flexible_entry::FlexibleEntry,
    field::FlexibleField,
    marshal::{read_u32, write_data, write_u32},
    storage::config::DEFAULT_DATA_BLOCK_SIZE,
};
use crate::errors::Result;

use super::{block, writer_disk_table::WriterFlexibleDiskTablePtr};

pub const ENTRY_METADATA_SIZE: u32 = 2 * size_of::<u32>() as u32;

fn block_entry_size(entry: &FlexibleEntry) -> usize {
    ENTRY_METADATA_SIZE as usize + entry.size()
}

pub struct DataBlock {
    block_data: RefCell<Vec<u8>>,
    max_size: usize,
    current_pos: usize,

    meta: Metadata,
}

struct Metadata {
    count_entries: u32,
}

impl Metadata {
    fn new() -> Self {
        Self {
            count_entries: 0,
        }
    }

    fn size(&self) -> usize {
        size_of::<u32>()
    }

    fn size_with_entry(&self) -> usize {
        self.size() + size_of::<u32>()
    }

    fn append(&mut self, _offset: u32) {
        self.count_entries += 1;
    }

    fn reset(&mut self) {
        self.count_entries = 0;
    }

    fn serialize_to(&self, buffer: &mut [u8]) -> Result<()> {
        write_u32(buffer, self.count_entries)?;
        Ok(())
    }
}

impl DataBlock {
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

    fn possible_append(&self, entry: &FlexibleEntry) -> bool {
        self.current_pos + block_entry_size(entry) < self.max_size - self.meta.size_with_entry()
    }

    pub fn append(&mut self, entry: &FlexibleEntry) -> crate::errors::Result<usize> {
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

impl block::WriteToTable for DataBlock {
    fn write_to(&self, ptr: &mut WriterFlexibleDiskTablePtr) -> Result<()> {
        let offset = self.max_size - self.meta.size();

        let mut dst = self.block_data.borrow_mut();
        self.meta.serialize_to(&mut dst[offset..])?;
        ptr.write(dst.as_slice())?;

        Ok(())
    }
}

pub struct ReadDataBlock {
    data: Vec<FlexibleEntry>,
}

impl ReadDataBlock {
    pub fn new(fd: &mut fs::File, block_offset: u32, block_size: u32) -> Self {
        let _base = fd.seek(SeekFrom::Start(block_offset as u64));

        let mut buffer = vec![0u8; block_size as usize];
        let Ok(bytes) = fd.read(&mut buffer) else {
            panic!("Failed read from disk")
        };

        assert_eq!(bytes, block_size as usize);

        let buffer_count_entries = &buffer[(block_size - size_of::<u32>() as u32) as usize..];
        let Ok(count_entries) = read_u32(&buffer_count_entries) else {
            panic!("Failed read count entires from block")
        };

        let mut offset = 0;
        let mut result = Vec::<FlexibleEntry>::with_capacity(count_entries as usize);

        for _ in 0..count_entries {
            let key_len = read_u32(&buffer[offset..]).unwrap() as usize;
            assert_ne!(key_len, 0);
            offset += size_of::<u32>() as usize;

            let value_len = read_u32(&buffer[offset..]).unwrap() as usize;
            assert_ne!(value_len, 0);
            offset += size_of::<u32>() as usize;

            let mut k = vec![0u8; key_len];
            write_data(&mut k, &buffer[offset..], key_len).unwrap();
            offset += key_len;

            let mut v = vec![0u8; value_len];
            write_data(&mut v, &buffer[offset..], value_len).unwrap();
            offset += value_len;

            result.push(FlexibleEntry::new(
                FlexibleField::new(k),
                FlexibleField::new(v),
            ));
        }

        Self { data: result }
    }

    pub fn get(&self, key: &FlexibleField) -> Option<FlexibleField> {
        let idx = self
            .data
            .binary_search_by(|entry| entry.get_key().cmp(key))
            .ok()?;

        Some(self.data[idx].get_value().clone())
    }
}
