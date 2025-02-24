use crate::core::{entry::flexible_entry::FlexibleEntry, storage::config::DEFAULT_DATA_BLOCK_SIZE};

pub const ENTRY_METADATA_SIZE: u32 = 2 * size_of::<u32>() as u32;

fn block_entry_size(entry: &FlexibleEntry) -> usize {
    ENTRY_METADATA_SIZE as usize + entry.size()
}

pub struct DataBlock {
    block_data: Vec<u8>,
    max_size: usize,
    current_pos: usize,

    meta: Metadata,
}

struct Metadata {
    count_entries: u32,
    offsets: Vec<u32>,
}

impl Metadata {
    fn new() -> Self {
        Self {
            count_entries: 0,
            offsets: Vec::new(),
        }
    }

    fn size(&self) -> usize {
        size_of::<u32>() + size_of::<u32>() * self.offsets.len()
    }

    fn size_with_entry(&self) -> usize {
        self.size() + size_of::<u32>()
    }

    fn append(&mut self, offset: u32) {
        self.offsets.push(offset);
        self.count_entries += 1;
    }

    fn reset(&mut self) {
        self.count_entries = 0;
        self.offsets.clear();
    }
}

impl DataBlock {
    pub fn new() -> Self {
        Self {
            // @todo change
            block_data: vec![0u8; DEFAULT_DATA_BLOCK_SIZE as usize],
            max_size: DEFAULT_DATA_BLOCK_SIZE,
            current_pos: 0,
            meta: Metadata::new(),
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
        self.current_pos + block_entry_size(entry) < self.max_size - self.meta.size_with_entry()
    }

    pub fn append(&mut self, entry: &FlexibleEntry) -> crate::errors::Result<usize> {
        if !self.possible_append(entry) {
            return Ok(0);
        }

        let dst = self.block_data.as_mut_slice();
        let entry_bytes = entry.serialize_to(&mut dst[self.current_pos..])? as usize;

        assert_eq!(block_entry_size(entry), entry_bytes);

        self.meta.append(self.current_pos as u32);
        self.current_pos += entry_bytes;

        Ok(entry_bytes)
    }

    pub fn reset(&mut self) {
        self.block_data.clear();
        self.block_data = vec![0u8; DEFAULT_DATA_BLOCK_SIZE as usize];

        self.current_pos = 0;

        self.meta.reset();
    }
}
