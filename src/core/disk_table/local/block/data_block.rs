use crate::core::{
    entry::flexible_user_entry::FlexibleUserEntry, field::FlexibleField, marshal::read_u32,
};
use std::{
    fs,
    io::{Read, Seek, SeekFrom},
};

pub struct DataBlock {
    _index_entries: Vec<u32>,
    data: Vec<FlexibleUserEntry>,
}

impl DataBlock {
    pub fn new(fd: &mut i32, block_offset: u32, block_size: u32) -> Self {
        nix::unistd::lseek(*fd, block_offset as i64, nix::unistd::Whence::SeekSet).unwrap();
        // let _base = fd.seek(SeekFrom::Start(block_offset as u64));

        let mut buffer = vec![0u8; block_size as usize];
        let Ok(bytes) = nix::unistd::read(*fd, &mut buffer) else {
            panic!("Failed read from disk")
        };

        assert_eq!(bytes, block_size as usize);

        let buffer_count_entries = &buffer[(block_size - size_of::<u32>() as u32) as usize..];
        let Ok(count_entries) = read_u32(&buffer_count_entries) else {
            panic!("Failed read count entires from block")
        };

        let mut index_entries = Vec::with_capacity(count_entries as usize);
        let mut metadata_offset = (block_size
            - size_of::<u32>() as u32
            - (count_entries * size_of::<u32>() as u32)) as usize;
        for _ in 0..count_entries {
            let Ok(index) = read_u32(&buffer[metadata_offset..]) else {
                panic!(
                    "Failed read index entires from block by metadata_offset {}",
                    metadata_offset
                )
            };
            metadata_offset += size_of::<u32>() as usize;
            index_entries.push(index);
        }

        let mut data = Vec::<FlexibleUserEntry>::with_capacity(count_entries as usize);

        for offset in &index_entries {
            let b = buffer.as_slice();
            data.push(FlexibleUserEntry::from(&b[*offset as usize..]));
        }

        Self {
            data,
            _index_entries: index_entries,
        }
    }

    pub fn get_by_key(&self, key: &FlexibleField) -> Option<FlexibleField> {
        let idx = self
            .data
            .binary_search_by(|entry| entry.get_key().cmp(key))
            .ok()?;

        Some(self.data[idx].get_value().clone())
    }

    pub fn get_by_index(&self, index: usize) -> &FlexibleUserEntry {
        &self.data[index]
    }

    pub fn into_iter(&self) -> impl Iterator<Item = &FlexibleUserEntry> {
        DataBlockIterator {
            block: self,
            pos: 0,
        }
    }
}

struct DataBlockIterator<'a> {
    block: &'a DataBlock,
    pos: usize,
}

impl<'a> Iterator for DataBlockIterator<'a> {
    type Item = &'a FlexibleUserEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.block._index_entries.len() {
            return None;
        }
        Some(
            self.block
                .get_by_index(self.block._index_entries[self.pos] as usize),
        )
    }
}
