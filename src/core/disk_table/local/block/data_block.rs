use crate::{
    common::memory::alloc_aligned,
    core::{
        disk_table::local::file_handle::ReadSeek,
        entry::{flexible_user_entry::FlexibleUserEntry, user_entry},
        field::{Field, FieldSize, FlexibleField},
        marshal::read_u32,
        storage::config::{DEFAULT_DATA_BLOCK_ALIGN, DEFAULT_DATA_BLOCK_SIZE},
    },
};
use std::io::{Read, Seek, SeekFrom};

pub struct DataBlock<K, V> {
    _index_entries: Vec<u32>,
    data: Vec<user_entry::UserEntry<K, V>>,
}

impl<'a, K, V> DataBlock<K, V>
where
    K: Field + FieldSize + Ord + PartialEq + Eq + PartialOrd + Clone,
    V: Field + FieldSize + Clone,
{
    pub fn new(fd: &mut Box<dyn ReadSeek>, block_offset: u32, block_size: u32) -> Self {
        let _base = fd.seek(SeekFrom::Start(block_offset as u64));

        let mut buffer = alloc_aligned(DEFAULT_DATA_BLOCK_SIZE, DEFAULT_DATA_BLOCK_ALIGN);

        let Ok(bytes) = fd.read(&mut buffer) else {
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

        let mut data = Vec::<user_entry::UserEntry<K, V>>::with_capacity(count_entries as usize);

        for offset in &index_entries {
            let b = buffer.as_slice();
            data.push(user_entry::UserEntry::from(&b[*offset as usize..]));
        }

        Self {
            data,
            _index_entries: index_entries,
        }
    }

    pub fn get_by_key(&self, key: &K) -> Option<V> {
        let idx = self
            .data
            .binary_search_by(|entry| entry.get_key().cmp(key))
            .ok()?;

        Some(self.data[idx].get_value().clone())
    }

    pub fn get_by_index(&self, index: usize) -> &user_entry::UserEntry<K, V> {
        &self.data[index]
    }

    pub fn into_iter(self) -> impl Iterator<Item = user_entry::UserEntry<K, V>> {
        DataBlockIterator {
            block: self,
            pos: 0,
        }
    }
}

pub struct DataBlockIterator<K, V> {
    block: DataBlock<K, V>,
    pos: usize,
}

impl<'a, K, V> Iterator for DataBlockIterator<K, V>
where
    K: Field + FieldSize + Ord + PartialEq + Eq + PartialOrd + Clone,
    V: Field + FieldSize + Clone,
{
    type Item = user_entry::UserEntry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.block._index_entries.len() {
            return None;
        }

        let r = self
            .block
            .get_by_index(self.block._index_entries[self.pos] as usize);

        Some(r.clone())
    }
}
