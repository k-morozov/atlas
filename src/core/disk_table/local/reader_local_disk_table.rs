use std::cell::RefCell;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::core::disk_table::local::block::{data_block, data_block_buffer, meta_block};
use crate::core::marshal::read_u32;
use crate::core::{
    disk_table::disk_table, entry::flexible_user_entry::FlexibleUserEntry, field::FlexibleField,
};
use crate::errors::Result;

pub type ReaderDiskTablePtr = disk_table::ReaderDiskTablePtr<FlexibleField, FlexibleField>;

pub struct ReaderFlexibleDiskTable {
    disk_table_path: PathBuf,
    fd: Mutex<RefCell<fs::File>>,
    count_entries: u32,
    entries_offsets: meta_block::Offsets,
    index_blocks: meta_block::IndexBlocks,
}

impl ReaderFlexibleDiskTable {
    pub(super) fn new<P: AsRef<Path>>(disk_table_path: P) -> ReaderDiskTablePtr {
        let mut fd = match fs::File::open(disk_table_path.as_ref()) {
            Ok(fd) => fd,
            Err(er) => panic!(
                "FlexibleReader: error={}, path={}",
                er,
                disk_table_path.as_ref().display()
            ),
        };

        // read entries offsets
        let _offset = fd.seek(SeekFrom::End(
            -(meta_block::INDEX_ENTRIES_COUNT_SIZE as i64),
        ));

        let mut buffer = [0u8; meta_block::INDEX_ENTRIES_COUNT_SIZE];
        let Ok(bytes) = fd.read(&mut buffer) else {
            panic!("Failed read from disk")
        };
        assert_eq!(bytes, meta_block::INDEX_ENTRIES_COUNT_SIZE);

        let count_entries = u32::from_le_bytes(buffer);
        let Ok(entries_offsets) =
            ReaderFlexibleDiskTable::read_index_entries(&mut fd, count_entries)
        else {
            panic!(
                "Failed read entries_offsets from {}",
                disk_table_path.as_ref().display()
            )
        };

        assert_ne!(entries_offsets.len(), 0);

        let base = (meta_block::INDEX_ENTRIES_COUNT_SIZE
            + count_entries as usize * meta_block::INDEX_ENTRIES_SIZE) as i64;

        let (offset_index_blocks, count_blocks) = meta_block::metadata_index_blocks(base, &mut fd);

        let Ok(index_blocks) =
            ReaderFlexibleDiskTable::read_index_blocks(&mut fd, offset_index_blocks, count_blocks)
        else {
            panic!(
                "Failed read index block from {}",
                disk_table_path.as_ref().display()
            )
        };

        assert_ne!(index_blocks.len(), 0);
        assert_ne!(index_blocks.size(), 0);

        Arc::new(Self {
            disk_table_path: disk_table_path.as_ref().to_path_buf(),
            fd: Mutex::new(RefCell::new(fd)),
            count_entries,
            entries_offsets,
            index_blocks,
        })
    }

    fn read_index_entries(fd: &mut fs::File, count_entries: u32) -> Result<meta_block::Offsets> {
        let _offset = fd.seek(SeekFrom::End(
            -(meta_block::INDEX_ENTRIES_COUNT_SIZE as i64
                + count_entries as i64 * meta_block::INDEX_ENTRIES_SIZE as i64),
        ))?;

        let mut index_entries = meta_block::Offsets::new();
        index_entries.reserve(count_entries as usize);

        for _entry_offset in 0..count_entries {
            let mut buffer = [0u8; meta_block::INDEX_ENTRIES_SIZE];

            let bytes = fd.read(&mut buffer)?;
            assert_eq!(bytes, meta_block::INDEX_ENTRIES_SIZE);
            let entry_offset = read_u32(&buffer[..meta_block::INDEX_ENTRIES_OFFSET_SIZE])?;
            let entry_size = read_u32(&buffer[meta_block::INDEX_ENTRIES_OFFSET_SIZE..])?;

            assert_ne!(entry_size, 0);

            index_entries.push(meta_block::Offset {
                pos: entry_offset,
                size: entry_size,
            });
        }

        Ok(index_entries)
    }

    fn read_index_blocks(
        fd: &mut fs::File,
        start_offset: i64,
        count_blocks: u32,
    ) -> Result<meta_block::IndexBlocks> {
        let _offset = fd.seek(SeekFrom::End(-(start_offset)))?;

        let mut index_blocks = meta_block::IndexBlocks::with_capacity(count_blocks as usize);

        for _ in 0..count_blocks {
            index_blocks.append(meta_block::IndexBlock::from(fd)?);
        }

        Ok(index_blocks)
    }
}

impl disk_table::ReaderDiskTable<FlexibleField, FlexibleField> for ReaderFlexibleDiskTable {}

impl disk_table::DiskTable<FlexibleField, FlexibleField> for ReaderFlexibleDiskTable {
    fn get_path(&self) -> &Path {
        self.disk_table_path.as_path()
    }

    fn remove(&self) -> Result<()> {
        fs::remove_file(self.disk_table_path.as_path())?;
        Ok(())
    }
}

impl disk_table::Reader<FlexibleField, FlexibleField> for ReaderFlexibleDiskTable {
    fn read(&self, key: &FlexibleField) -> Result<Option<FlexibleField>> {
        let mut left = 0;
        let mut right = self.index_blocks.len();

        let result = loop {
            if !(left < right) {
                break None;
            }
            let mid = (left + right) / 2;
            let index = self.index_blocks.get_by_index(mid);

            match index.key.cmp(key) {
                std::cmp::Ordering::Less => {
                    if left + 1 == right {
                        break Some(mid);
                    }
                    left = mid + 1;
                }
                std::cmp::Ordering::Equal => {
                    break Some(mid);
                }
                std::cmp::Ordering::Greater => {
                    if left + 1 == right {
                        break Some(mid);
                    }
                    right = mid + 1;
                }
            };
        };

        match result {
            Some(index) => {
                let index_block = self.index_blocks.get_by_index(index);

                let block = data_block::DataBlock::new(
                    &mut self.fd.lock().unwrap().borrow_mut(),
                    index_block.block_offset,
                    index_block.block_size,
                );
                return Ok(block.get_by_key(key));
            }
            None => Ok(None),
        }
    }

    fn read_entry_by_index(&self, index: u32) -> Result<Option<FlexibleUserEntry>> {
        let Some(offset) = self.entries_offsets.get(index as usize) else {
            panic!("Something wrong: index {} must's be here", index)
        };

        let buffer = {
            let lock = self.fd.lock().unwrap();

            let _offset = lock.borrow_mut().seek(SeekFrom::Start(offset.pos as u64))?;

            assert_ne!(offset.size, 0);

            // read row with entry
            let mut buffer = vec![0u8; offset.size as usize];
            let bytes = lock.borrow_mut().read(&mut buffer)?;
            assert_eq!(bytes, offset.size as usize);

            buffer
        };

        // parse metadata
        let key_len = read_u32(&buffer[0..])? as usize;
        assert_ne!(key_len, 0);

        let value_len = read_u32(&buffer[size_of::<u32>() as usize..])?;
        assert_ne!(value_len, 0);

        let (buffer_with_key, buffer_value) =
            buffer.split_at(data_block_buffer::ENTRY_METADATA_SIZE as usize + key_len);

        let key_buffer = &buffer_with_key[data_block_buffer::ENTRY_METADATA_SIZE as usize
            ..data_block_buffer::ENTRY_METADATA_SIZE as usize + key_len];
        let value_buffer = &buffer_value[..];

        return Ok(Some(FlexibleUserEntry::new(
            FlexibleField::new(key_buffer.to_vec()),
            FlexibleField::new(value_buffer.to_vec()),
        )));
    }

    fn count_entries(&self) -> u32 {
        self.count_entries
    }
}
