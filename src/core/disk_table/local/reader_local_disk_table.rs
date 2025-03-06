use std::cell::RefCell;
use std::fs;
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use nix::fcntl::{self, OFlag};

use crate::core::disk_table::local::block::{data_block, data_block_buffer, meta_block};
use crate::core::marshal::read_u32;
use crate::core::{
    disk_table::disk_table, entry::flexible_user_entry::FlexibleUserEntry, field::FlexibleField,
};
use crate::errors::Result;

pub type ReaderDiskTablePtr = disk_table::ReaderDiskTablePtr<FlexibleField, FlexibleField>;

pub struct ReaderFlexibleDiskTable {
    disk_table_path: PathBuf,
    fd: Mutex<RefCell<RawFd>>,
    count_entries: u32,
    entries_offsets: meta_block::Offsets,
    index_blocks: meta_block::IndexBlocks,
}

// @todo drop
impl ReaderFlexibleDiskTable {
    pub(super) fn new<P: AsRef<Path>>(disk_table_path: P) -> Result<ReaderDiskTablePtr> {
        let fd: RawFd = fcntl::open(
            disk_table_path.as_ref(),
            OFlag::O_RDONLY,
            nix::sys::stat::Mode::empty(),
        )?;

        // read entries offsets
        nix::unistd::lseek(
            fd,
            -(meta_block::INDEX_ENTRIES_COUNT_SIZE as i64),
            nix::unistd::Whence::SeekEnd,
        )?;

        let mut buffer = [0u8; meta_block::INDEX_ENTRIES_COUNT_SIZE];

        match nix::unistd::read(fd, &mut buffer) {
            Ok(bytes) => assert_eq!(bytes, meta_block::INDEX_ENTRIES_COUNT_SIZE),
            Err(er) => return Err(er.into()),
        }

        let count_entries = u32::from_le_bytes(buffer);
        let entries_offsets = ReaderFlexibleDiskTable::read_index_entries(fd, count_entries)?;

        assert_ne!(entries_offsets.len(), 0);

        let base = (meta_block::INDEX_ENTRIES_COUNT_SIZE
            + count_entries as usize * meta_block::INDEX_ENTRIES_SIZE) as i64;

        let (offset_index_blocks, count_blocks) = meta_block::metadata_index_blocks(base, fd);

        let index_blocks =
            ReaderFlexibleDiskTable::read_index_blocks(fd, offset_index_blocks, count_blocks)?;

        assert_ne!(index_blocks.len(), 0);
        assert_ne!(index_blocks.size(), 0);

        Ok(Arc::new(Self {
            disk_table_path: disk_table_path.as_ref().to_path_buf(),
            fd: Mutex::new(RefCell::new(fd)),
            count_entries,
            entries_offsets,
            index_blocks,
        }))
    }

    fn read_index_entries(fd: RawFd, count_entries: u32) -> Result<meta_block::Offsets> {
        nix::unistd::lseek(
            fd,
            -(meta_block::INDEX_ENTRIES_COUNT_SIZE as i64
                + count_entries as i64 * meta_block::INDEX_ENTRIES_SIZE as i64),
            nix::unistd::Whence::SeekEnd,
        )?;

        let mut index_entries = meta_block::Offsets::new();
        index_entries.reserve(count_entries as usize);

        for _entry_offset in 0..count_entries {
            let mut buffer = [0u8; meta_block::INDEX_ENTRIES_SIZE];

            let bytes = nix::unistd::read(fd, &mut buffer)?;
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
        fd: RawFd,
        start_offset: i64,
        count_blocks: u32,
    ) -> Result<meta_block::IndexBlocks> {
        nix::unistd::lseek(fd, -(start_offset), nix::unistd::Whence::SeekEnd)?;

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

            match index.first_key.cmp(key) {
                std::cmp::Ordering::Less => {
                    if left + 1 == right {
                        break Some(mid);
                    }
                    left = mid;
                }
                std::cmp::Ordering::Equal => {
                    break Some(mid);
                }
                std::cmp::Ordering::Greater => {
                    if left + 1 == right {
                        break None;
                    }
                    right = mid;
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

            nix::unistd::lseek(
                *lock.borrow_mut(),
                offset.pos as i64,
                nix::unistd::Whence::SeekSet,
            )?;

            assert_ne!(offset.size, 0);

            // read row with entry
            let mut buffer = vec![0u8; offset.size as usize];

            let bytes = nix::unistd::read(*lock.borrow_mut(), &mut buffer)?;
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
