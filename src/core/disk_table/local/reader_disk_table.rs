use std::cell::RefCell;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::core::entry::entry::ENTRY_METADATA_OFFSET;
use crate::core::marshal::read_u32;
use crate::core::{
    disk_table::{disk_table, local::offset::Offset},
    entry::flexible_entry::FlexibleEntry,
    field::FlexibleField,
};
use crate::errors::Result;

use super::offset::Offsets;

pub type ReaderFlexibleDiskTablePtr = disk_table::ReaderDiskTablePtr<FlexibleField, FlexibleField>;

pub struct ReaderFlexibleDiskTable {
    disk_table_path: PathBuf,
    fd: RefCell<fs::File>,
    count_entries: u32,
    offsets: Offsets,
}

impl ReaderFlexibleDiskTable {
    pub(super) fn new<P: AsRef<Path>>(disk_table_path: P) -> ReaderFlexibleDiskTablePtr {
        let mut fd = match fs::File::open(disk_table_path.as_ref()) {
            Ok(fd) => fd,
            Err(er) => panic!(
                "FlexibleReader: error={}, path={}",
                er,
                disk_table_path.as_ref().display()
            ),
        };

        let _offset = fd.seek(SeekFrom::End(-(size_of::<u32>() as i64)));

        let mut buffer = [0u8; size_of::<u32>()];
        let Ok(bytes) = fd.read(&mut buffer) else {
            panic!("Failed read from disk")
        };

        assert_eq!(bytes, size_of::<u32>());

        let count_entries = u32::from_le_bytes(buffer);
        let Ok(offsets) = ReaderFlexibleDiskTable::read_offsets(&mut fd, count_entries) else {
            panic!(
                "Failed read offsets from {}",
                disk_table_path.as_ref().display()
            )
        };

        Box::new(Self {
            disk_table_path: disk_table_path.as_ref().to_path_buf(),
            fd: RefCell::new(fd),
            count_entries,
            offsets,
        })
    }

    fn read_offsets(fd: &mut fs::File, count_entries: u32) -> Result<Offsets> {
        // @todo to block
        let size_entry_offsets = size_of::<u32>() as i64;
        let _offset = fd.seek(SeekFrom::End(
            -(size_of::<u32>() as i64 + count_entries as i64 * size_entry_offsets),
        ))?;
        let mut index_entries = Offsets::new();
        index_entries.reserve(size_entry_offsets as usize);

        for _entry_offset in 0..count_entries {
            let mut buffer = [0u8; size_of::<u32>()];

            let bytes = fd.read(&mut buffer)?;
            assert_eq!(bytes, size_of::<u32>());
            let entry_offset = read_u32(&buffer)?;

            index_entries.push(Offset { pos: entry_offset });
        }

        Ok(index_entries)
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
        let mut right = self.count_entries();

        while left < right {
            let mid = (left + right) / 2;

            let r = self.read_entry_by_index(mid)?;
            assert!(r.is_some());

            let r = r.unwrap();

            match r.get_key().cmp(key) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Equal => return Ok(Some(r.get_value().clone())),
                std::cmp::Ordering::Greater => right = mid,
            }
        }

        Ok(None)
    }

    fn read_entry_by_index(&self, index: u32) -> Result<Option<FlexibleEntry>> {
        let Some(offset) = self.offsets.get(index as usize) else {
            return Ok(None);
        };

        let _offset = self
            .fd
            .borrow_mut()
            .seek(SeekFrom::Start(offset.pos as u64))?;

        // read metadata
        let mut metadata = vec![0u8; ENTRY_METADATA_OFFSET as usize];
        let bytes = self.fd.borrow_mut().read(&mut metadata)?;
        assert_eq!(bytes, ENTRY_METADATA_OFFSET as usize);
        let key_len = read_u32(&metadata)?;
        let value_len = read_u32(&metadata[size_of::<u32>() as usize..])?;

        // read key
        let mut key_buffer = vec![0u8; key_len as usize];
        let bytes = self.fd.borrow_mut().read(&mut key_buffer)?;
        assert_eq!(bytes, key_len as usize);

        // read value
        let mut value_buffer = vec![0u8; value_len as usize];
        let bytes = self.fd.borrow_mut().read(&mut value_buffer)?;
        assert_eq!(bytes, value_len as usize);

        return Ok(Some(FlexibleEntry::new(
            FlexibleField::new(key_buffer),
            FlexibleField::new(value_buffer),
        )));
    }

    fn count_entries(&self) -> u32 {
        self.count_entries
    }
}
