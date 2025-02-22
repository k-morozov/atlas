use std::path::{Path, PathBuf};

use super::reader_disk_table::{ReaderFlexibleDiskTable, ReaderFlexibleDiskTablePtr};
use super::writer_disk_table::{WriterFlexibleDiskTable, WriterFlexibleDiskTablePtr};
use crate::core::entry::flexible_entry::FlexibleEntry;
// use crate::core::storage::config::DEFAULT_DISK_ERASE_BLOCK_SIZE;

pub struct DiskTableBuilder {
    disk_table_path: PathBuf,
    building_disk_table: Option<WriterFlexibleDiskTablePtr>,
    // _buffer: [u8; DEFAULT_DISK_ERASE_BLOCK_SIZE],
}

impl DiskTableBuilder {
    pub fn new<P: AsRef<Path>>(disk_table_path: P) -> Self {
        DiskTableBuilder {
            disk_table_path: disk_table_path.as_ref().to_path_buf(),
            building_disk_table: Some(WriterFlexibleDiskTable::new(disk_table_path)),
            // _buffer: [0u8; DEFAULT_DISK_ERASE_BLOCK_SIZE],
        }
    }

    pub fn from<P: AsRef<Path>>(disk_table_path: P) -> Self {
        DiskTableBuilder {
            disk_table_path: disk_table_path.as_ref().to_path_buf(),
            building_disk_table: None,
            // _buffer: [0u8; DEFAULT_DISK_ERASE_BLOCK_SIZE],
        }
    }

    pub fn append_entry(&mut self, entry: &FlexibleEntry) -> &mut Self {
        // convert to bytes
        // add to buffer
        // after 4MB -> write

        match &mut self.building_disk_table {
            Some(ptr) => {
                if let Err(er) = ptr.write(entry) {
                    panic!("Failed write in builder: {}", er)
                }
                self
            }
            None => panic!("Failed write entry to None"),
        }
    }

    pub fn build(&mut self) -> ReaderFlexibleDiskTablePtr {
        //
        if let Some(mut writer) = self.building_disk_table.take() {
            if let Err(er) = writer.flush() {
                panic!("Failed flush in builder: {}", er)
            }
        }

        ReaderFlexibleDiskTable::new(self.disk_table_path.as_path())
    }
}
