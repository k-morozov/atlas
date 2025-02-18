use std::path::{Path, PathBuf};

use super::reader_disk_table::{ReaderFlexibleDiskTable, ReaderFlexibleDiskTablePtr};
use super::writer_disk_table::{WriterFlexibleDiskTable, WriterFlexibleDiskTablePtr};
use crate::core::entry::flexible_entry::FlexibleEntry;

pub struct FlexibleSegmentBuilder {
    disk_table_path: PathBuf,
    segment: Option<WriterFlexibleDiskTablePtr>,
}

impl FlexibleSegmentBuilder {
    pub fn new<P: AsRef<Path>>(disk_table_path: P) -> Self {
        FlexibleSegmentBuilder {
            disk_table_path: disk_table_path.as_ref().to_path_buf(),
            segment: Some(WriterFlexibleDiskTable::new(disk_table_path)),
        }
    }

    pub fn from<P: AsRef<Path>>(disk_table_path: P) -> Self {
        FlexibleSegmentBuilder {
            disk_table_path: disk_table_path.as_ref().to_path_buf(),
            segment: None,
        }
    }

    pub fn append_entry(&mut self, entry: &FlexibleEntry) -> &mut Self {
        match &mut self.segment {
            Some(ptr) => {
                if let Err(_er) = ptr.write(entry.clone()) {
                    panic!("Failed write in builder")
                }
                self
            }
            None => panic!("Failed write entry to None"),
        }
    }

    pub fn build(&mut self) -> ReaderFlexibleDiskTablePtr {
        let building_segment = self.segment.take();

        if let Some(mut writer) = building_segment {
            if let Err(er) = writer.flush() {
                panic!("Failed flush in builder: {}", er)
            }
        }

        ReaderFlexibleDiskTable::new(self.disk_table_path.as_path())
    }
}
