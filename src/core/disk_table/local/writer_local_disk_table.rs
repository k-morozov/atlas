use std::io::Write;
use std::path::{Path, PathBuf};

use crate::core::{disk_table::disk_table, field::FlexibleField};
use crate::errors::Result;

use super::file_handle::FileHandle;

pub type WriterFlexibleDiskTablePtr = disk_table::WriterDiskTablePtr<FlexibleField, FlexibleField>;

pub struct WriterFlexibleDiskTable {
    disk_table_path: PathBuf,
    fd: FileHandle,
}

impl WriterFlexibleDiskTable {
    pub(super) fn new<P: AsRef<Path>>(disk_table_path: P) -> WriterFlexibleDiskTablePtr {
        let fd = match FileHandle::new_writer(disk_table_path.as_ref()) {
            Ok(fd) => fd,
            Err(er) => {
                panic!(
                    "Failed to create file handle for writting. path={}, error= {}",
                    disk_table_path.as_ref().display(),
                    er
                );
            }
        };

        Box::new(Self {
            disk_table_path: disk_table_path.as_ref().to_path_buf(),
            fd,
        })
    }
}

impl disk_table::WriterDiskTable<FlexibleField, FlexibleField> for WriterFlexibleDiskTable {}

impl disk_table::DiskTable<FlexibleField, FlexibleField> for WriterFlexibleDiskTable {
    fn get_path(&self) -> &Path {
        self.disk_table_path.as_path()
    }

    fn remove(&self) -> Result<()> {
        self.fd.remove()?;

        Ok(())
    }
}

impl disk_table::Writer<FlexibleField, FlexibleField> for WriterFlexibleDiskTable {
    fn write(&mut self, buffer: &[u8]) -> Result<()> {
        self.fd.write(buffer)?;

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.fd.flush()?;
        Ok(())
    }
}
