use std::fs;
use std::os::fd::{BorrowedFd, RawFd};
use std::path::{Path, PathBuf};

use nix::fcntl::OFlag;

use crate::core::{disk_table::disk_table, field::FlexibleField};
use crate::errors::Result;

pub type WriterFlexibleDiskTablePtr = disk_table::WriterDiskTablePtr<FlexibleField, FlexibleField>;

pub struct WriterFlexibleDiskTable {
    disk_table_path: PathBuf,
    fd: RawFd,
}

impl WriterFlexibleDiskTable {
    pub(super) fn new<P: AsRef<Path>>(disk_table_path: P) -> WriterFlexibleDiskTablePtr {
        let fd: RawFd = match nix::fcntl::open(
            disk_table_path.as_ref(),
            OFlag::O_CREAT | OFlag::O_APPEND | OFlag::O_WRONLY,
            nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
        ) {
            Ok(fd) => fd,
            Err(er) => {
                panic!(
                    "Failed to create new segment. path={}, error= {}",
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
        fs::remove_file(self.disk_table_path.as_path())?;

        Ok(())
    }
}

impl disk_table::Writer<FlexibleField, FlexibleField> for WriterFlexibleDiskTable {
    fn write(&mut self, buffer: &[u8]) -> Result<()> {
        let fd = unsafe { BorrowedFd::borrow_raw(self.fd) };

        match nix::unistd::write(fd, buffer) {
            Ok(bytes) => assert_eq!(bytes, buffer.len()),
            Err(er) => panic!("broken write from buffer to fd, error={}", er),
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let fd = self.fd;

        nix::unistd::fsync(fd).unwrap();
        nix::unistd::close(fd).unwrap();

        Ok(())
    }
}
