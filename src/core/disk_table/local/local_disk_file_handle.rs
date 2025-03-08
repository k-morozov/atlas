use std::io;
use std::io::Write;
use std::os::fd::{BorrowedFd, RawFd};
use std::path::Path;

use nix::fcntl;
use nix::fcntl::OFlag;

use crate::errors::Result;

pub(super) struct LocalDiskFileHandle {
    fd: RawFd,
}

impl LocalDiskFileHandle {
    pub fn new<P: AsRef<Path>>(disk_table_path: P) -> Result<Self> {
        let fd = fcntl::open(
            disk_table_path.as_ref(),
            OFlag::O_CREAT | OFlag::O_APPEND | OFlag::O_WRONLY,
            nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
        )?;

        Ok(Self { fd })
    }
}

impl std::io::Write for LocalDiskFileHandle {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let fd = unsafe { BorrowedFd::borrow_raw(self.fd) };

        match nix::unistd::write(fd, buf) {
            Ok(bytes) => {
                assert_eq!(bytes, buf.len());
                return Ok(bytes);
            }
            Err(er) => panic!("broken write from buffer to fd, error={}", er),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        nix::unistd::fsync(self.fd)?;

        Ok(())
    }
}

impl Drop for LocalDiskFileHandle {
    fn drop(&mut self) {
        self.flush().expect("Doesn't expect the problem with sync");
        if let Err(er) = nix::unistd::close(self.fd) {
            panic!("Problem with closing file handle. {}", er)
        }
    }
}
