use std::io::Write;
use std::io::{self, SeekFrom};
use std::os::fd::{BorrowedFd, RawFd};
use std::path::Path;

use nix::fcntl;
use nix::fcntl::OFlag;
use nix::unistd::Whence;

use crate::errors::Result;

pub(super) struct FileHandle {
    fd: RawFd,
}

pub(super) trait ReadSeek: std::io::Read + std::io::Seek + Send + Sync {}

impl FileHandle {
    pub fn new_writer<P: AsRef<Path>>(disk_table_path: P) -> Result<Box<dyn std::io::Write>> {
        let fd = fcntl::open(
            disk_table_path.as_ref(),
            OFlag::O_CREAT | OFlag::O_APPEND | OFlag::O_WRONLY,
            nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
        )?;

        Ok(Box::new(Self { fd }))
    }

    pub fn new_reader<P: AsRef<Path>>(disk_table_path: P) -> Result<Box<dyn ReadSeek>> {
        let fd = fcntl::open(
            disk_table_path.as_ref(),
            OFlag::O_RDONLY,
            nix::sys::stat::Mode::empty(),
        )?;

        // #[cfg(target_os = "linux")]
        // {
        //     fcntl::posix_fadvise(fd, 0, 0, fcntl::PosixFadviseAdvice::POSIX_FADV_RANDOM)?;
        // }

        Ok(Box::new(Self { fd }))
    }
}

impl std::io::Write for FileHandle {
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

impl ReadSeek for FileHandle {}

impl std::io::Read for FileHandle {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let bytes = nix::unistd::read(self.fd, buf)?;

        Ok(bytes)
    }
}

impl std::io::Seek for FileHandle {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let (offset, whence) = match pos {
            SeekFrom::Start(off) => (off as i64, Whence::SeekSet),
            SeekFrom::End(off) => (off, Whence::SeekEnd),
            SeekFrom::Current(off) => (off, Whence::SeekCur),
        };

        let new_offset = nix::unistd::lseek(self.fd, offset, whence)?;

        Ok(new_offset as u64)
    }
}

impl Drop for FileHandle {
    fn drop(&mut self) {
        self.flush().expect("Doesn't expect the problem with sync");
        if let Err(er) = nix::unistd::close(self.fd) {
            panic!("Problem with closing file handle. {}", er)
        }
    }
}
