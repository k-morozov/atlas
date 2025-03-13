use std::io::Write;
use std::io::{self, SeekFrom};
use std::os::fd::{BorrowedFd, RawFd};
use std::path::{Path, PathBuf};

use nix::fcntl;
use nix::fcntl::OFlag;
use nix::unistd::Whence;

use crate::errors::Result;
use crate::logicerr;

pub(super) struct FileHandle {
    fd: RawFd,
    disk_table_path: PathBuf,
}

pub trait ReadSeek: std::io::Read + std::io::Seek + Send + Sync {}

pub fn sync_dir<P: AsRef<Path>>(path: P) -> Result<()> {
    match path.as_ref().parent() {
        Some(parent_path) => {
            let parent_fd =
                fcntl::open(parent_path, OFlag::O_RDONLY, nix::sys::stat::Mode::empty())?;
            nix::unistd::fsync(parent_fd)?;
        }
        None => return logicerr!("disk table must have parent"),
    }

    Ok(())
}

impl FileHandle {
    pub fn new_data_writer<P: AsRef<Path>>(disk_table_path: P) -> Result<Box<dyn std::io::Write>> {
        let fd = fcntl::open(
            disk_table_path.as_ref(),
            OFlag::O_CREAT | OFlag::O_APPEND | OFlag::O_WRONLY | OFlag::O_DIRECT,
            nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
        )?;

        let disk_table_path = disk_table_path.as_ref().to_path_buf();
        Ok(Box::new(Self {
            fd,
            disk_table_path,
        }))
    }
    pub fn new_index_writer<P: AsRef<Path>>(
        index_table_path: P,
    ) -> Result<Box<dyn std::io::Write>> {
        let fd = fcntl::open(
            index_table_path.as_ref(),
            OFlag::O_CREAT | OFlag::O_APPEND | OFlag::O_WRONLY,
            nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
        )?;

        sync_dir(
            index_table_path
                .as_ref()
                .parent()
                .expect("disk table must have parent"),
        )?;

        let disk_table_path = index_table_path.as_ref().to_path_buf();
        Ok(Box::new(Self {
            fd,
            disk_table_path,
        }))
    }

    pub fn new_data_reader<P: AsRef<Path>>(disk_table_path: P) -> Result<Box<dyn ReadSeek>> {
        let fd = fcntl::open(
            disk_table_path.as_ref(),
            OFlag::O_RDONLY | OFlag::O_DIRECT,
            nix::sys::stat::Mode::empty(),
        )?;

        // #[cfg(target_os = "linux")]
        // {
        //     fcntl::posix_fadvise(fd, 0, 0, fcntl::PosixFadviseAdvice::POSIX_FADV_RANDOM)?;
        // }

        let disk_table_path = disk_table_path.as_ref().to_path_buf();

        // @todo remove
        Ok(Box::new(Self {
            fd,
            disk_table_path,
        }))
    }

    pub fn new_index_reader<P: AsRef<Path>>(disk_table_path: P) -> Result<Box<dyn ReadSeek>> {
        let fd = fcntl::open(
            disk_table_path.as_ref(),
            OFlag::O_RDONLY,
            nix::sys::stat::Mode::empty(),
        )?;

        #[cfg(target_os = "linux")]
        {
            fcntl::posix_fadvise(fd, 0, 0, fcntl::PosixFadviseAdvice::POSIX_FADV_RANDOM)?;
        }

        let disk_table_path = disk_table_path.as_ref().to_path_buf();

        // @todo remove
        Ok(Box::new(Self {
            fd,
            disk_table_path,
        }))
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
            Err(er) => panic!(
                "broken write {} bytes from buffer to fd, error={}",
                buf.len(),
                er
            ),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        nix::unistd::fsync(self.fd)?;

        let parent_path = self.disk_table_path.parent().expect("we have a parents");
        // sync_dir(parent_path)?;

        let parent_fd = fcntl::open(parent_path, OFlag::O_RDONLY, nix::sys::stat::Mode::empty())?;
        nix::unistd::fsync(parent_fd)?;

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
