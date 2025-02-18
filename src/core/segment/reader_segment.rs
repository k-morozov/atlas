use std::cell::RefCell;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::core::{
    entry::flexible_entry::FlexibleEntry,
    field::FlexibleField,
    segment::{offset::Offset, segment},
};
use crate::errors::Result;

pub type ReaderFlexibleSegmentPtr = segment::ReaderSegmentPtr<FlexibleField, FlexibleField>;

pub struct ReaderFlexibleSegment {
    segment_path: PathBuf,
    fd: RefCell<fs::File>,
}

impl ReaderFlexibleSegment {
    pub(super) fn new<P: AsRef<Path>>(segment_path: P) -> ReaderFlexibleSegmentPtr {
        let fd = match fs::File::open(segment_path.as_ref()) {
            Ok(fd) => fd,
            Err(er) => panic!(
                "FlexibleReader: error={}, path={}",
                er,
                segment_path.as_ref().display()
            ),
        };

        Box::new(Self {
            segment_path: segment_path.as_ref().to_path_buf(),
            fd: RefCell::new(fd),
        })
    }
}

impl segment::ReaderSegment<FlexibleField, FlexibleField> for ReaderFlexibleSegment {}

impl segment::Segment<FlexibleField, FlexibleField> for ReaderFlexibleSegment {
    fn get_path(&self) -> &Path {
        self.segment_path.as_path()
    }

    fn remove(&self) -> Result<()> {
        fs::remove_file(self.segment_path.as_path())?;
        Ok(())
    }
}

impl segment::Reader<FlexibleField, FlexibleField> for ReaderFlexibleSegment {
    fn read(&self, key: &FlexibleField) -> Result<Option<FlexibleField>> {
        let offset = self
            .fd
            .borrow_mut()
            .seek(SeekFrom::End(-(size_of::<u32>() as i64)))?;

        let mut buffer = [0u8; size_of::<u32>()];
        let bytes = self.fd.borrow_mut().read(&mut buffer)?;

        assert_eq!(bytes, size_of::<u32>());

        let count_entry_offsets = u32::from_le_bytes(buffer);

        let size_entry_offsets = 4 * size_of::<u32>() as u64;

        let _offset = self.fd.borrow_mut().seek(SeekFrom::Start(
            offset - count_entry_offsets as u64 * size_entry_offsets,
        ))?;

        let mut entries_offsets = Vec::<(Offset, Offset)>::new();

        for _entry_offset in 0..count_entry_offsets {
            let mut buffer = [0u8; size_of::<u32>()];

            // read key offset
            let bytes = self.fd.borrow_mut().read(&mut buffer)?;
            assert_eq!(bytes, size_of::<u32>());
            let key_offset = u32::from_le_bytes(buffer);

            // read key len
            let bytes = self.fd.borrow_mut().read(&mut buffer)?;
            assert_eq!(bytes, size_of::<u32>());
            let key_len = u32::from_le_bytes(buffer);

            // read value offset
            let bytes = self.fd.borrow_mut().read(&mut buffer)?;
            assert_eq!(bytes, size_of::<u32>());
            let value_offset = u32::from_le_bytes(buffer);

            // read value len
            let bytes = self.fd.borrow_mut().read(&mut buffer)?;
            assert_eq!(bytes, size_of::<u32>());
            let value_len = u32::from_le_bytes(buffer);

            let key_offset = Offset {
                start: key_offset,
                len: key_len,
            };

            let value_offset = Offset {
                start: value_offset,
                len: value_len,
            };

            entries_offsets.push((key_offset, value_offset));
        }

        for (entry_key, entry_value) in entries_offsets {
            let _offset = self
                .fd
                .borrow_mut()
                .seek(SeekFrom::Start(entry_key.start as u64))?;
            let mut buffer = vec![0u8; entry_key.len as usize];

            let bytes = self.fd.borrow_mut().read(&mut buffer)?;
            assert_eq!(bytes, entry_key.len as usize);

            if key.data == buffer {
                let _offset = self
                    .fd
                    .borrow_mut()
                    .seek(SeekFrom::Start(entry_value.start as u64))?;
                let mut buffer = vec![0u8; entry_value.len as usize];

                let bytes = self.fd.borrow_mut().read(&mut buffer)?;
                assert_eq!(bytes, entry_value.len as usize);

                return Ok(Some(FlexibleField::new(buffer)));
            }
        }

        Ok(None)
    }

    fn read_entry_by_index(&self, index: u64) -> Result<Option<FlexibleEntry>> {
        let offset = self
            .fd
            .borrow_mut()
            .seek(SeekFrom::End(-(size_of::<u32>() as i64)))?;

        let mut buffer = [0u8; size_of::<u32>()];
        let bytes: usize = self.fd.borrow_mut().read(&mut buffer)?;

        assert_eq!(bytes, size_of::<u32>());

        let count_entry_offsets = u32::from_le_bytes(buffer) as u64;

        let size_entry_offsets = 4 * size_of::<u32>() as u64;
        let size_bytes_offsets = count_entry_offsets * size_entry_offsets;

        let expected = self.read_size()?;
        assert!(count_entry_offsets == expected);

        assert!(
            offset >= size_bytes_offsets,
            "offset={:?}, count_entry_offsets={}, size_bytes_offsets={}",
            offset,
            count_entry_offsets,
            size_bytes_offsets
        );

        let _offset = self.fd.borrow_mut().seek(SeekFrom::Start(
            offset - count_entry_offsets * size_entry_offsets,
        ))?;

        let mut entries_offsets = Vec::<(Offset, Offset)>::new();

        for entry_offset in 0..count_entry_offsets {
            let mut buffer = [0u8; size_of::<u32>()];

            // read key offset
            let bytes = self.fd.borrow_mut().read(&mut buffer)?;
            assert_eq!(bytes, size_of::<u32>());
            let key_offset = u32::from_le_bytes(buffer);

            // read key len
            let bytes = self.fd.borrow_mut().read(&mut buffer)?;
            assert_eq!(bytes, size_of::<u32>());
            let key_len = u32::from_le_bytes(buffer);

            // read value offset
            let bytes = self.fd.borrow_mut().read(&mut buffer)?;
            assert_eq!(bytes, size_of::<u32>());
            let value_offset = u32::from_le_bytes(buffer);

            // read value len
            let bytes = self.fd.borrow_mut().read(&mut buffer)?;
            assert_eq!(bytes, size_of::<u32>());
            let value_len = u32::from_le_bytes(buffer);

            let key_offset = Offset {
                start: key_offset,
                len: key_len,
            };

            let value_offset = Offset {
                start: value_offset,
                len: value_len,
            };

            if entry_offset == index {
                entries_offsets.push((key_offset, value_offset));
                break;
            }
        }

        for (entry_key, entry_value) in entries_offsets {
            let _offset = self
                .fd
                .borrow_mut()
                .seek(SeekFrom::Start(entry_key.start as u64))?;
            let mut key_buffer = vec![0u8; entry_key.len as usize];

            let bytes = self.fd.borrow_mut().read(&mut key_buffer)?;
            assert_eq!(bytes, entry_key.len as usize);

            let _offset = self
                .fd
                .borrow_mut()
                .seek(SeekFrom::Start(entry_value.start as u64))?;
            let mut value_buffer = vec![0u8; entry_value.len as usize];

            let bytes = self.fd.borrow_mut().read(&mut value_buffer)?;
            assert_eq!(bytes, entry_value.len as usize);

            return Ok(Some(FlexibleEntry::new(
                FlexibleField::new(key_buffer),
                FlexibleField::new(value_buffer),
            )));
        }

        Ok(None)
    }

    fn read_size(&self) -> Result<u64> {
        let _offset = self
            .fd
            .borrow_mut()
            .seek(SeekFrom::End(-(size_of::<u32>() as i64)))?;

        let mut buffer = [0u8; size_of::<u32>()];
        let bytes = self.fd.borrow_mut().read(&mut buffer)?;

        assert_eq!(bytes, size_of::<u32>());

        let count_offsets = u32::from_le_bytes(buffer);

        Ok(count_offsets as u64)
    }
}
