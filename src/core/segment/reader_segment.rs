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
    count_entries: u32,
    offsets: Vec<(Offset, Offset)>,
}

impl ReaderFlexibleSegment {
    pub(super) fn new<P: AsRef<Path>>(segment_path: P) -> ReaderFlexibleSegmentPtr {
        let mut fd = match fs::File::open(segment_path.as_ref()) {
            Ok(fd) => fd,
            Err(er) => panic!(
                "FlexibleReader: error={}, path={}",
                er,
                segment_path.as_ref().display()
            ),
        };

        let _offset = fd.seek(SeekFrom::End(-(size_of::<u32>() as i64)));

        let mut buffer = [0u8; size_of::<u32>()];
        let Ok(bytes) = fd.read(&mut buffer) else {
            panic!("Failed read from disk")
        };

        assert_eq!(bytes, size_of::<u32>());

        let count_entries = u32::from_le_bytes(buffer);
        let Ok(offsets) = ReaderFlexibleSegment::read_offsets(&mut fd, count_entries) else {
            panic!(
                "Failed read offsets from {}",
                segment_path.as_ref().display()
            )
        };

        Box::new(Self {
            segment_path: segment_path.as_ref().to_path_buf(),
            fd: RefCell::new(fd),
            count_entries,
            offsets,
        })
    }

    fn read_offsets(fd: &mut fs::File, count_entries: u32) -> Result<Vec<(Offset, Offset)>> {
        let size_entry_offsets = 4 * size_of::<u32>() as i64;
        let _offset = fd.seek(SeekFrom::End(
            -(size_of::<u32>() as i64 + count_entries as i64 * size_entry_offsets),
        ))?;
        let mut entries_offsets = Vec::<(Offset, Offset)>::new();

        for _entry_offset in 0..count_entries {
            let mut buffer = [0u8; size_of::<u32>()];

            // read key offset
            let bytes = fd.read(&mut buffer)?;
            assert_eq!(bytes, size_of::<u32>());
            let key_offset = u32::from_le_bytes(buffer);

            // read key len
            let bytes = fd.read(&mut buffer)?;
            assert_eq!(bytes, size_of::<u32>());
            let key_len = u32::from_le_bytes(buffer);

            // read value offset
            let bytes = fd.read(&mut buffer)?;
            assert_eq!(bytes, size_of::<u32>());
            let value_offset = u32::from_le_bytes(buffer);

            // read value len
            let bytes = fd.read(&mut buffer)?;
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

        Ok(entries_offsets)
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
        for (entry_key, entry_value) in &self.offsets {
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

    fn read_entry_by_index(&self, index: u32) -> Result<Option<FlexibleEntry>> {
        let Some((entry_key, entry_value)) = self.offsets.get(index as usize) else {
            return Ok(None);
        };

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

    fn count_entries(&self) -> u32 {
        self.count_entries
    }
}
