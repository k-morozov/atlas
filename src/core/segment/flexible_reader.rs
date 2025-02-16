use std::fs::File;
use std::io::SeekFrom;
use std::io::{Read, Seek};
use std::path::Path;

use crate::core::entry::flexible_entry::FlexibleEntry;
use crate::core::field::FlexibleField;
use crate::core::segment::offset::Offset;
use crate::errors::Result;

pub struct FlexibleReader {
    fd: File,
}

impl FlexibleReader {
    pub fn new<P: AsRef<Path>>(path_to_part: P) -> Self {
        let fd = match File::open(path_to_part.as_ref()) {
            Ok(fd) => fd,
            Err(er) => panic!(
                "FlexibleReader: error={}, path={}",
                er,
                path_to_part.as_ref().display()
            ),
        };
        FlexibleReader { fd }
    }

    pub fn read(mut self, key: &FlexibleField) -> Result<Option<FlexibleField>> {
        let offset = self.fd.seek(SeekFrom::End(-(size_of::<u32>() as i64)))?;

        let mut buffer = [0u8; size_of::<u32>()];
        let bytes = self.fd.read(&mut buffer)?;

        assert_eq!(bytes, size_of::<u32>());

        let count_entry_offsets = u32::from_le_bytes(buffer);

        let size_entry_offsets = 4 * size_of::<u32>() as u64;

        let _offset = self.fd.seek(SeekFrom::Start(
            offset - count_entry_offsets as u64 * size_entry_offsets,
        ))?;

        let mut entries_offsets = Vec::<(Offset, Offset)>::new();

        for _entry_offset in 0..count_entry_offsets {
            let mut buffer = [0u8; size_of::<u32>()];

            // read key offset
            let bytes = self.fd.read(&mut buffer)?;
            assert_eq!(bytes, size_of::<u32>());
            let key_offset = u32::from_le_bytes(buffer);

            // read key len
            let bytes = self.fd.read(&mut buffer)?;
            assert_eq!(bytes, size_of::<u32>());
            let key_len = u32::from_le_bytes(buffer);

            // read value offset
            let bytes = self.fd.read(&mut buffer)?;
            assert_eq!(bytes, size_of::<u32>());
            let value_offset = u32::from_le_bytes(buffer);

            // read value len
            let bytes = self.fd.read(&mut buffer)?;
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
            let _offset = self.fd.seek(SeekFrom::Start(entry_key.start as u64))?;
            let mut buffer = vec![0u8; entry_key.len as usize];

            let bytes = self.fd.read(&mut buffer)?;
            assert_eq!(bytes, entry_key.len as usize);

            if key.data == buffer {
                let _offset = self.fd.seek(SeekFrom::Start(entry_value.start as u64))?;
                let mut buffer = vec![0u8; entry_value.len as usize];

                let bytes = self.fd.read(&mut buffer)?;
                assert_eq!(bytes, entry_value.len as usize);

                return Ok(Some(FlexibleField::new(buffer)));
            }
        }

        Ok(None)
    }

    // @todo
    pub fn read_by_index(&mut self, index: u32) -> Result<Option<FlexibleEntry>> {
        let offset = self.fd.seek(SeekFrom::End(-(size_of::<u32>() as i64)))?;

        let mut buffer = [0u8; size_of::<u32>()];
        let bytes: usize = self.fd.read(&mut buffer)?;

        assert_eq!(bytes, size_of::<u32>());

        let count_entry_offsets = u32::from_le_bytes(buffer);

        let size_entry_offsets = 4 * size_of::<u32>() as u64;
        let size_bytes_offsets = count_entry_offsets as u64 * size_entry_offsets;

        let expected = self.read_size()?;
        assert!(count_entry_offsets as u64 == expected);

        assert!(
            offset >= size_bytes_offsets,
            "offset={:?}, count_entry_offsets={}, size_bytes_offsets={}",
            offset,
            count_entry_offsets,
            size_bytes_offsets
        );

        let _offset = self.fd.seek(SeekFrom::Start(
            offset - count_entry_offsets as u64 * size_entry_offsets,
        ))?;

        let mut entries_offsets = Vec::<(Offset, Offset)>::new();

        for entry_offset in 0..count_entry_offsets {
            let mut buffer = [0u8; size_of::<u32>()];

            // read key offset
            let bytes = self.fd.read(&mut buffer)?;
            assert_eq!(bytes, size_of::<u32>());
            let key_offset = u32::from_le_bytes(buffer);

            // read key len
            let bytes = self.fd.read(&mut buffer)?;
            assert_eq!(bytes, size_of::<u32>());
            let key_len = u32::from_le_bytes(buffer);

            // read value offset
            let bytes = self.fd.read(&mut buffer)?;
            assert_eq!(bytes, size_of::<u32>());
            let value_offset = u32::from_le_bytes(buffer);

            // read value len
            let bytes = self.fd.read(&mut buffer)?;
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
            let _offset = self.fd.seek(SeekFrom::Start(entry_key.start as u64))?;
            let mut key_buffer = vec![0u8; entry_key.len as usize];

            let bytes = self.fd.read(&mut key_buffer)?;
            assert_eq!(bytes, entry_key.len as usize);

            let _offset = self.fd.seek(SeekFrom::Start(entry_value.start as u64))?;
            let mut value_buffer = vec![0u8; entry_value.len as usize];

            let bytes = self.fd.read(&mut value_buffer)?;
            assert_eq!(bytes, entry_value.len as usize);

            return Ok(Some(FlexibleEntry::new(
                FlexibleField::new(key_buffer),
                FlexibleField::new(value_buffer),
            )));
        }

        Ok(None)
    }

    // @todo
    pub fn read_size(&mut self) -> Result<u64> {
        let _offset = self.fd.seek(SeekFrom::End(-(size_of::<u32>() as i64)))?;

        let mut buffer = [0u8; size_of::<u32>()];
        let bytes = self.fd.read(&mut buffer)?;

        assert_eq!(bytes, size_of::<u32>());

        let count_offsets = u32::from_le_bytes(buffer);

        Ok(count_offsets as u64)
    }
}
