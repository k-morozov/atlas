use std::fs::File;
use std::io::SeekFrom;
use std::io::{Read, Seek};
use std::path::Path;

use crate::core::field::FlexibleField;
use crate::core::segment::offset::Offset;
use crate::errors::Result;

pub struct FlexibleReader {
    fd: File,
}

impl FlexibleReader {
    pub fn new(path_to_part: &Path) -> Self {
        let fd = match File::open(path_to_part) {
            Ok(fd) => fd,
            Err(er) => panic!(
                "FlexibleReader: error={}, path={}",
                er,
                path_to_part.display()
            ),
        };
        FlexibleReader { fd }
    }

    pub fn read(mut self, key: &FlexibleField) -> Result<Option<FlexibleField>> {
        let offset = self.fd.seek(SeekFrom::End(-(size_of::<u32>() as i64)))?;

        let mut buffer = [0u8; size_of::<u32>()];
        let bytes = self.fd.read(&mut buffer)?;

        assert_eq!(bytes, size_of::<u32>());

        let count_offsets = u32::from_le_bytes(buffer);

        let size_entry_offsets = 4 * size_of::<u32>() as u64;

        let offset = self.fd.seek(SeekFrom::Start(
            offset - count_offsets as u64 * size_entry_offsets,
        ))?;

        let mut entries_offsets = Vec::<(Offset, Offset)>::new();

        for _entry_offset in 0..count_offsets {
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

        Ok(None)
    }
}
