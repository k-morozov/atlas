use std::fs::File;
use std::io::{BufWriter, Write};
use std::iter::Iterator;
use std::mem::MaybeUninit;
use std::path::Path;
use std::slice::from_raw_parts;

use crate::core::entry::entry::{Entry, ReadEntry, WriteEntry};
use crate::core::entry::fixed_entry::FixedEntry;
use crate::core::field::FixedField;
use crate::core::marshal::Marshal;
use crate::errors::{Error, Result};

pub struct SegmentWriter {
    buf: BufWriter<File>,
}

impl SegmentWriter {
    pub fn new(wfd: File) -> Self {
        Self {
            buf: BufWriter::new(wfd),
        }
    }

    pub fn write_entry(&mut self, entry: &Entry<FixedField, FixedField>) -> Result<()> {
        let mut row_buf_raw = vec![MaybeUninit::uninit(); entry.size()];

        entry
            .serialize(&mut row_buf_raw)
            .map_err(|_| Error::InvalidData("empty".to_string()))?;

        let row_buf_initialized =
            unsafe { from_raw_parts(row_buf_raw.as_ptr() as *const u8, entry.size()) };

        self.buf
            .write_all(&row_buf_initialized)
            .map_err(|_| Error::InvalidData("empty".to_string()))?;

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.buf.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::fs::{create_dir_all, remove_file};
    use std::io::ErrorKind;
    use std::path::Path;

    use crate::core::field::{FieldType, FixedField};
    use crate::core::segment::segment_writer::*;

    #[test]
    fn create_segment() {
        let path = Path::new("/tmp/kvs/test/create_segment/part1.bin");

        if let Some(parent) = path.parent() {
            create_dir_all(parent).unwrap();
        }

        if let Err(er) = remove_file(path) {
            assert_eq!(ErrorKind::NotFound, er.kind());
        }

        let mut entries: Vec<FixedEntry> = Vec::new();

        for index in 1..4 {
            entries.push(FixedEntry::new(
                FixedField::new(FieldType::Int32(index)),
                FixedField::new(FieldType::Int32(index * 10)),
            ));
        }

        let wfd = File::create(path).unwrap();

        let mut writer = SegmentWriter::new(wfd);
        let result = writer.write_entry(&entries[0]);

        assert!(result.is_ok());
    }
}
