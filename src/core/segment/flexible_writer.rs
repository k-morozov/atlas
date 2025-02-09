use std::fs::File;
use std::io::{BufWriter, Write};
use std::iter::Iterator;
use std::path::Path;

use crate::core::entry::flexible_entry::FlexibleEntry;
use crate::core::segment::offset::Offset;
use crate::errors::{Error, Result};
use crate::core::field::FieldSize;

pub struct FlexibleWriter<'a> {
    buf: BufWriter<File>,
    row_it: Option<Box<dyn Iterator<Item = &'a FlexibleEntry> + 'a>>,
}

impl<'a> FlexibleWriter<'a> {
    pub fn new<T>(path_to_segment: &Path, row_it: T) -> Self
    where
        T: Iterator<Item = &'a FlexibleEntry> + 'a,
    {
        let result_create = File::create(path_to_segment);
        if let Err(er) = result_create {
            panic!(
                "Failed to create new part. path={}, error= {}",
                path_to_segment.display(),
                er
            );
        };

        Self {
            buf: BufWriter::new(result_create.unwrap()),
            row_it: Some(Box::new(row_it)),
        }
    }

    pub fn write_entries(&mut self) -> Result<()> {
        if self.row_it.is_none() {
            return Err(Error::InvalidData("empty rows".to_string()));
        }
        let row_it = self
            .row_it
            .take()
            .ok_or(Error::InvalidData("Failed take from rows".to_string()))?;

        let mut entries_offsets = Vec::<(Offset, Offset)>::new();
        let mut segment_offset = 0u32;

        for row in row_it {
            let key_offset = segment_offset;
            segment_offset += row.get_key().size() as u32;
            let value_offset = segment_offset;
            segment_offset += row.get_value().size() as u32;

            entries_offsets.push((
                Offset {
                    start: key_offset,
                    len: row.get_key().size() as u32,
                },
                Offset {
                    start: value_offset,
                    len: row.get_value().size() as u32,
                },
            ));

            self.buf.write(&row.get_key().data)?;
            self.buf.write(&row.get_value().data)?;
        }

        for offsets in &entries_offsets {
            let temp = vec![
                offsets.0.start,
                offsets.0.len,
                offsets.1.start,
                offsets.1.len,
            ];

            for offset in temp {
                let bytes = offset.to_le_bytes();
                self.buf.write(&bytes)?;
            }
        }

        self.buf
            .write(&(entries_offsets.len() as u32).to_le_bytes())?;

        self.buf.flush()?;

        Ok(())
    }
}
