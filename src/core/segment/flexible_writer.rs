use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::core::entry::flexible_entry::FlexibleEntry;
use crate::core::field::FieldSize;
use crate::core::segment::offset::Offset;
use crate::errors::Result;

pub struct FlexibleWriter {
    buf: BufWriter<File>,
    entries_offsets: Vec<(Offset, Offset)>,
    segment_offset: u32,
}

impl FlexibleWriter {
    pub fn new<P: AsRef<Path>>(path_to_segment: P) -> Self {
        let result_create = File::create(path_to_segment.as_ref());
        if let Err(er) = result_create {
            panic!(
                "Failed to create new part. path={}, error= {}",
                path_to_segment.as_ref().display(),
                er
            );
        };

        Self {
            buf: BufWriter::new(result_create.unwrap()),
            entries_offsets: Vec::<(Offset, Offset)>::new(),
            segment_offset: 0,
        }
    }

    pub fn write_entry(&mut self, entry: &FlexibleEntry) -> Result<()> {
        let key_offset = self.segment_offset;
        self.segment_offset += entry.get_key().size() as u32;
        let value_offset = self.segment_offset;
        self.segment_offset += entry.get_value().size() as u32;

        self.entries_offsets.push((
            Offset {
                start: key_offset,
                len: entry.get_key().size() as u32,
            },
            Offset {
                start: value_offset,
                len: entry.get_value().size() as u32,
            },
        ));

        self.buf.write(&entry.get_key().data)?;
        self.buf.write(&entry.get_value().data)?;

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        for offsets in &self.entries_offsets {
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
            .write(&(self.entries_offsets.len() as u32).to_le_bytes())?;

        self.buf.flush()?;

        Ok(())
    }
}
