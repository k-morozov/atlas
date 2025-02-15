use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;


use crate::core::entry::flexible_entry::FlexibleEntry;
use crate::core::field::FieldSize;
use crate::core::segment::offset::Offset;
use crate::errors::Result;

pub struct FlexibleWriter {
    buf: Option<BufWriter<File>>,
    entries_offsets: Vec<(Offset, Offset)>,
    segment_offset: u32,
}

impl FlexibleWriter {
    pub fn new<P: AsRef<Path>>(path_to_segment: P) -> Self {
        let fd = match File::create(path_to_segment.as_ref()) {
            Ok(fd) => fd,
            Err(er) => {
                panic!(
                    "Failed to create new part. path={}, error= {}",
                    path_to_segment.as_ref().display(),
                    er
                );
            }
        };

        Self {
            buf: Some(BufWriter::new(fd)),
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

        match &mut self.buf {
            Some(buf) => {
                buf.write(&entry.get_key().data)?;
                buf.write(&entry.get_value().data)?;
            }
            None => {
                panic!("broken buffer")
            }
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        let Some(buffer) = &mut self.buf else {
            panic!("broken buffer")
        };

        for offsets in &self.entries_offsets {
            let temp = vec![
                offsets.0.start,
                offsets.0.len,
                offsets.1.start,
                offsets.1.len,
            ];

            for offset in temp {
                let bytes = offset.to_le_bytes();
                buffer.write(&bytes)?;
            }
        }

        buffer.write(&(self.entries_offsets.len() as u32).to_le_bytes())?;
        buffer.flush()?;

        let fd: File = self.buf.take().unwrap().into_inner().unwrap();
        fd.sync_all()?;

        Ok(())
    }
}
