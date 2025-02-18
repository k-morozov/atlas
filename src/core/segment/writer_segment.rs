use std::fs;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::core::{
    entry::flexible_entry::FlexibleEntry,
    field::{FieldSize, FlexibleField},
    segment::{offset::Offset, segment},
};
use crate::errors::Result;

pub type WriterFlexibleSegmentPtr = segment::WriterSegmentPtr<FlexibleField, FlexibleField>;

pub struct WriterFlexibleSegment {
    segment_path: PathBuf,

    buf: Option<io::BufWriter<fs::File>>,
    entries_offsets: Vec<(Offset, Offset)>,
    segment_offset: u32,
}

impl WriterFlexibleSegment {
    pub(super) fn new<P: AsRef<Path>>(segment_path: P) -> WriterFlexibleSegmentPtr {
        let mut options = fs::OpenOptions::new();
        options.write(true).create(true);

        if let Err(er) = options.open(segment_path.as_ref()) {
            panic!(
                "FlexibleSegment: Failed to create part. path={}, error= {}",
                segment_path.as_ref().display(),
                er
            );
        };

        let fd = match fs::File::create(segment_path.as_ref()) {
            Ok(fd) => fd,
            Err(er) => {
                panic!(
                    "Failed to create new segment. path={}, error= {}",
                    segment_path.as_ref().display(),
                    er
                );
            }
        };

        Box::new(Self {
            segment_path: segment_path.as_ref().to_path_buf(),
            buf: Some(io::BufWriter::new(fd)),
            entries_offsets: Vec::<(Offset, Offset)>::new(),
            segment_offset: 0,
        })
    }
}

impl segment::WriterSegment<FlexibleField, FlexibleField> for WriterFlexibleSegment {}

impl segment::Segment<FlexibleField, FlexibleField> for WriterFlexibleSegment {
    fn get_path(&self) -> &Path {
        self.segment_path.as_path()
    }

    fn remove(&self) -> Result<()> {
        fs::remove_file(self.segment_path.as_path())?;
        Ok(())
    }
}

impl segment::Writer<FlexibleField, FlexibleField> for WriterFlexibleSegment {
    fn write(&mut self, entry: FlexibleEntry) -> Result<()> {
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

    fn flush(&mut self) -> Result<()> {
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

        let fd = self.buf.take().unwrap().into_inner().unwrap();
        fd.sync_all()?;

        Ok(())
    }
}
