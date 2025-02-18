use std::fs;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::core::{
    disk_table::{disk_table, local::offset::Offset},
    entry::flexible_entry::FlexibleEntry,
    field::{FieldSize, FlexibleField},
};
use crate::errors::Result;

pub type WriterFlexibleDiskTablePtr = disk_table::WriterDiskTablePtr<FlexibleField, FlexibleField>;

pub struct WriterFlexibleDiskTable {
    disk_table_path: PathBuf,

    buf: Option<io::BufWriter<fs::File>>,
    entries_offsets: Vec<(Offset, Offset)>,
    segment_offset: u32,
}

impl WriterFlexibleDiskTable {
    pub(super) fn new<P: AsRef<Path>>(disk_table_path: P) -> WriterFlexibleDiskTablePtr {
        let mut options = fs::OpenOptions::new();
        options.write(true).create(true);

        if let Err(er) = options.open(disk_table_path.as_ref()) {
            panic!(
                "FlexibleSegment: Failed to create part. path={}, error= {}",
                disk_table_path.as_ref().display(),
                er
            );
        };

        let fd = match fs::File::create(disk_table_path.as_ref()) {
            Ok(fd) => fd,
            Err(er) => {
                panic!(
                    "Failed to create new segment. path={}, error= {}",
                    disk_table_path.as_ref().display(),
                    er
                );
            }
        };

        Box::new(Self {
            disk_table_path: disk_table_path.as_ref().to_path_buf(),
            buf: Some(io::BufWriter::new(fd)),
            entries_offsets: Vec::<(Offset, Offset)>::new(),
            segment_offset: 0,
        })
    }
}

impl disk_table::WriterDiskTable<FlexibleField, FlexibleField> for WriterFlexibleDiskTable {}

impl disk_table::DiskTable<FlexibleField, FlexibleField> for WriterFlexibleDiskTable {
    fn get_path(&self) -> &Path {
        self.disk_table_path.as_path()
    }

    fn remove(&self) -> Result<()> {
        fs::remove_file(self.disk_table_path.as_path())?;
        Ok(())
    }
}

impl disk_table::Writer<FlexibleField, FlexibleField> for WriterFlexibleDiskTable {
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
