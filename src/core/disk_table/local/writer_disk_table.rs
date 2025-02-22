use std::fs;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::core::marshal::write_u32;
use crate::core::{
    disk_table::{disk_table, local::offset::Offset},
    entry::flexible_entry::FlexibleEntry,
    field::FlexibleField,
};
use crate::errors::Result;

use super::block;

pub type WriterFlexibleDiskTablePtr = disk_table::WriterDiskTablePtr<FlexibleField, FlexibleField>;

pub struct WriterFlexibleDiskTable {
    disk_table_path: PathBuf,

    buf: Option<io::BufWriter<fs::File>>,
    index_entries: Vec<Offset>,
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
            index_entries: Vec::<Offset>::new(),
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
    fn write(&mut self, entry: &FlexibleEntry) -> Result<()> {
        let entry_offset = self.segment_offset;

        let esstimate_entry_size = block::ENTRY_METADATA_OFFSET as usize + entry.size();
        let mut buffer = vec![0; esstimate_entry_size];
        let entry_bytes = entry.serialize_to(buffer.as_mut_slice())?;

        assert_eq!(entry_bytes, esstimate_entry_size as u64);

        self.segment_offset += entry_bytes as u32;

        match &mut self.buf {
            Some(buf) => {
                buf.write(buffer.as_slice())?;
            }
            None => {
                panic!("broken buffer")
            }
        }

        assert_ne!(esstimate_entry_size, 0);

        self.index_entries.push(Offset {
            pos: entry_offset,
            size: esstimate_entry_size as u32,
        });

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let Some(buffer) = &mut self.buf else {
            panic!("broken buffer")
        };

        for offset in &self.index_entries {
            let mut tmp = [0; block::INDEX_ENTRIES_SIZE];
            write_u32(&mut tmp[0..block::INDEX_ENTRIES_OFFSET_SIZE], offset.pos)?;
            write_u32(&mut tmp[block::INDEX_ENTRIES_OFFSET_SIZE..], offset.size)?;
            buffer.write(&tmp)?;
        }

        buffer.write(&(self.index_entries.len() as u32).to_le_bytes())?;
        buffer.flush()?;

        let fd = self.buf.take().unwrap().into_inner().unwrap();
        fd.sync_all()?;

        Ok(())
    }
}
