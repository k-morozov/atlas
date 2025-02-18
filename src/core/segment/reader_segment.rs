use std::fs;
use std::path::{Path, PathBuf};

use super::{flexible_reader::FlexibleReader, segment};
use crate::core::{
    entry::flexible_entry::FlexibleEntry,
    field::FlexibleField,
};
use crate::errors::Result;

pub type ReaderFlexibleSegmentPtr = segment::ReaderSegmentPtr<FlexibleField, FlexibleField>;

pub struct ReaderFlexibleSegment {
    segment_path: PathBuf,
    reader: FlexibleReader,
}

impl ReaderFlexibleSegment {
    pub(super) fn new<P: AsRef<Path>>(segment_path: P) -> ReaderFlexibleSegmentPtr {
        let mut options = fs::OpenOptions::new();
        options.write(true).create(true);

        if let Err(er) = options.open(segment_path.as_ref()) {
            panic!(
                "FlexibleSegment: Failed to create part. path={}, error= {}",
                segment_path.as_ref().display(),
                er
            );
        };

        Box::new(Self {
            segment_path: segment_path.as_ref().to_path_buf(),
            reader: FlexibleReader::new(segment_path.as_ref()),
        })
    }

    pub(super) fn from<P: AsRef<Path>>(segment_path: P) -> ReaderFlexibleSegmentPtr {
        if let Err(er) = fs::File::open(segment_path.as_ref()) {
            panic!(
                "FlexibleSegment: Failed to create part. path={}, error= {}",
                segment_path.as_ref().display(),
                er
            );
        };

        Box::new(Self {
            segment_path: segment_path.as_ref().to_path_buf(),
            reader: FlexibleReader::new(segment_path.as_ref()),
        })
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
        if let Some(r) = self.reader.read(&key)? {
            return Ok(Some(r));
        }

        Ok(None)
    }

    fn read_entry_by_index(&self, index: u64) -> Result<Option<FlexibleEntry>> {
        if let Some(r) = self.reader.read_by_index(index as u32)? {
            return Ok(Some(r));
        }

        Ok(None)
    }

    fn read_size(&self) -> Result<u64> {
        self.reader.read_size()
    }
}
