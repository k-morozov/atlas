use std::{
    fs::File,
    path::{Path, PathBuf},
};

use crate::core::{
    entry::{
        entry::{ReadEntry, WriteEntry},
        flexible_entry::FlexibleEntry,
    },
    field::FlexibleField,
};
use crate::errors::Result;

use super::{
    flexible_reader::FlexibleReader,
    segment::{get_segment_path, Segment, SegmentPtr},
};

pub type FlexibleSegmentPtr = SegmentPtr<FlexibleField, FlexibleField>;

pub struct FlexibleSegment {
    table_path: PathBuf,
    segment_name: String,
}

impl FlexibleSegment {
    pub fn new(table_path: &Path, segment_name: &str) -> Self {
        let segment_path = get_segment_path(table_path, &segment_name);

        if let Err(er) = File::create(segment_path.as_path()) {
            panic!(
                "FlexibleSegment: Failed to create part. path={}, error= {}",
                segment_path.display(),
                er
            );
        };

        Self {
            table_path: table_path.to_path_buf(),
            segment_name: segment_name.to_string(),
        }
    }
}

impl Segment<FlexibleField, FlexibleField> for FlexibleSegment {
    fn get_table_path(&self) -> &Path {
        &self.table_path
    }

    fn get_name(&self) -> &str {
        &self.segment_name
    }
}

impl WriteEntry<FlexibleField, FlexibleField> for FlexibleSegment {
    fn write(&mut self, _entry: FlexibleEntry) -> Result<()> {
        unreachable!()
    }
}

impl ReadEntry<FlexibleField, FlexibleField> for FlexibleSegment {
    fn read(&self, key: &FlexibleField) -> Result<Option<FlexibleField>> {
        let reader = FlexibleReader::new(
            get_segment_path(self.table_path.as_path(), self.get_name()).as_path(),
        );
        if let Some(r) = reader.read(&key)? {
            return Ok(Some(r));
        }

        Ok(None)
    }
}
