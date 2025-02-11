use std::path::{Path, PathBuf};

use crate::core::{
    entry::entry::{Entry, ReadEntry, WriteEntry},
    field::FlexibleField,
};
use crate::errors::Result;

use super::segment::Segment;

pub struct FlexibleSegment {
    table_path: PathBuf,
    segment_name: String,
}

impl FlexibleSegment {
    pub fn new(table_path: &Path, segment_name: &str) -> Self {
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
    fn write(&mut self, _entry: Entry<FlexibleField, FlexibleField>) -> Result<()> {
        Ok(())
    }
}

impl ReadEntry<FlexibleField, FlexibleField> for FlexibleSegment {
    fn read(&self, _key: &FlexibleField) -> Result<Option<FlexibleField>> {
        Ok(None)
    }
}
