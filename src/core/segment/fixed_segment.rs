use std::fs::File;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use super::id::SegmentID;
use super::segment_writer::SegmentWriter;
use super::table::Levels;
use crate::core::entry::entry::{ReadEntry, WriteEntry};
use crate::core::entry::fixed_entry::FixedEntry;
use crate::core::field::FixedField;
use crate::core::mem_table::MemTable;
use crate::core::schema::Schema;
use crate::core::segment::{
    segment::{get_segment_name, get_segment_name_by_level, get_segment_path, Segment, SegmentPtr},
    segment_reader::SegmentReader,
};
use crate::errors::Result;

pub type FixedSegmentPtr = SegmentPtr<FixedField, FixedField>;

pub struct FixedSegment {
    table_path: PathBuf,
    segment_name: String,
    schema: Rc<Schema>,
}

impl FixedSegment {
    // pub fn create(
    //     table_path: &Path,
    //     sgm_id: &mut SegmentID,
    //     mem_table: &mut MemTable,
    //     schema: Rc<Schema>,
    // ) -> Result<FixedSegmentPtr> {
    //     let segment_id = sgm_id.get_and_next();

    //     let segment_name = get_segment_name(segment_id);
    //     let segment_path = get_segment_path(table_path, &segment_name);

    //     let wfd = File::create(segment_path)?;
    //     let mut writer = SegmentWriter::new(wfd);
    //     for entry in mem_table.into_iter() {
    //         writer.write_entry(entry)?;
    //     }
    //     writer.flush()?;

    //     Ok(Self::new(table_path, segment_name.as_str(), schema))
    // }

    pub fn for_merge(
        table_path: &Path,
        sgm_id: &mut SegmentID,
        schema: Rc<Schema>,
        level: Levels,
    ) -> Result<FixedSegmentPtr> {
        let segment_id = sgm_id.get_and_next();

        let segment_name = get_segment_name_by_level(segment_id, level);
        let segment_path = get_segment_path(table_path, &segment_name);

        if let Err(er) = File::create(segment_path.as_path()) {
            panic!(
                "from_merge: Failed to create new part. path={}, error= {}",
                segment_path.display(),
                er
            );
        };

        Ok(Self::new(table_path, segment_name.as_str(), schema))
    }

    pub fn new(table_path: &Path, segment_name: &str, schema: Rc<Schema>) -> FixedSegmentPtr {
        Box::new(Self {
            table_path: table_path.to_path_buf(),
            segment_name: segment_name.to_string(),
            schema,
        })
    }
}

impl Segment<FixedField, FixedField> for FixedSegment {
    fn get_table_path(&self) -> &Path {
        &self.table_path
    }

    fn get_name(&self) -> &str {
        &self.segment_name
    }
}

impl WriteEntry<FixedField, FixedField> for FixedSegment {
    fn write(&mut self, _entry: FixedEntry) -> Result<()> {
        Ok(())
    }
}

impl ReadEntry<FixedField, FixedField> for FixedSegment {
    fn read(&self, key: &FixedField) -> Result<Option<FixedField>> {
        let reader = SegmentReader::new(
            get_segment_path(self.table_path.as_path(), self.get_name()).as_path(),
            self.schema.clone(),
        );
        if let Some(r) = reader.read(&key)? {
            return Ok(Some(r));
        }

        Ok(None)
    }
}
