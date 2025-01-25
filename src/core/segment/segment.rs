use std::path::Path;

use super::id::SegmentID;
use super::segment_writer::SegmentWriter;
use crate::core::mem_table::MemTable;
use crate::core::pg_errors::PgError;

pub type Segments = Vec<Segment>;
pub struct Segment {
    segment_name: String,
}

impl Segment {
    pub fn create(
        table_path: &str,
        sgm_id: &mut SegmentID,
        mem_table: &mut MemTable,
    ) -> Result<Self, PgError> {
        let segment_id = sgm_id.get_and_next();

        let segment_name = format!("segment_{:07}_1.bin", segment_id);
        let segment_path = format!("{table_path}/segment/{}", segment_name);

        let mut writer = SegmentWriter::new(Path::new(&segment_path), mem_table.iter());
        writer.write_entries()?;

        Ok(Self { segment_name })
    }

    pub fn new(segment_name: &str) -> Self {
        Self {
            segment_name: segment_name.to_string(),
        }
    }

    pub fn get_name(&self) -> &str {
        &self.segment_name
    }
}
