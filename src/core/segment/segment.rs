use std::fs::File;
use std::path::{Path, PathBuf};

use super::id::SegmentID;
use super::segment_writer::SegmentWriter;
use super::table::Levels;
use crate::core::mem_table::MemTable;
use crate::core::pg_errors::PgError;

pub struct Segment {
    table_path: PathBuf,
    segment_name: String,
}

impl Segment {
    pub fn create(
        table_path: &Path,
        sgm_id: &mut SegmentID,
        mem_table: &mut MemTable,
    ) -> Result<Self, PgError> {
        let segment_id = sgm_id.get_and_next();

        let segment_name = format!("segment_{:07}_1.bin", segment_id);
        let segment_path = Segment::get_path(table_path, &segment_name);

        let mut writer = SegmentWriter::new(Path::new(&segment_path), mem_table.iter());
        writer.write_entries()?;

        Ok(Self {
            table_path: table_path.to_path_buf(),
            segment_name,
        })
    }

    pub fn for_merge(
        table_path: &Path,
        sgm_id: &mut SegmentID,
        level: Levels,
    ) -> Result<Self, PgError> {
        let segment_id = sgm_id.get_and_next();

        let segment_name = format!("segment_{:07}_{}.bin", segment_id, level);
        let segment_path = Segment::get_path(table_path, &segment_name);

        if let Err(er) = File::create(segment_path.as_path()) {
            panic!(
                "from_merge: Failed to create new part. path={}, error= {}",
                segment_path.display(),
                er
            );
        };

        Ok(Self {
            table_path: table_path.to_path_buf(),
            segment_name,
        })
    }

    pub fn new(table_path: &Path, segment_name: &str) -> Self {
        Self {
            table_path: table_path.to_path_buf(),
            segment_name: segment_name.to_string(),
        }
    }

    pub fn get_table_path(&self) -> &Path {
        &self.table_path
    }

    pub fn get_name(&self) -> &str {
        &self.segment_name
    }

    pub fn get_path(table_path: &Path, segment_name: &str) -> PathBuf {
        let segment_path = format!("{}/segment/{}", table_path.to_str().unwrap(), segment_name);

        PathBuf::from(segment_path)
    }
}
