use std::path::{Path, PathBuf};

use crate::core::entry::flexible_entry::FlexibleEntry;

use super::{
    flexible_segment::{FlexibleSegment, FlexibleSegmentPtr},
    flexible_writer::FlexibleWriter,
};

pub struct FlexibleSegmentBuilder {
    writer: FlexibleWriter,
    building_segment: Option<Box<FlexibleSegment>>,
    table_path: Option<PathBuf>,
    segment_name: Option<String>,
}

impl FlexibleSegmentBuilder {
    pub fn new(path: &Path) -> Self {
        FlexibleSegmentBuilder {
            writer: FlexibleWriter::new(path),
            building_segment: None,
            table_path: None,
            segment_name: None,
        }
    }

    pub fn set_table_path(&mut self, table_path: &Path) -> &mut Self {
        self.table_path = Some(table_path.to_path_buf());
        self
    }

    pub fn set_segment_name(&mut self, segment_name: &str) -> &mut Self {
        self.segment_name = Some(segment_name.to_string());
        self
    }

    pub fn prepare_empty_segment(&mut self) -> &mut Self {
        let table_path = match &self.table_path {
            Some(table_path) => table_path,
            None => panic!("build_empty_segment without table_path"),
        };

        let segment_name = match &self.segment_name {
            Some(segment_name) => segment_name,
            None => panic!("build_empty_segment without segment_name"),
        };

        self.building_segment = Some(Box::new(FlexibleSegment::new(
            table_path.as_path(),
            segment_name.as_str(),
        )));
        self
    }

    pub fn append_entry(&mut self, entry: FlexibleEntry) -> &mut Self {
        if let Err(_er) = self.writer.write_entry(&entry) {
            panic!("Failed write in builder")
        }
        self
    }

    pub fn build(&mut self) -> FlexibleSegmentPtr {
        if let Err(_) = self.writer.flush() {
            panic!("Failed flush in builder")
        }

        match self.building_segment.take() {
            Some(segment) => segment,
            None => panic!("building_segment is empty"),
        }
    }
}
