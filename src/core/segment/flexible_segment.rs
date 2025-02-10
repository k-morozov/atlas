use std::path::{Path, PathBuf};

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
