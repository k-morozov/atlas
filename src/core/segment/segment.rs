use std::path::{Path, PathBuf};

use crate::core::entry::entry::{ReadEntry, WriteEntry};

use super::table::Levels;

pub type SegmentPtr<K, V> = Box<dyn Segment<K, V>>;

pub trait Segment<K, V>: WriteEntry<K, V> + ReadEntry<K, V> {
    fn get_table_path(&self) -> &Path;
    fn get_name(&self) -> &str;
}

pub fn get_segment_path(table_path: &Path, segment_name: &str) -> PathBuf {
    let segment_path = format!("{}/segment/{}", table_path.to_str().unwrap(), segment_name);

    PathBuf::from(segment_path)
}

pub fn get_segment_name(segment_id: u64) -> String {
    get_segment_name_by_level(segment_id, 1)
}

pub fn get_segment_name_by_level(segment_id: u64, level: Levels) -> String {
    let segment_name = format!("segment_{:07}_{}.bin", segment_id, level);
    segment_name
}
