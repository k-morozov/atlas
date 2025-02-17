use std::path::{Path, PathBuf};

use super::utils::Levels;
use crate::core::entry::entry::Entry;
use crate::errors::Result;

pub type SegmentPtr<K, V> = Box<dyn Segment<K, V>>;

pub trait SegmentWriter<K, V> {
    fn write(&mut self, entry: Entry<K, V>) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

pub trait SegmentReader<K, V> {
    fn read(&self, key: &K) -> Result<Option<V>>;
    fn read_entry_by_index(&self, index: u64) -> Result<Option<Entry<K, V>>>;
    fn read_size(&self) -> Result<u64>;
}

pub trait Segment<K, V>: SegmentWriter<K, V> + SegmentReader<K, V> {
    fn get_path(&self) -> &Path;
    fn remove(&self) -> Result<()>;
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

pub struct SegmentIterator<'a, K, V> {
    segment: &'a dyn Segment<K, V>,
    index: u64,
    max_index: u64,
}

impl<'a, K, V> Iterator for SegmentIterator<'a, K, V> {
    type Item = Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.max_index {
            return None;
        }
        match self.segment.read_entry_by_index(self.index) {
            Ok(r) => {
                self.index += 1;
                return r;
            }
            Err(e) => {
                panic!("failed read iterator: {}", e)
            }
        }
    }
}

impl<'a, K, V> IntoIterator for &'a dyn Segment<K, V> {
    type Item = Entry<K, V>;
    type IntoIter = SegmentIterator<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        let max_index = match self.read_size() {
            Ok(idx) => idx,
            Err(er) => panic!("failed read max index: {}", er),
        };

        SegmentIterator {
            segment: self,
            index: 0,
            max_index,
        }
    }
}
