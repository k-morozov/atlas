use std::path::{Path, PathBuf};

use super::utils::Levels;
use crate::core::entry::entry::Entry;
use crate::errors::Result;

pub type WriterSegmentPtr<K, V> = Box<dyn WriterSegment<K, V>>;
pub type ReaderSegmentPtr<K, V> = Box<dyn ReaderSegment<K, V>>;

pub trait Writer<K, V> {
    fn write(&mut self, entry: Entry<K, V>) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

pub trait Reader<K, V> {
    fn read(&self, key: &K) -> Result<Option<V>>;
    fn read_entry_by_index(&self, index: u32) -> Result<Option<Entry<K, V>>>;
    fn count_entries(&self) -> u32;
}

pub trait Segment<K, V> {
    fn get_path(&self) -> &Path;
    fn remove(&self) -> Result<()>;
}

pub trait WriterSegment<K, V>: Segment<K, V> + Writer<K, V> {}
pub trait ReaderSegment<K, V>: Segment<K, V> + Reader<K, V> {}

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

pub struct ReaderSegmentIterator<'a, K, V> {
    segment: &'a dyn ReaderSegment<K, V>,
    index: u32,
    max_index: u32,
}

impl<'a, K, V> Iterator for ReaderSegmentIterator<'a, K, V> {
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

impl<'a, K, V> IntoIterator for &'a dyn ReaderSegment<K, V> {
    type Item = Entry<K, V>;
    type IntoIter = ReaderSegmentIterator<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        let max_index = self.count_entries();

        ReaderSegmentIterator {
            segment: self,
            index: 0,
            max_index,
        }
    }
}
