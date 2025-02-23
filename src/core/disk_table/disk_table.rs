use std::path::{Path, PathBuf};

use super::id::DiskTableID;
use super::utils::Levels;
use crate::core::entry::entry::Entry;
use crate::errors::Result;

pub type WriterDiskTablePtr<K, V> = Box<dyn WriterDiskTable<K, V>>;
pub type ReaderDiskTablePtr<K, V> = Box<dyn ReaderDiskTable<K, V>>;

pub trait Writer<K, V> {
    fn write(&mut self, buffer: &[u8]) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

pub trait Reader<K, V> {
    fn read(&self, key: &K) -> Result<Option<V>>;
    fn read_entry_by_index(&self, index: u32) -> Result<Option<Entry<K, V>>>;
    fn count_entries(&self) -> u32;
}

pub trait DiskTable<K, V> {
    fn get_path(&self) -> &Path;
    fn remove(&self) -> Result<()>;
}

pub trait WriterDiskTable<K, V>: DiskTable<K, V> + Writer<K, V> {}
pub trait ReaderDiskTable<K, V>: DiskTable<K, V> + Reader<K, V> {}

pub fn get_disk_table_path(storage_path: &Path, segment_name: &str) -> PathBuf {
    let disk_table_path = format!(
        "{}/segment/{}",
        storage_path.to_str().unwrap(),
        segment_name
    );

    PathBuf::from(disk_table_path)
}

pub fn get_disk_table_name(disk_table_id: DiskTableID) -> String {
    get_disk_table_name_by_level(disk_table_id, 1)
}

pub fn get_disk_table_name_by_level(disk_table_id: DiskTableID, level: Levels) -> String {
    let segment_name = format!("segment_{:07}_{}.bin", disk_table_id, level);
    segment_name
}

pub struct ReaderDiskTableIterator<'a, K, V> {
    disk_table: &'a dyn ReaderDiskTable<K, V>,
    index: u32,
    max_index: u32,
}

impl<'a, K, V> Iterator for ReaderDiskTableIterator<'a, K, V> {
    type Item = Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.max_index {
            return None;
        }
        match self.disk_table.read_entry_by_index(self.index) {
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

impl<'a, K, V> IntoIterator for &'a dyn ReaderDiskTable<K, V> {
    type Item = Entry<K, V>;
    type IntoIter = ReaderDiskTableIterator<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        let max_index = self.count_entries();

        ReaderDiskTableIterator {
            disk_table: self,
            index: 0,
            max_index,
        }
    }
}
