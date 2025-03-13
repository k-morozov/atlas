use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::disk_tables_shard::Levels;
use super::id::DiskTableID;
use super::local::block::data_block;
use crate::core::entry::user_entry::UserEntry;
use crate::core::field::Field;
use crate::errors::Result;

pub type WriterDiskTablePtr<K, V> = Box<dyn WriterDiskTable<K, V>>;
pub type ReaderDiskTablePtr<K, V> = Arc<dyn ReaderDiskTable<K, V>>;

pub trait Writer<K, V> {
    fn write(&mut self, buffer: &[u8]) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

pub trait Reader<K, V> {
    fn read(&self, key: &K) -> Result<Option<V>>;
    fn read_entry_by_index(&self, index: u32) -> Result<Option<UserEntry<K, V>>>;
    fn read_block(&self, index: usize) -> Option<data_block::DataBlock<K, V>>;
    fn count_entries(&self) -> u32;
}

// @todo
pub trait DiskTable<K, V> {
    fn get_path(&self) -> &Path;
    fn remove(&self) -> Result<()>;
}

pub trait WriterDiskTable<K, V>: DiskTable<K, V> + Writer<K, V> {}
pub trait ReaderDiskTable<K, V>: DiskTable<K, V> + Reader<K, V> + Send + Sync {}

pub fn get_disk_table_path(
    storage_path: &Path,
    disk_table_name: &str,
    index_table_name: &str,
) -> (PathBuf, PathBuf) {
    let disk_table_path = format!(
        "{}/segment/{}",
        storage_path.to_str().unwrap(),
        disk_table_name
    );
    let index_table_path = format!(
        "{}/segment/{}",
        storage_path.to_str().unwrap(),
        index_table_name
    );

    (
        PathBuf::from(disk_table_path),
        PathBuf::from(index_table_path),
    )
}

pub fn get_disk_table_name(disk_table_id: DiskTableID) -> (String, String) {
    get_disk_table_name_by_level(disk_table_id, 1)
}

pub fn get_disk_table_name_by_level(disk_table_id: DiskTableID, level: Levels) -> (String, String) {
    let disk_table_name = format!("segment_{:07}_{}.bin", disk_table_id, level);
    let index_table_name = format!("segment_{:07}_{}.idx", disk_table_id, level);
    (disk_table_name, index_table_name)
}

pub struct ReaderDiskTableIterator<'a, K, V> {
    disk_table: &'a dyn ReaderDiskTable<K, V>,
    index: usize,
    block_it: Option<Box<dyn Iterator<Item = UserEntry<K, V>> + 'a>>,
}

impl<'a, K, V> Iterator for ReaderDiskTableIterator<'a, K, V>
where
    K: Field + Clone + Ord,
    V: Field + Clone,
{
    type Item = UserEntry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.block_it {
            Some(it) => {
                let res = it.next();
                if res.is_none() {
                    let next_block = self.disk_table.read_block(self.index)?;
                    self.index += 1;

                    let it = next_block.into_iter();
                    let it = self.block_it.insert(Box::new(it));

                    let res = it.next();
                    return res;
                } else {
                    return res;
                }
            }
            None => {
                let next_block = self.disk_table.read_block(self.index)?;
                self.index += 1;

                let it = next_block.into_iter();
                let it = self.block_it.insert(Box::new(it));

                let res = it.next();
                return res;
            }
        }
    }
}

impl<'a, K, V> IntoIterator for &'a dyn ReaderDiskTable<K, V>
where
    K: Field + Clone + Ord,
    V: Field + Clone,
{
    type Item = UserEntry<K, V>;
    type IntoIter = ReaderDiskTableIterator<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        ReaderDiskTableIterator {
            disk_table: self,
            index: 0,
            block_it: None,
        }
    }
}
