use std::sync::{Arc, RwLock};

use crate::errors::Result;

use crate::core::disk_table::local::reader_local_disk_table::ReaderDiskTablePtr;

pub type ReaderDiskTables = Vec<ReaderDiskTablePtr>;

pub(super) struct ShardLevel {
    pub disk_tables: Arc<RwLock<ReaderDiskTables>>,
}

impl ShardLevel {
    pub fn new() -> Self {
        Self {
            disk_tables: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn push(&self, reader: ReaderDiskTablePtr) {
        let mut lock = self.disk_tables.write().unwrap();
        lock.push(reader);
    }

    pub fn get(&self, index: usize) -> ReaderDiskTablePtr {
        let lock = self.disk_tables.read().unwrap();
        lock[index].clone()
    }

    pub fn clear(&self) -> Result<()> {
        let mut lock = self.disk_tables.write().unwrap();

        for r in &mut lock.iter() {
            assert_eq!(Arc::strong_count(r), 1);
            r.remove()?;
        }
        lock.clear();

        Ok(())
    }

    pub fn len(&self) -> usize {
        let lock = self.disk_tables.read().unwrap();
        lock.len()
    }

    pub fn iter(&self) -> ShardLevelIterator {
        ShardLevelIterator {
            shard: self,
            pos: 0,
        }
    }
}

pub struct ShardLevelIterator<'a> {
    shard: &'a ShardLevel,
    pos: usize,
}

impl<'a> Iterator for ShardLevelIterator<'a> {
    type Item = ReaderDiskTablePtr;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.shard.len() {
            return None;
        }
        let index = self.pos;
        self.pos += 1;
        Some(self.shard.get(index))
    }
}
