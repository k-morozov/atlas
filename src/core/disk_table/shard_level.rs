use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, RwLock},
};

use crate::errors::{Error, Result};

use crate::core::{
    disk_table::{
        disk_table::ReaderDiskTableIterator, local::local_disk_table_builder::DiskTableBuilder,
    },
    field::FlexibleField,
    storage::config,
};

use crate::core::disk_table::local::reader_local_disk_table::ReaderDiskTablePtr;

use super::disk_table::ReaderDiskTable;

pub const SEGMENTS_MIN_LEVEL: Levels = 1;
pub const SEGMENTS_MAX_LEVEL: Levels = 3;

pub type Levels = u8;
pub type ReaderDiskTables = Vec<ReaderDiskTablePtr>;
// pub type DiskTablesShards = BTreeMap<Levels, ShardLevel>;

pub struct ShardLevel {
    disk_tables: Arc<RwLock<ReaderDiskTables>>,
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
        let it = &mut lock.iter();
        for r in it.next() {
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

    pub fn into_iter<'a>(&'a self) -> impl Iterator<Item = ReaderDiskTablePtr> + '_ {
        ShardLevelIterator {
            shard: self,
            pos: 0,
        }
    }
}

struct ShardLevelIterator<'a> {
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

pub struct DiskTablesShards {
    table: RwLock<BTreeMap<Levels, ShardLevel>>,
}

impl DiskTablesShards {
    pub fn new() -> Self {
        Self {
            table: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn put_disk_table_by_level(&self, level: Levels, disk_table: ReaderDiskTablePtr) {
        let mut no_level = false;
        {
            let lock = self.table.read().unwrap();
            if !lock.contains_key(&level) {
                no_level = true;
            }
        }

        let mut lock = self.table.write().unwrap();
        if no_level {
            lock.insert(level, ShardLevel::new());
        }
        lock.get(&level)
            .expect("we checked key early")
            .push(disk_table);

        // @todo return status?
    }

    pub fn get_disk_table_by_level(&self, level: Levels) -> Option<ReaderDiskTablePtr> {
        todo!()
    }

    pub fn merge_level(
        &self,
        level: Levels,
        disk_table_path: &Path,
    ) -> Arc<dyn ReaderDiskTable<FlexibleField, FlexibleField>> {
        let lock = self.table.read().unwrap();

        assert!(lock.contains_key(&level));

        let merging_tables = lock.get(&level).unwrap();

        // @todo
        let dt = merging_tables.disk_tables.read().unwrap();

        let mut its = dt
            .iter()
            .map(|disk_table| disk_table.into_iter())
            .collect::<Vec<ReaderDiskTableIterator<FlexibleField, FlexibleField>>>();

        let mut entries = its.iter_mut().map(|it| it.next()).collect::<Vec<_>>();
        let mut builder = DiskTableBuilder::new(disk_table_path);

        while entries.iter().any(|v| v.is_some()) {
            let (index, entry) = entries
                .iter()
                .enumerate()
                .filter(|(_index, v)| v.is_some())
                .map(|(index, entry)| {
                    let e = entry.as_ref().unwrap();
                    (index, e)
                })
                .collect::<Vec<_>>()
                // @todo iter vs into_iter
                .into_iter()
                .min_by(|lhs, rhs| lhs.1.get_key().cmp(rhs.1.get_key()))
                .unwrap();

            builder.append_entry(entry);

            if let Some(it) = its.get_mut(index) {
                entries[index] = it.next();
            }
        }

        let Ok(merged_disk_table) = builder.build() else {
            panic!("Failed create disk table for merge_disk_tables")
        };

        merged_disk_table
    }

    pub fn remove_tables_from_level(&self, level: Levels) -> Result<()> {
        let lock = self.table.read().unwrap();

        assert!(lock.contains_key(&level));

        let removing_tables = lock.get(&level).unwrap();
        removing_tables.clear()?;

        Ok(())
    }

    pub fn is_ready_to_merge(&self, level: Levels) -> bool {
        let lock = self.table.read().unwrap();

        if !lock.contains_key(&level) {
            return false;
        }
        lock[&level].len() == config::DEFAULT_DISK_TABLES_LIMIT_BY_LEVEL
    }

    // workaround
    pub fn get(&self, key: &FlexibleField) -> Result<Option<FlexibleField>> {
        for (_level, shard) in self.table.read().unwrap().iter() {
            for segment in shard.into_iter() {
                match segment.read(key) {
                    Ok(v) => match v {
                        Some(v) => return Ok(Some(v)),
                        None => continue,
                    },
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
        }

        Ok(None)
    }
}
