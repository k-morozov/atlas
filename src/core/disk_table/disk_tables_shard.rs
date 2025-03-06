use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, RwLock},
};

use log::trace;

use crate::errors::Result;

use crate::core::{
    disk_table::{
        disk_table::ReaderDiskTableIterator, local::local_disk_table_builder::DiskTableBuilder,
        local::reader_local_disk_table::ReaderDiskTablePtr, shard_level::ShardLevel,
    },
    field::FlexibleField,
    storage::config,
};

use super::disk_table::ReaderDiskTable;

pub const SEGMENTS_MIN_LEVEL: Levels = 1;
pub const SEGMENTS_MAX_LEVEL: Levels = 3;

pub type Levels = u8;

pub struct DiskTablesShards {
    shards: RwLock<BTreeMap<Levels, ShardLevel>>,
}

impl DiskTablesShards {
    pub fn new() -> Self {
        Self {
            shards: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn remove_level_and_put(
        &self,
        removing_level: Levels,
        merged_level: Levels,
        disk_table: ReaderDiskTablePtr,
    ) -> Result<()> {
        let mut lock = self.shards.write().unwrap();

        assert!(lock.contains_key(&removing_level));

        let removing_tables = lock.get(&removing_level).unwrap();
        removing_tables.clear()?;

        let mut no_level = false;
        {
            // let lock = self.shards.read().unwrap();
            if !lock.contains_key(&merged_level) {
                no_level = true;
            }
        }

        // let mut lock = self.shards.write().unwrap();
        if no_level {
            lock.insert(merged_level, ShardLevel::new());
        }

        let shard = lock.get(&merged_level).expect("we checked key early");

        shard.push(disk_table);

        Ok(())
    }

    pub fn put_disk_table_by_level(&self, level: Levels, disk_table: ReaderDiskTablePtr) {
        trace!("call put_disk_table_by_level with level={}", level);

        let mut no_level = false;
        {
            let lock = self.shards.read().unwrap();
            if !lock.contains_key(&level) {
                no_level = true;
            }
        }

        let mut lock = self.shards.write().unwrap();
        if no_level {
            lock.insert(level, ShardLevel::new());
        }

        let shard = lock.get(&level).expect("we checked key early");

        shard.push(disk_table);

        trace!(
            "merged disk talbe was appended to level={}, {} tables in level ",
            level,
            shard.len(),
        );

        // @todo return status?
    }

    pub fn merge_level(
        &self,
        level: Levels,
        disk_table_path: &Path,
    ) -> Arc<dyn ReaderDiskTable<FlexibleField, FlexibleField>> {
        let lock = self.shards.read().unwrap();

        assert!(lock.contains_key(&level));

        let merging_tables = lock.get(&level).unwrap();

        // @todo
        let disk_tables = merging_tables.disk_tables.read().unwrap();

        let mut its = disk_tables
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

    pub fn is_ready_to_merge(&self, level: Levels) -> bool {
        let lock = self.shards.read().unwrap();

        if !lock.contains_key(&level) {
            return false;
        }

        lock[&level].len() == config::DEFAULT_DISK_TABLES_LIMIT_BY_LEVEL
    }

    // workaround
    pub fn get(&self, key: &FlexibleField) -> Result<Option<FlexibleField>> {
        let shards = self.shards.read().unwrap();

        for (_level, shard) in shards.iter() {
            for (_index, disk_table) in shard.iter().enumerate() {
                match disk_table.read(key) {
                    Ok(v) => match v {
                        Some(v) => return Ok(Some(v)),
                        None => continue,
                    },
                    Err(e) => return Err(e),
                }
            }
        }

        Ok(None)
    }
}
