use std::fs::create_dir_all;
use std::path::{Path, PathBuf};

use super::storage::Storage;
use crate::core::disk_table::{
    disk_table::{get_disk_table_name, get_disk_table_path},
    local::disk_table_builder::DiskTableBuilder,
    utils::{get_disk_tables, LevelsReaderDiskTables, SEGMENTS_MIN_LEVEL},
};
use crate::core::entry::flexible_entry::FlexibleEntry;
use crate::core::field::FlexibleField;
use crate::core::mem_table::MemoryTable;
use crate::core::merge::merge::{is_ready_to_merge, merge_disk_tables};
use crate::core::storage::{
    config::{StorageConfig, DEFAULT_TEST_TABLES_PATH},
    metadata::StorageMetadata,
};
use crate::errors::Error;

fn create_dirs(storage_path: &Path) -> Result<(), Error> {
    create_dir_all(Path::new(storage_path))?;
    let segment_dir = format!("{}/segment", storage_path.to_str().unwrap());
    create_dir_all(Path::new(&segment_dir))?;

    Ok(())
}

pub struct OrderedStorage {
    storage_path: PathBuf,
    mem_table: MemoryTable,
    metadata: StorageMetadata,
    disk_tables: LevelsReaderDiskTables,
    config: StorageConfig,
}

impl OrderedStorage {
    pub fn new<P: AsRef<Path>>(storage_path: P, config: StorageConfig) -> Self {
        if let Err(er) = create_dirs(storage_path.as_ref()) {
            panic!(
                "Faield create storage dirs: table_path={}, error={}",
                storage_path.as_ref().display(),
                er
            )
        }

        let Ok(disk_tables) = get_disk_tables(storage_path.as_ref()) else {
            panic!("Faield read disk tables")
        };

        let metadata =
            StorageMetadata::from_file(StorageMetadata::make_path(&storage_path).as_path());

        Self {
            mem_table: MemoryTable::new(config.mem_table_size),
            storage_path: storage_path.as_ref().to_path_buf(),
            metadata,
            disk_tables,
            config,
        }
    }

    pub fn table_path(table_name: &str) -> PathBuf {
        PathBuf::from(&String::from(
            DEFAULT_TEST_TABLES_PATH.to_string() + table_name,
        ))
    }

    fn save_mem_table(&mut self) {
        let segment_id = self.metadata.segment_id.get_and_next();
        let segment_name = get_disk_table_name(segment_id);
        let disk_table_path = get_disk_table_path(self.storage_path.as_path(), &segment_name);

        let segment_from_mem_table = self
            .mem_table
            .into_iter()
            .fold(
                DiskTableBuilder::new(disk_table_path.as_path()),
                |mut builder, entry| {
                    builder.append_entry(entry);
                    builder
                },
            )
            .build();

        match segment_from_mem_table {
            Ok(disk_table) => {
                self.disk_tables
                    .entry(SEGMENTS_MIN_LEVEL)
                    .or_insert_with(Vec::new)
                    .push(disk_table);
            }
            Err(er) => panic!("Failed save_mem_table. {}", er),
        }

        self.mem_table = MemoryTable::new(self.config.mem_table_size);

        self.metadata
            .sync_disk(Path::new(self.metadata.get_metadata_path()));
        self.mem_table.clear();
    }
}

impl Storage for OrderedStorage {
    fn put(&mut self, entry: FlexibleEntry) -> Result<(), Error> {
        self.mem_table.append(entry.clone());

        if self.mem_table.current_size() == self.mem_table.max_table_size() {
            self.save_mem_table();

            // @todo
            if is_ready_to_merge(&self.disk_tables) {
                merge_disk_tables(
                    &mut self.disk_tables,
                    self.storage_path.as_path(),
                    &mut self.metadata.segment_id,
                );
            }
        }

        Ok(())
    }

    fn get(&self, key: &FlexibleField) -> Result<Option<FlexibleField>, Error> {
        if let Some(value) = self.mem_table.get_value(key) {
            return Ok(Some(value));
        }

        for (_level, segments) in &self.disk_tables {
            for segment in segments {
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

#[cfg(test)]
mod tests {
    use std::io;
    use tempfile::Builder;

    use super::*;
    use crate::core::field::*;
    use crate::core::storage::config::DEFAULT_TEST_TABLES_PATH;

    #[test]
    fn test_segment() -> io::Result<()> {
        let tmp_dir = Builder::new().prefix(DEFAULT_TEST_TABLES_PATH).tempdir()?;
        let table_path = tmp_dir.path().join("test_segment");

        {
            let config = StorageConfig::default_config();
            let mut table = OrderedStorage::new(table_path, config.clone());

            for index in 0..=config.mem_table_size as u8 {
                let entry = FlexibleEntry::new(
                    FlexibleField::new(vec![index, 3, 4]),
                    FlexibleField::new(vec![index * 10, 30, 40]),
                );
                table.put(entry).unwrap();
            }

            for index in 0..=config.mem_table_size as u8 {
                let result = table.get(&FlexibleField::new(vec![index, 3, 4])).unwrap();
                assert_eq!(
                    result.unwrap(),
                    FlexibleField::new(vec!(index * 10, 30, 40))
                );
            }
        }

        Ok(())
    }

    #[test]
    fn test_some_segments() -> io::Result<()> {
        let tmp_dir = Builder::new().prefix(DEFAULT_TEST_TABLES_PATH).tempdir()?;
        let table_path = tmp_dir.path().join("test_some_segments");

        let config = StorageConfig::default_config();
        let mut table = OrderedStorage::new(table_path, config.clone());

        for index in 0..3 * config.mem_table_size as u8 {
            let entry = FlexibleEntry::new(
                FlexibleField::new(vec![index, 3, 4]),
                FlexibleField::new(vec![index * 10, 30, 40]),
            );
            table.put(entry).unwrap();
        }

        for index in 0..3 * config.mem_table_size as u8 {
            let result = table.get(&FlexibleField::new(vec![index, 3, 4])).unwrap();
            assert_eq!(
                result.unwrap(),
                FlexibleField::new(vec![index * 10, 30, 40])
            );
        }

        Ok(())
    }

    #[test]
    fn test_some_segments_with_restart() -> io::Result<()> {
        let tmp_dir = Builder::new().prefix(DEFAULT_TEST_TABLES_PATH).tempdir()?;
        let table_path = tmp_dir.path().join("test_some_segments_with_restart");

        let config = StorageConfig::default_config();
        {
            let mut table = OrderedStorage::new(&table_path, config.clone());

            for index in 0..10 * config.mem_table_size as u8 {
                let entry = FlexibleEntry::new(
                    FlexibleField::new(vec![index, 3, 4]),
                    FlexibleField::new(vec![index * 2, 30, 40]),
                );
                table.put(entry).unwrap();
            }

            for index in 0..10 * config.mem_table_size as u8 {
                let result = table.get(&FlexibleField::new(vec![index, 3, 4])).unwrap();
                assert_eq!(result.unwrap(), FlexibleField::new(vec![index * 2, 30, 40]));
            }
        }

        let table = OrderedStorage::new(&table_path, config.clone());
        for index in 0..10 * config.mem_table_size as u8 {
            let result = table.get(&FlexibleField::new(vec![index, 3, 4])).unwrap();
            assert_eq!(result.unwrap(), FlexibleField::new(vec![index * 2, 30, 40]));
        }

        Ok(())
    }

    #[test]
    fn test_merge_sements() -> io::Result<()> {
        let tmp_dir = Builder::new().prefix(DEFAULT_TEST_TABLES_PATH).tempdir()?;
        let table_path = tmp_dir.path().join("test_merge_sements");

        let config = StorageConfig::default_config();

        let mut table = OrderedStorage::new(table_path, config.clone());

        for index in 0..5 * config.mem_table_size as u8 {
            let entry = FlexibleEntry::new(
                FlexibleField::new(vec![index, 3, 4]),
                FlexibleField::new(vec![index * 2, 30, 40]),
            );
            table.put(entry).unwrap();
        }

        for index in 0..5 * config.mem_table_size as u8 {
            let result = table.get(&FlexibleField::new(vec![index, 3, 4])).unwrap();
            assert_eq!(result.unwrap(), FlexibleField::new(vec![index * 2, 30, 40]));
        }

        Ok(())
    }

    #[test]
    fn test_merge_sements_max_level() -> io::Result<()> {
        let tmp_dir = Builder::new().prefix(DEFAULT_TEST_TABLES_PATH).tempdir()?;
        let table_path = tmp_dir.path().join("test_merge_sements_max_level");

        let config = StorageConfig::default_config();

        let mut table = OrderedStorage::new(table_path, config.clone());

        for index in 0..64 * (config.mem_table_size - 1) as u8 {
            let entry = FlexibleEntry::new(
                FlexibleField::new(vec![index, 3, 4]),
                FlexibleField::new(vec![index, 30, 40]),
            );
            table.put(entry).unwrap();
        }

        for index in 0..64 * (config.mem_table_size - 1) as u8 {
            let result = table.get(&FlexibleField::new(vec![index, 3, 4])).unwrap();
            assert_eq!(result.unwrap(), FlexibleField::new(vec![index, 30, 40]));
        }

        Ok(())
    }
}
