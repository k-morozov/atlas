use std::{
    fs::create_dir_all,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    thread,
};

use log::{error, info};

use crate::{
    core::{
        disk_table::{
            disk_table::{
                get_disk_table_name, get_disk_table_name_by_level, get_disk_table_path,
                ReaderDiskTableIterator,
            },
            local::{
                local_disk_table_builder::DiskTableBuilder,
                reader_local_disk_table::ReaderDiskTablePtr,
            },
            utils::{
                get_disk_tables, Levels, LevelsReaderDiskTables, SEGMENTS_MAX_LEVEL,
                SEGMENTS_MIN_LEVEL,
            },
        },
        entry::flexible_user_entry::FlexibleUserEntry,
        field::FlexibleField,
        mem_table::MemoryTable,
        merge::merge::is_ready_to_merge,
        storage::{
            config::{self, StorageConfig, DEFAULT_TEST_TABLES_PATH},
            metadata::StorageMetadata,
            storage::Storage,
        },
    },
    errors::Error,
};

fn create_dirs(storage_path: &Path) -> Result<(), Error> {
    create_dir_all(Path::new(storage_path))?;
    let segment_dir = format!("{}/segment", storage_path.to_str().unwrap());
    create_dir_all(Path::new(&segment_dir))?;

    Ok(())
}

pub struct OrderedStorage {
    // @todo
    storage_path: PathBuf,
    m_mem_table: Arc<RwLock<MemoryTable>>,
    // i_mem_table: Option<Box<MemoryTable>>,
    need_flush: Arc<AtomicBool>,
    flush_worker: Option<thread::JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
    metadata: Arc<Mutex<StorageMetadata>>,
    disk_tables: Arc<RwLock<LevelsReaderDiskTables>>,
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

        let metadata = Arc::new(Mutex::new(StorageMetadata::from_file(
            StorageMetadata::make_path(&storage_path).as_path(),
        )));

        let m_mem_table = Arc::new(RwLock::new(MemoryTable::new(config.mem_table_size)));
        let need_flush = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));

        let disk_tables = Arc::new(RwLock::new(disk_tables));

        let storage_path = storage_path.as_ref().to_path_buf();

        Self {
            m_mem_table: m_mem_table.clone(),
            // i_mem_table: None,
            need_flush: need_flush.clone(),
            shutdown: shutdown.clone(),
            storage_path: storage_path.clone(),
            metadata: metadata.clone(),
            disk_tables: disk_tables.clone(),
            config,

            flush_worker: Some(thread::spawn(move || loop {
                while !need_flush.load(Ordering::SeqCst) && !shutdown.load(Ordering::SeqCst) {
                    thread::sleep(std::time::Duration::from_secs(2));
                }

                if shutdown.load(Ordering::SeqCst) && !need_flush.load(Ordering::SeqCst) {
                    return;
                }
                info!("call flush");

                Self::save_mem_table(
                    m_mem_table.clone(),
                    metadata.clone(),
                    storage_path.clone(),
                    disk_tables.clone(),
                );

                Self::merge_disk_tables(
                    disk_tables.clone(),
                    metadata.clone(),
                    storage_path.clone(),
                );

                need_flush.store(false, Ordering::SeqCst);
            })),
        }
    }

    pub fn table_path(table_name: &str) -> PathBuf {
        PathBuf::from(&String::from(
            DEFAULT_TEST_TABLES_PATH.to_string() + table_name,
        ))
    }

    fn save_mem_table(
        mem_table: Arc<RwLock<MemoryTable>>,
        metadata: Arc<Mutex<StorageMetadata>>,
        storage_path: PathBuf,
        disk_tables: Arc<RwLock<LevelsReaderDiskTables>>,
    ) {
        let mut mem_table = mem_table.write().unwrap();

        if mem_table.current_size() == 0 {
            return;
        }

        let disk_table_id = metadata.lock().unwrap().get_new_disk_table_id();
        let segment_name = get_disk_table_name(disk_table_id);
        let disk_table_path = get_disk_table_path(storage_path.as_path(), &segment_name);

        let segment_from_mem_table = mem_table
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
                disk_tables
                    .write()
                    .unwrap()
                    .entry(SEGMENTS_MIN_LEVEL)
                    .or_insert_with(Vec::new)
                    .push(disk_table);
            }
            Err(er) => panic!("Failed save_mem_table. {}", er),
        }

        mem_table.clear();

        metadata.lock().unwrap().sync_disk();
    }

    fn merge_disk_tables(
        disk_tables: Arc<RwLock<LevelsReaderDiskTables>>,
        metadata: Arc<Mutex<StorageMetadata>>,
        storage_path: PathBuf,
    ) {
        for merging_level in SEGMENTS_MIN_LEVEL..=SEGMENTS_MAX_LEVEL {
            let level_for_new_sg = if merging_level != SEGMENTS_MAX_LEVEL {
                merging_level + 1
            } else {
                merging_level
            };

            let merged_disk_table = match Self::create_merged_disk_table(
                &disk_tables,
                merging_level,
                metadata.clone(),
                storage_path.as_path(),
            ) {
                Some(new_disk_table) => new_disk_table,
                None => continue,
            };

            let mut storages = disk_tables.write().unwrap();

            for merging_disk_table in storages.get_mut(&merging_level).unwrap() {
                match merging_disk_table.remove() {
                    Ok(_) => {}
                    Err(er) => panic!(
                        "failed remove merged disk table: path={}, error={}",
                        merging_disk_table.get_path().display(),
                        er,
                    ),
                }
            }

            storages.get_mut(&merging_level).unwrap().clear();
            storages
                .entry(level_for_new_sg)
                .or_insert_with(Vec::new)
                .push(merged_disk_table);
        }
    }

    fn create_merged_disk_table(
        disk_tables: &Arc<RwLock<LevelsReaderDiskTables>>,
        merging_level: Levels,
        metadata: Arc<Mutex<StorageMetadata>>,
        storage_path: &Path,
    ) -> Option<ReaderDiskTablePtr> {
        let storages = disk_tables.read().unwrap();
        if !is_ready_to_merge(&storages, merging_level) {
            return None;
        }
        // @todo
        match storages.get(&merging_level) {
            Some(segments_by_level) => {
                if segments_by_level.len() != config::DEFAULT_DISK_TABLES_LIMIT_BY_LEVEL {
                    return None;
                }
            }
            None => return None,
        }

        let level_for_new_sg = if merging_level != SEGMENTS_MAX_LEVEL {
            merging_level + 1
        } else {
            merging_level
        };

        let disk_table_id = metadata.lock().unwrap().get_new_disk_table_id();
        let segment_name = get_disk_table_name_by_level(disk_table_id, level_for_new_sg);
        let disk_table_path = get_disk_table_path(storage_path, &segment_name);
        let merging_segments = &storages[&merging_level];

        let mut its = merging_segments
            .iter()
            .map(|disk_table| disk_table.into_iter())
            .collect::<Vec<ReaderDiskTableIterator<FlexibleField, FlexibleField>>>();
        let mut entries = its.iter_mut().map(|it| it.next()).collect::<Vec<_>>();
        let mut builder = DiskTableBuilder::new(disk_table_path.as_path());

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

        Some(merged_disk_table)
    }
}

impl Drop for OrderedStorage {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.need_flush.store(true, Ordering::SeqCst);

        match self.flush_worker.take().unwrap().join() {
            Ok(_) => info!("Flush worker was joined"),
            Err(er) => error!("Failed join flush worker with error={:?}", er),
        }
    }
}

unsafe impl Sync for OrderedStorage {}

impl Storage for OrderedStorage {
    fn put(&self, entry: FlexibleUserEntry) -> Result<(), Error> {
        if self.shutdown.load(Ordering::SeqCst) {
            // @todo
            return Ok(());
        }
        let mut table = self.m_mem_table.write().unwrap();
        table.append(entry.clone());

        // refactoring
        if table.current_size() == table.max_table_size() {
            // cas
            self.need_flush.store(true, Ordering::SeqCst);
        }

        Ok(())
    }

    fn get(&self, key: &FlexibleField) -> Result<Option<FlexibleField>, Error> {
        if self.shutdown.load(Ordering::SeqCst) {
            return Ok(None);
        }
        if let Some(value) = self.m_mem_table.read().unwrap().get_value(key) {
            return Ok(Some(value));
        }

        let disk_tables = self.disk_tables.read().unwrap();
        let mut it = disk_tables.iter();

        for (_level, segments) in it.by_ref() {
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
    fn test_simple() -> io::Result<()> {
        let tmp_dir = Builder::new().prefix(DEFAULT_TEST_TABLES_PATH).tempdir()?;
        let table_path = tmp_dir.path().join("test_simple");

        {
            let mut config = StorageConfig::default_config();
            config.mem_table_size = 2;
            let table = OrderedStorage::new(table_path, config.clone());

            for index in 0..=config.mem_table_size as u8 {
                let entry = FlexibleUserEntry::new(
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
        let table = OrderedStorage::new(table_path, config.clone());

        for index in 0..3 * config.mem_table_size as u8 {
            let entry = FlexibleUserEntry::new(
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
            let table = OrderedStorage::new(&table_path, config.clone());

            for index in 0..10 * config.mem_table_size as u8 {
                let entry = FlexibleUserEntry::new(
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

        let table = OrderedStorage::new(table_path, config.clone());

        for index in 0..5 * config.mem_table_size as u8 {
            let entry = FlexibleUserEntry::new(
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

        let table = OrderedStorage::new(table_path, config.clone());

        for index in 0..64 * (config.mem_table_size - 1) as u8 {
            let entry = FlexibleUserEntry::new(
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
