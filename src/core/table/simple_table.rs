use std::fs::create_dir_all;
use std::path::{Path, PathBuf};

use super::table::Table;
use crate::core::entry::flexible_entry::FlexibleEntry;
use crate::core::field::FlexibleField;
use crate::core::mem_table::MemTable;
use crate::core::merge::merge::{is_ready_to_merge, merge_segments};
use crate::core::segment::segment::{get_segment_name, get_segment_path};
use crate::core::segment::{
    segment_builder::FlexibleSegmentBuilder,
    table::{get_table_segments, TableSegments, SEGMENTS_MIN_LEVEL},
};
use crate::core::table::config::{TableConfig, DEFAULT_TABLES_PATH};
use crate::core::table::metadata::TableMetadata;
use crate::errors::Error;

fn create_dirs(table_path: &Path) -> Result<(), Error> {
    create_dir_all(Path::new(table_path))?;
    let segment_dir = format!("{}/segment", table_path.to_str().unwrap());
    create_dir_all(Path::new(&segment_dir))?;

    Ok(())
}

pub struct SimpleTable {
    table_name: String,
    table_path: PathBuf,

    mem_table: MemTable,

    metadata: TableMetadata,

    // @todo possibly add to metadata
    segments: TableSegments,

    config: TableConfig,
}

impl SimpleTable {
    pub fn new(table_name: &str, config: TableConfig) -> Self {
        let table_path = SimpleTable::table_path(table_name);

        if let Err(_) = create_dirs(table_path.as_path()) {
            panic!("Faield create table dirs")
        }

        // let schema = Rc::new(vec![
        //     FixedField::new(FieldType::Int32(0)),
        //     FixedField::new(FieldType::Int32(0)),
        // ]);

        let segments = get_table_segments(table_path.as_path());

        if let Err(_) = segments {
            panic!("Faield read segments")
        }

        let segments = segments.unwrap();

        let metadata =
            TableMetadata::from_file(TableMetadata::make_path(table_path.as_path()).as_path());

        Self {
            mem_table: MemTable::new(config.mem_table_size),
            table_name: table_name.to_string(),
            table_path: table_path.to_path_buf(),
            metadata,
            segments,
            config,
        }
    }

    pub fn table_path(table_name: &str) -> PathBuf {
        PathBuf::from(&String::from(DEFAULT_TABLES_PATH.to_string() + table_name))
    }

    fn save_mem_table(&mut self) {
        let segment_id = self.metadata.segment_id.get_and_next();
        let segment_name = get_segment_name(segment_id);
        let segment_path = get_segment_path(self.table_path.as_path(), &segment_name);

        let segment_from_mem_table = self
            .mem_table
            .into_iter()
            .fold(
                FlexibleSegmentBuilder::new(&segment_path)
                    .set_segment_name(&segment_name)
                    .set_table_path(self.table_path.as_path())
                    .prepare_empty_segment(),
                |builder, entry| builder.append_entry(entry),
            )
            .build();

        self.segments
            .entry(SEGMENTS_MIN_LEVEL)
            .or_insert_with(Vec::new)
            .push(segment_from_mem_table);

        self.mem_table = MemTable::new(self.config.mem_table_size);

        self.metadata
            .sync_disk(Path::new(self.metadata.get_metadata_path()));
        self.mem_table.clear();
    }
}

impl Table for SimpleTable {
    fn put(&mut self, entry: FlexibleEntry) -> Result<(), Error> {
        self.mem_table.append(entry);

        if self.mem_table.current_size() == self.mem_table.max_table_size() {
            self.save_mem_table();

            // @todo
            if is_ready_to_merge(&self.segments) {
                merge_segments(
                    &mut self.segments,
                    self.table_path.as_path(),
                    &mut self.metadata.segment_id,
                );
            }
        }

        Ok(())
    }

    fn get(&self, key: FlexibleField) -> Result<Option<FlexibleField>, Error> {
        if let Some(value) = self.mem_table.get_value(&key) {
            return Ok(Some(value));
        }

        for (_level, segments) in &self.segments {
            for segment in segments {
                match segment.read(&key) {
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
    use std::fs::{remove_dir_all, remove_file};
    use std::io::ErrorKind;

    use super::*;
    use crate::core::field::*;

    // @todo
    fn prepare_dir() {
        let path = Path::new(DEFAULT_TABLES_PATH);
        if let Err(er) = remove_file(path) {
            match er.kind() {
                ErrorKind::NotFound | ErrorKind::IsADirectory => return,
                _ => panic!("unexpected errror: {}", er),
            }
        }
    }

    fn drop_dir(path: &Path) {
        if let Err(er) = remove_dir_all(path) {
            match er.kind() {
                ErrorKind::NotFound | ErrorKind::IsADirectory => return,
                _ => panic!("unexpected errror from remove_dir_all: {}", er),
            }
        }
    }

    #[test]
    fn test_segment() {
        prepare_dir();

        let table_name = "test_table_segment";

        let config = TableConfig::default_config();
        let mut table = SimpleTable::new(table_name, config.clone());

        for index in 0..=config.mem_table_size as u8 {
            let entry = FlexibleEntry::new(
                FlexibleField::new(vec![index, 3, 4]),
                FlexibleField::new(vec![index * 10, 30, 40]),
            );
            table.put(entry).unwrap();
        }

        for index in 0..=config.mem_table_size as u8 {
            let result = table.get(FlexibleField::new(vec![index, 3, 4])).unwrap();
            assert_eq!(
                result.unwrap(),
                FlexibleField::new(vec!(index * 10, 30, 40))
            );
        }

        drop_dir(SimpleTable::table_path(table_name).as_path());
    }

    #[test]
    fn test_some_segments() {
        prepare_dir();

        let table_name = "test_table_some_segments";
        let config = TableConfig::default_config();
        let mut table = SimpleTable::new(table_name, config.clone());

        for index in 0..3 * config.mem_table_size as u8 {
            let entry = FlexibleEntry::new(
                FlexibleField::new(vec![index, 3, 4]),
                FlexibleField::new(vec![index * 10, 30, 40]),
            );
            table.put(entry).unwrap();
        }

        for index in 0..3 * config.mem_table_size as u8 {
            let result = table.get(FlexibleField::new(vec![index, 3, 4])).unwrap();
            assert_eq!(
                result.unwrap(),
                FlexibleField::new(vec![index * 10, 30, 40])
            );
        }

        drop_dir(SimpleTable::table_path(table_name).as_path());
    }

    #[test]
    fn test_some_segments_with_restart() {
        prepare_dir();

        let table_name = "test_table_some_segments_with_restart";
        let config = TableConfig::default_config();
        {
            let mut table = SimpleTable::new(table_name, config.clone());

            for index in 0..10 * config.mem_table_size as u8 {
                let entry = FlexibleEntry::new(
                    FlexibleField::new(vec![index, 3, 4]),
                    FlexibleField::new(vec![index * 2, 30, 40]),
                );
                table.put(entry).unwrap();
            }

            for index in 0..10 * config.mem_table_size as u8 {
                let result = table.get(FlexibleField::new(vec![index, 3, 4])).unwrap();
                assert_eq!(result.unwrap(), FlexibleField::new(vec![index * 2, 30, 40]));
            }
        }

        let table = SimpleTable::new(table_name, config.clone());
        for index in 0..10 * config.mem_table_size as u8 {
            let result = table.get(FlexibleField::new(vec![index, 3, 4])).unwrap();
            assert_eq!(result.unwrap(), FlexibleField::new(vec![index * 2, 30, 40]));
        }

        drop_dir(SimpleTable::table_path(table_name).as_path());
    }

    #[test]
    fn test_merge_sements() {
        prepare_dir();

        let table_name = "test_merge_sements";
        let config = TableConfig::default_config();

        let mut table = SimpleTable::new(table_name, config.clone());

        for index in 0..5 * config.mem_table_size as u8 {
            let entry = FlexibleEntry::new(
                FlexibleField::new(vec![index, 3, 4]),
                FlexibleField::new(vec![index * 2, 30, 40]),
            );
            table.put(entry).unwrap();
        }

        for index in 0..5 * config.mem_table_size as u8 {
            let result = table.get(FlexibleField::new(vec![index, 3, 4])).unwrap();
            assert_eq!(result.unwrap(), FlexibleField::new(vec![index * 2, 30, 40]));
        }

        drop_dir(SimpleTable::table_path(table_name).as_path());
    }

    #[test]
    fn test_merge_sements_max_level() {
        prepare_dir();

        let table_name = "test_merge_sements_max_level";
        let config = TableConfig::default_config();

        let mut table = SimpleTable::new(table_name, config.clone());

        for index in 0..64 * (config.mem_table_size - 1) as u8 {
            let entry = FlexibleEntry::new(
                FlexibleField::new(vec![index, 3, 4]),
                FlexibleField::new(vec![index, 30, 40]),
            );
            table.put(entry).unwrap();
        }

        for index in 0..64 * (config.mem_table_size - 1) as u8 {
            let result = table.get(FlexibleField::new(vec![index, 3, 4])).unwrap();
            assert_eq!(result.unwrap(), FlexibleField::new(vec![index, 30, 40]));
        }

        drop_dir(SimpleTable::table_path(table_name).as_path());
    }
}
