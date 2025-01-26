use std::fs::{create_dir_all, read_dir, remove_file};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use super::table::Table;
use crate::core::entry::Entry;
use crate::core::field::{Field, FieldType};
use crate::core::mem_table::MemTable;
use crate::core::merge::merge::merge;
use crate::core::pg_errors::PgError;
use crate::core::schema::Schema;
use crate::core::segment::{
    segment::Segment,
    segment_reader::SegmentReader,
    segment_writer::SegmentWriter,
    table::{get_table_segments, TableSegments, SEGMENTS_MAX_LEVEL, SEGMENTS_MIN_LEVEL},
};
use crate::core::table::config::{
    DEFAULT_SEGMENTS_LIMIT, DEFAULT_TABLES_PATH, DETAULT_MEM_TABLE_SIZE,
};
use crate::core::table::metadata::TableMetadata;

fn create_dirs(table_path: &Path) -> Result<(), PgError> {
    create_dir_all(Path::new(table_path)).map_err(|_| PgError::FailedCreateTableDirs)?;
    let segment_dir = format!("{}/segment", table_path.to_str().unwrap());
    create_dir_all(Path::new(&segment_dir)).map_err(|_| PgError::FailedCreateTableDirs)?;

    Ok(())
}

struct SimpleTable {
    table_name: String,
    table_path: PathBuf,

    mem_table: MemTable,

    // @todo refuse to use
    schema: Rc<Schema>,
    metadata: TableMetadata,

    // @todo possibly add to metadata
    segments: TableSegments,
}

impl SimpleTable {
    pub fn new(table_name: &str) -> Self {
        let table_path = SimpleTable::table_path(table_name);

        if let Err(_) = create_dirs(table_path.as_path()) {
            panic!("Faield create table dirs")
        }

        let segments = get_table_segments(table_path.as_path());
        if let Err(_) = segments {
            panic!("Faield read segments")
        }

        let segments = segments.unwrap();

        let schema = Rc::new(vec![
            Field::new(FieldType::Int32(0)),
            Field::new(FieldType::Int32(0)),
        ]);

        let metadata =
            TableMetadata::from_file(TableMetadata::make_path(table_path.as_path()).as_path());

        Self {
            mem_table: MemTable::new(DETAULT_MEM_TABLE_SIZE),
            table_name: table_name.to_string(),
            table_path: table_path.to_path_buf(),
            metadata,
            segments,
            schema,
        }
    }

    pub fn table_path(table_name: &str) -> PathBuf {
        PathBuf::from(&String::from(DEFAULT_TABLES_PATH.to_string() + table_name))
    }
}

impl Table for SimpleTable {
    fn put(&mut self, entry: Entry) -> Result<(), PgError> {
        self.mem_table.append(entry);

        if self.mem_table.current_size() == self.mem_table.max_table_size() {
            {
                match Segment::create(
                    self.table_path.as_path(),
                    &mut self.metadata.segment_id,
                    &mut self.mem_table,
                ) {
                    Ok(sg) => {
                        self.segments
                            .entry(SEGMENTS_MIN_LEVEL)
                            .or_insert_with(Vec::new)
                            .push(sg);

                        self.mem_table = MemTable::new(DETAULT_MEM_TABLE_SIZE);

                        if self.segments[&SEGMENTS_MIN_LEVEL].len() == DEFAULT_SEGMENTS_LIMIT {
                            for merged_level in SEGMENTS_MIN_LEVEL..=SEGMENTS_MAX_LEVEL {
                                let level_for_new_sg = if merged_level != SEGMENTS_MAX_LEVEL {
                                    merged_level + 1
                                } else {
                                    merged_level
                                };
                                match Segment::for_merge(
                                    self.table_path.as_path(),
                                    &mut self.metadata.segment_id,
                                    level_for_new_sg,
                                ) {
                                    Ok(mut merged_sg) => {
                                        merge(&mut merged_sg, &self.segments[&merged_level]);
                                        self.segments.get_mut(&merged_level).unwrap().clear();
                                        self.segments
                                            .entry(level_for_new_sg)
                                            .or_insert_with(Vec::new)
                                            .push(merged_sg);
                                    }
                                    Err(_) => panic!("Failed merge"),
                                }
                            }
                        }
                    }
                    Err(_er) => {
                        panic!("Failed to put the segment")
                    }
                }
            }
            self.metadata
                .sync_disk(Path::new(self.metadata.get_metadata_path()));
            self.mem_table.clear();
        }

        Ok(())
    }

    fn get(&self, key: Field) -> Result<Option<Field>, PgError> {
        if let Some(value) = self.mem_table.get_value(&key) {
            return Ok(Some(value));
        }

        for (_level, segments) in &self.segments {
            for segment in segments {
                let reader = SegmentReader::new(
                    Segment::get_path(self.table_path.as_path(), segment.get_name()).as_path(),
                    self.schema.clone(),
                );
                if let Some(r) = reader.read(&key)? {
                    return Ok(Some(r));
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
    use crate::core::entry::*;
    use crate::core::field::*;

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
        let mut table = SimpleTable::new(table_name);

        for index in 0..=DETAULT_MEM_TABLE_SIZE {
            let entry = Entry::new(
                Field::new(FieldType::Int32(index as i32)),
                Field::new(FieldType::Int32((index as i32) * 10)),
            );
            table.put(entry).unwrap();
        }

        for index in 0..=DETAULT_MEM_TABLE_SIZE {
            let result = table
                .get(Field::new(FieldType::Int32(index as i32)))
                .unwrap();
            assert_eq!(
                result.unwrap(),
                Field::new(FieldType::Int32((index as i32) * 10))
            );
        }

        drop_dir(SimpleTable::table_path(table_name).as_path());
    }

    #[test]
    fn test_some_segments() {
        prepare_dir();

        let table_name = "test_table_some_segments";
        let mut table = SimpleTable::new(table_name);

        for index in 0..3 * DETAULT_MEM_TABLE_SIZE {
            let entry = Entry::new(
                Field::new(FieldType::Int32(index as i32)),
                Field::new(FieldType::Int32((index as i32) * 10)),
            );
            table.put(entry).unwrap();
        }

        for index in 0..3 * DETAULT_MEM_TABLE_SIZE {
            let result = table
                .get(Field::new(FieldType::Int32(index as i32)))
                .unwrap();
            assert_eq!(
                result.unwrap(),
                Field::new(FieldType::Int32((index as i32) * 10))
            );
        }

        drop_dir(SimpleTable::table_path(table_name).as_path());
    }

    #[test]
    fn test_some_segments_with_restart() {
        prepare_dir();

        let table_name = "test_table_some_segments_with_restart";

        {
            let mut table = SimpleTable::new(table_name);

            for index in 0..10 * DETAULT_MEM_TABLE_SIZE {
                let entry = Entry::new(
                    Field::new(FieldType::Int32(index as i32)),
                    Field::new(FieldType::Int32((index as i32) * 10)),
                );
                table.put(entry).unwrap();
            }

            for index in 0..10 * DETAULT_MEM_TABLE_SIZE {
                let result = table
                    .get(Field::new(FieldType::Int32(index as i32)))
                    .unwrap();
                assert_eq!(
                    result.unwrap(),
                    Field::new(FieldType::Int32((index as i32) * 10))
                );
            }
        }

        let table = SimpleTable::new(table_name);
        for index in 0..10 * DETAULT_MEM_TABLE_SIZE {
            let result = table
                .get(Field::new(FieldType::Int32(index as i32)))
                .unwrap();
            assert_eq!(
                result.unwrap(),
                Field::new(FieldType::Int32((index as i32) * 10))
            );
        }

        drop_dir(SimpleTable::table_path(table_name).as_path());
    }

    #[test]
    fn test_merge_sements() {
        prepare_dir();

        let table_name = "test_merge_sements";
        let mut table = SimpleTable::new(table_name);

        for index in 0..5 * DETAULT_MEM_TABLE_SIZE {
            let entry = Entry::new(
                Field::new(FieldType::Int32(index as i32)),
                Field::new(FieldType::Int32((index as i32) * 10)),
            );
            table.put(entry).unwrap();
        }

        for index in 0..5 * DETAULT_MEM_TABLE_SIZE {
            let result = table
                .get(Field::new(FieldType::Int32(index as i32)))
                .unwrap();
            assert_eq!(
                result.unwrap(),
                Field::new(FieldType::Int32((index as i32) * 10))
            );
        }

        // drop_dir(SimpleTable::table_path(table_name).as_path());
    }

    #[test]
    fn test_merge_sements_max_level() {
        prepare_dir();

        let table_name = "test_merge_sements_max_level";
        let mut table = SimpleTable::new(table_name);

        for index in 0..64 * DETAULT_MEM_TABLE_SIZE {
            let entry = Entry::new(
                Field::new(FieldType::Int32(index as i32)),
                Field::new(FieldType::Int32((index as i32) * 10)),
            );
            table.put(entry).unwrap();
        }

        for index in 0..64 * DETAULT_MEM_TABLE_SIZE {
            let result = table
                .get(Field::new(FieldType::Int32(index as i32)))
                .unwrap();
            assert_eq!(
                result.unwrap(),
                Field::new(FieldType::Int32((index as i32) * 10))
            );
        }

        drop_dir(SimpleTable::table_path(table_name).as_path());
    }
}
