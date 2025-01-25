use std::fs::{create_dir_all, read_dir};
use std::path::Path;
use std::rc::Rc;

use super::table::Table;
use crate::core::entry::Entry;
use crate::core::field::{Field, FieldType};
use crate::core::mem_table::MemTable;
use crate::core::pg_errors::PgError;
use crate::core::schema::Schema;
use crate::core::segment::{segment_reader::SegmentReader, segment_writer::SegmentWriter};
use crate::core::table::config::{DEFAULT_TABLES_PATH, DETAULT_MEM_TABLE_SIZE};
use crate::core::table::metadata;

struct SimpleTable {
    table_path: String,
    mem_table: MemTable,
    schema: Rc<Schema>,
    metadata: metadata::TableMetadata,
    segments: Vec<String>,
}

fn create_dirs(table_path: &str) -> Result<(), PgError> {
    create_dir_all(Path::new(table_path)).map_err(|_| PgError::FailedCreateTableDirs)?;
    let segment_dir = format!("{table_path}/segment");
    create_dir_all(Path::new(&segment_dir)).map_err(|_| PgError::FailedCreateTableDirs)?;

    Ok(())
}

fn get_segment_names(table_path: &str) -> Result<Vec<String>, PgError> {
    let segment_dir = format!("{table_path}/segment");

    let segment_names = read_dir(segment_dir)
        .map_err(|_| PgError::FailedReadSegmentNames)?
        .map(|entry| {
            match entry {
                Ok(entry) => {
                    return entry.path().to_str().unwrap().to_string();
                }
                Err(er) => {
                    panic!("get_segment_names failed with error={}", er);
                },
            };
        })
        .collect::<Vec<String>>();
    
    Ok(segment_names)
}

impl SimpleTable {
    pub fn new() -> Self {
        let table_path = String::from(DEFAULT_TABLES_PATH.to_string() + "simple_table");
        if let Err(_) = create_dirs(&table_path) {
            panic!("Faield create table dirs")
        }

        let segments = get_segment_names(&table_path);
        if let Err(_) = segments {
            panic!("Faield read segments")
        }

        let segments = segments.unwrap();

        let schema = Rc::new(vec![
            Field::new(FieldType::Int32(0)),
            Field::new(FieldType::Int32(0)),
        ]);

        let metadata_path = format!("{}{}", table_path, "/metadata");
        let metadata = metadata::TableMetadata::from_file(Path::new(&metadata_path));

        Self {
            mem_table: MemTable::new(DETAULT_MEM_TABLE_SIZE),
            segments,
            table_path,
            metadata,
            schema,
        }
    }
}

impl Table for SimpleTable {
    fn write(&mut self, entry: Entry) -> Result<(), PgError> {
        self.mem_table.append(entry);

        if self.mem_table.current_size() == self.mem_table.max_table_size() {
            {
                let table_path = &self.table_path;
                let next_segment_id = self.metadata.segment_id.get_and_next();
                let segment_path =
                    format!("{table_path}/segment/segment_{:07}.bin", next_segment_id);
                let mut writer =
                    SegmentWriter::new(Path::new(&segment_path), self.mem_table.iter());
                writer.write_entries()?;
                self.segments.push(segment_path);
            }
            let metadata_path = format!("{}{}", self.table_path, "/metadata");
            self.metadata.sync_disk(Path::new(&metadata_path));
            self.mem_table.clear();
        }

        Ok(())
    }

    fn read(&self, key: Field) -> Result<Option<Field>, PgError> {
        if let Some(value) = self.mem_table.get_value(&key) {
            return Ok(Some(value));
        }

        for segment_name in &self.segments {
            // let segment_path = format!("{table_path}/{segment_name}");
            let reader = SegmentReader::new(Path::new(&segment_name), self.schema.clone());
            if let Some(r) = reader.read(&key)? {
                return Ok(Some(r));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::remove_file;
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

    #[test]
    fn test_segment() {
        prepare_dir();

        let mut table = SimpleTable::new();

        for index in 0..=DETAULT_MEM_TABLE_SIZE {
            let entry = Entry::new(
                Field::new(FieldType::Int32(index as i32)),
                Field::new(FieldType::Int32((index as i32) * 10)),
            );
            table.write(entry).unwrap();
        }

        for index in 0..=DETAULT_MEM_TABLE_SIZE {
            let result = table
                .read(Field::new(FieldType::Int32(index as i32)))
                .unwrap();
            assert_eq!(
                result.unwrap(),
                Field::new(FieldType::Int32((index as i32) * 10))
            );
        }
    }

    #[test]
    fn test_some_segments() {
        prepare_dir();

        let mut table = SimpleTable::new();

        for index in 0..3 * DETAULT_MEM_TABLE_SIZE {
            let entry = Entry::new(
                Field::new(FieldType::Int32(index as i32)),
                Field::new(FieldType::Int32((index as i32) * 10)),
            );
            table.write(entry).unwrap();
        }

        for index in 0..3 * DETAULT_MEM_TABLE_SIZE {
            let result = table
                .read(Field::new(FieldType::Int32(index as i32)))
                .unwrap();
            assert_eq!(
                result.unwrap(),
                Field::new(FieldType::Int32((index as i32) * 10))
            );
        }
    }

    #[test]
    fn test_some_segments_with_restart() {
        prepare_dir();

        {
            let mut table = SimpleTable::new();

            for index in 0..10 * DETAULT_MEM_TABLE_SIZE {
                let entry = Entry::new(
                    Field::new(FieldType::Int32(index as i32)),
                    Field::new(FieldType::Int32((index as i32) * 10)),
                );
                table.write(entry).unwrap();
            }

            for index in 0..10 * DETAULT_MEM_TABLE_SIZE {
                let result = table
                    .read(Field::new(FieldType::Int32(index as i32)))
                    .unwrap();
                assert_eq!(
                    result.unwrap(),
                    Field::new(FieldType::Int32((index as i32) * 10))
                );
            }
        }

        let table = SimpleTable::new();
        for index in 0..10 * DETAULT_MEM_TABLE_SIZE {
            let result = table
                .read(Field::new(FieldType::Int32(index as i32)))
                .unwrap();
            assert_eq!(
                result.unwrap(),
                Field::new(FieldType::Int32((index as i32) * 10))
            );
        }
    }
}
