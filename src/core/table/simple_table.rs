use std::fs::create_dir_all;
use std::path::Path;
use std::rc::Rc;

use super::table::Table;
use crate::core::entry::Entry;
use crate::core::field::{Field, FieldType};
use crate::core::mem_table::MemTable;
use crate::core::pg_errors::PgError;
use crate::core::schema::Schema;
use crate::core::segment::{
    segment_id::SegmentID, segment_reader::SegmentReader, segment_writer::SegmentWriter,
};
use crate::core::table::config::{DEFAULT_TABLES_PATH, DETAULT_MEM_TABLE_SIZE};

struct SimpleTable {
    table_path: String,
    mem_table: MemTable,
    schema: Rc<Schema>,
    segment_id: SegmentID,
    segments: Vec<String>,
}

impl SimpleTable {
    pub fn new() -> Self {
        let path = String::from(DEFAULT_TABLES_PATH.to_string() + "simple_table");

        create_dir_all(Path::new(&path)).unwrap();

        let schema = Rc::new(vec![
            Field::new(FieldType::Int32(0)),
            Field::new(FieldType::Int32(0)),
        ]);

        Self {
            table_path: path,
            mem_table: MemTable::new(DETAULT_MEM_TABLE_SIZE),
            segment_id: SegmentID::new(),
            segments: Vec::new(),
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
                let next_segment_id = self.segment_id.generate();
                let segment_path = format!("{table_path}/segment_{:07}.bin", next_segment_id);
                let mut writer =
                    SegmentWriter::new(Path::new(&segment_path), self.mem_table.iter());
                writer.write_entries()?;
                self.segments.push(segment_path);
            }

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
}
