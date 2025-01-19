use std::fs::create_dir_all;
use std::path::Path;
use std::rc::Rc;

use super::table::Table;
use crate::core::entry::Entry;
use crate::core::field::{Field, FieldType};
use crate::core::mem_table::MemTable;
use crate::core::pg_errors::PgError;
use crate::core::schema::Schema;
use crate::core::segment::segment_reader::SegmentReader;
use crate::core::segment::segment_writer::SegmentWriter;
use crate::core::table::config::{DEFAULT_TABLES_PATH, DETAULT_MEM_TABLE_SIZE};

struct SimpleTable {
    table_path: String,
    mem_table: MemTable,
    schema: Rc<Schema>,
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
                let segment_path = format!("{table_path}/segment1.bin");
                let mut writer =
                    SegmentWriter::new(Path::new(&segment_path), self.mem_table.iter());
                writer.write_entries()?;
            }

            self.mem_table.clear();
        }

        Ok(())
    }

    fn read(&self, key: Field) -> Result<Option<Field>, PgError> {
        if let Some(value) = self.mem_table.get_value(&key) {
            return Ok(Some(value));
        }
        let table_path = &self.table_path;
        let segment_path = format!("{table_path}/segment1.bin");
        let reader = SegmentReader::new(Path::new(&segment_path), self.schema.clone());

        reader.read(&key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::entry::*;
    use crate::core::field::*;

    #[test]
    fn simple_check() {
        let mut table = SimpleTable::new();

        for index in 0..=DETAULT_MEM_TABLE_SIZE {
            let entry = Entry::new(
                Field::new(FieldType::Int32(index as i32)),
                Field::new(FieldType::Int32((index as i32) * 10)),
            );
            table.write(entry).unwrap();
        }

        for index in 0..=DETAULT_MEM_TABLE_SIZE {
            let result = table.read(Field::new(FieldType::Int32(index as i32))).unwrap();
            assert!(result.is_some(), "index={}", index);
            assert_eq!(result.unwrap(), Field::new(FieldType::Int32((index as i32) * 10)));
        }
    }
}
