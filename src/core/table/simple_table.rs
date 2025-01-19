use std::fs::create_dir_all;
use std::path::Path;

use super::table::Table;
use crate::core::entry::Entry;
use crate::core::mem_table::MemTable;
use crate::core::pg_errors::PgError;
use crate::core::segment::segment_writer::SegmentWriter;
use crate::core::table::config::DEFAULT_TABLES_PATH;

struct SimpleTable {
    table_path: String,
    mem_table: MemTable,
}

impl SimpleTable {
    pub fn new() -> Self {
        let path = String::from(DEFAULT_TABLES_PATH.to_string() + "simple_table");

        create_dir_all(Path::new(&path)).unwrap();

        Self {
            table_path: path,
            mem_table: MemTable::new(4),
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

    fn read(&self) -> Result<Entry, PgError> {
        unreachable!()
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

        for index in 1..5 {
            let entry = Entry::new(
                Field::new(FieldType::Int32(index)),
                Field::new(FieldType::Int32(index * 10)),
            );
            table.write(entry).unwrap();
        }
    }
}
