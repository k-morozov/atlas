use std::fs::create_dir_all;
use std::path::Path;

use super::table::Table;
use crate::core::mem_table::MemTable;
use crate::core::pg_errors::PgError;
use crate::core::row::Row;
use crate::core::segment::segment_writer::SegmentWriter;
use crate::core::table::config::DEFAULT_TABLES_PATH;

struct SimpleTable {
    table_path: String,
    mem_table: MemTable,
}

impl SimpleTable {
    pub fn new() -> Self {
        let path = String::from(DEFAULT_TABLES_PATH.to_string() + "simple_table/");

        create_dir_all(Path::new(&path)).unwrap();

        Self {
            table_path: path,
            mem_table: MemTable::new(4, 4),
        }
    }
}

impl Table for SimpleTable {
    fn write(&mut self, row: Row) -> Result<(), PgError> {
        self.mem_table.append(row);

        if self.mem_table.current_size() == self.mem_table.max_table_size() {
            {
                let segment_path = self.table_path.clone() + "segment1.bin";
                let mut writer =
                    SegmentWriter::new(Path::new(&segment_path), self.mem_table.iter());
                writer.write_rows()?;
            }

            self.mem_table.clear();
        }

        Ok(())
    }

    fn read(&self) -> Result<Row, PgError> {
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::field::*;
    use crate::core::row::*;

    #[test]
    fn simple_check() {
        let mut table = SimpleTable::new();

        // let mut rows: Vec<Row> = Vec::new();

        for index in 1..5 {
            let row = RowBuilder::new(3)
                .add_field(Field::new(FieldType::Int32(12 + index)))
                .add_field(Field::new(FieldType::Int32(100 + index)))
                .build()
                .unwrap();
            table.write(row).unwrap();
        }
    }
}
