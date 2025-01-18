use super::table::Table;
use crate::core::mem_table::MemTable;
use crate::core::pg_errors::PgError;
use crate::core::row::Row;

struct SimpleTable {
    mem_table: MemTable,
}

impl SimpleTable {
    pub fn new() -> Self {
        Self {
            mem_table: MemTable::new(4, 4),
        }
    }
}

impl Table for SimpleTable {
    fn write(&mut self, row: Row) -> Result<(), PgError> {
        self.mem_table.append(row);

        // flush

        Ok(())
    }
    fn read(&self) -> Result<Row, PgError> {
        unreachable!()
    }
}
