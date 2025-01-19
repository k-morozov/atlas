use crate::core::pg_errors::PgError;
use crate::core::row::Row;

pub trait Table {
    fn write(&mut self, row: Row) -> Result<(), PgError>;
    fn read(&self) -> Result<Row, PgError>;
}
