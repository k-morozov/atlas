use crate::core::row::Row;
use crate::core::pg_errors::PgError;

pub trait Table {
    fn write(row: Row) -> Result<(), PgError>;
    fn read() -> Result<Row, PgError>;
}
