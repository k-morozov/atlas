use crate::core::entry::Entry;
use crate::core::pg_errors::PgError;

pub trait Table {
    fn write(&mut self, row: Entry) -> Result<(), PgError>;
    fn read(&self) -> Result<Entry, PgError>;
}
