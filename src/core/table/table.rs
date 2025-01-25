use crate::core::entry::Entry;
use crate::core::field::Field;
use crate::core::pg_errors::PgError;

pub trait Table {
    fn put(&mut self, entry: Entry) -> Result<(), PgError>;
    fn get(&self, key: Field) -> Result<Option<Field>, PgError>;
}
