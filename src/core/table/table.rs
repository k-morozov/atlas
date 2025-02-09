use crate::core::entry::fixed_entry::FixedEntry;
use crate::core::field::FixedField;
use crate::errors::Error;

pub trait Table {
    fn put(&mut self, entry: FixedEntry) -> Result<(), Error>;
    fn get(&self, key: FixedField) -> Result<Option<FixedField>, Error>;
}
