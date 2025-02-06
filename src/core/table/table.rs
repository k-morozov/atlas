use crate::core::entry::Entry;
use crate::core::field::FixedField;
use crate::errors::Error;

pub trait Table {
    fn put(&mut self, entry: Entry) -> Result<(), Error>;
    fn get(&self, key: FixedField) -> Result<Option<FixedField>, Error>;
}
