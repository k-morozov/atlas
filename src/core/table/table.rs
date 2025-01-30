use crate::core::entry::Entry;
use crate::core::field::Field;
use crate::errors::Error;

pub trait Table {
    fn put(&mut self, entry: Entry) -> Result<(), Error>;
    fn get(&self, key: Field) -> Result<Option<Field>, Error>;
}
