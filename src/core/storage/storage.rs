use crate::core::entry::flexible_entry::FlexibleEntry;
use crate::core::field::FlexibleField;
use crate::errors::Error;

pub trait Storage {
    fn put(&mut self, entry: FlexibleEntry) -> Result<(), Error>;
    fn get(&self, key: &FlexibleField) -> Result<Option<FlexibleField>, Error>;
}
