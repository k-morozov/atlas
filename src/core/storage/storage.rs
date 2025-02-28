use crate::core::entry::flexible_user_entry::FlexibleUserEntry;
use crate::core::field::FlexibleField;
use crate::errors::Error;

pub trait Storage {
    fn put(&self, entry: FlexibleUserEntry) -> Result<(), Error>;
    fn get(&self, key: &FlexibleField) -> Result<Option<FlexibleField>, Error>;
}
