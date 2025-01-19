use std::cmp::{Eq, Ord, PartialEq, PartialOrd};
use std::mem::MaybeUninit;

use crate::core::field::{Field, FieldType};
use crate::core::marshal::Marshal;
use crate::core::pg_errors::PgError;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct Entry {
    key: Field,
    value: Field,
}

impl Entry {
    pub fn new(key: Field, value: Field) -> Self {
        Entry { key, value }
    }

    pub fn get_key(&self) -> &Field {
        &self.key
    }

    pub fn get_value(&self) -> &Field {
        &self.value
    }

    pub fn size(&self) -> usize {
        self.key.size() + self.value.size()
    }
}

impl Marshal for Entry {
    fn serialize(&self, dst: &mut [MaybeUninit<u8>]) -> Result<(), PgError> {
        let mut offset = 0;

        self.key
            .serialize(&mut dst[offset..offset + self.key.size()])?;
        offset += self.key.size();

        self.value
            .serialize(&mut dst[offset..offset + self.value.size()])?;
        // offset += self.value.size();

        Ok(())
    }
    fn deserialize(&mut self, src: &[u8]) -> Result<(), PgError> {
        let mut offset = 0;

        self.key
            .deserialize(&src[offset..offset + self.key.size()])?;
        offset += self.key.size();

        self.value
            .deserialize(&src[offset..offset + self.value.size()])?;
        // offset += self.value.size();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::core::entry::*;
    use crate::core::field::*;

    #[test]
    fn test_get_value() {
        let row = Entry::new(
            Field::new(FieldType::Int32(3)),
            Field::new(FieldType::Int32(30)),
        );

        assert_eq!(*row.get_value(), Field::new(FieldType::Int32(30)));
    }

    #[test]
    fn serialization() {
        let entry = Entry::new(
            Field::new(FieldType::Int32(3)),
            Field::new(FieldType::Int32(33)),
        );

        let mut entry_buffer_raw = vec![MaybeUninit::<u8>::uninit(); entry.size()];
        let result = entry.serialize(&mut entry_buffer_raw);
        assert!(result.is_ok());

        let mut entry_out = Entry::new(
            Field::new(FieldType::Int32(0)),
            Field::new(FieldType::Int32(0)),
        );

        let row_buffer_initialized = entry_buffer_raw
            .iter()
            .map(|entry| unsafe { entry.assume_init() })
            .collect::<Vec<u8>>();

        let result = entry_out.deserialize(&row_buffer_initialized);
        assert!(result.is_ok());

        assert_eq!(*entry_out.get_key(), Field::new(FieldType::Int32(3)));
        assert_eq!(*entry_out.get_value(), Field::new(FieldType::Int32(33)));

        assert_eq!(entry_out.size(), 2 * size_of::<i32>());
    }
}
