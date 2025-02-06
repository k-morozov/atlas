use std::cmp::{Eq, Ord, PartialEq, PartialOrd};
use std::mem::MaybeUninit;

use crate::core::field::{FixedField, FlexibleField};
use crate::core::marshal::Marshal;
use crate::errors::Error;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct Entry {
    pub key: FixedField,
    pub value: FixedField,
}

impl Entry {
    pub fn new(key: FixedField, value: FixedField) -> Self {
        Entry { key, value }
    }

    pub fn get_key(&self) -> &FixedField {
        &self.key
    }

    pub fn get_value(&self) -> &FixedField {
        &self.value
    }

    pub fn size(&self) -> usize {
        self.key.size() + self.value.size()
    }
}

impl Marshal for Entry {
    fn serialize(&self, dst: &mut [MaybeUninit<u8>]) -> Result<(), Error> {
        let mut offset = 0;

        self.key
            .serialize(&mut dst[offset..offset + self.key.size()])?;
        offset += self.key.size();

        self.value
            .serialize(&mut dst[offset..offset + self.value.size()])?;
        // offset += self.value.size();

        Ok(())
    }
    fn deserialize(&mut self, src: &[u8]) -> Result<(), Error> {
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

pub struct FlexibleEntry {
    pub key: FlexibleField,
    pub value: FlexibleField,
}

impl FlexibleEntry {
    pub fn new(key: FlexibleField, value: FlexibleField) -> Self {
        FlexibleEntry { key, value }
    }

    pub fn get_key(&self) -> &FlexibleField {
        &self.key
    }

    pub fn get_value(&self) -> &FlexibleField {
        &self.value
    }

    pub fn len(&self) -> usize {
        self.key.len() + self.value.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::core::entry::*;
    use crate::core::field::*;

    #[test]
    fn test_get_value() {
        let row = Entry::new(
            FixedField::new(FieldType::Int32(3)),
            FixedField::new(FieldType::Int32(30)),
        );

        assert_eq!(*row.get_value(), FixedField::new(FieldType::Int32(30)));
    }

    #[test]
    fn serialization() {
        let entry = Entry::new(
            FixedField::new(FieldType::Int32(3)),
            FixedField::new(FieldType::Int32(33)),
        );

        let mut entry_buffer_raw = vec![MaybeUninit::<u8>::uninit(); entry.size()];
        let result = entry.serialize(&mut entry_buffer_raw);
        assert!(result.is_ok());

        let mut entry_out = Entry::new(
            FixedField::new(FieldType::Int32(0)),
            FixedField::new(FieldType::Int32(0)),
        );

        let row_buffer_initialized = entry_buffer_raw
            .iter()
            .map(|entry| unsafe { entry.assume_init() })
            .collect::<Vec<u8>>();

        let result = entry_out.deserialize(&row_buffer_initialized);
        assert!(result.is_ok());

        assert_eq!(*entry_out.get_key(), FixedField::new(FieldType::Int32(3)));
        assert_eq!(
            *entry_out.get_value(),
            FixedField::new(FieldType::Int32(33))
        );

        assert_eq!(entry_out.size(), 2 * size_of::<i32>());
    }
}
