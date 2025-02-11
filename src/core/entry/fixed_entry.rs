use std::mem::MaybeUninit;

use crate::core::entry::entry;
use crate::core::field::{FieldSize, FixedField};
use crate::core::marshal::Marshal;
use crate::errors::Error;

pub type FixedEntry = entry::Entry<FixedField, FixedField>;

impl Marshal for FixedEntry {
    fn serialize(&self, dst: &mut [MaybeUninit<u8>]) -> Result<(), Error> {
        let mut offset = 0;

        self.get_key()
            .serialize(&mut dst[offset..offset + self.get_key().size()])?;
        offset += self.get_key().size();

        self.get_value()
            .serialize(&mut dst[offset..offset + self.get_value().size()])?;
        // offset += self.value.size();

        Ok(())
    }

    fn deserialize(&mut self, src: &[u8]) -> Result<(), Error> {
        let mut offset = 0;

        let field_size = self.get_key().size();
        self.get_mut_key()
            .deserialize(&src[offset..offset + field_size])?;
        offset += self.get_key().size();

        let field_size = self.get_value().size();
        self.get_mut_value()
            .deserialize(&src[offset..offset + field_size])?;
        // offset += self.value.size();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::core::entry::fixed_entry::*;
    use crate::core::field::*;

    #[test]
    fn test_get_value() {
        let row = FixedEntry::new(
            FixedField::new(FieldType::Int32(3)),
            FixedField::new(FieldType::Int32(30)),
        );

        assert_eq!(*row.get_value(), FixedField::new(FieldType::Int32(30)));
    }

    #[test]
    fn serialization() {
        let entry = FixedEntry::new(
            FixedField::new(FieldType::Int32(3)),
            FixedField::new(FieldType::Int32(33)),
        );

        let mut entry_buffer_raw = vec![MaybeUninit::<u8>::uninit(); entry.size()];
        let result = entry.serialize(&mut entry_buffer_raw);
        assert!(result.is_ok());

        let mut entry_out = FixedEntry::new(
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
