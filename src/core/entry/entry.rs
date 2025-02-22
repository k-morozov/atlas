use crate::core::field::{Field, FieldSize};
use crate::core::marshal::{write_data, write_u32};
use crate::errors::Result;

// @todo to block
pub const ENTRY_METADATA_OFFSET: u32 = 2 * size_of::<u32>() as u32;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct Entry<K, V>(K, V);

impl<K, V> Entry<K, V>
where
    K: Field + FieldSize,
    V: Field + FieldSize,
{
    pub fn new(key: K, value: V) -> Self {
        Entry { 0: key, 1: value }
    }

    pub fn get_key(&self) -> &K {
        &self.0
    }

    pub fn get_value(&self) -> &V {
        &self.1
    }

    pub fn size(&self) -> usize {
        self.0.size() + self.1.size()
    }

    pub fn serialize_to(&self, buffer: &mut [u8]) -> Result<u64> {
        let k_bytes = self.get_key().size() as u32;
        let v_bytes = self.get_value().size() as u32;

        let mut offset = 0usize;

        // write size of key
        offset += write_u32(&mut buffer[0..size_of::<u32>()], k_bytes)?;

        // write size of value
        offset += write_u32(&mut buffer[offset..offset + size_of::<u32>()], v_bytes)?;
        assert_eq!(offset as u32, ENTRY_METADATA_OFFSET);

        // write key
        offset += write_data(
            &mut buffer[offset..offset + k_bytes as usize],
            &mut self.get_key().data(),
            k_bytes as usize,
        )?;

        // write value
        offset += write_data(
            &mut buffer[offset..offset + v_bytes as usize],
            &mut self.get_value().data(),
            v_bytes as usize,
        )?;

        Ok(offset as u64)
    }
}
