use crate::core::field::{Field, FieldSize};
use crate::core::marshal::{read_u32, write_data, write_u32};
use crate::errors::Result;
use crate::logicerr;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct UserEntry<K, V>(K, V);

impl<K, V> UserEntry<K, V>
where
    K: Field + FieldSize + PartialEq + Eq + PartialOrd,
    V: Field + FieldSize,
{
    pub fn new(key: K, value: V) -> Self {
        UserEntry { 0: key, 1: value }
    }

    pub fn from(buffer: &[u8]) -> Self {
        let mut offset = 0;
        let key_len = read_u32(buffer).unwrap() as usize;
        assert_ne!(key_len, 0);
        offset += size_of::<u32>() as usize;

        let value_len = read_u32(&buffer[offset..]).unwrap() as usize;
        assert_ne!(value_len, 0);
        offset += size_of::<u32>() as usize;

        let mut k: Vec<u8> = vec![0u8; key_len];
        write_data(&mut k, &buffer[offset..], key_len).unwrap();
        offset += key_len;

        let mut v = vec![0u8; value_len];
        write_data(&mut v, &buffer[offset..], value_len).unwrap();
        // offset += value_len;

        UserEntry {
            0: K::new(k),
            1: V::new(v),
        }
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

        assert_ne!(k_bytes, 0);
        assert_ne!(v_bytes, 0);

        let mut offset = 0usize;

        // write size of key
        offset += write_u32(&mut buffer[0..size_of::<u32>()], k_bytes)?;

        // write size of value
        offset += write_u32(&mut buffer[offset..offset + size_of::<u32>()], v_bytes)?;

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

        if offset == 0 {
            return logicerr!("Cann't be zero bytes after serialize entry");
        }

        Ok(offset as u64)
    }
}
