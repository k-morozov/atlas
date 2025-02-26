use crate::core::entry::user_entry;
use crate::core::field::FlexibleField;
use crate::core::marshal::{read_u32, write_data};

pub type FlexibleUserEntry = user_entry::UserEntry<FlexibleField, FlexibleField>;

impl FlexibleUserEntry {
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

        FlexibleUserEntry::new(FlexibleField::new(k), FlexibleField::new(v))
    }
}
