use std::cmp::{Eq, Ord, PartialEq, PartialOrd};
use std::mem::MaybeUninit;
use std::ptr::copy;

use crate::core::marshal::Marshal;
use crate::errors::{Error, Result};

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub enum FieldType {
    Int32(i32),
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct FixedField {
    pub value: FieldType,
}

impl FixedField {
    pub fn new(value: FieldType) -> Self {
        FixedField { value }
    }
}

impl FieldSize for FixedField {
    fn size(&self) -> usize {
        match self.value {
            FieldType::Int32(_) => size_of::<i32>(),
        }
    }
}

impl Marshal for FixedField {
    fn serialize(&self, dst: &mut [MaybeUninit<u8>]) -> Result<()> {
        match &self.value {
            FieldType::Int32(number) => {
                if size_of::<i32>() != dst.len() {
                    return Err(Error::InvalidData("failed dst size".to_string()));
                }

                unsafe {
                    copy(
                        number as *const i32 as *const u8,
                        dst.as_mut_ptr() as *mut u8,
                        size_of::<i32>(),
                    );
                }
            }
        }
        Ok(())
    }

    fn deserialize(&mut self, src: &[u8]) -> Result<()> {
        match &mut self.value {
            FieldType::Int32(dst) => {
                if size_of::<i32>() != src.len() {
                    return Err(Error::InvalidData("failed dst size".to_string()));
                }
                unsafe {
                    copy(src.as_ptr(), dst as *mut i32 as *mut u8, size_of::<i32>());
                }
            }
        }
        Ok(())
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct FlexibleField {
    pub data: Vec<u8>,
}

impl FlexibleField {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

impl FieldSize for FlexibleField {
    fn size(&self) -> usize {
        self.len() * size_of::<u8>()
    }
}


pub trait FieldSize {
    fn size(&self) -> usize;
}

#[cfg(test)]
mod test {
    use std::mem::MaybeUninit;

    use crate::core::field::*;

    #[test]
    fn simple_serialize() {
        let field = FixedField::new(FieldType::Int32(432));

        let mut dst = vec![MaybeUninit::uninit(); size_of::<i32>()];

        let r = field.serialize(&mut dst);
        assert!(r.is_ok());

        assert_eq!(
            dst.iter()
                .map(|entry| unsafe { entry.assume_init() })
                .collect::<Vec<u8>>(),
            &[176, 1, 0, 0]
        );
    }

    #[test]
    fn simple_deserialize() {
        let mut field = FixedField::new(FieldType::Int32(0));
        let src = &[176, 1, 0, 0];

        let r = field.deserialize(src);
        assert!(r.is_ok());

        assert_eq!(field.value, FieldType::Int32(432));
    }

    #[test]
    fn failed_serialize() {
        let field: FixedField = FixedField::new(FieldType::Int32(432));

        let mut dst = vec![MaybeUninit::uninit(); 1];

        let r = field.serialize(&mut dst);
        assert!(r.is_err());
        assert_eq!(r, Err(Error::InvalidData("failed dst size".to_string())));
    }

    #[test]
    fn failed_deserialize() {
        let mut field = FixedField::new(FieldType::Int32(0));
        let src = &[176, 1, 0, 0, 13];

        let r = field.deserialize(src);
        assert_eq!(r, Err(Error::InvalidData("failed dst size".to_string())));
    }
}
