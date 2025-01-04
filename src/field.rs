use crate::marshal::Marshal;
use crate::pg_errors::PgError;
use std::cmp::{Eq, Ord, PartialEq, PartialOrd};
use std::ptr::copy;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub enum FieldType {
    Int32(i32),
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct Field {
    pub field: FieldType,
}

impl Field {
    pub fn new(field: FieldType) -> Self {
        Field { field }
    }

    pub fn size(&self) -> usize {
        match self.field {
            FieldType::Int32(_) => size_of::<i32>(),
        }
    }
}

impl Marshal for Field {
    fn serialize(&self, dst: &mut [u8]) -> Result<(), PgError> {
        match &self.field {
            FieldType::Int32(number) => {
                if size_of::<i32>() != dst.len() {
                    return Err(PgError::MarshalFailedSerialization);
                }
                unsafe {
                    copy(
                        number as *const i32 as *const u8,
                        dst.as_mut_ptr(),
                        size_of::<i32>(),
                    );
                }
            }
        }
        Ok(())
    }
    fn deserialize(&mut self, src: &[u8]) -> Result<(), PgError> {
        match &mut self.field {
            FieldType::Int32(dst) => {
                if size_of::<i32>() != src.len() {
                    return Err(PgError::MarshalFailedSerialization);
                }
                unsafe {
                    copy(src.as_ptr(), dst as *mut i32 as *mut u8, size_of::<i32>());
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::field::*;

    #[test]
    fn simple_serialize() {
        let field = Field::new(FieldType::Int32(432));
        let mut dst = Vec::<u8>::new();
        dst.resize(size_of::<i32>(), 0);

        let r = field.serialize(&mut dst[0..]);
        assert!(r.is_ok());

        assert_eq!(dst, &[176, 1, 0, 0]);
    }

    #[test]
    fn simple_deserialize() {
        let mut field = Field::new(FieldType::Int32(0));
        let src = &[176, 1, 0, 0];

        let r = field.deserialize(src);
        assert!(r.is_ok());

        assert_eq!(field.field, FieldType::Int32(432));
    }

    #[test]
    fn failed_serialize() {
        let field: Field = Field::new(FieldType::Int32(432));
        let mut dst = Vec::<u8>::new();
        dst.resize(1, 0);

        let r = field.serialize(&mut dst[0..]);
        assert!(r.is_err());
        assert_eq!(r, Err(PgError::MarshalFailedSerialization));
    }

    #[test]
    fn failed_deserialize() {
        let mut field = Field::new(FieldType::Int32(0));
        let src = &[176, 1, 0, 0, 13];

        let r = field.deserialize(src);
        assert!(r.is_err());
        assert_eq!(r, Err(PgError::MarshalFailedSerialization));
    }
}
