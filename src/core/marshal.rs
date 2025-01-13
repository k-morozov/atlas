use std::mem::MaybeUninit;

use crate::core::pg_errors::PgError;

pub trait Marshal {
    fn serialize(&self, dst: &mut [MaybeUninit<u8>]) -> Result<(), PgError>;
    fn deserialize(&mut self, src: &[u8]) -> Result<(), PgError>;
}
