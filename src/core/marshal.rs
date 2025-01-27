use std::mem::MaybeUninit;

use crate::errors::Error;

pub trait Marshal {
    fn serialize(&self, dst: &mut [MaybeUninit<u8>]) -> Result<(), Error>;
    fn deserialize(&mut self, src: &[u8]) -> Result<(), Error>;
}
