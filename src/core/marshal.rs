use std::mem::MaybeUninit;

use crate::errors::Result;

pub trait Marshal {
    fn serialize(&self, dst: &mut [MaybeUninit<u8>]) -> Result<()>;
    fn deserialize(&mut self, src: &[u8]) -> Result<()>;
}
