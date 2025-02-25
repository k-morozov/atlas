use std::cmp::{Eq, Ord, PartialEq, PartialOrd};

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

impl Field for FlexibleField {
    fn data(&self) -> &[u8] {
        &self.data
    }
    fn mut_data(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

impl FieldSize for FlexibleField {
    fn size(&self) -> usize {
        self.len() * size_of::<u8>()
    }
}

pub trait Field {
    fn data(&self) -> &[u8];
    fn mut_data(&mut self) -> &mut [u8];
}
pub trait FieldSize {
    fn size(&self) -> usize;
}
