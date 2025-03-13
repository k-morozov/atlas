use std::cmp::{Eq, Ord, PartialEq, PartialOrd};

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct FlexibleField {
    pub data: Vec<u8>,
}

impl Field for FlexibleField {
    fn new<T: Into<Vec<u8>>>(data: T) -> Self {
        Self { data: data.into() }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn size(&self) -> usize {
        self.len() * size_of::<u8>()
    }

    fn data(&self) -> &[u8] {
        &self.data
    }
    fn mut_data(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

pub trait Field {
    fn new<T: Into<Vec<u8>>>(data: T) -> Self;
    fn len(&self) -> usize;
    fn size(&self) -> usize;
    fn data(&self) -> &[u8];
    fn mut_data(&mut self) -> &mut [u8];
}
