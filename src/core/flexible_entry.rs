use std::cmp::{Eq, Ord, PartialEq, PartialOrd};
use std::mem::MaybeUninit;

use crate::core::field::{FixedField, FlexibleField};
use crate::core::marshal::Marshal;
use crate::errors::Error;

pub trait WriteEntry {

}

pub trait ReadEntry {

}

pub struct FlexibleEntry {
    pub key: FlexibleField,
    pub value: FlexibleField,
}

impl FlexibleEntry {
    pub fn new(key: FlexibleField, value: FlexibleField) -> Self {
        FlexibleEntry { key, value }
    }

    pub fn get_key(&self) -> &FlexibleField {
        &self.key
    }

    pub fn get_value(&self) -> &FlexibleField {
        &self.value
    }

    pub fn len(&self) -> usize {
        self.key.len() + self.value.len()
    }
}