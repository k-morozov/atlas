use std::cmp::{Eq, Ord, PartialEq, PartialOrd};

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub enum FieldType {
    Int(i32),
    String(String),
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct Field {
    pub field: FieldType,
}

impl Field {
    pub fn new(field: FieldType) -> Self {
        Field { field }
    }
}
