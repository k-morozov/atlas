use std::cmp::{Eq, Ord, PartialEq, PartialOrd};

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub enum FieldType {
    Null,
    Int(i32),
    String(String),
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct Field {
    field: FieldType,
}

impl Field {
    pub fn new(field: FieldType) -> Self {
        Field { field }
    }
}
