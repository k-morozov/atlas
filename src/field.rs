use std::cmp::{
    PartialOrd,    
    Ord,
    PartialEq,
    Eq,
};

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub enum FieldType {
    Null,
    Int,
    String,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct Field {
    field: FieldType,
}

impl Field {
    pub fn new(field: FieldType) -> Self {
        Field {
            field
        }
    }
}