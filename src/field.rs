use std::cmp::{
    PartialOrd,    
    Ord,
    PartialEq,
    Eq,
};

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
pub enum FieldType {
    Null,
    Int,
    String,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
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