use std::cmp::{
    PartialOrd,    
    Ord,
    PartialEq,
    Eq,
};

use crate::field::{
    Field,
    FieldType,
};

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct Row {
    fields: Vec<Field>,
}

impl Row {
    pub fn new(length: usize) -> Self {
        let mut fields = Vec::<Field>::new();
        fields.reserve(length); 

        for index in 0..length {
            fields.insert(
                index, 
                Field::new(FieldType::Null));
        }

        fields.shrink_to_fit();

        Row {
            fields,
        }
    }

    pub fn size(&self) -> usize {
        self.fields.len()
    }

    // operator []
    // iter?
}

#[cfg(test)]
mod tests {
    use crate::row::*;

    #[test]
    fn check_row_size() {
        let row = Row::new(3);
        assert_eq!(row.size(), 3);
    }
}
