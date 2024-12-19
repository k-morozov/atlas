use std::cmp::{
    PartialOrd,    
    Ord,
    PartialEq,
    Eq,
};

use std::ops::{Add, Index};
use std::iter::{
    IntoIterator,
    Iterator,
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

    pub fn iter(& self) -> impl Iterator<Item = &Field> {
        RowIterator::new(self)
    }
}

impl Index<usize> for Row {
    type Output = Field;

    fn index(&self, index: usize) -> &Self::Output {
        return &self.fields.get(index).unwrap();
    }
}

pub struct RowIterator<'a> {
    pos: usize,
    row: &'a Row,
}

impl<'a> RowIterator<'a> {
    fn new(row: &'a Row) -> Self {
        RowIterator {
            pos: 0,
            row,
        }
    }
}

impl<'a> Iterator for RowIterator<'a> {
    type Item = &'a Field;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos == self.row.size() {
            return None;
        }
        
        let result = self.row.fields.get(self.pos)?;
        self.pos = self.pos.add(1);

        Some(result)
    }
}

impl<'a> IntoIterator for &'a Row {
    type Item = &'a Field;
    type IntoIter = RowIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        RowIterator::new(self)
    }
}

// impl Iterator for Row {
//     type Item = Field;

//     fn next(&mut self) -> Option<Self::Item> {
//         None
//     } 
// }


#[cfg(test)]
mod tests {
    use crate::row::*;

    #[test]
    fn check_size() {
        let row = Row::new(3);
        assert_eq!(row.size(), 3);
    }

    #[test]
    fn check_index() {
        let row = Row::new(3);

        for index in 0..3 {
            assert_eq!(row[index], Field::new(FieldType::Null));
        }
    }

    #[test]
    fn check_iter() {
        let row = Row::new(3);
        let mut it = row.iter();
        
        assert_eq!(*it.next().unwrap(), Field::new(FieldType::Null));
        assert_eq!(*it.next().unwrap(), Field::new(FieldType::Null));
        assert_eq!(*it.next().unwrap(), Field::new(FieldType::Null));

        assert_eq!(it.next(), None);
    }

    #[test]
    fn check_iter_for() {
        let row = Row::new(3);
        for it in &row {
            assert_eq!(*it, Field::new(FieldType::Null));
        }
    }
}
