use std::cmp::{Eq, Ord, PartialEq, PartialOrd};

use std::iter::{IntoIterator, Iterator};
use std::ops::{Add, Index};

use crate::field::{Field, FieldType};
use crate::marshal::Marshal;
use crate::pg_errors::PgError;
use std::ptr::copy;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Row {
    fields: Vec<Field>,
    max_length: usize,
}

impl Row {
    pub fn new(max_length: usize) -> Self {
        let mut fields = Vec::<Field>::new();
        fields.reserve(max_length);

        Row { max_length, fields }
    }

    pub fn size(&self) -> usize {
        self.fields.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Field> {
        RowIterator::new(self)
    }

    fn push(&mut self, field: Field) {
        if self.size() == self.max_length {
            panic!("Try push to row more than max_length");
        }
        self.fields.push(field);
    }

    pub fn get(&self, index: usize) -> Option<&Field> {
        self.fields.get(index)
    }

    pub fn get_fields(&self) -> Vec<Field> {
        self.fields.clone()
    }
}

impl Marshal for Row {
    fn serialize(&self, dst: &mut [u8]) -> Result<(), PgError> {
        let mut dst: Vec<_> = Vec::with_capacity(self.max_length);

        for field in &self.fields {
            match &field.field {
                FieldType::Int32(number) => unsafe {
                    copy(&number, dst.as_mut_ptr(), size_of::<i32>());
                },
            }
        }

        unreachable!()
    }
    fn deserialize(&mut self, src: &[u8]) -> Result<(), PgError> {
        unreachable!()
    }
}

pub struct RowBuilder {
    row: Row,
}

impl RowBuilder {
    pub fn new(max_length: usize) -> Self {
        Self {
            row: Row::new(max_length),
        }
    }

    pub fn add_field(mut self, field: Field) -> Self {
        self.row.push(field);
        self
    }

    pub fn build(self) -> Result<Row, PgError> {
        Ok(self.row)
    }
}

impl Index<usize> for Row {
    type Output = Field;

    fn index(&self, index: usize) -> &Self::Output {
        &self.fields[index]
    }
}

pub struct RowIterator<'a> {
    row: &'a Row,
    pos: usize,
}

impl<'a> RowIterator<'a> {
    fn new(row: &'a Row) -> Self {
        RowIterator { pos: 0, row }
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

#[cfg(test)]
mod tests {
    use crate::row::*;

    #[test]
    fn check_size() {
        let row = Row::new(3);
        assert_eq!(row.size(), 0);
    }

    #[test]
    fn check_get() {
        let row = Row::new(3);

        for index in 0..3 {
            assert_eq!(row.get(index), None);
        }
    }

    #[test]
    #[should_panic]
    fn check_index() {
        let row = Row::new(2);
        let _r = &row[0];
    }

    #[test]
    fn check_empty_iter() {
        let row = Row::new(3);
        let mut it = row.iter();

        assert_eq!(it.next(), None);
        assert_eq!(it.next(), None);
    }

    #[test]
    fn check_iter_for() {
        let row = Row::new(3);
        for _it in &row {
            assert_eq!(false, true);
        }
    }

    #[test]
    #[should_panic]
    fn check_failed_push() {
        let mut row = Row::new(1);
        row.push(Field::new(FieldType::Int32(12)));
        row.push(Field::new(FieldType::Int32(11)));
    }

    #[test]
    fn check_push() {
        let mut row = Row::new(2);

        row.push(Field::new(FieldType::Int32(32)));
        // row.push(Field::new(FieldType::String("test msg".to_string())));
        row.push(Field::new(FieldType::Int32(33)));

        assert_eq!(row[1], Field::new(FieldType::Int32(33)));
        // assert_eq!(
        //     row[1],
        //     Field::new(FieldType::String("test msg".to_string()))
        // );
        assert_eq!(row[0], Field::new(FieldType::Int32(32)));

        assert_eq!(*row.get(0).unwrap(), Field::new(FieldType::Int32(32)));
        // assert_eq!(
        //     *row.get(1).unwrap(),
            // Field::new(FieldType::String("test msg".to_string()))
        // );
        assert_eq!(*row.get(1).unwrap(), Field::new(FieldType::Int32(33)));

        assert_eq!(row.get(2), None);
    }

    #[test]
    fn check_build() {
        let builder = RowBuilder::new(2);

        let row = builder
            .add_field(Field::new(FieldType::Int32(42)))
            // .add_field(Field::new(FieldType::String("test msg".to_string())))
            .add_field(Field::new(FieldType::Int32(33)))
            .build()
            .unwrap();

        assert_eq!(row[1], Field::new(FieldType::Int32(33)));
        // assert_eq!(
        //     row[1],
        //     Field::new(FieldType::String("test msg".to_string()))
        // );
        assert_eq!(row[0], Field::new(FieldType::Int32(42)));

        assert_eq!(*row.get(0).unwrap(), Field::new(FieldType::Int32(42)));
        // assert_eq!(
        //     *row.get(1).unwrap(),
        //     Field::new(FieldType::String("test msg".to_string()))
        // );
        assert_eq!(*row.get(1).unwrap(), Field::new(FieldType::Int32(33)));

        assert!(row.get(2).is_none());
    }
}
