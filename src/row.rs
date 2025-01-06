use std::cmp::{Eq, Ord, PartialEq, PartialOrd};

use std::iter::{IntoIterator, Iterator};
use std::ops::{Add, Index};
use std::rc::Rc;

use crate::field::{Field, FieldType};
use crate::marshal::Marshal;
use crate::pg_errors::PgError;
use crate::schema::Schema;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Row {
    fields: Vec<Field>,
    max_length: usize,
    schema: Option<Rc<Schema>>,
}

impl Row {
    pub fn new(max_length: usize) -> Self {
        let mut fields = Vec::<Field>::new();
        fields.reserve(max_length);

        Row {
            schema: None,
            max_length,
            fields,
        }
    }

    pub fn fields_len(&self) -> usize {
        self.fields.len()
    }

    pub fn size(&self) -> usize {
        let mut total_size = 0;
        self.fields.iter().for_each(|f| total_size += f.size());
        total_size
    }

    pub fn iter(&self) -> impl Iterator<Item = &Field> {
        RowIterator::new(self)
    }

    fn push(&mut self, field: Field) {
        if self.fields_len() == self.max_length {
            panic!("Try push to row more than max_length");
        }
        self.fields.push(field);
    }

    pub fn get(&self, index: usize) -> Option<&Field> {
        self.fields.get(index)
    }

    pub fn set_schema(&mut self, schema: Rc<Schema>) -> Result<(), PgError> {
        match &self.schema {
            Some(_) => {
                return Err(PgError::RowAlreadyContainsSchema);
            }
            None => {
                self.schema = Some(schema);
            }
        }
        Ok(())
    }
}

impl Marshal for Row {
    fn serialize(&self, dst: &mut [u8]) -> Result<(), PgError> {
        let mut offset = 0;
        for field in &self.fields {
            field.serialize(&mut dst[offset..offset + field.size()])?;
            offset += field.size();
        }

        Ok(())
    }
    fn deserialize(&mut self, src: &[u8]) -> Result<(), PgError> {
        match &self.schema {
            Some(schema) => {
                let mut offset = 0;

                for schema_field in schema.iter() {
                    let mut deserialized_field = Field::new(schema_field.field.clone());

                    deserialized_field.deserialize(&src[offset..offset+schema_field.size()])?;
                    offset += deserialized_field.size();

                    self.fields.push(deserialized_field);
                }
            }
            None => {
                return Err(PgError::NoSchemaInRow);
            }
        }

        Ok(())
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
        if self.pos == self.row.fields_len() {
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
    use crate::field::*;
    use crate::row::*;
    use crate::schema::Schema;

    #[test]
    fn check_size() {
        let row = Row::new(3);
        assert_eq!(row.fields_len(), 0);
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
        row.push(Field::new(FieldType::Int32(33)));

        assert_eq!(row[1], Field::new(FieldType::Int32(33)));
        assert_eq!(row[0], Field::new(FieldType::Int32(32)));

        assert_eq!(*row.get(0).unwrap(), Field::new(FieldType::Int32(32)));
        assert_eq!(*row.get(1).unwrap(), Field::new(FieldType::Int32(33)));

        assert_eq!(row.get(2), None);
    }

    #[test]
    fn check_builder() {
        let builder = RowBuilder::new(2);

        let row = builder
            .add_field(Field::new(FieldType::Int32(42)))
            .add_field(Field::new(FieldType::Int32(33)))
            .build()
            .unwrap();

        assert_eq!(row[1], Field::new(FieldType::Int32(33)));
        assert_eq!(row[0], Field::new(FieldType::Int32(42)));

        assert_eq!(*row.get(0).unwrap(), Field::new(FieldType::Int32(42)));
        assert_eq!(*row.get(1).unwrap(), Field::new(FieldType::Int32(33)));

        assert!(row.get(2).is_none());
    }

    #[test]
    fn serialization() {
        let builder = RowBuilder::new(2);

        let row_in = builder
            .add_field(Field::new(FieldType::Int32(42)))
            .add_field(Field::new(FieldType::Int32(33)))
            .build()
            .unwrap();

        let mut row_buffer = vec![0u8; row_in.size()];
        let result = row_in.serialize(&mut row_buffer);
        assert!(result.is_ok());

        let schema = Rc::new(vec![
            Field::new(FieldType::Int32(0)),
            Field::new(FieldType::Int32(0)),
        ]);

        let mut row_out = Row::new(2);
        let result = row_out.set_schema(schema);
        assert!(result.is_ok());

        let result = row_out.deserialize(&row_buffer);
        assert!(result.is_ok());

        assert_eq!(row_out[0], Field::new(FieldType::Int32(42)));
        assert_eq!(row_out[1], Field::new(FieldType::Int32(33)));

        assert_eq!(row_out.size(), 2 * size_of::<i32>());
    }
}
