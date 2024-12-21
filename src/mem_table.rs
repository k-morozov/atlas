use crate::row::Row;
use std::iter::IntoIterator;

struct MemTable {
    rows: Vec<Row>,
    current_size: usize,
    max_table_size: usize,
    max_row_length: usize,
}

impl MemTable {
    pub fn new(max_table_size: usize, max_row_length: usize) -> Self {
        let data = Vec::with_capacity(max_table_size);

        MemTable {
            rows: data,
            current_size: 0,
            max_table_size,
            max_row_length,
        }
    }

    pub fn current_size(&self) -> usize {
        self.current_size
    }

    pub fn max_table_size(&self) -> usize {
        self.max_table_size
    }

    pub fn max_row_length(&self) -> usize {
        self.max_row_length
    }

    pub fn append(&mut self, row: Row) {
        self.rows.push(row);
        self.rows.sort();
        self.current_size += 1;
    }

    pub fn iter(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter()
    }

    pub fn get(&self, index: usize) -> Option<&Row> {
        self.rows.get(index)
    }
}

struct MemTableIterator<'a> {
    table: &'a MemTable,
    pos: usize,
}

impl<'a> Iterator for MemTableIterator<'a> {
    type Item = &'a Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos == self.table.max_table_size() {
            return None;
        }

        let result = self.table.get(self.pos);
        self.pos += 1;
        result
    }
}

impl<'a> IntoIterator for &'a MemTable {
    type Item = &'a Row;
    type IntoIter = MemTableIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        MemTableIterator{
            table: self,
            pos: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::field::*;
    use crate::mem_table;
    use crate::row::{Row, RowBuilder};
    use std::iter::zip;

    fn create_row(fields: &[Field]) -> Row {
        let mut builder = RowBuilder::new(fields.len());

        for field in fields {
            builder = builder.add_field(field.clone());
        }

        builder.build().unwrap()
    }

    fn check_row_with_expected(xs: &Row, ys: &[Field]) {
        assert!(
            xs.iter().zip(ys).all(|(l, r)| l == r),
            "row and expected field are diffrent"
        );
    }

    #[test]
    fn check_sizes() {
        let mem_table = mem_table::MemTable::new(3, 2);
        assert_eq!(mem_table.current_size(), 0);
        assert_eq!(mem_table.max_table_size(), 3);
        assert_eq!(mem_table.max_row_length(), 2);
    }

    #[test]
    fn check_append() {
        let max_row_length = 3;

        let mut mem_table = mem_table::MemTable::new(3, max_row_length);

        let fields1 = [
            Field::new(FieldType::Null),
            Field::new(FieldType::String("a1".to_string())),
            Field::new(FieldType::String("a2".to_string())),
        ];

        let row = create_row(&fields1);
        mem_table.append(row);

        let fields2 = [
            Field::new(FieldType::Null),
            Field::new(FieldType::String("b1".to_string())),
            Field::new(FieldType::String("b2".to_string())),
        ];

        let row = create_row(&fields2);
        mem_table.append(row);

        let fields3 = [
            Field::new(FieldType::Null),
            Field::new(FieldType::String("c1".to_string())),
            Field::new(FieldType::String("c2".to_string())),
        ];

        let row = create_row(&fields3);
        mem_table.append(row);

        let mut it = mem_table.iter();

        let r = it.next();
        check_row_with_expected(r.unwrap(), &fields1);

        let r = it.next();
        check_row_with_expected(r.unwrap(), &fields2);

        let r = it.next();
        check_row_with_expected(r.unwrap(), &fields3);

        assert!(it.next().is_none());

        let expected = [&fields1, &fields2, &fields3];
        for (row, field) in zip(&mem_table, expected) {
            check_row_with_expected(row, field);
        }
    }
}
