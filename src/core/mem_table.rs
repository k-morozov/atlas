use std::iter::IntoIterator;

use crate::core::entry::Entry;

pub struct MemTable {
    rows: Vec<Entry>,
    current_size: usize,
    max_table_size: usize,
}

impl MemTable {
    pub fn new(max_table_size: usize) -> Self {
        let data = Vec::with_capacity(max_table_size);

        MemTable {
            rows: data,
            current_size: 0,
            max_table_size,
        }
    }

    pub fn current_size(&self) -> usize {
        self.current_size
    }

    pub fn max_table_size(&self) -> usize {
        self.max_table_size
    }

    pub fn append(&mut self, row: Entry) {
        self.rows.push(row);
        self.rows.sort();
        self.current_size += 1;
    }

    pub fn iter(&self) -> impl Iterator<Item = &Entry> {
        self.rows.iter()
    }

    pub fn get(&self, index: usize) -> Option<&Entry> {
        self.rows.get(index)
    }

    pub fn clear(&mut self) {
        self.rows.clear();
        self.current_size = 0;
    }
}

pub struct MemTableIterator<'a> {
    table: &'a MemTable,
    pos: usize,
}

impl<'a> Iterator for MemTableIterator<'a> {
    type Item = &'a Entry;

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
    type Item = &'a Entry;
    type IntoIter = MemTableIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        MemTableIterator {
            table: self,
            pos: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::core::entry::Entry;
    use crate::core::field::*;
    use crate::core::mem_table;
    use std::iter::zip;

    #[test]
    fn check_sizes() {
        let mem_table = mem_table::MemTable::new(3);
        assert_eq!(mem_table.current_size(), 0);
        assert_eq!(mem_table.max_table_size(), 3);
    }

    #[test]
    fn check_append() {
        let mut mem_table = mem_table::MemTable::new(3);

        let entry1 = Entry::new(
            Field::new(FieldType::Int32(33)),
            Field::new(FieldType::Int32(330)),
        );
        mem_table.append(entry1.clone());

        let entry2 = Entry::new(
            Field::new(FieldType::Int32(34)),
            Field::new(FieldType::Int32(340)),
        );
        mem_table.append(entry2.clone());

        let entry3 = Entry::new(
            Field::new(FieldType::Int32(35)),
            Field::new(FieldType::Int32(350)),
        );
        mem_table.append(entry3.clone());

        let mut it = mem_table.iter();

        let r = it.next();
        assert_eq!(&entry1, r.unwrap());

        let r = it.next();
        assert_eq!(&entry2, r.unwrap());

        let r = it.next();
        assert_eq!(&entry3, r.unwrap());

        assert!(it.next().is_none());

        let expected = [&entry1, &entry2, &entry3];
        for (actual, expected) in zip(&mem_table, expected) {
            assert_eq!(actual, expected);
        }
    }
}
