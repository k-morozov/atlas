use std::iter::IntoIterator;

use crate::core::entry::fixed_entry::FixedEntry;
use crate::core::field::FixedField;

pub struct MemTable {
    entries: Vec<FixedEntry>,
    current_size: usize,
    max_table_size: usize,
}

impl MemTable {
    pub fn new(max_table_size: usize) -> Self {
        let entries = Vec::with_capacity(max_table_size);

        MemTable {
            entries: entries,
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

    pub fn append(&mut self, row: FixedEntry) {
        self.entries.push(row);
        // self.entries.sort();
        self.current_size += 1;
    }

    pub fn get_value(&self, key: &FixedField) -> Option<FixedField> {
        self.entries
            .iter()
            .find(|entry| entry.get_key() == key)
            .map(|entry| entry.get_value().clone())
    }

    pub fn iter(&self) -> impl Iterator<Item = &FixedEntry> {
        self.entries.iter()
    }

    fn get(&self, index: usize) -> Option<&FixedEntry> {
        self.entries.get(index)
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.current_size = 0;
    }
}

pub struct MemTableIterator<'a> {
    table: &'a MemTable,
    pos: usize,
}

impl<'a> Iterator for MemTableIterator<'a> {
    type Item = &'a FixedEntry;

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
    type Item = &'a FixedEntry;
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
    use crate::core::entry::fixed_entry::FixedEntry;
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

        let entry1 = FixedEntry::new(
            FixedField::new(FieldType::Int32(33)),
            FixedField::new(FieldType::Int32(330)),
        );
        mem_table.append(entry1.clone());

        let entry2 = FixedEntry::new(
            FixedField::new(FieldType::Int32(34)),
            FixedField::new(FieldType::Int32(340)),
        );
        mem_table.append(entry2.clone());

        let entry3 = FixedEntry::new(
            FixedField::new(FieldType::Int32(35)),
            FixedField::new(FieldType::Int32(350)),
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
