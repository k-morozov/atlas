use crate::row::Row;


struct MemTable {
    rows: Vec<Row>,
    current_size: usize,
    max_table_size: usize,
    row_length: usize,
}

impl MemTable {
    pub fn new(max_table_size: usize, row_length: usize) -> Self {
        let mut data = Vec::with_capacity(max_table_size);

        for index in 0..max_table_size {
            data.insert(index, Row::new(row_length));
        }

        MemTable {
            rows: data,
            current_size: 0,
            max_table_size,
            row_length,
        }
    }

    pub fn current_size(&self) -> usize {
        self.current_size
    }

    pub fn max_table_size(&self) -> usize {
        self.max_table_size
    }

    pub fn row_length(&self) -> usize {
        self.row_length
    }

    pub fn add_row(&mut self, row: Row) {
        self.rows.push(row);
        self.rows.sort();
    }
}

#[cfg(test)]
mod tests {
    use crate::mem_table;
    use crate::row::Row;

    #[test]
    fn check_sizes() {
        let mem_table = mem_table::MemTable::new(3, 2);
        assert_eq!(mem_table.current_size(), 0);
        assert_eq!(mem_table.max_table_size(), 3);
        assert_eq!(mem_table.row_length(), 2);
    }

    #[test]
    fn check_add_row() {
        let row_length = 3;

        let mem_table = mem_table::MemTable::new(3, row_length);
        let row = Row::new(row_length);
    }
}