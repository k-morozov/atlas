use rand::Rng;

use kvs::core::entry::Entry;
use kvs::core::field::{Field, FieldType};
use kvs::core::table::simple_table::SimpleTable;
use kvs::core::table::table::Table;

const TOTAL_VALUE: usize = 1000000;
const K: i32 = 117;

fn main() {
    let table_name = "example_table";
    let mut table = SimpleTable::new(table_name);

    for index in 0..TOTAL_VALUE {
        let entry = Entry::new(
            Field::new(FieldType::Int32(index as i32)),
            Field::new(FieldType::Int32((index as i32) * K)),
        );
        table.put(entry).unwrap();
    }

    for i in 0..100 {
        for j in (i..TOTAL_VALUE).step_by(100) {
            let result = table.get(Field::new(FieldType::Int32(j as i32))).unwrap();

            assert_eq!(
                result.unwrap(),
                Field::new(FieldType::Int32((j as i32) * K))
            );
        }
    }
}
