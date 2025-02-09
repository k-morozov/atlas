use rand::Rng;

use kvs::core::entry::fixed_entry::FixedEntry;
use kvs::core::field::{FieldType, FixedField};
use kvs::core::table::simple_table::SimpleTable;
use kvs::core::table::{config::TableConfig, table::Table};

const TOTAL_VALUE: usize = 1000000;
const K: i32 = 117;

fn main() {
    let table_name = "example_table";
    let config = TableConfig::new_config(512, 16);
    let mut table = SimpleTable::new(table_name, config);

    for index in 0..TOTAL_VALUE {
        let entry = FixedEntry::new(
            FixedField::new(FieldType::Int32(index as i32)),
            FixedField::new(FieldType::Int32((index as i32) * K)),
        );
        table.put(entry).unwrap();
    }

    println!("Data was inserted.");

    for index in (0..TOTAL_VALUE).step_by(10000) {
        println!("Start searching index={}", index);
        let result = table
            .get(FixedField::new(FieldType::Int32(index as i32)))
            .unwrap();

        assert_eq!(
            result.unwrap(),
            FixedField::new(FieldType::Int32((index as i32) * K))
        );
    }

    // for index in 0..100 {
    //     println!("Start searching index={}", index);

    //     for j in (index..TOTAL_VALUE).step_by(100) {
    //         println!("Searching index={}, j={}", index, j);
    //         let result = table.get(Field::new(FieldType::Int32(j as i32))).unwrap();

    //         assert_eq!(
    //             result.unwrap(),
    //             Field::new(FieldType::Int32((j as i32) * K))
    //         );
    //     }
    // }
}
