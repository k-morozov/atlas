use log::info;
use rand::Rng;

use kvs::core::entry::flexible_entry::FlexibleEntry;
use kvs::core::field::FlexibleField;
use kvs::core::storage::{
    config::StorageConfig, ordered_storage::OrderedStorage, storage::Storage,
};

const TOTAL_VALUE: usize = 100_000;

pub fn init() {
    simple_logger::SimpleLogger::new().init().unwrap();
}

fn generate_random_bytes(start: u32, finish: u32) -> Vec<u8> {
    let bytes = rand::rng().random_range(start..=finish);

    let mut rng = rand::rng();

    let random_bytes = (0..bytes).map(|_| rng.random_range(0..255)).collect();

    random_bytes
}

fn main() {
    init();

    info!("start example");

    let table_name = "/tmp/kvs/examples/example_table";
    let mut config = StorageConfig::default_config();
    config.mem_table_size = 256;
    config.disk_tables_limit_by_level = 16;

    let mut table = OrderedStorage::new(table_name, config);

    let mut expected = Vec::with_capacity(TOTAL_VALUE);

    for index in 0..TOTAL_VALUE {
        let key = FlexibleField::new(generate_random_bytes(64, 128));
        let value = FlexibleField::new(generate_random_bytes(512, 1024));

        let entry = FlexibleEntry::new(key, value);
        table.put(entry.clone()).unwrap();

        expected.push(entry);

        if index % 10000 == 0 {
            info!("{} entries were inserted", index);
        }
    }

    info!("Data was inserted.");

    for index in (0..TOTAL_VALUE).step_by(10) {
        if index % 10000 == 0 {
            info!("searching index={}", index);
        }

        let expected_entry = &expected[index];

        let result = table.get(expected_entry.get_key()).unwrap();

        assert_eq!(result.unwrap(), *expected_entry.get_value());
    }
}
