use std::sync::mpsc::channel;
use std::{io, thread};

use rand::Rng;

use kvs::core::entry::flexible_user_entry::FlexibleUserEntry;
use kvs::core::field::FlexibleField;
use kvs::core::storage::{
    config::StorageConfig, ordered_storage::OrderedStorage, storage::Storage,
};

const TOTAL_VALUE: usize = 1_000;

fn generate_random_bytes(start: u32, finish: u32) -> Vec<u8> {
    let bytes = rand::rng().random_range(start..=finish);

    let mut rng = rand::rng();

    let random_bytes = (0..bytes).map(|_| rng.random_range(0..255)).collect();

    random_bytes
}

fn random_entry() -> FlexibleUserEntry {
    let key = FlexibleField::new(generate_random_bytes(64, 128));
    let value = FlexibleField::new(generate_random_bytes(512, 1024));

    FlexibleUserEntry::new(key, value)
}

#[test]
fn test_mpsc_table() -> io::Result<()> {
    let table_name = "/tmp/kvs/test/simple_flexible_segment";
    let mut config = StorageConfig::default_config();
    config.mem_table_size = 32;
    config.disk_tables_limit_by_level = 3;

    let table = OrderedStorage::new(table_name, config);

    let (tx, rx) = channel();
    let mid = TOTAL_VALUE / 2;

    thread::scope(|s| {
        s.spawn(|| {
            for _index in 0..mid {
                let entry = random_entry();
                table.put(&entry).unwrap();
                tx.send(entry).unwrap();
            }
        });

        s.spawn(|| {
            for _index in mid..TOTAL_VALUE {
                let entry = random_entry();
                table.put(&entry).unwrap();
                tx.send(entry).unwrap();
            }
        });

        for _index in 0..TOTAL_VALUE {
            match rx.recv() {
                Ok(entry) => {
                    let result = table.get(entry.get_key()).unwrap();
                    let expected = entry.get_value();
                    if result.is_none() {
                        assert!(false, "expected {:?}", *expected);
                        continue;
                    }

                    assert_eq!(result.unwrap(), *expected, "expected {:?}", *expected);
                }
                Err(er) => panic!("Error: {:?}", er),
            }
        }
    });

    Ok(())
}
