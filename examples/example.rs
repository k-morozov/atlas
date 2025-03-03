use std::sync::mpsc::channel;
use std::thread::{self, sleep};

use log::info;
use rand::Rng;

use kvs::core::entry::flexible_user_entry::FlexibleUserEntry;
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

fn random_entry() -> FlexibleUserEntry {
    let key = FlexibleField::new(generate_random_bytes(64, 128));
    let value = FlexibleField::new(generate_random_bytes(512, 1024));

    FlexibleUserEntry::new(key, value)
}

fn main() {
    init();

    info!("start example");

    let table_name = "/tmp/kvs/examples/example_table";
    let mut config = StorageConfig::default_config();
    config.mem_table_size = 256;
    config.disk_tables_limit_by_level = 16;

    let table = OrderedStorage::new(table_name, config);

    let (tx, rx) = channel();
    let mid = TOTAL_VALUE / 2;

    thread::scope(|s| {
        s.spawn(|| {
            for index in 0..mid {
                let entry = random_entry();
                table.put(entry.clone()).unwrap();
                tx.send(entry).unwrap();

                if index % 10000 == 0 {
                    info!("thread 1: {} entries were inserted", index);
                }
            }
        });

        s.spawn(|| {
            for index in mid..TOTAL_VALUE {
                let entry = random_entry();
                table.put(entry.clone()).unwrap();
                tx.send(entry).unwrap();

                if index % 10000 == 0 {
                    info!("thread 2: {} entries were inserted", index);
                }
            }
        });

        for index in 0..TOTAL_VALUE {
            match rx.try_recv() {
                Ok(entry) => {
                    if index % 10000 == 0 {
                        info!("searching index={}", index);
                    }

                    let result = table.get(entry.get_key()).unwrap();
                    let expected = entry.get_value();
                    if result.is_none() {
                        assert!(false, "expected {:?}", *expected);
                    }
                    
                    assert_eq!(result.unwrap(), *expected, "expected {:?}", *expected);
                }
                Err(er) => match er {
                    std::sync::mpsc::TryRecvError::Empty => {
                        thread::sleep(std::time::Duration::from_secs(2));
                        continue;
                    }
                    std::sync::mpsc::TryRecvError::Disconnected => {
                        panic!("Disconnected is unexpected")
                    }
                },
            }
        }
    });
}
