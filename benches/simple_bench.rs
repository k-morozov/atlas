extern crate test;
use test::Bencher;

use std::sync::mpsc::channel;
use std::time::{Duration, Instant};
use std::{fs, io, thread};

use log::info;
use rand::Rng;

use kvs::core::entry::flexible_user_entry::FlexibleUserEntry;
use kvs::core::field::FlexibleField;
use kvs::core::storage::{
    config::StorageConfig, ordered_storage::OrderedStorage, storage::Storage,
};

const TOTAL_VALUE: usize = 500_000;

pub fn init() {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();
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

#[bench]
fn bench_table() {
    init();

    info!("Prepare dir for example");

    let storage_path = "/tmp/kvs/examples/example_table";

    if fs::exists(storage_path)? {
        fs::remove_dir_all(storage_path)?;
    }

    info!("Start example");

    let mut config = StorageConfig::default_config();
    config.mem_table_size = 256;
    config.disk_tables_limit_by_level = 4;

    let table = OrderedStorage::new(storage_path, config);

    let (tx, rx) = channel();
    let mid = TOTAL_VALUE / 2;

    thread::scope(|s| {
        s.spawn(|| {
            for index in 0..mid {
                let entry = random_entry();
                table.put(&entry).unwrap();
                tx.send(entry).unwrap();

                if index % 10000 == 0 {
                    info!("thread 1: {} entries were inserted", index);
                }
            }
        });

        s.spawn(|| {
            for index in 0..mid {
                let entry = random_entry();
                table.put(&entry).unwrap();
                tx.send(entry).unwrap();

                if index % 10000 == 0 {
                    info!("thread 2: {} entries were inserted", index);
                }
            }
        });

        let mut durations = Vec::with_capacity(10000);
        for index in 0..TOTAL_VALUE {
            match rx.recv() {
                Ok(entry) => {
                    if index % 10000 == 0 && index != 0 {
                        let total_nanos: u128 =
                            durations.iter().map(|d: &Duration| d.as_nanos()).sum();
                        let avg_nanos = total_nanos / durations.len() as u128;

                        info!(
                            "thread 3: proccessed index={}, avg get duration={:?}, total duration={:?}",
                            index,
                            Duration::from_nanos(avg_nanos as u64),
                            Duration::from_nanos(total_nanos as u64),
                        );
                        durations.clear();
                    }

                    let start = Instant::now();
                    let result = table.get(entry.get_key()).unwrap();
                    let duration = start.elapsed();
                    durations.push(duration);

                    let expected = entry.get_value();

                    assert_eq!(result.unwrap(), *expected, "expected {:?}", *expected);
                }
                Err(er) => panic!("Error: {:?}", er),
            }
        }
    });
}
