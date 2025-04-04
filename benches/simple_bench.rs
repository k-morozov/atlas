use criterion::{criterion_group, criterion_main, Criterion};
use kvs::core::storage::config::DEFAULT_TEST_TABLES_PATH;
use tempfile::Builder;

use std::sync::mpsc::channel;
use std::time::{Duration, Instant};
use std::{fs, thread};

use log::info;
use rand::Rng;

use kvs::core::entry::flexible_user_entry::FlexibleUserEntry;
use kvs::core::field::{Field, FlexibleField};
use kvs::core::storage::{
    config::StorageConfig, ordered_storage::OrderedStorage, storage::Storage,
};

const TOTAL_VALUE: usize = 100_000;

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

fn example_table() {
    info!("Prepare dir for example");

    let tmp_dir = Builder::new()
        .prefix(DEFAULT_TEST_TABLES_PATH)
        .tempdir()
        .unwrap();
    let storage_path = tmp_dir.path().join("example_table");

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

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple-run");
    // group.measurement_time(Duration::new(90, 0));
    group.warm_up_time(Duration::new(10, 0));
    group.measurement_time(Duration::new(90, 0));
    group.sample_size(20);

    group.bench_function("my benchmark", |b| b.iter(|| example_table()));
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
