use std::fs;
use std::io;
use std::path::Path;

use kvs::core::storage::config::DEFAULT_TEST_TABLES_PATH;
use kvs::core::storage::ordered_storage::OrderedStorage;
use kvs::core::storage::storage::Storage;
use tempfile::Builder;

use kvs::core::disk_table::disk_table::get_disk_table_name;
use kvs::core::disk_table::id::DiskTableID;
use kvs::core::disk_table::local::local_disk_table_builder::DiskTableBuilder;
use kvs::core::entry::flexible_user_entry::FlexibleUserEntry;
use kvs::core::field::FlexibleField;
use kvs::core::storage::config::StorageConfig;

#[test]
fn simple_flexible_segment() -> io::Result<()> {
    // let table_path = Path::new("/tmp/kvs/test/simple_flexible_segment");
    let segment_name = get_disk_table_name(DiskTableID::from(12));
    let disk_table_path = format!(
        "/tmp/kvs/test/simple_flexible_segment/segment/{}",
        segment_name
    );
    let disk_table_path: &Path = Path::new(&disk_table_path);

    if let Some(parent) = disk_table_path.parent() {
        fs::create_dir_all(parent).unwrap();
    }

    if let Err(er) = fs::remove_file(disk_table_path) {
        assert_eq!(io::ErrorKind::NotFound, er.kind());
    }

    let mut entries: Vec<FlexibleUserEntry> = Vec::new();

    const MAX_SIZE: u32 = 6;

    for index in (1..MAX_SIZE).step_by(1) {
        entries.push(FlexibleUserEntry::new(
            FlexibleField::new(index.to_le_bytes().to_vec()),
            FlexibleField::new((index + 10).to_le_bytes().to_vec()),
        ));
    }

    let segment = entries
        .into_iter()
        .fold(
            DiskTableBuilder::new(disk_table_path),
            |mut builder, entry| {
                builder.append_entry(&entry);
                builder
            },
        )
        .build()
        .unwrap();

    for index in (1..MAX_SIZE).step_by(1) {
        let actual = segment
            .read(&FlexibleField::new(index.to_le_bytes().to_vec()))
            .unwrap()
            .unwrap();

        let expected = FlexibleField::new((index + 10).to_le_bytes().to_vec());
        assert_eq!(actual, expected);
    }

    Ok(())
}

#[test]
fn test_get_from_some_data_blocks() -> io::Result<()> {
    let tmp_dir = Builder::new().prefix(DEFAULT_TEST_TABLES_PATH).tempdir()?;
    let table_path = tmp_dir.path().join("test_get_from_some_data_blocks");

    let mut config = StorageConfig::default_config();

    config.mem_table_size = 3;
    config.data_block_size = 64;

    let table = OrderedStorage::new(table_path, config.clone());

    for index in 0..9 as u8 {
        let entry = FlexibleUserEntry::new(
            FlexibleField::new(vec![index, index, index]),
            FlexibleField::new(vec![index * 10, index * 10, index * 10]),
        );
        table.put(entry).unwrap();
    }

    for index in 0..9 as u8 {
        let result = table
            .get(&FlexibleField::new(vec![index, index, index]))
            .unwrap();
        assert_eq!(
            result.unwrap(),
            FlexibleField::new(vec![index * 10, index * 10, index * 10])
        );
    }

    let result = table.get(&FlexibleField::new(vec![1, 1, 10])).unwrap();
    assert_eq!(result, None);

    let result = table.get(&FlexibleField::new(vec![10, 1, 1])).unwrap();
    assert_eq!(result, None);

    let result = table.get(&FlexibleField::new(vec![2])).unwrap();
    assert_eq!(result, None);

    Ok(())
}
