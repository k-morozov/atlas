use std::io;

use kvs::core::{
    disk_table::{
        disk_tables_shard::DiskTablesShards, local::disk_table_builder::DiskTableBuilder,
    },
    entry::flexible_user_entry::FlexibleUserEntry,
    field::{Field, FlexibleField},
    storage::{
        config::{StorageConfig, DEFAULT_TEST_TABLES_PATH},
        ordered_storage::OrderedStorage,
        storage::Storage,
    },
};
use tempfile::Builder;

#[test]
fn test_merge_with_some_data_blocks_by_some_levels() -> io::Result<()> {
    let tmp_dir = Builder::new().prefix(DEFAULT_TEST_TABLES_PATH).tempdir()?;
    let table_path = tmp_dir.path().join("test_merge_with_some_data_blocks");

    let mut config = StorageConfig::default_config();
    config.mem_table_size = 32;

    let amount = 2 * config.mem_table_size * config.disk_tables_limit_by_level;
    let value_len = 1900;

    {
        let table = OrderedStorage::new(table_path.as_path(), config.clone());

        for i in 0..amount {
            let k = i as u32;
            let v = vec![i as u8; value_len];
            let r = table.put(&FlexibleUserEntry::new(
                FlexibleField::new(k.to_be_bytes()),
                FlexibleField::new(v),
            ));
            assert!(r.is_ok());
        }
    }

    let table = OrderedStorage::new(table_path.as_path(), config.clone());

    for i in 0..amount {
        let k = i as u32;
        let expected = FlexibleField::new(vec![i as u8; value_len]);
        let r = table.get(&FlexibleField::new(k.to_be_bytes())).unwrap();

        assert!(r.is_some());
        let result = r.unwrap();

        assert_eq!(result.len(), expected.len());
        assert_eq!(result, expected);
    }

    Ok(())
}

#[test]
fn test_simple_merge() -> io::Result<()> {
    let tmp_dir = Builder::new().prefix(DEFAULT_TEST_TABLES_PATH).tempdir()?;

    let shards = DiskTablesShards::new();

    let disk_table_path = tmp_dir.path().join("segment_1_1.bin");
    let index_table_path = tmp_dir.path().join("segment_1_1.idx");

    let mut builder = DiskTableBuilder::new(disk_table_path, index_table_path);
    let value_len = 256;

    for i in 0..16u32 {
        let k = i as u32;
        let v = vec![i as u8; value_len];
        builder.append_entry(&FlexibleUserEntry::new(
            FlexibleField::new(k.to_be_bytes()),
            FlexibleField::new(v),
        ));
    }

    let reader = builder.build().unwrap();

    shards.put_disk_table_by_level(1, reader);

    let disk_table_path = tmp_dir.path().join("segment_2_1.bin");
    let index_table_path = tmp_dir.path().join("segment_2_1.idx");

    let mut builder = DiskTableBuilder::new(disk_table_path, index_table_path);

    for i in 16..32u32 {
        let k = i as u32;
        let v = vec![i as u8; value_len];
        builder.append_entry(&FlexibleUserEntry::new(
            FlexibleField::new(k.to_be_bytes()),
            FlexibleField::new(v),
        ));
    }

    let reader = builder.build().unwrap();

    shards.put_disk_table_by_level(1, reader);

    let disk_table_path = tmp_dir.path().join("segment_3_1.bin");
    let index_table_path = tmp_dir.path().join("segment_3_1.idx");

    let mut builder = DiskTableBuilder::new(disk_table_path, index_table_path);

    for i in 32..64u32 {
        let k = i as u32;
        let v = vec![i as u8; value_len];
        builder.append_entry(&FlexibleUserEntry::new(
            FlexibleField::new(k.to_be_bytes()),
            FlexibleField::new(v),
        ));
    }

    let reader = builder.build().unwrap();

    shards.put_disk_table_by_level(1, reader);

    let disk_table_path = tmp_dir.path().join("segment_4_2.bin");
    let index_table_path = tmp_dir.path().join("segment_4_2.idx");

    let reader = shards.merge_level(1, disk_table_path.as_path(), index_table_path.as_path());

    for index in 0..64u32 {
        let r = reader.read_block(index as usize);
        assert!(r.is_some());
        let block = r.unwrap();

        let result = block.get_by_index(0).clone();
        let expected = FlexibleUserEntry::new(
            FlexibleField::new(index.to_be_bytes()),
            FlexibleField::new(vec![index as u8; value_len]),
        );

        assert_eq!(result, expected);
    }

    Ok(())
}
