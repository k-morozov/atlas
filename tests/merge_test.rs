use std::io;

use kvs::core::{
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
