use std::io;

use tempfile::Builder;

use kvs::core::{
    entry::flexible_user_entry::FlexibleUserEntry,
    field::{Field, FlexibleField},
    storage::{
        config::{StorageConfig, DEFAULT_TEST_TABLES_PATH},
        ordered_storage::OrderedStorage,
        storage::Storage,
    },
};

#[test]
fn test_1_block() -> io::Result<()> {
    let tmp_dir = Builder::new().prefix(DEFAULT_TEST_TABLES_PATH).tempdir()?;
    let table_path = tmp_dir.path().join("test_1_block");

    let mut config = StorageConfig::default_config();

    config.mem_table_size = 32;
    config.data_block_size = 32;

    let count = config.data_block_size;

    {
        let table = OrderedStorage::new(table_path.as_path(), config.clone());

        for index in 0..count {
            let entry = FlexibleUserEntry::new(
                FlexibleField::new(index.to_be_bytes()),
                FlexibleField::new((index * 117).to_be_bytes()),
            );
            table.put(&entry).unwrap();
        }
    }

    let table = OrderedStorage::new(table_path.as_path(), config.clone());

    for index in 0..count {
        let result = table.get(&FlexibleField::new(index.to_be_bytes())).unwrap();
        assert_eq!(
            result.unwrap(),
            FlexibleField::new((index * 117).to_be_bytes())
        );
    }

    Ok(())
}

#[test]
fn test_2_blocks() -> io::Result<()> {
    let tmp_dir = Builder::new().prefix(DEFAULT_TEST_TABLES_PATH).tempdir()?;
    let table_path = tmp_dir.path().join("test_2_blocks");

    let mut config = StorageConfig::default_config();

    config.mem_table_size = 32;
    config.data_block_size = 32;

    let count = 2 * config.data_block_size;

    {
        let table = OrderedStorage::new(table_path.as_path(), config.clone());

        for index in 0..count {
            let entry = FlexibleUserEntry::new(
                FlexibleField::new(index.to_be_bytes()),
                FlexibleField::new((index * 117).to_be_bytes()),
            );
            table.put(&entry).unwrap();
        }
    }

    let table = OrderedStorage::new(table_path.as_path(), config.clone());

    for index in 0..count {
        let result = table.get(&FlexibleField::new(index.to_be_bytes())).unwrap();
        assert_eq!(
            result.unwrap(),
            FlexibleField::new((index * 117).to_be_bytes())
        );
    }

    Ok(())
}

#[test]
fn test_3_blocks() -> io::Result<()> {
    let tmp_dir = Builder::new().prefix(DEFAULT_TEST_TABLES_PATH).tempdir()?;
    let table_path = tmp_dir.path().join("test_3_blocks");

    let mut config = StorageConfig::default_config();

    config.mem_table_size = 32;
    config.data_block_size = 32;

    let count = 3 * config.data_block_size;

    {
        let table = OrderedStorage::new(table_path.as_path(), config.clone());

        for index in 0..count {
            let entry = FlexibleUserEntry::new(
                FlexibleField::new(index.to_be_bytes()),
                FlexibleField::new((index * 117).to_be_bytes()),
            );
            table.put(&entry).unwrap();
        }
    }

    let table = OrderedStorage::new(table_path.as_path(), config.clone());

    for index in 0..count {
        let result = table.get(&FlexibleField::new(index.to_be_bytes())).unwrap();
        assert_eq!(
            result.unwrap(),
            FlexibleField::new((index * 117).to_be_bytes())
        );
    }

    Ok(())
}

#[test]
fn test_4_blocks() -> io::Result<()> {
    let tmp_dir = Builder::new().prefix(DEFAULT_TEST_TABLES_PATH).tempdir()?;
    let table_path = tmp_dir.path().join("test_4_blocks");

    let mut config = StorageConfig::default_config();

    config.mem_table_size = 32;
    config.data_block_size = 32;

    let count = 4 * config.data_block_size;

    {
        let table = OrderedStorage::new(table_path.as_path(), config.clone());

        for index in 0..count {
            let entry = FlexibleUserEntry::new(
                FlexibleField::new(index.to_be_bytes()),
                FlexibleField::new((index * 117).to_be_bytes()),
            );
            table.put(&entry).unwrap();
        }
    }

    let table = OrderedStorage::new(table_path.as_path(), config.clone());

    for index in 0..count {
        let result = table.get(&FlexibleField::new(index.to_be_bytes())).unwrap();
        assert_eq!(
            result.unwrap(),
            FlexibleField::new((index * 117).to_be_bytes())
        );
    }

    Ok(())
}

#[test]
fn test_5_blocks() -> io::Result<()> {
    let tmp_dir = Builder::new().prefix(DEFAULT_TEST_TABLES_PATH).tempdir()?;
    let table_path = tmp_dir.path().join("test_5_blocks");

    let mut config = StorageConfig::default_config();

    config.mem_table_size = 32;
    config.data_block_size = 32;

    let count = 5 * config.data_block_size;

    {
        let table = OrderedStorage::new(table_path.as_path(), config.clone());

        for index in 0..count {
            let entry = FlexibleUserEntry::new(
                FlexibleField::new(index.to_be_bytes()),
                FlexibleField::new((index * 117).to_be_bytes()),
            );
            table.put(&entry).unwrap();
        }
    }

    let table = OrderedStorage::new(table_path.as_path(), config.clone());

    for index in 0..count {
        let result = table.get(&FlexibleField::new(index.to_be_bytes())).unwrap();
        assert_eq!(
            result.unwrap(),
            FlexibleField::new((index * 117).to_be_bytes())
        );
    }

    Ok(())
}
