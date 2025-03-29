use std::io;

use kvs::core::{
    entry::flexible_user_entry::FlexibleUserEntry,
    field::{Field, FlexibleField},
    storage::{config::StorageConfig, ordered_storage::OrderedStorage, storage::Storage},
};

#[test]
fn test_override_entry_on_level() -> io::Result<()> {
    let table_name = "/tmp/kvs/test/test_override_entry_on_level";
    let config = StorageConfig::default_config();

    let key = FlexibleField::new([1, 1, 1, 1]);

    let value = FlexibleField::new([2, 2, 2, 2]);
    let entry1 = FlexibleUserEntry::new(key.clone(), value);

    let value = FlexibleField::new([4, 4, 4, 4]);
    let entry2 = FlexibleUserEntry::new(key.clone(), value);

    {
        let table = OrderedStorage::new(table_name, config.clone());
        table.put(&entry1).unwrap();
    }

    {
        let table = OrderedStorage::new(table_name, config.clone());
        table.put(&entry2).unwrap();
    }

    {
        let table = OrderedStorage::new(table_name, config.clone());
        let result = table.get(&key).unwrap().expect("value was inserted");

        assert_eq!(result, *entry2.get_value());
    }

    let value = FlexibleField::new([8, 8, 8, 8]);
    let entry3 = FlexibleUserEntry::new(key.clone(), value);

    {
        let table = OrderedStorage::new(table_name, config.clone());
        table.put(&entry3).unwrap();
    }

    {
        let table = OrderedStorage::new(table_name, config.clone());
        let result = table.get(&key).unwrap().expect("value was inserted");

        assert_eq!(result, *entry3.get_value());
    }

    Ok(())
}

#[test]
fn test_override_entry_after_merge() -> io::Result<()> {
    let table_name = "/tmp/kvs/test/test_override_entry_after_merge";
    let mut config = StorageConfig::default_config();
    config.disk_tables_limit_by_level = 3;

    let key = FlexibleField::new([1, 1, 1, 1]);

    let value = FlexibleField::new([2, 2, 2, 2]);
    let entry1 = FlexibleUserEntry::new(key.clone(), value);

    let value = FlexibleField::new([4, 4, 4, 4]);
    let entry2 = FlexibleUserEntry::new(key.clone(), value);

    let value = FlexibleField::new([8, 8, 8, 8]);
    let entry3 = FlexibleUserEntry::new(key.clone(), value);

    {
        let table = OrderedStorage::new(table_name, config.clone());
        table.put(&entry1).unwrap();
    }

    {
        let table = OrderedStorage::new(table_name, config.clone());
        table.put(&entry2).unwrap();
    }

    {
        let table = OrderedStorage::new(table_name, config.clone());
        table.put(&entry3).unwrap();
    }

    {
        let table = OrderedStorage::new(table_name, config.clone());
        let result = table.get(&key).unwrap().expect("value was inserted");

        assert_eq!(result, *entry3.get_value());
    }

    Ok(())
}
