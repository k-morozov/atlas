use std::io;

use kvs::core::{
    disk_table::local::disk_table_builder::DiskTableBuilder,
    entry::flexible_user_entry::FlexibleUserEntry,
    field::{Field, FlexibleField},
    storage::config::DEFAULT_TEST_TABLES_PATH,
};
use tempfile::Builder;

#[test]
fn test_data_blocks() -> io::Result<()> {
    let tmp_dir = Builder::new().prefix(DEFAULT_TEST_TABLES_PATH).tempdir()?;

    let disk_table_path = tmp_dir.path().join("segment_001.bin");
    let index_table_path = tmp_dir.path().join("segment_001.idx");

    let mut builder = DiskTableBuilder::new(disk_table_path, index_table_path);
    let value_len = 4000;

    for i in 0..16u32 {
        let k = i as u32;
        let v = vec![i as u8; value_len];
        builder.append_entry(&FlexibleUserEntry::new(
            FlexibleField::new(k.to_be_bytes()),
            FlexibleField::new(v),
        ));
    }

    let reader = builder.build().unwrap();

    for index in 0..16u32 {
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
