use std::fs::{create_dir_all, remove_file};
use std::io::ErrorKind;
use std::path::Path;

use kvs::core::entry::FlexibleEntry;
use kvs::core::field::FlexibleField;
use kvs::core::segment::{flexible_reader::FlexibleReader, flexible_writer::FlexibleWriter};

#[test]
fn simple() {
    let path = Path::new("/tmp/kvs/test/create_flex_segment/part1.bin");

    if let Some(parent) = path.parent() {
        create_dir_all(parent).unwrap();
    }

    if let Err(er) = remove_file(path) {
        assert_eq!(ErrorKind::NotFound, er.kind());
    }

    let mut entries: Vec<FlexibleEntry> = Vec::new();

    for index in (1..6u32).step_by(1) {
        entries.push(FlexibleEntry::new(
            FlexibleField::new(index.to_le_bytes().to_vec()),
            FlexibleField::new((index + 10).to_le_bytes().to_vec()),
        ));
    }
    let mut writer = FlexibleWriter::new(path, entries.iter());
    let result = writer.write_entries();

    assert!(result.is_ok());

    for index in (1..6u32).step_by(1) {
        let reader = FlexibleReader::new(path);
        let _r = reader
            .read(&FlexibleField::new(index.to_le_bytes().to_vec()))
            .unwrap()
            .unwrap();
    }
}
