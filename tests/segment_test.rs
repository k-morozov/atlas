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

    const MAX_SIZE: u32 = 6;

    for index in (1..MAX_SIZE).step_by(1) {
        entries.push(FlexibleEntry::new(
            FlexibleField::new(index.to_le_bytes().to_vec()),
            FlexibleField::new((index + 10).to_le_bytes().to_vec()),
        ));
    }
    let mut writer = FlexibleWriter::new(path, entries.iter());
    let result = writer.write_entries();

    assert!(result.is_ok());

    for index in (1..MAX_SIZE).step_by(1) {
        let reader = FlexibleReader::new(path);
        let actual = reader
            .read(&FlexibleField::new(index.to_le_bytes().to_vec()))
            .unwrap()
            .unwrap();
        let expected = FlexibleField::new((index + 10).to_le_bytes().to_vec());
        assert_eq!(actual, expected);
    }
}

#[test]
fn diffrent_types() {
    let path = Path::new("/tmp/kvs/test/flex_segment_diffrent_type/part1.bin");

    if let Some(parent) = path.parent() {
        create_dir_all(parent).unwrap();
    }

    if let Err(er) = remove_file(path) {
        assert_eq!(ErrorKind::NotFound, er.kind());
    }

    let mut entries: Vec<FlexibleEntry> = Vec::new();

    for index in (1..6u32).step_by(1) {
        let value = format!("{}", index);

        entries.push(FlexibleEntry::new(
            FlexibleField::new(index.to_le_bytes().to_vec()),
            FlexibleField::new(value.as_bytes().to_vec()),
        ));
    }

    for index in (7..15u32).step_by(1) {
        let key = format!("{}-some-key", index);
        let value = index % 2 == 0;

        entries.push(FlexibleEntry::new(
            FlexibleField::new(key.as_bytes().to_vec()),
            FlexibleField::new(value.to_string().as_bytes().to_vec()),
        ));
    }

    let mut writer = FlexibleWriter::new(path, entries.iter());
    let result = writer.write_entries();

    assert!(result.is_ok());

    for index in (1..6u32).step_by(1) {
        let reader = FlexibleReader::new(path);
        let actual = reader
            .read(&FlexibleField::new(index.to_le_bytes().to_vec()))
            .unwrap()
            .unwrap();
        let value = format!("{}", index);
        let expected = FlexibleField::new(value.as_bytes().to_vec());
        assert_eq!(actual, expected);
    }

    for index in (7..15u32).step_by(1) {
        let reader = FlexibleReader::new(path);

        let key = format!("{}-some-key", index);
        let actual = reader
            .read(&FlexibleField::new(key.as_bytes().to_vec()))
            .unwrap()
            .unwrap();

        let value = index % 2 == 0;
        let expected = FlexibleField::new(value.to_string().as_bytes().to_vec());
        assert_eq!(actual, expected);
    }

    for index in (15..20u32).step_by(1) {
        let reader = FlexibleReader::new(path);

        let key = format!("{}-some-key", index);
        let actual = reader
            .read(&FlexibleField::new(key.as_bytes().to_vec()))
            .unwrap();

        assert_eq!(actual, None);
    }
}
