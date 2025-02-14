use std::fs;
use std::io;
use std::path::Path;

use kvs::core::table::config::DEFAULT_TEST_TABLES_PATH;
use tempfile::Builder;

use kvs::core::entry::flexible_entry::FlexibleEntry;
use kvs::core::field::FlexibleField;
use kvs::core::segment::segment::get_segment_name;
use kvs::core::segment::{
    flexible_reader::FlexibleReader, flexible_writer::FlexibleWriter,
    segment_builder::FlexibleSegmentBuilder,
};

#[test]
fn simple() -> io::Result<()> {
    let path_to_segment = Builder::new().prefix("/tmp/part1.bin").tempfile()?;

    let mut entries: Vec<FlexibleEntry> = Vec::new();

    const MAX_SIZE: u32 = 6;

    for index in (1..MAX_SIZE).step_by(1) {
        entries.push(FlexibleEntry::new(
            FlexibleField::new(index.to_le_bytes().to_vec()),
            FlexibleField::new((index + 10).to_le_bytes().to_vec()),
        ));
    }

    let mut writer = FlexibleWriter::new(&path_to_segment);
    for entry in entries {
        let result = writer.write_entry(&entry);
        assert!(result.is_ok());
    }
    writer.flush().unwrap();

    for index in (1..MAX_SIZE).step_by(1) {
        let reader = FlexibleReader::new(&path_to_segment);
        let actual = reader
            .read(&FlexibleField::new(index.to_le_bytes().to_vec()))
            .unwrap()
            .unwrap();
        let expected = FlexibleField::new((index + 10).to_le_bytes().to_vec());
        assert_eq!(actual, expected);
    }

    Ok(())
}

#[test]
fn diffrent_types() -> io::Result<()> {
    let path_to_segment = Builder::new().prefix("/tmp/part1.bin").tempfile()?;

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

    let mut writer = FlexibleWriter::new(&path_to_segment);
    for entry in entries {
        let result = writer.write_entry(&entry);
        assert!(result.is_ok());
    }
    writer.flush().unwrap();

    for index in (1..6u32).step_by(1) {
        let reader = FlexibleReader::new(&path_to_segment);
        let actual = reader
            .read(&FlexibleField::new(index.to_le_bytes().to_vec()))
            .unwrap()
            .unwrap();
        let value = format!("{}", index);
        let expected = FlexibleField::new(value.as_bytes().to_vec());
        assert_eq!(actual, expected);
    }

    for index in (7..15u32).step_by(1) {
        let reader = FlexibleReader::new(&path_to_segment);

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
        let reader = FlexibleReader::new(&path_to_segment);

        let key = format!("{}-some-key", index);
        let actual = reader
            .read(&FlexibleField::new(key.as_bytes().to_vec()))
            .unwrap();

        assert_eq!(actual, None);
    }

    Ok(())
}

#[test]
fn simple_flexible_segment() -> io::Result<()> {
    let table_path = Path::new("/tmp/kvs/test/simple_flexible_segment");
    let segment_name = get_segment_name(12);
    let segment_path = format!(
        "/tmp/kvs/test/simple_flexible_segment/segment/{}",
        segment_name
    );
    let segment_path: &Path = Path::new(&segment_path);

    if let Some(parent) = segment_path.parent() {
        fs::create_dir_all(parent).unwrap();
    }

    if let Err(er) = fs::remove_file(segment_path) {
        assert_eq!(io::ErrorKind::NotFound, er.kind());
    }

    let mut entries: Vec<FlexibleEntry> = Vec::new();

    const MAX_SIZE: u32 = 6;

    for index in (1..MAX_SIZE).step_by(1) {
        entries.push(FlexibleEntry::new(
            FlexibleField::new(index.to_le_bytes().to_vec()),
            FlexibleField::new((index + 10).to_le_bytes().to_vec()),
        ));
    }

    let segment = entries
        .into_iter()
        .fold(
            FlexibleSegmentBuilder::new(&segment_path)
                .set_segment_name(segment_name.as_str())
                .set_table_path(table_path)
                .prepare_empty_segment(),
            |builder, entry| builder.append_entry(&entry),
        )
        .build();

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
