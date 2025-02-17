use std::fs;
use std::io;
use std::path::Path;

use kvs::core::entry::flexible_entry::FlexibleEntry;
use kvs::core::field::FlexibleField;
use kvs::core::segment::segment::get_segment_name;
use kvs::core::segment::segment_builder::FlexibleSegmentBuilder;

#[test]
fn simple_flexible_segment() -> io::Result<()> {
    // let table_path = Path::new("/tmp/kvs/test/simple_flexible_segment");
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
            FlexibleSegmentBuilder::new(segment_path),
            |mut builder, entry| {
                builder.append_entry(&entry);
                builder
            },
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
