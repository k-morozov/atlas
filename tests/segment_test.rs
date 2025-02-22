use std::fs;
use std::io;
use std::path::Path;

use kvs::core::disk_table::disk_table::get_disk_table_name;
use kvs::core::disk_table::id::DiskTableID;
use kvs::core::disk_table::local::disk_table_builder::DiskTableBuilder;
use kvs::core::entry::flexible_entry::FlexibleEntry;
use kvs::core::field::FlexibleField;

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
            DiskTableBuilder::new(disk_table_path),
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
