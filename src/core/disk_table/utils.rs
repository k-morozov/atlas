use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use crate::core::disk_table::local::reader_disk_table::ReaderFlexibleDiskTablePtr;
use crate::errors::Error;

use super::disk_table::get_disk_table_path;
use super::local::segment_builder::FlexibleSegmentBuilder;

pub type Levels = u8;
pub type Segments = Vec<ReaderFlexibleDiskTablePtr>;
pub type StorageSegments = BTreeMap<Levels, Segments>;

pub const SEGMENTS_MIN_LEVEL: Levels = 1;
pub const SEGMENTS_MAX_LEVEL: Levels = 3;

fn extract_level(disk_table: &str) -> Option<u8> {
    // segment_123_4.bin

    let sg_pos = disk_table.find('_')?;
    let op_prefix = disk_table[sg_pos + 1..].find('_')? + sg_pos + 1;
    let extension_index = disk_table.rfind(".bin")?;
    let level = &disk_table[op_prefix + 1..extension_index];

    level.parse::<u8>().ok()
}

pub fn get_disk_tables(table_path: &Path) -> Result<StorageSegments, Error> {
    let segment_dir = format!("{}/segment", table_path.to_str().unwrap());

    let disk_tables = fs::read_dir(segment_dir)?
        .map(|entry| {
            let result = match entry {
                Ok(entry) => {
                    let pb = entry.path();
                    let disk_table_name = pb.file_name().unwrap().to_str().unwrap();

                    let result = match extract_level(disk_table_name) {
                        Some(level) => {
                            let disk_table_path = get_disk_table_path(table_path, &disk_table_name);
                            let sg = FlexibleSegmentBuilder::from(disk_table_path).build();
                            (level, sg)
                        }
                        None => panic!("failed parse disk table name ={}.", disk_table_name),
                    };
                    result
                }
                Err(er) => {
                    panic!("get_disk_tables failed with error={}", er);
                }
            };
            result
        })
        .fold(BTreeMap::new(), |mut table, (level, segment)| {
            table.entry(level).or_insert_with(Vec::new).push(segment);
            table
        });

    Ok(disk_tables)
}
