use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use crate::core::segment::flexible_segment::{FlexibleSegment, FlexibleSegmentPtr};
use crate::errors::Error;

use super::segment::get_segment_path;

pub type Levels = u8;
pub type Segments = Vec<FlexibleSegmentPtr>;
pub type TableSegments = BTreeMap<Levels, Segments>;

pub const SEGMENTS_MIN_LEVEL: Levels = 1;
pub const SEGMENTS_MAX_LEVEL: Levels = 3;

fn extract_level(segment_name: &str) -> Option<u8> {
    // segment_123_4.bin

    let sg_pos = segment_name.find('_')?;

    let op_prefix = segment_name[sg_pos + 1..].find('_')? + sg_pos + 1;

    let extension_index = segment_name.rfind(".bin")?;

    let level = &segment_name[op_prefix + 1..extension_index];
    level.parse::<u8>().ok()
}

pub fn get_table_segments(table_path: &Path) -> Result<TableSegments, Error> {
    let segment_dir = format!("{}/segment", table_path.to_str().unwrap());

    let segments = fs::read_dir(segment_dir)?
        .map(|entry| {
            let result = match entry {
                Ok(entry) => {
                    let pb = entry.path();
                    let segment_name = pb.file_name().unwrap().to_str().unwrap();

                    let result = match extract_level(segment_name) {
                        Some(level) => {
                            let segment_path = get_segment_path(table_path, &segment_name);
                            let sg = FlexibleSegment::from(segment_path);
                            (level, sg)
                        }
                        None => panic!("failed parse segment name ={}.", segment_name),
                    };
                    result
                }
                Err(er) => {
                    panic!("get_segment_names failed with error={}", er);
                }
            };
            result
        })
        .fold(BTreeMap::new(), |mut table, (level, segment)| {
            table.entry(level).or_insert_with(Vec::new).push(segment);
            table
        });

    Ok(segments)
}
