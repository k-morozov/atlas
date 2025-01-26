use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use super::segment::Segment;
use crate::core::pg_errors::PgError;

pub type Levels = u8;
pub type Segments = Vec<Segment>;
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

pub fn get_table_segments(table_path: &Path) -> Result<TableSegments, PgError> {
    let segment_dir = format!("{}/segment", table_path.to_str().unwrap());

    let segment_names = fs::read_dir(segment_dir)
        .map_err(|_| PgError::FailedReadSegmentNames)?
        .map(|entry| {
            let result = match entry {
                Ok(entry) => {
                    let pb = entry.path();
                    let segment_name = pb.file_name().unwrap().to_str().unwrap();

                    let result = match extract_level(segment_name) {
                        Some(level) => {
                            let sg = Segment::new(table_path, segment_name);
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
        .fold(BTreeMap::new(), |mut result, (level, segment)| {
            result.entry(level).or_insert_with(Vec::new).push(segment);
            result
        });

    Ok(segment_names)
}
