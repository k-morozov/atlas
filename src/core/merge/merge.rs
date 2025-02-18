use std::path::Path;

use crate::core::disk_table::{
    disk_table::{get_disk_table_name_by_level, get_disk_table_path},
    id::DiskTableID,
    local::segment_builder::FlexibleSegmentBuilder,
};

use crate::core::disk_table::utils::{StorageSegments, SEGMENTS_MAX_LEVEL, SEGMENTS_MIN_LEVEL};
use crate::core::storage::config::DEFAULT_SEGMENTS_LIMIT;

pub fn is_ready_to_merge(table: &StorageSegments) -> bool {
    table[&SEGMENTS_MIN_LEVEL].len() == DEFAULT_SEGMENTS_LIMIT
}

pub fn merge_segments(
    stoarge: &mut StorageSegments,
    stoarge_path: &Path,
    sgm_id: &mut DiskTableID,
) {
    for merging_level in SEGMENTS_MIN_LEVEL..=SEGMENTS_MAX_LEVEL {
        let disk_table_id = sgm_id.get_and_next();

        // @todo
        match stoarge.get(&merging_level) {
            Some(segments_by_level) => {
                if segments_by_level.len() != DEFAULT_SEGMENTS_LIMIT {
                    continue;
                }
            }
            None => continue,
        }

        let level_for_new_sg = if merging_level != SEGMENTS_MAX_LEVEL {
            merging_level + 1
        } else {
            merging_level
        };

        let segment_name = get_disk_table_name_by_level(disk_table_id, level_for_new_sg);
        let disk_table_path = get_disk_table_path(stoarge_path, &segment_name);

        let merging_segments = &stoarge[&merging_level];

        let merged_segment = merging_segments
            .into_iter()
            .fold(
                FlexibleSegmentBuilder::new(disk_table_path.as_path()),
                |mut builder, merging_segment| {
                    for entry in merging_segment.into_iter() {
                        builder.append_entry(&entry);
                    }
                    builder
                },
            )
            .build();

        for merging_segment in stoarge.get_mut(&merging_level).unwrap() {
            match merging_segment.remove() {
                Ok(_) => {}
                Err(er) => panic!(
                    "failed remove merged segment: path={}, error={}",
                    merging_segment.get_path().display(),
                    er,
                ),
            }
        }

        stoarge.get_mut(&merging_level).unwrap().clear();
        stoarge
            .entry(level_for_new_sg)
            .or_insert_with(Vec::new)
            .push(merged_segment);
    }
}
