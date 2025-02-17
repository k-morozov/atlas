use std::path::Path;

use crate::core::segment::{
    id::SegmentID,
    segment::{get_segment_name_by_level, get_segment_path},
    segment_builder::FlexibleSegmentBuilder,
};

use crate::core::segment::utils::{TableSegments, SEGMENTS_MAX_LEVEL, SEGMENTS_MIN_LEVEL};
use crate::core::table::config::DEFAULT_SEGMENTS_LIMIT;

pub fn is_ready_to_merge(table: &TableSegments) -> bool {
    table[&SEGMENTS_MIN_LEVEL].len() == DEFAULT_SEGMENTS_LIMIT
}

pub fn merge_segments(table: &mut TableSegments, table_path: &Path, sgm_id: &mut SegmentID) {
    for merging_level in SEGMENTS_MIN_LEVEL..=SEGMENTS_MAX_LEVEL {
        let segment_id = sgm_id.get_and_next();

        // @todo
        match table.get(&merging_level) {
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

        let segment_name = get_segment_name_by_level(segment_id, level_for_new_sg);
        let segment_path = get_segment_path(table_path, &segment_name);

        let merging_segments = &table[&merging_level];

        let merged_segment = merging_segments
            .into_iter()
            .fold(
                FlexibleSegmentBuilder::new(segment_path.as_path()),
                |mut builder, merging_segment| {
                    for entry in merging_segment.into_iter() {
                        builder.append_entry(&entry);
                    }
                    builder
                },
            )
            .build();

        for merging_segment in table.get_mut(&merging_level).unwrap() {
            match merging_segment.remove() {
                Ok(_) => {}
                Err(er) => panic!(
                    "failed remove merged segment: path={}, error={}",
                    merging_segment.get_path().display(),
                    er,
                ),
            }
        }

        table.get_mut(&merging_level).unwrap().clear();
        table
            .entry(level_for_new_sg)
            .or_insert_with(Vec::new)
            .push(merged_segment);
    }
}
