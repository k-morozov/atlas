use std::fs::{File, OpenOptions};
use std::io::copy;
use std::path::Path;
use std::rc::Rc;

use crate::core::segment::{
    id::SegmentID,
    segment::{get_path, Segment},
};

use crate::core::segment::{
    fixed_segment,
    table::{Segments, TableSegments, SEGMENTS_MAX_LEVEL, SEGMENTS_MIN_LEVEL},
};
use crate::core::table::config::DEFAULT_SEGMENTS_LIMIT;

pub fn is_ready_to_merge(table: &TableSegments) -> bool {
    table[&SEGMENTS_MIN_LEVEL].len() == DEFAULT_SEGMENTS_LIMIT
}

pub fn merge_segments(
    table: &mut TableSegments,
    table_path: &Path,
    sgm_id: &mut SegmentID,
    schema: Rc<crate::core::schema::Schema>,
) {
    for merged_level in SEGMENTS_MIN_LEVEL..=SEGMENTS_MAX_LEVEL {
        let level_for_new_sg = if merged_level != SEGMENTS_MAX_LEVEL {
            merged_level + 1
        } else {
            merged_level
        };
        match fixed_segment::FixedSegment::for_merge(
            table_path,
            sgm_id,
            schema.clone(),
            level_for_new_sg,
        ) {
            Ok(mut merged_sg) => {
                merge_impl(&mut merged_sg, &table[&merged_level]);
                table.get_mut(&merged_level).unwrap().clear();
                table
                    .entry(level_for_new_sg)
                    .or_insert_with(Vec::new)
                    .push(merged_sg);
            }
            Err(_) => panic!("Failed create segment for merge"),
        }
    }
}

fn merge_impl(dst: &mut fixed_segment::FixedSegmentPtr, srcs: &Segments) {
    let dst_path = get_path(dst.get_table_path(), dst.get_name());

    let mut options: OpenOptions = OpenOptions::new();
    options.write(true).create(true);

    let mut dst_fd = match options.open(dst_path.as_path()) {
        Ok(fd) => fd,
        Err(er) => panic!(
            "merge: open dst error={}, path={}",
            er,
            dst_path.as_path().display()
        ),
    };

    for src in srcs {
        let src_path = get_path(src.get_table_path(), src.get_name());
        let mut src_fd: File = match File::open(src_path.as_path()) {
            Ok(fd) => fd,
            Err(er) => panic!("merge: error={}, path={}", er, dst_path.as_path().display()),
        };

        match copy(&mut src_fd, &mut dst_fd) {
            Ok(_n) => {}
            Err(er) => panic!(
                "failed copy: error={}, dst={}, src={}",
                er,
                dst.get_name(),
                src.get_name()
            ),
        }
    }

    for src in srcs {
        let src_path = get_path(src.get_table_path(), src.get_name());
        match std::fs::remove_file(src_path) {
            Ok(_) => {}
            Err(er) => panic!(
                "failed remove merged segment: error={}, src={}",
                er,
                src.get_name()
            ),
        }
    }
}
