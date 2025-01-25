use std::fs::{File, OpenOptions};
use std::io::copy;

use crate::core::segment::segment;

pub fn merge(dst: &mut segment::Segment, srcs: &segment::Segments) {
    let dst_path = segment::Segment::get_path(dst.get_table_path(), dst.get_name());

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
        let src_path = segment::Segment::get_path(src.get_table_path(), src.get_name());
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
        let src_path = segment::Segment::get_path(src.get_table_path(), src.get_name());
        match std::fs::remove_file(src_path) {
            Ok(_n) => {}
            Err(er) => panic!(
                "failed remove merged segment: error={}, src={}",
                er,
                src.get_name()
            ),
        }
    }
}
