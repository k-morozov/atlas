use crate::row::Row;
use std::iter::Iterator;

struct SegmentWriter {
    path_to_segment: String,
    row_it: Box<dyn Iterator<Item = Row>>,
}

impl SegmentWriter {
    pub fn new<T: Iterator<Item = Row> + 'static>(path_to_segment: String, row_it: T) -> Self {
        Self {
            row_it: Box::new(row_it),
            path_to_segment,
        }
    }
}
