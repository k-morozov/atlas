use std::path::Path;

use super::flexible_segment::{FlexibleSegment, FlexibleSegmentPtr};
use crate::core::entry::flexible_entry::FlexibleEntry;

pub struct FlexibleSegmentBuilder {
    building_segment: Option<FlexibleSegmentPtr>,
}

impl FlexibleSegmentBuilder {
    pub fn new<P: AsRef<Path>>(segment_path: P) -> Self {
        FlexibleSegmentBuilder {
            building_segment: Some(FlexibleSegment::new(segment_path)),
        }
    }

    pub fn append_entry(&mut self, entry: &FlexibleEntry) -> &mut Self {
        match &mut self.building_segment {
            Some(ptr) => {
                if let Err(_er) = ptr.write(entry.clone()) {
                    panic!("Failed write in builder")
                }
                self
            }
            None => panic!("Failed write entry to None"),
        }
    }

    pub fn build(&mut self) -> FlexibleSegmentPtr {
        let building_segment = self.building_segment.take();

        match building_segment {
            Some(mut ptr) => {
                if let Err(er) = ptr.flush() {
                    panic!("Failed flush in builder: {}", er)
                }
                ptr
            }
            None => panic!("Failed build from None"),
        }
    }
}
