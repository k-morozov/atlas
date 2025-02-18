use std::path::Path;

use super::reader_segment::{ReaderFlexibleSegment, ReaderFlexibleSegmentPtr};
use super::writer_segment::{WriterFlexibleSegment, WriterFlexibleSegmentPtr};
use crate::core::entry::flexible_entry::FlexibleEntry;

pub struct FlexibleSegmentBuilder {
    segment: Option<WriterFlexibleSegmentPtr>,
}

impl FlexibleSegmentBuilder {
    pub fn new<P: AsRef<Path>>(segment_path: P) -> Self {
        FlexibleSegmentBuilder {
            segment: Some(WriterFlexibleSegment::new(segment_path)),
        }
    }

    pub fn append_entry(&mut self, entry: &FlexibleEntry) -> &mut Self {
        match &mut self.segment {
            Some(ptr) => {
                if let Err(_er) = ptr.write(entry.clone()) {
                    panic!("Failed write in builder")
                }
                self
            }
            None => panic!("Failed write entry to None"),
        }
    }

    pub fn build(&mut self) -> ReaderFlexibleSegmentPtr {
        let building_segment = self.segment.take();

        match building_segment {
            Some(mut writer) => {
                if let Err(er) = writer.flush() {
                    panic!("Failed flush in builder: {}", er)
                }

                ReaderFlexibleSegment::new(writer.get_path())
            }
            None => panic!("Failed build from None"),
        }
    }
}
