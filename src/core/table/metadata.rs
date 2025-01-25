use std::fs::exists;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::core::segment::id::SegmentID;

pub struct TableMetadata {
    pub segment_id: SegmentID,
    metadata_path: PathBuf,
}

impl TableMetadata {
    pub fn new(table_path: &Path) -> Self {
        TableMetadata {
            segment_id: SegmentID::new(),
            metadata_path: TableMetadata::make_path(table_path),
        }
    }

    pub fn get_metadata_path(&self) -> &Path {
        &self.metadata_path
    }

    pub fn make_path(table_path: &Path) -> PathBuf {
        let mut metadata_path = table_path.to_path_buf();
        metadata_path.push("metadata");

        metadata_path
    }

    pub fn from_file(metadata_path: &Path) -> Self {
        let mut metadata = TableMetadata {
            segment_id: SegmentID::new(),
            metadata_path: metadata_path.to_path_buf(),
        };

        match exists(metadata_path) {
            Ok(metadata_exists) => {
                if metadata_exists {
                    match File::open(metadata_path) {
                        Ok(mut fd) => {
                            let mut data = String::new();
                            let _r = fd.read_to_string(&mut data);
                            match data.parse::<u64>() {
                                Ok(id) => metadata.segment_id = SegmentID::from(id),
                                Err(_) => {
                                    panic!(
                                        "broken metadata: {}, path={}",
                                        data,
                                        metadata_path.display()
                                    );
                                }
                            }
                        }
                        Err(er) => {
                            panic!(
                                "Failed to open table metadata. metadata_path={}, error= {}",
                                metadata_path.display(),
                                er
                            );
                        }
                    }
                } else {
                    let result_create = File::create(metadata_path);
                    if let Err(er) = result_create {
                        panic!(
                            "Failed to create table metadata. metadata_path={}, error= {}",
                            metadata_path.display(),
                            er
                        );
                    };
                }
            }
            Err(_) => {}
        };

        metadata
    }

    pub fn sync_disk(&self, metadata_path: &Path) {
        let mut options: OpenOptions = OpenOptions::new();
        options.write(true).create(true);

        match options.open(metadata_path) {
            Ok(mut fd) => {
                let segment_id = self.segment_id.get_id().to_string();
                if let Err(er) = fd.write_all(segment_id.as_bytes()) {
                    panic!(
                        "Failed to write segment_id to table metadata. segment_id={}, metadata_path={}, error= {}",
                        segment_id,
                        metadata_path.display(),
                        er
                    );
                }
            }
            Err(er) => {
                panic!(
                    "Failed to open table metadata for update. metadata_path={}, error= {}",
                    metadata_path.display(),
                    er
                );
            }
        }
    }
}
