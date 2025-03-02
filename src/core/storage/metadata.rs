use std::fs::exists;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::core::disk_table::id::DiskTableID;

pub struct StorageMetadata {
    segment_id: DiskTableID,
    metadata_path: PathBuf,
}

impl StorageMetadata {
    pub fn new(table_path: &Path) -> Self {
        StorageMetadata {
            segment_id: DiskTableID::new(),
            metadata_path: StorageMetadata::make_path(table_path),
        }
    }

    pub fn from_file<P: AsRef<Path> + Copy>(metadata_path: P) -> Self {
        let mut metadata = StorageMetadata {
            segment_id: DiskTableID::new(),
            metadata_path: metadata_path.as_ref().to_path_buf(),
        };

        match exists(metadata_path) {
            Ok(metadata_exists) => {
                if metadata_exists {
                    match File::open(metadata_path) {
                        Ok(mut fd) => {
                            let mut data = String::new();
                            let _r = fd.read_to_string(&mut data);
                            match data.parse::<u64>() {
                                Ok(id) => metadata.segment_id = DiskTableID::from(id),
                                Err(_) => {
                                    panic!(
                                        "broken metadata: {}, path={}",
                                        data,
                                        metadata_path.as_ref().display()
                                    );
                                }
                            }
                        }
                        Err(er) => {
                            panic!(
                                "Failed to open table metadata. metadata_path={}, error= {}",
                                metadata_path.as_ref().display(),
                                er
                            );
                        }
                    }
                } else {
                    let result_create = File::create(metadata_path);
                    if let Err(er) = result_create {
                        panic!(
                            "Failed to create table metadata. metadata_path={}, error= {}",
                            metadata_path.as_ref().display(),
                            er
                        );
                    };
                }
            }
            Err(_) => {}
        };

        metadata
    }

    fn get_metadata_path(&self) -> &Path {
        &self.metadata_path
    }

    pub fn make_path<P: AsRef<Path>>(table_path: P) -> PathBuf {
        let mut metadata_path = table_path.as_ref().to_path_buf();
        metadata_path.push("metadata");

        metadata_path
    }

    pub fn get_new_disk_table_id(&self) -> DiskTableID {
        self.segment_id.get_and_next()
    }

    pub fn sync_disk(&self) {
        let mut options: OpenOptions = OpenOptions::new();
        options.write(true).create(true);

        match options.open(self.get_metadata_path()) {
            Ok(mut fd) => {
                let segment_id = self.segment_id.get_id().to_string();
                if let Err(er) = fd.write_all(segment_id.as_bytes()) {
                    panic!(
                        "Failed to write segment_id to table metadata. segment_id={}, metadata_path={}, error= {}",
                        segment_id,
                        self.get_metadata_path().display(),
                        er
                    );
                }
                fd.sync_all().unwrap();
            }
            Err(er) => {
                panic!(
                    "Failed to open table metadata for update. metadata_path={}, error= {}",
                    self.get_metadata_path().display(),
                    er
                );
            }
        }
    }
}
