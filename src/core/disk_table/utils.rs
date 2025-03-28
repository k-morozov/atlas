use std::fs;
use std::path::Path;

use crate::errors::Result;

use super::disk_table::get_disk_table_path;
use super::disk_tables_shard::DiskTablesShards;
use super::local::disk_table_builder::DiskTableBuilder;

fn extract_level(disk_table: &str) -> Option<u8> {
    // segment_123_4.bin

    let sg_pos = disk_table.find('_')?;
    let op_prefix = disk_table[sg_pos + 1..].find('_')? + sg_pos + 1;
    let extension_index = disk_table.rfind(".bin")?;
    let level = &disk_table[op_prefix + 1..extension_index];

    level.parse::<u8>().ok()
}

pub fn get_disk_tables(storage_path: &Path) -> Result<DiskTablesShards> {
    let segment_dir = format!("{}/segment", storage_path.to_str().unwrap());

    let shards = fs::read_dir(segment_dir)?
        .filter_map(|entry| {
            let result = match entry {
                Ok(entry) => {
                    let pb = entry.path();
                    let ext = pb.extension().unwrap().to_str().unwrap();
                    if ext == "idx" {
                        return None;
                    }
                    let disk_table_name = pb.file_name().unwrap().to_str().unwrap();

                    let result = match extract_level(disk_table_name) {
                        Some(level) => {
                            let idx_file_path = pb.with_extension("idx");
                            assert!(idx_file_path.exists());

                            let index_table_name =
                                idx_file_path.file_name().unwrap().to_str().unwrap();

                            let (disk_table_path, index_table_path) = get_disk_table_path(
                                storage_path,
                                &disk_table_name,
                                &index_table_name,
                            );

                            // @todo
                            let reader_disk_table =
                                DiskTableBuilder::from(disk_table_path, index_table_path)
                                    .build()
                                    .unwrap();
                            Some((level, reader_disk_table))
                        }
                        None => panic!("failed parse disk table name ={}.", disk_table_name),
                    };
                    Some(result)
                }
                Err(er) => {
                    panic!("get_disk_tables failed with error={}", er);
                }
            };
            result
        })
        // Sorting in shard level for every push isn't good idea. However, it is very fast implemention.
        // Assume here we could accumalate all disk tables for sorting.
        .fold(DiskTablesShards::new(), |table, res| {
            if let Some((level, reader_disk_table)) = res {
                table.put_disk_table_by_level(level, reader_disk_table);
            }
            table
        });

    Ok(shards)
}
