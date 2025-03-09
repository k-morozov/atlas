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

pub fn get_disk_tables(table_path: &Path) -> Result<DiskTablesShards> {
    let segment_dir = format!("{}/segment", table_path.to_str().unwrap());

    let shards = fs::read_dir(segment_dir)?
        .map(|entry| {
            let result = match entry {
                Ok(entry) => {
                    let pb = entry.path();
                    let disk_table_name = pb.file_name().unwrap().to_str().unwrap();

                    let result = match extract_level(disk_table_name) {
                        Some(level) => {
                            let disk_table_path = get_disk_table_path(table_path, &disk_table_name);

                            // @todo
                            let disk_table =
                                DiskTableBuilder::from(disk_table_path).build().unwrap();
                            (level, disk_table)
                        }
                        None => panic!("failed parse disk table name ={}.", disk_table_name),
                    };
                    result
                }
                Err(er) => {
                    panic!("get_disk_tables failed with error={}", er);
                }
            };
            result
        })
        .fold(DiskTablesShards::new(), |table, (level, disk_table)| {
            table.put_disk_table_by_level(level, disk_table);
            table
        });

    Ok(shards)
}
