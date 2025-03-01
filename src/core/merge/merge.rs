use crate::core::disk_table::utils::{LevelsReaderDiskTables, SEGMENTS_MIN_LEVEL};
use crate::core::storage::config;

pub fn is_ready_to_merge(table: &LevelsReaderDiskTables) -> bool {
    table[&SEGMENTS_MIN_LEVEL].len() == config::DEFAULT_DISK_TABLES_LIMIT_BY_LEVEL
}
