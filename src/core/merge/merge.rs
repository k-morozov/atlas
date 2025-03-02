use crate::core::disk_table::utils;
use crate::core::storage::config;

pub fn is_ready_to_merge(table: &utils::LevelsReaderDiskTables, level: utils::Levels) -> bool {
    if !table.contains_key(&level) {
        return false;
    }
    table[&level].len() == config::DEFAULT_DISK_TABLES_LIMIT_BY_LEVEL
}
