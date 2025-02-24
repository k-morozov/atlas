pub const DEFAULT_TABLES_PATH: &'static str = "/tmp/kvs/tables/";
pub const DEFAULT_TEST_TABLES_PATH: &'static str = "/tmp/";
pub const DETAULT_MEM_TABLE_SIZE: usize = 4;
pub const DEFAULT_DISK_TABLES_LIMIT_BY_LEVEL: usize = 4;

pub const DEFAULT_DATA_BLOCK_SIZE: usize = 256 * 2 << 10;

#[derive(Clone)]
pub struct StorageConfig {
    pub mem_table_size: usize,
    pub disk_tables_limit_by_level: usize,
    pub data_block_size: usize,
}

impl StorageConfig {
    pub fn new_config(
        mem_table_size: usize,
        disk_tables_limit_by_level: usize,
        data_block_size: usize,
    ) -> Self {
        StorageConfig {
            mem_table_size,
            disk_tables_limit_by_level,
            data_block_size,
        }
    }

    pub fn default_config() -> Self {
        StorageConfig {
            mem_table_size: DETAULT_MEM_TABLE_SIZE,
            disk_tables_limit_by_level: DEFAULT_DISK_TABLES_LIMIT_BY_LEVEL,
            data_block_size: DEFAULT_DATA_BLOCK_SIZE,
        }
    }
}
