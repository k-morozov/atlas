pub const DEFAULT_TABLES_PATH: &'static str = "/tmp/kvs/tables/";
pub const DEFAULT_TEST_TABLES_PATH: &'static str = "/tmp/";
pub const DETAULT_MEM_TABLE_SIZE: usize = 4;
pub const DEFAULT_SEGMENTS_LIMIT: usize = 4;

pub const DEFAULT_DISK_BLOCK_SIZE: usize = 4 * 2 << 10;
pub const DEFAULT_DISK_ERASE_BLOCK_SIZE: usize = 256 * 2 << 10;

#[derive(Clone)]
pub struct StorageConfig {
    pub mem_table_size: usize,
    pub segments_limit: usize,
}

impl StorageConfig {
    pub fn new_config(mem_table_size: usize, segments_limit: usize) -> Self {
        StorageConfig {
            mem_table_size,
            segments_limit,
        }
    }

    pub fn default_config() -> Self {
        StorageConfig {
            mem_table_size: DETAULT_MEM_TABLE_SIZE,
            segments_limit: DEFAULT_SEGMENTS_LIMIT,
        }
    }
}
