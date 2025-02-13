pub const DEFAULT_TABLES_PATH: &'static str = "/tmp/kvs/tables/";
pub const DEFAULT_TEST_TABLES_PATH: &'static str = "/tmp/kvs/tables/test/";
pub const DETAULT_MEM_TABLE_SIZE: usize = 4;
pub const DEFAULT_SEGMENTS_LIMIT: usize = 4;

#[derive(Clone)]
pub struct TableConfig {
    pub mem_table_size: usize,
    pub segments_limit: usize,
}

impl TableConfig {
    pub fn new_config(mem_table_size: usize, segments_limit: usize) -> Self {
        TableConfig {
            mem_table_size,
            segments_limit,
        }
    }

    pub fn default_config() -> Self {
        TableConfig {
            mem_table_size: DETAULT_MEM_TABLE_SIZE,
            segments_limit: DEFAULT_SEGMENTS_LIMIT,
        }
    }
}
