pub const ENTRY_METADATA_OFFSET: u32 = 2 * size_of::<u32>() as u32;

pub(super) const INDEX_ENTRIES_OFFSET_SIZE: usize = size_of::<u32>();
pub(super) const INDEX_ENTRIES_LEN_SIZE: usize = size_of::<u32>();
pub(super) const INDEX_ENTRIES_SIZE: usize = INDEX_ENTRIES_OFFSET_SIZE + INDEX_ENTRIES_LEN_SIZE;

pub(super) const INDEX_COUNT_SIZE: usize = size_of::<u32>();
