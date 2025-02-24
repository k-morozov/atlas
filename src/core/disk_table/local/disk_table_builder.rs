use std::path::{Path, PathBuf};

use super::data_block::DataBlock;
use super::meta_block::{IndexBlock, IndexBlocks, Offset};
use super::reader_disk_table::{ReaderFlexibleDiskTable, ReaderFlexibleDiskTablePtr};
use super::writer_disk_table::{WriterFlexibleDiskTable, WriterFlexibleDiskTablePtr};
use crate::core::disk_table::local::{data_block, meta_block};
use crate::core::entry::flexible_entry::FlexibleEntry;
use crate::core::field::FieldSize;
use crate::core::marshal::write_u32;
use crate::errors::Result;

pub struct DiskTableBuilder {
    disk_table_path: PathBuf,
    building_disk_table: Option<WriterFlexibleDiskTablePtr>,
    index_entries: Vec<Offset>,
    index_blocks: IndexBlocks,
    offset: u32,

    data_block: Option<DataBlock>,
}

impl DiskTableBuilder {
    pub fn new<P: AsRef<Path>>(disk_table_path: P) -> Self {
        DiskTableBuilder {
            disk_table_path: disk_table_path.as_ref().to_path_buf(),
            building_disk_table: Some(WriterFlexibleDiskTable::new(disk_table_path)),
            index_entries: Vec::<Offset>::new(),
            index_blocks: IndexBlocks::new(),
            offset: 0,
            data_block: Some(DataBlock::new()),
        }
    }

    pub fn from<P: AsRef<Path>>(disk_table_path: P) -> Self {
        DiskTableBuilder {
            disk_table_path: disk_table_path.as_ref().to_path_buf(),
            building_disk_table: None,
            index_entries: Vec::<Offset>::new(),
            index_blocks: IndexBlocks::new(),
            offset: 0,
            data_block: None,
        }
    }

    pub fn append_entry(&mut self, entry: &FlexibleEntry) -> &mut Self {
        let esstimate_entry_size = data_block::ENTRY_METADATA_SIZE as usize + entry.size();

        let Some(data_block) = &mut self.data_block else {
            panic!("Logic error")
        };

        for i in 0..3 {
            if i == 2 {
                panic!("Logic error")
            }

            let is_block_empty = data_block.empty();
            match data_block.append(entry) {
                Ok(0) => {
                    let remaining_bytes = data_block.remaining_size();
                    self.offset += remaining_bytes as u32;

                    match &mut self.building_disk_table {
                        Some(ptr) => {
                            if let Err(er) = ptr.write(data_block.data()) {
                                panic!("Failed write data block in builder: {}", er)
                            }
                        }
                        None => panic!("Failed write entry to None"),
                    }

                    data_block.reset();
                }
                Ok(bytes) => {
                    if is_block_empty {
                        self.index_blocks.append(IndexBlock {
                            block_offset: self.offset,
                            block_size: data_block.max_size() as u32,
                            key_size: entry.get_key().size() as u32,
                            key: entry.get_key().clone(),
                        });
                    }
                    self.index_entries.push(Offset {
                        pos: self.offset,
                        size: esstimate_entry_size as u32,
                    });
                    self.offset += bytes as u32;
                    break;
                }
                Err(er) => panic!(
                    "Attemt appending entry to data block was failed with error: {}",
                    er
                ),
            }
        }

        self
    }

    pub fn build(&mut self) -> Result<ReaderFlexibleDiskTablePtr> {
        let Some(ptr) = &mut self.building_disk_table else {
            return Ok(ReaderFlexibleDiskTable::new(self.disk_table_path.as_path()));
        };

        if let Some(data_block) = &mut self.data_block {
            if let Err(er) = ptr.write(data_block.data()) {
                panic!("Failed write last data block in builder: {}", er)
            }
            data_block.reset();
        };

        self.index_blocks.write_to(ptr)?;

        // write index_entries
        for offset in &self.index_entries {
            let mut tmp = [0; meta_block::INDEX_ENTRIES_SIZE];
            write_u32(
                &mut tmp[0..meta_block::INDEX_ENTRIES_OFFSET_SIZE],
                offset.pos,
            )?;
            write_u32(
                &mut tmp[meta_block::INDEX_ENTRIES_OFFSET_SIZE..],
                offset.size,
            )?;

            ptr.write(&tmp)?;
        }

        // write index_entries size
        ptr.write(&(self.index_entries.len() as u32).to_le_bytes())?;

        if let Some(mut writer) = self.building_disk_table.take() {
            if let Err(er) = writer.flush() {
                panic!("Failed flush in builder: {}", er)
            }
        }

        Ok(ReaderFlexibleDiskTable::new(self.disk_table_path.as_path()))
    }
}
