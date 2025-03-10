use std::path::{Path, PathBuf};

use super::file_handle::FileHandle;
use super::reader_local_disk_table::{ReaderDiskTablePtr, ReaderFlexibleDiskTable};
use crate::core::disk_table::local::block::{
    block::WriteToTable,
    data_block_buffer,
    data_block_buffer::DataBlockBuffer,
    meta_block,
    meta_block::{IndexBlock, IndexBlocks, Offset},
};
use crate::core::entry::flexible_user_entry::FlexibleUserEntry;
use crate::core::field::FieldSize;
use crate::core::marshal::write_u32;
use crate::errors::Result;

pub struct DiskTableBuilder {
    disk_table_path: PathBuf,
    index_table_path: PathBuf,
    building_disk_table: Option<Box<dyn std::io::Write>>,
    building_index_table: Option<Box<dyn std::io::Write>>,

    data_block: Option<DataBlockBuffer>,
    index_entries: Vec<Offset>,
    index_blocks: IndexBlocks,
    offset: u32,
}

impl DiskTableBuilder {
    pub fn new<P: AsRef<Path>>(disk_table_path: P, index_table_path: P) -> Self {
        let data_handle = match FileHandle::new_data_writer(disk_table_path.as_ref()) {
            Ok(h) => h,
            Err(er) => panic!("Failed create file data handle: {}", er),
        };

        let index_handle = match FileHandle::new_index_writer(index_table_path.as_ref()) {
            Ok(h) => h,
            Err(er) => panic!("Failed create file data handle: {}", er),
        };

        DiskTableBuilder {
            disk_table_path: disk_table_path.as_ref().to_path_buf(),
            index_table_path: index_table_path.as_ref().to_path_buf(),
            building_disk_table: Some(data_handle),
            building_index_table: Some(index_handle),
            index_entries: Vec::<Offset>::new(),
            index_blocks: IndexBlocks::new(),
            offset: 0,
            data_block: Some(DataBlockBuffer::new()),
        }
    }

    pub fn from<P: AsRef<Path>>(disk_table_path: P, index_table_path: P) -> Self {
        DiskTableBuilder {
            disk_table_path: disk_table_path.as_ref().to_path_buf(),
            index_table_path: index_table_path.as_ref().to_path_buf(),
            building_disk_table: None,
            building_index_table: None,
            index_entries: Vec::<Offset>::new(),
            index_blocks: IndexBlocks::new(),
            offset: 0,
            data_block: None,
        }
    }

    pub fn append_entry(&mut self, entry: &FlexibleUserEntry) -> &mut Self {
        let esstimate_entry_size = data_block_buffer::ENTRY_METADATA_SIZE as usize + entry.size();

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
                        Some(writer) => {
                            if let Err(er) = data_block.write_to(writer) {
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
                            first_key: entry.get_key().clone(),
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

    fn write_index_table(&mut self) -> Result<()> {
        let Some(index_table) = &mut self.building_index_table else {
            return Ok(());
        };

        assert_ne!(self.index_blocks.len(), 0);
        assert_ne!(self.index_blocks.size(), 0);

        self.index_blocks.write_to(index_table)?;

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

            index_table.write(&tmp)?;
        }

        // write index_entries size
        index_table.write(&(self.index_entries.len() as u32).to_le_bytes())?;

        {
            let Some(mut writer) = self.building_index_table.take() else {
                panic!("Broken building_disk_table")
            };

            if let Err(er) = writer.flush() {
                panic!("Failed flush in builder: {}", er)
            }
        }

        Ok(())
    }

    pub fn build(&mut self) -> Result<ReaderDiskTablePtr> {
        let Some(disk_table) = &mut self.building_disk_table else {
            assert!(self.data_block.is_none());

            self.write_index_table()?;

            let reader = ReaderFlexibleDiskTable::new(
                self.disk_table_path.as_path(),
                self.index_table_path.as_path(),
            )?;
            return Ok(reader);
        };

        if let Some(data_block) = &mut self.data_block {
            if let Err(er) = data_block.write_to(disk_table) {
                panic!("Failed write last data block in builder: {}", er)
            }
            data_block.reset();
        };

        // @todo close?
        {
            let Some(mut writer) = self.building_disk_table.take() else {
                panic!("Broken building_disk_table")
            };

            if let Err(er) = writer.flush() {
                panic!("Failed flush in builder: {}", er)
            }
        }

        self.write_index_table()?;

        let reader = ReaderFlexibleDiskTable::new(
            self.disk_table_path.as_path(),
            self.index_table_path.as_path(),
        )?;
        Ok(reader)
    }
}
