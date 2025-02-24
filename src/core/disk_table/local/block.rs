use super::writer_disk_table::WriterFlexibleDiskTablePtr;
use crate::errors::Result;

pub trait WriteToTable {
    fn write_to(&self, ptr: &mut WriterFlexibleDiskTablePtr) -> Result<()>;
}
