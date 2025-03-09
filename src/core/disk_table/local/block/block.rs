use crate::errors::Result;

pub trait WriteToTable {
    fn write_to(&self, ptr: &mut Box<dyn std::io::Write>) -> Result<()>;
}
