use crate::pg_errors::PgError;

pub trait Marshal {
    fn serialize(&self, dst: &mut [u8]) -> Result<(), PgError>;
    fn deserialize(&mut self, src: &[u8]) -> Result<(), PgError>;
}
