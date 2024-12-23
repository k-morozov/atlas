#[derive(Debug)]
pub enum PgError {
    RowBuilderError,
    MemTableFlushError,
}
