#[derive(Debug, PartialEq)]
pub enum PgError {
    RowBuilderError,
    MemTableFlushError,
    SegmentWriterFlushError,
    MarshalFailedSerialization,
}
