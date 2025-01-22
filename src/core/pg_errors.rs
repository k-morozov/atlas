#[derive(Debug, PartialEq)]
pub enum PgError {
    RowBuilderError,
    MemTableFlushError,
    SegmentWriterFlushError,
    MarshalFailedSerialization,
    MarshalFailedDeserialization,
    RowAlreadyContainsSchema,
    NoSchemaInRow,
    FailedCreateTableDirs,
}
