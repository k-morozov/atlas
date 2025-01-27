#[derive(Debug, PartialEq)]
pub enum Error {
    RowBuilderError,
    MemTableFlushError,
    SegmentWriterFlushError,
    MarshalFailedSerialization,
    MarshalFailedDeserialization,
    RowAlreadyContainsSchema,
    NoSchemaInRow,
    FailedCreateTableDirs,
    FailedReadSegmentNames,
}
