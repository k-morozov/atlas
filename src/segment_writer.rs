use crate::field::FieldType;
use crate::marshal::Marshal;
use crate::pg_errors::PgError;
use crate::row::Row;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::iter::Iterator;

struct SegmentWriter<'a> {
    buf: BufWriter<File>,
    row_it: Option<Box<dyn Iterator<Item = &'a Row> + 'a>>,
}

impl<'a> SegmentWriter<'a> {
    pub fn new<T>(path_to_segment: String, row_it: T) -> Self
    where
        T: Iterator<Item = &'a Row> + 'a,
    {
        let result_create = File::create(path_to_segment);
        if let Err(_) = result_create {
            panic!("Failed to create new part");
        };

        Self {
            buf: BufWriter::new(result_create.unwrap()),
            row_it: Some(Box::new(row_it)),
        }
    }

    // trait Writer is more suitable?
    pub fn write_rows(&mut self) -> Result<(), PgError> {
        if self.row_it.is_none() {
            return Err(PgError::MarshalFailedSerialization);
        }
        let row_it = self
            .row_it
            .take()
            .ok_or(PgError::MarshalFailedSerialization)?;

        for row in row_it {
            let mut row_buffer = vec![0u8; row.size()];

            row.serialize(&mut row_buffer[0..])
                .map_err(|_| PgError::MarshalFailedSerialization)?;

            self.buf
                .write_all(&row_buffer[0..])
                .map_err(|_| PgError::MarshalFailedSerialization)?;
            self.buf
                .flush()
                .map_err(|_| PgError::MarshalFailedSerialization)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::field::{Field, FieldType};
    use crate::row::RowBuilder;
    use crate::segment_writer::*;
    use std::fs::{create_dir_all, remove_file};
    use std::io::ErrorKind;
    use std::path::Path;

    #[test]
    fn create_segment() {
        let path = String::from("/tmp/pegasus/test/create_segment/part1.bin");

        if let Some(parent) = Path::new(&path).parent() {
            create_dir_all(parent).unwrap();
        }

        if let Err(er) = remove_file(path.clone()) {
            assert_eq!(ErrorKind::NotFound, er.kind());
        }

        let mut rows: Vec<Row> = Vec::new();

        for index in 1..4 {
            let row = RowBuilder::new(3)
                .add_field(Field::new(FieldType::Int32(12 + index)))
                .add_field(Field::new(FieldType::Int32(100 + index)))
                .build()
                .unwrap();

            rows.push(row);
        }
        let mut writer = SegmentWriter::new(path.clone(), rows.iter());
        let result = writer.write_rows();

        assert!(result.is_ok());
    }
}
