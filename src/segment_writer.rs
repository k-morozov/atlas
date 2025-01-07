use crate::field::FieldType;
use crate::marshal::Marshal;
use crate::pg_errors::PgError;
use crate::row::Row;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::iter::Iterator;
use std::mem::MaybeUninit;
use std::path::Path;

struct SegmentWriter<'a> {
    buf: BufWriter<File>,
    row_it: Option<Box<dyn Iterator<Item = &'a Row> + 'a>>,
}

impl<'a> SegmentWriter<'a> {
    pub fn new<T>(path_to_segment: &Path, row_it: T) -> Self
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
            // let mut row_buffer: Vec<MaybeUninit<u8>> = Vec::with_capacity(row.size());
            // unsafe {
            //     row_buffer.set_len(row.size());
            // }

            let mut row_buffer_raw = vec![MaybeUninit::uninit(); row.size()];

            row.serialize(&mut row_buffer_raw)
                .map_err(|_| PgError::MarshalFailedSerialization)?;

            let row_buffer_initialized = row_buffer_raw
                .iter()
                .map(|entry| unsafe { entry.assume_init() })
                .collect::<Vec<u8>>();

            self.buf
                .write_all(&row_buffer_initialized)
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
        let path = Path::new("/tmp/pegasus/test/create_segment/part1.bin");

        if let Some(parent) = path.parent() {
            create_dir_all(parent).unwrap();
        }

        if let Err(er) = remove_file(path) {
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
        let mut writer = SegmentWriter::new(path, rows.iter());
        let result = writer.write_rows();

        assert!(result.is_ok());
    }
}
