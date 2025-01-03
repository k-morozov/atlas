use crate::field::FieldType;
use crate::pg_errors::PgError;
use crate::row::Row;
use std::fs::File;
use std::io::Write;
use std::iter::Iterator;

struct SegmentWriter<'a> {
    path_to_segment: String,
    row_it: Option<Box<dyn Iterator<Item = &'a Row> + 'a>>,
}

impl<'a> SegmentWriter<'a> {
    pub fn new<T>(path_to_segment: String, row_it: T) -> Self
    where
        T: Iterator<Item = &'a Row> + 'a,
    {
        Self {
            row_it: Some(Box::new(row_it)),
            path_to_segment,
        }
    }

    pub fn flush(&mut self) -> Result<(), PgError> {
        let row_it = self.row_it.take().ok_or(PgError::SegmentWriterFlushError)?;

        let result_create = File::create(self.path_to_segment.clone());
        if let Err(_) = result_create {
            return Err(PgError::SegmentWriterFlushError);
        };
        let mut fd = result_create.unwrap();

        for row in row_it {
            for (index, field) in row.iter().enumerate() {
                match &field.field {
                    FieldType::Int(number) => {
                        fd.write_all(&number.to_be_bytes())
                            .map_err(|_| PgError::SegmentWriterFlushError)?;
                    }
                    FieldType::String(text) => {
                        fd.write_all(text.as_bytes())
                            .map_err(|_| PgError::SegmentWriterFlushError)?;
                    }
                }
                if index != row.size() - 1 {
                    let _ = fd.write_all(b"\t");
                }
            }
            let _ = fd.write_all(b"\n");
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
    use std::path::Path;
    use std::io::ErrorKind;

    #[test]
    fn create_segment() {
        let path = String::from("/tmp/pegasus/test/create_segment/part1.bin");

        if let Some(parent) = Path::new(&path).parent() {
            create_dir_all(parent).unwrap();
        }

        if let Err(er) = remove_file(path.clone()) {
            assert_eq!(ErrorKind::NotFound, er.kind());
        }

        let mut rows = Vec::new();

        for index in 1..4 {
            let row = RowBuilder::new(3)
                .add_field(Field::new(FieldType::Int(index)))
                .add_field(Field::new(FieldType::String(
                    format!("hello msg {}", index).to_string(),
                )))
                .add_field(Field::new(FieldType::Int(100 + index)))
                .build()
                .unwrap();

            rows.push(row);
        }
        let mut writer = SegmentWriter::new(path.clone(), rows.iter());
        let result = writer.flush();

        assert!(result.is_ok());
    }
}
