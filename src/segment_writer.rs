use crate::field::FieldType;
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

    pub fn write_rows(&mut self) -> Result<(), std::io::Error> {
        let row_it = self.row_it.take().ok_or(std::io::ErrorKind::NotFound)?;

        for row in row_it {
            for (index, field) in row.iter().enumerate() {
                match &field.field {
                    FieldType::Int32(number) => {
                        self.buf.write_all(&number.to_le_bytes())?;
                    } // FieldType::String(text) => {
                      //     self.buf.write_all(text.as_bytes())?;
                      // }
                }
                if index != row.size() - 1 {
                    let _ = self.buf.write_all(b"\t");
                }
            }
            let _ = self.buf.write_all(b"\n");

            self.buf.flush()?;
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
                // .add_field(Field::new(FieldType::String(
                //     format!("hello msg {}", index).to_string(),
                // )))
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
