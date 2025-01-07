use std::fs::File;
use std::io::{BufReader, ErrorKind::UnexpectedEof, Read};
use std::mem::MaybeUninit;
use std::path::Path;
use std::rc::Rc;

use crate::field::FieldType;
use crate::marshal::Marshal;
use crate::pg_errors::PgError;
use crate::row::Row;
use crate::schema::{schema_size, Schema};

struct SegmentReader {
    schema: Rc<Schema>,
    schema_size: usize,
    part_file: File,
}

impl SegmentReader {
    pub fn new(path_to_part: &Path, schema: Rc<Schema>) -> Self {
        SegmentReader {
            part_file: File::open(path_to_part).unwrap(),
            schema_size: schema_size(&schema),
            schema,
        }
    }

    pub fn read(&self) -> Result<Vec<Row>, PgError> {
        let mut part_buffer = BufReader::new(&self.part_file);
        let mut result = Vec::<Row>::new();

        loop {
            let mut row_buffer = vec![0u8; self.schema_size];
            match part_buffer.read_exact(&mut row_buffer) {
                Ok(_) => {
                    let mut row = Row::new(self.schema.len());
                    row.set_schema(self.schema.clone())?;

                    row.deserialize(&row_buffer)
                        .map_err(|_| PgError::MarshalFailedDeserialization)?;

                    result.push(row);
                }

                Err(e) if e.kind() == UnexpectedEof => break,
                Err(_) => {
                    return Err(PgError::MarshalFailedDeserialization);
                }
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use std::fs::*;
    use std::io::{BufWriter, Write};
    use std::rc::Rc;

    use crate::field::*;
    use crate::marshal::Marshal;
    use crate::row::*;
    use crate::schema::*;
    use crate::segment_reader::*;
    use std::io::ErrorKind;
    use std::path::Path;

    fn create_part(path: &Path, rows: &Vec<Row>) {
        if let Some(parent) = path.parent() {
            create_dir_all(parent).unwrap();
        }

        if let Err(er) = remove_file(path) {
            assert_eq!(ErrorKind::NotFound, er.kind());
        }

        let mut buf = BufWriter::new(File::create(path).unwrap());

        for row in rows {
            let mut row_buf_raw = vec![MaybeUninit::uninit(); row.size()];
            let r: Result<(), PgError> = row.serialize(&mut row_buf_raw);
            assert!(r.is_ok());

            let row_buf_initialized = row_buf_raw
                .iter()
                .map(|entry| unsafe { entry.assume_init() })
                .collect::<Vec<u8>>();

            let r = buf.write_all(&row_buf_initialized);
            assert!(r.is_ok());
        }

        let r = buf.flush();
        assert!(r.is_ok());
    }

    #[test]
    fn simple_segment_reader() {
        let schema = Rc::new(vec![
            Field::new(FieldType::Int32(0)),
            Field::new(FieldType::Int32(0)),
        ]);

        let mut row1 = RowBuilder::new(2)
            .add_field(Field::new(FieldType::Int32(42)))
            .add_field(Field::new(FieldType::Int32(33)))
            .build()
            .unwrap();

        let mut row2 = RowBuilder::new(2)
            .add_field(Field::new(FieldType::Int32(142)))
            .add_field(Field::new(FieldType::Int32(133)))
            .build()
            .unwrap();

        let r = row1.set_schema(schema.clone());
        assert!(r.is_ok());
        let r = row2.set_schema(schema.clone());
        assert!(r.is_ok());

        let mut rows = Vec::new();
        rows.push(row1.clone());
        rows.push(row2.clone());

        let path = Path::new("/tmp/pegasus/test/simple_segment_reader/part1.bin");
        create_part(path, &rows);

        let reader = SegmentReader::new(path, schema.clone());

        let result = reader.read();
        assert!(result.is_ok());

        let actual = result.unwrap();

        assert_eq!(actual[0], row1);
        assert_eq!(actual[1], row2);
    }
}
