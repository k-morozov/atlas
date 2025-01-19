use std::fs::File;
use std::io::{BufReader, ErrorKind::UnexpectedEof, Read};
use std::mem::MaybeUninit;
use std::path::Path;
use std::rc::Rc;

use crate::core::entry::Entry;
use crate::core::field::FieldType;
use crate::core::marshal::Marshal;
use crate::core::pg_errors::PgError;
use crate::core::schema::{schema_size, Schema};

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

    pub fn read(&self) -> Result<Vec<Entry>, PgError> {
        let mut part_buffer = BufReader::new(&self.part_file);
        let mut result = Vec::<Entry>::new();

        loop {
            let mut row_buffer = vec![0u8; self.schema_size];
            match part_buffer.read_exact(&mut row_buffer) {
                Ok(_) => {
                    let mut entry = Entry::new(self.schema[0].clone(), self.schema[1].clone());

                    entry
                        .deserialize(&row_buffer)
                        .map_err(|_| PgError::MarshalFailedDeserialization)?;

                    result.push(entry);
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
    use std::io::ErrorKind;
    use std::io::{BufWriter, Write};
    use std::path::Path;
    use std::rc::Rc;
    use std::slice::from_raw_parts;

    use crate::core::entry::*;
    use crate::core::field::*;
    use crate::core::marshal::Marshal;
    use crate::core::schema::*;
    use crate::core::segment::segment_reader::*;

    fn create_part(path: &Path, rows: &Vec<Entry>) {
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

            let row_buf_initialized =
                unsafe { from_raw_parts(row_buf_raw.as_ptr() as *const u8, row.size()) };

            let r = buf.write_all(row_buf_initialized);
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

        let entry1 = Entry::new(
            Field::new(FieldType::Int32(42)),
            Field::new(FieldType::Int32(420)),
        );

        let entry2 = Entry::new(
            Field::new(FieldType::Int32(43)),
            Field::new(FieldType::Int32(430)),
        );

        let mut entries = Vec::new();
        entries.push(entry1.clone());
        entries.push(entry2.clone());

        let path = Path::new("/tmp/kvs/test/simple_segment_reader/part1.bin");
        create_part(path, &entries);

        let reader = SegmentReader::new(path, schema.clone());

        let result = reader.read();
        assert!(result.is_ok());

        let actual = result.unwrap();

        assert_eq!(actual[0], entry1);
        assert_eq!(actual[1], entry2);
    }
}
