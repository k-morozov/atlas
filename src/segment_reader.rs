use std::fs::File;
use std::io::{BufReader, ErrorKind::UnexpectedEof, Read};
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
    pub fn new(path_to_part: String, schema: Rc<Schema>) -> Self {
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
mod test {}
