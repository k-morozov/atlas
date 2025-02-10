use std::fs::File;
use std::io::{BufReader, ErrorKind::UnexpectedEof, Read};
use std::path::Path;
use std::rc::Rc;

use crate::core::entry::fixed_entry::FixedEntry;
use crate::core::field::{FieldSize, FixedField};
use crate::core::marshal::Marshal;
use crate::core::schema::{schema_size, Schema};
use crate::errors::Result;

pub struct SegmentReader {
    schema: Rc<Schema>,
    schema_size: usize,
    part_file: File,
}

impl SegmentReader {
    pub fn new(path_to_part: &Path, schema: Rc<Schema>) -> Self {
        let fd = match File::open(path_to_part) {
            Ok(fd) => fd,
            Err(er) => panic!(
                "SegmentReader: error={}, path={}",
                er,
                path_to_part.display()
            ),
        };
        SegmentReader {
            part_file: fd,
            schema_size: schema_size(&schema),
            schema,
        }
    }

    pub fn read(&self, key: &FixedField) -> Result<Option<FixedField>> {
        let mut part_buffer = BufReader::new(&self.part_file);

        loop {
            let mut entry_buffer = vec![0u8; self.schema_size];
            match part_buffer.read_exact(&mut entry_buffer) {
                Ok(_) => {
                    let mut entry = FixedEntry::new(self.schema[0].clone(), self.schema[1].clone());

                    entry.deserialize(&entry_buffer)?;

                    if entry.get_key() == key {
                        return Ok(Some(entry.get_value().clone()));
                    }
                }

                Err(e) if e.kind() == UnexpectedEof => break,
                Err(err) => {
                    return Err(err.into());
                }
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use std::fs::*;
    use std::io::ErrorKind;
    use std::io::{BufWriter, Write};
    use std::mem::MaybeUninit;
    use std::path::Path;
    use std::rc::Rc;
    use std::slice::from_raw_parts;

    use crate::core::entry::fixed_entry::*;
    use crate::core::field::*;
    use crate::core::marshal::Marshal;
    use crate::core::segment::segment_reader::*;

    fn create_part(path: &Path, rows: &Vec<FixedEntry>) {
        if let Some(parent) = path.parent() {
            create_dir_all(parent).unwrap();
        }

        if let Err(er) = remove_file(path) {
            assert_eq!(ErrorKind::NotFound, er.kind());
        }

        let mut buf = BufWriter::new(File::create(path).unwrap());

        for row in rows {
            let mut row_buf_raw = vec![MaybeUninit::uninit(); row.size()];
            let r = row.serialize(&mut row_buf_raw);
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
            FixedField::new(FieldType::Int32(0)),
            FixedField::new(FieldType::Int32(0)),
        ]);

        let entry1 = FixedEntry::new(
            FixedField::new(FieldType::Int32(42)),
            FixedField::new(FieldType::Int32(420)),
        );

        let entry2 = FixedEntry::new(
            FixedField::new(FieldType::Int32(43)),
            FixedField::new(FieldType::Int32(430)),
        );

        let mut entries = Vec::new();
        entries.push(entry1.clone());
        entries.push(entry2.clone());

        let path = Path::new("/tmp/kvs/test/simple_segment_reader/part1.bin");
        create_part(path, &entries);

        let reader = SegmentReader::new(path, schema.clone());

        let key = FixedField::new(FieldType::Int32(43));
        let result = reader.read(&key);
        assert!(result.is_ok());

        let actual = result.unwrap();
        assert!(actual.is_some());

        assert_eq!(actual.unwrap(), FixedField::new(FieldType::Int32(430)));

        let key = FixedField::new(FieldType::Int32(431));
        let result = reader.read(&key);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }
}
