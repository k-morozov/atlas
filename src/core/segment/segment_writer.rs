use crate::core::entry::Entry;
use crate::core::marshal::Marshal;
use crate::core::pg_errors::PgError;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::iter::Iterator;
use std::mem::MaybeUninit;
use std::path::Path;
use std::slice::from_raw_parts;

pub struct SegmentWriter<'a> {
    buf: BufWriter<File>,
    row_it: Option<Box<dyn Iterator<Item = &'a Entry> + 'a>>,
}

impl<'a> SegmentWriter<'a> {
    pub fn new<T>(path_to_segment: &Path, row_it: T) -> Self
    where
        T: Iterator<Item = &'a Entry> + 'a,
    {
        let result_create = File::create(path_to_segment);
        if let Err(er) = result_create {
            panic!(
                "Failed to create new part. path={}, error= {}",
                path_to_segment.display(),
                er
            );
        };

        Self {
            buf: BufWriter::new(result_create.unwrap()),
            row_it: Some(Box::new(row_it)),
        }
    }

    // trait Writer is more suitable?
    pub fn write_entries(&mut self) -> Result<(), PgError> {
        if self.row_it.is_none() {
            return Err(PgError::MarshalFailedSerialization);
        }
        let row_it = self
            .row_it
            .take()
            .ok_or(PgError::MarshalFailedSerialization)?;

        for row in row_it {
            let mut row_buf_raw = vec![MaybeUninit::uninit(); row.size()];

            row.serialize(&mut row_buf_raw)
                .map_err(|_| PgError::MarshalFailedSerialization)?;

            let row_buf_initialized =
                unsafe { from_raw_parts(row_buf_raw.as_ptr() as *const u8, row.size()) };

            self.buf
                .write_all(&row_buf_initialized)
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
    use std::fs::{create_dir_all, remove_file};
    use std::io::ErrorKind;
    use std::path::Path;

    use crate::core::field::{Field, FieldType};
    use crate::core::segment::segment_writer::*;

    #[test]
    fn create_segment() {
        let path = Path::new("/tmp/kvs/test/create_segment/part1.bin");

        if let Some(parent) = path.parent() {
            create_dir_all(parent).unwrap();
        }

        if let Err(er) = remove_file(path) {
            assert_eq!(ErrorKind::NotFound, er.kind());
        }

        let mut entries: Vec<Entry> = Vec::new();

        for index in 1..4 {
            entries.push(Entry::new(
                Field::new(FieldType::Int32(index)),
                Field::new(FieldType::Int32(index * 10)),
            ));
        }
        let mut writer = SegmentWriter::new(path, entries.iter());
        let result = writer.write_entries();

        assert!(result.is_ok());
    }
}
