use crate::row::Row;
use std::iter::Iterator;

struct SegmentWriter {
    path_to_segment: String,
    row_it: Box<dyn Iterator<Item = Row>>,
}

impl SegmentWriter {
    pub fn new<T: Iterator<Item = Row> + 'static>(path_to_segment: String, row_it: T) -> Self {
        Self {
            row_it: Box::new(row_it),
            path_to_segment,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::field::{Field, FieldType};
    use crate::row::RowBuilder;

    #[test]
    fn create_segment() {
        let mut rows = Vec::new();

        for index in 0..5 {
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
    }
}
