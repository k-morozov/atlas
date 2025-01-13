use crate::core::field::Field;

pub type Schema = Vec<Field>;

pub fn schema_size(schema: &Schema) -> usize {
    let mut total = 0;
    for field in schema {
        total += field.size();
    }
    total
}
