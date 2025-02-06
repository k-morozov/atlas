use crate::core::field::FixedField;

pub type Schema = Vec<FixedField>;

pub fn schema_size(schema: &Schema) -> usize {
    let mut total = 0;
    for field in schema {
        total += field.size();
    }
    total
}
