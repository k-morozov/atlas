use std::cmp;

pub enum FieldType {
    Null,
    Int,
    String,
}

pub struct Field {
    field: FieldType,
}

pub struct Row {
    fields: Vec<Field>,
}

impl Row {
    pub fn new(length: usize) -> Self {
        let mut fields = Vec::<Field>::new();
        fields.reserve(length); 

        for index in 0..length {
            fields.insert(index, Field{
                field: FieldType::Null,
            });
        }

        fields.shrink_to_fit();

        Row {
            fields,
        }
    }
}

// https://doc.rust-lang.org/std/cmp/trait.Ord.html#how-can-i-implement-ord
impl cmp::Ord for Row {
    fn cmp(&self, other: &Row) -> std::cmp::Ordering {
        cmp::Ordering::Equal
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn check_row() {
    }
}
