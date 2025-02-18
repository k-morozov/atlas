use std::fmt::Display;

#[derive(Clone)]
pub struct DiskTableID {
    id: u64,
}

impl DiskTableID {
    pub fn new() -> Self {
        DiskTableID { id: 1 }
    }

    pub fn from(id: u64) -> Self {
        DiskTableID { id }
    }

    pub fn get_and_next(&mut self) -> DiskTableID {
        let result = self.clone();
        self.id += 1;
        result
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }
}

impl Display for DiskTableID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}
