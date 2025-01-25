pub struct SegmentID {
    id: u64,
}

impl SegmentID {
    pub fn new() -> Self {
        SegmentID { id: 1 }
    }

    pub fn from(id: u64) -> Self {
        SegmentID { id }
    }

    pub fn get_and_next(&mut self) -> u64 {
        let result = self.id;
        self.id += 1;
        result
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }
}
