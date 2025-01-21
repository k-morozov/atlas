pub struct SegmentID {
    id: u64,
}

impl SegmentID {
    pub fn new() -> Self {
        SegmentID { id: 0 }
    }

    pub fn generate(&mut self) -> u64 {
        let result = self.id;
        self.id += 1;
        result
    }
}
