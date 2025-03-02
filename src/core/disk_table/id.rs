use std::sync::atomic::Ordering;
use std::{fmt::Display, sync::atomic::AtomicU64};

pub struct DiskTableID {
    id: AtomicU64,
}

impl DiskTableID {
    pub fn new() -> Self {
        DiskTableID {
            id: AtomicU64::new(1),
        }
    }

    pub fn from(id: u64) -> Self {
        DiskTableID {
            id: AtomicU64::new(id),
        }
    }

    pub fn get_and_next(&self) -> DiskTableID {
        let result = self.id.fetch_add(1, Ordering::SeqCst);
        DiskTableID { id: result.into() }
    }

    pub fn get_id(&self) -> u64 {
        self.id.load(Ordering::SeqCst)
    }
}

impl Display for DiskTableID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id.load(Ordering::SeqCst))
    }
}
