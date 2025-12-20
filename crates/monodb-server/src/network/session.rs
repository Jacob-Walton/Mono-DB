use std::sync::atomic::AtomicU64;

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

pub struct Session {
    pub id: u64,
    pub tx_id: Option<u64>,
    pub user: Option<String>,
    pub database: Option<String>,
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

impl Session {
    pub fn new() -> Self {
        let id = SESSION_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Session {
            id,
            tx_id: None,
            user: None,
            database: None,
        }
    }
}
