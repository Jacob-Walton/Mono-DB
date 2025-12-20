use std::{sync::atomic::AtomicU64, time::Instant};

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

pub struct Session {
    pub id: u64,
    pub tx_id: Option<u64>,
    pub user_id: String,
    // pub permissions: Permissions, - TODO: Implement permissions
    pub created_at: Instant,
    pub last_activity: Instant,
    pub authenticated: bool,
}

impl Session {
    pub fn new(user_id: String, authenticated: bool) -> Self {
        let id = SESSION_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Session {
            id,
            tx_id: None,
            user_id,
            created_at: Instant::now(),
            last_activity: Instant::now(),
            authenticated,
        }
    }
}
