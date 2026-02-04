//! Background task management

use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Result of running a single plugin
#[derive(Debug, Clone)]
pub enum PluginResult {
    Success {
        elapsed: std::time::Duration,
    },
    PluginError {
        kind: String,
        message: String,
        context: Option<String>,
        elapsed: std::time::Duration,
    },
    Trap {
        message: String,
    },
    Rejected {
        reason: String,
    },
    BackgroundStarted {
        task_id: u64,
        name: String,
    },
}

/// Status of a background task
#[derive(Debug, Clone)]
pub enum TaskStatus {
    Running,
    Completed(PluginResult),
    Failed(String),
}

/// Background task registry
pub struct BackgroundTaskRegistry {
    next_id: AtomicU64,
    tasks: Mutex<BTreeMap<u64, (String, Option<PluginResult>)>>,
}

impl BackgroundTaskRegistry {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            tasks: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn register(&self, name: String) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.tasks.lock().insert(id, (name, None));
        id
    }

    pub fn complete(&self, id: u64, result: PluginResult) {
        if let Some(entry) = self.tasks.lock().get_mut(&id) {
            entry.1 = Some(result);
        }
    }

    pub fn get_status(&self, id: u64) -> Option<TaskStatus> {
        let tasks = self.tasks.lock();
        tasks.get(&id).map(|(_, result)| match result {
            Some(r) => TaskStatus::Completed(r.clone()),
            None => TaskStatus::Running,
        })
    }

    pub fn list_all(&self) -> Vec<(u64, String, TaskStatus)> {
        let tasks = self.tasks.lock();
        tasks
            .iter()
            .map(|(id, (name, result))| {
                let status = match result {
                    Some(r) => TaskStatus::Completed(r.clone()),
                    None => TaskStatus::Running,
                };
                (*id, name.clone(), status)
            })
            .collect()
    }
}

impl Default for BackgroundTaskRegistry {
    fn default() -> Self {
        Self::new()
    }
}
