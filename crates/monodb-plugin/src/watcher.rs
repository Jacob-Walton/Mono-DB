//! Filesystem watcher for hot-reload of plugins

use anyhow::Result;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use tracing::{debug, error, info};

/// Events from the plugin watcher
#[derive(Debug, Clone)]
pub enum PluginEvent {
    /// A new plugin was added
    Added(PathBuf),
    /// A plugin was modified
    Modified(PathBuf),
    /// A plugin was removed
    Removed(PathBuf),
}

/// Filesystem watcher for plugin hot-reload
pub struct PluginWatcher {
    _watcher: RecommendedWatcher,
    event_rx: Mutex<Receiver<PluginEvent>>,
    _handle: JoinHandle<()>,
}

impl PluginWatcher {
    /// Create a new plugin watcher for the given directory
    pub fn new(plugins_dir: impl AsRef<Path>) -> Result<Self> {
        let plugins_dir = plugins_dir.as_ref().to_path_buf();

        // Channel for plugin events
        let (event_tx, event_rx) = mpsc::channel();

        // Channel for raw notify events
        let (notify_tx, notify_rx) = mpsc::channel::<notify::Result<Event>>();

        // Create the watcher
        let watcher = notify::recommended_watcher(move |res| {
            let _ = notify_tx.send(res);
        })?;

        // Spawn event processing thread
        let handle = {
            let event_tx = event_tx.clone();
            thread::spawn(move || {
                process_events(notify_rx, event_tx);
            })
        };

        let mut watcher = watcher;
        watcher.watch(&plugins_dir, RecursiveMode::NonRecursive)?;

        info!(path = %plugins_dir.display(), "Watching plugins directory for changes");

        Ok(Self {
            _watcher: watcher,
            event_rx: Mutex::new(event_rx),
            _handle: handle,
        })
    }

    /// Try to receive the next plugin event (non-blocking)
    pub fn try_recv(&self) -> Option<PluginEvent> {
        self.event_rx.lock().try_recv().ok()
    }

    /// Receive all pending events
    pub fn drain(&self) -> Vec<PluginEvent> {
        let rx = self.event_rx.lock();
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        events
    }
}

fn process_events(rx: Receiver<notify::Result<Event>>, tx: Sender<PluginEvent>) {
    for result in rx {
        match result {
            Ok(event) => {
                // Only process .wasm files
                let wasm_paths: Vec<PathBuf> = event
                    .paths
                    .iter()
                    .filter(|p| p.extension().map(|e| e == "wasm").unwrap_or(false))
                    .cloned()
                    .collect();

                if wasm_paths.is_empty() {
                    continue;
                }

                for path in wasm_paths {
                    let plugin_event = match &event.kind {
                        EventKind::Create(_) => {
                            debug!(path = %path.display(), "Plugin created");
                            Some(PluginEvent::Added(path))
                        }
                        EventKind::Modify(_) => {
                            debug!(path = %path.display(), "Plugin modified");
                            Some(PluginEvent::Modified(path))
                        }
                        EventKind::Remove(_) => {
                            debug!(path = %path.display(), "Plugin removed");
                            Some(PluginEvent::Removed(path))
                        }
                        _ => None,
                    };

                    if let Some(evt) = plugin_event {
                        if tx.send(evt).is_err() {
                            // Receiver dropped, exit thread
                            return;
                        }
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Filesystem watcher error");
            }
        }
    }
}

/// Debounced watcher that coalesces rapid changes
pub struct DebouncedPluginWatcher {
    inner: Arc<PluginWatcher>,
    pending: Mutex<std::collections::HashMap<PathBuf, (PluginEvent, std::time::Instant)>>,
    debounce_ms: u64,
}

impl DebouncedPluginWatcher {
    pub fn new(plugins_dir: impl AsRef<Path>, debounce_ms: u64) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(PluginWatcher::new(plugins_dir)?),
            pending: Mutex::new(std::collections::HashMap::new()),
            debounce_ms,
        })
    }

    /// Get debounced events (events that have settled for at least debounce_ms)
    pub fn get_debounced_events(&self) -> Vec<PluginEvent> {
        let now = std::time::Instant::now();
        let debounce = std::time::Duration::from_millis(self.debounce_ms);

        // Drain new events into pending
        {
            let mut pending = self.pending.lock();
            for event in self.inner.drain() {
                let path = match &event {
                    PluginEvent::Added(p) | PluginEvent::Modified(p) | PluginEvent::Removed(p) => {
                        p.clone()
                    }
                };
                pending.insert(path, (event, now));
            }
        }

        // Return events that have settled
        let mut pending = self.pending.lock();
        let mut ready = Vec::new();
        let mut to_remove = Vec::new();

        for (path, (event, timestamp)) in pending.iter() {
            if now.duration_since(*timestamp) >= debounce {
                ready.push(event.clone());
                to_remove.push(path.clone());
            }
        }

        for path in to_remove {
            pending.remove(&path);
        }

        ready
    }
}
