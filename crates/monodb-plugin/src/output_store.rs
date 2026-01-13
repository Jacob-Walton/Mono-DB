//! Plugin output storage

use anyhow::Result;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(debug_assertions)]
use tracing::debug;

/// Stored plugin output
#[derive(Debug, Clone)]
pub struct PluginOutput {
    pub plugin_name: String,
    pub stdout: String,
    pub stderr: String,
    pub timestamp: u64,
    pub run_id: u64,
}

/// Store for plugin outputs
pub struct OutputStore {
    outputs: RwLock<BTreeMap<u64, PluginOutput>>,
    next_id: AtomicU64,
    #[cfg(debug_assertions)]
    output_dir: std::path::PathBuf,
}

impl OutputStore {
    pub fn new() -> Result<Self> {
        #[cfg(debug_assertions)]
        let output_dir = {
            let dir = std::env::temp_dir().join("monodb-plugin-outputs");
            std::fs::create_dir_all(&dir)?;
            debug!(path = %dir.display(), "Plugin output directory");
            dir
        };

        Ok(Self {
            outputs: RwLock::new(BTreeMap::new()),
            next_id: AtomicU64::new(1),
            #[cfg(debug_assertions)]
            output_dir,
        })
    }

    /// Store plugin output and return the run ID
    pub fn store(&self, plugin_name: &str, stdout: &[u8], stderr: &[u8]) -> Result<u64> {
        let run_id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let stdout_str = String::from_utf8_lossy(stdout).to_string();
        let stderr_str = String::from_utf8_lossy(stderr).to_string();

        let output = PluginOutput {
            plugin_name: plugin_name.to_string(),
            stdout: stdout_str.clone(),
            stderr: stderr_str.clone(),
            timestamp,
            run_id,
        };

        // Store in memory
        self.outputs.write().insert(run_id, output);

        // In debug mode, also write to files
        #[cfg(debug_assertions)]
        {
            let run_dir = self.output_dir.join(format!("run-{}", run_id));
            std::fs::create_dir_all(&run_dir)?;

            let meta_path = run_dir.join("meta.txt");
            let stdout_path = run_dir.join("stdout.txt");
            let stderr_path = run_dir.join("stderr.txt");

            std::fs::write(
                &meta_path,
                format!(
                    "plugin: {}\ntimestamp: {}\nrun_id: {}\n",
                    plugin_name, timestamp, run_id
                ),
            )?;
            std::fs::write(&stdout_path, &stdout_str)?;
            std::fs::write(&stderr_path, &stderr_str)?;

            debug!(
                run_id = run_id,
                path = %run_dir.display(),
                "Stored plugin output to disk"
            );
        }

        Ok(run_id)
    }

    /// Get stored output by run ID
    pub fn get(&self, run_id: u64) -> Option<PluginOutput> {
        self.outputs.read().get(&run_id).cloned()
    }

    /// Get all outputs for a specific plugin
    pub fn get_by_plugin(&self, plugin_name: &str) -> Vec<PluginOutput> {
        self.outputs
            .read()
            .values()
            .filter(|o| o.plugin_name == plugin_name)
            .cloned()
            .collect()
    }

    /// Get the most recent output for a plugin
    pub fn get_latest(&self, plugin_name: &str) -> Option<PluginOutput> {
        self.outputs
            .read()
            .values()
            .filter(|o| o.plugin_name == plugin_name)
            .max_by_key(|o| o.run_id)
            .cloned()
    }

    /// List all stored outputs (metadata only)
    pub fn list(&self) -> Vec<(u64, String, u64)> {
        self.outputs
            .read()
            .values()
            .map(|o| (o.run_id, o.plugin_name.clone(), o.timestamp))
            .collect()
    }

    /// Clear old outputs (keep only the last N per plugin)
    pub fn cleanup(&self, keep_per_plugin: usize) {
        let mut outputs = self.outputs.write();

        // Group by plugin name
        let mut by_plugin: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        for output in outputs.values() {
            by_plugin
                .entry(output.plugin_name.clone())
                .or_default()
                .push(output.run_id);
        }

        // Keep only the last N for each plugin
        for (_plugin, mut run_ids) in by_plugin {
            run_ids.sort();
            if run_ids.len() > keep_per_plugin {
                let to_remove = run_ids.len() - keep_per_plugin;
                for run_id in run_ids.into_iter().take(to_remove) {
                    outputs.remove(&run_id);
                }
            }
        }
    }

    #[cfg(debug_assertions)]
    pub fn output_dir(&self) -> &std::path::Path {
        &self.output_dir
    }
}
