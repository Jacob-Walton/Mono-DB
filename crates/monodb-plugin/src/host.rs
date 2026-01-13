//! Plugin host, manages loading and running WASM plugins

use crate::bindings::{self, Plugin};
use crate::component_cache::ComponentCache;
use crate::config::PluginHostConfig;
use crate::function_registry::{FunctionRegistry, RegisteredFunction};
use crate::output_store::OutputStore;
use crate::registry::{PluginInfo, PluginRegistry};
use crate::state::{PluginPermissions, State};
use crate::tasks::{BackgroundTaskRegistry, PluginResult, TaskStatus};
use crate::watcher::{DebouncedPluginWatcher, PluginEvent};
use crate::PluginStorage;
use anyhow::{anyhow, Result};
use monodb_common::Value as MonoValue;
use parking_lot::RwLock;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info, warn};
use wasmtime::component::{HasSelf, Linker};
use wasmtime::{Config, Engine, Store, StoreLimitsBuilder};
use wasmtime_wasi::p2::pipe::MemoryOutputPipe;

/// The plugin host manages loading and running WASM plugins
pub struct PluginHost {
    config: PluginHostConfig,
    engine: Engine,
    linker: Linker<State>,
    component_cache: ComponentCache,
    epoch_thread_running: Arc<AtomicBool>,
    background_tasks: Arc<BackgroundTaskRegistry>,
    registry: Arc<PluginRegistry>,
    function_registry: Arc<FunctionRegistry>,
    output_store: Arc<OutputStore>,
    storage: Arc<dyn PluginStorage>,
    watcher: Option<RwLock<DebouncedPluginWatcher>>,
}

impl PluginHost {
    /// Create a new plugin host with the given configuration and storage backend
    pub fn new(config: PluginHostConfig, storage: Arc<dyn PluginStorage>) -> Result<Self> {
        if !config.epoch.enabled {
            warn!("Epoch interruption is DISABLED, plugins cannot be interrupted by timeout");
        }
        if !config.fuel.enabled {
            warn!("Fuel consumption is DISABLED, plugins have unlimited execution budget");
        }

        let mut cfg = Config::new();
        cfg.epoch_interruption(config.epoch.enabled);
        cfg.consume_fuel(config.fuel.enabled);
        let engine = Engine::new(&cfg)?;

        let mut linker: Linker<State> = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)?;
        bindings::monodb::plugin::database::add_to_linker::<_, HasSelf<_>>(
            &mut linker,
            |s: &mut State| s,
        )?;
        bindings::monodb::plugin::transaction::add_to_linker::<_, HasSelf<_>>(
            &mut linker,
            |s: &mut State| s,
        )?;
        bindings::monodb::plugin::schema::add_to_linker::<_, HasSelf<_>>(
            &mut linker,
            |s: &mut State| s,
        )?;

        let epoch_thread_running = Arc::new(AtomicBool::new(false));
        let background_tasks = Arc::new(BackgroundTaskRegistry::new());
        let registry = Arc::new(PluginRegistry::new());
        let function_registry = Arc::new(FunctionRegistry::new());
        let output_store = Arc::new(OutputStore::new()?);
        let component_cache =
            ComponentCache::new(config.cache.cache_dir.clone(), config.cache.persist)?;

        // Set up hot-reload watcher if enabled
        let watcher = if config.hot_reload && config.plugins_dir.exists() {
            match DebouncedPluginWatcher::new(&config.plugins_dir, 500) {
                Ok(w) => Some(RwLock::new(w)),
                Err(e) => {
                    warn!(error = %e, "Failed to set up hot-reload watcher");
                    None
                }
            }
        } else {
            None
        };

        #[cfg(debug_assertions)]
        info!(path = %output_store.output_dir().display(), "Plugin output directory");

        Ok(Self {
            config,
            engine,
            linker,
            component_cache,
            epoch_thread_running,
            background_tasks,
            registry,
            function_registry,
            output_store,
            storage,
            watcher,
        })
    }

    /// Start the epoch increment thread (call once before running plugins)
    pub fn start_epoch_thread(&self) {
        if !self.config.epoch.enabled || self.epoch_thread_running.load(Ordering::SeqCst) {
            return;
        }

        self.epoch_thread_running.store(true, Ordering::SeqCst);
        let engine = self.engine.clone();
        let interval_secs = self.config.epoch.increment_interval_secs;
        let running = self.epoch_thread_running.clone();

        std::thread::spawn(move || {
            while running.load(Ordering::SeqCst) {
                std::thread::sleep(std::time::Duration::from_secs(interval_secs));
                engine.increment_epoch();
            }
        });

        info!("Started epoch interrupt thread");
    }

    /// Process any pending hot-reload events
    pub fn process_hot_reload(&self) -> Result<Vec<String>> {
        let Some(watcher) = &self.watcher else {
            return Ok(Vec::new());
        };

        let events = watcher.read().get_debounced_events();
        let mut reloaded = Vec::new();

        for event in events {
            match event {
                PluginEvent::Added(path) | PluginEvent::Modified(path) => {
                    // Invalidate cache
                    let _ = self.component_cache.invalidate(&path);

                    // Reload plugin
                    match self.load_plugin_info(&path) {
                        Ok(info) => {
                            info!(name = %info.name, version = %info.version, "Hot-reloaded plugin");
                            reloaded.push(info.name.clone());
                            self.registry.register(info);
                        }
                        Err(e) => {
                            warn!(path = %path.display(), error = %e, "Failed to reload plugin");
                        }
                    }
                }
                PluginEvent::Removed(path) => {
                    // Find and remove the plugin
                    let plugins = self.registry.list();
                    for plugin in plugins {
                        if plugin.path == path {
                            info!(name = %plugin.name, "Plugin removed");
                            self.registry.unregister(&plugin.name);
                            self.function_registry.unregister_plugin(&plugin.name);
                            let _ = self.component_cache.invalidate(&path);
                            reloaded.push(format!("-{}", plugin.name));
                            break;
                        }
                    }
                }
            }
        }

        Ok(reloaded)
    }

    /// Load and run a single plugin from a WASM file path
    fn run_plugin(&self, path: &Path, permissions: PluginPermissions) -> Result<PluginResult> {
        let component = self.component_cache.load(&self.engine, path)?;

        let stdout = MemoryOutputPipe::new(1024 * 1024);

        let mut store = Store::new(
            &self.engine,
            State::new(stdout.clone(), self.storage.clone(), permissions)?,
        );

        store.data_mut().limits = StoreLimitsBuilder::new()
            .memory_size(self.config.limits.max_memory_size)
            .table_elements(self.config.limits.max_table_elements)
            .instances(self.config.limits.max_instances)
            .tables(self.config.limits.max_tables)
            .memories(self.config.limits.max_memories)
            .build();

        store.limiter(|state| &mut state.limits);

        // Set initial epoch deadline before instantiation
        if self.config.epoch.enabled {
            store.epoch_deadline_trap();
            store.set_epoch_deadline(self.config.epoch.max_deadline);
        }

        // Set initial fuel for plugin instantiation and metadata calls
        if self.config.fuel.enabled {
            store.set_fuel(1_000_000)?;
        }

        let bindings = Plugin::instantiate(&mut store, &component, &self.linker)?;

        // Read plugin configuration
        let plugin_config = bindings.api().call_config(&mut store)?;
        let plugin_name = bindings.api().call_name(&mut store)?;

        // Validate and apply epoch deadline
        let epoch_deadline = if self.config.epoch.enabled {
            if plugin_config.long_running {
                self.config.epoch.long_running_deadline
            } else {
                match plugin_config.epoch_deadline {
                    Some(requested) => {
                        if requested > self.config.epoch.max_deadline {
                            return Ok(PluginResult::Rejected {
                                reason: format!(
                                    "Plugin '{}' requested epoch deadline {} exceeds max {}",
                                    plugin_name, requested, self.config.epoch.max_deadline
                                ),
                            });
                        }
                        requested
                    }
                    None => self.config.epoch.default_deadline,
                }
            }
        } else {
            0
        };

        // Validate and apply fuel limit
        let fuel_limit = if self.config.fuel.enabled {
            match plugin_config.fuel_limit {
                Some(requested) => {
                    if requested > self.config.fuel.max_fuel {
                        return Ok(PluginResult::Rejected {
                            reason: format!(
                                "Plugin '{}' requested fuel {} exceeds max {}",
                                plugin_name, requested, self.config.fuel.max_fuel
                            ),
                        });
                    }
                    requested
                }
                None => self.config.fuel.default_fuel,
            }
        } else {
            0
        };

        // Apply the validated configuration
        if self.config.epoch.enabled {
            store.epoch_deadline_trap();
            store.set_epoch_deadline(epoch_deadline);
        }

        if self.config.fuel.enabled {
            store.set_fuel(fuel_limit)?;
        }

        info!(
            plugin = %plugin_name,
            long_running = plugin_config.long_running,
            epoch_deadline = epoch_deadline,
            fuel_limit = fuel_limit,
            "Running plugin"
        );

        // Execute the plugin
        let start_time = Instant::now();
        let result = match bindings.api().call_run(&mut store) {
            Ok(Ok(())) => PluginResult::Success {
                elapsed: start_time.elapsed(),
            },
            Ok(Err(e)) => {
                use bindings::monodb::plugin::types::ErrorKind;
                let kind_str = match e.kind {
                    ErrorKind::IoError => "IoError",
                    ErrorKind::StorageError => "StorageError",
                    ErrorKind::ParseError => "ParseError",
                    ErrorKind::ExecutionError => "ExecutionError",
                    ErrorKind::TypeError => "TypeError",
                    ErrorKind::NotFound => "NotFound",
                    ErrorKind::AlreadyExists => "AlreadyExists",
                    ErrorKind::InvalidOperation => "InvalidOperation",
                    ErrorKind::TransactionError => "TransactionError",
                    ErrorKind::WriteConflict => "WriteConflict",
                };
                PluginResult::PluginError {
                    kind: kind_str.to_string(),
                    message: e.message,
                    context: e.details,
                    elapsed: start_time.elapsed(),
                }
            }
            Err(trap) => PluginResult::Trap {
                message: trap.to_string(),
            },
        };

        // Capture and store plugin output
        let stdout_bytes = store.data().stdout.contents();
        let stderr_bytes = store.data().stderr.contents();

        if let Err(e) = self
            .output_store
            .store(&plugin_name, &stdout_bytes, &stderr_bytes)
        {
            warn!(error = %e, "Failed to store plugin output");
        }

        Ok(result)
    }

    /// Get the output store for retrieving plugin outputs
    pub fn output_store(&self) -> &Arc<OutputStore> {
        &self.output_store
    }

    /// Get the status of a background task by its ID
    pub fn get_task_status(&self, task_id: u64) -> Option<TaskStatus> {
        self.background_tasks.get_status(task_id)
    }

    /// List all background tasks and their statuses
    pub fn list_tasks(&self) -> Vec<(u64, String, TaskStatus)> {
        self.background_tasks.list_all()
    }

    /// Scan a directory for WASM plugins and register them
    pub fn scan_plugins_dir(&self, dir: &Path) -> Result<(usize, Vec<String>)> {
        if !dir.is_dir() {
            return Err(anyhow!("'{}' is not a directory", dir.display()));
        }

        // First, prune plugins whose files no longer exist
        let removed_infos = self.registry.prune_missing();
        let mut removed_names = Vec::new();
        for info in removed_infos {
            info!(name = %info.name, "Removed plugin (file no longer exists)");
            let _ = self.component_cache.invalidate(&info.path);
            self.function_registry.unregister_plugin(&info.name);
            removed_names.push(info.name);
        }

        let mut count = 0;
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map(|e| e == "wasm").unwrap_or(false) {
                match self.load_plugin_info(&path) {
                    Ok(info) => {
                        info!(
                            name = %info.name,
                            version = %info.version,
                            long_running = info.long_running,
                            "Registered plugin"
                        );
                        self.registry.register(info);
                        count += 1;
                    }
                    Err(e) => {
                        warn!(
                            path = %path.display(),
                            error = %e,
                            "Failed to load plugin metadata, skipping"
                        );
                    }
                }
            }
        }

        Ok((count, removed_names))
    }

    /// Load plugin metadata without executing it
    pub fn load_plugin_info(&self, path: &Path) -> Result<PluginInfo> {
        let component = self.component_cache.load(&self.engine, path)?;

        let stdout = MemoryOutputPipe::new(1024);
        let permissions = PluginPermissions::read_only();

        let mut store = Store::new(
            &self.engine,
            State::new(stdout.clone(), self.storage.clone(), permissions)?,
        );

        store.data_mut().limits = StoreLimitsBuilder::new()
            .memory_size(self.config.limits.max_memory_size)
            .table_elements(self.config.limits.max_table_elements)
            .instances(self.config.limits.max_instances)
            .tables(self.config.limits.max_tables)
            .memories(self.config.limits.max_memories)
            .build();

        store.limiter(|state| &mut state.limits);

        if self.config.epoch.enabled {
            store.epoch_deadline_trap();
            store.set_epoch_deadline(self.config.epoch.max_deadline);
        }

        if self.config.fuel.enabled {
            store.set_fuel(1_000_000)?;
        }

        let bindings = Plugin::instantiate(&mut store, &component, &self.linker)?;
        let plugin_config = bindings.api().call_config(&mut store)?;
        let plugin_name = bindings.api().call_name(&mut store)?;
        let plugin_description = bindings.api().call_description(&mut store)?;
        let plugin_version = bindings.api().call_version(&mut store)?;

        // Load and register functions from this plugin
        let functions = bindings.api().call_list_functions(&mut store)?;
        let func_count = functions.len();

        let collisions = self
            .function_registry
            .register_functions(&plugin_name, functions);

        for collision in &collisions {
            warn!(plugin = %plugin_name, "{}", collision);
        }

        if func_count > 0 {
            info!(plugin = %plugin_name, count = func_count, "Registered functions");
        }

        Ok(PluginInfo {
            name: plugin_name,
            description: plugin_description,
            version: format!(
                "{}.{}.{}",
                plugin_version.major, plugin_version.minor, plugin_version.patch
            ),
            long_running: plugin_config.long_running,
            path: path.to_path_buf(),
            size_bytes: std::fs::metadata(path)?.len(),
        })
    }

    /// List all registered plugins
    pub fn list_plugins(&self) -> Vec<PluginInfo> {
        self.registry.list()
    }

    /// Run a plugin by its registered name with full permissions (admin use)
    pub fn run_plugin_by_name(&self, name: &str) -> Result<PluginResult> {
        let info = self
            .registry
            .get(name)
            .ok_or_else(|| anyhow!("Plugin '{}' not found in registry", name))?;

        self.run_plugin(&info.path, PluginPermissions::full_access())
    }

    /// Run a plugin by name in a background thread with full permissions
    pub fn run_plugin_by_name_background(self: &Arc<Self>, name: &str) -> Result<u64> {
        let info = self
            .registry
            .get(name)
            .ok_or_else(|| anyhow!("Plugin '{}' not found in registry", name))?;

        let task_id = self.background_tasks.register(info.name.clone());
        let path = info.path.clone();
        let host = Arc::clone(self);
        let tasks = Arc::clone(&self.background_tasks);
        let plugin_name = info.name.clone();

        std::thread::spawn(move || {
            info!(task_id = task_id, name = %plugin_name, "Background plugin started");

            match host.run_plugin(&path, PluginPermissions::full_access()) {
                Ok(result) => {
                    match &result {
                        PluginResult::Success { elapsed } => {
                            info!(task_id = task_id, elapsed = ?elapsed, "Background plugin completed");
                        }
                        PluginResult::PluginError { kind, message, .. } => {
                            error!(task_id = task_id, kind = %kind, message = %message, "Background plugin error");
                        }
                        PluginResult::Trap { message } => {
                            error!(task_id = task_id, message = %message, "Background plugin trapped");
                        }
                        _ => {}
                    }
                    tasks.complete(task_id, result);
                }
                Err(e) => {
                    error!(task_id = task_id, error = %e, "Background plugin failed to run");
                    tasks.complete(
                        task_id,
                        PluginResult::Trap {
                            message: e.to_string(),
                        },
                    );
                }
            }
        });

        Ok(task_id)
    }

    /// Access the plugin registry
    pub fn registry(&self) -> &Arc<PluginRegistry> {
        &self.registry
    }

    /// Access the function registry
    pub fn function_registry(&self) -> &Arc<FunctionRegistry> {
        &self.function_registry
    }

    /// Get component cache statistics
    pub fn cache_stats(&self) -> crate::component_cache::CacheStats {
        self.component_cache.stats()
    }

    /// Clear the component cache
    pub fn clear_cache(&self) -> Result<()> {
        self.component_cache.clear()
    }

    /// Invalidate a specific plugin from cache
    pub fn invalidate_plugin(&self, path: &Path) -> Result<()> {
        self.component_cache.invalidate(path)
    }

    /// List all registered functions
    pub fn list_functions(&self) -> Vec<RegisteredFunction> {
        self.function_registry.list()
    }

    /// Get a function by name
    pub fn get_function(&self, name: &str) -> Option<RegisteredFunction> {
        self.function_registry.get(name)
    }

    /// Call a function by name with the given arguments and user permissions.
    ///
    /// The `user_permissions` parameter ensures the plugin can only access
    /// resources that the calling user is allowed to access. This prevents
    /// privilege escalation through plugin invocation.
    pub fn call_function(
        &self,
        name: &str,
        args: Vec<MonoValue>,
        user_permissions: PluginPermissions,
    ) -> Result<MonoValue> {
        let func = self
            .function_registry
            .get(name)
            .ok_or_else(|| anyhow!("Function '{}' not found", name))?;

        let plugin_info = self
            .registry
            .get(&func.plugin_name)
            .ok_or_else(|| anyhow!("Plugin '{}' not found", func.plugin_name))?;

        // Load the plugin
        let component = self.component_cache.load(&self.engine, &plugin_info.path)?;

        let stdout = MemoryOutputPipe::new(1024);

        let mut store = Store::new(
            &self.engine,
            State::new(stdout.clone(), self.storage.clone(), user_permissions)?,
        );

        store.data_mut().limits = StoreLimitsBuilder::new()
            .memory_size(self.config.limits.max_memory_size)
            .table_elements(self.config.limits.max_table_elements)
            .instances(self.config.limits.max_instances)
            .tables(self.config.limits.max_tables)
            .memories(self.config.limits.max_memories)
            .build();

        store.limiter(|state| &mut state.limits);

        if self.config.epoch.enabled {
            store.epoch_deadline_trap();
            store.set_epoch_deadline(self.config.epoch.default_deadline);
        }

        if self.config.fuel.enabled {
            store.set_fuel(self.config.fuel.default_fuel)?;
        }

        let bindings = Plugin::instantiate(&mut store, &component, &self.linker)?;

        // Convert arguments to WIT format
        let wit_args: Vec<Vec<u8>> = args.iter().map(|v| v.to_bytes()).collect();

        // Call the function
        let result = bindings
            .api()
            .call_call_function(&mut store, name, &wit_args)?;

        match result {
            Ok(bytes) => {
                let (value, _) = MonoValue::from_bytes(&bytes)
                    .map_err(|e| anyhow!("Failed to deserialize result: {}", e))?;
                Ok(value)
            }
            Err(e) => Err(anyhow!(
                "Function error: {} - {}",
                e.message,
                e.details.unwrap_or_default()
            )),
        }
    }

    /// Check if a function exists in the registry
    pub fn has_function(&self, name: &str) -> bool {
        self.function_registry.get(name).is_some()
    }
}

impl Drop for PluginHost {
    fn drop(&mut self) {
        self.epoch_thread_running.store(false, Ordering::SeqCst);
    }
}
