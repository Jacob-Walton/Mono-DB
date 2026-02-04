//! Component cache

use anyhow::{Context, Result};
use memmap2::Mmap;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tracing::{debug, info, warn};
use wasmtime::Engine;
use wasmtime::component::Component;

/// Cached component
struct CachedComponent {
    component: Component,
    modified: SystemTime,
}

/// Cache for compiled WASM components
pub struct ComponentCache {
    /// Directory for storing compiled .cwasm files
    cache_dir: PathBuf,
    /// In-memory cache of loaded components (keyed by canonical path)
    components: RwLock<HashMap<PathBuf, CachedComponent>>,
    /// Whether to persist compiled components to disk
    persist: bool,
}

impl ComponentCache {
    /// Create a new component cache
    ///
    /// If `cache_dir` is None, uses a temp directory.
    /// If `persist` is true, compiled components are saved to disk.
    pub fn new(cache_dir: Option<PathBuf>, persist: bool) -> Result<Self> {
        let cache_dir =
            cache_dir.unwrap_or_else(|| std::env::temp_dir().join("monodb-component-cache"));

        if persist {
            fs::create_dir_all(&cache_dir)
                .with_context(|| format!("Failed to create cache dir: {}", cache_dir.display()))?;
        }

        Ok(Self {
            cache_dir,
            components: RwLock::new(HashMap::new()),
            persist,
        })
    }

    /// Load a component, using cache if available
    ///
    /// Loading strategy:
    /// 1. Check in-memory cache (verify not stale)
    /// 2. Check disk cache (.cwasm), mmap if exists and valid
    /// 3. Load raw .wasm via mmap, compile, cache result
    pub fn load(&self, engine: &Engine, wasm_path: &Path) -> Result<Component> {
        let canonical = wasm_path
            .canonicalize()
            .with_context(|| format!("Failed to canonicalize: {}", wasm_path.display()))?;

        let wasm_modified = fs::metadata(&canonical)?.modified()?;

        // Check in-memory cache (verify not stale)
        {
            let cache = self.components.read();
            if let Some(cached) = cache.get(&canonical) {
                if cached.modified >= wasm_modified {
                    debug!(path = %wasm_path.display(), "Component cache hit (memory)");
                    return Ok(cached.component.clone());
                }
                debug!(path = %wasm_path.display(), "Memory cache stale, reloading");
            }
        }

        // Try loading from disk cache
        if self.persist
            && let Some(component) = self.try_load_cached(engine, &canonical)?
        {
            // Store in memory cache with modification time
            self.components.write().insert(
                canonical.clone(),
                CachedComponent {
                    component: component.clone(),
                    modified: wasm_modified,
                },
            );
            return Ok(component);
        }

        // Load fresh via mmap
        let component = self.load_and_compile(engine, &canonical)?;

        // Cache to disk if enabled
        if self.persist
            && let Err(e) = self.save_to_cache(engine, &canonical, &component)
        {
            warn!(
                path = %wasm_path.display(),
                error = %e,
                "Failed to cache compiled component"
            );
        }

        // Store in memory cache with modification time
        self.components.write().insert(
            canonical,
            CachedComponent {
                component: component.clone(),
                modified: wasm_modified,
            },
        );

        Ok(component)
    }

    /// Load and compile a WASM file using mmap
    fn load_and_compile(&self, engine: &Engine, wasm_path: &Path) -> Result<Component> {
        let file = File::open(wasm_path)
            .with_context(|| format!("Failed to open: {}", wasm_path.display()))?;

        // SAFETY: We're memory-mapping a file we just opened for reading.
        // The file should not be modified while mapped.
        let mmap = unsafe { Mmap::map(&file) }
            .with_context(|| format!("Failed to mmap: {}", wasm_path.display()))?;

        debug!(
            path = %wasm_path.display(),
            size_bytes = mmap.len(),
            "Loading component via mmap"
        );

        let component = Component::from_binary(engine, &mmap)
            .with_context(|| format!("Failed to compile: {}", wasm_path.display()))?;

        info!(
            path = %wasm_path.display(),
            "Compiled component from mmap'd WASM"
        );

        Ok(component)
    }

    /// Try to load a cached compiled component
    fn try_load_cached(&self, engine: &Engine, wasm_path: &Path) -> Result<Option<Component>> {
        let cache_path = self.cache_path_for(wasm_path);

        if !cache_path.exists() {
            return Ok(None);
        }

        // Check if cache is stale (wasm file is newer)
        let wasm_modified = fs::metadata(wasm_path)?.modified()?;
        let cache_modified = fs::metadata(&cache_path)?.modified()?;

        if wasm_modified > cache_modified {
            debug!(
                path = %wasm_path.display(),
                "Cache stale, recompiling"
            );
            // Remove stale cache
            let _ = fs::remove_file(&cache_path);
            return Ok(None);
        }

        // SAFETY: deserialize_file is unsafe because it mmaps the file and
        // trusts the contents. We only load files we created.
        match unsafe { Component::deserialize_file(engine, &cache_path) } {
            Ok(component) => {
                debug!(
                    path = %wasm_path.display(),
                    cache = %cache_path.display(),
                    "Component cache hit (disk)"
                );
                Ok(Some(component))
            }
            Err(e) => {
                warn!(
                    cache = %cache_path.display(),
                    error = %e,
                    "Failed to load cached component, recompiling"
                );
                // Remove invalid cache
                let _ = fs::remove_file(&cache_path);
                Ok(None)
            }
        }
    }

    /// Save a compiled component to disk cache
    fn save_to_cache(
        &self,
        _engine: &Engine,
        wasm_path: &Path,
        component: &Component,
    ) -> Result<()> {
        let cache_path = self.cache_path_for(wasm_path);

        // Ensure parent directory exists
        if let Some(parent) = cache_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let serialized = component
            .serialize()
            .context("Failed to serialize component")?;

        // Write atomically via temp file
        let temp_path = cache_path.with_extension("cwasm.tmp");
        let mut file = File::create(&temp_path)?;
        file.write_all(&serialized)?;
        file.sync_all()?;

        fs::rename(&temp_path, &cache_path)?;

        debug!(
            wasm = %wasm_path.display(),
            cache = %cache_path.display(),
            size_bytes = serialized.len(),
            "Cached compiled component"
        );

        Ok(())
    }

    /// Get the cache file path for a given WASM file
    fn cache_path_for(&self, wasm_path: &Path) -> PathBuf {
        // Create a unique filename based on the path
        let hash = {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            wasm_path.hash(&mut hasher);
            hasher.finish()
        };

        let stem = wasm_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");

        self.cache_dir.join(format!("{}-{:016x}.cwasm", stem, hash))
    }

    /// Clear all cached components (memory and disk)
    pub fn clear(&self) -> Result<()> {
        self.components.write().clear();

        if self.persist && self.cache_dir.exists() {
            for entry in fs::read_dir(&self.cache_dir)? {
                let entry = entry?;
                if entry
                    .path()
                    .extension()
                    .map(|e| e == "cwasm")
                    .unwrap_or(false)
                {
                    fs::remove_file(entry.path())?;
                }
            }
        }

        info!("Cleared component cache");
        Ok(())
    }

    /// Invalidate a specific component from cache
    pub fn invalidate(&self, wasm_path: &Path) -> Result<()> {
        if let Ok(canonical) = wasm_path.canonicalize() {
            self.components.write().remove(&canonical);

            let cache_path = self.cache_path_for(&canonical);
            if cache_path.exists() {
                fs::remove_file(&cache_path)?;
            }
        }

        Ok(())
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let memory_count = self.components.read().len();

        let disk_count = if self.persist && self.cache_dir.exists() {
            fs::read_dir(&self.cache_dir)
                .map(|entries| {
                    entries
                        .filter_map(|e| e.ok())
                        .filter(|e| {
                            e.path()
                                .extension()
                                .map(|ext| ext == "cwasm")
                                .unwrap_or(false)
                        })
                        .count()
                })
                .unwrap_or(0)
        } else {
            0
        };

        let disk_bytes = if self.persist && self.cache_dir.exists() {
            fs::read_dir(&self.cache_dir)
                .map(|entries| {
                    entries
                        .filter_map(|e| e.ok())
                        .filter(|e| {
                            e.path()
                                .extension()
                                .map(|ext| ext == "cwasm")
                                .unwrap_or(false)
                        })
                        .filter_map(|e| e.metadata().ok())
                        .map(|m| m.len())
                        .sum()
                })
                .unwrap_or(0)
        } else {
            0
        };

        CacheStats {
            memory_count,
            disk_count,
            disk_bytes,
            cache_dir: self.cache_dir.clone(),
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub memory_count: usize,
    pub disk_count: usize,
    pub disk_bytes: u64,
    pub cache_dir: PathBuf,
}
