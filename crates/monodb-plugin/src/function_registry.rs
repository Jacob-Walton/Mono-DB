//! Function registry for plugin-provided UDFs
//!
//! Tracks which functions are available from which plugins and provides
//! a unified interface for calling them.

use crate::bindings::monodb::plugin::types::{FunctionInfo, ValueType};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Registered function with its source plugin
#[derive(Debug, Clone)]
pub struct RegisteredFunction {
    /// Function metadata
    pub info: FunctionInfoHost,
    /// Name of the plugin that provides this function
    pub plugin_name: String,
}

/// Host-side representation of function info
#[derive(Debug, Clone)]
pub struct FunctionInfoHost {
    pub name: String,
    pub description: String,
    pub params: Vec<ParamInfoHost>,
    pub return_type: String,
    pub deterministic: bool,
    pub aggregate: bool,
}

/// Host-side representation of parameter info
#[derive(Debug, Clone)]
pub struct ParamInfoHost {
    pub name: String,
    pub param_type: String,
    pub optional: bool,
}

impl From<&FunctionInfo> for FunctionInfoHost {
    fn from(info: &FunctionInfo) -> Self {
        Self {
            name: info.name.clone(),
            description: info.description.clone(),
            params: info
                .params
                .iter()
                .map(|p| ParamInfoHost {
                    name: p.name.clone(),
                    param_type: value_type_to_string(&p.param_type),
                    optional: p.optional,
                })
                .collect(),
            return_type: value_type_to_string(&info.return_type),
            deterministic: info.deterministic,
            aggregate: info.aggregate,
        }
    }
}

fn value_type_to_string(vt: &ValueType) -> String {
    match vt {
        ValueType::TypeNull => "null",
        ValueType::TypeBool => "bool",
        ValueType::TypeInt32 => "int32",
        ValueType::TypeInt64 => "int64",
        ValueType::TypeFloat32 => "float32",
        ValueType::TypeFloat64 => "float64",
        ValueType::TypeString => "string",
        ValueType::TypeBinary => "binary",
        ValueType::TypeArray => "array",
        ValueType::TypeObject => "object",
        ValueType::TypeUuid => "uuid",
        ValueType::TypeDatetime => "datetime",
        ValueType::TypeGeopoint => "geopoint",
        ValueType::TypeAny => "any",
    }
    .to_string()
}

/// Registry of all available functions from all plugins
pub struct FunctionRegistry {
    /// Functions indexed by name
    functions: RwLock<HashMap<String, RegisteredFunction>>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Self {
            functions: RwLock::new(HashMap::new()),
        }
    }

    /// Register functions from a plugin, returning any name collisions
    pub fn register_functions(
        &self,
        plugin_name: &str,
        functions: Vec<FunctionInfo>,
    ) -> Vec<String> {
        let mut registry = self.functions.write();
        let mut collisions = Vec::new();

        for func in functions {
            let key = func.name.to_lowercase();

            // Check for collision with another plugin's function
            if let Some(existing) = registry.get(&key)
                && existing.plugin_name != plugin_name
            {
                collisions.push(format!(
                    "Function '{}' already registered by plugin '{}'",
                    func.name, existing.plugin_name
                ));
                continue;
            }

            registry.insert(
                key,
                RegisteredFunction {
                    info: FunctionInfoHost::from(&func),
                    plugin_name: plugin_name.to_string(),
                },
            );
        }

        collisions
    }

    /// Unregister all functions from a plugin
    pub fn unregister_plugin(&self, plugin_name: &str) {
        let mut registry = self.functions.write();
        registry.retain(|_, v| v.plugin_name != plugin_name);
    }

    /// Look up a function by name
    pub fn get(&self, name: &str) -> Option<RegisteredFunction> {
        self.functions.read().get(&name.to_lowercase()).cloned()
    }

    /// List all registered functions
    pub fn list(&self) -> Vec<RegisteredFunction> {
        self.functions.read().values().cloned().collect()
    }

    /// List functions from a specific plugin
    pub fn list_by_plugin(&self, plugin_name: &str) -> Vec<RegisteredFunction> {
        self.functions
            .read()
            .values()
            .filter(|f| f.plugin_name == plugin_name)
            .cloned()
            .collect()
    }

    /// Check if a function exists
    pub fn exists(&self, name: &str) -> bool {
        self.functions.read().contains_key(&name.to_lowercase())
    }

    /// Get count of registered functions
    pub fn count(&self) -> usize {
        self.functions.read().len()
    }

    /// Get a snapshot of the current registry for atomic reads
    pub fn snapshot(&self) -> Arc<HashMap<String, RegisteredFunction>> {
        Arc::new(self.functions.read().clone())
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}
