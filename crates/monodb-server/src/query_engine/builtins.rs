#![allow(dead_code)]

//! Built-in function registry
//!
//! Defines the standard library of functions available in the query language.
//! These are evaluated at runtime by the executor.

use monodb_common::ValueType;

/// Information about a function parameter.
#[derive(Debug, Clone)]
pub struct ParamInfo {
    /// Parameter name (for documentation)
    pub name: &'static str,
    /// Expected type (None means any type)
    pub expected_type: Option<ValueType>,
    /// Whether this parameter is optional
    pub optional: bool,
}

impl ParamInfo {
    pub const fn required(name: &'static str, ty: ValueType) -> Self {
        Self {
            name,
            expected_type: Some(ty),
            optional: false,
        }
    }

    pub const fn optional(name: &'static str, ty: ValueType) -> Self {
        Self {
            name,
            expected_type: Some(ty),
            optional: true,
        }
    }

    pub const fn any(name: &'static str) -> Self {
        Self {
            name,
            expected_type: None,
            optional: false,
        }
    }

    pub const fn any_optional(name: &'static str) -> Self {
        Self {
            name,
            expected_type: None,
            optional: true,
        }
    }
}

/// Information about a built-in function.
#[derive(Debug, Clone)]
pub struct BuiltinFunctionInfo {
    /// Function name
    pub name: &'static str,
    /// Alternative names (aliases)
    pub aliases: &'static [&'static str],
    /// Description for documentation
    pub description: &'static str,
    /// Parameter definitions
    pub params: &'static [ParamInfo],
    /// Return type (None means depends on input types)
    pub return_type: Option<ValueType>,
    /// Whether the function is deterministic (same inputs = same output)
    pub deterministic: bool,
    /// Minimum required arguments
    pub min_args: usize,
    /// Maximum allowed arguments (None = unlimited for variadic)
    pub max_args: Option<usize>,
}

impl BuiltinFunctionInfo {
    /// Check if the given argument count is valid
    pub fn check_arity(&self, arg_count: usize) -> Result<(), String> {
        if arg_count < self.min_args {
            return Err(format!(
                "Function '{}' requires at least {} argument(s), got {}",
                self.name, self.min_args, arg_count
            ));
        }
        if let Some(max) = self.max_args
            && arg_count > max
        {
            return Err(format!(
                "Function '{}' accepts at most {} argument(s), got {}",
                self.name, max, arg_count
            ));
        }
        Ok(())
    }

    /// Check if a given type can be coerced to the expected param type
    pub fn check_arg_type(&self, arg_index: usize, arg_type: &ValueType) -> Result<(), String> {
        if arg_index >= self.params.len() {
            // For variadic functions, no type checking beyond defined params
            return Ok(());
        }

        let param = &self.params[arg_index];
        if let Some(expected) = &param.expected_type
            && !arg_type.can_coerce_to(expected)
            && *arg_type != ValueType::Null
        {
            return Err(format!(
                "Function '{}' expects {} for parameter '{}', got {}",
                self.name,
                expected.display_name(),
                param.name,
                arg_type.display_name()
            ));
        }
        Ok(())
    }
}

// Built-in function definitions

/// String functions
pub static UPPER: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "upper",
    aliases: &["uppercase"],
    description: "Convert string to uppercase",
    params: &[ParamInfo::required("value", ValueType::String)],
    return_type: Some(ValueType::String),
    deterministic: true,
    min_args: 1,
    max_args: Some(1),
};

pub static LOWER: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "lower",
    aliases: &["lowercase"],
    description: "Convert string to lowercase",
    params: &[ParamInfo::required("value", ValueType::String)],
    return_type: Some(ValueType::String),
    deterministic: true,
    min_args: 1,
    max_args: Some(1),
};

pub static LENGTH: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "length",
    aliases: &["len"],
    description: "Get length of string or array",
    params: &[ParamInfo::any("value")],
    return_type: Some(ValueType::Int64),
    deterministic: true,
    min_args: 1,
    max_args: Some(1),
};

pub static CONCAT: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "concat",
    aliases: &[],
    description: "Concatenate strings",
    params: &[
        ParamInfo::required("first", ValueType::String),
        ParamInfo::required("second", ValueType::String),
    ],
    return_type: Some(ValueType::String),
    deterministic: true,
    min_args: 2,
    max_args: None, // Variadic
};

pub static SUBSTRING: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "substring",
    aliases: &["substr"],
    description: "Extract substring from start index with optional length",
    params: &[
        ParamInfo::required("value", ValueType::String),
        ParamInfo::required("start", ValueType::Int64),
        ParamInfo::optional("length", ValueType::Int64),
    ],
    return_type: Some(ValueType::String),
    deterministic: true,
    min_args: 2,
    max_args: Some(3),
};

pub static TRIM: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "trim",
    aliases: &[],
    description: "Remove leading and trailing whitespace",
    params: &[ParamInfo::required("value", ValueType::String)],
    return_type: Some(ValueType::String),
    deterministic: true,
    min_args: 1,
    max_args: Some(1),
};

pub static REPLACE: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "replace",
    aliases: &[],
    description: "Replace occurrences of a substring",
    params: &[
        ParamInfo::required("value", ValueType::String),
        ParamInfo::required("search", ValueType::String),
        ParamInfo::required("replacement", ValueType::String),
    ],
    return_type: Some(ValueType::String),
    deterministic: true,
    min_args: 3,
    max_args: Some(3),
};

/// Numeric functions
pub static ABS: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "abs",
    aliases: &[],
    description: "Absolute value",
    params: &[ParamInfo::any("value")], // Accepts any numeric
    return_type: None,                  // Returns same type as input
    deterministic: true,
    min_args: 1,
    max_args: Some(1),
};

pub static FLOOR: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "floor",
    aliases: &[],
    description: "Round down to nearest integer",
    params: &[ParamInfo::required("value", ValueType::Float64)],
    return_type: Some(ValueType::Int64),
    deterministic: true,
    min_args: 1,
    max_args: Some(1),
};

pub static CEIL: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "ceil",
    aliases: &["ceiling"],
    description: "Round up to nearest integer",
    params: &[ParamInfo::required("value", ValueType::Float64)],
    return_type: Some(ValueType::Int64),
    deterministic: true,
    min_args: 1,
    max_args: Some(1),
};

pub static ROUND: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "round",
    aliases: &[],
    description: "Round to nearest integer or specified decimal places",
    params: &[
        ParamInfo::required("value", ValueType::Float64),
        ParamInfo::optional("decimals", ValueType::Int64),
    ],
    return_type: Some(ValueType::Float64),
    deterministic: true,
    min_args: 1,
    max_args: Some(2),
};

pub static SQRT: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "sqrt",
    aliases: &[],
    description: "Square root",
    params: &[ParamInfo::required("value", ValueType::Float64)],
    return_type: Some(ValueType::Float64),
    deterministic: true,
    min_args: 1,
    max_args: Some(1),
};

pub static POW: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "pow",
    aliases: &["power"],
    description: "Raise to power",
    params: &[
        ParamInfo::required("base", ValueType::Float64),
        ParamInfo::required("exponent", ValueType::Float64),
    ],
    return_type: Some(ValueType::Float64),
    deterministic: true,
    min_args: 2,
    max_args: Some(2),
};

/// Null handling functions
pub static COALESCE: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "coalesce",
    aliases: &[],
    description: "Return first non-null value",
    params: &[ParamInfo::any("values")],
    return_type: None, // Returns type of first non-null
    deterministic: true,
    min_args: 1,
    max_args: None, // Variadic
};

pub static NULLIF: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "nullif",
    aliases: &[],
    description: "Return null if values are equal",
    params: &[ParamInfo::any("value1"), ParamInfo::any("value2")],
    return_type: None, // Returns type of first arg
    deterministic: true,
    min_args: 2,
    max_args: Some(2),
};

pub static IFNULL: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "ifnull",
    aliases: &["nvl"],
    description: "Return second value if first is null",
    params: &[ParamInfo::any("value"), ParamInfo::any("default")],
    return_type: None, // Returns common type
    deterministic: true,
    min_args: 2,
    max_args: Some(2),
};

/// Date/Time functions
pub static NOW: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "now",
    aliases: &["current_timestamp"],
    description: "Current date and time",
    params: &[],
    return_type: Some(ValueType::DateTime),
    deterministic: false, // Returns different value each call
    min_args: 0,
    max_args: Some(0),
};

pub static DATE: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "date",
    aliases: &[],
    description: "Extract date from datetime or parse date string",
    params: &[ParamInfo::any("value")],
    return_type: Some(ValueType::Date),
    deterministic: true,
    min_args: 1,
    max_args: Some(1),
};

pub static TIME: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "time",
    aliases: &[],
    description: "Extract time from datetime or parse time string",
    params: &[ParamInfo::any("value")],
    return_type: Some(ValueType::Time),
    deterministic: true,
    min_args: 1,
    max_args: Some(1),
};

pub static YEAR: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "year",
    aliases: &[],
    description: "Extract year from date/datetime",
    params: &[ParamInfo::any("value")],
    return_type: Some(ValueType::Int64),
    deterministic: true,
    min_args: 1,
    max_args: Some(1),
};

pub static MONTH: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "month",
    aliases: &[],
    description: "Extract month from date/datetime",
    params: &[ParamInfo::any("value")],
    return_type: Some(ValueType::Int64),
    deterministic: true,
    min_args: 1,
    max_args: Some(1),
};

pub static DAY: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "day",
    aliases: &[],
    description: "Extract day from date/datetime",
    params: &[ParamInfo::any("value")],
    return_type: Some(ValueType::Int64),
    deterministic: true,
    min_args: 1,
    max_args: Some(1),
};

/// Type functions
pub static TYPEOF: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "typeof",
    aliases: &["type"],
    description: "Get the type name of a value",
    params: &[ParamInfo::any("value")],
    return_type: Some(ValueType::String),
    deterministic: true,
    min_args: 1,
    max_args: Some(1),
};

pub static CAST: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "cast",
    aliases: &[],
    description: "Convert value to specified type",
    params: &[
        ParamInfo::any("value"),
        ParamInfo::required("type_name", ValueType::String),
    ],
    return_type: None, // Depends on target type
    deterministic: true,
    min_args: 2,
    max_args: Some(2),
};

/// UUID functions
pub static UUID_GENERATE: BuiltinFunctionInfo = BuiltinFunctionInfo {
    name: "uuid",
    aliases: &["uuid_generate", "gen_uuid"],
    description: "Generate a new UUID",
    params: &[],
    return_type: Some(ValueType::Uuid),
    deterministic: false,
    min_args: 0,
    max_args: Some(0),
};

/// All built-in functions
pub static BUILTINS: &[&BuiltinFunctionInfo] = &[
    // String
    &UPPER,
    &LOWER,
    &LENGTH,
    &CONCAT,
    &SUBSTRING,
    &TRIM,
    &REPLACE,
    // Numeric
    &ABS,
    &FLOOR,
    &CEIL,
    &ROUND,
    &SQRT,
    &POW,
    // Null handling
    &COALESCE,
    &NULLIF,
    &IFNULL,
    // Date/Time
    &NOW,
    &DATE,
    &TIME,
    &YEAR,
    &MONTH,
    &DAY,
    // Type
    &TYPEOF,
    &CAST,
    // UUID
    &UUID_GENERATE,
];

/// Look up a built-in function by name (including aliases)
pub fn lookup_builtin(name: &str) -> Option<&'static BuiltinFunctionInfo> {
    let name_lower = name.to_lowercase();

    for func in BUILTINS {
        if func.name == name_lower {
            return Some(func);
        }
        for alias in func.aliases {
            if *alias == name_lower {
                return Some(func);
            }
        }
    }
    None
}

/// Check if a name is a built-in function
pub fn is_builtin(name: &str) -> bool {
    lookup_builtin(name).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lookup_builtin() {
        assert!(lookup_builtin("upper").is_some());
        assert!(lookup_builtin("UPPER").is_some());
        assert!(lookup_builtin("uppercase").is_some());
        assert!(lookup_builtin("nonexistent").is_none());
    }

    #[test]
    fn test_arity_check() {
        let upper = lookup_builtin("upper").unwrap();
        assert!(upper.check_arity(1).is_ok());
        assert!(upper.check_arity(0).is_err());
        assert!(upper.check_arity(2).is_err());

        let concat = lookup_builtin("concat").unwrap();
        assert!(concat.check_arity(2).is_ok());
        assert!(concat.check_arity(5).is_ok()); // Variadic
        assert!(concat.check_arity(1).is_err());
    }

    #[test]
    fn test_type_check() {
        let upper = lookup_builtin("upper").unwrap();
        assert!(upper.check_arg_type(0, &ValueType::String).is_ok());
        assert!(upper.check_arg_type(0, &ValueType::Int64).is_err());
    }
}
