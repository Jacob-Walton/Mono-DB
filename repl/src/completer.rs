use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Context, Helper, Result};

// TODO: Implement SQL support

pub struct NsqlCompleter {
    query_keywords: Vec<&'static str>,
    command_keywords: Vec<&'static str>,
    condition_keywords: Vec<&'static str>,
    data_types: Vec<&'static str>,
    functions: Vec<&'static str>,
    tables: Vec<String>,
}

impl NsqlCompleter {
    pub fn new() -> Self {
        Self {
            query_keywords: vec![
                "FIND",
                "SHOW",
                "GET",
                "COUNT",
                "ASK",
                "HOW MANY",
                "SHOW ME",
                "FIND ALL",
                "GET ALL",
                "COUNT ALL",
                "WITH",
                "RECURSIVE",
                "AS",
                "FROM",
                "IN",
                "ORDER BY",
                "SORT BY",
                "GROUP BY",
                "LIMIT",
                "OFFSET",
                "TOP",
                "FIRST",
                "ROWS",
                "DISTINCT",
                "ALL",
                "EVERYTHING",
            ],
            command_keywords: vec![
                "ADD",
                "CREATE",
                "CHANGE",
                "UPDATE",
                "MODIFY",
                "DELETE",
                "REMOVE",
                "MERGE",
                "TO",
                "SET",
                "ADD NEW",
                "CREATE NEW",
                "REMOVE ALL",
                "WITH",
                "ON DUPLICATE",
                "SKIP",
                "REPLACE",
                "RETURNING",
                "VALUES",
                "RECORDS FROM",
                "DATA FROM",
            ],
            condition_keywords: vec![
                "WHERE",
                "WHEN",
                "IF",
                "THAT",
                "WHICH",
                "HAVING",
                "AND",
                "OR",
                "NOT",
                "BETWEEN",
                "LIKE",
                "IN",
                "IS",
                "IS NOT",
                "IS NULL",
                "IS NOT NULL",
                "EXISTS",
                "NOT EXISTS",
                "EMPTY",
                "MISSING",
                "TODAY",
                "YESTERDAY",
                "TOMORROW",
                "NOW",
            ],
            data_types: vec![
                "INTEGER",
                "DECIMAL",
                "FLOAT",
                "DOUBLE",
                "TEXT",
                "STRING",
                "DATE",
                "TIME",
                "DATETIME",
                "BOOLEAN",
                "BINARY",
                "BLOB",
                "JSON",
                "LIST",
                "ARRAY",
                "MAP",
                "NODE",
                "EDGE",
                "OBJECT",
                "REQUIRED",
                "UNIQUE",
                "PRIMARY",
                "REFERENCES",
                "CHECK",
                "ENCRYPTED",
                "DEFAULT",
                "PRIMARY KEY",
                "FOREIGN KEY",
            ],
            functions: vec![
                "SUM",
                "AVG",
                "MIN",
                "MAX",
                "COUNT",
                "LENGTH",
                "UPPER",
                "LOWER",
                "TRIM",
                "SUBSTRING",
                "REPLACE",
                "CONCAT",
                "YEAR",
                "MONTH",
                "DAY",
                "HOUR",
                "DATE_ADD",
                "DATE_DIFF",
                "ROUND",
                "FLOOR",
                "CEILING",
                "ABS",
                "AGE",
            ],
            tables: vec![
                "users".to_string(),
                "orders".to_string(),
                "products".to_string(),
                "customers".to_string(),
                "inventory".to_string(),
            ],
        }
    }

    fn get_context(&self, line: &str, pos: usize) -> CompletionContext {
        let line_to_pos = &line[..pos].to_uppercase();

        // Analyze the context based on what's in the line
        if line_to_pos.is_empty() {
            return CompletionContext::Start;
        }

        // Check for natural language query starters
        if line_to_pos.starts_with("FIND")
            || line_to_pos.starts_with("SHOW")
            || line_to_pos.starts_with("GET")
        {
            if line_to_pos.contains(" FROM ") || line_to_pos.contains(" IN ") {
                if self.has_condition_starter(line_to_pos) {
                    CompletionContext::Condition
                } else {
                    CompletionContext::AfterFrom
                }
            } else if line_to_pos.ends_with(" ")
                && !line_to_pos.contains(" FROM ")
                && !line_to_pos.contains(" IN ")
            {
                CompletionContext::NeedFromOrIn
            } else {
                CompletionContext::SelectItems
            }
        } else if line_to_pos.starts_with("COUNT") || line_to_pos.starts_with("HOW MANY") {
            if line_to_pos.contains(" IN ") {
                if self.has_condition_starter(line_to_pos) {
                    CompletionContext::Condition
                } else {
                    CompletionContext::AfterFrom
                }
            } else {
                CompletionContext::CountTarget
            }
        } else if line_to_pos.starts_with("ASK") {
            if line_to_pos.contains("ASK IF") {
                CompletionContext::Condition
            } else {
                CompletionContext::AskIf
            }
        } else if line_to_pos.starts_with("ADD") || line_to_pos.starts_with("CREATE") {
            if line_to_pos.contains(" TO ") {
                CompletionContext::Complete
            } else if line_to_pos.contains(" WITH ") {
                CompletionContext::FieldAssignment
            } else {
                CompletionContext::AddWhat
            }
        } else if line_to_pos.starts_with("UPDATE")
            || line_to_pos.starts_with("CHANGE")
            || line_to_pos.starts_with("MODIFY")
        {
            if line_to_pos.contains(" SET ") {
                if self.has_condition_starter(line_to_pos) {
                    CompletionContext::Condition
                } else {
                    CompletionContext::FieldAssignment
                }
            } else {
                CompletionContext::TableName
            }
        } else if line_to_pos.starts_with("DELETE") || line_to_pos.starts_with("REMOVE") {
            if self.has_condition_starter(line_to_pos) {
                CompletionContext::Condition
            } else {
                CompletionContext::DeleteFrom
            }
        } else if line_to_pos.starts_with("CREATE TABLE")
            || line_to_pos.starts_with("CREATE COLLECTION")
        {
            CompletionContext::DDL
        } else if self.has_condition_starter(line_to_pos) {
            CompletionContext::Condition
        } else {
            CompletionContext::Start
        }
    }

    fn has_condition_starter(&self, line: &str) -> bool {
        ["WHERE", "WHEN", "IF", "THAT", "WHICH", "HAVING"]
            .iter()
            .any(|keyword| line.contains(keyword))
    }

    fn get_completions(&self, line: &str, pos: usize) -> Vec<Pair> {
        let mut completions = Vec::new();

        // Get the word being completed
        let start = line[..pos]
            .rfind(|c: char| c.is_whitespace() || "(),;".contains(c))
            .map(|i| i + 1)
            .unwrap_or(0);

        let partial = &line[start..pos];
        let partial_upper = partial.to_uppercase();

        let context = self.get_context(line, pos);

        match context {
            CompletionContext::Start => {
                // Query starters
                for keyword in &self.query_keywords {
                    if keyword.starts_with(&partial_upper) {
                        completions.push(Pair {
                            display: keyword.to_string(),
                            replacement: keyword.to_string(),
                        });
                    }
                }

                // Command starters
                for keyword in &self.command_keywords {
                    if keyword.starts_with(&partial_upper) {
                        completions.push(Pair {
                            display: keyword.to_string(),
                            replacement: keyword.to_string(),
                        });
                    }
                }
            }

            CompletionContext::SelectItems => {
                // Field names, functions, and common items
                completions.extend([
                    Pair {
                        display: "*".to_string(),
                        replacement: "*".to_string(),
                    },
                    Pair {
                        display: "ALL".to_string(),
                        replacement: "ALL".to_string(),
                    },
                    Pair {
                        display: "EVERYTHING".to_string(),
                        replacement: "EVERYTHING".to_string(),
                    },
                ]);

                for func in &self.functions {
                    if func.starts_with(&partial_upper) {
                        completions.push(Pair {
                            display: format!("{}()", func),
                            replacement: format!("{}(", func),
                        });
                    }
                }
            }

            CompletionContext::NeedFromOrIn => {
                completions.extend([
                    Pair {
                        display: "FROM".to_string(),
                        replacement: "FROM".to_string(),
                    },
                    Pair {
                        display: "IN".to_string(),
                        replacement: "IN".to_string(),
                    },
                ]);
            }

            CompletionContext::AfterFrom | CompletionContext::TableName => {
                for table in &self.tables {
                    if table.starts_with(&partial.to_lowercase()) {
                        completions.push(Pair {
                            display: table.clone(),
                            replacement: table.clone(),
                        });
                    }
                }
            }

            CompletionContext::Condition => {
                for keyword in &self.condition_keywords {
                    if keyword.starts_with(&partial_upper) {
                        completions.push(Pair {
                            display: keyword.to_string(),
                            replacement: keyword.to_string(),
                        });
                    }
                }

                // Add comparison operators
                if partial.is_empty() || "=<>!".contains(&partial_upper) {
                    completions.extend([
                        Pair {
                            display: "=".to_string(),
                            replacement: "=".to_string(),
                        },
                        Pair {
                            display: "!=".to_string(),
                            replacement: "!=".to_string(),
                        },
                        Pair {
                            display: "<".to_string(),
                            replacement: "<".to_string(),
                        },
                        Pair {
                            display: ">".to_string(),
                            replacement: ">".to_string(),
                        },
                        Pair {
                            display: "<=".to_string(),
                            replacement: "<=".to_string(),
                        },
                        Pair {
                            display: ">=".to_string(),
                            replacement: ">=".to_string(),
                        },
                    ]);
                }
            }

            CompletionContext::CountTarget => {
                completions.extend([
                    Pair {
                        display: "*".to_string(),
                        replacement: "*".to_string(),
                    },
                    Pair {
                        display: "ALL".to_string(),
                        replacement: "ALL".to_string(),
                    },
                ]);

                for table in &self.tables {
                    if table.starts_with(&partial.to_lowercase()) {
                        completions.push(Pair {
                            display: table.clone(),
                            replacement: table.clone(),
                        });
                    }
                }
            }

            CompletionContext::AskIf => {
                completions.push(Pair {
                    display: "IF".to_string(),
                    replacement: "IF".to_string(),
                });
            }

            CompletionContext::AddWhat => {
                completions.extend([
                    Pair {
                        display: "NEW".to_string(),
                        replacement: "NEW".to_string(),
                    },
                    Pair {
                        display: "RECORDS FROM".to_string(),
                        replacement: "RECORDS FROM".to_string(),
                    },
                    Pair {
                        display: "DATA FROM".to_string(),
                        replacement: "DATA FROM".to_string(),
                    },
                ]);
            }

            CompletionContext::FieldAssignment => {
                // TODO: Get field names from schema or context
                completions.extend([
                    Pair {
                        display: "name".to_string(),
                        replacement: "name".to_string(),
                    },
                    Pair {
                        display: "email".to_string(),
                        replacement: "email".to_string(),
                    },
                    Pair {
                        display: "age".to_string(),
                        replacement: "age".to_string(),
                    },
                    Pair {
                        display: "active".to_string(),
                        replacement: "active".to_string(),
                    },
                ]);
            }

            CompletionContext::DeleteFrom => {
                completions.extend([
                    Pair {
                        display: "ALL".to_string(),
                        replacement: "ALL".to_string(),
                    },
                    Pair {
                        display: "FROM".to_string(),
                        replacement: "FROM".to_string(),
                    },
                ]);

                for table in &self.tables {
                    if table.starts_with(&partial.to_lowercase()) {
                        completions.push(Pair {
                            display: table.clone(),
                            replacement: table.clone(),
                        });
                    }
                }
            }

            CompletionContext::DDL => {
                for dtype in &self.data_types {
                    if dtype.starts_with(&partial_upper) {
                        completions.push(Pair {
                            display: dtype.to_string(),
                            replacement: dtype.to_string(),
                        });
                    }
                }
            }

            CompletionContext::Complete => {
                // No specific completions needed
            }
        }

        // Sort by relevance and length
        completions.sort_by(|a, b| {
            // Exact matches first
            let a_exact = a.display.to_uppercase() == partial_upper;
            let b_exact = b.display.to_uppercase() == partial_upper;

            match (a_exact, b_exact) {
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => a.display.len().cmp(&b.display.len()),
            }
        });

        completions
    }
}

#[derive(Debug, Clone, Copy)]
enum CompletionContext {
    Start,
    SelectItems,
    NeedFromOrIn,
    AfterFrom,
    TableName,
    Condition,
    CountTarget,
    AskIf,
    AddWhat,
    FieldAssignment,
    DeleteFrom,
    DDL,
    Complete,
}

impl Completer for NsqlCompleter {
    type Candidate = Pair;

    fn complete(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> Result<(usize, Vec<Pair>)> {
        let completions = self.get_completions(line, pos);

        let start = line[..pos]
            .rfind(|c: char| c.is_whitespace() || "(),;".contains(c))
            .map(|i| i + 1)
            .unwrap_or(0);

        Ok((start, completions))
    }
}

impl Hinter for NsqlCompleter {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> Option<String> {
        if pos < line.len() {
            return None;
        }

        let line_upper = line.to_uppercase();

        // Natural language hints
        if line_upper == "FIND" {
            return Some(" ALL users".to_string());
        }

        if line_upper == "SHOW" {
            return Some(" ME * FROM users".to_string());
        }

        if line_upper == "GET" {
            return Some(" name, email FROM users".to_string());
        }

        if line_upper == "COUNT" {
            return Some(" users WHERE active = true".to_string());
        }

        if line_upper == "HOW MANY" {
            return Some(" orders WHERE status = 'pending'".to_string());
        }

        if line_upper == "ASK" {
            return Some(" IF EXISTS users WHERE email = 'test@example.com'".to_string());
        }

        if line_upper.ends_with("FIND ") {
            return Some("ALL <table_name>".to_string());
        }

        if line_upper.ends_with("SHOW ") {
            return Some("* FROM <table_name>".to_string());
        }

        if line_upper.ends_with("GET ") {
            return Some("<columns> FROM <table_name>".to_string());
        }

        if line_upper.ends_with(" FROM ") {
            return Some("<table_name>".to_string());
        }

        if line_upper.ends_with(" WHERE ") || line_upper.ends_with(" WHEN ") {
            return Some("<condition>".to_string());
        }

        if line_upper.ends_with("ADD ") {
            return Some("NEW <type> WITH <field> = <value> TO <table>".to_string());
        }

        if line_upper.ends_with("UPDATE ") || line_upper.ends_with("CHANGE ") {
            return Some("<table> SET <field> = <value> WHERE <condition>".to_string());
        }

        if line_upper.ends_with("DELETE ") || line_upper.ends_with("REMOVE ") {
            return Some("FROM <table> WHERE <condition>".to_string());
        }

        None
    }
}

impl Highlighter for NsqlCompleter {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> std::borrow::Cow<'l, str> {
        // For now, return the line as-is to avoid highlighting bug
        // TODO: Implement proper syntax highlighting that doesn't interfere with existing escape sequences
        std::borrow::Cow::Borrowed(line)
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        _default: bool,
    ) -> std::borrow::Cow<'b, str> {
        std::borrow::Cow::Borrowed(prompt)
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> std::borrow::Cow<'h, str> {
        std::borrow::Cow::Owned(format!("\x1b[2m{}\x1b[0m", hint))
    }
}

impl Validator for NsqlCompleter {}

impl Helper for NsqlCompleter {}
