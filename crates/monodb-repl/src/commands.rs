//! REPL command parsing
//!
//! Handles parsing of colon-prefixed commands like :help, :quit, etc.

/// A REPL command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Exit the REPL
    Quit,
    /// Show help
    Help,
    /// Execute the buffer
    Run,
    /// Clear the buffer
    Clear,
    /// List buffer contents
    List,
    /// Remove last line from buffer
    Undo,
    /// Delete a specific line
    Delete(usize),
    /// List tables (current namespace only)
    Tables,
    /// List all tables across all namespaces
    TablesAll,
    /// Show current namespace
    Namespace,
    /// Unknown command
    Unknown(String),
}

impl Command {
    /// Parse a command string (without the leading colon).
    pub fn parse(input: &str) -> Self {
        let input = input.trim();

        // Handle commands with arguments first
        if let Some(rest) = input.strip_prefix("d ") {
            return rest
                .trim()
                .parse()
                .map(Command::Delete)
                .unwrap_or_else(|_| Command::Unknown(input.to_string()));
        }

        // Simple commands
        match input {
            "q" | "quit" | "exit" => Command::Quit,
            "h" | "help" | "?" => Command::Help,
            "x" | "run" => Command::Run,
            "c" | "clear" => Command::Clear,
            "l" | "list" | "show" => Command::List,
            "u" | "undo" => Command::Undo,
            "t" | "tables" | "lt" => Command::Tables,
            "ta" | "tables all" | "all" => Command::TablesAll,
            "ns" | "namespace" => Command::Namespace,
            _ => Command::Unknown(input.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_commands() {
        assert_eq!(Command::parse("q"), Command::Quit);
        assert_eq!(Command::parse("quit"), Command::Quit);
        assert_eq!(Command::parse("h"), Command::Help);
        assert_eq!(Command::parse("help"), Command::Help);
        assert_eq!(Command::parse("x"), Command::Run);
        assert_eq!(Command::parse("d 5"), Command::Delete(5));
        assert_eq!(Command::parse("t"), Command::Tables);
        assert_eq!(Command::parse("ta"), Command::TablesAll);
        assert_eq!(Command::parse("ns"), Command::Namespace);
        assert_eq!(
            Command::parse("unknown"),
            Command::Unknown("unknown".into())
        );
    }
}
