use super::display::{self, DisplayConfig};
use colored::Colorize;
use driver::BlockingDatabaseDriver;

pub enum CommandResult {
    Continue,
    Exit,
    Error(String),
    ClearScreen,
    ShowHistory,
    ExecuteFromHistory(usize),
}

pub struct CommandHandler {
    // No fields needed for now
}

impl CommandHandler {
    pub fn new() -> Self {
        CommandHandler {}
    }

    pub fn handle(
        &self,
        line: &str,
        display_config: &mut DisplayConfig,
        driver: &mut BlockingDatabaseDriver,
    ) -> CommandResult {
        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.is_empty() {
            return CommandResult::Continue;
        }

        match parts[0] {
            ".help" | ".h" | ".?" => {
                self.show_help();
                CommandResult::Continue
            }
            ".exit" | ".quit" | ".q" => CommandResult::Exit,
            ".clear" | ".cls" => CommandResult::ClearScreen,
            ".connect" => match driver.ping() {
                Ok(msg) => {
                    display::print_success(&msg);
                    CommandResult::Continue
                }
                Err(e) => CommandResult::Error(format!("Connection failed: {}", e)),
            },
            ".disconnect" => {
                driver.disconnect();
                display::print_info("Disconnected from server");
                CommandResult::Continue
            }
            ".server" => {
                if parts.len() > 1 {
                    let new_address = parts[1].to_string();
                    driver.set_server_address(new_address.clone());
                    display::print_info(&format!("Server address set to: {}", new_address));
                } else {
                    display::print_info(&format!("Current server: {}", driver.server_address()));
                }
                CommandResult::Continue
            }
            ".status" => {
                if driver.is_connected() {
                    display::print_success(&format!("Connected to {}", driver.server_address()));
                } else {
                    display::print_warning(&format!(
                        "Not connected to {}",
                        driver.server_address()
                    ));
                }
                CommandResult::Continue
            }
            ".ping" => match driver.ping() {
                Ok(_) => {
                    display::print_success("Server is responding");
                    CommandResult::Continue
                }
                Err(e) => CommandResult::Error(format!("Ping failed: {}", e)),
            },
            ".tables" => match driver.list_tables() {
                Ok(tables) => {
                    self.show_tables(tables);
                    CommandResult::Continue
                }
                Err(e) => CommandResult::Error(format!("Failed to list tables: {}", e)),
            },
            ".schema" => {
                if parts.len() > 1 {
                    match driver.get_table_schema(parts[1]) {
                        Ok(schema) => {
                            println!("{}", schema);
                            CommandResult::Continue
                        }
                        Err(e) => CommandResult::Error(format!(
                            "Failed to get schema for {}: {}",
                            parts[1], e
                        )),
                    }
                } else {
                    CommandResult::Error("Usage: .schema <table_name>".to_string())
                }
            }
            ".timing" | ".time" => {
                display_config.show_timing = !display_config.show_timing;
                display::print_toggle("Query timing", display_config.show_timing);
                CommandResult::Continue
            }
            ".color" | ".colors" => {
                display_config.use_colors = !display_config.use_colors;
                display::print_toggle("Syntax highlighting", display_config.use_colors);
                CommandResult::Continue
            }
            ".history" | ".hist" => {
                if parts.len() > 1 {
                    if let Ok(n) = parts[1].parse::<usize>() {
                        CommandResult::ExecuteFromHistory(n)
                    } else {
                        CommandResult::Error(format!("Invalid history number: {}", parts[1]))
                    }
                } else {
                    CommandResult::ShowHistory
                }
            }
            ".mode" => {
                if parts.len() > 1 {
                    match parts[1] {
                        "table" => display_config.output_mode = display::OutputMode::Table,
                        "json" => display_config.output_mode = display::OutputMode::Json,
                        "csv" => display_config.output_mode = display::OutputMode::Csv,
                        "plain" => display_config.output_mode = display::OutputMode::Plain,
                        _ => {
                            return CommandResult::Error(format!("Unknown mode: {}", parts[1]));
                        }
                    }
                    display::print_info(&format!("Output mode set to: {}", parts[1]));
                } else {
                    display::print_info(&format!(
                        "Current output mode: {:?}",
                        display_config.output_mode
                    ));
                }
                CommandResult::Continue
            }
            ".width" => {
                if parts.len() > 1 {
                    if let Ok(width) = parts[1].parse::<usize>() {
                        display_config.max_column_width = width;
                        display::print_info(&format!("Max column width set to: {}", width));
                    } else {
                        return CommandResult::Error(format!("Invalid width: {}", parts[1]));
                    }
                } else {
                    display::print_info(&format!(
                        "Current max column width: {}",
                        display_config.max_column_width
                    ));
                }
                CommandResult::Continue
            }
            _ => CommandResult::Error(format!(
                "Unknown command: {}. Type .help for help.",
                parts[0]
            )),
        }
    }

    fn show_help(&self) {
        println!("\n{}", "NSQL REPL Commands".bright_cyan().bold());
        println!("{}", "─".repeat(50).bright_black());

        let commands = vec![
            (".help, .h, .?", "Show this help message"),
            (".exit, .quit, .q", "Exit the REPL"),
            (".clear, .cls", "Clear the screen"),
            ("", ""),
            ("Connection:", ""),
            (".connect", "Connect to the server"),
            (".disconnect", "Disconnect from the server"),
            (".server <address>", "Set server address (host:port)"),
            (".status", "Show connection status"),
            (".ping", "Ping the server"),
            ("", ""),
            ("Display Options:", ""),
            (".timing, .time", "Toggle query timing"),
            (".color, .colors", "Toggle syntax highlighting"),
            (".mode <format>", "Set output format (table/json/csv/plain)"),
            (".width <n>", "Set maximum column width"),
            ("", ""),
            ("Database Info:", ""),
            (".tables", "List all tables"),
            (".schema <table>", "Show table schema"),
            ("", ""),
            ("History:", ""),
            (".history, .hist", "Show query history"),
            (".history <n>", "Execute history entry n"),
        ];

        for (cmd, desc) in commands {
            if cmd.is_empty() {
                println!();
            } else if desc.is_empty() {
                println!("\n{}", cmd.bright_yellow());
            } else {
                println!("  {:20} {}", cmd.bright_green(), desc.bright_white());
            }
        }

        println!("\n{}", "Query Input:".bright_yellow());
        println!("  • End statements with ';' to execute");
        println!("  • Press Enter without ';' for multiline input");
        println!("  • Empty line ends multiline mode");
        println!("  • Ctrl+C cancels current input");
        println!("  • Ctrl+D exits the REPL");
        println!();
    }

    fn show_tables(&self, tables: Vec<String>) {
        if tables.is_empty() {
            display::print_info("No tables found");
        } else {
            display::print_info(&format!("Found {} table(s):", tables.len()));
            for table in &tables {
                println!("  • {}", table.bright_green());
            }
        }
    }
}
