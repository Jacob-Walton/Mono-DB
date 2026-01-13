//! MonoDB Interactive Shell (REPL)

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use colored::*;
use monodb_client::Client;
use rustyline::completion::Completer;
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Cmd, Config, Editor, Helper, KeyCode, KeyEvent, Result as RlResult};

mod commands;
mod format;

use commands::Command;
use format::Formatter;

const HISTORY_FILE: &str = ".mdb_history";

/// CLI arguments.
struct Args {
    addr: String,
    cert_path: Option<PathBuf>,
}

impl Args {
    fn parse() -> Self {
        let mut args = std::env::args().skip(1);
        let mut addr = "127.0.0.1:6432".to_string();
        let mut cert_path = None;

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--cert-path" => {
                    cert_path = args.next().map(PathBuf::from);
                }
                _ => {
                    // Assume it's the address
                    addr = arg;
                }
            }
        }

        Self { addr, cert_path }
    }
}

/// Custom helper that disables tab completion so tabs can be typed literally.
struct ReplHelper;

impl Helper for ReplHelper {}

impl Completer for ReplHelper {
    type Candidate = String;

    fn complete(
        &self,
        _line: &str,
        _pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> RlResult<(usize, Vec<Self::Candidate>)> {
        // Return empty completions, this allows literal tab to be inserted
        Ok((0, vec![]))
    }
}

impl Hinter for ReplHelper {
    type Hint = String;
}

impl Highlighter for ReplHelper {}

impl Validator for ReplHelper {}

fn main() -> anyhow::Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> anyhow::Result<()> {
    let args = Args::parse();

    print_banner();

    let tls_info = if args.cert_path.is_some() {
        " (TLS)"
    } else {
        ""
    };
    println!("Connecting to {}{}...", args.addr, tls_info);

    let client = if let Some(ref cert_path) = args.cert_path {
        Client::connect_with_cert(&args.addr, cert_path).await?
    } else {
        Client::connect(&args.addr).await?
    };
    println!("{}\n", "Connected".green());

    let mut repl = Repl::new(client)?;
    repl.run().await?;

    Ok(())
}

fn print_banner() {
    println!();
    println!("{}", "MonoDB Interactive Shell".bold());
    println!(
        "Type {} for help, {} to quit",
        ":h".cyan(),
        ":q".cyan()
    );
    println!();
}

/// The main REPL state machine.
struct Repl {
    client: Client,
    editor: Editor<ReplHelper, rustyline::history::DefaultHistory>,
    buffer: Vec<String>,
    formatter: Formatter,
    current_namespace: String,
}

impl Repl {
    fn new(client: Client) -> RlResult<Self> {
        let config = Config::builder().tab_stop(4).indent_size(4).build();

        let mut editor = Editor::with_config(config)?;
        editor.set_helper(Some(ReplHelper));

        // Rebind Tab to insert 4 spaces (visible indentation) instead of triggering completion
        editor.bind_sequence(
            KeyEvent(KeyCode::Tab, rustyline::Modifiers::NONE),
            Cmd::Insert(1, "    ".to_string()),
        );

        let _ = editor.load_history(HISTORY_FILE);

        Ok(Self {
            client,
            editor,
            buffer: Vec::new(),
            formatter: Formatter::new(),
            current_namespace: "default".to_string(),
        })
    }

    fn prompt(&self) -> String {
        if self.buffer.is_empty() {
            format!("{}> ", self.current_namespace)
        } else {
            "...> ".to_string()
        }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            let prompt = self.prompt();

            match self.editor.readline(&prompt) {
                Ok(line) => {
                    if self.handle_line(&line).await? {
                        break;
                    }
                }
                Err(ReadlineError::Interrupted | ReadlineError::Eof) => break,
                Err(e) => return Err(e.into()),
            }
        }

        let _ = self.editor.save_history(HISTORY_FILE);
        self.client.close().await?;
        println!("Goodbye!");
        Ok(())
    }

    /// Handle a line of input. Returns true if we should exit.
    async fn handle_line(&mut self, line: &str) -> anyhow::Result<bool> {
        let trimmed = line.trim();

        // Handle commands (starting with :)
        if let Some(cmd_str) = trimmed.strip_prefix(':') {
            let cmd = Command::parse(cmd_str);
            return Ok(self.execute_command(cmd).await);
        }

        // Empty line with buffer = execute
        if trimmed.is_empty() && !self.buffer.is_empty() {
            self.execute_buffer().await;
            return Ok(false);
        }

        // Add non-empty lines to buffer
        if !trimmed.is_empty() {
            self.editor.add_history_entry(line)?;
            self.buffer.push(line.to_string());
        }

        Ok(false)
    }

    /// Execute a command. Returns true if we should exit.
    async fn execute_command(&mut self, cmd: Command) -> bool {
        match cmd {
            Command::Quit => return true,

            Command::Help => Self::print_help(),

            Command::Run => {
                if self.buffer.is_empty() {
                    println!("Buffer is empty");
                } else {
                    self.execute_buffer().await;
                }
            }

            Command::Clear => {
                self.buffer.clear();
                println!("Buffer cleared");
            }

            Command::List => {
                if self.buffer.is_empty() {
                    println!("{}", "(empty)".dimmed());
                } else {
                    for (i, line) in self.buffer.iter().enumerate() {
                        println!("{:3} | {}", (i + 1).to_string().dimmed(), line);
                    }
                }
            }

            Command::Undo => {
                if self.buffer.pop().is_some() {
                    println!("Removed last line");
                } else {
                    println!("Buffer is empty");
                }
            }

            Command::Delete(n) => {
                if n > 0 && n <= self.buffer.len() {
                    self.buffer.remove(n - 1);
                    println!("Deleted line {}", n);
                } else {
                    println!("Invalid line number (1-{})", self.buffer.len().max(1));
                }
            }

            Command::Tables => {
                self.list_tables(false).await;
            }

            Command::TablesAll => {
                self.list_tables(true).await;
            }

            Command::Namespace => {
                println!(
                    "Current namespace: {}",
                    self.current_namespace.cyan().bold()
                );
            }

            Command::Unknown(s) => {
                println!("Unknown command: {} (type :h for help)", s.red());
            }
        }
        false
    }

    fn print_help() {
        println!();
        println!("{}", "Commands:".bold());
        println!(
            "  {}  {}      Execute buffered query",
            ":x".cyan(),
            ":run".cyan()
        );
        println!(
            "  {}  {}    Clear buffer",
            ":c".cyan(),
            ":clear".cyan()
        );
        println!(
            "  {}  {}     Show buffer contents",
            ":l".cyan(),
            ":list".cyan()
        );
        println!(
            "  {}  {}     Remove last line",
            ":u".cyan(),
            ":undo".cyan()
        );
        println!("  {} <N>         Delete line N", ":d".cyan());
        println!(
            "  {} {}   List tables in current namespace",
            ":t".cyan(),
            ":tables".cyan()
        );
        println!(
            "  {}           List all tables across namespaces",
            ":ta".cyan()
        );
        println!(
            "  {}           Show current namespace",
            ":ns".cyan()
        );
        println!(
            "  {}  {}     Exit",
            ":q".cyan(),
            ":quit".cyan()
        );
        println!(
            "  {}  {}     Show this help",
            ":h".cyan(),
            ":help".cyan()
        );
        println!();
    }

    async fn execute_buffer(&mut self) {
        let query = self.buffer.join("\n");
        self.buffer.clear();
        self.execute_query(&query).await;
    }

    async fn execute_query(&mut self, query: &str) {
        let start = Instant::now();

        // Check for USE statement to update local namespace tracking
        let trimmed = query.trim().to_lowercase();
        let is_use = trimmed.starts_with("use ");

        // Always send the query to the server
        match self.client.query(query).await {
            Ok(result) => {
                let elapsed = start.elapsed();

                // If USE was successful, update local namespace
                if is_use {
                    let ns = query.trim()[4..].trim();
                    self.current_namespace = ns.to_string();
                    println!();
                    println!("Switched to namespace: {}", ns.cyan().bold());
                    println!("{}", format!("({:.2?})", elapsed).dimmed());
                    println!();
                } else {
                    println!();
                    self.formatter.format_result(&result, elapsed);
                }
            }
            Err(e) => {
                println!();
                println!("{}: {}", "Error".red().bold(), e);
                println!();
            }
        }
    }

    async fn list_tables(&mut self, all_namespaces: bool) {
        let start = Instant::now();

        match self.client.list_tables().await {
            Ok(result) => {
                let elapsed = start.elapsed();
                println!();
                self.format_tables(&result, elapsed, all_namespaces);
            }
            Err(e) => {
                println!();
                println!("{}: {}", "Error".red().bold(), e);
                println!();
            }
        }
    }

    fn format_tables(
        &self,
        result: &monodb_client::QueryResult,
        elapsed: Duration,
        all_namespaces: bool,
    ) {
        let rows = result.rows();
        if rows.is_empty() {
            println!("{}", "(no tables)".dimmed());
            println!("{}", format!("({:.2?})", elapsed).dimmed());
            println!();
            return;
        }

        // Group tables by namespace
        let mut namespaces: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();

        for row in &rows {
            let full_name = row
                .get("name")
                .and_then(|v| v.as_string())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "?".to_string());

            let kind = row
                .get("schema")
                .and_then(|v| v.as_string())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "?".to_string());

            // Parse namespace.table format
            let (namespace, table) = if let Some(dot_pos) = full_name.find('.') {
                (
                    full_name[..dot_pos].to_string(),
                    full_name[dot_pos + 1..].to_string(),
                )
            } else {
                ("default".to_string(), full_name)
            };

            namespaces.entry(namespace).or_default().push((table, kind));
        }

        let mut total_count = 0;

        if all_namespaces {
            // Show all namespaces
            for (ns, tables) in &namespaces {
                self.print_namespace_header(ns);
                for (table, kind) in tables {
                    self.print_table_entry(table, kind);
                    total_count += 1;
                }
                println!();
            }
        } else {
            // Show only current namespace
            if let Some(tables) = namespaces.get(&self.current_namespace) {
                self.print_namespace_header(&self.current_namespace);
                for (table, kind) in tables {
                    self.print_table_entry(table, kind);
                    total_count += 1;
                }
                println!();
            } else {
                println!(
                    "{}",
                    format!("(no tables in namespace '{}')", self.current_namespace).dimmed()
                );
            }
        }

        let ns_text = if all_namespaces {
            format!(" across {} namespace(s)", namespaces.len())
        } else {
            String::new()
        };

        println!(
            "{} table(s){}",
            total_count.to_string().bold(),
            ns_text.dimmed()
        );
        println!("{}", format!("({:.2?})", elapsed).dimmed());
        println!();
    }

    fn print_namespace_header(&self, ns: &str) {
        let is_current = ns == self.current_namespace;
        let marker = if is_current { " *" } else { "" };
        println!(
            "{} {}",
            format!("[{}]", ns).bold().blue(),
            marker.green()
        );
    }

    fn print_table_entry(&self, table: &str, kind: &str) {
        let kind_colored = match kind {
            "relational" => "relational".cyan(),
            "document" => "document".yellow(),
            "keyspace" => "keyspace".magenta(),
            _ => kind.normal(),
        };
        println!(
            "    {} {}",
            table.bold(),
            format!("({})", kind_colored).dimmed()
        );
    }
}
