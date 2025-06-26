mod commands;
mod completer;
pub mod display;
mod history;
mod prompt;

use driver::BlockingDatabaseDriver;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::{Config, Editor, Result};
use std::fs;
use std::io::{self, Read};
use std::path::Path;
use std::time::Instant;

pub use self::commands::CommandHandler;
pub use self::display::DisplayConfig;

pub struct Repl {
	editor: Editor<completer::NsqlCompleter, DefaultHistory>,
	command_handler: CommandHandler,
	display_config: DisplayConfig,
	driver: BlockingDatabaseDriver,
	history: history::HistoryManager,
	multiline_buffer: String,
	in_multiline: bool,
	query_count: usize,
	start_time: Instant,
}

#[derive(Debug, Clone)]
pub enum InputSource {
	Interactive,
	File(String),
	Stdin,
	CommandLine(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct ReplOptions {
	pub input_source: InputSource,
	pub quiet: bool,
	pub exit_on_error: bool,
	pub no_history: bool,
	pub output_format: Option<display::OutputMode>,
	pub server_address: Option<String>,
}

impl Default for ReplOptions {
	fn default() -> Self {
		Self {
			input_source: InputSource::Interactive,
			quiet: false,
			exit_on_error: false,
			no_history: false,
			output_format: None,
			server_address: None,
		}
	}
}

impl Repl {
	pub fn new() -> Result<Self> {
		Self::with_options(ReplOptions::default())
	}

	pub fn with_options(options: ReplOptions) -> Result<Self> {
		let config = Config::builder()
			.history_ignore_space(true)
			.completion_type(rustyline::CompletionType::List)
			.edit_mode(rustyline::EditMode::Emacs)
			.build();

		let completer = completer::NsqlCompleter::new();
		let mut editor = Editor::with_config(config)?;

		editor.set_helper(Some(completer));

		let history = if !options.no_history {
			let hist = history::HistoryManager::new(".nsql_history")?;
			hist.load(&mut editor)?;
			hist
		} else {
			history::HistoryManager::new(".nsql_history")?
		};

		let mut display_config = DisplayConfig::default();

		// Apply command line options
		if options.quiet {
			display_config.show_timing = false;
		}

		if let Some(format) = options.output_format {
			display_config.output_mode = format;
		}

		// Create executor with server address
		let driver = if let Some(server_addr) = options.server_address {
			BlockingDatabaseDriver::with_server(server_addr)
		} else {
			BlockingDatabaseDriver::new()
		};

		Ok(Self {
			editor,
			command_handler: CommandHandler::new(),
			display_config,
			driver,
			history,
			multiline_buffer: String::new(),
			in_multiline: false,
			query_count: 0,
			start_time: Instant::now(),
		})
	}

	pub fn run(&mut self) -> Result<()> {
		self.run_with_options(ReplOptions::default())
	}

	pub fn run_with_options(&mut self, options: ReplOptions) -> Result<()> {
		match &options.input_source {
			InputSource::Interactive => self.run_interactive(options.quiet),
			InputSource::File(path) => self.run_file(path, &options),
			InputSource::Stdin => self.run_stdin(&options),
			InputSource::CommandLine(commands) => self.run_commands(commands, &options),
		}
	}

	fn run_interactive(&mut self, quiet: bool) -> Result<()> {
		if !quiet {
			display::print_welcome();
			self.show_connection_status();
		}

		loop {
			let prompt_str =
				prompt::generate_prompt(self.in_multiline, self.query_count, &self.display_config);

			match self.editor.readline(&prompt_str) {
				Ok(line) => {
					if !self.handle_line(line)? {
						break;
					}
				}
				Err(ReadlineError::Interrupted) => {
					self.handle_interrupt();
				}
				Err(ReadlineError::Eof) => {
					self.handle_eof();
					break;
				}
				Err(err) => {
					display::print_error(&format!("Readline error: {:?}", err));
					break;
				}
			}
		}

		self.cleanup()?;
		Ok(())
	}

	fn run_file(&mut self, path: &str, options: &ReplOptions) -> Result<()> {
		if !options.quiet {
			display::print_info(&format!("Executing file: {}", path));
		}

		let content = fs::read_to_string(path)
			.map_err(|e| ReadlineError::Io(io::Error::new(io::ErrorKind::Other, e)))?;

		self.execute_script(&content, options)
	}

	fn run_stdin(&mut self, options: &ReplOptions) -> Result<()> {
		if !options.quiet {
			display::print_info("Reading from standard input...");
		}

		let mut content = String::new();
		io::stdin()
			.read_to_string(&mut content)
			.map_err(|e| ReadlineError::Io(e))?;

		self.execute_script(&content, options)
	}

	fn run_commands(&mut self, commands: &[String], options: &ReplOptions) -> Result<()> {
		if !options.quiet && commands.len() > 1 {
			display::print_info(&format!("Executing {} commands...", commands.len()));
		}

		for (i, command) in commands.iter().enumerate() {
			if !options.quiet && commands.len() > 1 {
				let display_command = if command.len() > 50 {
					format!("{}...", &command[..47])
				} else {
					command.clone()
				};

				display::print_info(&format!(
					"Command {}/{}: {}",
					i + 1,
					commands.len(),
					display_command
				));
			}

			if !self.execute_single_command(command, options)? {
				break;
			}
		}

		Ok(())
	}

	fn execute_script(&mut self, content: &str, options: &ReplOptions) -> Result<()> {
		let lines: Vec<&str> = content.lines().collect();
		let mut current_statement = String::new();
		let mut line_num = 0;

		for line in lines {
			line_num += 1;
			let trimmed = line.trim();

			// Skip empty lines and comments
			if trimmed.is_empty() || trimmed.starts_with(">>") || trimmed.starts_with("--") {
				continue;
			}

			// Handle REPL commands
			if trimmed.starts_with('.') {
				if !current_statement.trim().is_empty() {
					// Execute any pending statement first
					if !self.execute_single_command(&current_statement, options)? {
						return Ok(());
					}
					current_statement.clear();
				}

				// Execute REPL command
				match self.handle_repl_command(trimmed, options) {
					Ok(true) => continue,
					Ok(false) => return Ok(()),
					Err(e) => {
						if options.exit_on_error {
							display::print_error(&format!("Error at line {}: {}", line_num, e));
							return Err(ReadlineError::Io(io::Error::new(
								io::ErrorKind::Other,
								e.to_string(),
							)));
						} else {
							display::print_error(&format!("Error at line {}: {}", line_num, e));
							continue;
						}
					}
				}
			}

			// Add line to current statement
			if !current_statement.is_empty() {
				current_statement.push('\n');
			}
			current_statement.push_str(line);

			// Check if statement is complete (ends with semicolon)
			if trimmed.ends_with(';') {
				if !self.execute_single_command(&current_statement, options)? {
					return Ok(());
				}
				current_statement.clear();
			}
		}

		// Execute any remaining statement
		if !current_statement.trim().is_empty() {
			self.execute_single_command(&current_statement, options)?;
		}

		Ok(())
	}

	fn execute_single_command(&mut self, command: &str, options: &ReplOptions) -> Result<bool> {
		let trimmed = command.trim();
		if trimmed.is_empty() {
			return Ok(true);
		}

		// Handle REPL commands
		if trimmed.starts_with('.') {
			return self.handle_repl_command(trimmed, options);
		}

		// Execute SQL/NSQL query
		let start = Instant::now();
		let result = self.driver.execute(command);
		let duration = start.elapsed();

		self.query_count += 1;

		match result {
			Ok(exec_result) => {
				if !options.quiet {
					display::print_execution_result(&exec_result, duration, &self.display_config);
				}
				Ok(true)
			}
			Err(e) => {
				if !options.quiet {
					display::print_query_error(&e, command, &self.display_config);
				}

				if options.exit_on_error {
					std::process::exit(1);
				} else {
					Ok(true)
				}
			}
		}
	}

	fn handle_repl_command(&mut self, command: &str, options: &ReplOptions) -> Result<bool> {
		match self
			.command_handler
			.handle(command, &mut self.display_config, &mut self.driver)
		{
			commands::CommandResult::Continue => Ok(true),
			commands::CommandResult::Exit => {
				if !options.quiet {
					display::print_goodbye(self.query_count, self.start_time.elapsed());
				}
				Ok(false)
			}
			commands::CommandResult::Error(msg) => {
				display::print_error(&msg);
				if options.exit_on_error {
					std::process::exit(1);
				} else {
					Ok(true)
				}
			}
			commands::CommandResult::ClearScreen => {
				display::clear_screen();
				if !options.quiet {
					display::print_welcome();
					self.show_connection_status();
				}
				Ok(true)
			}
			commands::CommandResult::ShowHistory => {
				self.history.display(&self.editor)?;
				Ok(true)
			}
			commands::CommandResult::ExecuteFromHistory(n) => {
				if let Some(query) = self.history.get_entry(&self.editor, n) {
					self.execute_single_command(&query, options)
				} else {
					display::print_error(&format!("History entry {} not found", n));
					Ok(true)
				}
			}
		}
	}

	fn handle_line(&mut self, line: String) -> Result<bool> {
		// Add to history if non-empty
		if !line.trim().is_empty() {
			self.editor.add_history_entry(&line)?;
		}

		// Check for commands (only when not in multiline mode)
		if !self.in_multiline && line.trim().starts_with('.') {
			return self.handle_command(&line);
		}

		// Handle multiline input
		self.handle_query_input(line)
	}

	fn handle_command(&mut self, line: &str) -> Result<bool> {
		match self
			.command_handler
			.handle(line, &mut self.display_config, &mut self.driver)
		{
			commands::CommandResult::Continue => Ok(true),
			commands::CommandResult::Exit => {
				display::print_goodbye(self.query_count, self.start_time.elapsed());
				Ok(false)
			}
			commands::CommandResult::Error(msg) => {
				display::print_error(&msg);
				Ok(true)
			}
			commands::CommandResult::ClearScreen => {
				display::clear_screen();
				display::print_welcome();
				self.show_connection_status();
				Ok(true)
			}
			commands::CommandResult::ShowHistory => {
				self.history.display(&self.editor)?;
				Ok(true)
			}
			commands::CommandResult::ExecuteFromHistory(n) => {
				if let Some(query) = self.history.get_entry(&self.editor, n) {
					self.execute_query(&query);
				} else {
					display::print_error(&format!("History entry {} not found", n));
				}
				Ok(true)
			}
		}
	}

	fn handle_query_input(&mut self, line: String) -> Result<bool> {
		// Add to multiline buffer
		if self.in_multiline {
			self.multiline_buffer.push('\n');
		}
		self.multiline_buffer.push_str(&line);

		// Check if we should continue multiline

		if self.should_continue_multiline(&line) {
			self.in_multiline = true;
			Ok(true)
		} else {
			// Execute the query
			self.in_multiline = false;
			let query = std::mem::take(&mut self.multiline_buffer);

			if !query.trim().is_empty() {
				self.execute_query(&query);
			}

			Ok(true)
		}
	}

	fn should_continue_multiline(&self, line: &str) -> bool {
		let trimmed = line.trim();

		// Empty line ends multiline
		if trimmed.is_empty() && self.in_multiline {
			return false;
		}

		// Check for statement terminator
		!trimmed.ends_with(';')
	}

	fn execute_query(&mut self, query: &str) {
		self.query_count += 1;

		let start = Instant::now();
		let result = self.driver.execute(query);
		let duration = start.elapsed();

		match result {
			Ok(exec_result) => {
				display::print_execution_result(&exec_result, duration, &self.display_config);
			}
			Err(e) => {
				display::print_query_error(&e, query, &self.display_config);
			}
		}
	}

	fn show_connection_status(&mut self) {
		if self.driver.is_connected() {
			display::print_success(&format!(
				"Connected to server at {}",
				self.driver.server_address()
			));
		} else {
			display::print_warning(&format!(
				"Not connected to server ({})",
				self.driver.server_address()
			));
			display::print_hint(
				"Use .connect to establish connection or .server <address> to change server",
			);
		}
	}

	fn handle_interrupt(&mut self) {
		if self.in_multiline {
			display::print_warning("Cancelled multiline input");
			self.multiline_buffer.clear();
			self.in_multiline = false;
		} else {
			display::print_hint("Use .exit or Ctrl+D to quit");
		}
	}

	fn handle_eof(&self) {
		println!(); // New line before exit
		display::print_goodbye(self.query_count, self.start_time.elapsed());
	}

	fn cleanup(&mut self) -> Result<()> {
		self.history.save(&mut self.editor)?;
		Ok(())
	}

	pub fn execute_file<P: AsRef<Path>>(path: P, options: ReplOptions) -> Result<()> {
		let mut repl = Self::with_options(options.clone())?;
		let file_path = path.as_ref().to_string_lossy().to_string();
		let file_options = ReplOptions {
			input_source: InputSource::File(file_path),
			..options
		};
		repl.run_with_options(file_options)
	}

	pub fn execute_commands(commands: Vec<String>, options: ReplOptions) -> Result<()> {
		let mut repl = Self::with_options(options.clone())?;
		let cmd_options = ReplOptions {
			input_source: InputSource::CommandLine(commands),
			..options
		};
		repl.run_with_options(cmd_options)
	}

	pub fn execute_stdin(options: ReplOptions) -> Result<()> {
		let mut repl = Self::with_options(options.clone())?;
		let stdin_options = ReplOptions {
			input_source: InputSource::Stdin,
			..options
		};
		repl.run_with_options(stdin_options)
	}

	pub fn set_show_ast(&mut self, show: bool) {
		self.display_config.show_ast = show;
	}

	pub fn set_show_bytecode(&mut self, show: bool) {
		self.display_config.show_bytecode = show;
	}

	pub fn set_show_timing(&mut self, show: bool) {
		self.display_config.show_timing = show;
	}

	pub fn set_use_colors(&mut self, use_colors: bool) {
		self.display_config.use_colors = use_colors;
	}

	pub fn set_output_mode(&mut self, mode: display::OutputMode) {
		self.display_config.output_mode = mode;
	}

	pub fn set_server_address(&mut self, address: String) {
		self.driver.set_server_address(address);
	}

	pub fn connect(&mut self) -> Result<()> {
		match self.driver.ping() {
			Ok(msg) => {
				display::print_success(&msg);
				Ok(())
			}
			Err(e) => {
				display::print_error(&format!("Connection failed: {}", e));
				Err(ReadlineError::Io(std::io::Error::new(
					std::io::ErrorKind::Other,
					e,
				)))
			}
		}
	}

	pub fn disconnect(&mut self) {
		self.driver.disconnect();
		display::print_info("Disconnected from server");
	}
}

fn main() -> Result<()> {
	let mut repl = Repl::new()?;
	repl.run()
}
