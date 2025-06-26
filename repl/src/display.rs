use colored::Colorize;
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum OutputMode {
	Table,
	Json,
	Csv,
	Plain,
}

#[derive(Debug, Clone)]
pub struct DisplayConfig {
	pub show_ast: bool,
	pub show_bytecode: bool,
	pub show_timing: bool,
	pub use_colors: bool,
	pub output_mode: OutputMode,
	pub max_column_width: usize,
}

impl Default for DisplayConfig {
	fn default() -> Self {
		Self {
			show_ast: false,
			show_bytecode: false,
			show_timing: true,
			use_colors: true,
			output_mode: OutputMode::Table,
			max_column_width: 30,
		}
	}
}

pub fn print_welcome() {
	println!(
		"\n{}",
		"╔═══════════════════════════════════════╗".bright_cyan()
	);
	println!(
		"{}",
		"║       Welcome to NSQL REPL! 🚀        ║"
			.bright_cyan()
			.bold()
	);
	println!(
		"{}",
		"╚═══════════════════════════════════════╝".bright_cyan()
	);
	println!();
	println!(
		"Type {} for help, {} to exit",
		".help".bright_green(),
		".exit".bright_green()
	);
	println!("Try {} to see example queries", ".example".bright_green());
	println!();
}

pub fn print_goodbye(query_count: usize, duration: Duration) {
	println!();
	println!("{}", "─".repeat(40).bright_black());
	println!("👋 {}", "Thanks for using NSQL!".bright_cyan());
	println!("   Executed {} queries in {:.1?}", query_count, duration);
	println!();
}

pub fn clear_screen() {
	print!("\x1B[2J\x1B[1;1H");
}

pub fn print_error(msg: &str) {
	println!("{} {}", "✗".bright_red().bold(), msg.bright_red());
}

pub fn print_warning(msg: &str) {
	println!("{} {}", "⚠".bright_yellow(), msg.bright_yellow());
}

pub fn print_info(msg: &str) {
	println!("{} {}", "ℹ".bright_blue(), msg.bright_white());
}

pub fn print_success(msg: &str) {
	println!("{} {}", "✓".bright_green().bold(), msg.bright_green());
}

pub fn print_hint(msg: &str) {
	println!("{} {}", "💡".bright_white(), msg.bright_white().dimmed());
}

pub fn print_toggle(feature: &str, enabled: bool) {
	let status = if enabled {
		"ON".bright_green().bold()
	} else {
		"OFF".bright_red()
	};
	println!("{}: {}", feature, status);
}

pub fn print_execution_result(
	result: &driver::ExecutionResult,
	duration: Duration,
	config: &DisplayConfig,
) {
	if result.success {
		print_success(&result.message);

		// Display table output if available
		if let Some(table_output) = &result.table_output {
			println!("\n{}", table_output);
		}
	} else {
		print_error(&result.message);
	}

	if let Some(rows) = result.rows_affected {
		println!("  {} row(s) affected", rows.to_string().bright_white());
	}

	if config.show_timing {
		println!(
			"  {} {:.3}ms (network: {:.3}ms)",
			"Time:".bright_black(),
			result.execution_time.as_secs_f64() * 1000.0,
			duration.as_secs_f64() * 1000.0
		);
	}
}

pub fn print_query_error(error_msg: &str, query: &str, _config: &DisplayConfig) {
	print_error(error_msg);

	// Show query context
	if !query.trim().is_empty() {
		let lines: Vec<&str> = query.lines().collect();
		if lines.len() <= 5 {
			println!();
			println!("{}", "Query:".bright_yellow());
			for (i, line) in lines.iter().enumerate() {
				println!("{:3} │ {}", i + 1, line.dimmed());
			}
		}
	}
}
