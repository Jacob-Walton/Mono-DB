use colored::*;
use monodb_client::Client;
use monodb_common::Value;
use std::io::{self, BufRead, Write};

mod formatter;
use formatter::format_value;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "localhost:7899".to_string());

    println!("\n{}", "MonoDB Interactive Shell".bold());
    println!("Connecting to {}...", addr);

    let client = Client::connect(&addr).await?;
    println!("{}\n", "Connected".green());
    println!("Type :h for help, :q to quit\n");

    let stdin = io::stdin();
    let mut lines = Vec::new();

    loop {
        let prompt = if lines.is_empty() { "mdb> " } else { "...> " };

        print!("{}", prompt);
        io::stdout().flush()?;

        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                if line.ends_with('\n') {
                    line.pop();
                }
                if line.ends_with('\r') {
                    line.pop();
                }

                let trimmed = line.trim();

                if trimmed.starts_with(':') {
                    if handle_command(trimmed, &mut lines, &client).await {
                        break;
                    }
                    continue;
                }

                if trimmed.is_empty() && !lines.is_empty() {
                    let query = lines.join("\n");
                    execute(&client, &query).await;
                    lines.clear();
                    continue;
                }

                if !trimmed.is_empty() {
                    lines.push(line);
                }
            }
            Err(_) => break,
        }
    }

    client.close().await?;
    Ok(())
}

async fn handle_command(cmd: &str, lines: &mut Vec<String>, client: &Client) -> bool {
    match cmd {
        ":x" | ":run" => {
            if !lines.is_empty() {
                let query = lines.join("\n");
                execute(client, &query).await;
                lines.clear();
            } else {
                println!("Nothing to execute");
            }
        }
        ":c" | ":clear" => {
            lines.clear();
            println!("Buffer cleared");
        }
        ":l" | ":list" => {
            if lines.is_empty() {
                println!("(empty)");
            } else {
                for (i, l) in lines.iter().enumerate() {
                    println!("{:3} | {}", i + 1, l);
                }
            }
        }
        ":lt" | ":tables" => {
            get_tables(client).await;
        }
        ":q" | ":quit" => {
            return true;
        }
        ":h" | ":help" => {
            println!("\nCommands:");
            println!("  :x,  :run        Execute buffered query");
            println!("  :c,  :clear      Clear buffer");
            println!("  :l,  :list       Show buffered lines");
            println!("  :lt, :tables     List tables from server");
            println!("  :d   <N>         Delete line N");
            println!("  :u,  :undo       Remove last line");
            println!("  :q,  :quit       Exit");
            println!("  :h,  :help       Show this help");
            println!();
        }
        _ if cmd.starts_with(":d ") => {
            if let Ok(n) = cmd[3..].trim().parse::<usize>() {
                if n > 0 && n <= lines.len() {
                    lines.remove(n - 1);
                    println!("Deleted line {}", n);
                } else {
                    println!("Invalid line number (1-{})", lines.len());
                }
            } else {
                println!("Usage: :d <line_number>");
            }
        }
        ":u" | ":undo" => {
            if !lines.is_empty() {
                lines.pop();
                println!("Removed last line");
            }
        }
        _ => {
            println!("Unknown command: {} (type :h for help)", cmd);
        }
    }
    false
}

async fn execute(client: &Client, query: &str) {
    let start = std::time::Instant::now();

    match client.query(query).await {
        Ok(result) => {
            let elapsed = start.elapsed();
            println!();
            format_result(&result, elapsed);
        }
        Err(e) => {
            println!("\n{}: {}\n", "Error".red(), e);
        }
    }
}

async fn get_tables(client: &Client) {
    let start = std::time::Instant::now();

    match client.list_tables().await {
        Ok(result) => {
            let elapsed = start.elapsed();
            println!();
            
            for row in result.rows() {
                if let Some(Value::Array(arr)) = row.value().as_array().and_then(|a| a.first()) {
                    for item in arr {
                        if let Value::Array(table) = item {
                            let fallback = Value::String("unknown".to_string());
                            let table_type = table.first().unwrap_or(&fallback);
                            let table_name = table.get(1).unwrap_or(&fallback);

                            if let (Some(name), Some(ttype)) = (table_name.as_string(), table_type.as_string()) {
                                println!("{}\t{}", name, ttype);
                            }
                        }
                    }
                }
            }
            println!("({:.2?})\n", elapsed);
        }
        Err(e) => {
            println!("\n{}: {}\n", "Error".red(), e);
        }
    }
}

fn format_result(result: &monodb_client::QueryResult, elapsed: std::time::Duration) {
    // Check result type
    if result.is_created() {
        println!("{}", "Created".green());
    } else if result.is_modified() {
        let rows = result.rows_affected();
        println!(
            "{} ({} row{})",
            "Modified".green(),
            rows,
            if rows == 1 { "" } else { "s" }
        );
    } else {
        // Display data rows
        let rows = result.rows();
        for row in &rows {
            format_value(row.value(), 0);
        }

        if !rows.is_empty() {
            let count = rows.len();
            println!("\n{} row{}", count, if count == 1 { "" } else { "s" });
        }
    }
    
    println!("({:.2?})\n", elapsed);
}
