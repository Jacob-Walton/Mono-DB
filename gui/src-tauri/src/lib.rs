use driver::{DatabaseDriver, ExecutionResult};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use tauri::State;

#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectionConfig {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
    row_count: usize,
    execution_time: u64,
}

struct DatabaseState {
    driver: Option<DatabaseDriver>,
}

impl Default for DatabaseState {
    fn default() -> Self {
        Self { driver: None }
    }
}

#[tauri::command]
async fn db_connect(
    config: ConnectionConfig,
    state: State<'_, Mutex<DatabaseState>>,
) -> Result<(), String> {
    let server_address = format!("{}:{}", config.host, config.port);

    // Create new driver with server address
    let mut driver = DatabaseDriver::with_server(server_address);

    // Test connection
    match driver.ping().await {
        Ok(_) => {
            let mut db_state = state.lock().unwrap();
            db_state.driver = Some(driver);
            Ok(())
        }
        Err(e) => Err(format!("Connection failed: {}", e)),
    }
}

#[tauri::command]
async fn db_disconnect(state: State<'_, Mutex<DatabaseState>>) -> Result<(), String> {
    let mut db_state = state.lock().unwrap();

    if let Some(ref mut driver) = db_state.driver {
        driver.disconnect();
    }

    db_state.driver = None;
    Ok(())
}

#[tauri::command]
async fn db_execute_query(
    query: String,
    state: State<'_, Mutex<DatabaseState>>,
) -> Result<QueryResult, String> {
    // Extract driver from state to avoid holding the lock during async operation
    let mut driver = {
        let mut db_state = state.lock().unwrap();
        match db_state.driver.take() {
            Some(driver) => driver,
            None => return Err("Not connected to database".to_string()),
        }
    };

    // Execute query without holding the lock
    let result = driver.execute(&query).await;

    // Put the driver back
    {
        let mut db_state = state.lock().unwrap();
        db_state.driver = Some(driver);
    }

    match result {
        Ok(exec_result) => Ok(convert_execution_result(exec_result)),
        Err(e) => Err(e),
    }
}

#[tauri::command]
async fn db_get_tables(state: State<'_, Mutex<DatabaseState>>) -> Result<Vec<String>, String> {
    // Extract driver from state to avoid holding the lock during async operation
    let mut driver = {
        let mut db_state = state.lock().unwrap();
        match db_state.driver.take() {
            Some(driver) => driver,
            None => return Ok(vec![]),
        }
    };

    // List tables
    let result = driver.list_tables().await;

    // Put the driver back
    {
        let mut db_state = state.lock().unwrap();
        db_state.driver = Some(driver);
    }

    match result {
        Ok(tables) => Ok(tables),
        Err(e) => {
            // Log error but don't fail completely
            eprintln!("Failed to get tables: {}", e);
            Ok(vec![])
        }
    }
}

fn convert_execution_result(result: ExecutionResult) -> QueryResult {
    // Parse table output if available to extract columns and rows
    if let Some(table_output) = &result.table_output {
        // Try to parse structured table data
        if let Some((columns, rows)) = parse_table_output(table_output) {
            let row_count = result.rows_affected.unwrap_or(rows.len());
            return QueryResult {
                columns,
                rows,
                row_count,
                execution_time: result.execution_time.as_millis() as u64,
            };
        }
    }

    // Fallback: create a simple result display
    let columns = vec!["Result".to_string()];
    let rows = if result.success {
        vec![vec![serde_json::Value::String(result.message)]]
    } else {
        vec![vec![serde_json::Value::String(format!(
            "Error: {}",
            result.message
        ))]]
    };

    QueryResult {
        columns,
        rows,
        row_count: result.rows_affected.unwrap_or(1),
        execution_time: result.execution_time.as_millis() as u64,
    }
}

fn parse_table_output(table_output: &str) -> Option<(Vec<String>, Vec<Vec<serde_json::Value>>)> {
    let lines: Vec<&str> = table_output.lines().collect();
    if lines.len() < 3 {
        return None; // Not enough lines for a table
    }

    // Look for table separator patterns
    let separator_line = lines
        .iter()
        .position(|line| line.contains("─") || line.contains("-"))?;

    if separator_line == 0 || separator_line >= lines.len() - 1 {
        return None;
    }

    // Extract column headers from the line before separator
    let header_line = lines[separator_line - 1];
    let columns: Vec<String> = header_line
        .split('│')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if columns.is_empty() {
        return None;
    }

    // Extract data rows from lines after separator
    let mut rows = Vec::new();
    for line in &lines[separator_line + 1..] {
        if line.trim().is_empty() || line.contains("─") || line.contains("-") {
            continue;
        }

        let row_data: Vec<serde_json::Value> = line
            .split('│')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| {
                // Try to parse as number first, then as string
                if let Ok(num) = s.parse::<i64>() {
                    serde_json::Value::Number(num.into())
                } else if let Ok(num) = s.parse::<f64>() {
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(num).unwrap_or_else(|| 0.into()),
                    )
                } else if s.eq_ignore_ascii_case("null") || s.eq_ignore_ascii_case("none") {
                    serde_json::Value::Null
                } else if s.eq_ignore_ascii_case("true") {
                    serde_json::Value::Bool(true)
                } else if s.eq_ignore_ascii_case("false") {
                    serde_json::Value::Bool(false)
                } else {
                    serde_json::Value::String(s.to_string())
                }
            })
            .collect();

        if row_data.len() == columns.len() {
            rows.push(row_data);
        }
    }

    Some((columns, rows))
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(Mutex::new(DatabaseState::default()))
        .invoke_handler(tauri::generate_handler![
            db_connect,
            db_disconnect,
            db_execute_query,
            db_get_tables
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
