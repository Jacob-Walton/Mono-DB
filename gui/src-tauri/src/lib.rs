use monodb_client::Client;
use monodb_common::protocol::{ExecutionResult, Response};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tauri::State;
use tokio::sync::Mutex;

/// Global database connection state
pub struct DbState {
    client: Arc<Mutex<Option<Client>>>,
    connection_info: Arc<Mutex<Option<ConnectionInfo>>>,
}

impl Default for DbState {
    fn default() -> Self {
        Self {
            client: Arc::new(Mutex::new(None)),
            connection_info: Arc::new(Mutex::new(None)),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub host: String,
    pub port: u16,
    pub connected: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<serde_json::Value>,
    pub row_count: u64,
    pub execution_time: u64,
    pub result_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub table_type: String,
}

/// Connect to a MonoDB server
#[tauri::command]
async fn connect(host: String, port: u16, state: State<'_, DbState>) -> Result<ConnectionInfo, String> {
    let addr = format!("{}:{}", host, port);

    let client = Client::connect(&addr)
        .await
        .map_err(|e| format!("Failed to connect: {}", e))?;

    let info = ConnectionInfo {
        host: host.clone(),
        port,
        connected: true,
    };

    *state.client.lock().await = Some(client);
    *state.connection_info.lock().await = Some(info.clone());

    Ok(info)
}

/// Disconnect from the server
#[tauri::command]
async fn disconnect(state: State<'_, DbState>) -> Result<(), String> {
    let client = state.client.lock().await.take();
    if let Some(client) = client {
        client.close().await.map_err(|e| format!("Failed to disconnect: {}", e))?;
    }
    *state.connection_info.lock().await = None;
    Ok(())
}

/// Get current connection status
#[tauri::command]
async fn get_connection_status(state: State<'_, DbState>) -> Result<Option<ConnectionInfo>, String> {
    Ok(state.connection_info.lock().await.clone())
}

/// Execute a query
#[tauri::command]
async fn execute_query(query: String, state: State<'_, DbState>) -> Result<QueryResult, String> {
    // Get pool clone and release lock quickly
    let pool = {
        let guard = state.client.lock().await;
        let client = guard.as_ref().ok_or_else(|| "Not connected to database".to_string())?;
        client.pool().await.clone()
    };

    let mut conn = pool
        .get()
        .await
        .map_err(|e| format!("Failed to get connection: {}", e))?;

    let response = conn
        .execute(query)
        .await
        .map_err(|e| format!("Query execution failed: {}", e))?;

    pool.return_connection(conn);

    match response {
        Response::Success { result } => {
            let mut query_result = QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                execution_time: 0,
                result_type: "success".to_string(),
            };

            for exec_result in result {
                match exec_result {
                    ExecutionResult::Ok {
                        data,
                        time: _,
                        commit_timestamp: _,
                        time_elapsed,
                        row_count,
                    } => {
                        query_result.execution_time = time_elapsed.unwrap_or(0);
                        query_result.row_count = row_count.unwrap_or(0);
                        query_result.result_type = "ok".to_string();

                        let json_data = data.to_json();
                        if let serde_json::Value::Array(arr) = json_data {
                            for item in arr {
                                if let serde_json::Value::Object(obj) = &item {
                                    if query_result.columns.is_empty() {
                                        query_result.columns = obj.keys().cloned().collect();
                                    }
                                }
                                query_result.rows.push(item);
                            }
                        } else if let serde_json::Value::Object(obj) = json_data {
                            if query_result.columns.is_empty() {
                                query_result.columns = obj.keys().cloned().collect();
                            }
                            query_result.rows.push(serde_json::Value::Object(obj));
                        }
                    }
                    ExecutionResult::Created { .. } => {
                        query_result.result_type = "created".to_string();
                    }
                    ExecutionResult::Modified { rows_affected, .. } => {
                        query_result.result_type = "modified".to_string();
                        query_result.row_count = rows_affected.unwrap_or(0);
                    }
                }
            }

            Ok(query_result)
        }
        Response::Error { code: _, message } => Err(message),
        _ => Err("Unexpected response".to_string()),
    }
}

/// List all tables
#[tauri::command]
async fn list_tables(state: State<'_, DbState>) -> Result<Vec<TableInfo>, String> {
    // Get pool clone and release lock quickly
    let pool = {
        let guard = state.client.lock().await;
        let client = guard.as_ref().ok_or_else(|| "Not connected to database".to_string())?;
        client.pool().await.clone()
    };

    let mut conn = pool
        .get()
        .await
        .map_err(|e| format!("Failed to get connection: {}", e))?;

    let response = conn
        .list_tables()
        .await
        .map_err(|e| format!("Failed to list tables: {}", e))?;

    pool.return_connection(conn);

    match response {
        Response::Success { result } => {
            let mut tables = Vec::new();

            for exec_result in result {
                if let ExecutionResult::Ok { data, .. } = exec_result {
                    let json_data = data.to_json();
                    if let serde_json::Value::Array(arr) = json_data {
                        for item in arr {
                            // Each item should be [type, name]
                            if let serde_json::Value::Array(pair) = item {
                                if pair.len() == 2 {
                                    if let (
                                        serde_json::Value::String(table_type),
                                        serde_json::Value::String(name),
                                    ) = (&pair[0], &pair[1])
                                    {
                                        tables.push(TableInfo {
                                            name: name.clone(),
                                            table_type: table_type.clone(),
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }

            Ok(tables)
        }
        Response::Error { code: _, message } => Err(message),
        _ => Err("Unexpected response".to_string()),
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(DbState::default())
        .invoke_handler(tauri::generate_handler![
            connect,
            disconnect,
            get_connection_status,
            execute_query,
            list_tables,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
