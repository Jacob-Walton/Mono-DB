use mono_core::network::NetworkConnection;
use mono_core::network::protocol::{
	ClientMessage, ErrorCode, ServerMessage, format_execution_result, format_table_for_display,
};
use mono_core::{MonoError, MonoResult};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout};

#[derive(Debug, Clone)]
pub struct ExecutionResult {
	pub success: bool,
	pub message: String,
	pub table_output: Option<String>,
	pub rows_affected: Option<usize>,
	pub execution_time: Duration,
	pub ast: Option<String>,
}

pub struct DatabaseDriver {
	connection: Option<NetworkConnection>,
	server_address: String,
	heartbeat_interval: Duration,
	heartbeat_timeout: Duration,
	last_heartbeat: Option<Instant>,
}

impl DatabaseDriver {
	pub fn new() -> Self {
		Self {
			connection: None,
			server_address: "127.0.0.1:3282".to_string(),
			heartbeat_interval: Duration::from_secs(30),
			heartbeat_timeout: Duration::from_secs(5),
			last_heartbeat: None,
		}
	}

	pub fn with_server(address: String) -> Self {
		Self {
			connection: None,
			server_address: address,
			heartbeat_interval: Duration::from_secs(30),
			heartbeat_timeout: Duration::from_secs(5),
			last_heartbeat: None,
		}
	}

	pub fn with_heartbeat_config(mut self, interval: Duration, timeout: Duration) -> Self {
		self.heartbeat_interval = interval;
		self.heartbeat_timeout = timeout;
		self
	}

	pub async fn connect(&mut self) -> MonoResult<()> {
		if self.connection.is_some() {
			return Ok(());
		}

		let stream = TcpStream::connect(&self.server_address)
			.await
			.map_err(|e| {
				MonoError::Connection(format!(
					"Failed to connect to {}: {}",
					self.server_address, e
				))
			})?;

		let mut connection = NetworkConnection::new(stream);

		let ping_message = ClientMessage::Ping;
		connection.send_message(ping_message).await?;

		match connection.read_response().await? {
			Some(ServerMessage::Pong) => {
				self.connection = Some(connection);
				self.last_heartbeat = Some(Instant::now());
				Ok(())
			}
			Some(ServerMessage::Error { message, .. }) => Err(MonoError::Protocol(format!(
				"Server error during ping: {}",
				message
			))),
			Some(response) => Err(MonoError::Protocol(format!(
				"Unexpected response to ping: {:?}",
				response
			))),
			None => Err(MonoError::Connection(
				"Connection closed during handshake".to_string(),
			)),
		}
	}

	pub async fn execute(&mut self, sql: &str) -> Result<ExecutionResult, String> {
		if let Err(e) = self.ensure_connection().await {
			return Err(format!("Connection failed: {}", e));
		}

		let connection = self.connection.as_mut().unwrap();
		let message = ClientMessage::Execute {
			sql: sql.to_string(),
			database: None,
		};

		let start_time = std::time::Instant::now();

		if let Err(e) = connection.send_message(message).await {
			return Err(format!("Failed to send query: {}", e));
		}

		match connection.read_response().await {
			Ok(Some(response)) => {
				let execution_time = start_time.elapsed();
				self.handle_server_response(response, execution_time)
			}
			Ok(None) => {
				self.connection = None;
				Err("Connection closed by server".to_string())
			}
			Err(e) => {
				self.connection = None;
				Err(format!("Failed to read response: {}", e))
			}
		}
	}

	pub async fn list_tables(&mut self) -> Result<Vec<String>, String> {
		if let Err(e) = self.ensure_connection().await {
			return Err(format!("Connection failed: {}", e));
		}

		let connection = self.connection.as_mut().unwrap();
		let message = ClientMessage::ListTables { database: None };

		if let Err(e) = connection.send_message(message).await {
			return Err(format!("Failed to send request: {}", e));
		}

        match connection.read_response().await {
            Ok(Some(ServerMessage::TableList { tables })) => {
                Ok(tables)
            }
            Ok(Some(ServerMessage::Error { message, .. })) => {
                Err(format!("Server error: {}", message))
            }
            Ok(Some(_)) => Err("Unexpected response from server".to_string()),
            Ok(None) => {
                self.connection = None;
                Err("Connection closed by server".to_string())
            }
            Err(e) => {
                self.connection = None;
                Err(format!("Failed to read response: {}", e))
            }
        }
    }

    pub async fn get_table_schema(&mut self, table_name: &str) -> Result<String, String> {
        let describe_sql = format!("DESCRIBE {}", table_name);
        match self.execute(&describe_sql).await {
            Ok(result) => {
                if result.success {
                    Ok(result.table_output.unwrap_or_else(|| "No schema information available".to_string()))
                } else {
                    Err(result.message)
                }
            }
            Err(e) => Err(e),
        }
    }

	pub fn is_connected(&self) -> bool {
		self.connection.is_some()
	}

	pub fn disconnect(&mut self) {
		self.connection = None;
	}

	pub fn server_address(&self) -> &str {
		&self.server_address
	}

	pub fn set_server_address(&mut self, address: String) {
		if self.server_address != address {
			self.disconnect();
			self.server_address = address;
		}
	}

	pub async fn ping(&mut self) -> Result<String, String> {
		if let Err(e) = self.ensure_connection().await {
			return Err(format!("Failed to connect: {}", e));
		}

		let connection = self.connection.as_mut().unwrap();
		let ping_message = ClientMessage::Ping;

		if let Err(e) = connection.send_message(ping_message).await {
			self.connection = None;
			return Err(format!("Failed to send ping: {}", e));
		}

		match timeout(self.heartbeat_timeout, connection.read_response()).await {
			Ok(Ok(Some(ServerMessage::Pong))) => {
				self.last_heartbeat = Some(Instant::now());
				Ok("Server is alive".to_string())
			}
			Ok(Ok(Some(ServerMessage::Error { message, .. }))) => {
				Err(format!("Server error: {}", message))
			}
			Ok(Ok(Some(_))) => Err("Unexpected response to ping".to_string()),
			Ok(Ok(None)) => {
				self.connection = None;
				Err("Connection closed by server".to_string())
			}
			Ok(Err(e)) => {
				self.connection = None;
				Err(format!("Network error: {}", e))
			}
			Err(_) => {
				self.connection = None;
				Err("Ping timeout".to_string())
			}
		}
	}

	async fn ensure_connection(&mut self) -> MonoResult<()> {
		// Check if we need to send a heartbeat
		if let Some(last_heartbeat) = self.last_heartbeat {
			if last_heartbeat.elapsed() >= self.heartbeat_interval {
				if let Err(_) = self.send_heartbeat().await {
					// Heartbeat failed, disconnect and reconnect
					self.connection = None;
					self.last_heartbeat = None;
				}
			}
		}

		self.connect().await
	}

	async fn send_heartbeat(&mut self) -> Result<(), String> {
		if let Some(connection) = &mut self.connection {
			let ping_message = ClientMessage::Ping;

			if let Err(e) = connection.send_message(ping_message).await {
				return Err(format!("Failed to send heartbeat: {}", e));
			}

			match timeout(self.heartbeat_timeout, connection.read_response()).await {
				Ok(Ok(Some(ServerMessage::Pong))) => {
					self.last_heartbeat = Some(Instant::now());
					Ok(())
				}
				Ok(Ok(Some(ServerMessage::Error { message, .. }))) => {
					Err(format!("Heartbeat error: {}", message))
				}
				Ok(Ok(Some(_))) => Err("Unexpected heartbeat response".to_string()),
				Ok(Ok(None)) => Err("Connection closed during heartbeat".to_string()),
				Ok(Err(e)) => Err(format!("Heartbeat network error: {}", e)),
				Err(_) => Err("Heartbeat timeout".to_string()),
			}
		} else {
			Err("No connection available".to_string())
		}
	}

	fn handle_server_response(
		&self,
		response: ServerMessage,
		execution_time: Duration,
	) -> Result<ExecutionResult, String> {
		match response {
			ServerMessage::ExecuteResult {
				results,
				rows_affected,
				#[allow(unused)]
				execution_time_ms,
			} => {
				if results.is_empty() {
					return Ok(ExecutionResult {
						success: true,
						message: "Query executed successfully".to_string(),
						table_output: None,
						rows_affected,
						execution_time,
						ast: None,
					});
				}

				let mut output_parts = Vec::new();
				let mut total_rows_affected = 0;
				let mut has_data = false;

				for result in &results {
					let result_message = format_execution_result(result);
					output_parts.push(result_message);

					if let Some(rows) = result.rows_affected {
						total_rows_affected += rows;
					}

					if !result.rows.is_empty() {
						has_data = true;
						let table_display = format_table_for_display(result);
						output_parts.push(table_display);
					}
				}

				let final_rows_affected = if total_rows_affected > 0 {
					Some(total_rows_affected)
				} else {
					rows_affected
				};

				Ok(ExecutionResult {
					success: true,
					message: if has_data {
						"Query executed successfully".to_string()
					} else {
						output_parts.join("\n")
					},
					table_output: if has_data {
						Some(output_parts.join("\n\n"))
					} else {
						None
					},
					rows_affected: final_rows_affected,
					execution_time,
					ast: None,
				})
			}

			ServerMessage::Error {
				code,
				message,
				details,
			} => {
				let error_msg = match code {
					ErrorCode::ParseError => format!("Parse Error: {}", message),
					ErrorCode::ExecutionError => format!("Execution Error: {}", message),
					ErrorCode::TableNotFound => format!("Table Not Found: {}", message),
					ErrorCode::ColumnNotFound => format!("Column Not Found: {}", message),
					ErrorCode::TypeMismatch => format!("Type Mismatch: {}", message),
					ErrorCode::ConstraintViolation => format!("Constraint Violation: {}", message),
					ErrorCode::InternalError => format!("Internal Error: {}", message),
					ErrorCode::NotImplemented => format!("Not Implemented: {}", message),
				};

				let full_message = if let Some(details) = details {
					format!("{}\nDetails: {}", error_msg, details)
				} else {
					error_msg
				};

				Err(full_message)
			}

			ServerMessage::Pong => Ok(ExecutionResult {
				success: true,
				message: "Server is alive".to_string(),
				table_output: None,
				rows_affected: None,
				execution_time,
				ast: None,
			}),

			ServerMessage::HeartbeatAck => Ok(ExecutionResult {
				success: true,
				message: "Heartbeat acknowledged".to_string(),
				table_output: None,
				rows_affected: None,
				execution_time,
				ast: None,
			}),

			ServerMessage::DatabaseList { databases } => {
				let table_output = format!("Available databases:\n{}", databases.join("\n"));
				Ok(ExecutionResult {
					success: true,
					message: format!("Found {} database(s)", databases.len()),
					table_output: Some(table_output),
					rows_affected: None,
					execution_time,
					ast: None,
				})
			}

            ServerMessage::TableList { tables } => {
                let table_output = if tables.is_empty() {
                    "No tables found".to_string()
                } else {
                    let mut output = String::from("Available tables:\n");
                    for table in &tables {
                        output.push_str(&format!(
                            "  {}\n",
                            table
                        ));
                    }
                    output
                };

				Ok(ExecutionResult {
					success: true,
					message: format!("Found {} table(s)", tables.len()),
					table_output: Some(table_output),
					rows_affected: None,
					execution_time,
					ast: None,
				})
			}
		}
	}
}

// Blocking wrapper for synchronous usage
pub struct BlockingDatabaseDriver {
	driver: DatabaseDriver,
	runtime: tokio::runtime::Runtime,
}

impl BlockingDatabaseDriver {
	pub fn new() -> Self {
		let runtime = tokio::runtime::Runtime::new().expect("Failed to create async runtime");
		Self {
			driver: DatabaseDriver::new(),
			runtime,
		}
	}

	pub fn with_server(address: String) -> Self {
		let runtime = tokio::runtime::Runtime::new().expect("Failed to create async runtime");
		Self {
			driver: DatabaseDriver::with_server(address),
			runtime,
		}
	}

	pub fn with_heartbeat_config(interval: Duration, timeout: Duration) -> Self {
		let runtime = tokio::runtime::Runtime::new().expect("Failed to create async runtime");
		Self {
			driver: DatabaseDriver::new().with_heartbeat_config(interval, timeout),
			runtime,
		}
	}

	pub fn execute(&mut self, sql: &str) -> Result<ExecutionResult, String> {
		self.runtime.block_on(self.driver.execute(sql))
	}

	pub fn list_tables(&mut self) -> Result<Vec<String>, String> {
		self.runtime.block_on(self.driver.list_tables())
	}

    pub fn get_table_schema(&mut self, table: &str) -> Result<String, String> {
        let sql = format!("DESCRIBE {}", table);
        let result = self.execute(&sql)?;
        if let Some(table_output) = &result.table_output {
            if !table_output.trim().is_empty() {
                return Ok(table_output.clone());
            }
        }
        // Try to parse "Columns: id: INTEGER, username: VARCHAR(255), ..." style message
        if result.message.starts_with("Columns: ") {
            let columns_str = result.message.trim_start_matches("Columns: ").trim();
            let mut rows = Vec::new();
            for coldef in columns_str.split(',') {
                let coldef = coldef.trim();
                // Split "name: TYPE" or "name: TYPE(ARGS)"
                if let Some((name, dtype)) = coldef.split_once(':') {
                    rows.push((name.trim(), dtype.trim()));
                }
            }
            if !rows.is_empty() {
                use prettytable::{Table, Row, Cell};
                let mut table = Table::new();
                table.set_format(*prettytable::format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
                table.set_titles(Row::new(vec![
                    Cell::new("Column"),
                    Cell::new("Type"),
                ]));
                for (name, dtype) in rows {
                    table.add_row(Row::new(vec![
                        Cell::new(name),
                        Cell::new(dtype),
                    ]));
                }
                return Ok(table.to_string());
            }
        }
        if result.message.contains("No columns found") || result.message.contains("No schema information available") {
            Ok("No schema information available".to_string())
        } else if result.success && !result.message.trim().is_empty() {
            Ok(result.message.clone())
        } else {
            Ok("No schema information available".to_string())
        }
    }

	pub fn is_connected(&self) -> bool {
		self.driver.is_connected()
	}

	pub fn disconnect(&mut self) {
		self.driver.disconnect()
	}

	pub fn server_address(&self) -> &str {
		self.driver.server_address()
	}

	pub fn set_server_address(&mut self, address: String) {
		self.driver.set_server_address(address)
	}

	pub fn ping(&mut self) -> Result<String, String> {
		self.runtime.block_on(self.driver.ping())
	}
}
