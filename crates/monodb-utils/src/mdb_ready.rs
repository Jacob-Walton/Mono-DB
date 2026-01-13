//! mdb_ready - MonoDB server connection checker
//!
//! Exit codes:
//!   0 - server is accepting connections
//!   1 - server rejected the connection (e.g., authentication required)
//!   2 - no response (server not running or unreachable)
//!   3 - no connection attempt was made (bad parameters)

use std::env;
use std::path::PathBuf;
use std::process;
use std::time::{Duration, Instant};

use monodb_client::Connection;

/// Exit codes
mod exit_code {
    pub const ACCEPTING: i32 = 0;
    pub const REJECTING: i32 = 1;
    pub const NO_RESPONSE: i32 = 2;
    pub const NO_ATTEMPT: i32 = 3;
}

#[derive(Debug)]
struct Config {
    host: String,
    port: u16,
    timeout: Duration,
    wait: Option<Duration>,
    wait_interval: Duration,
    quiet: bool,
    tls_cert: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6432,
            timeout: Duration::from_secs(5),
            wait: None,
            wait_interval: Duration::from_millis(500),
            quiet: false,
            tls_cert: None,
        }
    }
}

#[derive(Debug)]
enum ConnectionStatus {
    /// Server accepted the connection
    Accepting,
    /// Server explicitly rejected the connection (auth failure, etc.)
    Rejecting(String),
    /// No response from server (connection refused, timeout, etc.)
    NoResponse(String),
}

impl ConnectionStatus {
    fn exit_code(&self) -> i32 {
        match self {
            ConnectionStatus::Accepting => exit_code::ACCEPTING,
            ConnectionStatus::Rejecting(_) => exit_code::REJECTING,
            ConnectionStatus::NoResponse(_) => exit_code::NO_RESPONSE,
        }
    }
}

#[tokio::main]
async fn main() {
    let config = match parse_args() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("mdb_ready: {e}");
            eprintln!("Try 'mdb_ready --help' for more information.");
            process::exit(exit_code::NO_ATTEMPT);
        }
    };

    let addr = format!("{}:{}", config.host, config.port);
    let status = run_check(&addr, &config).await;

    if !config.quiet {
        print_status(&addr, &status, &config);
    }

    process::exit(status.exit_code());
}

async fn run_check(addr: &str, config: &Config) -> ConnectionStatus {
    match config.wait {
        Some(max_wait) => wait_for_server(addr, config, max_wait).await,
        None => check_connection(addr, config).await,
    }
}

async fn wait_for_server(addr: &str, config: &Config, max_wait: Duration) -> ConnectionStatus {
    let start = Instant::now();

    loop {
        let status = check_connection(addr, config).await;

        match &status {
            ConnectionStatus::Accepting => return status,
            ConnectionStatus::Rejecting(_) => return status, // Don't retry on rejection
            ConnectionStatus::NoResponse(_) => {
                if start.elapsed() >= max_wait {
                    return status;
                }
                if !config.quiet {
                    eprint!(".");
                }
                tokio::time::sleep(config.wait_interval).await;
            }
        }
    }
}

async fn check_connection(addr: &str, config: &Config) -> ConnectionStatus {
    let connect_future = async {
        match &config.tls_cert {
            Some(cert_path) => Connection::connect_tls(addr, cert_path).await,
            None => Connection::connect(addr).await,
        }
    };

    match tokio::time::timeout(config.timeout, connect_future).await {
        Ok(Ok(_)) => ConnectionStatus::Accepting,
        Ok(Err(e)) => {
            let err_str = e.to_string().to_lowercase();
            // Check for rejection indicators (authentication, permission, etc.)
            if err_str.contains("auth")
                || err_str.contains("permission")
                || err_str.contains("denied")
                || err_str.contains("reject")
            {
                ConnectionStatus::Rejecting(e.to_string())
            } else {
                ConnectionStatus::NoResponse(e.to_string())
            }
        }
        Err(_) => ConnectionStatus::NoResponse("connection timeout".to_string()),
    }
}

fn print_status(addr: &str, status: &ConnectionStatus, config: &Config) {
    let tls_info = if config.tls_cert.is_some() {
        " (TLS)"
    } else {
        ""
    };

    match status {
        ConnectionStatus::Accepting => {
            println!("{addr}{tls_info}: accepting connections");
        }
        ConnectionStatus::Rejecting(reason) => {
            eprintln!("{addr}{tls_info}: server is rejecting connections: {reason}");
        }
        ConnectionStatus::NoResponse(reason) => {
            eprintln!("{addr}{tls_info}: no response: {reason}");
        }
    }
}

fn parse_args() -> Result<Config, String> {
    let args: Vec<String> = env::args().collect();
    let mut config = Config::default();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--host" => {
                config.host = get_arg_value(&args, &mut i, "host")?;
            }
            "-p" | "--port" => {
                let port_str = get_arg_value(&args, &mut i, "port")?;
                config.port = port_str
                    .parse()
                    .map_err(|_| format!("invalid port number: {port_str}"))?;
            }
            "-t" | "--timeout" => {
                let timeout_str = get_arg_value(&args, &mut i, "timeout")?;
                let secs: u64 = timeout_str
                    .parse()
                    .map_err(|_| format!("invalid timeout: {timeout_str}"))?;
                config.timeout = Duration::from_secs(secs);
            }
            "-w" | "--wait" => {
                let wait_str = get_arg_value(&args, &mut i, "wait")?;
                let secs: u64 = wait_str
                    .parse()
                    .map_err(|_| format!("invalid wait time: {wait_str}"))?;
                config.wait = Some(Duration::from_secs(secs));
            }
            "-i" | "--interval" => {
                let interval_str = get_arg_value(&args, &mut i, "interval")?;
                let ms: u64 = interval_str
                    .parse()
                    .map_err(|_| format!("invalid interval: {interval_str}"))?;
                config.wait_interval = Duration::from_millis(ms);
            }
            "-c" | "--cert" => {
                let cert_path = get_arg_value(&args, &mut i, "cert")?;
                config.tls_cert = Some(PathBuf::from(cert_path));
            }
            "-q" | "--quiet" => {
                config.quiet = true;
            }
            "-?" | "--help" => {
                print_help();
                process::exit(exit_code::ACCEPTING);
            }
            "-V" | "--version" => {
                println!("mdb_ready {}", env!("CARGO_PKG_VERSION"));
                process::exit(exit_code::ACCEPTING);
            }
            arg if arg.starts_with('-') => {
                return Err(format!("unknown option: {arg}"));
            }
            _ => {
                // Ignore positional arguments
            }
        }
        i += 1;
    }

    // Validate config
    if config.port == 0 {
        return Err("port cannot be 0".to_string());
    }

    if let Some(ref cert_path) = config.tls_cert
        && !cert_path.exists()
    {
        return Err(format!(
            "certificate file not found: {}",
            cert_path.display()
        ));
    }

    Ok(config)
}

fn get_arg_value(args: &[String], i: &mut usize, name: &str) -> Result<String, String> {
    if *i + 1 < args.len() {
        *i += 1;
        Ok(args[*i].clone())
    } else {
        Err(format!("option --{name} requires an argument"))
    }
}

fn print_help() {
    println!(
        r#"mdb_ready - check MonoDB server connection status

Usage: mdb_ready [OPTIONS]

Options:
  -h, --host HOST       Server hostname (default: 127.0.0.1)
  -p, --port PORT       Server port (default: 6432)
  -t, --timeout SECS    Connection timeout in seconds (default: 5)
  -w, --wait SECS       Wait up to SECS seconds for server to start
  -i, --interval MS     Retry interval in milliseconds when waiting (default: 500)
  -c, --cert FILE       Path to TLS certificate for secure connections
  -q, --quiet           Suppress all output, only set exit code
  -V, --version         Show version information
  -?, --help            Show this help message

Exit codes:
  0   Server is accepting connections
  1   Server rejected the connection (e.g., authentication failure)
  2   No response from server (not running or unreachable)
  3   No connection attempt was made (invalid parameters)

Examples:
  mdb_ready                             Check localhost:6432
  mdb_ready -h db.example.com -p 6433   Check remote server
  mdb_ready -w 30                       Wait up to 30 seconds for server
  mdb_ready -c /path/to/ca.pem          Connect with TLS
  mdb_ready -q && echo "ready"          Silent check in scripts
"#
    );
}
