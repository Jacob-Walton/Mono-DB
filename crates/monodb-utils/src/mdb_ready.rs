use std::env;
use std::process;

#[tokio::main]
async fn main() {
    let mut host = "127.0.0.1".to_string();
    let mut port = 7899u16;
    let mut quiet = false;

    let args: Vec<String> = env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--host" => {
                if i + 1 < args.len() {
                    host = args[i + 1].clone();
                    i += 1;
                }
            }
            "-p" | "--port" => {
                if i + 1 < args.len() {
                    if let Ok(p) = args[i + 1].parse() {
                        port = p;
                    }
                    i += 1;
                }
            }
            "-q" | "--quiet" => {
                quiet = true;
            }
            "-?" | "--help" => {
                print_help();
                process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }

    let addr = format!("{}:{}", host, port);

    let status = match mdb_is_ready(&addr).await {
        Ok(()) => {
            if !quiet {
                println!("server is accepting connections ({addr})");
            }
            0
        }
        Err(e) => {
            if !quiet {
                eprintln!("server is not accepting connections ({addr}): {e}");
            }
            2
        }
    };
    process::exit(status);
}

fn print_help() {
    println!("mdb_ready checks the connection status of a MonoDB server.\n");
    println!("Usage: mdb_ready [OPTIONS]\n");
    println!("Options:");
    println!("  -h, --host HOST     Server host (default: 127.0.0.1)");
    println!("  -p, --port PORT     Server port (default: 5532)");
    println!("  -q, --quiet         Suppress output, only set exit code");
    println!("  -?, --help          Show this help message");
}

async fn mdb_is_ready(addr: &str) -> anyhow::Result<()> {
    match try_monodb_client(addr).await {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

async fn try_monodb_client(addr: &str) -> anyhow::Result<()> {
    use monodb_client::connection::Connection;
    Connection::connect(addr).await.map(|_| ()).map_err(|e| anyhow::anyhow!(e))
}