//! MonoDB Server Executable
//!
//! Main entry point for the MonoDB database server.

use colored::Colorize;
use mono_core::{Config, MonoResult, server::DatabaseServer};
use std::sync::Arc;
use tracing_subscriber::{filter::EnvFilter, fmt};

#[tokio::main]
async fn main() -> MonoResult<()> {
    // Initialize logging
    fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("debug".parse().unwrap()))
        .init();

    // Print startup banner
    print_banner();

    // Create server configuration
    let mut config = Config::default();
    config.port = 3282; // Set the port to 3282 as requested
    config.data_dir = "./monodb_data".to_string();

    println!("{}", "Starting MonoDB Server...".bright_green());
    println!("Configuration:");
    println!("  Port: {}", config.port.to_string().bright_cyan());
    println!("  Data Directory: {}", config.data_dir.bright_cyan());
    println!(
        "  Max Connections: {}",
        config.max_connections.to_string().bright_cyan()
    );
    println!(
        "  WAL Enabled: {}",
        config.wal_enabled.to_string().bright_cyan()
    );

    // Create the database server
    let server = Arc::new(DatabaseServer::new(config)?);

    // Set up graceful shutdown handler
    let server_for_shutdown = Arc::clone(&server);
    let shutdown_handle = tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        println!("\n{}", "Received shutdown signal".bright_yellow());

        // Trigger server shutdown
        if let Err(e) = server_for_shutdown.shutdown().await {
            eprintln!("{} {}", "Error during shutdown:".bright_red(), e);
        }
    });

    // Start the server
    let server_result = tokio::select! {
        result = server.start() => result,
        _ = shutdown_handle => {
            // Shutdown completed
            println!("{}", "MonoDB Server stopped".bright_green());
            Ok(())
        }
    };

    match server_result {
        Ok(_) => {
            println!("{}", "MonoDB Server stopped".bright_green());
        }
        Err(e) => {
            eprintln!("{} {}", "Server error:".bright_red(), e);
            return Err(e);
        }
    }

    Ok(())
}

fn print_banner() {
    let banner = r#"
    ███╗   ███╗ ██████╗ ███╗   ██╗ ██████╗ ██████╗ ██████╗ 
    ████╗ ████║██╔═══██╗████╗  ██║██╔═══██╗██╔══██╗██╔══██╗
    ██╔████╔██║██║   ██║██╔██╗ ██║██║   ██║██║  ██║██████╔╝
    ██║╚██╔╝██║██║   ██║██║╚██╗██║██║   ██║██║  ██║██╔══██╗
    ██║ ╚═╝ ██║╚██████╔╝██║ ╚████║╚██████╔╝██████╔╝██████╔╝
    ╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═══╝ ╚═════╝ ╚═════╝ ╚═════╝ 
                       Database Engine v0.1.0
    "#;

    println!("{}", banner.bright_blue());
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to create SIGINT handler");
        let mut sigterm =
            signal(SignalKind::terminate()).expect("Failed to create SIGTERM handler");

        tokio::select! {
            _ = sigint.recv() => {},
            _ = sigterm.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        use tokio::signal;
        signal::ctrl_c().await.expect("Failed to listen for ctrl-c");
    }
}
