//! MonoDB Server Main Entry Point

use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{config::Config, daemon::Server};

mod auth;
mod config;
mod daemon;
mod namespace;
mod network;
mod query_engine;
mod storage;

fn get_env_filter() -> EnvFilter {
    if std::env::var_os("RUST_LOG").is_some() {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    } else if cfg!(debug_assertions) {
        EnvFilter::new("monod=debug")
    } else {
        EnvFilter::new("monod=info")
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .append(true)
        .read(false)
        .open("server.log")?;

    // Non-blocking log appenders
    let (file_non_blocking, file_guard) = tracing_appender::non_blocking(log_file);
    let (console_non_blocking, console_guard) = tracing_appender::non_blocking(std::io::stderr());
    // Maintain guard references to keep log threads alive
    let _guards = (file_guard, console_guard);

    // Configure tracing layers for file and console output
    let env_filter = get_env_filter();

    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(file_non_blocking)
        .with_ansi(false)
        .with_target(true)
        .with_line_number(true)
        .with_file(true)
        .with_thread_ids(true)
        .with_thread_names(true);

    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(console_non_blocking)
        .with_ansi(true)
        .with_target(false)
        .with_timer(tracing_subscriber::fmt::time::time())
        .compact();

    Registry::default()
        .with(env_filter)
        .with(file_layer)
        .with(stderr_layer)
        .init();

    let config = Config::load_from_path("config.toml")?;

    #[cfg(debug_assertions)]
    {
        tracing::debug!("Server running in debug mode");

        let cwd = std::env::current_dir().unwrap();
        tracing::debug!("Current Working Directory: {}", cwd.display());
    }

    let server = Server::new(config.server, config.storage, config.plugins, config.auth).await?;

    tracing::info!("MonoDB Server starting on {}", server.address());

    // Spawn signal handler for graceful shutdown
    let shutdown_signal = server.shutdown_signal();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal;

            let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
                .expect("Failed to register SIGINT handler");
            let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("Failed to register SIGTERM handler");

            tokio::select! {
                _ = sigint.recv() => {
                    tracing::info!("Received SIGINT (Ctrl+C), initiating shutdown...");
                }
                _ = sigterm.recv() => {
                    tracing::info!("Received SIGTERM, initiating shutdown...");
                }
            }
        }

        #[cfg(windows)]
        {
            use tokio::signal;

            // Attempt to register Windows console signals
            let ctrl_c_result = signal::windows::ctrl_c();
            let ctrl_break_result = signal::windows::ctrl_break();
            let ctrl_close_result = signal::windows::ctrl_close();
            let ctrl_shutdown_result = signal::windows::ctrl_shutdown();
            let ctrl_logoff_result = signal::windows::ctrl_logoff();

            // Handle signal registration results
            match (
                ctrl_c_result,
                ctrl_break_result,
                ctrl_close_result,
                ctrl_shutdown_result,
                ctrl_logoff_result,
            ) {
                (
                    Ok(mut ctrl_c),
                    Ok(mut ctrl_break),
                    Ok(mut ctrl_close),
                    Ok(mut ctrl_shutdown),
                    Ok(mut ctrl_logoff),
                ) => {
                    tracing::info!("All Windows signal handlers registered successfully");
                    tokio::select! {
                        _ = ctrl_c.recv() => {
                            tracing::info!("Received Ctrl+C, initiating graceful shutdown...");
                        }
                        _ = ctrl_break.recv() => {
                            tracing::info!("Received Ctrl+Break, initiating graceful shutdown...");
                        }
                        _ = ctrl_close.recv() => {
                            tracing::info!("Received Ctrl+Close, initiating graceful shutdown...");
                        }
                        _ = ctrl_shutdown.recv() => {
                            tracing::info!("Received system shutdown signal, initiating graceful shutdown...");
                        }
                        _ = ctrl_logoff.recv() => {
                            tracing::info!("Received logoff signal, initiating graceful shutdown...");
                        }
                    }
                }
                _ => {
                    // Fallback: try to register at least Ctrl+C
                    tracing::warn!(
                        "Some Windows signal handlers failed to register, using fallback"
                    );
                    match signal::windows::ctrl_c() {
                        Ok(mut ctrl_c) => {
                            tracing::info!("Ctrl+C handler registered as fallback");
                            ctrl_c.recv().await;
                            tracing::info!("Received Ctrl+C, initiating graceful shutdown...");
                        }
                        Err(e) => {
                            tracing::error!("Failed to register Ctrl+C handler: {}", e);
                            tracing::warn!("Server will run without console signal handling");
                            std::future::pending::<()>().await;
                        }
                    }
                }
            }
        }

        if let Err(e) = shutdown_signal.send(()) {
            tracing::error!("Failed to send shutdown signal: {e}");
        }
    });

    // Run server until shutdown
    server.run().await?;

    // Perform graceful shutdown (flush storage, etc.)
    server.shutdown().await?;

    tracing::info!("MonoDB Server stopped");
    Ok(())
}
