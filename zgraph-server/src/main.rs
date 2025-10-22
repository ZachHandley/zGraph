//! zGraph web server binary
//!
//! Provides HTTP API endpoints for SQL, Cypher, and vector queries
//! with JWT authentication, organization isolation, and observability.

use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

use zgraph_server::{serve, ServerConfig};

#[derive(Parser, Debug)]
#[command(name = "zgraph-server")]
#[command(about = "zGraph web server providing unified query language APIs")]
struct Cli {
    /// Server bind address
    #[arg(long, default_value = "127.0.0.1:8686")]
    bind: String,

    /// Organization ID
    #[arg(long, default_value_t = 1)]
    org_id: u64,

    /// Database path
    #[arg(long, default_value = "data.fjall")]
    database: String,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// JWT secret for development
    #[arg(long, default_value = "dev-secret")]
    jwt_secret: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(&cli.log_level)
        .init();

    info!("Starting zGraph web server");

    // Create server configuration
    let config = ServerConfig {
        bind_addr: cli.bind.parse()?,
        org_id: cli.org_id,
        database_path: cli.database,
        jwt_secret: cli.jwt_secret,
        log_level: cli.log_level,
    };

    // Start the server
    serve(config).await
}