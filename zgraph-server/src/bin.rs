//! zGraph web server binary
//!
//! Launches the HTTP API server for zGraph database operations.

use std::net::SocketAddr;
use tracing::info;

use zgraph_server::serve;

/// Default server configuration
const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8686";
const DEFAULT_ORG_ID: u64 = 1;
const DEFAULT_DATABASE_PATH: &str = "data.fjall";
const DEFAULT_JWT_SECRET: &str = "zgraph-secret-key";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let bind_addr = std::env::var("ZGRAPH_BIND_ADDR")
        .unwrap_or_else(|_| DEFAULT_BIND_ADDR.parse().unwrap());

    let org_id = std::env::var("ZGRAPH_ORG_ID")
        .map(|s| s.parse().unwrap_or_else(|_| DEFAULT_ORG_ID));

    let database_path = std::env::var("ZGRAPH_DATABASE_PATH")
        .unwrap_or_else(|_| DEFAULT_DATABASE_PATH);

    let jwt_secret = std::env::var("ZGRAPH_JWT_SECRET")
        .unwrap_or_else(|_| DEFAULT_JWT_SECRET);

    let config = zgraph_server::ServerConfig {
        bind_addr,
        org_id,
        database_path,
        jwt_secret,
        log_level: "info".to_string(),
    };

    info!("Starting zGraph web server");
    info!("Listening on {}", config.bind_addr);

    zgraph_server::serve(config).await
}