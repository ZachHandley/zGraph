use std::net::SocketAddr;
use zserver::{init_tracing, run};

#[tokio::main]
async fn main() {
    init_tracing();

    let addr = std::env::var("ZDB_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
        .parse::<SocketAddr>()
        .expect("Invalid address");

    tracing::info!("Starting ZRUSTDB server on {}", addr);
    run(addr).await;
}