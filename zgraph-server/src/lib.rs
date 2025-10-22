//! zgraph-server: Web server providing unified query language APIs for zGraph
//!
//! This crate provides HTTP endpoints for SQL, Cypher, and vector queries
//! with JWT authentication, organization-based isolation, and observability.
//!
//! # Features
//! - RESTful API endpoints for SQL and Cypher queries
//! - JWT-based authentication with organization isolation
//! - Vector similarity search with HNSW indexing
//! - Structured logging and metrics collection
//! - Health check endpoints for monitoring
//! - Rate limiting and security middleware

pub mod auth;
pub mod endpoints;
pub mod handlers;
pub mod middleware;
pub mod models;
pub mod observability;

use axum::{
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{delete, get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tracing::{info, error, warn};

use zcore_storage as zs;
use zcore_catalog as zcat;
use zpermissions as zp;

/// Application state shared across all handlers
#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<zs::Store>,
    pub catalog: Arc<zcat::Catalog>,
    pub auth_service: Arc<auth::AuthService>,
    pub metrics: observability::MetricsCollector,
}

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub org_id: u64,
    pub database_path: String,
    pub jwt_secret: String,
    pub log_level: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:8686".parse().unwrap(),
            org_id: 1,
            database_path: "data.fjall".to_string(),
            jwt_secret: "dev-secret".to_string(),
            log_level: "info".to_string(),
        }
    }
}

/// Start the zGraph server with given configuration
pub async fn serve(config: ServerConfig) -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(&config.log_level)
        .init();

    info!("Starting zGraph server on {}", config.bind_addr);
    info!("Organization ID: {}", config.org_id);
    info!("Database path: {}", config.database_path);

    // Initialize storage and catalog
    let storage = Arc::new(zs::Store::open(&config.database_path)?);
    let catalog = Arc::new(zcat::Catalog::new(&storage));

    // Initialize authentication service
    let auth_config = auth::AuthConfig {
        jwt_secret: config.jwt_secret.clone(),
        org_id: config.org_id,
    };
    let auth_service = Arc::new(auth::AuthService::new(auth_config)?);

    // Initialize metrics
    let metrics = observability::MetricsCollector::new();

    // Create application state
    let app_state = AppState {
        storage,
        catalog,
        auth_service,
        metrics,
    };

    // Build router
    let app = create_router(app_state.clone());

    // Create server
    let listener = TcpListener::bind(&config.bind_addr).await?;

    info!("zGraph server listening on {}", config.bind_addr);

    // Run server
    axum::Server::new(listener, ServiceBuilder::new().into_inner())
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

/// Create the main router with all endpoints
fn create_router(app_state: AppState) -> Router {
    Router::new()
        // Health check endpoints
        .route("/health", get(handlers::health::health_check))
        .route("/ready", get(handlers::health::ready_check))

        // Authentication endpoints
        .route("/v1/auth/signup", post(handlers::auth::signup))
        .route("/v1/auth/login", post(handlers::auth::login))
        .route("/v1/auth/refresh", post(handlers::auth::refresh))
        .route("/v1/auth/logout", post(handlers::auth::logout))

        // Query endpoints
        .route("/v1/sql", post(handlers::queries::sql))
        .route("/v1/cypher", post(handlers::queries::cypher))
        .route("/v1/knn", post(handlers::queries::knn))

        // Vector and table management
        .route("/v1/tables", post(handlers::tables::create_table))
        .route("/v1/tables/:name", post(handlers::tables::insert_row))
        .route("/v1/tables/:name/search", get(handlers::tables::search_table))

        // Static file serving for development
        .nest_service("/static", tower_http::services::ServeDir::new("static"))

        // Add global state and middleware
        .with_state(Arc::new(app_state))
        .layer(axum::middleware::from_fn(middleware::security::security_headers))
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ApiResponse<T> {
    pub data: Option<T>,
    pub error: Option<String>,
    pub message: String,
    pub request_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
    pub details: Option<serde_json::Value>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T, request_id: String) -> Self {
        Self {
            data: Some(data),
            error: None,
            message: "Success".to_string(),
            request_id,
        }
    }

    pub fn error(code: &str, message: &str, request_id: String) -> Self {
        Self {
            data: None,
            error: Some(code.to_string()),
            message: message.to_string(),
            request_id,
        }
    }

    pub fn error_with_details(
        code: &str,
        message: &str,
        _details: serde_json::Value,
        request_id: String,
    ) -> Self {
        Self {
            data: None,
            error: Some(code.to_string()),
            message: message.to_string(),
            request_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_creation() {
        let config = ServerConfig::default();
        // Note: This test would require actual database setup
        // For now, just test that the function compiles
        assert_eq!(config.bind_addr.to_string(), "127.0.0.1:8686");
        assert_eq!(config.org_id, 1);
    }
}