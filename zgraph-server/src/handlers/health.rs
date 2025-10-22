//! Health check handlers for zGraph web server
//!
//! Provides /health and /ready endpoints for monitoring
//! and integrates with the observability system.

use axum::{
    extract::State,
    response::{IntoResponse, Json},
    Json,
};
use std::sync::Arc;
use tracing::{info, warn};

use crate::{
    models::{ApiResponse, ErrorResponse},
    observability::{self as obs, ReadyResponse},
};

/// Health check response
#[derive(Debug, serde::Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub version: String,
    pub uptime_seconds: u64,
    pub database_status: String,
    pub vector_index_status: Option<String>,
}

/// Database health response
#[derive(Debug, serde::Serialize)]
pub struct DatabaseHealthResponse {
    pub status: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub storage_ping: bool,
    pub catalog_ping: bool,
    pub error: Option<String>,
}

/// Database health status
#[derive(Debug, serde::Serialize)]
pub struct DatabaseHealth {
    pub overall_status: String,
    pub storage_ping: bool,
    pub catalog_ping: bool,
    pub last_check: chrono::DateTime<chrono::Utc>,
    pub uptime_seconds: u64,
}

/// Vector index health response
#[derive(Debug, serde::Serialize)]
pub struct VectorIndexHealth {
    pub status: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub table_accessible: bool,
    pub index_available: bool,
    pub error: Option<String>,
}

/// Main health check endpoint
pub async fn health_check(
    State(app_state): State<Arc<crate::AppState>>,
) -> Result<Json<HealthResponse>, StatusCode> {
    info!("Health check requested");

    let app_state = app_state.0;
    let metrics = app_state.metrics.clone();

    // Perform database health checks
    let database_health = obs::database_health(
        app_state.storage.clone(),
        app_state.catalog.clone(),
    ).await;

    // Check vector index availability
    let vector_health = obs::vector_index_health(
        app_state.storage.clone(),
        app_state.catalog.clone(),
        1, // org_id
        "users", // table
        "profile_vector", // column
    ).await;

    // Determine overall health status
    let overall_status = match (&database_health, &vector_health) {
        (obs::HealthStatus::Healthy, obs::HealthStatus::Healthy) => "healthy",
        (obs::HealthStatus::Healthy, obs::HealthStatus::Degraded) => "degraded",
        (obs::HealthStatus::Degraded, obs::HealthStatus::Healthy) => "degraded",
        (obs::HealthStatus::Unhealthy, _) => "unhealthy",
    };

    let uptime_seconds = metrics.get_metrics().await.uptime_seconds;
    let current_time = chrono::Utc::now();

    let response_data = HealthResponse {
        status: overall_status.to_string(),
        timestamp: current_time,
        version: env!("CARGO_PKG_VERSION").unwrap_or_else(|_| "unknown".to_string()),
        uptime_seconds,
        database_status: format!("storage={}, catalog={}", database_health.storage_ping, database_health.catalog_ping),
        vector_index_status: vector_health.status.map(|s| s.to_string()),
    };

    info!("Health check completed: status={}", overall_status);

    let response = ApiResponse::success(response_data, "health_check".to_string());
    Ok((StatusCode::OK, Json(response)).into_response())
}

/// Server ready check endpoint
pub async fn ready_check(
    State(app_state): State<Arc<crate::AppState>>,
) -> Result<Json<obs::ReadyResponse>, StatusCode> {
    info!("Ready check requested");

    let current_time = chrono::Utc::now();
    let uptime_seconds = app_state.metrics.get_metrics().await.uptime_seconds;

    let response_data = obs::ReadyResponse {
        ready: true,
        timestamp: current_time,
        version: env!("CARGO_PKG_VERSION").unwrap_or_else(|_| "unknown".to_string()),
        message: "zGraph server is ready to accept queries".to_string(),
        uptime_seconds,
    };

    info!("Ready check completed: server is accepting queries");

    let response = ApiResponse::success(response_data, "ready_check".to_string());
    Ok((StatusCode::OK, Json(response)).into_response())
}