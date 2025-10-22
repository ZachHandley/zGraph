//! Service handlers for zGraph API
//!
//! Provides HTTP handlers for service management, health checks, and administrative
//! operations with proper authentication and error handling.

use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::{
    auth::{AuthSessionExtractor, OrgIdExtractor},
    models::{ApiResponse, ErrorResponse},
    AppState,
};

/// Service status response
#[derive(Debug, Serialize)]
pub struct ServiceStatusResponse {
    pub name: String,
    pub status: String,
    pub uptime_seconds: u64,
    pub last_check: chrono::DateTime<chrono::Utc>,
    pub details: serde_json::Value,
}

/// Service list response
#[derive(Debug, Serialize)]
pub struct ServiceListResponse {
    pub services: Vec<ServiceStatusResponse>,
    pub total_count: usize,
}

/// Get all services status
pub async fn list_services(
    State(app_state): State<Arc<AppState>>,
    AuthSessionExtractor(_session): AuthSessionExtractor,
    OrgIdExtractor(_org_id): OrgIdExtractor,
) -> Result<Json<ApiResponse<ServiceListResponse>>, StatusCode> {
    debug!("Listing all services");

    // For now, return a simple success response
    // In Phase 2, this will query actual service status
    let services = vec![
        ServiceStatusResponse {
            name: "database".to_string(),
            status: "running".to_string(),
            uptime_seconds: 3600,
            last_check: chrono::Utc::now(),
            details: serde_json::json!({"connections": 5}),
        },
        ServiceStatusResponse {
            name: "vector-index".to_string(),
            status: "running".to_string(),
            uptime_seconds: 3600,
            last_check: chrono::Utc::now(),
            details: serde_json::json!({"index_size": "1.2GB"}),
        },
    ];

    let response_data = ServiceListResponse {
        services,
        total_count: 2,
    };

    let success_response = ApiResponse::success(response_data, "list_services".to_string());
    Ok(Json(success_response))
}

/// Get specific service status
pub async fn get_service_status(
    State(app_state): State<Arc<AppState>>,
    AuthSessionExtractor(_session): AuthSessionExtractor,
    OrgIdExtractor(_org_id): OrgIdExtractor,
    Path(service_name): Path<String>,
) -> Result<Json<ApiResponse<ServiceStatusResponse>>, StatusCode> {
    debug!("Getting status for service: {}", service_name);

    // For now, return a simple success response
    // In Phase 2, this will query actual service status
    let response_data = ServiceStatusResponse {
        name: service_name.clone(),
        status: "running".to_string(),
        uptime_seconds: 3600,
        last_check: chrono::Utc::now(),
        details: serde_json::json!({}),
    };

    let success_response = ApiResponse::success(response_data, "get_service_status".to_string());
    Ok(Json(success_response))
}

/// Restart a service
pub async fn restart_service(
    State(app_state): State<Arc<AppState>>,
    AuthSessionExtractor(_session): AuthSessionExtractor,
    OrgIdExtractor(_org_id): OrgIdExtractor,
    Path(service_name): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    debug!("Restarting service: {}", service_name);

    // For now, return a simple success response
    // In Phase 2, this will actually restart the service
    let response_data = serde_json::json!({
        "message": format!("Service '{}' restart initiated", service_name),
        "service_name": service_name
    });

    let success_response = ApiResponse::success(response_data, "restart_service".to_string());
    Ok(Json(success_response))
}

/// Get service metrics
pub async fn get_service_metrics(
    State(app_state): State<Arc<AppState>>,
    AuthSessionExtractor(_session): AuthSessionExtractor,
    OrgIdExtractor(_org_id): OrgIdExtractor,
    Path(service_name): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    debug!("Getting metrics for service: {}", service_name);

    // For now, return a simple success response
    // In Phase 2, this will query actual service metrics
    let response_data = serde_json::json!({
        "service_name": service_name,
        "cpu_usage": 15.5,
        "memory_usage": 512.3,
        "request_count": 1000,
        "error_rate": 0.01,
        "response_time_avg": 45.2,
        "timestamp": chrono::Utc::now().to_rfc3339()
    });

    let success_response = ApiResponse::success(response_data, "get_service_metrics".to_string());
    Ok(Json(success_response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_status_response_serialization() {
        let response = ServiceStatusResponse {
            name: "test-service".to_string(),
            status: "running".to_string(),
            uptime_seconds: 3600,
            last_check: chrono::Utc::now(),
            details: serde_json::json!({"test": true}),
        };

        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["name"], "test-service");
        assert_eq!(json["status"], "running");
        assert_eq!(json["uptime_seconds"], 3600);
    }
}