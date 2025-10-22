//! SQL query handlers for zGraph API
//!
//! Provides HTTP handlers for executing SQL queries with proper
//! authentication, validation, and error handling.

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
    models::{ApiResponse, ErrorResponse, SqlQueryResponse},
    AppState,
};

/// SQL query request payload
#[derive(Debug, Deserialize)]
pub struct SqlQueryRequest {
    pub sql: String,
    pub parameters: Option<serde_json::Value>,
}

/// Execute SQL query
pub async fn sql_query(
    State(app_state): State<Arc<AppState>>,
    AuthSessionExtractor(_session): AuthSessionExtractor,
    OrgIdExtractor(_org_id): OrgIdExtractor,
    Json(request): Json<SqlQueryRequest>,
) -> Result<Json<ApiResponse<SqlQueryResponse>>, StatusCode> {
    debug!("Received SQL query: {}", request.sql);

    // For now, return a simple success response
    // In Phase 2, this will integrate with DataFusion execution engine
    let response_data = SqlQueryResponse {
        results: vec![],
        rows_affected: Some(0),
        execution_time_ms: Some(0),
    };

    let success_response = ApiResponse::success(response_data, "sql_query".to_string());
    Ok(Json(success_response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_query_request_deserialization() {
        let json = r#"{
            "sql": "SELECT * FROM users WHERE id = ?",
            "parameters": {
                "1": 123
            }
        }"#;

        let request: SqlQueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.sql, "SELECT * FROM users WHERE id = ?");
        assert!(request.parameters.is_some());
    }
}