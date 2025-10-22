//! Query handlers for zGraph API
//!
//! Provides HTTP handlers for SQL, Cypher, and SPARQL queries with proper
//! authentication, validation, and error handling.

use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

use crate::{
    auth::{AuthSessionExtractor, OrgIdExtractor},
    models::{ApiResponse, ErrorResponse, SqlQueryResponse, CypherQueryResponse},
    AppState,
};
use zexec_engine;

/// SQL query request payload
#[derive(Debug, Deserialize)]
pub struct SqlQueryRequest {
    pub sql: String,
    pub parameters: Option<serde_json::Value>,
}

/// Cypher query request payload
#[derive(Debug, Deserialize)]
pub struct CypherQueryRequest {
    pub cypher: String,
    pub parameters: Option<serde_json::Value>,
}

/// SPARQL query request payload
#[derive(Debug, Deserialize)]
pub struct SparqlQueryRequest {
    pub sparql: String,
    pub parameters: Option<serde_json::Value>,
}

/// Execute SQL query
pub async fn sql_query(
    State(app_state): State<Arc<AppState>>,
    AuthSessionExtractor(session): AuthSessionExtractor,
    OrgIdExtractor(org_id): OrgIdExtractor,
    Json(request): Json<SqlQueryRequest>,
) -> Result<Json<ApiResponse<SqlQueryResponse>>, StatusCode> {
    let start_time = Instant::now();
    debug!("Received SQL query: {} from user: {} in org: {}",
           request.sql, session.user_id, org_id);

    // Validate SQL input
    if request.sql.trim().is_empty() {
        warn!("Empty SQL query received from user: {}", session.user_id);
        return Err(StatusCode::BAD_REQUEST);
    }

    // Execute SQL using zexec-engine
    match zexec_engine::exec_sql_store_with_metadata(
        &request.sql,
        org_id,
        &app_state.storage,
        &app_state.catalog,
    ) {
        Ok(result_with_metadata) => {
            let execution_time_ms = start_time.elapsed().as_millis() as u64;

            debug!("SQL query executed successfully in {}ms, rows: {:?}",
                   execution_time_ms, result_with_metadata.result);

            // Convert execution result to response format
            let response_data = match result_with_metadata.result {
                zexec_engine::ExecResult::Rows(rows) => {
                    SqlQueryResponse {
                        results: rows.into_iter()
                            .map(|row| serde_json::to_value(row).unwrap_or_default())
                            .collect(),
                        rows_affected: Some(rows.len() as u64),
                        execution_time_ms: Some(execution_time_ms),
                    }
                }
                zexec_engine::ExecResult::Affected(count) => {
                    SqlQueryResponse {
                        results: vec![],
                        rows_affected: Some(count),
                        execution_time_ms: Some(execution_time_ms),
                    }
                }
                zexec_engine::ExecResult::None => {
                    SqlQueryResponse {
                        results: vec![],
                        rows_affected: Some(0),
                        execution_time_ms: Some(execution_time_ms),
                    }
                }
            };

            let success_response = ApiResponse::success(response_data, "sql_query".to_string());
            Ok(Json(success_response))
        }
        Err(e) => {
            error!("SQL query failed: {} for user: {} in org: {}",
                   e, session.user_id, org_id);

            // Convert execution errors to appropriate HTTP status codes
            let status_code = if e.to_string().contains("not found") {
                StatusCode::NOT_FOUND
            } else if e.to_string().contains("permission") {
                StatusCode::FORBIDDEN
            } else if e.to_string().contains("syntax") {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };

            Err(status_code)
        }
    }
}

/// Execute Cypher query
pub async fn cypher_query(
    State(app_state): State<Arc<AppState>>,
    AuthSessionExtractor(_session): AuthSessionExtractor,
    OrgIdExtractor(_org_id): OrgIdExtractor,
    Json(request): Json<CypherQueryRequest>,
) -> Result<Json<ApiResponse<CypherQueryResponse>>, StatusCode> {
    debug!("Received Cypher query: {}", request.cypher);

    // For now, return a simple success response
    // In Phase 2, this will integrate with actual Cypher execution
    let response_data = CypherQueryResponse {
        results: vec![],
        execution_time_ms: Some(0),
    };

    let success_response = ApiResponse::success(response_data, "cypher_query".to_string());
    Ok(Json(success_response))
}

/// Execute SPARQL query
pub async fn sparql_query(
    State(app_state): State<Arc<AppState>>,
    AuthSessionExtractor(_session): AuthSessionExtractor,
    OrgIdExtractor(_org_id): OrgIdExtractor,
    Json(request): Json<SparqlQueryRequest>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    debug!("Received SPARQL query: {}", request.sparql);

    // For now, return a simple success response
    // In Phase 2, this will integrate with SPARQL execution engine
    let response_data = serde_json::json!({
        "results": [],
        "execution_time_ms": 0
    });

    let success_response = ApiResponse::success(response_data, "sparql_query".to_string());
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

    #[test]
    fn test_cypher_query_request_deserialization() {
        let json = r#"{
            "cypher": "MATCH (n:User) WHERE n.id = $id RETURN n",
            "parameters": {
                "id": "user123"
            }
        }"#;

        let request: CypherQueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.cypher, "MATCH (n:User) WHERE n.id = $id RETURN n");
        assert!(request.parameters.is_some());
    }
}