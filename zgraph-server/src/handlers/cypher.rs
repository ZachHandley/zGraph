//! Cypher query handlers for zGraph API
//!
//! Provides HTTP handlers for executing Cypher graph queries with proper
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
    models::{ApiResponse, ErrorResponse, CypherQueryResponse},
    AppState,
};

/// Cypher query request payload
#[derive(Debug, Deserialize)]
pub struct CypherQueryRequest {
    pub cypher: String,
    pub parameters: Option<serde_json::Value>,
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

#[cfg(test)]
mod tests {
    use super::*;

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