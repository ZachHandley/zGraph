//! Table management handlers for zGraph API
//!
//! Provides HTTP handlers for creating, reading, updating, and deleting tables
//! with proper authentication and validation.

use axum::{
    extract::{Path, State, Query},
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::{
    auth::{AuthSessionExtractor, OrgIdExtractor},
    models::{ApiResponse, ErrorResponse, CreateTableResponse, TableSearchResponse, ColumnDefinition},
    AppState,
};

/// Create table request payload
#[derive(Debug, Deserialize)]
pub struct CreateTableRequest {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
}

/// Create a new table
pub async fn create_table(
    State(app_state): State<Arc<AppState>>,
    AuthSessionExtractor(_session): AuthSessionExtractor,
    OrgIdExtractor(_org_id): OrgIdExtractor,
    Json(request): Json<CreateTableRequest>,
) -> Result<Json<ApiResponse<CreateTableResponse>>, StatusCode> {
    debug!("Creating table: {}", request.name);

    // For now, return a simple success response
    // In Phase 2, this will actually create the table
    let response_data = CreateTableResponse {
        table_name: request.name.clone(),
        columns: request.columns,
        row_count: 0,
    };

    let success_response = ApiResponse::success(response_data, "create_table".to_string());
    Ok(Json(success_response))
}

/// Get table information by name
pub async fn get_table(
    State(app_state): State<Arc<AppState>>,
    AuthSessionExtractor(_session): AuthSessionExtractor,
    OrgIdExtractor(_org_id): OrgIdExtractor,
    Path(table_name): Path<String>,
) -> Result<Json<ApiResponse<TableSearchResponse>>, StatusCode> {
    debug!("Getting table info for: {}", table_name);

    // For now, return a simple success response
    // In Phase 2, this will get actual table information
    let response_data = TableSearchResponse {
        table_name: table_name.clone(),
        columns: vec![],
        row_count: 0,
    };

    let success_response = ApiResponse::success(response_data, "get_table".to_string());
    Ok(Json(success_response))
}

/// Search in a table
pub async fn search_table(
    State(app_state): State<Arc<AppState>>,
    AuthSessionExtractor(_session): AuthSessionExtractor,
    OrgIdExtractor(_org_id): OrgIdExtractor,
    Path(table_name): Path<String>,
    Query(params): Query<serde_json::Value>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    debug!("Searching in table: {} with params: {:?}", table_name, params);

    // For now, return a simple success response
    // In Phase 2, this will perform actual table search
    let response_data = serde_json::json!({
        "table": table_name,
        "results": [],
        "total_count": 0
    });

    let success_response = ApiResponse::success(response_data, "search_table".to_string());
    Ok(Json(success_response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table_request_deserialization() {
        let json = r#"{
            "name": "users",
            "columns": [
                {
                    "name": "id",
                    "type": "integer",
                    "nullable": false,
                    "vector": null
                },
                {
                    "name": "name",
                    "type": "text",
                    "nullable": false,
                    "vector": null
                }
            ]
        }"#;

        let request: CreateTableRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.name, "users");
        assert_eq!(request.columns.len(), 2);
    }
}