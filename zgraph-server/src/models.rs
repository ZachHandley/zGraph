//! Data models for zGraph web API requests and responses
//!
//! Provides consistent JSON structures for all API endpoints
//! with proper serialization/deserialization support.

use serde::{Deserialize, Serialize};

/// API response wrapper for consistent error handling
#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub data: Option<T>,
    pub error: Option<String>,
    pub message: String,
    pub request_id: String,
}

/// Standard error response format
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
    pub details: Option<serde_json::Value>,
}

/// Health check response
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub version: String,
    pub uptime_seconds: u64,
    pub database_status: String,
    pub vector_index_status: String,
}

/// SQL query response
#[derive(Debug, Serialize)]
pub struct SqlQueryResponse {
    pub results: Vec<serde_json::Value>,
    pub rows_affected: Option<u64>,
    pub execution_time_ms: Option<u64>,
}

/// Cypher query response
#[derive(Debug, Serialize)]
pub struct CypherQueryResponse {
    pub results: Vec<serde_json::Value>,
    pub execution_time_ms: Option<u64>,
}

/// KNN search response
#[derive(Debug, Serialize)]
pub struct KnnResponse {
    pub results: Vec<KnnResult>,
    pub search_time_ms: Option<u64>,
}

/// KNN search result item
#[derive(Debug, Serialize)]
pub struct KnnResult {
    pub row_id: u64,
    pub distance: f32,
    pub data: Option<serde_json::Value>,
}

/// Table creation response
#[derive(Debug, Serialize)]
pub struct CreateTableResponse {
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
}

/// Column definition for table creation
#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: String,
    pub nullable: bool,
    pub vector: Option<VectorDefinition>,
}

/// Vector column specification
#[derive(Debug, Serialize, Deserialize)]
pub struct VectorDefinition {
    pub dimensions: Option<usize>,
    #[serde(default = "default_l2")]
    pub metric: String,
}

fn default_l2() -> String {
    "l2".to_string()
}

/// Row insertion response
#[derive(Debug, Serialize)]
pub struct InsertRowResponse {
    pub row_id: u64,
    pub table_name: String,
}

/// Table search response
#[derive(Debug, Serialize)]
pub struct TableSearchResponse {
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
    pub row_count: u64,
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