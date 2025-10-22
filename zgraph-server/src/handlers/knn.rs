//! KNN (K-Nearest Neighbors) search handlers for zGraph API
//!
//! Provides HTTP handlers for vector similarity search with proper
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
    models::{ApiResponse, ErrorResponse, KnnResponse, KnnResult},
    AppState,
};
use zexec_engine;

/// KNN search request payload
#[derive(Debug, Deserialize)]
pub struct KnnSearchRequest {
    pub table: String,
    pub column: String,
    pub vector: Vec<f32>,
    pub k: usize,
    #[serde(default = "default_metric")]
    pub metric: String,
}

fn default_metric() -> String {
    "l2".to_string()
}

/// Execute KNN vector search
pub async fn knn_search(
    State(app_state): State<Arc<AppState>>,
    AuthSessionExtractor(session): AuthSessionExtractor,
    OrgIdExtractor(org_id): OrgIdExtractor,
    Json(request): Json<KnnSearchRequest>,
) -> Result<Json<ApiResponse<KnnResponse>>, StatusCode> {
    let start_time = Instant::now();
    debug!("Received KNN request for table: {}, column: {}, k: {}, metric: {} from user: {} in org: {}",
           request.table, request.column, request.k, request.metric, session.user_id, org_id);

    // Validate request parameters
    if request.table.trim().is_empty() || request.column.trim().is_empty() {
        warn!("Invalid KNN request: empty table or column name from user: {}", session.user_id);
        return Err(StatusCode::BAD_REQUEST);
    }

    if request.vector.is_empty() {
        warn!("Invalid KNN request: empty vector from user: {}", session.user_id);
        return Err(StatusCode::BAD_REQUEST);
    }

    if request.k == 0 || request.k > 10000 {
        warn!("Invalid KNN request: k={} must be between 1 and 10000 from user: {}",
              request.k, session.user_id);
        return Err(StatusCode::BAD_REQUEST);
    }

    // Convert metric string to SQL distance operator
    let distance_operator = match request.metric.to_lowercase().as_str() {
        "l2" | "euclidean" => "<->",
        "cosine" | "angular" => "<=>",
        "ip" | "inner_product" | "dot" => "<#>",
        _ => {
            warn!("Unsupported distance metric: {} from user: {}", request.metric, session.user_id);
            return Err(StatusCode::BAD_REQUEST);
        }
    };

    // Format vector as SQL array literal
    let vector_str = request.vector
        .iter()
        .map(|v| format!("{:.6}", v))
        .collect::<Vec<_>>()
        .join(",");

    // Construct KNN SQL query
    let sql_query = format!(
        "SELECT *, embedding {} ARRAY[{}] as distance FROM {} ORDER BY distance LIMIT {}",
        distance_operator,
        vector_str,
        request.table,
        request.k
    );

    debug!("Executing KNN via SQL: {}", sql_query);

    // Execute KNN search using zexec-engine
    match zexec_engine::exec_sql_store_with_metadata(
        &sql_query,
        org_id,
        &app_state.storage,
        &app_state.catalog,
    ) {
        Ok(result_with_metadata) => {
            let search_time_ms = start_time.elapsed().as_millis() as u64;

            debug!("KNN search executed successfully in {}ms", search_time_ms);

            // Convert execution result to KNN response format
            let response_data = match result_with_metadata.result {
                zexec_engine::ExecResult::Rows(rows) => {
                    let mut knn_results = Vec::new();

                    for row in rows {
                        // Extract distance from the result
                        let distance = row.get("distance")
                            .and_then(|v| v.as_f64())
                            .unwrap_or(0.0) as f32;

                        // Try to extract a row ID - look for common ID column names
                        let row_id = row.get("id")
                            .or_else(|| row.get("row_id"))
                            .or_else(|| row.get("_rowid"))
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);

                        // Clone row and remove the distance field from the data
                        let mut data = row.clone();
                        data.shift_remove("distance");

                        knn_results.push(KnnResult {
                            row_id,
                            distance,
                            data: Some(serde_json::to_value(data).unwrap_or_default()),
                        });
                    }

                    KnnResponse {
                        results: knn_results,
                        search_time_ms: Some(search_time_ms),
                    }
                }
                zexec_engine::ExecResult::Affected(_) | zexec_engine::ExecResult::None => {
                    KnnResponse {
                        results: vec![],
                        search_time_ms: Some(search_time_ms),
                    }
                }
            };

            let success_response = ApiResponse::success(response_data, "knn_search".to_string());
            Ok(Json(success_response))
        }
        Err(e) => {
            error!("KNN search failed: {} for user: {} in org: {}",
                   e, session.user_id, org_id);

            // Convert execution errors to appropriate HTTP status codes
            let status_code = if e.to_string().contains("not found") || e.to_string().contains("no such table") {
                StatusCode::NOT_FOUND
            } else if e.to_string().contains("permission") {
                StatusCode::FORBIDDEN
            } else if e.to_string().contains("column") || e.to_string().contains("syntax") {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };

            Err(status_code)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_knn_search_request_deserialization() {
        let json = r#"{
            "table": "documents",
            "column": "embedding",
            "vector": [0.1, 0.2, 0.3, 0.4],
            "k": 10,
            "metric": "cosine"
        }"#;

        let request: KnnSearchRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.table, "documents");
        assert_eq!(request.column, "embedding");
        assert_eq!(request.vector, vec![0.1, 0.2, 0.3, 0.4]);
        assert_eq!(request.k, 10);
        assert_eq!(request.metric, "cosine");
    }

    #[test]
    fn test_default_metric() {
        let json = r#"{
            "table": "documents",
            "column": "embedding",
            "vector": [0.1, 0.2, 0.3],
            "k": 5
        }"#;

        let request: KnnSearchRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.metric, "l2");
    }
}