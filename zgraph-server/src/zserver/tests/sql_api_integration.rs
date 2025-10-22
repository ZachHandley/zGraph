use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;
use tower::util::ServiceExt;
use zauth::{AuthConfig, AuthService};
use zbrain::{JobQueue, Storage};
use zcore_catalog::Catalog;
use zcore_storage::{Store, TBL_ENVIRONMENTS, TBL_USERS, TBL_USER_EMAIL, TBL_SESSIONS, TBL_CATALOG, TBL_ROWS, TBL_SEQ};
use zpermissions::PermissionService;
use zserver::{create_app, AppState, WsMetrics, IceServerConfig, WebRTCState, SubscriptionManager, FunctionTriggerManager};
use std::collections::HashMap;
use tokio::sync::RwLock as TokioRwLock;

async fn setup_test_app() -> (axum::Router, String) {
    // Ensure we're not in production mode for tests
    std::env::set_var("ENVIRONMENT", "development");
    std::env::set_var("IS_PRODUCTION", "false");
    std::env::set_var("DEV_MODE", "true");

    let tmp = tempdir().unwrap();
    let store = Store::open(tmp.path().join("test.redb")).unwrap();
    let store_static: &'static Store = Box::leak(Box::new(store));
    let catalog = Catalog::new(store_static);
    let storage = Storage::in_memory().unwrap();
    let artifacts_root = tmp.path().join("artifacts");
    std::fs::create_dir_all(&artifacts_root).unwrap();
    let (job_queue, _rx) = JobQueue::new(storage, &artifacts_root);
    let job_queue = Arc::new(job_queue);

    // Initialize database tables
    {
        let mut w = store_static.begin_write().unwrap();

        // Core storage tables
        let _ = w.open_table(store_static, TBL_CATALOG).unwrap();
        let _ = w.open_table(store_static, TBL_ROWS).unwrap();
        let _ = w.open_table(store_static, TBL_SEQ).unwrap();

        // Auth tables
        let _ = w.open_table(store_static, TBL_ENVIRONMENTS).unwrap();
        let _ = w.open_table(store_static, TBL_USERS).unwrap();
        let _ = w.open_table(store_static, TBL_USER_EMAIL).unwrap();
        let _ = w.open_table(store_static, TBL_SESSIONS).unwrap();

        w.commit(store_static).unwrap();
    }

    let state = AppState {
        auth: AuthConfig::with_dev(123, "secret"),
        store: store_static,
        catalog,
        files_root: tmp.path().join("files"),
        job_queue: job_queue.clone(),
        ws_metrics: Arc::new(WsMetrics::default()),
        permissions: PermissionService::new(store_static),
        default_org_id: 123,
        mesh_network: None,
        webrtc_api: Arc::new("stub_api".to_string()),
        ice_servers: vec![IceServerConfig {
            urls: vec!["stun:stun.l.google.com:19302".to_string()],
            username: None,
            credential: None,
        }],
        rtc_connections: Arc::new(TokioRwLock::new(HashMap::new())),
        webrtc_state: WebRTCState::new(Arc::new("stub_api".to_string()), Default::default()),
        subscription_manager: Arc::new(SubscriptionManager::new(Arc::new(PermissionService::new(store_static)))),
        function_trigger_manager: Arc::new(FunctionTriggerManager::new()),
    };

    // Initialize default environments
    let auth_service = AuthService::new(store_static, state.auth.clone());
    auth_service.repo().ensure_default_environments(123).unwrap();

    let token = zauth::issue_dev_token(&state.auth).unwrap();
    let app = create_app(state);

    (app, token)
}

#[tokio::test]
async fn test_sql_create_table_success() {
    let (app, token) = setup_test_app().await;

    let sql_request = json!({
        "sql": "CREATE TABLE users (id INT, name TEXT, email TEXT)"
    });

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/sql")
                .method("POST")
                .header("authorization", format!("Bearer {}", token))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&sql_request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let response_json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Should return empty rows array for CREATE TABLE
    assert_eq!(response_json["rows"], json!([]));
}

#[tokio::test]
async fn test_sql_insert_and_select() {
    let (app, token) = setup_test_app().await;

    // First, create a table
    let create_sql = json!({
        "sql": "CREATE TABLE test_table (id INT, name TEXT)"
    });

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/sql")
                .method("POST")
                .header("authorization", format!("Bearer {}", token))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&create_sql).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Insert some data
    let insert_sql = json!({
        "sql": "INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')"
    });

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/sql")
                .method("POST")
                .header("authorization", format!("Bearer {}", token))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&insert_sql).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Select the data back
    let select_sql = json!({
        "sql": "SELECT * FROM test_table"
    });

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/sql")
                .method("POST")
                .header("authorization", format!("Bearer {}", token))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&select_sql).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let response_json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Should return the inserted rows
    let rows = &response_json["rows"];
    assert!(rows.is_array());
    let rows_array = rows.as_array().unwrap();
    assert_eq!(rows_array.len(), 2);

    // Check the data
    assert_eq!(rows_array[0]["id"], json!(1));
    assert_eq!(rows_array[0]["name"], json!("Alice"));
    assert_eq!(rows_array[1]["id"], json!(2));
    assert_eq!(rows_array[1]["name"], json!("Bob"));
}

#[tokio::test]
async fn test_sql_error_handling() {
    let (app, token) = setup_test_app().await;

    // Test invalid SQL syntax
    let invalid_sql = json!({
        "sql": "INVALID SQL SYNTAX"
    });

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/sql")
                .method("POST")
                .header("authorization", format!("Bearer {}", token))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&invalid_sql).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let error_message = String::from_utf8(body.to_vec()).unwrap();
    assert!(error_message.contains("SQL parsing failed"));

    // Test non-existent table
    let nonexistent_table_sql = json!({
        "sql": "SELECT * FROM nonexistent_table"
    });

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/sql")
                .method("POST")
                .header("authorization", format!("Bearer {}", token))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&nonexistent_table_sql).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let error_message = String::from_utf8(body.to_vec()).unwrap();
    assert!(error_message.contains("no such table") || error_message.contains("does not exist"));
}

#[tokio::test]
async fn test_sql_with_vector_data() {
    let (app, token) = setup_test_app().await;

    // Create table with vector column
    let create_sql = json!({
        "sql": "CREATE TABLE documents (id INT, title TEXT, embedding VECTOR)"
    });

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/sql")
                .method("POST")
                .header("authorization", format!("Bearer {}", token))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&create_sql).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Insert document with vector
    let insert_sql = json!({
        "sql": "INSERT INTO documents VALUES (1, 'Document 1', ARRAY[1.0, 2.0, 3.0])"
    });

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/sql")
                .method("POST")
                .header("authorization", format!("Bearer {}", token))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&insert_sql).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Select the data back
    let select_sql = json!({
        "sql": "SELECT * FROM documents"
    });

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/sql")
                .method("POST")
                .header("authorization", format!("Bearer {}", token))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&select_sql).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let response_json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let rows = &response_json["rows"];
    assert!(rows.is_array());
    let rows_array = rows.as_array().unwrap();
    assert_eq!(rows_array.len(), 1);

    // Check the vector data
    assert_eq!(rows_array[0]["id"], json!(1));
    assert_eq!(rows_array[0]["title"], json!("Document 1"));
    assert_eq!(rows_array[0]["embedding"], json!([1.0, 2.0, 3.0]));
}

#[tokio::test]
async fn test_sql_unauthorized_access() {
    let (app, _token) = setup_test_app().await;

    let sql_request = json!({
        "sql": "SELECT 1"
    });

    // Request without authorization header
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/sql")
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&sql_request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}