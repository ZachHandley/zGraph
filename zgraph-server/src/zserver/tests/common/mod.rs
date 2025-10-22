use axum::{
    body::Body,
    http::{Request, StatusCode},
    response::Response,
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
use tokio::time::{sleep, Duration};

pub struct TestClient {
    app: axum::Router,
    auth_token: String,
}

impl TestClient {
    pub async fn new() -> Self {
        let (app, token) = setup_test_app().await;
        Self { app, auth_token: token }
    }

    pub fn with_token(mut self, token: String) -> Self {
        self.auth_token = token;
        self
    }

    pub async fn request(&self, method: &str, uri: &str, body: Option<serde_json::Value>) -> Response {
        let body_bytes = body.map(|b| Body::from(serde_json::to_vec(&b).unwrap())).unwrap_or(Body::empty());

        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(body_bytes)
            .unwrap();

        if !self.auth_token.is_empty() {
            request.headers_mut().insert(
                "authorization",
                format!("Bearer {}", self.auth_token).parse().unwrap(),
            );
        }

        self.app.clone().oneshot(request).await.unwrap()
    }

    pub async fn get(&self, uri: &str) -> Response {
        self.request("GET", uri, None).await
    }

    pub async fn post(&self, uri: &str, body: serde_json::Value) -> Response {
        self.request("POST", uri, Some(body)).await
    }

    pub async fn put(&self, uri: &str, body: serde_json::Value) -> Response {
        self.request("PUT", uri, Some(body)).await
    }

    pub async fn delete(&self, uri: &str) -> Response {
        self.request("DELETE", uri, None).await
    }

    pub async fn get_json(&self, uri: &str) -> serde_json::Value {
        let response = self.get(uri).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    pub async fn post_json(&self, uri: &str, body: serde_json::Value) -> serde_json::Value {
        let response = self.post(uri, body).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    pub async fn get_token(&self) -> String {
        self.auth_token.clone()
    }
}

pub async fn setup_test_app() -> (axum::Router, String) {
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

pub async fn create_test_table(client: &TestClient, table_name: &str, columns: &str) {
    let create_sql = json!({
        "sql": format!("CREATE TABLE {} ({})", table_name, columns)
    });

    let response = client.post("/v1/sql", create_sql).await;
    assert_eq!(response.status(), StatusCode::OK);
}

pub async fn insert_test_data(client: &TestClient, table_name: &str, values: &str) {
    let insert_sql = json!({
        "sql": format!("INSERT INTO {} VALUES {}", table_name, values)
    });

    let response = client.post("/v1/sql", insert_sql).await;
    assert_eq!(response.status(), StatusCode::OK);
}

pub async fn select_test_data(client: &TestClient, table_name: &str, where_clause: Option<&str>) -> serde_json::Value {
    let sql = if let Some(where_clause) = where_clause {
        format!("SELECT * FROM {} WHERE {}", table_name, where_clause)
    } else {
        format!("SELECT * FROM {}", table_name)
    };

    let select_sql = json!({ "sql": sql });
    client.post_json("/v1/sql", select_sql).await
}

pub async fn wait_for_job_completion(client: &TestClient, job_id: &str, timeout_secs: u64) -> serde_json::Value {
    let mut attempts = 0;
    let max_attempts = timeout_secs * 2; // Check every 500ms

    while attempts < max_attempts {
        let response = client.get(&format!("/v1/jobs/{}", job_id)).await;
        if response.status() == StatusCode::OK {
            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let job_info: serde_json::Value = serde_json::from_slice(&body).unwrap();

            let status = job_info["state"].as_str().unwrap_or("");
            if status == "completed" || status == "failed" || status == "cancelled" {
                return job_info;
            }
        }

        sleep(Duration::from_millis(500)).await;
        attempts += 1;
    }

    panic!("Job {} did not complete within {} seconds", job_id, timeout_secs);
}

pub fn assert_json_contains(actual: &serde_json::Value, expected: &serde_json::Value) {
    match (actual, expected) {
        (serde_json::Value::Object(actual_obj), serde_json::Value::Object(expected_obj)) => {
            for (key, expected_value) in expected_obj {
                let actual_value = actual_obj.get(key).unwrap_or_else(|| {
                    panic!("Expected key '{}' not found in actual JSON", key);
                });
                assert_json_contains(actual_value, expected_value);
            }
        }
        (serde_json::Value::Array(actual_array), serde_json::Value::Array(expected_array)) => {
            assert_eq!(actual_array.len(), expected_array.len());
            for (actual_item, expected_item) in actual_array.iter().zip(expected_array.iter()) {
                assert_json_contains(actual_item, expected_item);
            }
        }
        _ => {
            assert_eq!(actual, expected);
        }
    }
}

pub async fn assert_error_response(response: Response, expected_status: StatusCode, expected_message: &str) {
    assert_eq!(response.status(), expected_status);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let error_text = String::from_utf8(body.to_vec()).unwrap();
    assert!(error_text.contains(expected_message),
            "Expected error message '{}', but got '{}'", expected_message, error_text);
}