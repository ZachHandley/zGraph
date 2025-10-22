use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use std::sync::Arc;
use tempfile::tempdir;
use tower::util::ServiceExt;
use zauth::{AuthConfig, AuthService};
use zbrain::{JobQueue, Storage};
use zcore_catalog::Catalog;
use zcore_storage::{Store, COL_ENVIRONMENTS, COL_USERS, COL_USER_EMAIL, COL_SESSIONS};
use zpermissions::PermissionService;
use zserver::{create_app, AppState, WsMetrics, IceServerConfig, WebRTCState, SubscriptionManager, FunctionTriggerManager, WebRTCConfig};
use std::collections::HashMap;
use tokio::sync::RwLock as TokioRwLock;

#[tokio::test]
async fn whoami_requires_auth() {
    // Ensure we're not in production mode
    std::env::set_var("ENVIRONMENT", "test");
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
        webrtc_state: WebRTCState::new(Arc::new("stub_api".to_string()), WebRTCConfig::default()),
        subscription_manager: Arc::new(SubscriptionManager::new(Arc::new(PermissionService::new(store_static)))),
        function_trigger_manager: Arc::new(FunctionTriggerManager::new()),
    };
    let app = create_app(state);
    let res = app
        .oneshot(
            Request::builder()
                .uri("/v1/whoami")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn whoami_ok_with_bearer() {
    // Ensure we're not in production mode for dev token issuance
    std::env::set_var("ENVIRONMENT", "test");
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
        webrtc_state: WebRTCState::new(Arc::new("stub_api".to_string()), WebRTCConfig::default()),
        subscription_manager: Arc::new(SubscriptionManager::new(Arc::new(PermissionService::new(store_static)))),
        function_trigger_manager: Arc::new(FunctionTriggerManager::new()),
    };
    // Initialize database tables by opening them (fjall creates tables on first open)
    {
        let mut w = store_static.begin_write().unwrap();
        let _ = w.open_table(store_static, COL_ENVIRONMENTS).unwrap();
        let _ = w.open_table(store_static, COL_USERS).unwrap();
        let _ = w.open_table(store_static, COL_USER_EMAIL).unwrap();
        let _ = w.open_table(store_static, COL_SESSIONS).unwrap();
        w.commit(store_static).unwrap();
    }

    // Initialize default environments (like main server does)
    let auth_service = AuthService::new(store_static, state.auth.clone());
    auth_service.repo().ensure_default_environments(123).expect("Failed to create default environments");

    let token = zauth::issue_dev_token(&state.auth).unwrap();
    let app = create_app(state);
    let res = app
        .oneshot(
            Request::builder()
                .uri("/v1/whoami")
                .header("authorization", format!("Bearer {}", token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}
