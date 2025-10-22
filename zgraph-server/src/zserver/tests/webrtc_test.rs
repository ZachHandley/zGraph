use anyhow::Result;
use serde_json;
use zserver::{create_app, AppState, IceServerConfig, WebRTCState, SubscriptionManager, FunctionTriggerManager};
use zauth::AuthConfig;
use zcore_storage::Store;
use zcore_catalog::Catalog;
use zpermissions::PermissionService;
use zbrain::JobQueue;
use zserver::WsMetrics;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock as TokioRwLock;
use tempfile::TempDir;

async fn create_test_app() -> Result<(axum::Router, TempDir, AuthConfig)> {
    // Set environment to development to enable dev tokens
    std::env::set_var("ENV", "development");
    std::env::set_var("ZAUTH_DEV_SECRET", "dev-secret");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.redb");
    let files_root = temp_dir.path().join("files");
    std::fs::create_dir_all(&files_root)?;

    let store = Store::open(&db_path)?;
    let store_static: &'static Store = Box::leak(Box::new(store));

    let catalog = Catalog::new(store_static);
    let artifacts_root = temp_dir.path().join("artifacts");
    std::fs::create_dir_all(&artifacts_root)?;

    let storage = zbrain::Storage::new(&temp_dir.path().join("jobs.redb"))?;
    let (job_queue, _job_events) = JobQueue::new(storage, &artifacts_root);
    let job_queue_arc = Arc::new(job_queue);

    let auth = AuthConfig::new(
        Some(1),
        Some("dev-secret".to_string()),
        "jwt-secret".to_string(),
        "refresh-secret".to_string(),
        900,
        7 * 24 * 3600,
    );

    let ws_metrics = Arc::new(WsMetrics::default());
    let permissions = PermissionService::new(store_static);
    let permissions_arc = Arc::new(permissions);
    let default_org_id = 1u64;

    // Initialize WebRTC components
    let webrtc_api_legacy = Arc::new("stub-api".to_string());
    let ice_servers = vec![
        IceServerConfig {
            urls: vec!["stun:stun.l.google.com:19302".to_string()],
            username: None,
            credential: None,
        }
    ];
    let rtc_connections = Arc::new(TokioRwLock::new(HashMap::new()));

    // Initialize enhanced WebRTC components
    let webrtc_api_stub = Arc::new("stub-api".to_string());
    let webrtc_config = Default::default(); // Use default WebRTCConfig
    let webrtc_state = WebRTCState::new(webrtc_api_stub.clone(), webrtc_config);
    let subscription_manager = Arc::new(SubscriptionManager::new(permissions_arc.clone()));
    let function_trigger_manager = Arc::new(FunctionTriggerManager::new());

    let state = AppState {
        auth: auth.clone(),
        store: store_static,
        catalog,
        files_root: files_root.to_path_buf(),
        job_queue: job_queue_arc,
        ws_metrics,
        permissions: (*permissions_arc).clone(),
        default_org_id,
        mesh_network: None,
        // Legacy WebRTC fields
        webrtc_api: webrtc_api_legacy,
        ice_servers,
        rtc_connections,
        // Enhanced WebRTC features
        webrtc_state,
        subscription_manager,
        function_trigger_manager,
    };

    let app = create_app(state);
    Ok((app, temp_dir, auth))
}

#[tokio::test]
async fn test_webrtc_endpoint_requires_auth() -> Result<()> {
    let (app, _temp_dir, auth) = create_test_app().await?;

    let offer_payload = serde_json::json!({
        "offer": {
            "type": "offer",
            "sdp": "v=0\r\no=- 123 0 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n"
        }
    });

    let response = axum_test::TestServer::new(app)?
        .post("/v2/rtc/offer")
        .json(&offer_payload)
        .await;

    // Should return 401 without authentication
    assert_eq!(response.status_code(), 401);

    Ok(())
}

#[tokio::test]
async fn test_webrtc_ice_servers_config() -> Result<()> {
    let ice_config = vec![
        IceServerConfig {
            urls: vec!["stun:example.com:3478".to_string()],
            username: Some("testuser".to_string()),
            credential: Some("testpass".to_string()),
        }
    ];

    let serialized = serde_json::to_string(&ice_config)?;
    let deserialized: Vec<IceServerConfig> = serde_json::from_str(&serialized)?;

    assert_eq!(ice_config.len(), deserialized.len());
    assert_eq!(ice_config[0].urls, deserialized[0].urls);
    assert_eq!(ice_config[0].username, deserialized[0].username);
    assert_eq!(ice_config[0].credential, deserialized[0].credential);

    Ok(())
}

#[tokio::test]
async fn test_webrtc_room_creation() -> Result<()> {
    let (app, _temp_dir, auth) = create_test_app().await?;
    let server = axum_test::TestServer::new(app)?;

    // Create a room without authentication should fail
    let create_room_payload = serde_json::json!({
        "room_id": "test-room-001",
        "metadata": {
            "name": "Test Room",
            "description": "A test WebRTC room",
            "max_participants": 10,
            "recording_enabled": false,
            "allow_screen_share": true,
            "allow_chat": true,
            "moderator_only_functions": [],
            "tags": []
        }
    });

    let response = server
        .post("/v2/rtc/rooms")
        .json(&create_room_payload)
        .await;

    assert_eq!(response.status_code(), 401);

    // Test with authentication token - generate proper development token
    let auth_token = zauth::issue_dev_token(&auth)?;
    let response = server
        .post("/v2/rtc/rooms")
        .add_header("Authorization", format!("Bearer {}", auth_token))
        .json(&create_room_payload)
        .await;

    // Should succeed with authentication
    if !response.status_code().is_success() {
        let error_body = response.text();
        println!("Error response status: {}", response.status_code());
        println!("Error response body: {}", error_body);
        panic!("WebRTC room creation failed");
    }
    assert!(response.status_code().is_success());

    let room_response: serde_json::Value = response.json();
    assert_eq!(room_response["id"], "test-room-001");
    assert_eq!(room_response["org_id"], 1);
    assert_eq!(room_response["participant_count"], 0);

    Ok(())
}

#[tokio::test]
async fn test_webrtc_room_management() -> Result<()> {
    let (app, _temp_dir, auth) = create_test_app().await?;
    let server = axum_test::TestServer::new(app)?;
    let auth_token = zauth::issue_dev_token(&auth)?;

    // Create a room
    let create_room_payload = serde_json::json!({
        "room_id": "management-test-room",
        "metadata": {
            "name": "Management Test Room",
            "max_participants": 5,
            "recording_enabled": false,
            "allow_screen_share": true,
            "allow_chat": true,
            "moderator_only_functions": [],
            "tags": []
        }
    });

    let create_response = server
        .post("/v2/rtc/rooms")
        .add_header("Authorization", format!("Bearer {}", auth_token))
        .json(&create_room_payload)
        .await;

    assert!(create_response.status_code().is_success());

    // List rooms - should contain our created room
    let list_response = server
        .get("/v2/rtc/rooms")
        .add_header("Authorization", format!("Bearer {}", auth_token))
        .await;

    assert!(list_response.status_code().is_success());
    let rooms: Vec<serde_json::Value> = list_response.json();
    assert!(rooms.iter().any(|room| room["id"] == "management-test-room"));

    // Get specific room
    let get_response = server
        .get("/v2/rtc/rooms/management-test-room")
        .add_header("Authorization", format!("Bearer {}", auth_token))
        .await;

    assert!(get_response.status_code().is_success());
    let room: serde_json::Value = get_response.json();
    assert_eq!(room["id"], "management-test-room");
    assert_eq!(room["org_id"], 1);

    // Try to delete room with participants should fail (room is empty, but test the endpoint)
    let delete_response = server
        .delete("/v2/rtc/rooms/management-test-room")
        .add_header("Authorization", format!("Bearer {}", auth_token))
        .await;

    assert!(delete_response.status_code().is_success());

    Ok(())
}

#[tokio::test]
async fn test_webrtc_room_join() -> Result<()> {
    let (app, _temp_dir, auth) = create_test_app().await?;
    let server = axum_test::TestServer::new(app)?;
    let auth_token = zauth::issue_dev_token(&auth)?;

    // Create a room first
    let create_room_payload = serde_json::json!({
        "room_id": "join-test-room"
    });

    let create_response = server
        .post("/v2/rtc/rooms")
        .add_header("Authorization", format!("Bearer {}", auth_token))
        .json(&create_room_payload)
        .await;

    assert!(create_response.status_code().is_success());

    // Join the room
    let join_payload = serde_json::json!({
        "display_name": "Test User",
        "role": "Participant",
        "ice_servers": [{
            "urls": ["stun:stun.l.google.com:19302"],
            "username": null,
            "credential": null
        }]
    });

    let join_response = server
        .post("/v2/rtc/rooms/join-test-room/join")
        .add_header("Authorization", format!("Bearer {}", auth_token))
        .json(&join_payload)
        .await;

    // Join should succeed and return session info
    assert!(join_response.status_code().is_success());
    let join_result: serde_json::Value = join_response.json();

    assert!(join_result["session_id"].is_string());
    assert!(join_result["answer"]["sdp"].is_string());
    assert!(join_result["ice_servers"].is_array());
    assert_eq!(join_result["room_info"]["id"], "join-test-room");
    assert_eq!(join_result["room_info"]["participant_count"], 1);

    Ok(())
}

#[tokio::test]
async fn test_webrtc_participants_management() -> Result<()> {
    let (app, _temp_dir, auth) = create_test_app().await?;
    let server = axum_test::TestServer::new(app)?;
    let auth_token = zauth::issue_dev_token(&auth)?;

    // Create and join room
    let create_room_payload = serde_json::json!({
        "room_id": "participants-test-room"
    });

    server
        .post("/v2/rtc/rooms")
        .add_header("Authorization", format!("Bearer {}", auth_token))
        .json(&create_room_payload)
        .await;

    let join_payload = serde_json::json!({
        "display_name": "Participant Test User",
        "role": "Moderator"
    });

    server
        .post("/v2/rtc/rooms/participants-test-room/join")
        .add_header("Authorization", format!("Bearer {}", auth_token))
        .json(&join_payload)
        .await;

    // List participants
    let participants_response = server
        .get("/v2/rtc/rooms/participants-test-room/participants")
        .add_header("Authorization", format!("Bearer {}", auth_token))
        .await;

    assert!(participants_response.status_code().is_success());
    let participants: Vec<serde_json::Value> = participants_response.json();
    assert_eq!(participants.len(), 1);
    assert_eq!(participants[0]["display_name"], "Participant Test User");
    assert_eq!(participants[0]["role"], "Moderator");

    // Leave room
    let leave_response = server
        .post("/v2/rtc/rooms/participants-test-room/leave")
        .add_header("Authorization", format!("Bearer {}", auth_token))
        .await;

    assert!(leave_response.status_code().is_success());

    // Verify participant list is now empty
    let empty_participants_response = server
        .get("/v2/rtc/rooms/participants-test-room/participants")
        .add_header("Authorization", format!("Bearer {}", auth_token))
        .await;

    assert!(empty_participants_response.status_code().is_success());
    let empty_participants: Vec<serde_json::Value> = empty_participants_response.json();
    assert_eq!(empty_participants.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_webrtc_authentication_required() -> Result<()> {
    let (app, _temp_dir, auth) = create_test_app().await?;
    let server = axum_test::TestServer::new(app)?;

    // Test core WebRTC room endpoints require authentication (not placeholder endpoints)
    let endpoints_to_test = vec![
        ("POST", "/v2/rtc/rooms"),
        ("GET", "/v2/rtc/rooms"),
        ("GET", "/v2/rtc/rooms/test-room"),
        ("DELETE", "/v2/rtc/rooms/test-room"),
        ("POST", "/v2/rtc/rooms/test-room/join"),
        ("POST", "/v2/rtc/rooms/test-room/leave"),
        ("GET", "/v2/rtc/rooms/test-room/participants"),
    ];

    for (method, endpoint) in endpoints_to_test {
        let response = match method {
            "GET" => server.get(endpoint).await,
            "POST" => server.post(endpoint).json(&serde_json::json!({})).await,
            "DELETE" => server.delete(endpoint).await,
            _ => continue,
        };

        assert_eq!(
            response.status_code(),
            401,
            "Endpoint {} {} should require authentication",
            method,
            endpoint
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_webrtc_placeholder_endpoints() -> Result<()> {
    let (app, _temp_dir, auth) = create_test_app().await?;
    let server = axum_test::TestServer::new(app)?;
    let auth_token = zauth::issue_dev_token(&auth)?;

    // Test placeholder endpoints return "Not implemented yet" responses
    let placeholder_endpoints = vec![
        ("GET", "/v2/rtc/sessions"),
        ("GET", "/v2/rtc/sessions/test-session"),
        ("PUT", "/v2/rtc/sessions/test-session"),
        ("POST", "/v2/rtc/subscriptions"),
        ("GET", "/v2/rtc/subscriptions"),
        ("GET", "/v2/rtc/subscriptions/test-subscription"),
        ("PUT", "/v2/rtc/subscriptions/test-subscription"),
        ("DELETE", "/v2/rtc/subscriptions/test-subscription"),
        ("POST", "/v2/rtc/triggers"),
        ("GET", "/v2/rtc/triggers"),
        ("GET", "/v2/rtc/triggers/test-trigger"),
        ("PUT", "/v2/rtc/triggers/test-trigger"),
        ("DELETE", "/v2/rtc/triggers/test-trigger"),
        ("POST", "/v2/rtc/triggers/test-trigger/execute"),
        ("GET", "/v2/rtc/triggers/test-trigger/history"),
    ];

    for (method, endpoint) in placeholder_endpoints {
        let response = match method {
            "GET" => server.get(endpoint)
                .add_header("Authorization", format!("Bearer {}", auth_token))
                .await,
            "POST" => server.post(endpoint)
                .add_header("Authorization", format!("Bearer {}", auth_token))
                .json(&serde_json::json!({}))
                .await,
            "PUT" => server.put(endpoint)
                .add_header("Authorization", format!("Bearer {}", auth_token))
                .json(&serde_json::json!({}))
                .await,
            "DELETE" => server.delete(endpoint)
                .add_header("Authorization", format!("Bearer {}", auth_token))
                .await,
            _ => continue,
        };

        // Placeholder endpoints should return success with "Not implemented yet" message
        assert!(response.status_code().is_success(), "Endpoint {} {} failed", method, endpoint);

        let body: serde_json::Value = response.json();
        assert_eq!(body["message"], "Not implemented yet");
    }

    Ok(())
}

#[tokio::test]
async fn test_webrtc_legacy_rtc_offer_endpoint() -> Result<()> {
    let (app, _temp_dir, auth) = create_test_app().await?;
    let server = axum_test::TestServer::new(app)?;
    let auth_token = zauth::issue_dev_token(&auth)?;

    let offer_payload = serde_json::json!({
        "offer": {
            "type": "offer",
            "sdp": "v=0\r\no=- 123456789 0 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n"
        },
        "ice_servers": [{
            "urls": ["stun:stun.example.com:3478"],
            "username": "testuser",
            "credential": "testpass"
        }]
    });

    let response = server
        .post("/v2/rtc/offer")
        .add_header("Authorization", format!("Bearer {}", auth_token))
        .json(&offer_payload)
        .await;

    assert!(response.status_code().is_success());

    let rtc_response: serde_json::Value = response.json();
    assert!(rtc_response["answer"]["sdp"].is_string());
    assert!(rtc_response["ice_servers"].is_array());

    Ok(())
}

#[tokio::test]
async fn test_webrtc_room_limits() -> Result<()> {
    let (app, _temp_dir, auth) = create_test_app().await?;
    let server = axum_test::TestServer::new(app)?;
    let auth_token = zauth::issue_dev_token(&auth)?;

    // Create room with specific participant limit
    let create_room_payload = serde_json::json!({
        "room_id": "limit-test-room",
        "metadata": {
            "max_participants": 1,
            "name": "Limited Room",
            "recording_enabled": false,
            "allow_screen_share": true,
            "allow_chat": true,
            "moderator_only_functions": [],
            "tags": []
        }
    });

    let create_response = server
        .post("/v2/rtc/rooms")
        .add_header("Authorization", format!("Bearer {}", auth_token))
        .json(&create_room_payload)
        .await;

    assert!(create_response.status_code().is_success());

    // Join the room (should succeed)
    let join_payload = serde_json::json!({
        "display_name": "First User",
        "role": "Participant"
    });

    let first_join_response = server
        .post("/v2/rtc/rooms/limit-test-room/join")
        .add_header("Authorization", format!("Bearer {}", auth_token))
        .json(&join_payload)
        .await;

    assert!(first_join_response.status_code().is_success());

    // Verify room now has 1 participant
    let room_info_response = server
        .get("/v2/rtc/rooms/limit-test-room")
        .add_header("Authorization", format!("Bearer {}", auth_token))
        .await;

    assert!(room_info_response.status_code().is_success());
    let room_info: serde_json::Value = room_info_response.json();
    assert_eq!(room_info["participant_count"], 1);

    Ok(())
}