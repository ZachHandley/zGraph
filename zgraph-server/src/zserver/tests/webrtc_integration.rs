use std::sync::Arc;
use zserver::webrtc::{WebRTCConfig, API, WebRTCSession, ParticipantRole, ParticipantPermissions};

#[tokio::test]
async fn test_webrtc_api_creation() {
    // Test creating WebRTC API with default ICE servers
    let ice_servers = API::default_ice_servers();
    assert!(!ice_servers.is_empty(), "Should have default ICE servers");

    let result = API::new(ice_servers).await;
    assert!(result.is_ok(), "WebRTC API creation should succeed");

    let api = result.unwrap();
    assert!(!api.ice_servers.is_empty(), "API should have ICE servers configured");
}

#[tokio::test]
async fn test_peer_connection_creation() {
    // Create WebRTC API
    let ice_servers = API::default_ice_servers();
    let api = API::new(ice_servers).await.expect("Failed to create WebRTC API");

    // Test peer connection creation
    let result = api.create_peer_connection().await;
    assert!(result.is_ok(), "Peer connection creation should succeed");

    let peer_connection = result.unwrap();

    // Verify initial state
    let initial_state = peer_connection.connection_state();
    println!("Initial peer connection state: {:?}", initial_state);
}

#[tokio::test]
async fn test_webrtc_session_creation() {
    // Create WebRTC API
    let ice_servers = API::default_ice_servers();
    let api = API::new(ice_servers).await.expect("Failed to create WebRTC API");

    // Test session creation
    let user_id = "test_user_123".to_string();
    let org_id = 1;
    let project_id = Some("test_project".to_string());
    let role = ParticipantRole::Participant;
    let permissions = ParticipantPermissions::default();

    let result = WebRTCSession::create_with_api(
        &api,
        user_id.clone(),
        org_id,
        project_id.clone(),
        role.clone(),
        permissions.clone(),
    ).await;

    assert!(result.is_ok(), "WebRTC session creation should succeed");

    let session = result.unwrap();
    assert_eq!(session.user_id, user_id);
    assert_eq!(session.org_id, org_id);
    assert_eq!(session.project_id, project_id);
    assert!(matches!(session.role, ParticipantRole::Participant));
}

#[tokio::test]
async fn test_data_channel_operations() {
    // Create WebRTC API and session
    let ice_servers = API::default_ice_servers();
    let api = API::new(ice_servers).await.expect("Failed to create WebRTC API");

    let session = WebRTCSession::create_with_api(
        &api,
        "test_user".to_string(),
        1,
        None,
        ParticipantRole::Participant,
        ParticipantPermissions::default(),
    ).await.expect("Failed to create session");

    // Test default data channel creation
    let result = session.create_default_data_channel().await;
    assert!(result.is_ok(), "Default data channel creation should succeed");

    // Test checking if data channel is ready
    let is_ready = session.is_data_channel_ready("default").await;
    // Note: This might be false initially since the peer connection isn't established
    println!("Default data channel ready: {}", is_ready);

    // Test getting data channel states
    let states = session.get_data_channel_states().await;
    assert!(states.contains_key("default"), "Should have default data channel");
}

#[tokio::test]
async fn test_session_metadata_operations() {
    // Create WebRTC session
    let ice_servers = API::default_ice_servers();
    let api = API::new(ice_servers).await.expect("Failed to create WebRTC API");

    let session = WebRTCSession::create_with_api(
        &api,
        "test_user".to_string(),
        1,
        None,
        ParticipantRole::Moderator,
        ParticipantPermissions::default_for_role(&ParticipantRole::Moderator),
    ).await.expect("Failed to create session");

    // Test feature flag operations
    session.set_feature_flag("test_feature", true).await;
    let flag_value = session.get_feature_flag("test_feature").await;
    assert!(flag_value, "Feature flag should be true");

    let non_existent_flag = session.get_feature_flag("non_existent").await;
    assert!(!non_existent_flag, "Non-existent flag should be false");

    // Test activity tracking
    let initial_activity = *session.last_activity.read().await;

    // Update activity
    session.update_activity().await;

    let updated_activity = *session.last_activity.read().await;
    assert!(updated_activity >= initial_activity, "Activity should be updated");

    // Test session info retrieval
    let session_info = session.get_session_info().await;
    assert_eq!(session_info.user_id, "test_user");
    assert_eq!(session_info.org_id, 1);
    assert!(matches!(session_info.role, ParticipantRole::Moderator));
}

#[tokio::test]
async fn test_permission_checking() {
    // Create session with different roles
    let ice_servers = API::default_ice_servers();
    let api = API::new(ice_servers).await.expect("Failed to create WebRTC API");

    // Test moderator permissions
    let moderator_session = WebRTCSession::create_with_api(
        &api,
        "moderator".to_string(),
        1,
        None,
        ParticipantRole::Moderator,
        ParticipantPermissions::default_for_role(&ParticipantRole::Moderator),
    ).await.expect("Failed to create moderator session");

    assert!(moderator_session.has_permission("speak").await);
    assert!(moderator_session.has_permission("share_screen").await);
    assert!(moderator_session.has_permission("record").await);
    assert!(moderator_session.has_permission("kick_participants").await);

    // Test observer permissions
    let observer_session = WebRTCSession::create_with_api(
        &api,
        "observer".to_string(),
        1,
        None,
        ParticipantRole::Observer,
        ParticipantPermissions::default_for_role(&ParticipantRole::Observer),
    ).await.expect("Failed to create observer session");

    assert!(!observer_session.has_permission("speak").await);
    assert!(!observer_session.has_permission("share_screen").await);
    assert!(!observer_session.has_permission("record").await);
    assert!(!observer_session.has_permission("kick_participants").await);
}

#[tokio::test]
async fn test_webrtc_config() {
    let config = WebRTCConfig::default();

    // Test default configuration values
    assert!(config.enable_video, "Video should be enabled by default");
    assert!(config.enable_audio, "Audio should be enabled by default");
    assert_eq!(config.max_participants_per_room, 50);
    assert_eq!(config.max_rooms_per_org, 100);
    assert!(config.media_relay_enabled, "Media relay should be enabled");
    assert!(config.enable_transactions, "Transactions should be enabled");

    // Test custom configuration
    let custom_config = WebRTCConfig {
        enable_video: false,
        enable_audio: true,
        max_participants_per_room: 10,
        max_rooms_per_org: 5,
        media_relay_enabled: false,
        ..Default::default()
    };

    assert!(!custom_config.enable_video);
    assert!(custom_config.enable_audio);
    assert_eq!(custom_config.max_participants_per_room, 10);
    assert_eq!(custom_config.max_rooms_per_org, 5);
    assert!(!custom_config.media_relay_enabled);
}

#[tokio::test]
async fn test_ice_server_parsing() {
    // Test default ICE servers
    let default_servers = API::default_ice_servers();
    assert!(!default_servers.is_empty(), "Should have default ICE servers");

    // Check for Google STUN servers
    let has_google_stun = default_servers.iter().any(|server| {
        server.urls.iter().any(|url| url.contains("stun.l.google.com"))
    });
    assert!(has_google_stun, "Should include Google STUN servers");

    // Test environment variable parsing (will use defaults if not set)
    let env_servers = API::parse_ice_servers_from_env();
    assert!(!env_servers.is_empty(), "Should have ICE servers from env or defaults");
}

#[tokio::test]
async fn test_session_lifecycle() {
    // Create WebRTC session
    let ice_servers = API::default_ice_servers();
    let api = API::new(ice_servers).await.expect("Failed to create WebRTC API");

    let session = WebRTCSession::create_with_api(
        &api,
        "lifecycle_test".to_string(),
        1,
        None,
        ParticipantRole::Participant,
        ParticipantPermissions::default(),
    ).await.expect("Failed to create session");

    // Test initial state
    assert!(!session.is_connected(), "Session should not be connected initially");

    let initial_state = session.get_connection_state();
    println!("Initial connection state: {:?}", initial_state);

    let ice_state = session.get_ice_connection_state();
    println!("Initial ICE state: {:?}", ice_state);

    // Test activity check
    let is_active = session.is_active(30).await; // 30 minute timeout
    assert!(is_active, "Newly created session should be active");

    // Test session close
    let close_result = session.close().await;
    assert!(close_result.is_ok(), "Session close should succeed");
}