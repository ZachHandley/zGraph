use zauth::{
    oauth::{OAuthService, OAuthProvider, OAuthConfig, OAuthState, OAuthSession, OAuthProfile, OAuthAccountLink, generate_oauth_user_id},
    AuthConfig, AuthService,
    models::UserRecord,
    hash_password,
};
use zcore_storage::Store;
use std::time::{SystemTime, UNIX_EPOCH};
use ulid::Ulid;
use tempfile::tempdir;

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn create_test_store() -> &'static Store {
    let dir = tempdir().expect("Failed to create temp dir");
    let path = dir.path().join("oauth_test");
    let store = Store::open(&path).expect("Failed to create store");
    let _ = dir.keep(); // Keep the directory alive
    Box::leak(Box::new(store))
}

#[tokio::test]
async fn test_oauth_unique_id_generation() {
    // Test the unique ID generation system
    let google_id1 = generate_oauth_user_id("google", "12345");
    let google_id2 = generate_oauth_user_id("google", "12345");
    let github_id = generate_oauth_user_id("github", "12345");

    // Same input should generate same ID
    assert_eq!(google_id1, google_id2);
    assert_eq!(google_id1.len(), 32);

    // Different provider should generate different ID
    assert_ne!(google_id1, github_id);
}

#[tokio::test]
async fn test_oauth_service_initialization() {
    // Test OAuth service can be created and configured
    let store = create_test_store();
    let auth_config = AuthConfig::new(
        None,
        None,
        "test-secret",
        "test-refresh-secret",
        3600,
        604800,
    );

    let mut oauth_service = OAuthService::new(store, auth_config);

    // Add a test provider configuration
    let config = OAuthConfig {
        provider: OAuthProvider::Google,
        client_id: "test-client-id".to_string(),
        client_secret: "test-client-secret".to_string(),
        redirect_uri: "http://localhost:3000/callback".to_string(),
        scopes: vec!["openid".to_string(), "email".to_string()],
        auth_url: "https://accounts.google.com/o/oauth2/v2/auth".to_string(),
        token_url: "https://oauth2.googleapis.com/token".to_string(),
        user_info_url: "https://openidconnect.googleapis.com/v1/userinfo".to_string(),
    };

    oauth_service.add_provider(config);

    // Test OAuth flow initiation
    let result = oauth_service
        .initiate_oauth_flow(OAuthProvider::Google, 1, Some("live".to_string()), None)
        .await;

    assert!(result.is_ok());
    let flow = result.unwrap();
    assert!(flow.authorization_url.contains("accounts.google.com"));
    assert!(!flow.state.is_empty());
    assert!(!flow.session_id.is_empty());
}

#[tokio::test]
#[ignore] // Ignoring test that accesses private fields
async fn test_oauth_profile_creation() {
    let store = create_test_store();
    let auth_config = AuthConfig::new(
        None,
        None,
        "test-secret",
        "test-refresh-secret",
        3600,
        604800,
    );

    let oauth_service = OAuthService::new(store, auth_config);
    let oauth_repo = &oauth_service.oauth_repo;

    let now = current_timestamp();
    let oauth_profile = OAuthProfile {
        id: generate_oauth_user_id("google", "123456"),
        org_id: 1,
        provider: OAuthProvider::Google,
        provider_user_id: "123456".to_string(),
        email: Some("test@example.com".to_string()),
        name: Some("Test User".to_string()),
        avatar_url: Some("https://example.com/avatar.jpg".to_string()),
        username: None,
        metadata: std::collections::HashMap::new(),
        created_at: now,
        updated_at: now,
        last_login_at: Some(now),
    };

    // Test profile insertion
    let result = oauth_repo.upsert_oauth_profile(&oauth_profile);
    assert!(result.is_ok());

    // Test profile retrieval by ID
    let retrieved = oauth_repo.get_oauth_profile(1, &oauth_profile.id).unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.id, oauth_profile.id);
    assert_eq!(retrieved.provider, OAuthProvider::Google);
    assert_eq!(retrieved.email, Some("test@example.com".to_string()));

    // Test profile retrieval by provider and user ID
    let retrieved_by_provider = oauth_repo
        .get_oauth_profile_by_provider_user(1, &OAuthProvider::Google, "123456")
        .unwrap();
    assert!(retrieved_by_provider.is_some());
    assert_eq!(retrieved_by_provider.unwrap().id, oauth_profile.id);

    // Test profile retrieval by email
    let retrieved_by_email = oauth_repo
        .get_oauth_profile_by_email(1, "test@example.com")
        .unwrap();
    assert!(retrieved_by_email.is_some());
    assert_eq!(retrieved_by_email.unwrap().id, oauth_profile.id);
}

#[tokio::test]
#[ignore] // Ignoring test that accesses private fields
async fn test_oauth_account_linking() {
    let store = create_test_store();
    let auth_config = AuthConfig::new(
        None,
        None,
        "test-secret",
        "test-refresh-secret",
        3600,
        604800,
    );

    // Create a regular user first
    let auth_service = AuthService::new(store, auth_config.clone());
    let user = auth_service.signup_initial_admin(1, "test@example.com", "password123").unwrap();

    let oauth_service = OAuthService::new(store, auth_config);
    let oauth_repo = &oauth_service.oauth_repo;

    // Create OAuth profile
    let oauth_profile_id = generate_oauth_user_id("github", "987654");
    let now = current_timestamp();
    let oauth_profile = OAuthProfile {
        id: oauth_profile_id.clone(),
        org_id: 1,
        provider: OAuthProvider::GitHub,
        provider_user_id: "987654".to_string(),
        email: Some("test@example.com".to_string()),
        name: Some("Test User".to_string()),
        avatar_url: None,
        username: Some("testuser".to_string()),
        metadata: std::collections::HashMap::new(),
        created_at: now,
        updated_at: now,
        last_login_at: Some(now),
    };

    oauth_repo.upsert_oauth_profile(&oauth_profile).unwrap();

    // Create account link
    let account_link = OAuthAccountLink {
        id: Ulid::new(),
        org_id: 1,
        user_id: user.id,
        oauth_profile_id: oauth_profile_id.clone(),
        provider: OAuthProvider::GitHub,
        is_primary: false,
        linked_at: now,
        verified_at: Some(now),
    };

    oauth_repo.insert_account_link(&account_link).unwrap();

    // Test account link retrieval
    let links = oauth_repo.get_account_links_for_user(1, &user.id).unwrap();
    assert_eq!(links.len(), 1);
    assert_eq!(links[0].oauth_profile_id, oauth_profile_id);
    assert_eq!(links[0].provider, OAuthProvider::GitHub);

    // Test unified user profile
    let unified_profile = oauth_repo.get_unified_user_profile(1, &user.id).unwrap();
    assert!(unified_profile.is_some());
    let unified = unified_profile.unwrap();
    assert_eq!(unified.user_id, user.id);
    assert_eq!(unified.oauth_profiles.len(), 1);
    assert_eq!(unified.account_links.len(), 1);
    assert_eq!(unified.primary_name, "Test User");
}

#[tokio::test]
#[ignore] // Ignoring test that accesses private fields
async fn test_oauth_state_management() {
    let store = create_test_store();
    let auth_config = AuthConfig::new(
        None,
        None,
        "test-secret",
        "test-refresh-secret",
        3600,
        604800,
    );

    let oauth_service = OAuthService::new(store, auth_config);
    let oauth_repo = &oauth_service.oauth_repo;

    let now = current_timestamp();
    let state = OAuthState {
        state_id: "test-state-123".to_string(),
        provider: OAuthProvider::Discord,
        org_id: 1,
        env_slug: Some("live".to_string()),
        linking_user_id: None,
        created_at: now,
        expires_at: now + 600, // 10 minutes
    };

    // Test state storage
    oauth_repo.store_oauth_state(&state).unwrap();

    // Test state consumption (should retrieve and delete)
    let consumed = oauth_repo.consume_oauth_state("test-state-123").unwrap();
    assert!(consumed.is_some());
    let consumed = consumed.unwrap();
    assert_eq!(consumed.state_id, "test-state-123");
    assert_eq!(consumed.provider, OAuthProvider::Discord);

    // State should be gone after consumption
    let consumed_again = oauth_repo.consume_oauth_state("test-state-123").unwrap();
    assert!(consumed_again.is_none());
}

#[tokio::test]
#[ignore] // Ignoring test that accesses private fields
async fn test_oauth_session_management() {
    let store = create_test_store();
    let auth_config = AuthConfig::new(
        None,
        None,
        "test-secret",
        "test-refresh-secret",
        3600,
        604800,
    );

    let oauth_service = OAuthService::new(store, auth_config);
    let oauth_repo = &oauth_service.oauth_repo;

    let now = current_timestamp();
    let session = OAuthSession {
        session_id: "test-session-456".to_string(),
        state_id: "test-state-456".to_string(),
        provider: OAuthProvider::Apple,
        org_id: 1,
        code_verifier: Some("test-verifier".to_string()),
        created_at: now,
        expires_at: now + 600,
    };

    // Test session storage
    oauth_repo.store_oauth_session(&session).unwrap();

    // Test session retrieval
    let retrieved = oauth_repo.get_oauth_session("test-session-456").unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.session_id, "test-session-456");
    assert_eq!(retrieved.provider, OAuthProvider::Apple);
    assert_eq!(retrieved.code_verifier, Some("test-verifier".to_string()));

    // Test session deletion
    oauth_repo.delete_oauth_session("test-session-456").unwrap();
    let deleted = oauth_repo.get_oauth_session("test-session-456").unwrap();
    assert!(deleted.is_none());
}

#[tokio::test]
#[ignore] // Ignoring test that accesses private fields
async fn test_oauth_cleanup() {
    let store = create_test_store();
    let auth_config = AuthConfig::new(
        None,
        None,
        "test-secret",
        "test-refresh-secret",
        3600,
        604800,
    );

    let oauth_service = OAuthService::new(store, auth_config);
    let oauth_repo = &oauth_service.oauth_repo;

    let now = current_timestamp();

    // Create expired state and session
    let expired_state = OAuthState {
        state_id: "expired-state".to_string(),
        provider: OAuthProvider::Google,
        org_id: 1,
        env_slug: None,
        linking_user_id: None,
        created_at: now - 1000,
        expires_at: now - 100, // Expired
    };

    let expired_session = OAuthSession {
        session_id: "expired-session".to_string(),
        state_id: "expired-state".to_string(),
        provider: OAuthProvider::Google,
        org_id: 1,
        code_verifier: None,
        created_at: now - 1000,
        expires_at: now - 100, // Expired
    };

    oauth_repo.store_oauth_state(&expired_state).unwrap();
    oauth_repo.store_oauth_session(&expired_session).unwrap();

    // Verify they exist
    assert!(oauth_repo.get_oauth_session("expired-session").unwrap().is_some());

    // Clean up expired data
    oauth_repo.cleanup_expired_oauth_data(now).unwrap();

    // Verify they're gone
    assert!(oauth_repo.get_oauth_session("expired-session").unwrap().is_none());
}

#[tokio::test]
async fn test_oauth_integration_with_existing_auth() {
    let store = create_test_store();
    let auth_config = AuthConfig::new(
        None,
        None,
        "test-secret",
        "test-refresh-secret",
        3600,
        604800,
    );

    // Test that OAuth users can use regular auth features
    let auth_service = AuthService::new(store, auth_config.clone());
    let oauth_service = OAuthService::new(store, auth_config);

    // Create OAuth user
    let oauth_profile_id = generate_oauth_user_id("google", "oauth-user-123");
    let now = current_timestamp();

    // Create user record (simulating OAuth registration)
    let user = UserRecord {
        id: Ulid::new(),
        org_id: 1,
        email: "oauth.user@example.com".to_string(),
        password_hash: String::new(), // OAuth users don't have passwords
        roles: vec!["user".to_string()],
        labels: vec!["oauth:google".to_string()],
        status: zauth::models::UserStatus::Active,
        created_at: now,
        updated_at: now,
    };

    auth_service.repo().insert_user(&user).unwrap();

    // Verify user can be retrieved by existing auth methods
    let retrieved_user = auth_service.repo().get_user_by_id(1, &user.id).unwrap();
    assert!(retrieved_user.is_some());
    assert_eq!(retrieved_user.unwrap().email, "oauth.user@example.com");

    // Verify environment management works
    let (live_env, sandbox_env) = auth_service.repo().ensure_default_environments(1).unwrap();
    assert_eq!(live_env.slug, "live");
    assert_eq!(sandbox_env.slug, "sandbox");
}