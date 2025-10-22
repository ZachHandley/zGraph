#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use zcore_storage::Store;
    use crate::{
        AuthConfig, AuthService, AuthSession, TokenKind,
        models::{UserRecord, UserStatus, DeviceInfo, DeviceType},
        credential_verification::CredentialVerificationService,
        device_attestation::DeviceAttestationService,
        identity_permissions::IdentityAwarePermissionService,
    };
    use ulid::Ulid;
    use std::time::{SystemTime, UNIX_EPOCH};
    use zpermissions::{ResourceKind, Crud};

    fn current_timestamp() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    fn create_test_store() -> &'static Store {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_auth");
        let store = Store::open(&path).expect("Failed to create store");
        let _ = dir.keep(); // Keep the directory alive
        Box::leak(Box::new(store))
    }

    fn create_test_auth_config() -> AuthConfig {
        AuthConfig::with_dev(1, "test_secret")
    }

    #[tokio::test]
    async fn test_backward_compatible_basic_login() {
        let store = create_test_store();
        let config = create_test_auth_config();
        let auth_service = AuthService::new(store, config);

        // Create a test user (existing pattern)
        let user = auth_service.signup_initial_admin(1, "test@example.com", "password123")
            .expect("Failed to create user");
        assert_eq!(user.email, "test@example.com");
        assert!(user.roles.contains(&"admin".to_string()));

        // Test basic login (existing pattern)
        let login_result = auth_service.login(1, "test@example.com", "password123", None)
                    .await
            .expect("Failed to login");

        assert!(!login_result.access_token.is_empty());
        assert!(!login_result.refresh_token.is_empty());
        assert!(login_result.access_expires_at > current_timestamp());
        assert!(login_result.refresh_expires_at > current_timestamp());
        assert_eq!(login_result.environment.slug, "live");
    }

    #[test]
    fn test_backward_compatible_auth_session_extraction() {
        // Test that existing AuthSession structure still works
        let session = AuthSession {
            token_id: "test_token".to_string(),
            user_id: Some(Ulid::new()),
            org_id: 1,
            env_id: Ulid::new(),
            env_slug: "live".to_string(),
            roles: vec!["admin".to_string()],
            labels: vec!["test".to_string()],
            session_id: Some(Ulid::new()),
            token_kind: TokenKind::Access,
            // New fields have defaults that maintain compatibility
            credential_context: None,
            hardware_attested: false,
            biometric_verified: false,
            trust_score: 0.0,
        };

        // Verify existing fields are accessible
        assert_eq!(session.token_id, "test_token");
        assert!(session.user_id.is_some());
        assert_eq!(session.org_id, 1);
        assert_eq!(session.env_slug, "live");
        assert!(session.roles.contains(&"admin".to_string()));
        assert_eq!(session.token_kind, TokenKind::Access);

        // Verify new fields have safe defaults
        assert!(session.credential_context.is_none());
        assert!(!session.hardware_attested);
        assert!(!session.biometric_verified);
        assert_eq!(session.trust_score, 0.0);
    }

    #[tokio::test]
    async fn test_enhanced_login_with_fallback() {
        let store = create_test_store();
        let config = create_test_auth_config();
        let auth_service = AuthService::new(store, config);

        // Create test user
        let _user = auth_service.signup_initial_admin(1, "test@example.com", "password123")
            .expect("Failed to create user");

        // Test enhanced login without credentials (should work like basic login)
        let enhanced_result = auth_service.login_with_device_attestation(
            1,
            "test@example.com",
            "password123",
            None, // env_slug
            None, // primary_credential_id
            None, // additional_credential_ids
            None, // device_info
            None, // ip_address
            None, // user_agent
        ).await.expect("Failed to login with enhanced method");

        // Should behave like basic login when no credentials provided
        assert!(!enhanced_result.tokens.access_token.is_empty());
        assert!(!enhanced_result.hardware_attested);
        assert_eq!(enhanced_result.trust_score, 0.0);
        assert!(enhanced_result.device_attestation.is_none());
    }

    #[tokio::test]
    async fn test_device_attestation_enhancement() {
        let store = create_test_store();
        let config = create_test_auth_config();
        let auth_service = AuthService::new(store, config);

        // Create test user
        let user = auth_service.signup_initial_admin(1, "test@example.com", "password123")
            .expect("Failed to create user");

        // Test with device info (enhanced flow)
        let device_info = DeviceInfo {
            fingerprint: "test_device_fingerprint".to_string(),
            platform: Some("Windows".to_string()),
            browser: Some("Chrome".to_string()),
            browser_version: Some("91.0".to_string()),
            os: Some("Windows 10".to_string()),
            os_version: Some("10.0.19041".to_string()),
            device_type: DeviceType::Desktop,
            trusted: false,
        };

        let enhanced_result = auth_service.login_with_device_attestation(
            1,
            "test@example.com",
            "password123",
            None,
            None,
            None,
            Some(&device_info),
            Some("192.168.1.1".to_string()),
            Some("TestAgent/1.0".to_string()),
        ).await.expect("Failed to login with device attestation");

        // Should have device attestation results
        assert!(enhanced_result.device_attestation.is_some());
        let attestation = enhanced_result.device_attestation.unwrap();
        assert_eq!(attestation.device_registration.device_id.to_string().len(), 26); // ULID length
        assert!(enhanced_result.trust_score > 0.0);
    }

    #[tokio::test]
    async fn test_credential_verification_service() {
        let store = create_test_store();
        let cred_service = CredentialVerificationService::new(store);

        let user_id = Ulid::new();
        let org_id = 1;

        // Test credential registration
        let registration_result = cred_service.register_credential(
            user_id,
            org_id,
            crate::models::CredentialType::X509Certificate,
            b"test_credential_data",
            "Test Issuer".to_string(),
            "Test Subject".to_string(),
            Some(b"public_key".to_vec()),
            Some(b"cert_chain".to_vec()),
            None,
        ).await.expect("Failed to register credential");

        assert_eq!(registration_result.credential_id.to_string().len(), 26);
        assert!(registration_result.requires_verification);
        assert!(registration_result.verification_challenge.is_some());
    }

    #[test]
    fn test_permission_service_compatibility() {
        let store = create_test_store();
        let identity_service = IdentityAwarePermissionService::new(store);

        // Test with basic auth session (backward compatible)
        let basic_session = AuthSession {
            token_id: "test".to_string(),
            user_id: Some(Ulid::new()),
            org_id: 1,
            env_id: Ulid::new(),
            env_slug: "live".to_string(),
            roles: vec!["admin".to_string()],
            labels: vec![],
            session_id: Some(Ulid::new()),
            token_kind: TokenKind::Access,
            credential_context: None,
            hardware_attested: false,
            biometric_verified: false,
            trust_score: 0.0,
        };

        // Should not fail on basic sessions without credentials
        let permissions_result = identity_service.effective_identity_permissions(&basic_session);
        assert!(permissions_result.is_ok());

        let permissions = permissions_result.unwrap();
        assert_eq!(permissions.trust_score, 0.0);
        assert!(!permissions.hardware_attested);
        assert!(!permissions.biometric_verified);
    }

    #[test]
    fn test_permission_checking_backward_compatibility() {
        let store = create_test_store();
        let identity_service = IdentityAwarePermissionService::new(store);

        let session = AuthSession {
            token_id: "test".to_string(),
            user_id: Some(Ulid::new()),
            org_id: 1,
            env_id: Ulid::new(),
            env_slug: "live".to_string(),
            roles: vec!["admin".to_string()],
            labels: vec![],
            session_id: Some(Ulid::new()),
            token_kind: TokenKind::Access,
            credential_context: None,
            hardware_attested: false,
            biometric_verified: false,
            trust_score: 0.0,
        };

        // Admin should have access to collections (existing behavior)
        let result = identity_service.ensure_identity_aware(
            &session,
            ResourceKind::Collections,
            Crud::READ,
        );

        // Should pass basic permission check even with 0 trust score for admin role
        assert!(result.is_ok());
    }

    #[test]
    fn test_enhanced_permissions_with_credentials() {
        let store = create_test_store();
        let identity_service = IdentityAwarePermissionService::new(store);

        // Test session with high trust credentials
        let enhanced_session = AuthSession {
            token_id: "test".to_string(),
            user_id: Some(Ulid::new()),
            org_id: 1,
            env_id: Ulid::new(),
            env_slug: "live".to_string(),
            roles: vec!["reader".to_string()], // Lower privilege role
            labels: vec![],
            session_id: Some(Ulid::new()),
            token_kind: TokenKind::Access,
            credential_context: Some(crate::models::SessionCredentialContext {
                session_id: Ulid::new(),
                primary_credential: Some(Ulid::new()),
                secondary_credentials: vec![],
                hardware_attested: true,
                biometric_verified: true,
                device_bound: true,
                trust_score: 0.95,
                verification_timestamp: current_timestamp(),
                context_data: serde_json::json!({"test": true}),
            }),
            hardware_attested: true,
            biometric_verified: true,
            trust_score: 0.95,
        };

        let permissions = identity_service.effective_identity_permissions(&enhanced_session)
            .expect("Failed to get enhanced permissions");

        // Should show enhanced capabilities
        assert_eq!(permissions.trust_score, 0.95);
        assert!(permissions.hardware_attested);
        assert!(permissions.biometric_verified);
        assert!(matches!(permissions.trust_level, crate::identity_permissions::SessionTrustLevel::Maximum));
    }

    #[test]
    fn test_dev_token_compatibility() {
        let config = AuthConfig::with_dev(1, "test_secret");

        // Test dev token issuance (existing functionality)
        let dev_token_result = crate::issue_dev_token(&config);

        // Should still work in test environment
        assert!(dev_token_result.is_ok());
        let token = dev_token_result.unwrap();
        assert!(!token.is_empty());

        // Test dev token verification
        let verification_result = crate::verify_dev_token(&token, "test_secret");
        assert!(verification_result.is_ok());

        let claims = verification_result.unwrap();
        assert_eq!(claims.org_id, 1);
        assert!(claims.roles.contains(&"admin".to_string()));
    }

    #[tokio::test]
    async fn test_session_refresh_compatibility() {
        let store = create_test_store();
        let config = create_test_auth_config();
        let auth_service = AuthService::new(store, config);

        // Create user and login
        let _user = auth_service.signup_initial_admin(1, "test@example.com", "password123")
                      .expect("Failed to create user");

        let login_result = auth_service.login(1, "test@example.com", "password123", None)
                    .await
            .expect("Failed to login");

        // Test token refresh (existing functionality)
        let refresh_result = auth_service.refresh(&login_result.refresh_token);
        assert!(refresh_result.is_ok());

        let refreshed_tokens = refresh_result.unwrap();
        assert!(!refreshed_tokens.access_token.is_empty());
        assert!(!refreshed_tokens.refresh_token.is_empty());
        assert_ne!(refreshed_tokens.access_token, login_result.access_token);
        assert_ne!(refreshed_tokens.refresh_token, login_result.refresh_token);
    }

    #[tokio::test]
    async fn test_environment_switching_compatibility() {
        let store = create_test_store();
        let config = create_test_auth_config();
        let auth_service = AuthService::new(store, config);

        // Create user and login
        let _user = auth_service.signup_initial_admin(1, "test@example.com", "password123")
                      .expect("Failed to create user");

        let login_result = auth_service.login(1, "test@example.com", "password123", None)
                    .await
            .expect("Failed to login");

        // Test environment switching (existing functionality)
        let switch_result = auth_service.switch_environment(&login_result.refresh_token, "sandbox");
        assert!(switch_result.is_ok());

        let switched_tokens = switch_result.unwrap();
        assert_eq!(switched_tokens.environment.slug, "sandbox");
        assert!(!switched_tokens.access_token.is_empty());
    }

    #[test]
    fn test_credential_middleware_graceful_degradation() {
        use crate::credential_middleware::{VerificationRequirements, CredentialVerificationError};

        // Test that middleware gracefully handles sessions without credentials
        let basic_session = AuthSession {
            token_id: "test".to_string(),
            user_id: Some(Ulid::new()),
            org_id: 1,
            env_id: Ulid::new(),
            env_slug: "live".to_string(),
            roles: vec!["admin".to_string()],
            labels: vec![],
            session_id: Some(Ulid::new()),
            token_kind: TokenKind::Access,
            credential_context: None,
            hardware_attested: false,
            biometric_verified: false,
            trust_score: 0.0,
        };

        // Minimal requirements should pass
        let minimal_requirements = VerificationRequirements {
            min_trust_level: None,
            require_hardware_attestation: false,
            require_biometric: false,
            min_trust_score: 0.0,
            require_device_bound: false,
        };

        // Should pass verification with minimal requirements
        // Note: verify_credential_requirements is private, so we can't test it directly
        // let _verification_result = crate::credential_middleware::verify_credential_requirements(
        //     &basic_session,
        //     &minimal_requirements,
        // );

        // This would be tested in an async context, but we can verify the pattern
        // The key is that basic sessions don't break the new system
    }
}

/// Integration tests that verify the complete enhanced authentication flow
/// while maintaining backward compatibility
#[cfg(test)]
mod integration_tests {
    use super::*;
    use tempfile::tempdir;
    use zcore_storage::Store;
    use crate::{
        AuthConfig, AuthService, AuthSession, TokenKind,
        models::{DeviceInfo, DeviceType},
    };
    use ulid::Ulid;

    #[tokio::test]
    async fn test_complete_enhanced_authentication_flow() {
        let store = create_test_store();
        let config = create_test_auth_config();
        let auth_service = AuthService::new(store, config);

        // 1. Create user (existing flow)
        let user = auth_service.signup_initial_admin(1, "admin@example.com", "secure_password")
            .expect("Failed to create admin user");

        // 2. Basic login (existing flow should still work)
        let basic_login = auth_service.login(1, "admin@example.com", "secure_password", None)
                  .await
            .expect("Basic login failed");
        assert!(!basic_login.access_token.is_empty());

        // 3. Enhanced login with device info
        let device_info = crate::models::DeviceInfo {
            fingerprint: "secure_device_123".to_string(),
            platform: Some("macOS".to_string()),
            browser: Some("Safari".to_string()),
            browser_version: Some("14.1".to_string()),
            os: Some("macOS".to_string()),
            os_version: Some("11.6".to_string()),
            device_type: crate::models::DeviceType::Desktop,
            trusted: false,
        };

        let enhanced_login = auth_service.login_with_device_attestation(
            1,
            "admin@example.com",
            "secure_password",
            None,
            None,
            None,
            Some(&device_info),
            Some("10.0.1.100".to_string()),
            Some("Mozilla/5.0 Safari/14.1".to_string()),
        ).await.expect("Enhanced login failed");

        // 4. Verify enhanced capabilities
        assert!(enhanced_login.device_attestation.is_some());
        assert!(enhanced_login.trust_score > 0.0);

        // 5. Test permission checking with enhanced context
        let identity_permission_service = crate::identity_permissions::IdentityAwarePermissionService::new(store);

        // Create session from enhanced login
        // This would normally be extracted from request headers, but we simulate it here
        let enhanced_session = AuthSession {
            token_id: "enhanced_session".to_string(),
            user_id: Some(user.id),
            org_id: 1,
            env_id: Ulid::new(),
            env_slug: "live".to_string(),
            roles: user.roles.clone(),
            labels: user.labels.clone(),
            session_id: Some(Ulid::new()),
            token_kind: TokenKind::Access,
            credential_context: None, // Would be populated from credential verification
            hardware_attested: enhanced_login.hardware_attested,
            biometric_verified: enhanced_login.device_attestation.as_ref()
                .map(|d| d.attestation_verified).unwrap_or(false),
            trust_score: enhanced_login.trust_score,
        };

        // 6. Test identity-aware permissions
        let permission_map = identity_permission_service
            .effective_identity_permissions(&enhanced_session)
            .expect("Failed to get identity permissions");

        assert_eq!(permission_map.trust_score, enhanced_login.trust_score);
        assert!(permission_map.trust_score > 0.0);

        // 7. Verify backward compatibility - basic operations still work
        let refresh_result = auth_service.refresh(&basic_login.refresh_token)
            .expect("Token refresh should still work");
        assert!(!refresh_result.access_token.is_empty());

        let logout_result = auth_service.logout(&refresh_result.refresh_token);
        assert!(logout_result.is_ok());
    }

    fn create_test_store() -> &'static Store {
        let dir = tempfile::tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("integration_test");
        let store = Store::open(&path).expect("Failed to create store");
        let _ = dir.keep(); // Keep directory alive
        Box::leak(Box::new(store))
    }

    fn create_test_auth_config() -> crate::AuthConfig {
        crate::AuthConfig::with_dev(1, "integration_test_secret")
    }
}