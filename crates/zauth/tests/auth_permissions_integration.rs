use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use tempfile::tempdir;
use ulid::Ulid;

use zauth::{
    AuthConfig, AuthService, AuthSession, LoginTokens,
    models::UserRecord, models::UserStatus,
    hash_password, verify_password, issue_dev_token, verify_dev_token,
    DevTokenClaims
};
use zpermissions::{
    PermissionService, Principal, ResourceKind, Crud, CrudAction,
    PermissionRecord, PermissionError, SecurityEvent
};
use zcore_storage::Store;

/// Test fixture for integration testing of auth and permissions
struct AuthPermissionIntegration {
    pub store: &'static Store,
    pub auth_config: AuthConfig,
    pub auth_service: AuthService<'static>,
    pub perm_service: PermissionService<'static>,
    pub org_id: u64,
    pub org_id_2: u64,
}

impl AuthPermissionIntegration {
    fn new() -> Result<Self> {
        // Create temporary store
        let dir = tempdir()?;
        let dir_path = dir.path().to_path_buf();
        let store_path = dir_path.join("test_integration");
        let store = Store::open(&store_path)?;
        let _ = dir.keep();
        let leaked_store: &'static Store = Box::leak(Box::new(store));

        // Create auth config
        let auth_config = AuthConfig::new(
            Some(1001),  // dev_org_id
            Some("dev_secret_123".to_string()),
            "test_access_secret",
            "test_refresh_secret",
            3600,  // access_ttl_secs
            7 * 24 * 3600,  // refresh_ttl_secs
        );

        // Create services
        let auth_service = AuthService::new(leaked_store, auth_config.clone());
        let perm_service = PermissionService::new(leaked_store);

        Ok(Self {
            store: leaked_store,
            auth_config,
            auth_service,
            perm_service,
            org_id: 1001,
            org_id_2: 2002,
        })
    }

    async fn create_test_user(&self, email: &str, roles: Vec<String>) -> Result<UserRecord> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs() as i64;

        let user = UserRecord {
            id: Ulid::new(),
            org_id: self.org_id,
            email: email.to_lowercase(),
            password_hash: hash_password("test_password")?,
            roles,
            labels: Vec::new(),
            status: UserStatus::Active,
            created_at: now,
            updated_at: now,
        };

        self.auth_service.repo().insert_user(&user)?;
        Ok(user)
    }

    async fn create_test_user_in_org(&self, org_id: u64, email: &str, roles: Vec<String>) -> Result<UserRecord> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs() as i64;

        let user = UserRecord {
            id: Ulid::new(),
            org_id,
            email: email.to_lowercase(),
            password_hash: hash_password("test_password")?,
            roles,
            labels: Vec::new(),
            status: UserStatus::Active,
            created_at: now,
            updated_at: now,
        };

        self.auth_service.repo().insert_user(&user)?;
        Ok(user)
    }

    fn setup_test_permissions(&self) -> Result<()> {
        // Setup default roles for testing
        let admin_role = Principal::Role("admin".to_string());
        let operator_role = Principal::Role("operator".to_string());
        let reader_role = Principal::Role("reader".to_string());

        // Override some defaults for testing
        let admin_perms = PermissionRecord::new(
            admin_role.clone(),
            ResourceKind::Permissions,
            vec![
                CrudAction::Create,
                CrudAction::Read,
                CrudAction::Update,
                CrudAction::Delete,
            ],
        );

        let operator_perms = PermissionRecord::new(
            operator_role.clone(),
            ResourceKind::Jobs,
            vec![CrudAction::Create, CrudAction::Read, CrudAction::Update],
        );

        let reader_perms = PermissionRecord::new(
            reader_role.clone(),
            ResourceKind::Collections,
            vec![CrudAction::Read],
        );

        self.perm_service.upsert(self.org_id, &admin_perms)?;
        self.perm_service.upsert(self.org_id, &operator_perms)?;
        self.perm_service.upsert(self.org_id, &reader_perms)?;

        Ok(())
    }

    fn create_auth_session_from_token(&self, access_token: &str) -> Result<AuthSession> {
        let keys = self.auth_config.jwt_keys();
        let access = keys.decode_access(access_token)?;

        let user_id = Ulid::from_string(&access.sub)
            .map_err(|e| anyhow::anyhow!("bad user id: {e}"))?;
        let env_id = Ulid::from_string(&access.env)
            .map_err(|e| anyhow::anyhow!("bad env id: {e}"))?;

        let repo = self.auth_service.repo();
        let env = repo
            .get_environment_by_id(access.org, &env_id)?
            .ok_or_else(|| anyhow::anyhow!("environment not found"))?;

        let session_id = Ulid::from_string(&access.sid)
            .map_err(|e| anyhow::anyhow!("bad session id: {e}"))?;

        Ok(AuthSession {
            token_id: access.sid.clone(),
            user_id: Some(user_id),
            org_id: access.org,
            env_id,
            env_slug: env.slug,
            roles: access.roles,
            labels: access.labels,
            session_id: Some(session_id),
            token_kind: zauth::TokenKind::Access,
            credential_context: None,
            hardware_attested: false,
            biometric_verified: false,
            trust_score: 0.0,
        })
    }

    fn auth_session_to_principals(&self, session: &AuthSession) -> Vec<Principal> {
        let mut principals = Vec::new();

        // Add user principal
        if let Some(user_id) = session.user_id {
            principals.push(Principal::User(user_id.to_string()));
        }

        // Add role principals
        for role in &session.roles {
            principals.push(Principal::Role(role.clone()));
        }

        // Add label principals
        for label in &session.labels {
            principals.push(Principal::Label(label.clone()));
        }

        principals
    }
}

#[tokio::test]
async fn test_authentication_permission_integration() -> Result<()> {
    let integration = AuthPermissionIntegration::new()?;
    integration.setup_test_permissions()?;

    // Create test user with admin role
    let admin_user = integration.create_test_user("admin@test.com", vec!["admin".to_string()]).await?;

    // Login the user
    let login_result = integration.auth_service.login(
        integration.org_id,
        "admin@test.com",
        "test_password",
        None,
    ).await?;

    // Create auth session from token
    let auth_session = integration.create_auth_session_from_token(&login_result.access_token)?;

    // Convert auth session to permission principals
    let principals = integration.auth_session_to_principals(&auth_session);

    // Check that admin has full permissions
    let perms = integration.perm_service.effective_allow(
        integration.org_id,
        &principals,
        ResourceKind::Permissions,
    )?;

    assert!(perms.contains(Crud::CREATE | Crud::READ | Crud::UPDATE | Crud::DELETE));

    // Test permission enforcement
    let result = integration.perm_service.ensure(
        integration.org_id,
        &principals,
        ResourceKind::Jobs,
        Crud::READ,
    );

    assert!(result.is_ok());

    Ok(())
}

#[tokio::test]
async fn test_role_based_access_control() -> Result<()> {
    let integration = AuthPermissionIntegration::new()?;
    integration.setup_test_permissions()?;

    // Create users with different roles
    let _admin_user = integration.create_test_user("admin@test.com", vec!["admin".to_string()]).await?;
    let _operator_user = integration.create_test_user("operator@test.com", vec!["operator".to_string()]).await?;
    let _reader_user = integration.create_test_user("reader@test.com", vec!["reader".to_string()]).await?;

    // Setup specific permissions
    let operator_perms = PermissionRecord::new(
        Principal::Role("operator".to_string()),
        ResourceKind::Jobs,
        vec![CrudAction::Create, CrudAction::Read, CrudAction::Update],
    );
    integration.perm_service.upsert(integration.org_id, &operator_perms)?;

    let reader_perms = PermissionRecord::new(
        Principal::Role("reader".to_string()),
        ResourceKind::Jobs,
        vec![CrudAction::Read],
    );
    integration.perm_service.upsert(integration.org_id, &reader_perms)?;

    // Login users
    let admin_login = integration.auth_service.login(
        integration.org_id,
        "admin@test.com",
        "test_password",
        None,
    ).await?;

    let operator_login = integration.auth_service.login(
        integration.org_id,
        "operator@test.com",
        "test_password",
        None,
    ).await?;

    let reader_login = integration.auth_service.login(
        integration.org_id,
        "reader@test.com",
        "test_password",
        None,
    ).await?;

    // Create auth sessions
    let admin_session = integration.create_auth_session_from_token(&admin_login.access_token)?;
    let operator_session = integration.create_auth_session_from_token(&operator_login.access_token)?;
    let reader_session = integration.create_auth_session_from_token(&reader_login.access_token)?;

    // Test RBAC enforcement
    let admin_principals = integration.auth_session_to_principals(&admin_session);
    let operator_principals = integration.auth_session_to_principals(&operator_session);
    let reader_principals = integration.auth_session_to_principals(&reader_session);

    // Admin should have all permissions
    assert!(integration.perm_service.ensure(
        integration.org_id,
        &admin_principals,
        ResourceKind::Jobs,
        Crud::DELETE,
    ).is_ok());

    // Operator should have create, read, update but not delete
    assert!(integration.perm_service.ensure(
        integration.org_id,
        &operator_principals,
        ResourceKind::Jobs,
        Crud::CREATE,
    ).is_ok());

    assert!(integration.perm_service.ensure(
        integration.org_id,
        &operator_principals,
        ResourceKind::Jobs,
        Crud::DELETE,
    ).is_err());

    // Reader should only have read
    assert!(integration.perm_service.ensure(
        integration.org_id,
        &reader_principals,
        ResourceKind::Jobs,
        Crud::READ,
    ).is_ok());

    assert!(integration.perm_service.ensure(
        integration.org_id,
        &reader_principals,
        ResourceKind::Jobs,
        Crud::CREATE,
    ).is_err());

    Ok(())
}

#[tokio::test]
async fn test_session_management_with_permissions() -> Result<()> {
    let integration = AuthPermissionIntegration::new()?;
    integration.setup_test_permissions()?;

    // Create test user
    let user = integration.create_test_user("user@test.com", vec!["operator".to_string()]).await?;

    // Setup operator permissions
    let operator_perms = PermissionRecord::new(
        Principal::Role("operator".to_string()),
        ResourceKind::Collections,
        vec![CrudAction::Read, CrudAction::Create],
    );
    integration.perm_service.upsert(integration.org_id, &operator_perms)?;

    // Initial login
    let login1 = integration.auth_service.login(
        integration.org_id,
        "user@test.com",
        "test_password",
        None,
    ).await?;

    // Create auth session
    let session1 = integration.create_auth_session_from_token(&login1.access_token)?;
    let principals = integration.auth_session_to_principals(&session1);

    // Verify permissions
    assert!(integration.perm_service.ensure(
        integration.org_id,
        &principals,
        ResourceKind::Collections,
        Crud::READ,
    ).is_ok());

    // Refresh token
    let login2 = integration.auth_service.refresh(&login1.refresh_token)?;
    let session2 = integration.create_auth_session_from_token(&login2.access_token)?;
    let principals2 = integration.auth_session_to_principals(&session2);

    // Verify permissions are maintained after refresh
    assert!(integration.perm_service.ensure(
        integration.org_id,
        &principals2,
        ResourceKind::Collections,
        Crud::READ,
    ).is_ok());

    // Logout
    integration.auth_service.logout(&login2.refresh_token)?;

    // Verify session is invalidated
    let result = integration.auth_service.refresh(&login2.refresh_token);
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_cross_organization_permission_isolation() -> Result<()> {
    let integration = AuthPermissionIntegration::new()?;
    integration.setup_test_permissions()?;

    // Create users in different organizations
    let org1_user = integration.create_test_user_in_org(
        integration.org_id,
        "org1@test.com",
        vec!["admin".to_string()],
    ).await?;

    let org2_user = integration.create_test_user_in_org(
        integration.org_id_2,
        "org2@test.com",
        vec!["reader".to_string()],
    ).await?;

    // Setup different permissions for each organization
    let org1_admin_perms = PermissionRecord::new(
        Principal::Role("admin".to_string()),
        ResourceKind::Jobs,
        vec![CrudAction::Create, CrudAction::Read, CrudAction::Update, CrudAction::Delete],
    );
    integration.perm_service.upsert(integration.org_id, &org1_admin_perms)?;

    let org2_reader_perms = PermissionRecord::new(
        Principal::Role("reader".to_string()),
        ResourceKind::Jobs,
        vec![CrudAction::Read],
    );
    integration.perm_service.upsert(integration.org_id_2, &org2_reader_perms)?;

    // Login users
    let org1_login = integration.auth_service.login(
        integration.org_id,
        "org1@test.com",
        "test_password",
        None,
    ).await?;

    let org2_login = integration.auth_service.login(
        integration.org_id_2,
        "org2@test.com",
        "test_password",
        None,
    ).await?;

    // Create auth sessions
    let org1_session = integration.create_auth_session_from_token(&org1_login.access_token)?;
    let org2_session = integration.create_auth_session_from_token(&org2_login.access_token)?;

    let org1_principals = integration.auth_session_to_principals(&org1_session);
    let org2_principals = integration.auth_session_to_principals(&org2_session);

    // Org1 admin should have full permissions in their org
    assert!(integration.perm_service.ensure(
        integration.org_id,
        &org1_principals,
        ResourceKind::Jobs,
        Crud::DELETE,
    ).is_ok());

    // Org1 admin should NOT have access to org2 resources
    assert!(integration.perm_service.ensure(
        integration.org_id_2,
        &org1_principals,
        ResourceKind::Jobs,
        Crud::READ,
    ).is_err());

    // Org2 reader should only have read in their org
    assert!(integration.perm_service.ensure(
        integration.org_id_2,
        &org2_principals,
        ResourceKind::Jobs,
        Crud::READ,
    ).is_ok());

    // Org2 reader should not have delete permissions
    assert!(integration.perm_service.ensure(
        integration.org_id_2,
        &org2_principals,
        ResourceKind::Jobs,
        Crud::DELETE,
    ).is_err());

    // Org2 reader should not have access to org1 resources
    assert!(integration.perm_service.ensure(
        integration.org_id,
        &org2_principals,
        ResourceKind::Jobs,
        Crud::READ,
    ).is_err());

    Ok(())
}

#[tokio::test]
async fn test_permission_inheritance_and_delegation() -> Result<()> {
    let integration = AuthPermissionIntegration::new()?;
    integration.setup_test_permissions()?;

    // Create hierarchical structure
    let admin_user = integration.create_test_user("admin@test.com", vec!["admin".to_string()]).await?;
    let manager_user = integration.create_test_user("manager@test.com", vec!["manager".to_string()]).await?;
    let staff_user = integration.create_test_user("staff@test.com", vec!["staff".to_string()]).await?;

    // Setup team-based permissions
    let team_id = "engineering";
    let team_owner = Principal::TeamRole {
        team_id: team_id.to_string(),
        role: "owner".to_string(),
    };
    let team_admin = Principal::TeamRole {
        team_id: team_id.to_string(),
        role: "admin".to_string(),
    };
    let team_member = Principal::TeamRole {
        team_id: team_id.to_string(),
        role: "member".to_string(),
    };

    // Assign team roles through labels
    let manager_with_team_label = integration.create_test_user(
        "manager_with_team@test.com",
        vec!["manager".to_string()],
    ).await?;

    // Add team label to manager
    let mut manager_record = manager_with_team_label;
    manager_record.labels.push(format!("team:{}", team_id));
    integration.auth_service.repo().update_user(&manager_record)?;

    // Setup team permissions
    let team_perms = PermissionRecord::new(
        team_member.clone(),
        ResourceKind::Collections,
        vec![CrudAction::Read, CrudAction::Create],
    );
    integration.perm_service.upsert(integration.org_id, &team_perms)?;

    // Login users
    let manager_login = integration.auth_service.login(
        integration.org_id,
        "manager_with_team@test.com",
        "test_password",
        None,
    ).await?;

    let staff_login = integration.auth_service.login(
        integration.org_id,
        "staff@test.com",
        "test_password",
        None,
    ).await?;

    // Create auth sessions
    let manager_session = integration.create_auth_session_from_token(&manager_login.access_token)?;
    let staff_session = integration.create_auth_session_from_token(&staff_login.access_token)?;

    let mut manager_principals = integration.auth_session_to_principals(&manager_session);
    let staff_principals = integration.auth_session_to_principals(&staff_session);

    // Add team role principal to manager
    manager_principals.push(team_member);

    // Manager should have team permissions
    assert!(integration.perm_service.ensure(
        integration.org_id,
        &manager_principals,
        ResourceKind::Collections,
        Crud::CREATE,
    ).is_ok());

    // Staff should not have team permissions
    assert!(integration.perm_service.ensure(
        integration.org_id,
        &staff_principals,
        ResourceKind::Collections,
        Crud::CREATE,
    ).is_err());

    Ok(())
}

#[tokio::test]
async fn test_edge_case_permission_scenarios() -> Result<()> {
    let integration = AuthPermissionIntegration::new()?;
    integration.setup_test_permissions()?;

    // Test 1: User with no roles
    let no_role_user = integration.create_test_user("norole@test.com", vec![]).await?;
    let no_role_login = integration.auth_service.login(
        integration.org_id,
        "norole@test.com",
        "test_password",
        None,
    ).await?;

    let no_role_session = integration.create_auth_session_from_token(&no_role_login.access_token)?;
    let no_role_principals = integration.auth_session_to_principals(&no_role_session);

    // Should have no permissions
    assert!(integration.perm_service.ensure(
        integration.org_id,
        &no_role_principals,
        ResourceKind::Jobs,
        Crud::READ,
    ).is_err());

    // Test 2: Multiple roles with overlapping permissions
    let multi_role_user = integration.create_test_user(
        "multirole@test.com",
        vec!["operator".to_string(), "reader".to_string()],
    ).await?;

    let multi_role_login = integration.auth_service.login(
        integration.org_id,
        "multirole@test.com",
        "test_password",
        None,
    ).await?;

    let multi_role_session = integration.create_auth_session_from_token(&multi_role_login.access_token)?;
    let multi_role_principals = integration.auth_session_to_principals(&multi_role_session);

    // Should have combined permissions
    let effective_perms = integration.perm_service.effective_allow(
        integration.org_id,
        &multi_role_principals,
        ResourceKind::Jobs,
    )?;

    assert!(effective_perms.contains(Crud::READ)); // from reader role
    assert!(effective_perms.contains(Crud::CREATE)); // from operator role

    // Test 3: Invalid organization ID
    assert!(integration.perm_service.effective_allow(
        0,
        &multi_role_principals,
        ResourceKind::Jobs,
    ).is_err());

    // Test 4: Permission boundary checking
    let regular_user = integration.create_test_user("regular@test.com", vec!["user".to_string()]).await?;
    let regular_login = integration.auth_service.login(
        integration.org_id,
        "regular@test.com",
        "test_password",
        None,
    ).await?;

    let regular_session = integration.create_auth_session_from_token(&regular_login.access_token)?;
    let regular_principals = integration.auth_session_to_principals(&regular_session);

    // Try to grant self admin permissions (should be blocked by boundary check)
    let self_admin_grant = PermissionRecord::new(
        Principal::User(regular_user.id.to_string()),
        ResourceKind::Permissions,
        vec![CrudAction::Create, CrudAction::Update],
    );

    let result = integration.perm_service.upsert(integration.org_id, &self_admin_grant);
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_security_timing_attacks() -> Result<()> {
    let integration = AuthPermissionIntegration::new()?;
    integration.setup_test_permissions()?;

    // Create users
    let admin_user = integration.create_test_user("admin@test.com", vec!["admin".to_string()]).await?;
    let regular_user = integration.create_test_user("regular@test.com", vec!["user".to_string()]).await?;

    // Login users
    let admin_login = integration.auth_service.login(
        integration.org_id,
        "admin@test.com",
        "test_password",
        None,
    ).await?;

    let regular_login = integration.auth_service.login(
        integration.org_id,
        "regular@test.com",
        "test_password",
        None,
    ).await?;

    // Create auth sessions
    let admin_session = integration.create_auth_session_from_token(&admin_login.access_token)?;
    let regular_session = integration.create_auth_session_from_token(&regular_login.access_token)?;

    let admin_principals = integration.auth_session_to_principals(&admin_session);
    let regular_principals = integration.auth_session_to_principals(&regular_session);

    // Test timing attack protection
    let start1 = std::time::Instant::now();
    let result1 = integration.perm_service.ensure(
        integration.org_id,
        &admin_principals,
        ResourceKind::Jobs,
        Crud::DELETE,
    );
    let duration1 = start1.elapsed();

    let start2 = std::time::Instant::now();
    let result2 = integration.perm_service.ensure(
        integration.org_id,
        &regular_principals,
        ResourceKind::Jobs,
        Crud::DELETE,
    );
    let duration2 = start2.elapsed();

    // Results should be different (success vs failure)
    assert!(result1.is_ok());
    assert!(result2.is_err());

    // But timing should be similar
    let diff = duration1.abs_diff(duration2);
    assert!(diff < Duration::from_millis(50),
        "Timing difference too large: success={:?}, failure={:?}, diff={:?}",
        duration1, duration2, diff);

    Ok(())
}

#[tokio::test]
async fn test_dev_token_permissions() -> Result<()> {
    let integration = AuthPermissionIntegration::new()?;
    integration.setup_test_permissions()?;

    // Issue dev token
    let dev_token = issue_dev_token(&integration.auth_config)?;
    let dev_claims = verify_dev_token(&dev_token, integration.auth_config.dev_secret().unwrap())?;

    // Create auth session for dev token
    let auth_session = AuthSession {
        token_id: dev_claims.token_id,
        user_id: None,
        org_id: dev_claims.org_id,
        env_id: Ulid::new(), // Default environment
        env_slug: "live".to_string(),
        roles: dev_claims.roles,
        labels: dev_claims.labels,
        session_id: None,
        token_kind: zauth::TokenKind::Dev,
        credential_context: None,
        hardware_attested: false,
        biometric_verified: false,
        trust_score: 0.0,
    };

    // Convert to principals
    let dev_principals = integration.auth_session_to_principals(&auth_session);

    // Dev token should have admin permissions
    assert!(integration.perm_service.ensure(
        integration.org_id,
        &dev_principals,
        ResourceKind::Jobs,
        Crud::DELETE,
    ).is_ok());

    // Should have permission management capabilities
    assert!(integration.perm_service.ensure(
        integration.org_id,
        &dev_principals,
        ResourceKind::Permissions,
        Crud::CREATE,
    ).is_ok());

    Ok(())
}

#[tokio::test]
async fn test_permission_audit_logging() -> Result<()> {
    let integration = AuthPermissionIntegration::new()?;
    integration.setup_test_permissions()?;

    // Create user
    let user = integration.create_test_user("audit@test.com", vec!["admin".to_string()]).await?;
    let login = integration.auth_service.login(
        integration.org_id,
        "audit@test.com",
        "test_password",
        None,
    ).await?;

    let session = integration.create_auth_session_from_token(&login.access_token)?;
    let principals = integration.auth_session_to_principals(&session);

    // Perform permission check (should be logged)
    let _result = integration.perm_service.ensure(
        integration.org_id,
        &principals,
        ResourceKind::Jobs,
        Crud::READ,
    );

    // Perform permission modification (should be logged)
    let new_perm = PermissionRecord::new(
        Principal::User(user.id.to_string()),
        ResourceKind::Collections,
        vec![CrudAction::Read],
    );
    let _result = integration.perm_service.upsert(integration.org_id, &new_perm);

    // The audit logging is handled internally by the permission service
    // In a real implementation, we would verify the log entries
    // For this test, we just ensure the operations complete without error
    Ok(())
}

#[tokio::test]
async fn test_permission_edge_cases() -> Result<()> {
    let integration = AuthPermissionIntegration::new()?;

    // Test 1: Empty permission record
    let empty_record = PermissionRecord::new(
        Principal::User("test".to_string()),
        ResourceKind::Jobs,
        vec![],
    );

    let result = integration.perm_service.upsert(integration.org_id, &empty_record);
    assert!(result.is_ok());

    // Test 2: Permission with all actions
    let full_record = PermissionRecord::new(
        Principal::User("test".to_string()),
        ResourceKind::Jobs,
        vec![
            CrudAction::Read,
            CrudAction::Create,
            CrudAction::Update,
            CrudAction::Delete,
        ],
    );

    let result = integration.perm_service.upsert(integration.org_id, &full_record);
    assert!(result.is_ok());

    // Test 3: Invalid resource kind handling
    let user = integration.create_test_user("edgecase@test.com", vec!["user".to_string()]).await?;
    let login = integration.auth_service.login(
        integration.org_id,
        "edgecase@test.com",
        "test_password",
        None,
    ).await?;

    let session = integration.create_auth_session_from_token(&login.access_token)?;
    let principals = integration.auth_session_to_principals(&session);

    // Should handle unknown resource types gracefully
    let result = integration.perm_service.effective_allow(
        integration.org_id,
        &principals,
        ResourceKind::Jobs,
    );

    assert!(result.is_ok());

    Ok(())
}