use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use tempfile::tempdir;
use ulid::Ulid;

use zauth::{
    AuthConfig, AuthService, AuthSession,
    models::UserRecord, models::UserStatus,
    hash_password, issue_dev_token, verify_dev_token,
    DevTokenClaims
};
use zpermissions::{
    PermissionService, Principal, ResourceKind, Crud, CrudAction,
    PermissionRecord
};
use zcore_storage::Store;

/// Test fixture for integration testing of auth and permissions
struct AuthPermissionIntegration {
    pub store: &'static Store,
    pub auth_config: AuthConfig,
    pub auth_service: AuthService<'static>,
    pub perm_service: PermissionService<'static>,
    pub org_id: u64,
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

    fn setup_test_permissions(&self) -> Result<()> {
        // Setup admin role with full permissions
        let admin_role = Principal::Role("admin".to_string());
        let admin_perms = PermissionRecord::new(
            admin_role.clone(),
            ResourceKind::Jobs,
            vec![
                CrudAction::Create,
                CrudAction::Read,
                CrudAction::Update,
                CrudAction::Delete,
            ],
        );

        self.perm_service.upsert(self.org_id, &admin_perms)?;
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

        principals
    }
}

#[tokio::test]
async fn test_basic_auth_permission_integration() -> Result<()> {
    let integration = AuthPermissionIntegration::new()?;
    integration.setup_test_permissions()?;

    // Create test user with admin role
    let _user = integration.create_test_user("admin@test.com", vec!["admin".to_string()]).await?;

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
        ResourceKind::Jobs,
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
async fn test_session_permission_isolation() -> Result<()> {
    let integration = AuthPermissionIntegration::new()?;
    integration.setup_test_permissions()?;

    // Create user with limited role
    let _user = integration.create_test_user("user@test.com", vec!["user".to_string()]).await?;

    // Setup limited permissions for user role
    let user_role = Principal::Role("user".to_string());
    let user_perms = PermissionRecord::new(
        user_role.clone(),
        ResourceKind::Jobs,
        vec![CrudAction::Read],  // Only read permissions
    );
    integration.perm_service.upsert(integration.org_id, &user_perms)?;

    // Login the user
    let login_result = integration.auth_service.login(
        integration.org_id,
        "user@test.com",
        "test_password",
        None,
    ).await?;

    // Create auth session from token
    let auth_session = integration.create_auth_session_from_token(&login_result.access_token)?;
    let principals = integration.auth_session_to_principals(&auth_session);

    // User should have read permissions
    assert!(integration.perm_service.ensure(
        integration.org_id,
        &principals,
        ResourceKind::Jobs,
        Crud::READ,
    ).is_ok());

    // User should NOT have create permissions
    assert!(integration.perm_service.ensure(
        integration.org_id,
        &principals,
        ResourceKind::Jobs,
        Crud::CREATE,
    ).is_err());

    // User should NOT have delete permissions
    assert!(integration.perm_service.ensure(
        integration.org_id,
        &principals,
        ResourceKind::Jobs,
        Crud::DELETE,
    ).is_err());

    Ok(())
}

#[tokio::test]
async fn test_dev_token_permissions() -> Result<()> {
    // Set development environment to allow dev tokens
    std::env::set_var("ZAUTH_DEV_SECRET", "test_dev_secret");
    std::env::remove_var("ENVIRONMENT"); // Clear any production indicators

    let integration = AuthPermissionIntegration::new()?;
    integration.setup_test_permissions()?;

    // Issue dev token
    let dev_token = issue_dev_token(&integration.auth_config)?;
    let _dev_claims = verify_dev_token(&dev_token, integration.auth_config.dev_secret().unwrap())?;

    // Create auth session for dev token
    let auth_session = AuthSession {
        token_id: "dev-token".to_string(),
        user_id: None,
        org_id: integration.org_id,
        env_id: Ulid::new(), // Default environment
        env_slug: "live".to_string(),
        roles: vec!["admin".to_string()], // Dev tokens have admin role
        labels: Vec::new(),
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

    Ok(())
}

#[tokio::test]
async fn test_permission_boundary_enforcement() -> Result<()> {
    let integration = AuthPermissionIntegration::new()?;

    // Create regular user
    let user = integration.create_test_user("regular@test.com", vec!["user".to_string()]).await?;

    // Login the user
    let login_result = integration.auth_service.login(
        integration.org_id,
        "regular@test.com",
        "test_password",
        None,
    ).await?;

    let auth_session = integration.create_auth_session_from_token(&login_result.access_token)?;
    let _user_principals = integration.auth_session_to_principals(&auth_session);

    // First, remove admin role's permissions to set up test condition
    let admin_role = Principal::Role("admin".to_string());
    let empty_admin_record = PermissionRecord::new(
        admin_role.clone(),
        ResourceKind::Permissions,
        vec![]
    );
    // This should succeed to set up the test condition
    integration.perm_service.upsert(integration.org_id, &empty_admin_record)?;

    // Try to grant self permission management permissions (should be blocked by boundary check)
    let self_admin_grant = PermissionRecord::new(
        Principal::User(user.id.to_string()),
        ResourceKind::Permissions,
        vec![CrudAction::Create, CrudAction::Update],
    );

    let result = integration.perm_service.upsert(integration.org_id, &self_admin_grant);
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_org_isolation() -> Result<()> {
    let integration = AuthPermissionIntegration::new()?;

    let org1 = 1001;
    let org2 = 2002;

    // Create user in org1 with admin role
    let _org1_user = integration.create_test_user("org1@test.com", vec!["admin".to_string()]).await?;

    // Create user in org2 with limited role using the same integration but different org
    let _org2_user = UserRecord {
        id: Ulid::new(),
        org_id: org2,
        email: "org2@test.com".to_lowercase(),
        password_hash: hash_password("test_password")?,
        roles: vec!["user".to_string()],
        labels: Vec::new(),
        status: UserStatus::Active,
        created_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs() as i64,
        updated_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs() as i64,
    };
    integration.auth_service.repo().insert_user(&_org2_user)?;

    // Setup admin permissions for org1
    let admin_role = Principal::Role("admin".to_string());
    let admin_perms = PermissionRecord::new(
        admin_role.clone(),
        ResourceKind::Jobs,
        vec![CrudAction::Create, CrudAction::Read, CrudAction::Update, CrudAction::Delete],
    );
    integration.perm_service.upsert(org1, &admin_perms)?;

    // Setup read-only permissions for org2
    let user_role = Principal::Role("user".to_string());
    let user_perms = PermissionRecord::new(
        user_role.clone(),
        ResourceKind::Jobs,
        vec![CrudAction::Read],
    );
    integration.perm_service.upsert(org2, &user_perms)?;

    // Explicitly restrict admin role in org2 (override default behavior)
    let admin_role_restricted = Principal::Role("admin".to_string());
    let admin_restricted_perms = PermissionRecord::new(
        admin_role_restricted.clone(),
        ResourceKind::Jobs,
        vec![], // No permissions for admin role in org2
    );
    integration.perm_service.upsert(org2, &admin_restricted_perms)?;

    // Login org1 user
    let org1_login = integration.auth_service.login(
        org1,
        "org1@test.com",
        "test_password",
        None,
    ).await?;

    // Login org2 user
    let org2_login = integration.auth_service.login(
        org2,
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
        org1,
        &org1_principals,
        ResourceKind::Jobs,
        Crud::DELETE,
    ).is_ok());

    // Org1 admin should NOT have access to org2 resources
    assert!(integration.perm_service.ensure(
        org2,
        &org1_principals,
        ResourceKind::Jobs,
        Crud::READ,
    ).is_err());

    // Org2 user should only have read in their org
    assert!(integration.perm_service.ensure(
        org2,
        &org2_principals,
        ResourceKind::Jobs,
        Crud::READ,
    ).is_ok());

    // Org2 user should not have delete permissions
    assert!(integration.perm_service.ensure(
        org2,
        &org2_principals,
        ResourceKind::Jobs,
        Crud::DELETE,
    ).is_err());

    Ok(())
}