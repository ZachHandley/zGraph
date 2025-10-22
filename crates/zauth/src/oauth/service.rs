use super::{
    generate_oauth_user_id, models::*, providers::OAuthProviders, repository::OAuthRepository,
};
use crate::{
    models::UserRecord, models::UserStatus, repository::AuthRepository,
    AuthConfig, AuthService, LoginTokens,
};
use anyhow::{anyhow, Result};
use oauth2::{
    reqwest::async_http_client, AuthorizationCode, CsrfToken, PkceCodeChallenge,
    PkceCodeVerifier, TokenResponse,
};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use ulid::Ulid;
use uuid::Uuid;
use zcore_storage::Store;

pub struct OAuthService<'a> {
    providers: OAuthProviders,
    oauth_repo: OAuthRepository<'a>,
    auth_repo: AuthRepository<'a>,
    auth_config: AuthConfig,
}

impl<'a> OAuthService<'a> {
    pub fn new(store: &'a Store, auth_config: AuthConfig) -> Self {
        let mut providers = OAuthProviders::new();

        // Initialize providers with configurations from environment
        for config in super::providers::create_default_configs() {
            if !config.client_id.is_empty() && !config.client_secret.is_empty() {
                providers.add_provider(config);
            }
        }

        Self {
            providers,
            oauth_repo: OAuthRepository::new(store),
            auth_repo: AuthRepository::new(store),
            auth_config,
        }
    }

    /// Add OAuth provider configuration
    pub fn add_provider(&mut self, config: OAuthConfig) {
        self.providers.add_provider(config);
    }

    /// Initiate OAuth authorization flow
    pub async fn initiate_oauth_flow(
        &self,
        provider: OAuthProvider,
        org_id: u64,
        env_slug: Option<String>,
        linking_user_id: Option<Ulid>,
    ) -> Result<OAuthFlowInitiation, OAuthError> {
        let client = self
            .providers
            .create_client(&provider)
            .map_err(|e| OAuthError::ConfigError(e.to_string()))?;

        // Generate PKCE challenge for enhanced security
        let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();

        // Generate CSRF state
        let state_id = Uuid::new_v4().to_string();
        let csrf_token = CsrfToken::new(state_id.clone());

        let now = current_timestamp();
        let expires_at = now + 600; // 10 minutes

        // Store OAuth state
        let oauth_state = OAuthState {
            state_id: state_id.clone(),
            provider: provider.clone(),
            org_id,
            env_slug,
            linking_user_id,
            created_at: now,
            expires_at,
        };

        self.oauth_repo.store_oauth_state(&oauth_state)?;

        // Store OAuth session with PKCE verifier
        let session_id = Uuid::new_v4().to_string();
        let oauth_session = OAuthSession {
            session_id: session_id.clone(),
            state_id: state_id.clone(),
            provider: provider.clone(),
            org_id,
            code_verifier: Some(pkce_verifier.secret().to_string()),
            created_at: now,
            expires_at,
        };

        self.oauth_repo.store_oauth_session(&oauth_session)?;

        // Get provider scopes
        let scopes = super::providers::get_provider_scopes(&provider);

        // Build authorization URL
        let (auth_url, _) = client
            .authorize_url(|| csrf_token)
            .add_scopes(scopes)
            .set_pkce_challenge(pkce_challenge)
            .url();

        Ok(OAuthFlowInitiation {
            authorization_url: auth_url.to_string(),
            state: state_id,
            session_id,
        })
    }

    /// Handle OAuth callback and complete authentication
    pub async fn handle_oauth_callback(
        &self,
        code: String,
        state: String,
        session_id: Option<String>,
    ) -> Result<OAuthLoginResult, OAuthError> {
        // Validate and consume state
        let oauth_state = self
            .oauth_repo
            .consume_oauth_state(&state)?
            .ok_or(OAuthError::InvalidState)?;

        let now = current_timestamp();
        if oauth_state.expires_at < now {
            return Err(OAuthError::StateExpired);
        }

        // Get OAuth session
        let session_id = session_id.unwrap_or(oauth_state.state_id.clone());
        let oauth_session = self
            .oauth_repo
            .get_oauth_session(&session_id)?
            .ok_or(OAuthError::InvalidState)?;

        if oauth_session.expires_at < now {
            return Err(OAuthError::StateExpired);
        }

        // Clean up session
        self.oauth_repo.delete_oauth_session(&session_id)?;

        // Exchange authorization code for tokens
        let tokens = self
            .exchange_code_for_tokens(&oauth_state.provider, &code, &oauth_session)
            .await?;

        // Fetch user information from provider
        let user_info = self
            .providers
            .fetch_user_info(&oauth_state.provider, &tokens.access_token)
            .await
            .map_err(|e| OAuthError::UserInfoFailed(e.to_string()))?;

        // Handle user authentication/registration
        if let Some(linking_user_id) = oauth_state.linking_user_id {
            // Account linking flow
            self.link_oauth_account(oauth_state.org_id, linking_user_id, &oauth_state.provider, &user_info, &tokens)
                .await
        } else {
            // Regular OAuth login/registration flow
            self.authenticate_or_register_oauth_user(oauth_state.org_id, &oauth_state.provider, &user_info, &tokens, oauth_state.env_slug)
                .await
        }
    }

    /// Link OAuth account to existing user
    async fn link_oauth_account(
        &self,
        org_id: u64,
        user_id: Ulid,
        provider: &OAuthProvider,
        user_info: &ProviderUserInfo,
        _tokens: &OAuthTokens,
    ) -> Result<OAuthLoginResult, OAuthError> {
        // Check if this OAuth account is already linked to another user
        let oauth_profile_id = generate_oauth_user_id(provider.as_str(), &user_info.id);

        if let Some(existing_link) = self
            .oauth_repo
            .get_account_link_by_oauth_profile(org_id, &oauth_profile_id)?
        {
            if existing_link.user_id != user_id {
                return Err(OAuthError::AccountAlreadyLinked);
            }
        }

        // Create or update OAuth profile
        let oauth_profile = self.create_or_update_oauth_profile(org_id, provider, user_info)?;

        // Create account link if it doesn't exist
        if self
            .oauth_repo
            .get_account_link_by_oauth_profile(org_id, &oauth_profile_id)?
            .is_none()
        {
            let account_link = OAuthAccountLink {
                id: Ulid::new(),
                org_id,
                user_id,
                oauth_profile_id: oauth_profile_id.clone(),
                provider: provider.clone(),
                is_primary: false, // Don't make linked accounts primary by default
                linked_at: current_timestamp(),
                verified_at: if user_info.verified_email.unwrap_or(false) {
                    Some(current_timestamp())
                } else {
                    None
                },
            };

            self.oauth_repo.insert_account_link(&account_link)?;
        }

        // Generate JWT tokens for the user
        let _auth_service = AuthService::new(self.oauth_repo.store(), self.auth_config.clone());
        let user = self
            .auth_repo
            .get_user_by_id(org_id, &user_id)?
            .ok_or_else(|| anyhow!("User not found"))?;

        let (live_env, _) = self.auth_repo.ensure_default_environments(org_id)?;
        let login_tokens = self.generate_user_tokens(&user, &live_env.id, &live_env.slug)?;

        Ok(OAuthLoginResult {
            user_id,
            is_new_user: false,
            is_linked_account: true,
            oauth_profile,
            access_token: login_tokens.access_token,
            refresh_token: login_tokens.refresh_token,
            access_expires_at: login_tokens.access_expires_at,
            refresh_expires_at: login_tokens.refresh_expires_at,
        })
    }

    /// Authenticate existing user or register new user via OAuth
    async fn authenticate_or_register_oauth_user(
        &self,
        org_id: u64,
        provider: &OAuthProvider,
        user_info: &ProviderUserInfo,
        _tokens: &OAuthTokens,
        env_slug: Option<String>,
    ) -> Result<OAuthLoginResult, OAuthError> {
        let oauth_profile_id = generate_oauth_user_id(provider.as_str(), &user_info.id);

        // Check if OAuth profile exists and is linked to a user
        if let Some(existing_link) = self
            .oauth_repo
            .get_account_link_by_oauth_profile(org_id, &oauth_profile_id)?
        {
            // Existing OAuth user - authenticate
            let user = self
                .auth_repo
                .get_user_by_id(org_id, &existing_link.user_id)?
                .ok_or_else(|| anyhow!("Linked user not found"))?;

            // Update OAuth profile
            let oauth_profile = self.create_or_update_oauth_profile(org_id, provider, user_info)?;

            // Determine target environment
            let (live_env, sandbox_env) = self.auth_repo.ensure_default_environments(org_id)?;
            let target_env = match env_slug.as_deref() {
                Some("sandbox") => sandbox_env,
                Some("live") | None => live_env,
                Some(other) => self
                    .auth_repo
                    .get_environment_by_slug(org_id, other)?
                    .ok_or_else(|| anyhow!("Unknown environment"))?,
            };

            let login_tokens = self.generate_user_tokens(&user, &target_env.id, &target_env.slug)?;

            Ok(OAuthLoginResult {
                user_id: user.id,
                is_new_user: false,
                is_linked_account: false,
                oauth_profile,
                access_token: login_tokens.access_token,
                refresh_token: login_tokens.refresh_token,
                access_expires_at: login_tokens.access_expires_at,
                refresh_expires_at: login_tokens.refresh_expires_at,
            })
        } else {
            // New OAuth user - check email conflicts and register
            if let Some(email) = &user_info.email {
                // Check if email is already registered with regular auth
                if self.auth_repo.get_user_by_email(org_id, email)?.is_some() {
                    return Err(OAuthError::EmailConflict);
                }

                // Check if email is used by another OAuth account
                if let Some(existing_profile) = self.oauth_repo.get_oauth_profile_by_email(org_id, email)? {
                    if existing_profile.id != oauth_profile_id {
                        return Err(OAuthError::EmailConflict);
                    }
                }
            }

            // Register new user
            self.register_new_oauth_user(org_id, provider, user_info, env_slug)
                .await
        }
    }

    /// Register new user via OAuth
    async fn register_new_oauth_user(
        &self,
        org_id: u64,
        provider: &OAuthProvider,
        user_info: &ProviderUserInfo,
        env_slug: Option<String>,
    ) -> Result<OAuthLoginResult, OAuthError> {
        let now = current_timestamp();

        // Create OAuth profile
        let oauth_profile = self.create_or_update_oauth_profile(org_id, provider, user_info)?;

        // Create new user record
        let user_id = Ulid::new();
        let email = user_info.email.clone().unwrap_or_else(|| {
            // Generate a placeholder email for providers that don't provide email
            format!("{}+{}@oauth.placeholder", provider.as_str(), user_info.id)
        });

        let user = UserRecord {
            id: user_id,
            org_id,
            email: email.clone(),
            password_hash: String::new(), // OAuth users don't have passwords
            roles: vec!["user".to_string()], // Default role
            labels: vec![format!("oauth:{}", provider.as_str())],
            status: UserStatus::Active,
            created_at: now,
            updated_at: now,
        };

        self.auth_repo.insert_user(&user)?;

        // Create account link
        let account_link = OAuthAccountLink {
            id: Ulid::new(),
            org_id,
            user_id,
            oauth_profile_id: oauth_profile.id.clone(),
            provider: provider.clone(),
            is_primary: true, // First OAuth login is primary
            linked_at: now,
            verified_at: if user_info.verified_email.unwrap_or(false) {
                Some(now)
            } else {
                None
            },
        };

        self.oauth_repo.insert_account_link(&account_link)?;

        // Determine target environment
        let (live_env, sandbox_env) = self.auth_repo.ensure_default_environments(org_id)?;
        let target_env = match env_slug.as_deref() {
            Some("sandbox") => sandbox_env,
            Some("live") | None => live_env,
            Some(other) => self
                .auth_repo
                .get_environment_by_slug(org_id, other)?
                .ok_or_else(|| anyhow!("Unknown environment"))?,
        };

        let login_tokens = self.generate_user_tokens(&user, &target_env.id, &target_env.slug)?;

        Ok(OAuthLoginResult {
            user_id,
            is_new_user: true,
            is_linked_account: false,
            oauth_profile,
            access_token: login_tokens.access_token,
            refresh_token: login_tokens.refresh_token,
            access_expires_at: login_tokens.access_expires_at,
            refresh_expires_at: login_tokens.refresh_expires_at,
        })
    }

    /// Create or update OAuth profile
    fn create_or_update_oauth_profile(
        &self,
        org_id: u64,
        provider: &OAuthProvider,
        user_info: &ProviderUserInfo,
    ) -> Result<OAuthProfile> {
        let oauth_profile_id = generate_oauth_user_id(provider.as_str(), &user_info.id);
        let now = current_timestamp();

        // Convert raw_data to metadata HashMap
        let metadata = if let serde_json::Value::Object(map) = &user_info.raw_data {
            map.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        } else {
            HashMap::new()
        };

        let oauth_profile = if let Some(mut existing) = self.oauth_repo.get_oauth_profile(org_id, &oauth_profile_id)? {
            // Update existing profile
            existing.email = user_info.email.clone();
            existing.name = user_info.name.clone();
            existing.avatar_url = user_info.avatar_url.clone();
            existing.username = user_info.username.clone();
            existing.metadata = metadata;
            existing.updated_at = now;
            existing.last_login_at = Some(now);
            existing
        } else {
            // Create new profile
            OAuthProfile {
                id: oauth_profile_id,
                org_id,
                provider: provider.clone(),
                provider_user_id: user_info.id.clone(),
                email: user_info.email.clone(),
                name: user_info.name.clone(),
                avatar_url: user_info.avatar_url.clone(),
                username: user_info.username.clone(),
                metadata,
                created_at: now,
                updated_at: now,
                last_login_at: Some(now),
            }
        };

        self.oauth_repo.upsert_oauth_profile(&oauth_profile)?;
        Ok(oauth_profile)
    }

    /// Exchange authorization code for tokens
    async fn exchange_code_for_tokens(
        &self,
        provider: &OAuthProvider,
        code: &str,
        oauth_session: &OAuthSession,
    ) -> Result<OAuthTokens, OAuthError> {
        let client = self
            .providers
            .create_client(provider)
            .map_err(|e| OAuthError::ConfigError(e.to_string()))?;

        let mut token_request = client.exchange_code(AuthorizationCode::new(code.to_string()));

        // Add PKCE verifier if available
        if let Some(verifier) = &oauth_session.code_verifier {
            token_request = token_request.set_pkce_verifier(PkceCodeVerifier::new(verifier.clone()));
        }

        let token_response = token_request
            .request_async(async_http_client)
            .await
            .map_err(|e| OAuthError::TokenExchangeFailed(e.to_string()))?;

        let expires_at = token_response
            .expires_in()
            .map(|duration| current_timestamp() + duration.as_secs() as i64);

        Ok(OAuthTokens {
            access_token: token_response.access_token().secret().to_string(),
            refresh_token: token_response
                .refresh_token()
                .map(|token| token.secret().to_string()),
            token_type: "Bearer".to_string(),
            scope: token_response
                .scopes()
                .map(|scopes| {
                    scopes
                        .iter()
                        .map(|scope| scope.to_string())
                        .collect::<Vec<_>>()
                        .join(" ")
                }),
            expires_at,
        })
    }

    /// Generate JWT tokens for user
    fn generate_user_tokens(&self, user: &UserRecord, env_id: &Ulid, env_slug: &str) -> Result<LoginTokens> {
        use crate::jwt::{AccessClaims, RefreshClaims};

        let session_id = Ulid::new();
        let now = current_timestamp();
        let access_exp = now + self.auth_config.access_ttl().as_secs() as i64;
        let refresh_exp = now + self.auth_config.refresh_ttl().as_secs() as i64;

        let access_claims = AccessClaims {
            sub: user.id.to_string(),
            org: user.org_id,
            env: env_id.to_string(),
            roles: user.roles.clone(),
            labels: user.labels.clone(),
            sid: session_id.to_string(),
            iat: now,
            exp: access_exp,
            typ: "access".into(),
        };

        let refresh_claims = RefreshClaims {
            sub: user.id.to_string(),
            org: user.org_id,
            sid: session_id.to_string(),
            env: env_id.to_string(),
            iat: now,
            exp: refresh_exp,
            typ: "refresh".into(),
        };

        let keys = self.auth_config.jwt_keys();
        let access_token = keys.encode_access(&access_claims)?;
        let refresh_token = keys.encode_refresh(&refresh_claims)?;

        let session_payload = bincode::serialize(&refresh_claims)?;
        self.auth_repo.insert_session(session_id, &session_payload)?;

        Ok(LoginTokens {
            access_token,
            refresh_token,
            access_expires_at: access_exp,
            refresh_expires_at: refresh_exp,
            environment: crate::EnvironmentSummary {
                id: env_id.to_string(),
                slug: env_slug.to_string(),
                display_name: env_slug.to_string(),
                auto_clear_policy: crate::models::AutoClearPolicy::Never,
            },
        })
    }

    /// Get unified user profile
    pub fn get_unified_user_profile(&self, org_id: u64, user_id: &Ulid) -> Result<Option<UnifiedUserProfile>> {
        self.oauth_repo.get_unified_user_profile(org_id, user_id)
    }

    /// Unlink OAuth account
    pub fn unlink_oauth_account(&self, org_id: u64, user_id: &Ulid, provider: &OAuthProvider) -> Result<()> {
        let account_links = self.oauth_repo.get_account_links_for_user(org_id, user_id)?;

        for link in account_links {
            if link.provider == *provider {
                self.oauth_repo.delete_account_link(org_id, &link.id)?;
                break;
            }
        }

        Ok(())
    }

    /// Clean expired OAuth data
    pub fn cleanup_expired_data(&self) -> Result<()> {
        let now = current_timestamp();
        self.oauth_repo.cleanup_expired_oauth_data(now)
    }
}

/// OAuth flow initiation response
#[derive(Debug, serde::Serialize)]
pub struct OAuthFlowInitiation {
    pub authorization_url: String,
    pub state: String,
    pub session_id: String,
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}