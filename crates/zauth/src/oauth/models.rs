use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use ulid::Ulid;

/// OAuth provider enumeration
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum OAuthProvider {
    Google,
    GitHub,
    Discord,
    Apple,
}

impl OAuthProvider {
    pub fn as_str(&self) -> &'static str {
        match self {
            OAuthProvider::Google => "google",
            OAuthProvider::GitHub => "github",
            OAuthProvider::Discord => "discord",
            OAuthProvider::Apple => "apple",
        }
    }
}

impl std::str::FromStr for OAuthProvider {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "google" => Ok(OAuthProvider::Google),
            "github" => Ok(OAuthProvider::GitHub),
            "discord" => Ok(OAuthProvider::Discord),
            "apple" => Ok(OAuthProvider::Apple),
            _ => Err(anyhow::anyhow!("Unknown OAuth provider: {}", s)),
        }
    }
}

/// OAuth application configuration for a specific provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthConfig {
    pub provider: OAuthProvider,
    pub client_id: String,
    pub client_secret: String,
    pub redirect_uri: String,
    pub scopes: Vec<String>,
    pub auth_url: String,
    pub token_url: String,
    pub user_info_url: String,
}

/// OAuth state parameter for CSRF protection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthState {
    pub state_id: String,
    pub provider: OAuthProvider,
    pub org_id: u64,
    pub env_slug: Option<String>,
    pub linking_user_id: Option<Ulid>, // For account linking
    pub created_at: i64,
    pub expires_at: i64,
}

/// OAuth profile information from provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthProfile {
    pub id: String,                                      // Generated 32-char unique ID
    pub org_id: u64,
    pub provider: OAuthProvider,
    pub provider_user_id: String,                        // Platform-specific user ID
    pub email: Option<String>,
    pub name: Option<String>,
    pub avatar_url: Option<String>,
    pub username: Option<String>,
    pub metadata: HashMap<String, serde_json::Value>,    // Provider-specific data
    pub created_at: i64,
    pub updated_at: i64,
    pub last_login_at: Option<i64>,
}

/// OAuth account linking between ZRUSTDB user and OAuth profiles
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthAccountLink {
    pub id: Ulid,
    pub org_id: u64,
    pub user_id: Ulid,                                   // ZRUSTDB user ID
    pub oauth_profile_id: String,                        // OAuth profile unique ID
    pub provider: OAuthProvider,
    pub is_primary: bool,                                // Primary login method
    pub linked_at: i64,
    pub verified_at: Option<i64>,
}

/// OAuth session for tracking authorization flows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthSession {
    pub session_id: String,
    pub state_id: String,
    pub provider: OAuthProvider,
    pub org_id: u64,
    pub code_verifier: Option<String>,                   // For PKCE
    pub created_at: i64,
    pub expires_at: i64,
}

/// OAuth tokens from provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthTokens {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub token_type: String,
    pub scope: Option<String>,
    pub expires_at: Option<i64>,
}

/// Unified user profile combining ZRUSTDB user and OAuth profiles
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedUserProfile {
    pub user_id: Ulid,
    pub org_id: u64,
    pub email: String,
    pub primary_name: String,
    pub primary_avatar: Option<String>,
    pub oauth_profiles: Vec<OAuthProfile>,
    pub account_links: Vec<OAuthAccountLink>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// OAuth login result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthLoginResult {
    pub user_id: Ulid,
    pub is_new_user: bool,
    pub is_linked_account: bool,
    pub oauth_profile: OAuthProfile,
    pub access_token: String,
    pub refresh_token: String,
    pub access_expires_at: i64,
    pub refresh_expires_at: i64,
}

/// Provider-specific user information from OAuth callback
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderUserInfo {
    pub id: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub username: Option<String>,
    pub avatar_url: Option<String>,
    pub verified_email: Option<bool>,
    pub raw_data: serde_json::Value,
}

/// OAuth error types
#[derive(Debug, thiserror::Error)]
pub enum OAuthError {
    #[error("Invalid OAuth state parameter")]
    InvalidState,
    #[error("OAuth state expired")]
    StateExpired,
    #[error("Provider error: {0}")]
    ProviderError(String),
    #[error("Invalid authorization code")]
    InvalidCode,
    #[error("Token exchange failed: {0}")]
    TokenExchangeFailed(String),
    #[error("User info fetch failed: {0}")]
    UserInfoFailed(String),
    #[error("Account already linked to another user")]
    AccountAlreadyLinked,
    #[error("Email already registered with different provider")]
    EmailConflict,
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Network error: {0}")]
    NetworkError(#[from] reqwest::Error),
    #[error("JSON parsing error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Storage error: {0}")]
    StorageError(#[from] anyhow::Error),
}