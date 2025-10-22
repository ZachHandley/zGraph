use super::models::{OAuthConfig, OAuthProvider, ProviderUserInfo};
use anyhow::{anyhow, Result};
use oauth2::{
    basic::BasicClient, AuthUrl, ClientId, ClientSecret, RedirectUrl, Scope, TokenUrl,
};
use reqwest;
use serde_json::Value;
use std::collections::HashMap;

/// OAuth provider configurations
pub struct OAuthProviders {
    configs: HashMap<OAuthProvider, OAuthConfig>,
    http_client: reqwest::Client,
}

impl OAuthProviders {
    pub fn new() -> Self {
        Self {
            configs: HashMap::new(),
            http_client: reqwest::Client::new(),
        }
    }

    /// Add OAuth provider configuration
    pub fn add_provider(&mut self, config: OAuthConfig) {
        self.configs.insert(config.provider.clone(), config);
    }

    /// Get provider configuration
    pub fn get_config(&self, provider: &OAuthProvider) -> Option<&OAuthConfig> {
        self.configs.get(provider)
    }

    /// Create OAuth client for provider
    pub fn create_client(&self, provider: &OAuthProvider) -> Result<BasicClient> {
        let config = self
            .get_config(provider)
            .ok_or_else(|| anyhow!("Provider not configured: {:?}", provider))?;

        let client = BasicClient::new(
            ClientId::new(config.client_id.clone()),
            Some(ClientSecret::new(config.client_secret.clone())),
            AuthUrl::new(config.auth_url.clone())
                .map_err(|e| anyhow!("Invalid auth URL: {}", e))?,
            Some(
                TokenUrl::new(config.token_url.clone())
                    .map_err(|e| anyhow!("Invalid token URL: {}", e))?,
            ),
        )
        .set_redirect_uri(
            RedirectUrl::new(config.redirect_uri.clone())
                .map_err(|e| anyhow!("Invalid redirect URI: {}", e))?,
        );

        Ok(client)
    }

    /// Fetch user information from provider
    pub async fn fetch_user_info(
        &self,
        provider: &OAuthProvider,
        access_token: &str,
    ) -> Result<ProviderUserInfo> {
        let config = self
            .get_config(provider)
            .ok_or_else(|| anyhow!("Provider not configured: {:?}", provider))?;

        let response = self
            .http_client
            .get(&config.user_info_url)
            .bearer_auth(access_token)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "Failed to fetch user info: HTTP {}",
                response.status()
            ));
        }

        let user_data: Value = response.json().await?;
        self.parse_user_info(provider, user_data)
    }

    /// Parse provider-specific user information
    fn parse_user_info(
        &self,
        provider: &OAuthProvider,
        raw_data: Value,
    ) -> Result<ProviderUserInfo> {
        match provider {
            OAuthProvider::Google => self.parse_google_user_info(raw_data),
            OAuthProvider::GitHub => self.parse_github_user_info(raw_data),
            OAuthProvider::Discord => self.parse_discord_user_info(raw_data),
            OAuthProvider::Apple => self.parse_apple_user_info(raw_data),
        }
    }

    fn parse_google_user_info(&self, data: Value) -> Result<ProviderUserInfo> {
        let id = data["sub"]
            .as_str()
            .ok_or_else(|| anyhow!("Missing Google user ID"))?
            .to_string();

        let email = data["email"].as_str().map(|s| s.to_string());
        let name = data["name"].as_str().map(|s| s.to_string());
        let avatar_url = data["picture"].as_str().map(|s| s.to_string());
        let verified_email = data["email_verified"].as_bool();

        Ok(ProviderUserInfo {
            id,
            email,
            name,
            username: None, // Google doesn't provide username
            avatar_url,
            verified_email,
            raw_data: data,
        })
    }

    fn parse_github_user_info(&self, data: Value) -> Result<ProviderUserInfo> {
        let id = data["id"]
            .as_u64()
            .ok_or_else(|| anyhow!("Missing GitHub user ID"))?
            .to_string();

        let email = data["email"].as_str().map(|s| s.to_string());
        let name = data["name"].as_str().map(|s| s.to_string());
        let username = data["login"].as_str().map(|s| s.to_string());
        let avatar_url = data["avatar_url"].as_str().map(|s| s.to_string());

        Ok(ProviderUserInfo {
            id,
            email,
            name,
            username,
            avatar_url,
            verified_email: None, // GitHub email verification is separate
            raw_data: data,
        })
    }

    fn parse_discord_user_info(&self, data: Value) -> Result<ProviderUserInfo> {
        let id = data["id"]
            .as_str()
            .ok_or_else(|| anyhow!("Missing Discord user ID"))?
            .to_string();

        let email = data["email"].as_str().map(|s| s.to_string());
        let username = data["username"].as_str().map(|s| s.to_string());
        let discriminator = data["discriminator"].as_str().unwrap_or("0000");

        // Discord name is username#discriminator
        let name = username.as_ref().map(|u| format!("{}#{}", u, discriminator));

        let avatar_url = data["avatar"].as_str().map(|avatar_hash| {
            format!(
                "https://cdn.discordapp.com/avatars/{}/{}.png",
                id, avatar_hash
            )
        });

        let verified_email = data["verified"].as_bool();

        Ok(ProviderUserInfo {
            id,
            email,
            name,
            username,
            avatar_url,
            verified_email,
            raw_data: data,
        })
    }

    fn parse_apple_user_info(&self, data: Value) -> Result<ProviderUserInfo> {
        let id = data["sub"]
            .as_str()
            .ok_or_else(|| anyhow!("Missing Apple user ID"))?
            .to_string();

        let email = data["email"].as_str().map(|s| s.to_string());
        let verified_email = data["email_verified"].as_bool();

        // Apple provides minimal user info for privacy
        Ok(ProviderUserInfo {
            id,
            email,
            name: None,
            username: None,
            avatar_url: None,
            verified_email,
            raw_data: data,
        })
    }
}

/// Create default OAuth configurations for development
pub fn create_default_configs() -> Vec<OAuthConfig> {
    vec![
        OAuthConfig {
            provider: OAuthProvider::Google,
            client_id: std::env::var("GOOGLE_CLIENT_ID").unwrap_or_default(),
            client_secret: std::env::var("GOOGLE_CLIENT_SECRET").unwrap_or_default(),
            redirect_uri: std::env::var("GOOGLE_REDIRECT_URI")
                .unwrap_or_else(|_| "http://localhost:3000/auth/google/callback".to_string()),
            scopes: vec!["openid".to_string(), "email".to_string(), "profile".to_string()],
            auth_url: "https://accounts.google.com/o/oauth2/v2/auth".to_string(),
            token_url: "https://oauth2.googleapis.com/token".to_string(),
            user_info_url: "https://openidconnect.googleapis.com/v1/userinfo".to_string(),
        },
        OAuthConfig {
            provider: OAuthProvider::GitHub,
            client_id: std::env::var("GITHUB_CLIENT_ID").unwrap_or_default(),
            client_secret: std::env::var("GITHUB_CLIENT_SECRET").unwrap_or_default(),
            redirect_uri: std::env::var("GITHUB_REDIRECT_URI")
                .unwrap_or_else(|_| "http://localhost:3000/auth/github/callback".to_string()),
            scopes: vec!["user:email".to_string()],
            auth_url: "https://github.com/login/oauth/authorize".to_string(),
            token_url: "https://github.com/login/oauth/access_token".to_string(),
            user_info_url: "https://api.github.com/user".to_string(),
        },
        OAuthConfig {
            provider: OAuthProvider::Discord,
            client_id: std::env::var("DISCORD_CLIENT_ID").unwrap_or_default(),
            client_secret: std::env::var("DISCORD_CLIENT_SECRET").unwrap_or_default(),
            redirect_uri: std::env::var("DISCORD_REDIRECT_URI")
                .unwrap_or_else(|_| "http://localhost:3000/auth/discord/callback".to_string()),
            scopes: vec!["identify".to_string(), "email".to_string()],
            auth_url: "https://discord.com/api/oauth2/authorize".to_string(),
            token_url: "https://discord.com/api/oauth2/token".to_string(),
            user_info_url: "https://discord.com/api/users/@me".to_string(),
        },
        OAuthConfig {
            provider: OAuthProvider::Apple,
            client_id: std::env::var("APPLE_CLIENT_ID").unwrap_or_default(),
            client_secret: std::env::var("APPLE_CLIENT_SECRET").unwrap_or_default(),
            redirect_uri: std::env::var("APPLE_REDIRECT_URI")
                .unwrap_or_else(|_| "http://localhost:3000/auth/apple/callback".to_string()),
            scopes: vec!["email".to_string(), "name".to_string()],
            auth_url: "https://appleid.apple.com/auth/authorize".to_string(),
            token_url: "https://appleid.apple.com/auth/token".to_string(),
            user_info_url: "https://appleid.apple.com/auth/userinfo".to_string(),
        },
    ]
}

/// Get scopes for OAuth provider
pub fn get_provider_scopes(provider: &OAuthProvider) -> Vec<Scope> {
    match provider {
        OAuthProvider::Google => vec![
            Scope::new("openid".to_string()),
            Scope::new("email".to_string()),
            Scope::new("profile".to_string()),
        ],
        OAuthProvider::GitHub => vec![Scope::new("user:email".to_string())],
        OAuthProvider::Discord => vec![
            Scope::new("identify".to_string()),
            Scope::new("email".to_string()),
        ],
        OAuthProvider::Apple => vec![
            Scope::new("email".to_string()),
            Scope::new("name".to_string()),
        ],
    }
}