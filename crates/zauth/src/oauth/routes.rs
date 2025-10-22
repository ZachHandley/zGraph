use axum::{
    routing::{get, post},
    Router,
};

use super::handlers::*;
use crate::AuthState;

/// Create OAuth routes for integration with Axum applications
pub fn oauth_routes() -> Router<AuthState> {
    Router::new()
        // OAuth authorization endpoints
        .route("/oauth/:provider/authorize", get(oauth_authorize))
        .route("/oauth/:provider/callback", get(oauth_callback))
        .route("/oauth/:provider/redirect", get(oauth_redirect))

        // User profile management
        .route("/profile", get(get_user_profile))

        // Account linking/unlinking
        .route("/oauth/unlink", post(unlink_oauth_account))
}

/// OAuth configuration helper for setting up providers
pub struct OAuthRouterConfig;

impl OAuthRouterConfig {
    /// Create OAuth routes with custom configuration
    pub fn with_custom_handlers() -> Router<AuthState> {
        Router::new()
            .route("/oauth/:provider/authorize", get(oauth_authorize))
            .route("/oauth/:provider/callback", get(oauth_callback))
            .route("/oauth/:provider/redirect", get(oauth_redirect))
            .route("/profile", get(get_user_profile))
            .route("/oauth/unlink", post(unlink_oauth_account))
    }

    /// Get list of supported OAuth providers
    pub fn supported_providers() -> Vec<&'static str> {
        vec!["google", "github", "discord", "apple"]
    }

    /// Get provider configuration requirements
    pub fn provider_env_vars(provider: &str) -> Vec<&'static str> {
        match provider {
            "google" => vec![
                "GOOGLE_CLIENT_ID",
                "GOOGLE_CLIENT_SECRET",
                "GOOGLE_REDIRECT_URI"
            ],
            "github" => vec![
                "GITHUB_CLIENT_ID",
                "GITHUB_CLIENT_SECRET",
                "GITHUB_REDIRECT_URI"
            ],
            "discord" => vec![
                "DISCORD_CLIENT_ID",
                "DISCORD_CLIENT_SECRET",
                "DISCORD_REDIRECT_URI"
            ],
            "apple" => vec![
                "APPLE_CLIENT_ID",
                "APPLE_CLIENT_SECRET",
                "APPLE_REDIRECT_URI"
            ],
            _ => vec![]
        }
    }
}