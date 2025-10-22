use super::{models::*, service::OAuthService, OAuthError};
use crate::{AuthSession, AuthState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// OAuth authorization initiation request
#[derive(Debug, Deserialize)]
pub struct OAuthAuthRequest {
    pub provider: String,
    pub env_slug: Option<String>,
    #[serde(default)]
    pub link_account: bool,
}

/// OAuth callback query parameters
#[derive(Debug, Deserialize)]
pub struct OAuthCallbackParams {
    pub code: Option<String>,
    pub state: Option<String>,
    pub error: Option<String>,
    pub error_description: Option<String>,
    pub session_id: Option<String>,
}

/// OAuth authorization response
#[derive(Debug, Serialize)]
pub struct OAuthAuthResponse {
    pub authorization_url: String,
    pub state: String,
    pub session_id: String,
}

/// OAuth callback success response
#[derive(Debug, Serialize)]
pub struct OAuthCallbackResponse {
    pub success: bool,
    pub user_id: String,
    pub is_new_user: bool,
    pub is_linked_account: bool,
    pub access_token: String,
    pub refresh_token: String,
    pub access_expires_at: i64,
    pub refresh_expires_at: i64,
    pub profile: OAuthProfileSummary,
}

/// OAuth profile summary for response
#[derive(Debug, Serialize)]
pub struct OAuthProfileSummary {
    pub id: String,
    pub provider: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub avatar_url: Option<String>,
    pub username: Option<String>,
}

/// OAuth error response
#[derive(Debug, Serialize)]
pub struct OAuthErrorResponse {
    pub error: String,
    pub description: Option<String>,
}

/// User profile response
#[derive(Debug, Serialize)]
pub struct UserProfileResponse {
    pub user_id: String,
    pub email: String,
    pub primary_name: String,
    pub primary_avatar: Option<String>,
    pub oauth_profiles: Vec<OAuthProfileSummary>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Account linking request
#[derive(Debug, Deserialize)]
pub struct LinkAccountRequest {
    pub provider: String,
}

/// Account unlinking request
#[derive(Debug, Deserialize)]
pub struct UnlinkAccountRequest {
    pub provider: String,
}

/// Initiate OAuth authorization flow
/// GET /auth/oauth/:provider/authorize
pub async fn oauth_authorize(
    State(auth_state): State<AuthState>,
    Path(provider_str): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    auth_session: Option<AuthSession>,
) -> Response {
    let provider = match provider_str.parse::<OAuthProvider>() {
        Ok(p) => p,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(OAuthErrorResponse {
                    error: "invalid_provider".to_string(),
                    description: Some(format!("Unsupported provider: {}", provider_str)),
                }),
            )
                .into_response();
        }
    };

    let org_id = if let Some(session) = &auth_session {
        session.org_id
    } else {
        // For OAuth login (not account linking), we need to determine org_id
        // This could come from subdomain, query param, or default org
        params
            .get("org_id")
            .and_then(|s| s.parse().ok())
            .unwrap_or(1) // Default org for demo
    };

    let env_slug = params.get("env_slug").cloned();
    let linking_user_id = auth_session.as_ref().and_then(|s| s.user_id);

    let oauth_service = OAuthService::new(auth_state.store, auth_state.config.clone());

    match oauth_service
        .initiate_oauth_flow(provider, org_id, env_slug, linking_user_id)
        .await
    {
        Ok(flow_initiation) => {
            let response = OAuthAuthResponse {
                authorization_url: flow_initiation.authorization_url,
                state: flow_initiation.state,
                session_id: flow_initiation.session_id,
            };
            Json(response).into_response()
        }
        Err(e) => {
            let error_response = OAuthErrorResponse {
                error: "authorization_failed".to_string(),
                description: Some(e.to_string()),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    }
}

/// Handle OAuth callback
/// GET /auth/oauth/:provider/callback
pub async fn oauth_callback(
    State(auth_state): State<AuthState>,
    Path(_provider_str): Path<String>,
    Query(params): Query<OAuthCallbackParams>,
) -> Response {
    // Check for OAuth errors first
    if let Some(error) = params.error {
        let error_response = OAuthErrorResponse {
            error,
            description: params.error_description,
        };
        return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
    }

    let code = match params.code {
        Some(c) => c,
        None => {
            let error_response = OAuthErrorResponse {
                error: "missing_code".to_string(),
                description: Some("Authorization code is required".to_string()),
            };
            return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
        }
    };

    let state = match params.state {
        Some(s) => s,
        None => {
            let error_response = OAuthErrorResponse {
                error: "missing_state".to_string(),
                description: Some("State parameter is required".to_string()),
            };
            return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
        }
    };

    let oauth_service = OAuthService::new(auth_state.store, auth_state.config.clone());

    match oauth_service
        .handle_oauth_callback(code, state, params.session_id)
        .await
    {
        Ok(login_result) => {
            let profile_summary = OAuthProfileSummary {
                id: login_result.oauth_profile.id,
                provider: login_result.oauth_profile.provider.as_str().to_string(),
                email: login_result.oauth_profile.email,
                name: login_result.oauth_profile.name,
                avatar_url: login_result.oauth_profile.avatar_url,
                username: login_result.oauth_profile.username,
            };

            let response = OAuthCallbackResponse {
                success: true,
                user_id: login_result.user_id.to_string(),
                is_new_user: login_result.is_new_user,
                is_linked_account: login_result.is_linked_account,
                access_token: login_result.access_token,
                refresh_token: login_result.refresh_token,
                access_expires_at: login_result.access_expires_at,
                refresh_expires_at: login_result.refresh_expires_at,
                profile: profile_summary,
            };

            Json(response).into_response()
        }
        Err(e) => {
            let status = match e {
                OAuthError::InvalidState | OAuthError::StateExpired => StatusCode::BAD_REQUEST,
                OAuthError::EmailConflict | OAuthError::AccountAlreadyLinked => {
                    StatusCode::CONFLICT
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };

            let error_response = OAuthErrorResponse {
                error: "callback_failed".to_string(),
                description: Some(e.to_string()),
            };

            (status, Json(error_response)).into_response()
        }
    }
}

/// Get unified user profile
/// GET /auth/profile
pub async fn get_user_profile(
    State(auth_state): State<AuthState>,
    auth_session: AuthSession,
) -> Response {
    let user_id = match auth_session.user_id {
        Some(id) => id,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(OAuthErrorResponse {
                    error: "no_user_session".to_string(),
                    description: Some("User session required".to_string()),
                }),
            )
                .into_response();
        }
    };

    let oauth_service = OAuthService::new(auth_state.store, auth_state.config.clone());

    match oauth_service.get_unified_user_profile(auth_session.org_id, &user_id) {
        Ok(Some(profile)) => {
            let oauth_profiles: Vec<OAuthProfileSummary> = profile
                .oauth_profiles
                .into_iter()
                .map(|p| OAuthProfileSummary {
                    id: p.id,
                    provider: p.provider.as_str().to_string(),
                    email: p.email,
                    name: p.name,
                    avatar_url: p.avatar_url,
                    username: p.username,
                })
                .collect();

            let response = UserProfileResponse {
                user_id: profile.user_id.to_string(),
                email: profile.email,
                primary_name: profile.primary_name,
                primary_avatar: profile.primary_avatar,
                oauth_profiles,
                created_at: profile.created_at,
                updated_at: profile.updated_at,
            };

            Json(response).into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND).into_response(),
        Err(e) => {
            let error_response = OAuthErrorResponse {
                error: "profile_fetch_failed".to_string(),
                description: Some(e.to_string()),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    }
}

/// Unlink OAuth account
/// POST /auth/oauth/unlink
pub async fn unlink_oauth_account(
    State(auth_state): State<AuthState>,
    auth_session: AuthSession,
    Json(request): Json<UnlinkAccountRequest>,
) -> Response {
    let user_id = match auth_session.user_id {
        Some(id) => id,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(OAuthErrorResponse {
                    error: "no_user_session".to_string(),
                    description: Some("User session required".to_string()),
                }),
            )
                .into_response();
        }
    };

    let provider = match request.provider.parse::<OAuthProvider>() {
        Ok(p) => p,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(OAuthErrorResponse {
                    error: "invalid_provider".to_string(),
                    description: Some(format!("Unsupported provider: {}", request.provider)),
                }),
            )
                .into_response();
        }
    };

    let oauth_service = OAuthService::new(auth_state.store, auth_state.config.clone());

    match oauth_service.unlink_oauth_account(auth_session.org_id, &user_id, &provider) {
        Ok(()) => {
            #[derive(Serialize)]
            struct UnlinkResponse {
                success: bool,
                message: String,
            }

            let response = UnlinkResponse {
                success: true,
                message: format!("{} account unlinked successfully", provider.as_str()),
            };

            Json(response).into_response()
        }
        Err(e) => {
            let error_response = OAuthErrorResponse {
                error: "unlink_failed".to_string(),
                description: Some(e.to_string()),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    }
}

/// OAuth redirect handler for frontend integration
/// GET /auth/oauth/:provider/redirect
pub async fn oauth_redirect(
    State(auth_state): State<AuthState>,
    Path(_provider_str): Path<String>,
    Query(params): Query<OAuthCallbackParams>,
) -> Response {
    // This is an alternative callback handler that redirects to frontend
    // instead of returning JSON

    let frontend_url = std::env::var("FRONTEND_URL")
        .unwrap_or_else(|_| "http://localhost:3000".to_string());

    // Check for OAuth errors
    if let Some(error) = params.error {
        let redirect_url = format!(
            "{}/auth/callback?error={}&description={}",
            frontend_url,
            error,
            params.error_description.unwrap_or_default()
        );
        return Redirect::to(&redirect_url).into_response();
    }

    // Validate required parameters
    let code = match params.code {
        Some(c) => c,
        None => {
            let redirect_url = format!(
                "{}/auth/callback?error=missing_code&description=Authorization code is required",
                frontend_url
            );
            return Redirect::to(&redirect_url).into_response();
        }
    };

    let state = match params.state {
        Some(s) => s,
        None => {
            let redirect_url = format!(
                "{}/auth/callback?error=missing_state&description=State parameter is required",
                frontend_url
            );
            return Redirect::to(&redirect_url).into_response();
        }
    };

    let oauth_service = OAuthService::new(auth_state.store, auth_state.config.clone());

    match oauth_service
        .handle_oauth_callback(code, state, params.session_id)
        .await
    {
        Ok(login_result) => {
            let redirect_url = format!(
                "{}/auth/callback?success=true&access_token={}&refresh_token={}&user_id={}&is_new_user={}&is_linked_account={}",
                frontend_url,
                login_result.access_token,
                login_result.refresh_token,
                login_result.user_id,
                login_result.is_new_user,
                login_result.is_linked_account
            );
            Redirect::to(&redirect_url).into_response()
        }
        Err(e) => {
            let redirect_url = format!(
                "{}/auth/callback?error=callback_failed&description={}",
                frontend_url,
                urlencoding::encode(&e.to_string())
            );
            Redirect::to(&redirect_url).into_response()
        }
    }
}

impl IntoResponse for OAuthError {
    fn into_response(self) -> Response {
        let status = match self {
            OAuthError::InvalidState | OAuthError::StateExpired => StatusCode::BAD_REQUEST,
            OAuthError::EmailConflict | OAuthError::AccountAlreadyLinked => StatusCode::CONFLICT,
            OAuthError::ConfigError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let error_response = OAuthErrorResponse {
            error: "oauth_error".to_string(),
            description: Some(self.to_string()),
        };

        (status, Json(error_response)).into_response()
    }
}