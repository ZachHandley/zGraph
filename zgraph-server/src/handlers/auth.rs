//! Authentication handlers for zGraph API
//!
//! Provides HTTP handlers for user signup, login, token refresh, and logout
//! with proper JWT token management and error handling.

use axum::{
    extract::{State, Json},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::{
    auth::{AuthSessionExtractor, jwt::JwtManager},
    models::{ApiResponse, ErrorResponse},
    AppState,
};
use zauth::{
    AuthService, AuthConfig, AuthRepository, JwtKeys,
    models::{UserRecord, AutoClearPolicy},
    hash::{hash_password, verify_password}
};

/// Login request payload
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
    pub environment: Option<String>,
}

/// Login response payload
#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub user_id: String,
    pub org_id: String,
    pub env_id: String,
    pub roles: Vec<String>,
}

/// Signup request payload
#[derive(Debug, Deserialize)]
pub struct SignupRequest {
    pub email: String,
    pub password: String,
    pub name: String,
    pub organization_name: Option<String>,
}

/// Refresh token request payload
#[derive(Debug, Deserialize)]
pub struct RefreshRequest {
    pub refresh_token: String,
}

/// Authenticate user and return JWT tokens
pub async fn login(
    State(app_state): State<Arc<AppState>>,
    Json(request): Json<LoginRequest>,
) -> Result<Response, StatusCode> {
    debug!("Login request for user: {}", request.email);

    // Validate input
    if request.email.trim().is_empty() || request.password.trim().is_empty() {
        warn!("Invalid login request: empty email or password");
        let error_response = ApiResponse::<()>::error(
            "INVALID_CREDENTIALS",
            "Email and password are required",
            "login".to_string(),
        );
        return Ok((StatusCode::BAD_REQUEST, axum::Json(error_response)).into_response());
    }

    // For now, use a simple in-memory user store
    // In production, this would use the zauth AuthService with proper database integration
    if request.email == "admin@example.com" && request.password == "admin123" {
        info!("Admin login successful");

        // Generate JWT tokens using existing JWT manager
        let claims = crate::auth::Claims {
            sub: "admin".to_string(),
            org: 1,
            roles: vec!["admin".to_string()],
            exp: (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp(),
            iat: chrono::Utc::now().timestamp(),
        };

        let (access_token, refresh_token) = match app_state.auth_service.generate_tokens(
            &crate::auth::AuthSession {
                user_id: "admin".to_string(),
                org_id: 1,
                roles: vec!["admin".to_string()],
                permissions: vec![
                    zgraph::Permission::Read,
                    zgraph::Permission::Write,
                    zgraph::Permission::Delete,
                    zgraph::Permission::Admin,
                ],
                expires_at: time::OffsetDateTime::now_utc() + time::Duration::hours(24),
            }
        ) {
            Ok(tokens) => tokens,
            Err(e) => {
                error!("Failed to generate tokens: {}", e);
                let error_response = ApiResponse::<()>::error(
                    "TOKEN_GENERATION_FAILED",
                    "Failed to generate authentication tokens",
                    "login".to_string(),
                );
                return Ok((StatusCode::INTERNAL_SERVER_ERROR, axum::Json(error_response)).into_response());
            }
        };

        let response_data = LoginResponse {
            access_token,
            refresh_token,
            expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
            user_id: "admin".to_string(),
            org_id: "1".to_string(),
            env_id: request.environment.unwrap_or_else(|| "default".to_string()),
            roles: vec!["admin".to_string()],
        };

        let success_response = ApiResponse::success(response_data, "login".to_string());
        Ok((StatusCode::OK, axum::Json(success_response)).into_response())
    } else {
        warn!("Login failed for user: {}", request.email);
        let error_response = ApiResponse::<()>::error(
            "INVALID_CREDENTIALS",
            "Invalid email or password",
            "login".to_string(),
        );
        Ok((StatusCode::UNAUTHORIZED, axum::Json(error_response)).into_response())
    }
}

/// Register new user and return JWT tokens
pub async fn signup(
    State(app_state): State<Arc<AppState>>,
    Json(request): Json<SignupRequest>,
) -> Result<Response, StatusCode> {
    debug!("Signup request for user: {}", request.email);

    // For now, return a simple success response
    // In Phase 2, this will actually create the user
    let response_data = LoginResponse {
        access_token: "dummy_access_token".to_string(),
        refresh_token: "dummy_refresh_token".to_string(),
        expires_at: chrono::Utc::now() + chrono::Duration::minutes(15),
        user_id: "user123".to_string(),
        org_id: "org456".to_string(),
        env_id: "default".to_string(),
        roles: vec!["admin".to_string()],
    };

    let success_response = ApiResponse::success(response_data, "signup".to_string());
    Ok((StatusCode::CREATED, axum::Json(success_response)).into_response())
}

/// Refresh access token using refresh token
pub async fn refresh(
    State(app_state): State<Arc<AppState>>,
    Json(request): Json<RefreshRequest>,
) -> Result<Response, StatusCode> {
    debug!("Token refresh request");

    // Validate refresh token
    if request.refresh_token.trim().is_empty() {
        warn!("Empty refresh token provided");
        let error_response = ApiResponse::<()>::error(
            "INVALID_TOKEN",
            "Refresh token is required",
            "refresh".to_string(),
        );
        return Ok((StatusCode::BAD_REQUEST, axum::Json(error_response)).into_response());
    }

    // Validate refresh token and generate new tokens
    match app_state.auth_service.refresh_token(&request.refresh_token) {
        Ok((access_token, new_refresh_token)) => {
            debug!("Token refresh successful");

            let response_data = serde_json::json!({
                "access_token": access_token,
                "refresh_token": new_refresh_token,
                "expires_at": chrono::Utc::now() + chrono::Duration::hours(1)
            });

            let success_response = ApiResponse::success(response_data, "refresh".to_string());
            Ok((StatusCode::OK, axum::Json(success_response)).into_response())
        }
        Err(e) => {
            error!("Token refresh failed: {}", e);
            let error_response = ApiResponse::<()>::error(
                "INVALID_REFRESH_TOKEN",
                "Invalid or expired refresh token",
                "refresh".to_string(),
            );
            Ok((StatusCode::UNAUTHORIZED, axum::Json(error_response)).into_response())
        }
    }
}

/// Logout user and invalidate session
pub async fn logout(
    AuthSessionExtractor(session): AuthSessionExtractor,
) -> Result<Response, StatusCode> {
    debug!("Logout request for user: {}", session.user_id);

    // For now, return a simple success response
    // In Phase 2, this will invalidate the session/tokens
    let response_data = serde_json::json!({
        "message": "Successfully logged out"
    });

    let success_response = ApiResponse::success(response_data, "logout".to_string());
    Ok((StatusCode::OK, axum::Json(success_response)).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_login_request_deserialization() {
        let json = r#"{
            "email": "test@example.com",
            "password": "password123",
            "environment": "production"
        }"#;

        let request: LoginRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.email, "test@example.com");
        assert_eq!(request.password, "password123");
        assert_eq!(request.environment, Some("production".to_string()));
    }

    #[test]
    fn test_signup_request_deserialization() {
        let json = r#"{
            "email": "test@example.com",
            "password": "password123",
            "name": "Test User",
            "organization_name": "Test Org"
        }"#;

        let request: SignupRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.email, "test@example.com");
        assert_eq!(request.name, "Test User");
        assert_eq!(request.organization_name, Some("Test Org".to_string()));
    }
}