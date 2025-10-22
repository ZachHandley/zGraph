//! Authentication middleware and JWT token management for zGraph
//!
//! Provides JWT-based authentication with organization-level isolation,
//! user management, and session handling.


pub mod jwt;

use anyhow::Result;
use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Json, Response},
};
use base64::engine::general_purpose::STANDARD as STANDARD;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::sync::Arc;
use time::{OffsetDateTime, Utc};
use tower::ServiceExt;
use tracing::{debug, info, warn};

use zgraph::{self as zg, Permission};
use zcore_storage as zs;

/// Authentication configuration
#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub jwt_secret: String,
    pub org_id: u64,
}

/// Authentication service
#[derive(Clone)]
pub struct AuthService {
    config: AuthConfig,
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
}

/// User session information
#[derive(Debug, Clone, Serialize)]
pub struct AuthSession {
    pub user_id: String,
    pub org_id: u64,
    pub roles: Vec<String>,
    pub permissions: Vec<Permission>,
    pub expires_at: OffsetDateTime,
}

/// Login request
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

/// Refresh token request
#[derive(Debug, Deserialize)]
pub struct RefreshRequest {
    pub refresh_token: String,
}

/// Logout request
#[derive(Debug, Deserialize)]
pub struct LogoutRequest {
    pub refresh_token: String,
}

/// Login response
#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: i64,
    pub environment: String,
}

/// JWT claims structure
#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,      // User ID
    pub org: u64,         // Organization ID
    pub roles: Vec<String>, // User roles
    pub exp: i64,         // Expiration time
    pub iat: i64,         // Issued at time
}

impl AuthService {
    pub fn new(config: AuthConfig) -> Result<Self> {
        // Create encoding key from secret
        let encoding_key = EncodingKey::from_secret(config.jwt_secret.as_bytes());

        // Create decoding key
        let decoding_key = DecodingKey::from_secret(config.jwt_secret.as_bytes());

        Ok(Self {
            config,
            encoding_key,
            decoding_key,
        })
    }

    pub fn authenticate_user(&self, email: &str, password: &str) -> Result<Option<AuthSession>> {
        // For now, accept any email/password as valid user
        // In production, this would verify against user database
        if email.is_empty() || password.is_empty() {
            return Ok(None);
        }

        // Create mock user session
        let session = AuthSession {
            user_id: email.to_string(),
            org_id: self.config.org_id,
            roles: vec!["user".to_string()],
            permissions: vec![
                Permission::Read,
                Permission::Write,
            ],
            expires_at: Utc::now() + time::Duration::hours(24),
        };

        Ok(Some(session))
    }

    pub fn generate_tokens(&self, session: &AuthSession) -> Result<(String, String)> {
        let now = Utc::now();
        let exp = now + time::Duration::hours(1);

        let claims = Claims {
            sub: session.user_id.clone(),
            org: session.org_id,
            roles: session.roles.clone(),
            exp: exp.unix_timestamp(),
            iat: now.unix_timestamp(),
        };

        // Create access token (1 hour expiry)
        let access_token = encode(
            &Header::default(),
            &self.encoding_key,
            &claims,
        )?;

        // Create refresh token (24 hour expiry)
        let refresh_claims = Claims {
            sub: session.user_id.clone(),
            org: session.org_id,
            roles: session.roles,
            exp: (now + time::Duration::hours(24)).unix_timestamp(),
            iat: now.unix_timestamp(),
        };

        let refresh_token = encode(
            &Header::default(),
            &self.encoding_key,
            &refresh_claims,
        )?;

        Ok((access_token, refresh_token))
    }

    pub fn validate_token(&self, token: &str) -> Result<AuthSession> {
        let token_data = decode::<Claims>(
            token,
            &self.decoding_key,
            &Validation::new(Algorithm::HS256),
        );

        match token_data {
            Ok(data) => {
                let claims = data.claims;
                let now = Utc::now();

                // Check if token is expired
                if claims.exp < now.unix_timestamp() {
                    return Err(anyhow::anyhow!("Token expired"));
                }

                let session = AuthSession {
                    user_id: claims.sub,
                    org_id: claims.org,
                    roles: claims.roles,
                    permissions: self.get_permissions_for_roles(&claims.roles),
                    expires_at: OffsetDateTime::from_unix_timestamp(claims.exp)?,
                };

                debug!("Validated token for user: {} in org: {}", claims.sub, claims.org);
                Ok(session)
            }
            Err(err) => {
                warn!("Token validation failed: {}", err);
                Err(anyhow::anyhow!("Invalid token: {}", err))
            }
        }
    }

    pub fn refresh_token(&self, refresh_token: &str) -> Result<(String, String)> {
        let token_data = decode::<Claims>(
            refresh_token,
            &self.decoding_key,
            &Validation::new(Algorithm::HS256),
        );

        match token_data {
            Ok(data) => {
                let claims = data.claims;
                let now = Utc::now();

                // Check if refresh token is expired
                if claims.exp < now.unix_timestamp() {
                    return Err(anyhow::anyhow!("Refresh token expired"));
                }

                let session = AuthSession {
                    user_id: claims.sub,
                    org_id: claims.org,
                    roles: claims.roles,
                    permissions: self.get_permissions_for_roles(&claims.roles),
                    expires_at: OffsetDateTime::from_unix_timestamp(claims.exp)?,
                };

                self.generate_tokens(&session)
            }
            Err(err) => {
                Err(anyhow::anyhow!("Invalid refresh token: {}", err))
            }
        }
    }

    fn get_permissions_for_roles(&self, roles: &[String]) -> Vec<Permission> {
        let mut permissions = Vec::new();

        for role in roles {
            match role.as_str() {
                "admin" => {
                    permissions.extend_from_slice(&[
                        Permission::Read,
                        Permission::Write,
                        Permission::Delete,
                        Permission::Admin,
                    ]);
                }
                "user" => {
                    permissions.extend_from_slice(&[
                        Permission::Read,
                        Permission::Write,
                    ]);
                }
                "readonly" => {
                    permissions.push(Permission::Read);
                }
                _ => {}
            }
        }

        // Remove duplicates
        permissions.sort();
        permissions.dedup();
        permissions
    }
}

/// Authentication middleware
pub fn auth_layer<B>(
    State(app_state): State<Arc<crate::AppState>>,
) -> impl Fn(Request<B>, Next<B>) + Clone {
    move |request: Request<B>, next: Next<B>| {
        let app_state = request.extract::<State<Arc<crate::AppState>>>().unwrap();
        let auth_service = &app_state.auth_service;

        // Skip auth for health checks
        if request.uri().path().starts_with("/health") ||
           request.uri().path().starts_with("/ready") ||
           request.uri().path().starts_with("/static") {
            return next.run(request);
        }

        // Extract token from Authorization header
        let headers = request.headers();
        let auth_header = headers.get("authorization");

        if let Some(auth_header) = auth_header {
            if let Ok(auth_str) = auth_header.to_str() {
                if let Some(token) = auth_str.strip_prefix("Bearer ") {
                    match auth_service.validate_token(token) {
                        Ok(session) => {
                            debug!("Successfully authenticated user: {} in org: {}",
                                   session.user_id, session.org_id);

                            // Add session to request extensions
                            let mut request_with_session = request;
                            request_with_session.extensions_mut().insert(session);

                            next.run(request_with_session)
                        }
                        Err(_) => {
                            warn!("Invalid token provided");
                            let response = axum::Json(serde_json::json!({
                                "error": "INVALID_TOKEN",
                                "message": "Invalid or expired authentication token"
                            }));

                            return (StatusCode::UNAUTHORIZED, response).into_response();
                        }
                    }
                }
            }
        }

        // No valid token found
        warn!("No authentication token provided");
        let response = axum::Json(serde_json::json!({
            "error": "MISSING_TOKEN",
            "message": "Authentication token required"
        }));

        (StatusCode::UNAUTHORIZED, response).into_response()
    }
}

/// Extract authentication session from request
pub struct AuthSessionExtractor;

impl<B> axum::extract::FromRequestParts<B> for AuthSessionExtractor {
    type Rejection = StatusCode;

    fn from_request_parts(
        _parts: &axum::http::request::Parts,
        body: B,
        request: &axum::http::Request<B>,
    ) -> Result<Self, Self::Rejection> {
        let extensions = request.extensions();

        match extensions.get::<AuthSession>() {
            Some(session) => Ok(session),
            None => Err(StatusCode::UNAUTHORIZED),
        }
    }
}

/// Extract user ID from request
pub struct UserIdExtractor;

impl<B> axum::extract::FromRequestParts<B> for UserIdExtractor {
    type Rejection = StatusCode;

    fn from_request_parts(
        _parts: &axum::http::request::Parts,
        _body: B,
        request: &axum::http::Request<B>,
    ) -> Result<Self, Self::Rejection> {
        let extensions = request.extensions();

        match extensions.get::<AuthSession>() {
            Some(session) => Ok(session.user_id.clone()),
            None => Err(StatusCode::UNAUTHORIZED),
        }
    }
}

/// Extract organization ID from request
pub struct OrgIdExtractor;

impl<B> axum::extract::FromRequestParts<B> for OrgIdExtractor {
    type Rejection = StatusCode;

    fn from_request_parts(
        _parts: &axum::http::request::Parts,
        _body: B,
        request: &axum::http::Request<B>,
    ) -> Result<Self, Self::Rejection> {
        let extensions = request.extensions();

        match extensions.get::<AuthSession>() {
            Some(session) => Ok(session.org_id),
            None => Err(StatusCode::UNAUTHORIZED),
        }
    }
}