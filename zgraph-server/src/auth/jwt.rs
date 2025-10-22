//! JWT token utilities for zGraph authentication
//!
//! Provides JWT token creation, validation, and management with support for
//! access tokens and refresh tokens.

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc, Duration};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// JWT claims for access tokens
#[derive(Debug, Serialize, Deserialize)]
pub struct AccessClaims {
    pub sub: String,        // User ID
    pub org: String,        // Organization ID
    pub env: String,        // Environment ID
    pub roles: Vec<String>, // User roles
    pub exp: i64,           // Expiration time
    pub iat: i64,           // Issued at
    pub r#type: String,       // Token type
}

/// JWT claims for refresh tokens
#[derive(Debug, Serialize, Deserialize)]
pub struct RefreshClaims {
    pub sub: String,        // User ID
    pub org: String,        // Organization ID
    pub exp: i64,           // Expiration time
    pub iat: i64,           // Issued at
    pub r#type: String,       // Token type
}

/// JWT token manager
pub struct JwtManager {
    access_secret: EncodingKey,
    refresh_secret: EncodingKey,
    access_decoding_key: DecodingKey,
    refresh_decoding_key: DecodingKey,
    access_ttl: Duration,
    refresh_ttl: Duration,
}

impl JwtManager {
    /// Create a new JWT manager with specified secrets and TTL
    pub fn new(
        access_secret: &str,
        refresh_secret: &str,
        access_ttl_minutes: i64,
        refresh_ttl_days: i64,
    ) -> Result<Self> {
        Ok(Self {
            access_secret: EncodingKey::from_secret(access_secret.as_ref()),
            refresh_secret: EncodingKey::from_secret(refresh_secret.as_ref()),
            access_decoding_key: DecodingKey::from_secret(access_secret.as_ref()),
            refresh_decoding_key: DecodingKey::from_secret(refresh_secret.as_ref()),
            access_ttl: Duration::minutes(access_ttl_minutes),
            refresh_ttl: Duration::days(refresh_ttl_days),
        })
    }

    /// Create access and refresh tokens for a user
    pub fn create_token_pair(
        &self,
        user_id: &str,
        org_id: &str,
        env_id: &str,
        roles: Vec<String>,
    ) -> Result<(String, String, DateTime<Utc>)> {
        let now = Utc::now();
        let expires_at = now + self.access_ttl;

        // Create access token
        let access_claims = AccessClaims {
            sub: user_id.to_string(),
            org: org_id.to_string(),
            env: env_id.to_string(),
            roles,
            exp: expires_at.timestamp(),
            iat: now.timestamp(),
            r#type: "access".to_string(),
        };

        let access_token = encode(
            &Header::default(),
            &access_claims,
            &self.access_secret,
        )?;

        // Create refresh token
        let refresh_expires = now + self.refresh_ttl;
        let refresh_claims = RefreshClaims {
            sub: user_id.to_string(),
            org: org_id.to_string(),
            exp: refresh_expires.timestamp(),
            iat: now.timestamp(),
            r#type: "refresh".to_string(),
        };

        let refresh_token = encode(
            &Header::default(),
            &refresh_claims,
            &self.refresh_secret,
        )?;

        Ok((access_token, refresh_token, expires_at))
    }

    /// Validate an access token and return claims
    pub fn validate_access_token(&self, token: &str) -> Result<AccessClaims> {
        let validation = Validation::new(Algorithm::HS256);
        let token_data = decode::<AccessClaims>(token, &self.access_decoding_key, &validation)?;

        if token_data.claims.r#type != "access" {
            return Err(anyhow!("Invalid token type"));
        }

        Ok(token_data.claims)
    }

    /// Validate a refresh token and return claims
    pub fn validate_refresh_token(&self, token: &str) -> Result<RefreshClaims> {
        let validation = Validation::new(Algorithm::HS256);
        let token_data = decode::<RefreshClaims>(token, &self.refresh_decoding_key, &validation)?;

        if token_data.claims.r#type != "refresh" {
            return Err(anyhow!("Invalid token type"));
        }

        Ok(token_data.claims)
    }

    /// Check if a token is expired
    pub fn is_token_expired(&self, claims: impl TokenClaims) -> bool {
        Utc::now().timestamp() > claims.exp()
    }
}

/// Trait for token claims with expiration
pub trait TokenClaims {
    fn exp(&self) -> i64;
}

impl TokenClaims for AccessClaims {
    fn exp(&self) -> i64 {
        self.exp
    }
}

impl TokenClaims for RefreshClaims {
    fn exp(&self) -> i64 {
        self.exp
    }
}

/// Authentication session extracted from valid JWT
#[derive(Debug, Clone)]
pub struct AuthSession {
    pub user_id: String,
    pub org_id: String,
    pub env_id: String,
    pub roles: Vec<String>,
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: DateTime<Utc>,
}

impl AuthSession {
    /// Create a new auth session from token pair
    pub fn new(
        user_id: String,
        org_id: String,
        env_id: String,
        roles: Vec<String>,
        access_token: String,
        refresh_token: String,
        expires_at: DateTime<Utc>,
    ) -> Self {
        Self {
            user_id,
            org_id,
            env_id,
            roles,
            access_token,
            refresh_token,
            expires_at,
        }
    }

    /// Check if the user has a specific role
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.contains(&role.to_string())
    }

    /// Check if the user has any of the specified roles
    pub fn has_any_role(&self, roles: &[&str]) -> bool {
        let role_set: HashSet<&str> = self.roles.iter().map(|r| r.as_str()).collect();
        roles.iter().any(|role| role_set.contains(*role))
    }

    /// Check if the session is expired
    pub fn is_expired(&self) -> bool {
        Utc::now() > self.expires_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_creation_and_validation() -> Result<()> {
        let jwt_manager = JwtManager::new(
            "test_access_secret",
            "test_refresh_secret",
            15,
            7,
        )?;

        let user_id = "user123";
        let org_id = "org456";
        let env_id = "env789";
        let roles = vec!["admin".to_string(), "user".to_string()];

        let (access_token, refresh_token, expires_at) = jwt_manager.create_token_pair(
            user_id,
            org_id,
            env_id,
            roles.clone(),
        )?;

        // Validate access token
        let access_claims = jwt_manager.validate_access_token(&access_token)?;
        assert_eq!(access_claims.sub, user_id);
        assert_eq!(access_claims.org, org_id);
        assert_eq!(access_claims.env, env_id);
        assert_eq!(access_claims.roles, roles);
        assert_eq!(access_claims.r#type, "access");

        // Validate refresh token
        let refresh_claims = jwt_manager.validate_refresh_token(&refresh_token)?;
        assert_eq!(refresh_claims.sub, user_id);
        assert_eq!(refresh_claims.org, org_id);
        assert_eq!(refresh_claims.r#type, "refresh");

        // Check expiration
        assert!(!jwt_manager.is_token_expired(&access_claims));

        Ok(())
    }
}