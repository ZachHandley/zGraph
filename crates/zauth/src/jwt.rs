use anyhow::{anyhow, Result};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessClaims {
    pub sub: String,
    pub org: u64,
    pub env: String,
    pub roles: Vec<String>,
    pub labels: Vec<String>,
    pub sid: String,
    pub iat: i64,
    pub exp: i64,
    #[serde(default = "access_token_type")]
    pub typ: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshClaims {
    pub sub: String,
    pub org: u64,
    pub sid: String,
    pub env: String,
    pub iat: i64,
    pub exp: i64,
    #[serde(default = "refresh_token_type")]
    pub typ: String,
}

fn access_token_type() -> String {
    "access".into()
}

fn refresh_token_type() -> String {
    "refresh".into()
}

pub struct JwtKeys {
    access: EncodingKey,
    access_dec: DecodingKey,
    refresh: EncodingKey,
    refresh_dec: DecodingKey,
}

impl JwtKeys {
    pub fn new(access_secret: &str, refresh_secret: &str) -> Self {
        Self {
            access: EncodingKey::from_secret(access_secret.as_bytes()),
            access_dec: DecodingKey::from_secret(access_secret.as_bytes()),
            refresh: EncodingKey::from_secret(refresh_secret.as_bytes()),
            refresh_dec: DecodingKey::from_secret(refresh_secret.as_bytes()),
        }
    }

    pub fn encode_access(&self, claims: &AccessClaims) -> Result<String> {
        jsonwebtoken::encode(&Header::new(Algorithm::HS512), claims, &self.access)
            .map_err(|e| anyhow!("encode access token failed: {e}"))
    }

    pub fn encode_refresh(&self, claims: &RefreshClaims) -> Result<String> {
        jsonwebtoken::encode(&Header::new(Algorithm::HS512), claims, &self.refresh)
            .map_err(|e| anyhow!("encode refresh token failed: {e}"))
    }

    pub fn decode_access(&self, token: &str) -> Result<AccessClaims> {
        let mut validation = Validation::new(Algorithm::HS512);
        validation.validate_exp = true;
        validation.required_spec_claims.insert("typ".into());
        let data = jsonwebtoken::decode::<AccessClaims>(token, &self.access_dec, &validation)
            .map_err(|e| anyhow!("invalid access token: {e}"))?;
        if data.claims.typ != "access" {
            return Err(anyhow!("token typ mismatch"));
        }
        Ok(data.claims)
    }

    pub fn decode_refresh(&self, token: &str) -> Result<RefreshClaims> {
        let mut validation = Validation::new(Algorithm::HS512);
        validation.validate_exp = true;
        validation.required_spec_claims.insert("typ".into());
        let data = jsonwebtoken::decode::<RefreshClaims>(token, &self.refresh_dec, &validation)
            .map_err(|e| anyhow!("invalid refresh token: {e}"))?;
        if data.claims.typ != "refresh" {
            return Err(anyhow!("token typ mismatch"));
        }
        Ok(data.claims)
    }
}

pub fn new_session_id() -> Ulid {
    Ulid::new()
}
