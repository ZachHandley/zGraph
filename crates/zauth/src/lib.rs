mod hash;
mod jwt;
pub mod models;
mod repository;
pub mod oauth;
pub mod session;
pub mod team;
pub mod integration;
pub mod delegation;
pub mod api;
pub mod two_factor;
pub mod webauthn;
pub mod policy;
pub mod recovery;
pub mod api_2fa;
pub mod advanced_session;
pub mod device_trust;
pub mod session_security;
pub mod advanced_auth;
pub mod biometric;
pub mod observability;
pub mod credential_verification;
pub mod device_attestation;
pub mod credential_middleware;
pub mod identity_permissions;
pub mod hardware_session_context;
mod validation;

#[cfg(test)]
mod compatibility_tests;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use axum::{
    async_trait,
    extract::{FromRef, FromRequestParts},
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Response},
};
use base64::Engine;
use hmac::{digest::KeyInit, Mac};
use jwt::{AccessClaims, JwtKeys, RefreshClaims};
use models::{AutoClearPolicy, EnvironmentRecord, UserRecord, UserStatus};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;
use zcore_storage::Store;
use zcommon::metrics::AuthMetrics;

pub use hash::{hash_password, verify_password};
pub use repository::AuthRepository;
pub use oauth::{OAuthService, OAuthProvider, OAuthError, OAuthFlowInitiation, OAuthLoginResult};
pub use session::SessionService;
pub use team::{TeamService, TeamHierarchy};
pub use integration::{IntegratedAuthService, TeamResourceInfo, TeamActivitySummary, CleanupSummary};
pub use delegation::{DelegationService, DelegationContext, EffectiveRole, DelegationStats};
pub use api::{SessionApi, TeamApi, IntegratedApi, DelegationApi, ApiError};
pub use two_factor::{TwoFactorService, TotpSetup, TwoFactorStatus};
pub use webauthn::{WebAuthnService, WebAuthnRegistrationInfo};
pub use policy::{
    TwoFactorPolicyService, ComplianceStatus, EnforcementAction, PolicyStats,
    NonCompliantUser
};
pub use recovery::{
    RecoveryService, RecoveryMethod, RecoveryVerificationResult,
    RecoveryRequest, TempAccess
};
pub use api_2fa::{
    TwoFactorApi, ApiError as TwoFactorApiError,
    TotpSetupResponse, TotpVerificationResponse, SmsSetupResponse,
    VerificationResponse, BackupCodesResponse, TwoFactorStatusResponse
};
pub use advanced_session::{
    AdvancedSessionManager, DeviceTrustLevel, SessionValidation, SessionPolicies,
    SessionLimits, GeoRestrictions, RateLimits, CleanupSummary as SessionCleanupSummary
};
pub use device_trust::{
    DeviceTrustManager, DeviceRegistration, DeviceVerificationResult, DeviceTrustInfo,
    TrustEvent, DeviceEvent, LocationInfo
};
pub use session_security::{
    SessionSecurityMonitor, SecurityAssessment, RiskLevel, SessionRequest,
    SecurityAnalysis, SecurityEvent, SecurityEventType, SessionFingerprint,
    GeographicRegion, ThreatIntelligence
};
pub use advanced_auth::{
    AdvancedAuthService, EnhancedSecurityInfo,
    SessionValidationResult, ComprehensiveSecurityAnalysis, MaintenanceSummary
};
pub use biometric::{
    CancelableTemplateGenerator, LivenessDetector, BiometricMatchingEngine,
    BiometricKeyDerivationSystem, PrivacyPreservingBiometricSystem,
    MultimodalBiometricSystem, BiometricHSMIntegration, BiometricApi,
    BiometricModality, BiometricTemplate, BiometricChallenge, BiometricResponse,
    LivenessProof, ProtectedTemplate, BiometricKey, HSMProtectedTemplate,
    MultimodalTemplate, MatchingResult, LivenessType, PrivacyLevel,
    SecurityLevel, KeyPurpose, BiometricError
};
pub use credential_verification::{
    CredentialVerificationService, CredentialRegistrationResult, CredentialVerificationResult,
    CredentialEvent
};
pub use device_attestation::{
    DeviceAttestationService, DeviceAttestationResult, HardwareAttestationResult,
    DeviceChallengeResult, AttestationEvent
};
pub use credential_middleware::{
    CredentialVerificationLayer, CredentialVerificationError, VerificationRequirements,
    credential_verification_middleware, create_identity_assertion, CredentialChallengeResponse
};
pub use identity_permissions::{
    IdentityAwarePermissionService, IdentityPermissionError, IdentityAwarePermissionMap,
    IdentityRequirements, SessionTrustLevel, EnhancedPermissionActions
};
pub use hardware_session_context::{
    HardwareSessionContextService, HardwareSessionContext, HardwareSecurityLevel,
    PlatformInfo, PlatformType
};

pub use crate::jwt::new_session_id;

#[derive(Clone)]
pub struct AuthConfig {
    dev_org_id: Option<u64>,
    dev_secret: Option<String>,
    jwt_keys: Arc<JwtKeys>,
    access_ttl_secs: i64,
    refresh_ttl_secs: i64,
}

impl AuthConfig {
    pub fn new(
        dev_org_id: Option<u64>,
        dev_secret: Option<String>,
        access_secret: impl Into<String>,
        refresh_secret: impl Into<String>,
        access_ttl_secs: i64,
        refresh_ttl_secs: i64,
    ) -> Self {
        let access_secret = access_secret.into();
        let refresh_secret = refresh_secret.into();
        let keys = JwtKeys::new(&access_secret, &refresh_secret);
        Self {
            dev_org_id,
            dev_secret,
            jwt_keys: Arc::new(keys),
            access_ttl_secs,
            refresh_ttl_secs,
        }
    }

    pub fn with_dev(org_id: u64, secret: impl Into<String>) -> Self {
        let secret = secret.into();
        Self::new(
            Some(org_id),
            Some(secret.clone()),
            secret.clone(),
            secret,
            3600,
            7 * 24 * 3600,
        )
    }

    pub fn dev_org_id(&self) -> Option<u64> {
        self.dev_org_id
    }

    pub fn dev_secret(&self) -> Option<&String> {
        self.dev_secret.as_ref()
    }

    pub fn jwt_keys(&self) -> Arc<JwtKeys> {
        self.jwt_keys.clone()
    }

    pub fn access_ttl(&self) -> Duration {
        Duration::from_secs(self.access_ttl_secs as u64)
    }

    pub fn refresh_ttl(&self) -> Duration {
        Duration::from_secs(self.refresh_ttl_secs as u64)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DevTokenClaims {
    pub token_id: String,
    pub org_id: u64,
    pub roles: Vec<String>,
    #[serde(default)]
    pub labels: Vec<String>,
    pub exp: i64,
}

#[derive(Error, Debug)]
pub enum AuthError {
    #[error("missing Authorization header")]
    MissingHeader,
    #[error("invalid Authorization header")]
    InvalidHeader,
    #[error("invalid token format")]
    InvalidFormat,
    #[error("invalid signature")]
    InvalidSignature,
    #[error("token expired")]
    Expired,
    #[error("missing secret")]
    MissingSecret,
    #[error("unauthorized")]
    Unauthorized,
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("base64 error: {0}")]
    B64(#[from] base64::DecodeError),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        (StatusCode::UNAUTHORIZED, self.to_string()).into_response()
    }
}

#[derive(Clone)]
pub struct AuthState {
    pub config: AuthConfig,
    pub store: &'static Store,
}

impl FromRef<AuthState> for AuthConfig {
    fn from_ref(state: &AuthState) -> Self {
        state.config.clone()
    }
}

impl FromRef<AuthState> for &'static Store {
    fn from_ref(state: &AuthState) -> Self {
        state.store
    }
}

#[derive(Debug, Clone)]
pub struct AuthSession {
    pub token_id: String,
    pub user_id: Option<Ulid>,
    pub org_id: u64,
    pub env_id: Ulid,
    pub env_slug: String,
    pub roles: Vec<String>,
    pub labels: Vec<String>,
    pub session_id: Option<Ulid>,
    pub token_kind: TokenKind,
    pub credential_context: Option<models::SessionCredentialContext>,
    pub hardware_attested: bool,
    pub biometric_verified: bool,
    pub trust_score: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    Access,
    Dev,
    ApiKey,
}

#[async_trait]
impl<S> FromRequestParts<S> for AuthSession
where
    S: Send + Sync,
    AuthState: FromRef<S>,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let AuthState { config, store } = AuthState::from_ref(state);
        let hdr = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .ok_or(AuthError::MissingHeader)?;
        let s = hdr.to_str().map_err(|_| AuthError::InvalidHeader)?;
        if !s.starts_with("Bearer ") {
            return Err(AuthError::InvalidHeader);
        }
        let token = &s[7..];
        let keys = config.jwt_keys();
        // try JWT access token first
        if let Ok(access) = keys.decode_access(token) {
            let user_id =
                Ulid::from_string(&access.sub).map_err(|e| anyhow!("bad user id: {e}"))?;
            let env_id = Ulid::from_string(&access.env).map_err(|e| anyhow!("bad env id: {e}"))?;
            let repo = AuthRepository::new(store);
            let env = repo
                .get_environment_by_id(access.org, &env_id)?
                .ok_or(AuthError::Unauthorized)?;

            let session_id = Ulid::from_string(&access.sid).map_err(|e| anyhow!("bad sid: {e}"))?;

            // Get credential context if available
            let credential_context = repo.get_session_credential_context(session_id).ok().flatten();
            let hardware_attested = credential_context.as_ref().map(|c| c.hardware_attested).unwrap_or(false);
            let biometric_verified = credential_context.as_ref().map(|c| c.biometric_verified).unwrap_or(false);
            let trust_score = credential_context.as_ref().map(|c| c.trust_score).unwrap_or(0.0);

            return Ok(AuthSession {
                token_id: access.sid.clone(),
                user_id: Some(user_id),
                org_id: access.org,
                env_id,
                env_slug: env.slug,
                roles: access.roles,
                labels: access.labels,
                session_id: Some(session_id),
                token_kind: TokenKind::Access,
                credential_context,
                hardware_attested,
                biometric_verified,
                trust_score,
            });
        }
        // fallback to dev token (only in development/staging environments)
        if let Some(dev_secret) = config.dev_secret() {
            // Security: Prevent dev token usage in production
            if is_production_environment() {
                tracing::warn!("Dev token authentication attempted in production environment");
                return Err(AuthError::Unauthorized);
            }

            if let Ok(claims) = verify_dev_token(token, dev_secret) {
                let repo = AuthRepository::new(store);
                let (live_env, _) = repo.ensure_default_environments(claims.org_id)?;
                return Ok(AuthSession {
                    token_id: claims.token_id,
                    user_id: None,
                    org_id: claims.org_id,
                    env_id: live_env.id,
                    env_slug: live_env.slug,
                    roles: claims.roles,
                    labels: claims.labels,
                    session_id: None,
                    token_kind: TokenKind::Dev,
                    credential_context: None,
                    hardware_attested: false,
                    biometric_verified: false,
                    trust_score: 0.0,
                });
            }
        }
        Err(AuthError::Unauthorized)
    }
}

pub struct AuthService<'a> {
    repo: AuthRepository<'a>,
    config: AuthConfig,
    metrics: Option<Arc<dyn AuthMetrics>>,
}

impl<'a> AuthService<'a> {
    pub fn new(store: &'a Store, config: AuthConfig) -> Self {
        Self {
            repo: AuthRepository::new(store),
            config,
            metrics: None,
        }
    }

    pub fn with_metrics(store: &'a Store, config: AuthConfig, metrics: Arc<dyn AuthMetrics>) -> Self {
        Self {
            repo: AuthRepository::new(store),
            config,
            metrics: Some(metrics),
        }
    }

    pub fn repo(&self) -> &AuthRepository<'a> {
        &self.repo
    }

    pub fn config(&self) -> &AuthConfig {
        &self.config
    }

    pub fn signup_initial_admin(
        &self,
        org_id: u64,
        email: &str,
        password: &str,
    ) -> Result<UserRecord> {
        if let Some(existing) = self.repo.get_user_by_email(org_id, email)? {
            return Ok(existing);
        }
        let now = current_timestamp();
        let password_hash = hash_password(password)?;
        let user = UserRecord {
            id: Ulid::new(),
            org_id,
            email: email.to_lowercase(),
            password_hash,
            roles: vec!["admin".into()],
            labels: Vec::new(),
            status: UserStatus::Active,
            created_at: now,
            updated_at: now,
        };
        self.repo.insert_user(&user)?;
        let _ = self.repo.ensure_default_environments(org_id)?;
        Ok(user)
    }

    pub async fn login(
        &self,
        org_id: u64,
        email: &str,
        password: &str,
        env_slug: Option<String>,
    ) -> Result<LoginTokens> {
        self.login_with_credentials(org_id, email, password, env_slug, None, None).await
    }

    /// Enhanced login with credential-based authentication support
    pub async fn login_with_credentials(
        &self,
        org_id: u64,
        email: &str,
        password: &str,
        env_slug: Option<String>,
        primary_credential_id: Option<Ulid>,
        additional_credential_ids: Option<Vec<Ulid>>,
    ) -> Result<LoginTokens> {
        let result = self.login_with_device_attestation(
            org_id,
            email,
            password,
            env_slug,
            primary_credential_id,
            additional_credential_ids,
            None,
            None,
            None,
        ).await?;
        Ok(result.tokens)
    }

    /// Full login with device attestation and credential verification
    pub async fn login_with_device_attestation(
        &self,
        org_id: u64,
        email: &str,
        password: &str,
        env_slug: Option<String>,
        primary_credential_id: Option<Ulid>,
        additional_credential_ids: Option<Vec<Ulid>>,
        device_info: Option<&models::DeviceInfo>,
        _ip_address: Option<String>,
        _user_agent: Option<String>,
    ) -> Result<EnhancedLoginResult> {
        // Input validation and sanitization
        if org_id == 0 {
            return Err(anyhow!("Organization ID cannot be zero"));
        }

        let sanitized_email = validation::AuthValidator::validate_email(email)?;
        let _sanitized_password = validation::AuthValidator::validate_password(password)?;

        let _sanitized_env_slug = if let Some(ref slug) = env_slug {
            Some(validation::AuthValidator::validate_environment_slug(slug)?)
        } else {
            None
        };

        // Validate additional credential IDs if provided
        if let Some(ref credential_ids) = additional_credential_ids {
            if credential_ids.len() > 10 {
                return Err(anyhow!("Too many additional credentials: maximum 10 allowed"));
            }
            for &cred_id in credential_ids {
                if cred_id == Ulid::nil() {
                    return Err(anyhow!("Invalid credential ID: cannot be nil"));
                }
            }
        }

              if let Some(metrics) = &self.metrics {
            metrics.record_auth_attempt("password", false);
        }
        let start_time = std::time::Instant::now();

        let Some(user) = self.repo.get_user_by_email(org_id, &sanitized_email)? else {
            if let Some(metrics) = &self.metrics {
                metrics.record_auth_attempt("password", false);
            }
            return Err(anyhow!("invalid credentials"));
        };
        if !verify_password(&user.password_hash, password)? {
            if let Some(metrics) = &self.metrics {
                metrics.record_auth_attempt("password", false);
            }
            return Err(anyhow!("invalid credentials"));
        }
        if user.status != UserStatus::Active {
            if let Some(metrics) = &self.metrics {
                metrics.record_auth_attempt("password", false);
            }
            return Err(anyhow!("user disabled"));
        }
        let (live_env, sandbox_env) = self.repo.ensure_default_environments(org_id)?;
        let target_env = match env_slug.as_deref() {
            Some("sandbox") => sandbox_env,
            Some("live") | None => live_env,
            Some(other) => self
                .repo
                .get_environment_by_slug(org_id, other)?
                .ok_or_else(|| anyhow!("unknown environment"))?,
        };
        let session_id = Ulid::new();
        let now = current_timestamp();

        let mut _device_attestation_result: Option<crate::device_attestation::DeviceAttestationResult> = None;

        // Perform device attestation if device info is provided
        if let Some(device_info) = device_info {
            let attestation_service = device_attestation::DeviceAttestationService::new(
                self.repo.store
            );
            let attestation_result = attestation_service.attest_device_login(
                user.id,
                org_id,
                device_info,
                _ip_address.clone(),
                _user_agent.clone(),
                None, // Hardware attestation data would be provided by client
            ).await?;
            _device_attestation_result = Some(attestation_result);
        }

        // Create credential context if credentials are provided
        if primary_credential_id.is_some() || additional_credential_ids.is_some() {
            let cred_service = credential_verification::CredentialVerificationService::new(
                self.repo.store
            );
            let _ = cred_service.create_credential_context(
                user.id,
                org_id,
                session_id,
                primary_credential_id,
                additional_credential_ids.unwrap_or_default(),
            ).await;
        }

        let access_exp = now + self.config.access_ttl_secs;
        let refresh_exp = now + self.config.refresh_ttl_secs;
        let access_claims = AccessClaims {
            sub: user.id.to_string(),
            org: org_id,
            env: target_env.id.to_string(),
            roles: user.roles.clone(),
            labels: user.labels.clone(),
            sid: session_id.to_string(),
            iat: now,
            exp: access_exp,
            typ: "access".into(),
        };
        let refresh_claims = RefreshClaims {
            sub: user.id.to_string(),
            org: org_id,
            sid: session_id.to_string(),
            env: target_env.id.to_string(),
            iat: now,
            exp: refresh_exp,
            typ: "refresh".into(),
        };
        let keys = self.config.jwt_keys();
        let access_token = keys.encode_access(&access_claims)?;
        let refresh_token = keys.encode_refresh(&refresh_claims)?;
        let session_payload = bincode::serialize(&refresh_claims)?;
        self.repo.insert_session(session_id, &session_payload)?;

        // Track successful login metrics
        if let Some(metrics) = &self.metrics {
            metrics.record_auth_attempt("password", true);
            let duration = start_time.elapsed().as_secs_f64();
            tracing::debug!("Login completed in {:.3}s", duration);
            // Record expected session duration for analytics
            let expected_session_duration = refresh_exp - now;
            metrics.record_session_duration(expected_session_duration as f64);
        }

        let tokens = LoginTokens {
            access_token,
            refresh_token,
            access_expires_at: access_exp,
            refresh_expires_at: refresh_exp,
            environment: EnvironmentSummary::from(target_env),
        };

        Ok(EnhancedLoginResult {
            tokens,
            device_attestation: None,
            hardware_attested: false,
            trust_score: 0.5,
            requires_additional_verification: false,
        })
    }

    pub fn refresh(&self, refresh_token: &str) -> Result<LoginTokens> {
        let keys = self.config.jwt_keys();
        let claims = keys.decode_refresh(refresh_token)?;
        let session_id = Ulid::from_string(&claims.sid).map_err(|e| anyhow!("bad session: {e}"))?;
        let Some(payload) = self.repo.get_session(&session_id)? else {
            return Err(anyhow!("session revoked"));
        };
        let stored: RefreshClaims = bincode::deserialize(&payload)?;
        if stored.exp != claims.exp || stored.sub != claims.sub {
            return Err(anyhow!("refresh token mismatch"));
        }
        let user_id = Ulid::from_string(&claims.sub).map_err(|e| anyhow!("bad user id: {e}"))?;
        let env_id = Ulid::from_string(&stored.env).map_err(|e| anyhow!("bad env id: {e}"))?;
        let org_id = claims.org;
        let Some(user) = self.repo.get_user_by_id(org_id, &user_id)? else {
            return Err(anyhow!("user missing"));
        };
        let env = self
            .repo
            .get_environment_by_id(org_id, &env_id)?
            .ok_or_else(|| anyhow!("environment missing"))?;
        let now = current_timestamp();
        let session_id_new = Ulid::new();
        let access_exp = now + self.config.access_ttl_secs;
        let refresh_exp = now + self.config.refresh_ttl_secs;
        let access_claims = AccessClaims {
            sub: user_id.to_string(),
            org: org_id,
            env: env.id.to_string(),
            roles: user.roles.clone(),
            labels: user.labels.clone(),
            sid: session_id_new.to_string(),
            iat: now,
            exp: access_exp,
            typ: "access".into(),
        };
        let refresh_claims = RefreshClaims {
            sub: user_id.to_string(),
            org: org_id,
            sid: session_id_new.to_string(),
            env: env.id.to_string(),
            iat: now,
            exp: refresh_exp,
            typ: "refresh".into(),
        };
        self.repo.delete_session(&session_id)?;
        self.repo
            .insert_session(session_id_new, &bincode::serialize(&refresh_claims)?)?;
        let access_token = keys.encode_access(&access_claims)?;
        let refresh_token_new = keys.encode_refresh(&refresh_claims)?;
        Ok(LoginTokens {
            access_token,
            refresh_token: refresh_token_new,
            access_expires_at: access_exp,
            refresh_expires_at: refresh_exp,
            environment: EnvironmentSummary::from(env),
        })
    }

    pub fn logout(&self, refresh_token: &str) -> Result<()> {
        let keys = self.config.jwt_keys();
        let claims = keys.decode_refresh(refresh_token)?;
        let session_id = Ulid::from_string(&claims.sid).map_err(|e| anyhow!("bad session: {e}"))?;
        self.repo.delete_session(&session_id)?;
        Ok(())
    }

    pub fn switch_environment(&self, refresh_token: &str, env_slug: &str) -> Result<LoginTokens> {
        let keys = self.config.jwt_keys();
        let claims = keys.decode_refresh(refresh_token)?;
        let session_id = Ulid::from_string(&claims.sid).map_err(|e| anyhow!("bad session: {e}"))?;
        let Some(payload) = self.repo.get_session(&session_id)? else {
            return Err(anyhow!("session revoked"));
        };
        let stored: RefreshClaims = bincode::deserialize(&payload)?;
        if stored.sub != claims.sub {
            return Err(anyhow!("refresh token mismatch"));
        }
        let org_id = claims.org;
        let user_id = Ulid::from_string(&claims.sub).map_err(|e| anyhow!("bad user id: {e}"))?;
        let Some(user) = self.repo.get_user_by_id(org_id, &user_id)? else {
            return Err(anyhow!("user missing"));
        };
        let target_slug = env_slug.to_lowercase();
        let target_env = if target_slug == "live" || target_slug == "sandbox" {
            let (live, sandbox) = self.repo.ensure_default_environments(org_id)?;
            if target_slug == "live" {
                live
            } else {
                sandbox
            }
        } else {
            self.repo
                .get_environment_by_slug(org_id, &target_slug)?
                .ok_or_else(|| anyhow!("unknown environment"))?
        };
        let now = current_timestamp();
        let session_id_new = Ulid::new();
        let access_exp = now + self.config.access_ttl_secs;
        let refresh_exp = now + self.config.refresh_ttl_secs;
        let access_claims = AccessClaims {
            sub: user_id.to_string(),
            org: org_id,
            env: target_env.id.to_string(),
            roles: user.roles.clone(),
            labels: user.labels.clone(),
            sid: session_id_new.to_string(),
            iat: now,
            exp: access_exp,
            typ: "access".into(),
        };
        let refresh_claims = RefreshClaims {
            sub: user_id.to_string(),
            org: org_id,
            sid: session_id_new.to_string(),
            env: target_env.id.to_string(),
            iat: now,
            exp: refresh_exp,
            typ: "refresh".into(),
        };
        self.repo.delete_session(&session_id)?;
        self.repo
            .insert_session(session_id_new, &bincode::serialize(&refresh_claims)?)?;
        let access_token = keys.encode_access(&access_claims)?;
        let refresh_token_new = keys.encode_refresh(&refresh_claims)?;

        Ok(LoginTokens {
            access_token,
            refresh_token: refresh_token_new,
            access_expires_at: access_exp,
            refresh_expires_at: refresh_exp,
            environment: EnvironmentSummary::from(target_env),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LoginTokens {
    pub access_token: String,
    pub refresh_token: String,
    pub access_expires_at: i64,
    pub refresh_expires_at: i64,
    pub environment: EnvironmentSummary,
}

#[derive(Debug, Clone, Serialize)]
pub struct EnhancedLoginResult {
    pub tokens: LoginTokens,
    pub device_attestation: Option<device_attestation::DeviceAttestationResult>,
    pub hardware_attested: bool,
    pub trust_score: f64,
    pub requires_additional_verification: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentSummary {
    pub id: String,
    pub slug: String,
    pub display_name: String,
    pub auto_clear_policy: AutoClearPolicy,
}

impl From<EnvironmentRecord> for EnvironmentSummary {
    fn from(value: EnvironmentRecord) -> Self {
        Self {
            id: value.id.to_string(),
            slug: value.slug,
            display_name: value.display_name,
            auto_clear_policy: value.auto_clear_policy,
        }
    }
}

pub fn issue_dev_token(cfg: &AuthConfig) -> Result<String> {
    // Security: Prevent dev token issuance in production
    if is_production_environment() {
        return Err(anyhow!("Dev token issuance is disabled in production environment"));
    }

    let org_id = cfg
        .dev_org_id()
        .ok_or_else(|| anyhow!("missing dev_org_id"))?;
    let secret = cfg
        .dev_secret()
        .ok_or_else(|| anyhow!("missing dev_secret"))?;
    let now = current_timestamp();
    let claims = DevTokenClaims {
        token_id: format!("dev-{}", org_id),
        org_id,
        roles: vec!["admin".into()],
        labels: Vec::new(),
        exp: now + 3600,
    };
    let payload = serde_json::to_vec(&claims)?;
    let payload_b64 = base64::engine::general_purpose::STANDARD.encode(&payload);
    let mut mac = <hmac::Hmac<sha2::Sha256> as KeyInit>::new_from_slice(secret.as_bytes())?;
    mac.update(payload_b64.as_bytes());
    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    Ok(format!("{}.{}", payload_b64, sig_b64))
}

pub fn verify_dev_token(token: &str, secret: &str) -> Result<DevTokenClaims> {
    let mut it = token.split('.');
    let payload_b64 = it.next().ok_or(AuthError::InvalidFormat)?;
    let sig_b64 = it.next().ok_or(AuthError::InvalidFormat)?;
    if it.next().is_some() {
        return Err(AuthError::InvalidFormat.into());
    }
    let mut mac = <hmac::Hmac<sha2::Sha256> as KeyInit>::new_from_slice(secret.as_bytes())?;
    mac.update(payload_b64.as_bytes());
    let expected = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    if expected != sig_b64 {
        return Err(AuthError::InvalidSignature.into());
    }
    let bytes = base64::engine::general_purpose::STANDARD.decode(payload_b64)?;
    let claims: DevTokenClaims = serde_json::from_slice(&bytes)?;
    if claims.exp < current_timestamp() {
        return Err(AuthError::Expired.into());
    }
    Ok(claims)
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Check if the current environment is production based on environment variables
/// This function checks multiple common environment variable patterns
fn is_production_environment() -> bool {
    // Check common production environment indicators
    let env_patterns = [
        ("ENVIRONMENT", ["production", "prod"].as_slice()),
        ("ENV", ["production", "prod"].as_slice()),
        ("NODE_ENV", ["production"].as_slice()),
        ("RUST_ENV", ["production", "prod"].as_slice()),
        ("APP_ENV", ["production", "prod"].as_slice()),
        ("DEPLOYMENT_ENV", ["production", "prod"].as_slice()),
    ];

    for (var_name, prod_values) in env_patterns {
        if let Ok(value) = std::env::var(var_name) {
            let normalized = value.to_lowercase();
            if prod_values.iter().any(|&prod_val| normalized == prod_val) {
                return true;
            }
        }
    }

    // Check for explicit production flag
    if let Ok(is_prod) = std::env::var("IS_PRODUCTION") {
        return matches!(is_prod.to_lowercase().as_str(), "true" | "1" | "yes" | "on");
    }

    // Check for absence of development indicators (more conservative)
    let dev_indicators = ["ZAUTH_DEV_SECRET", "DEV_MODE", "DEBUG"];
    let has_dev_indicators = dev_indicators.iter().any(|&var| std::env::var(var).is_ok());

    // If no explicit environment is set and no dev indicators, assume production for safety
    !has_dev_indicators
}
