//! API endpoints for Two-Factor Authentication management

use axum::{
    extract::{State, Path, Query},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use crate::{
    AuthSession, AuthState,
    two_factor::TwoFactorService,
    webauthn::WebAuthnService,
    policy::TwoFactorPolicyService,
    recovery::{RecoveryService, RecoveryMethod, RecoveryVerificationResult},
    models::{ChallengeType, OrgTwoFactorPolicy, TrustedDevice},
};

/// API handlers for 2FA management
pub struct TwoFactorApi;

impl TwoFactorApi {
    /// Get current 2FA status for the authenticated user
    pub async fn get_2fa_status(
        State(state): State<AuthState>,
        session: AuthSession,
    ) -> Result<Json<TwoFactorStatusResponse>, ApiError> {
        let service = TwoFactorService::new(
            state.store,
            None,
            "https://app.example.com", // TODO: Get from config
            "example.com"
        )?;

        let user_id = session.user_id.ok_or(ApiError::Unauthorized)?;
        let status = service.get_2fa_status(&user_id, session.org_id)?;

        Ok(Json(TwoFactorStatusResponse {
            enabled: status.enabled,
            required: status.required,
            enabled_methods: status.enabled_methods,
            default_method: status.default_method,
            phone_verified: status.phone_verified,
            backup_codes_remaining: status.backup_codes_remaining,
        }))
    }

    /// Generate TOTP setup (QR code and secret)
    pub async fn generate_totp_setup(
        State(state): State<AuthState>,
        session: AuthSession,
        Json(request): Json<TotpSetupRequest>,
    ) -> Result<Json<TotpSetupResponse>, ApiError> {
        let notification_service = None; // TODO: Initialize from state
        let service = TwoFactorService::new(
            state.store,
            notification_service,
            "https://app.example.com", // TODO: Get from config
            "example.com"
        )?;

        let user_id = session.user_id.ok_or(ApiError::Unauthorized)?;
        let setup = service.generate_totp_setup(
            &user_id,
            session.org_id,
            &request.issuer,
            &request.account_name,
        )?;

        Ok(Json(TotpSetupResponse {
            secret_id: setup.secret_id,
            secret: setup.secret,
            qr_code_url: setup.qr_code_url,
            qr_code_png: setup.qr_code_png,
        }))
    }

    /// Verify TOTP setup and enable TOTP 2FA
    pub async fn verify_totp_setup(
        State(state): State<AuthState>,
        session: AuthSession,
        Json(request): Json<VerifyTotpRequest>,
    ) -> Result<Json<TotpVerificationResponse>, ApiError> {
        let notification_service = None; // TODO: Initialize from state
        let service = TwoFactorService::new(
            state.store,
            notification_service,
            "https://app.example.com", // TODO: Get from config
            "example.com"
        )?;

        let user_id = session.user_id.ok_or(ApiError::Unauthorized)?;
        let backup_codes = service.verify_totp_setup(
            &user_id,
            session.org_id,
            &request.secret_id,
            &request.code,
        ).await?;

        Ok(Json(TotpVerificationResponse {
            success: true,
            backup_codes,
        }))
    }

    /// Initiate SMS 2FA setup
    pub async fn initiate_sms_setup(
        State(state): State<AuthState>,
        session: AuthSession,
        Json(request): Json<SmsSetupRequest>,
    ) -> Result<Json<SmsSetupResponse>, ApiError> {
        let notification_service = None; // TODO: Initialize from state
        let service = TwoFactorService::new(
            state.store,
            notification_service,
            "https://app.example.com", // TODO: Get from config
            "example.com"
        )?;

        let user_id = session.user_id.ok_or(ApiError::Unauthorized)?;
        let verification_id = service.initiate_sms_setup(
            &user_id,
            session.org_id,
            &request.phone_number,
        ).await?;

        Ok(Json(SmsSetupResponse {
            verification_id,
            message: "Verification code sent to your phone".to_string(),
        }))
    }

    /// Verify SMS setup
    pub async fn verify_sms_setup(
        State(state): State<AuthState>,
        session: AuthSession,
        Json(request): Json<VerifySmsRequest>,
    ) -> Result<Json<VerificationResponse>, ApiError> {
        let notification_service = None; // TODO: Initialize from state
        let service = TwoFactorService::new(
            state.store,
            notification_service,
            "https://app.example.com", // TODO: Get from config
            "example.com"
        )?;

        let user_id = session.user_id.ok_or(ApiError::Unauthorized)?;
        service.verify_sms_setup(
            &user_id,
            session.org_id,
            &request.verification_id,
            &request.code,
        )?;

        Ok(Json(VerificationResponse {
            success: true,
            message: "SMS 2FA enabled successfully".to_string(),
        }))
    }

    /// Start WebAuthn registration
    pub async fn start_webauthn_registration(
        State(state): State<AuthState>,
        session: AuthSession,
        Json(request): Json<WebAuthnRegistrationRequest>,
    ) -> Result<Json<WebAuthnRegistrationResponse>, ApiError> {
        let webauthn_service = WebAuthnService::new(
            state.store,
            &request.rp_origin,
            &request.rp_id,
            &request.rp_name,
        )?;

        let user_id = session.user_id.ok_or(ApiError::Unauthorized)?;
        let (ccr, reg_state) = webauthn_service.start_registration(
            &user_id,
            session.org_id,
            &request.user_email,
            &request.display_name,
            None,
        )?;

        // Store registration state in session/cache
        // TODO: Implement state storage

        Ok(Json(WebAuthnRegistrationResponse {
            creation_options: serde_json::to_value(ccr)?,
            session_data: serde_json::to_value(reg_state)?,
        }))
    }

    /// Finish WebAuthn registration
    pub async fn finish_webauthn_registration(
        State(state): State<AuthState>,
        session: AuthSession,
        Json(_request): Json<WebAuthnRegistrationFinishRequest>,
    ) -> Result<Json<WebAuthnCredentialResponse>, ApiError> {
        let _webauthn_service = WebAuthnService::new(
            state.store,
            "https://app.example.com", // TODO: Get from config
            "example.com",
            "ZRUSTDB",
        )?;

        let _user_id = session.user_id.ok_or(ApiError::Unauthorized)?;

        // TODO: Retrieve registration state from session/cache
        // let reg_state = get_registration_state(&request.session_id)?;

        // For now, return an error as we need proper state management
        return Err(ApiError::InternalServerError("Registration state management not implemented".to_string()));
    }

    /// Create 2FA challenge for login
    pub async fn create_2fa_challenge(
        State(state): State<AuthState>,
        session: AuthSession,
        Query(params): Query<ChallengeParams>,
    ) -> Result<Json<ChallengeResponse>, ApiError> {
        let notification_service = None; // TODO: Initialize from state
        let service = TwoFactorService::new(
            state.store,
            notification_service,
            "https://app.example.com", // TODO: Get from config
            "example.com"
        )?;

        let user_id = session.user_id.ok_or(ApiError::Unauthorized)?;
        let challenge = service.create_challenge(
            &user_id,
            session.org_id,
            params.method,
        ).await?;

        Ok(Json(ChallengeResponse {
            challenge_id: challenge.id,
            challenge_type: challenge.challenge_type,
            expires_at: challenge.expires_at,
            message: "Challenge created successfully".to_string(),
        }))
    }

    /// Verify 2FA challenge
    pub async fn verify_2fa_challenge(
        State(state): State<AuthState>,
        Path(challenge_id): Path<Ulid>,
        Json(request): Json<VerifyChallengeRequest>,
    ) -> Result<Json<VerificationResponse>, ApiError> {
        let notification_service = None; // TODO: Initialize from state
        let service = TwoFactorService::new(
            state.store,
            notification_service,
            "https://app.example.com", // TODO: Get from config
            "example.com"
        )?;

        let verified = service.verify_challenge(&challenge_id, &request.code)?;

        Ok(Json(VerificationResponse {
            success: verified,
            message: if verified {
                "Challenge verified successfully".to_string()
            } else {
                "Invalid verification code".to_string()
            },
        }))
    }

    /// Disable a specific 2FA method
    pub async fn disable_2fa_method(
        State(state): State<AuthState>,
        session: AuthSession,
        Path(method): Path<String>,
    ) -> Result<Json<VerificationResponse>, ApiError> {
        let notification_service = None; // TODO: Initialize from state
        let service = TwoFactorService::new(
            state.store,
            notification_service,
            "https://app.example.com", // TODO: Get from config
            "example.com"
        )?;

        let user_id = session.user_id.ok_or(ApiError::Unauthorized)?;
        let challenge_type = match method.as_str() {
            "totp" => ChallengeType::Totp,
            "sms" => ChallengeType::Sms,
            "email" => ChallengeType::Email,
            "webauthn" => ChallengeType::WebAuthn,
            "backup_codes" => ChallengeType::BackupCode,
            _ => return Err(ApiError::BadRequest("Invalid 2FA method".to_string())),
        };

        service.disable_2fa_method(&user_id, session.org_id, challenge_type)?;

        Ok(Json(VerificationResponse {
            success: true,
            message: format!("{} 2FA disabled successfully", method),
        }))
    }

    /// Generate new backup codes
    pub async fn generate_backup_codes(
        State(state): State<AuthState>,
        session: AuthSession,
    ) -> Result<Json<BackupCodesResponse>, ApiError> {
        let notification_service = None; // TODO: Initialize from state
        let service = TwoFactorService::new(
            state.store,
            notification_service,
            "https://app.example.com", // TODO: Get from config
            "example.com"
        )?;

        let user_id = session.user_id.ok_or(ApiError::Unauthorized)?;
        let backup_codes = service.generate_backup_codes(&user_id, session.org_id)?;

        Ok(Json(BackupCodesResponse {
            backup_codes,
            message: "New backup codes generated successfully".to_string(),
        }))
    }

    /// Get organization 2FA policy
    pub async fn get_org_2fa_policy(
        State(state): State<AuthState>,
        session: AuthSession,
    ) -> Result<Json<OrgTwoFactorPolicy>, ApiError> {
        let policy_service = TwoFactorPolicyService::new(state.store, None);
        let policy = policy_service.get_org_policy(session.org_id)?;
        Ok(Json(policy))
    }

    /// Update organization 2FA policy (admin only)
    pub async fn update_org_2fa_policy(
        State(state): State<AuthState>,
        session: AuthSession,
        Json(policy): Json<OrgTwoFactorPolicy>,
    ) -> Result<Json<VerificationResponse>, ApiError> {
        // Check if user has admin role
        if !session.roles.contains(&"admin".to_string()) {
            return Err(ApiError::Forbidden);
        }

        let notification_service = None; // TODO: Initialize from state
        let policy_service = TwoFactorPolicyService::new(state.store, notification_service);
        let user_id = session.user_id.ok_or(ApiError::Unauthorized)?;

        policy_service.update_org_policy(session.org_id, &policy, &user_id).await?;

        Ok(Json(VerificationResponse {
            success: true,
            message: "Organization 2FA policy updated successfully".to_string(),
        }))
    }

    /// Check user compliance with 2FA policy
    pub async fn check_compliance(
        State(state): State<AuthState>,
        session: AuthSession,
    ) -> Result<Json<ComplianceResponse>, ApiError> {
        let policy_service = TwoFactorPolicyService::new(state.store, None);
        let user_id = session.user_id.ok_or(ApiError::Unauthorized)?;

        let compliance = policy_service.check_user_compliance(&user_id, session.org_id)?;

        Ok(Json(ComplianceResponse {
            compliant: compliance.compliant,
            required: compliance.required,
            enabled_methods: compliance.enabled_methods,
            missing_requirements: compliance.missing_requirements,
            grace_period_ends: compliance.grace_period_ends,
        }))
    }

    /// Initiate account recovery
    pub async fn initiate_recovery(
        State(state): State<AuthState>,
        Json(request): Json<RecoveryRequest>,
    ) -> Result<Json<RecoveryResponse>, ApiError> {
        let notification_service = None; // TODO: Initialize from state
        let recovery_service = RecoveryService::new(state.store, notification_service);

        let recovery_id = recovery_service.initiate_account_recovery(
            request.org_id,
            &request.email,
            request.method,
        ).await?;

        Ok(Json(RecoveryResponse {
            recovery_id,
            message: "Recovery process initiated. Check your email or phone for instructions.".to_string(),
        }))
    }

    /// Verify recovery token
    pub async fn verify_recovery(
        State(state): State<AuthState>,
        Json(request): Json<VerifyRecoveryRequest>,
    ) -> Result<Json<RecoveryVerificationResult>, ApiError> {
        let notification_service = None; // TODO: Initialize from state
        let recovery_service = RecoveryService::new(state.store, notification_service);

        let result = recovery_service.verify_recovery(&request.recovery_id, &request.token)?;

        Ok(Json(result))
    }

    /// List trusted devices
    pub async fn list_trusted_devices(
        State(state): State<AuthState>,
        session: AuthSession,
    ) -> Result<Json<TrustedDevicesResponse>, ApiError> {
        let recovery_service = RecoveryService::new(state.store, None);
        let user_id = session.user_id.ok_or(ApiError::Unauthorized)?;

        let devices = recovery_service.list_trusted_devices(&user_id, session.org_id)?;

        Ok(Json(TrustedDevicesResponse { devices }))
    }

    /// Remove trusted device
    pub async fn remove_trusted_device(
        State(state): State<AuthState>,
        session: AuthSession,
        Path(device_id): Path<Ulid>,
    ) -> Result<Json<VerificationResponse>, ApiError> {
        let recovery_service = RecoveryService::new(state.store, None);
        let user_id = session.user_id.ok_or(ApiError::Unauthorized)?;

        recovery_service.untrust_device(&user_id, session.org_id, &device_id)?;

        Ok(Json(VerificationResponse {
            success: true,
            message: "Trusted device removed successfully".to_string(),
        }))
    }
}

// Request/Response types

#[derive(Debug, Deserialize)]
pub struct TotpSetupRequest {
    pub issuer: String,
    pub account_name: String,
}

#[derive(Debug, Serialize)]
pub struct TotpSetupResponse {
    pub secret_id: Ulid,
    pub secret: String,
    pub qr_code_url: String,
    #[serde(with = "base64_serde")]
    pub qr_code_png: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub struct VerifyTotpRequest {
    pub secret_id: Ulid,
    pub code: String,
}

#[derive(Debug, Serialize)]
pub struct TotpVerificationResponse {
    pub success: bool,
    pub backup_codes: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct SmsSetupRequest {
    pub phone_number: String,
}

#[derive(Debug, Serialize)]
pub struct SmsSetupResponse {
    pub verification_id: Ulid,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct VerifySmsRequest {
    pub verification_id: Ulid,
    pub code: String,
}

#[derive(Debug, Serialize)]
pub struct VerificationResponse {
    pub success: bool,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct WebAuthnRegistrationRequest {
    pub rp_origin: String,
    pub rp_id: String,
    pub rp_name: String,
    pub user_email: String,
    pub display_name: String,
}

#[derive(Debug, Serialize)]
pub struct WebAuthnRegistrationResponse {
    pub creation_options: serde_json::Value,
    pub session_data: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct WebAuthnRegistrationFinishRequest {
    pub session_id: String,
    pub credential: serde_json::Value,
    pub credential_name: String,
}

#[derive(Debug, Serialize)]
pub struct WebAuthnCredentialResponse {
    pub credential_id: Ulid,
    pub name: String,
    pub created_at: i64,
}

#[derive(Debug, Deserialize)]
pub struct ChallengeParams {
    pub method: Option<ChallengeType>,
}

#[derive(Debug, Serialize)]
pub struct ChallengeResponse {
    pub challenge_id: Ulid,
    pub challenge_type: ChallengeType,
    pub expires_at: i64,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct VerifyChallengeRequest {
    pub code: String,
}

#[derive(Debug, Serialize)]
pub struct BackupCodesResponse {
    pub backup_codes: Vec<String>,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct TwoFactorStatusResponse {
    pub enabled: bool,
    pub required: bool,
    pub enabled_methods: Vec<ChallengeType>,
    pub default_method: Option<ChallengeType>,
    pub phone_verified: bool,
    pub backup_codes_remaining: u32,
}

#[derive(Debug, Serialize)]
pub struct ComplianceResponse {
    pub compliant: bool,
    pub required: bool,
    pub enabled_methods: Vec<ChallengeType>,
    pub missing_requirements: Vec<String>,
    pub grace_period_ends: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct RecoveryRequest {
    pub org_id: u64,
    pub email: String,
    pub method: RecoveryMethod,
}

#[derive(Debug, Serialize)]
pub struct RecoveryResponse {
    pub recovery_id: Ulid,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct VerifyRecoveryRequest {
    pub recovery_id: Ulid,
    pub token: String,
}

#[derive(Debug, Serialize)]
pub struct TrustedDevicesResponse {
    pub devices: Vec<TrustedDevice>,
}

// Error types
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Forbidden")]
    Forbidden,
    #[error("Bad request: {0}")]
    BadRequest(String),
    #[error("Internal server error: {0}")]
    InternalServerError(String),
    #[error(transparent)]
    Auth(#[from] crate::AuthError),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            ApiError::Forbidden => (StatusCode::FORBIDDEN, "Forbidden".to_string()),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::InternalServerError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            ApiError::Auth(e) => (StatusCode::UNAUTHORIZED, e.to_string()),
            ApiError::Anyhow(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            ApiError::Json(e) => (StatusCode::BAD_REQUEST, e.to_string()),
        };

        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

// Base64 serde module
mod base64_serde {
    use base64::Engine;
    use serde::Serializer;

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
    }

    }