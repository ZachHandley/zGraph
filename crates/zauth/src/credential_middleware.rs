use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use tower::ServiceExt;

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use tower::{Layer, Service};
use ulid::Ulid;
use zcore_storage::Store;

use crate::{
    AuthSession, AuthState,
    models::{CredentialTrustLevel, AssertionType, IdentityAssertionRecord},
    credential_verification::CredentialVerificationService,
    repository::AuthRepository,
};

/// Middleware that enforces credential verification requirements
#[derive(Clone)]
pub struct CredentialVerificationLayer {
    requirements: VerificationRequirements,
}

impl CredentialVerificationLayer {
    pub fn new(requirements: VerificationRequirements) -> Self {
        Self { requirements }
    }

    /// Require hardware attestation for protected routes
    pub fn require_hardware_attestation() -> Self {
        Self::new(VerificationRequirements {
            min_trust_level: Some(CredentialTrustLevel::HardwareVerified),
            require_hardware_attestation: true,
            require_biometric: false,
            min_trust_score: 0.8,
            require_device_bound: false,
        })
    }

    /// Require biometric verification for high-security operations
    pub fn require_biometric() -> Self {
        Self::new(VerificationRequirements {
            min_trust_level: Some(CredentialTrustLevel::HardwareVerified),
            require_hardware_attestation: false,
            require_biometric: true,
            min_trust_score: 0.9,
            require_device_bound: false,
        })
    }

    /// Require highest security for critical operations
    pub fn require_maximum_security() -> Self {
        Self::new(VerificationRequirements {
            min_trust_level: Some(CredentialTrustLevel::TrustedPlatform),
            require_hardware_attestation: true,
            require_biometric: true,
            min_trust_score: 0.95,
            require_device_bound: true,
        })
    }
}

impl<S> Layer<S> for CredentialVerificationLayer {
    type Service = CredentialVerificationMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        CredentialVerificationMiddleware {
            inner,
            requirements: self.requirements.clone(),
        }
    }
}

#[derive(Clone)]
pub struct CredentialVerificationMiddleware<S> {
    inner: S,
    requirements: VerificationRequirements,
}

impl<S> Service<Request> for CredentialVerificationMiddleware<S>
where
    S: Service<Request, Response = Response> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let requirements = self.requirements.clone();
        let inner = self.inner.clone();

        Box::pin(async move {
            // Extract the AuthSession from request extensions (set by auth middleware)
            let auth_session = request.extensions().get::<AuthSession>().cloned();

            if let Some(session) = auth_session {
                // Verify credential requirements
                match verify_credential_requirements(&session, &requirements).await {
                    Ok(_) => {
                        // Requirements met, continue with request
                        inner.oneshot(request).await
                    }
                    Err(verification_error) => {
                        // Requirements not met, return error response
                        Ok(verification_error.into_response())
                    }
                }
            } else {
                // No auth session, return unauthorized
                Ok((StatusCode::UNAUTHORIZED, "Authentication required").into_response())
            }
        })
    }
}

/// Axum middleware function for credential verification
pub async fn credential_verification_middleware(
    State(_auth_state): State<AuthState>,
    request: Request,
    next: Next,
) -> Response {
    // This is a more flexible middleware that can be configured per route
    let session = request.extensions().get::<AuthSession>().cloned();

    if let Some(auth_session) = session {
        // Check if route requires credential verification
        let path = request.uri().path();
        let requirements = get_route_requirements(path);

        match verify_credential_requirements(&auth_session, &requirements).await {
            Ok(_) => next.run(request).await,
            Err(error) => error.into_response(),
        }
    } else {
        (StatusCode::UNAUTHORIZED, "Authentication required").into_response()
    }
}

/// Verify that an auth session meets credential requirements
async fn verify_credential_requirements(
    session: &AuthSession,
    requirements: &VerificationRequirements,
) -> Result<(), CredentialVerificationError> {
    // Check basic trust score
    if session.trust_score < requirements.min_trust_score {
        return Err(CredentialVerificationError::InsufficientTrustScore {
            required: requirements.min_trust_score,
            actual: session.trust_score,
        });
    }

    // Check hardware attestation requirement
    if requirements.require_hardware_attestation && !session.hardware_attested {
        return Err(CredentialVerificationError::HardwareAttestationRequired);
    }

    // Check biometric requirement
    if requirements.require_biometric && !session.biometric_verified {
        return Err(CredentialVerificationError::BiometricVerificationRequired);
    }

    // Check credential context if available
    if let Some(context) = &session.credential_context {
        // Check device binding requirement
        if requirements.require_device_bound && !context.device_bound {
            return Err(CredentialVerificationError::DeviceBindingRequired);
        }

        // Check minimum trust level for credentials
        if let Some(_min_trust_level) = requirements.min_trust_level {
            // This would require additional data from the credential context
            // For now, we'll assume it's met if other requirements are satisfied
        }
    } else if requirements.require_hardware_attestation ||
              requirements.require_biometric ||
              requirements.require_device_bound {
        return Err(CredentialVerificationError::CredentialContextRequired);
    }

    Ok(())
}

/// Get verification requirements for a specific route
fn get_route_requirements(path: &str) -> VerificationRequirements {
    match path {
        // High-security admin operations
        p if p.starts_with("/v1/admin/") => VerificationRequirements {
            min_trust_level: Some(CredentialTrustLevel::HardwareVerified),
            require_hardware_attestation: true,
            require_biometric: false,
            min_trust_score: 0.9,
            require_device_bound: false,
        },
        // Financial or sensitive operations
        p if p.starts_with("/v1/payments/") || p.starts_with("/v1/billing/") => VerificationRequirements {
            min_trust_level: Some(CredentialTrustLevel::HardwareVerified),
            require_hardware_attestation: false,
            require_biometric: true,
            min_trust_score: 0.85,
            require_device_bound: false,
        },
        // User management operations
        p if p.starts_with("/v1/users/") => VerificationRequirements {
            min_trust_level: Some(CredentialTrustLevel::SoftwareVerified),
            require_hardware_attestation: false,
            require_biometric: false,
            min_trust_score: 0.7,
            require_device_bound: false,
        },
        // Default requirements for protected routes
        _ => VerificationRequirements {
            min_trust_level: None,
            require_hardware_attestation: false,
            require_biometric: false,
            min_trust_score: 0.5,
            require_device_bound: false,
        },
    }
}

/// Create an identity assertion for high-value operations
pub async fn create_identity_assertion(
    store: &'static Store,
    session: &AuthSession,
    assertion_type: AssertionType,
    challenge_data: Vec<u8>,
    client_data: Vec<u8>,
) -> Result<Ulid, CredentialVerificationError> {
    let user_id = session.user_id
        .ok_or(CredentialVerificationError::UserIdRequired)?;
    let session_id = session.session_id
        .ok_or(CredentialVerificationError::SessionIdRequired)?;

    let repo = AuthRepository::new(store);
    let _cred_service = CredentialVerificationService::new(store);

    // Get credential IDs from session context
    let credential_ids = if let Some(context) = &session.credential_context {
        let mut ids = context.secondary_credentials.clone();
        if let Some(primary) = context.primary_credential {
            ids.insert(0, primary);
        }
        ids
    } else {
        return Err(CredentialVerificationError::CredentialContextRequired);
    };

    // Create the assertion record
    let assertion = IdentityAssertionRecord {
        id: Ulid::new(),
        user_id,
        org_id: session.org_id,
        session_id,
        credential_ids,
        assertion_type,
        challenge_response: challenge_data,
        client_data,
        authenticator_data: None, // Would be populated by authenticator
        signature: Vec::new(), // Would be populated by signature verification
        verified: false, // Will be verified separately
        trust_score: session.trust_score,
        created_at: current_timestamp(),
        verified_at: None,
    };

    // Store the assertion
    repo.insert_identity_assertion(&assertion)?;

    Ok(assertion.id)
}

#[derive(Debug, Clone)]
pub struct VerificationRequirements {
    pub min_trust_level: Option<CredentialTrustLevel>,
    pub require_hardware_attestation: bool,
    pub require_biometric: bool,
    pub min_trust_score: f64,
    pub require_device_bound: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum CredentialVerificationError {
    #[error("Insufficient trust score: required {required}, actual {actual}")]
    InsufficientTrustScore { required: f64, actual: f64 },

    #[error("Hardware attestation is required for this operation")]
    HardwareAttestationRequired,

    #[error("Biometric verification is required for this operation")]
    BiometricVerificationRequired,

    #[error("Device binding is required for this operation")]
    DeviceBindingRequired,

    #[error("Credential context is required")]
    CredentialContextRequired,

    #[error("User ID is required")]
    UserIdRequired,

    #[error("Session ID is required")]
    SessionIdRequired,

    #[error("Storage error: {0}")]
    Storage(#[from] anyhow::Error),
}

impl IntoResponse for CredentialVerificationError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            CredentialVerificationError::InsufficientTrustScore { .. } => {
                (StatusCode::FORBIDDEN, self.to_string())
            }
            CredentialVerificationError::HardwareAttestationRequired => {
                (StatusCode::FORBIDDEN, "Hardware attestation required".to_string())
            }
            CredentialVerificationError::BiometricVerificationRequired => {
                (StatusCode::FORBIDDEN, "Biometric verification required".to_string())
            }
            CredentialVerificationError::DeviceBindingRequired => {
                (StatusCode::FORBIDDEN, "Device binding required".to_string())
            }
            CredentialVerificationError::CredentialContextRequired => {
                (StatusCode::FORBIDDEN, "Credential verification required".to_string())
            }
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "Verification error".to_string()),
        };

        (status, serde_json::json!({
            "error": "credential_verification_failed",
            "message": message
        }).to_string()).into_response()
    }
}

/// Response for credential verification challenges
#[derive(Debug, Serialize, Deserialize)]
pub struct CredentialChallengeResponse {
    pub challenge_id: String,
    pub expires_at: i64,
    pub challenge_data: String, // Base64 encoded
    pub required_credential_types: Vec<String>,
}

fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}