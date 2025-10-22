use super::*;
use crate::{AuthSession, AuthState};
use anyhow::Result;
use axum::{
    extract::{State, Json, Path, Query},
    response::Json as ResponseJson,
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Biometric API service for handling HTTP endpoints
pub struct BiometricApi {
    template_generator: CancelableTemplateGenerator,
    liveness_detector: LivenessDetector,
    matching_engine: BiometricMatchingEngine,
    key_derivation_system: BiometricKeyDerivationSystem,
    privacy_system: PrivacyPreservingBiometricSystem,
    multimodal_system: MultimodalBiometricSystem,
    hsm_integration: Option<BiometricHSMIntegration>,
}

impl Default for BiometricApi {
    fn default() -> Self {
        Self::new().expect("Failed to create BiometricApi")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BiometricEnrollmentRequest {
    pub modality: BiometricModality,
    pub biometric_data: Vec<u8>,
    pub quality_requirements: Option<QualityRequirements>,
    pub transform_method: Option<String>,
    pub privacy_level: Option<PrivacyLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultimodalEnrollmentRequest {
    pub biometric_samples: Vec<BiometricSample>,
    pub fusion_preferences: FusionPreferences,
    pub privacy_level: PrivacyLevel,
    pub security_level: SecurityLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BiometricSample {
    pub modality: BiometricModality,
    pub data: Vec<u8>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FusionPreferences {
    pub preferred_modalities: Vec<BiometricModality>,
    pub fallback_modalities: Vec<BiometricModality>,
    pub fusion_strategy: String,
    pub quality_threshold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BiometricAuthenticationRequest {
    pub template_id: String,
    pub biometric_data: Vec<u8>,
    pub challenge_response: Option<ChallengeResponseData>,
    pub device_info: Option<DeviceInfo>,
    pub require_liveness: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultimodalAuthenticationRequest {
    pub template_id: String,
    pub biometric_samples: Vec<BiometricSample>,
    pub challenge_responses: Vec<ChallengeResponseData>,
    pub security_level: SecurityLevel,
    pub context: AuthenticationContextRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChallengeResponseData {
    pub challenge_id: String,
    pub response_data: Vec<u8>,
    pub timestamp: u64,
    pub anti_replay_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticationContextRequest {
    pub device_id: String,
    pub location: LocationInfo,
    pub environment_factors: EnvironmentFactors,
    pub user_preferences: UserPreferences,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivenessChallengeRequest {
    pub modality: BiometricModality,
    pub timeout_seconds: u64,
    pub challenge_type: LivenessType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyDerivationRequest {
    pub template_id: String,
    pub key_purpose: KeyPurpose,
    pub key_length: u32,
    pub derivation_parameters: DerivationParameters,
}

// Response types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BiometricEnrollmentResponse {
    pub template_id: String,
    pub quality_metrics: QualityMetrics,
    pub enrollment_status: EnrollmentStatus,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultimodalEnrollmentResponse {
    pub template_id: String,
    pub modality_results: Vec<ModalityEnrollmentResult>,
    pub fusion_metadata: FusionMetadata,
    pub overall_quality: f64,
    pub enrollment_status: EnrollmentStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModalityEnrollmentResult {
    pub modality: BiometricModality,
    pub template_id: String,
    pub quality_metrics: QualityMetrics,
    pub status: EnrollmentStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum EnrollmentStatus {
    Success,
    QualityTooLow,
    LivenessFailure,
    TemplateExists,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BiometricAuthenticationResponse {
    pub is_authenticated: bool,
    pub confidence_score: f64,
    pub matching_details: MatchingDetails,
    pub liveness_verified: bool,
    pub authentication_time_ms: f64,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultimodalAuthenticationResponse {
    pub is_authenticated: bool,
    pub confidence_score: f64,
    pub modality_results: Vec<ModalityAuthenticationResult>,
    pub fusion_result: FusedMultimodalScore,
    pub risk_assessment: RiskAssessment,
    pub authentication_time_ms: f64,
    pub decision_factors: Vec<(String, f64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModalityAuthenticationResult {
    pub modality: BiometricModality,
    pub is_match: bool,
    pub confidence_score: f64,
    pub quality_impact: f64,
    pub liveness_verified: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchingDetails {
    pub match_score: f64,
    pub decision_threshold: f64,
    pub quality_impact: f64,
    pub feature_contributions: Vec<FeatureContribution>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivenessChallengeResponse {
    pub challenge_id: String,
    pub challenge_type: LivenessType,
    pub challenge_data: Vec<u8>,
    pub expires_at: u64,
    pub anti_replay_token: String,
    pub instructions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyDerivationResponse {
    pub key_id: String,
    pub key_strength: u32,
    pub derivation_method: String,
    pub key_handle: Option<String>, // HSM key handle if applicable
}

#[derive(Debug, Deserialize)]
pub struct BiometricQuery {
    pub include_quality: Option<bool>,
    pub include_metadata: Option<bool>,
    pub modality_filter: Option<String>,
}

impl BiometricApi {
    pub fn new() -> Result<Self> {
        Ok(Self {
            template_generator: CancelableTemplateGenerator::new(),
            liveness_detector: LivenessDetector::new(),
            matching_engine: BiometricMatchingEngine::new(),
            key_derivation_system: BiometricKeyDerivationSystem::new()?,
            privacy_system: PrivacyPreservingBiometricSystem::new(PrivacyLevel::Enhanced)?,
            multimodal_system: MultimodalBiometricSystem::new()?,
            hsm_integration: None, // Would be initialized if HSM is available
        })
    }

    pub fn with_hsm(mut self, hsm_integration: BiometricHSMIntegration) -> Self {
        self.hsm_integration = Some(hsm_integration);
        self
    }

    /// Enroll a single biometric modality
    pub async fn enroll_biometric(
        &mut self,
        auth_session: AuthSession,
        request: BiometricEnrollmentRequest,
    ) -> Result<BiometricEnrollmentResponse, BiometricApiError> {
        // Validate authentication
        if auth_session.user_id.is_none() {
            return Err(BiometricApiError::Unauthorized);
        }

        let user_id = auth_session.user_id.unwrap();

        // Check if template already exists for this modality
        // In a real implementation, this would check the database

        // Generate biometric template
        let transform_method = request.transform_method.unwrap_or_else(|| "biohash".to_string());

        let template = self.template_generator.generate_template(
            user_id,
            auth_session.org_id,
            request.modality.clone(),
            &request.biometric_data,
            &transform_method,
        ).map_err(|e| BiometricApiError::TemplateGeneration(BiometricError::TemplateGeneration(e.to_string())))?;

        // Check quality requirements
        let quality_requirements = request.quality_requirements.unwrap_or(QualityRequirements {
            minimum_quality: 0.7,
            quality_weight: 1.0,
            require_liveness: false,
        });

        let enrollment_status = if template.quality_metrics.overall_quality < quality_requirements.minimum_quality {
            EnrollmentStatus::QualityTooLow
        } else {
            EnrollmentStatus::Success
        };

        // Generate recommendations based on quality
        let recommendations = self.generate_enrollment_recommendations(&template, &quality_requirements);

        // Store template (in real implementation, would save to database)
        tracing::info!(
            "Biometric enrollment completed for user {} with modality {:?}",
            user_id,
            request.modality
        );

        Ok(BiometricEnrollmentResponse {
            template_id: template.template_id.to_string(),
            quality_metrics: template.quality_metrics,
            enrollment_status,
            recommendations,
        })
    }

    /// Enroll multiple biometric modalities (multimodal enrollment)
    pub async fn enroll_multimodal(
        &mut self,
        auth_session: AuthSession,
        request: MultimodalEnrollmentRequest,
    ) -> Result<MultimodalEnrollmentResponse, BiometricApiError> {
        if auth_session.user_id.is_none() {
            return Err(BiometricApiError::Unauthorized);
        }

        let user_id = auth_session.user_id.unwrap();

        // Prepare biometric data
        let biometric_data: Vec<(BiometricModality, Vec<u8>)> = request.biometric_samples
            .into_iter()
            .map(|sample| (sample.modality, sample.data))
            .collect();

        // Create fusion context
        let fusion_context = FusionContext {
            user_id,
            security_level: request.security_level,
            environment_factors: EnvironmentFactors {
                lighting_conditions: 0.8,
                noise_level: 0.2,
                device_stability: 0.9,
                network_quality: 0.8,
            },
            user_preferences: UserPreferences {
                preferred_modalities: request.fusion_preferences.preferred_modalities,
                fallback_modalities: request.fusion_preferences.fallback_modalities,
                max_enrollment_time: 300,
                privacy_level: request.privacy_level,
            },
            quality_requirements: QualityRequirements {
                minimum_quality: request.fusion_preferences.quality_threshold,
                quality_weight: 1.0,
                require_liveness: false,
            },
        };

        // Perform multimodal enrollment
        let multimodal_template = self.multimodal_system.enroll_multimodal_user(
            user_id,
            auth_session.org_id,
            biometric_data,
            fusion_context,
        ).map_err(|e| BiometricApiError::MultimodalEnrollment(BiometricError::TemplateGeneration(e.to_string())))?;

        // Generate individual modality results
        let modality_results: Vec<ModalityEnrollmentResult> = multimodal_template.modality_templates
            .iter()
            .zip(multimodal_template.quality_metrics.modality_qualities.iter())
            .map(|(template, quality)| ModalityEnrollmentResult {
                modality: template.modality.clone(),
                template_id: template.template_id.to_string(),
                quality_metrics: quality.clone(),
                status: if quality.overall_quality >= request.fusion_preferences.quality_threshold {
                    EnrollmentStatus::Success
                } else {
                    EnrollmentStatus::QualityTooLow
                },
            })
            .collect();

        let enrollment_status = if modality_results.iter().all(|r| r.status == EnrollmentStatus::Success) {
            EnrollmentStatus::Success
        } else {
            EnrollmentStatus::QualityTooLow
        };

        Ok(MultimodalEnrollmentResponse {
            template_id: multimodal_template.template_id.to_string(),
            modality_results,
            fusion_metadata: multimodal_template.fusion_metadata,
            overall_quality: multimodal_template.quality_metrics.overall_quality,
            enrollment_status,
        })
    }

    /// Generate a liveness challenge
    pub async fn create_liveness_challenge(
        &mut self,
        _auth_session: AuthSession,
        request: LivenessChallengeRequest,
    ) -> Result<LivenessChallengeResponse, BiometricApiError> {
        let challenge = self.liveness_detector.create_challenge(
            &request.modality,
            request.timeout_seconds,
        ).map_err(|e| BiometricApiError::LivenessChallenge(BiometricError::LivenessFailure(e.to_string())))?;

        let instructions = self.generate_challenge_instructions(&challenge);

        Ok(LivenessChallengeResponse {
            challenge_id: challenge.challenge_id.to_string(),
            challenge_type: request.challenge_type,
            challenge_data: challenge.challenge_data,
            expires_at: challenge.expires_at,
            anti_replay_token: challenge.anti_replay_token,
            instructions,
        })
    }

    /// Authenticate using a single biometric modality
    pub async fn authenticate_biometric(
        &mut self,
        auth_session: AuthSession,
        request: BiometricAuthenticationRequest,
    ) -> Result<BiometricAuthenticationResponse, BiometricApiError> {
        let start_time = std::time::Instant::now();

        // Parse template ID
        let template_id = Ulid::from_string(&request.template_id)
            .map_err(|_| BiometricApiError::InvalidTemplate)?;

        // In a real implementation, would retrieve template from database
        // For now, we'll simulate with a basic template
        let stored_template = BiometricTemplate {
            template_id,
            user_id: auth_session.user_id.unwrap_or(Ulid::new()),
            org_id: auth_session.org_id,
            modality: BiometricModality::Face, // Would be from database
            protected_template: vec![128u8; 256], // Simulated
            transform_parameters: TransformParameters {
                transform_id: "biohash".to_string(),
                parameters: vec![128, 0, 0, 1],
                salt: [0u8; 32],
                iteration_count: 1000,
            },
            quality_metrics: QualityMetrics {
                overall_quality: 0.85,
                uniformity: 0.8,
                sharpness: 0.9,
                contrast: 0.8,
                modality_specific: vec![],
            },
            created_at: current_timestamp(),
            updated_at: current_timestamp(),
            version: 1,
        };

        // Generate query template
        let query_template = self.template_generator.generate_template(
            stored_template.user_id,
            stored_template.org_id,
            stored_template.modality.clone(),
            &request.biometric_data,
            "biohash",
        ).map_err(|e| BiometricApiError::TemplateGeneration(BiometricError::TemplateGeneration(e.to_string())))?;

        // Perform liveness detection if required
        let mut liveness_verified = false;
        if request.require_liveness {
            if let Some(challenge_response) = request.challenge_response {
                let challenge = BiometricChallenge {
                    challenge_id: Ulid::from_string(&challenge_response.challenge_id)
                        .map_err(|_| BiometricApiError::InvalidChallenge)?,
                    modality: stored_template.modality.clone(),
                    challenge_data: vec![],
                    created_at: current_timestamp() - 30,
                    expires_at: current_timestamp() + 300,
                    nonce: [0u8; 32],
                    anti_replay_token: challenge_response.anti_replay_token,
                };

                let response = BiometricResponse {
                    challenge_id: challenge.challenge_id,
                    biometric_data: challenge_response.response_data,
                    liveness_proof: LivenessProof {
                        proof_type: LivenessType::ActiveChallenge,
                        proof_data: vec![],
                        confidence_score: 0.9,
                        metadata: vec![],
                    },
                    timestamp: challenge_response.timestamp,
                    device_attestation: None,
                    anti_replay_token: challenge.anti_replay_token.clone(),
                };

                match self.liveness_detector.verify_liveness(&challenge, &response) {
                    Ok(_) => liveness_verified = true,
                    Err(_) => return Err(BiometricApiError::LivenessFailure),
                }
            } else {
                return Err(BiometricApiError::LivenessRequired);
            }
        }

        // Perform matching
        let matching_results = self.matching_engine.match_biometrics(
            &query_template,
            &[stored_template],
            SecurityLevel::Medium,
        ).map_err(|e| BiometricApiError::MatchingFailed(e))?;

        let matching_result = matching_results.into_iter().next()
            .ok_or_else(|| BiometricApiError::MatchingFailed(anyhow::anyhow!("No matching result found")))?;

        let authentication_time = start_time.elapsed().as_millis() as f64;

        // Generate recommendations
        let recommendations = self.generate_authentication_recommendations(&matching_result);

        Ok(BiometricAuthenticationResponse {
            is_authenticated: matching_result.is_match,
            confidence_score: matching_result.confidence_score,
            matching_details: MatchingDetails {
                match_score: matching_result.match_score,
                decision_threshold: matching_result.decision_threshold,
                quality_impact: 0.0, // Would be computed from quality metrics
                feature_contributions: vec![], // Would be extracted from matching metadata
            },
            liveness_verified,
            authentication_time_ms: authentication_time,
            recommendations,
        })
    }

    /// Authenticate using multiple biometric modalities
    pub async fn authenticate_multimodal(
        &mut self,
        auth_session: AuthSession,
        request: MultimodalAuthenticationRequest,
    ) -> Result<MultimodalAuthenticationResponse, BiometricApiError> {
        if auth_session.user_id.is_none() {
            return Err(BiometricApiError::Unauthorized);
        }

        // Parse template ID
        let template_id = Ulid::from_string(&request.template_id)
            .map_err(|_| BiometricApiError::InvalidTemplate)?;

        // In a real implementation, would retrieve multimodal template from database
        // For now, simulate with basic templates
        let stored_template = MultimodalTemplate {
            template_id,
            user_id: auth_session.user_id.unwrap(),
            org_id: auth_session.org_id,
            modality_templates: vec![], // Would be populated from database
            fusion_metadata: FusionMetadata {
                fusion_strategy: "quality_weighted".to_string(),
                modality_weights: vec![0.5, 0.5],
                correlation_matrix: vec![vec![1.0, 0.2], vec![0.2, 1.0]],
                redundancy_scores: vec![0.1, 0.1],
                distinctiveness_scores: vec![0.9, 0.9],
            },
            quality_metrics: MultimodalQualityMetrics {
                overall_quality: 0.85,
                modality_qualities: vec![],
                cross_modal_consistency: 0.9,
                template_stability: 0.8,
                discriminability: 0.9,
            },
            created_at: current_timestamp(),
            updated_at: current_timestamp(),
            version: 1,
        };

        // Prepare query data
        let query_data: Vec<(BiometricModality, Vec<u8>)> = request.biometric_samples
            .into_iter()
            .map(|sample| (sample.modality, sample.data))
            .collect();

        // Create fusion context
        let fusion_context = FusionContext {
            user_id: auth_session.user_id.unwrap(),
            security_level: request.security_level,
            environment_factors: request.context.environment_factors,
            user_preferences: request.context.user_preferences,
            quality_requirements: QualityRequirements {
                minimum_quality: 0.7,
                quality_weight: 1.0,
                require_liveness: !request.challenge_responses.is_empty(),
            },
        };

        // Perform multimodal authentication
        let auth_result = self.multimodal_system.authenticate_multimodal(
            query_data,
            &stored_template,
            fusion_context,
        ).map_err(|e| BiometricApiError::MultimodalAuthentication(BiometricError::TemplateGeneration(e.to_string())))?;

        // Convert modality results
        let modality_results: Vec<ModalityAuthenticationResult> = auth_result.modality_results
            .into_iter()
            .map(|score| ModalityAuthenticationResult {
                modality: score.modality,
                is_match: score.similarity_score > 0.8,
                confidence_score: score.confidence,
                quality_impact: score.quality_impact,
                liveness_verified: true, // Would be computed from liveness results
            })
            .collect();

        Ok(MultimodalAuthenticationResponse {
            is_authenticated: auth_result.is_authenticated,
            confidence_score: auth_result.confidence_score,
            modality_results,
            fusion_result: auth_result.fusion_result,
            risk_assessment: auth_result.risk_assessment,
            authentication_time_ms: auth_result.authentication_time_ms,
            decision_factors: auth_result.decision_factors,
        })
    }

    /// Derive a cryptographic key from a biometric template
    pub async fn derive_biometric_key(
        &mut self,
        auth_session: AuthSession,
        request: KeyDerivationRequest,
    ) -> Result<KeyDerivationResponse, BiometricApiError> {
        if auth_session.user_id.is_none() {
            return Err(BiometricApiError::Unauthorized);
        }

        // Parse template ID
        let template_id = Ulid::from_string(&request.template_id)
            .map_err(|_| BiometricApiError::InvalidTemplate)?;

        // In a real implementation, would retrieve template from database
        let template = BiometricTemplate {
            template_id,
            user_id: auth_session.user_id.unwrap(),
            org_id: auth_session.org_id,
            modality: BiometricModality::Face, // Would be from database
            protected_template: vec![128u8; 256], // Simulated
            transform_parameters: TransformParameters {
                transform_id: "biohash".to_string(),
                parameters: vec![128, 0, 0, 1],
                salt: [0u8; 32],
                iteration_count: 1000,
            },
            quality_metrics: QualityMetrics {
                overall_quality: 0.85,
                uniformity: 0.8,
                sharpness: 0.9,
                contrast: 0.8,
                modality_specific: vec![],
            },
            created_at: current_timestamp(),
            updated_at: current_timestamp(),
            version: 1,
        };

        // Create key derivation context
        let key_context = KeyDerivationContext {
            key_purpose: request.key_purpose,
            key_length: request.key_length,
            derivation_parameters: request.derivation_parameters,
            security_level: KeySecurityLevel::Standard,
            salt: vec![0u8; 32], // Would be randomly generated
        };

        // Derive key
        let derived_key = self.key_derivation_system.derive_biometric_key(
            &template,
            key_context,
        ).map_err(|e| BiometricApiError::KeyDerivation(BiometricError::KeyDerivationFailure(e.to_string())))?;

        Ok(KeyDerivationResponse {
            key_id: derived_key.key_id.to_string(),
            key_strength: derived_key.key_strength,
            derivation_method: derived_key.derivation_method,
            key_handle: None, // Would include HSM handle if using HSM
        })
    }

    // Helper methods
    fn generate_enrollment_recommendations(
        &self,
        template: &BiometricTemplate,
        requirements: &QualityRequirements,
    ) -> Vec<String> {
        let mut recommendations = Vec::new();

        if template.quality_metrics.overall_quality < requirements.minimum_quality {
            recommendations.push("Improve sample quality by ensuring good lighting and stable positioning".to_string());
        }

        if template.quality_metrics.sharpness < 0.7 {
            recommendations.push("Ensure the biometric sensor is clean and properly focused".to_string());
        }

        if template.quality_metrics.contrast < 0.6 {
            recommendations.push("Improve lighting conditions for better contrast".to_string());
        }

        match template.modality {
            BiometricModality::Face => {
                recommendations.push("Look directly at the camera and avoid shadows".to_string());
            },
            BiometricModality::Fingerprint => {
                recommendations.push("Press firmly but not too hard on the sensor".to_string());
            },
            BiometricModality::Voice => {
                recommendations.push("Speak clearly in a quiet environment".to_string());
            },
            _ => {}
        }

        recommendations
    }

    fn generate_challenge_instructions(&self, challenge: &BiometricChallenge) -> Vec<String> {
        match challenge.modality {
            BiometricModality::Face => vec![
                "Look directly at the camera".to_string(),
                "Follow the on-screen prompts for head movements".to_string(),
                "Ensure good lighting on your face".to_string(),
            ],
            BiometricModality::Voice => vec![
                "Speak the displayed phrase clearly".to_string(),
                "Use your normal speaking voice".to_string(),
                "Minimize background noise".to_string(),
            ],
            BiometricModality::Fingerprint => vec![
                "Place your finger on the sensor".to_string(),
                "Apply steady pressure".to_string(),
                "Hold still until prompted to lift".to_string(),
            ],
            _ => vec!["Follow the on-screen instructions".to_string()],
        }
    }

    fn generate_authentication_recommendations(&self, result: &MatchingResult) -> Vec<String> {
        let mut recommendations = Vec::new();

        if !result.is_match && result.confidence_score > 0.6 {
            recommendations.push("Try again with better positioning or lighting".to_string());
        }

        if result.confidence_score < 0.8 {
            recommendations.push("Consider enrolling additional biometric samples for better accuracy".to_string());
        }

        recommendations
    }
}

/// Error types for the Biometric API
#[derive(Debug, thiserror::Error)]
pub enum BiometricApiError {
    #[error("Unauthorized")]
    Unauthorized,

    #[error("Invalid template ID")]
    InvalidTemplate,

    #[error("Invalid challenge ID")]
    InvalidChallenge,

    #[error("Template generation failed: {0}")]
    TemplateGeneration(#[from] BiometricError),

    #[error("Liveness challenge failed: {0}")]
    LivenessChallenge(BiometricError),

    #[error("Liveness detection failed")]
    LivenessFailure,

    #[error("Liveness detection required")]
    LivenessRequired,

    #[error("Matching failed: {0}")]
    MatchingFailed(#[from] anyhow::Error),

    #[error("Key derivation failed: {0}")]
    KeyDerivation(BiometricError),

    #[error("Multimodal enrollment failed: {0}")]
    MultimodalEnrollment(BiometricError),

    #[error("Multimodal authentication failed: {0}")]
    MultimodalAuthentication(BiometricError),

    #[error("HSM operation failed: {0}")]
    HsmOperation(BiometricError),

    #[error("Internal server error: {0}")]
    Internal(String),
}

impl axum::response::IntoResponse for BiometricApiError {
    fn into_response(self) -> axum::response::Response {
        use axum::response::Json;

        let (status, error_message) = match self {
            BiometricApiError::Unauthorized => (StatusCode::UNAUTHORIZED, self.to_string()),
            BiometricApiError::InvalidTemplate |
            BiometricApiError::InvalidChallenge => (StatusCode::BAD_REQUEST, self.to_string()),
            BiometricApiError::LivenessRequired => (StatusCode::BAD_REQUEST, self.to_string()),
            BiometricApiError::LivenessFailure => (StatusCode::FORBIDDEN, self.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };

        let body = Json(serde_json::json!({
            "error": error_message,
            "code": status.as_u16()
        }));

        (status, body).into_response()
    }
}

// Axum route handlers
pub async fn enroll_biometric_handler(
    auth_session: AuthSession,
    State(_state): State<AuthState>,
    Json(request): Json<BiometricEnrollmentRequest>,
) -> Result<ResponseJson<BiometricEnrollmentResponse>, BiometricApiError> {
    let mut api = BiometricApi::new().map_err(|e| BiometricApiError::Internal(e.to_string()))?;
    let response = api.enroll_biometric(auth_session, request).await?;
    Ok(ResponseJson(response))
}

pub async fn enroll_multimodal_handler(
    auth_session: AuthSession,
    State(_state): State<AuthState>,
    Json(request): Json<MultimodalEnrollmentRequest>,
) -> Result<ResponseJson<MultimodalEnrollmentResponse>, BiometricApiError> {
    let mut api = BiometricApi::new().map_err(|e| BiometricApiError::Internal(e.to_string()))?;
    let response = api.enroll_multimodal(auth_session, request).await?;
    Ok(ResponseJson(response))
}

pub async fn create_liveness_challenge_handler(
    auth_session: AuthSession,
    State(_state): State<AuthState>,
    Json(request): Json<LivenessChallengeRequest>,
) -> Result<ResponseJson<LivenessChallengeResponse>, BiometricApiError> {
    let mut api = BiometricApi::new().map_err(|e| BiometricApiError::Internal(e.to_string()))?;
    let response = api.create_liveness_challenge(auth_session, request).await?;
    Ok(ResponseJson(response))
}

pub async fn authenticate_biometric_handler(
    auth_session: AuthSession,
    State(_state): State<AuthState>,
    Json(request): Json<BiometricAuthenticationRequest>,
) -> Result<ResponseJson<BiometricAuthenticationResponse>, BiometricApiError> {
    let mut api = BiometricApi::new().map_err(|e| BiometricApiError::Internal(e.to_string()))?;
    let response = api.authenticate_biometric(auth_session, request).await?;
    Ok(ResponseJson(response))
}

pub async fn authenticate_multimodal_handler(
    auth_session: AuthSession,
    State(_state): State<AuthState>,
    Json(request): Json<MultimodalAuthenticationRequest>,
) -> Result<ResponseJson<MultimodalAuthenticationResponse>, BiometricApiError> {
    let mut api = BiometricApi::new().map_err(|e| BiometricApiError::Internal(e.to_string()))?;
    let response = api.authenticate_multimodal(auth_session, request).await?;
    Ok(ResponseJson(response))
}

pub async fn derive_key_handler(
    auth_session: AuthSession,
    State(_state): State<AuthState>,
    Json(request): Json<KeyDerivationRequest>,
) -> Result<ResponseJson<KeyDerivationResponse>, BiometricApiError> {
    let mut api = BiometricApi::new().map_err(|e| BiometricApiError::Internal(e.to_string()))?;
    let response = api.derive_biometric_key(auth_session, request).await?;
    Ok(ResponseJson(response))
}

pub async fn get_biometric_info_handler(
    auth_session: AuthSession,
    Path(template_id): Path<String>,
    Query(_query): Query<BiometricQuery>,
) -> Result<ResponseJson<serde_json::Value>, BiometricApiError> {
    // In a real implementation, would retrieve template info from database
    let info = serde_json::json!({
        "template_id": template_id,
        "user_id": auth_session.user_id.map(|id| id.to_string()),
        "created_at": current_timestamp(),
        "modalities": ["face", "fingerprint"],
        "status": "active"
    });

    Ok(ResponseJson(info))
}

pub async fn delete_biometric_template_handler(
    auth_session: AuthSession,
    Path(template_id): Path<String>,
) -> Result<ResponseJson<serde_json::Value>, BiometricApiError> {
    // In a real implementation, would delete template from database
    tracing::info!(
        "Biometric template {} deleted for user {:?}",
        template_id,
        auth_session.user_id
    );

    Ok(ResponseJson(serde_json::json!({
        "template_id": template_id,
        "status": "deleted"
    })))
}

/// Create router for biometric API endpoints
pub fn create_biometric_routes() -> axum::Router<AuthState> {
    use axum::routing::{get, post, delete};

    axum::Router::new()
        .route("/biometric/enroll", post(enroll_biometric_handler))
        .route("/biometric/enroll/multimodal", post(enroll_multimodal_handler))
        .route("/biometric/challenge", post(create_liveness_challenge_handler))
        .route("/biometric/authenticate", post(authenticate_biometric_handler))
        .route("/biometric/authenticate/multimodal", post(authenticate_multimodal_handler))
        .route("/biometric/derive-key", post(derive_key_handler))
        .route("/biometric/template/:template_id", get(get_biometric_info_handler))
        .route("/biometric/template/:template_id", delete(delete_biometric_template_handler))
}