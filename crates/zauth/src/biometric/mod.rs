mod templates;
mod liveness;
mod privacy;
mod matching;
mod keys;
mod hsm;
mod api;
mod multimodal;

pub use templates::*;
pub use liveness::*;
pub use privacy::*;
pub use matching::*;
pub use keys::*;
pub use hsm::*;
pub use api::*;
pub use multimodal::*;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;
use std::time::{SystemTime, UNIX_EPOCH};
use anyhow::Result;

#[derive(Error, Debug)]
pub enum BiometricError {
    #[error("Invalid biometric template format")]
    InvalidTemplate,

    #[error("Liveness detection failed: {0}")]
    LivenessFailure(String),

    #[error("Template generation failed: {0}")]
    TemplateGeneration(String),

    #[error("Matching threshold not met")]
    MatchingFailed,

    #[error("HSM operation failed: {0}")]
    HsmFailure(String),

    #[error("Challenge expired")]
    ChallengeExpired,

    #[error("Anti-spoofing check failed")]
    AntiSpoofingFailure,

    #[error("Replay attack detected")]
    ReplayAttack,

    #[error("Biometric modality not supported: {0}")]
    UnsupportedModality(String),

    #[error("Cryptographic operation failed: {0}")]
    CryptoError(String),

    #[error("Hardware security module unavailable")]
    HsmUnavailable,

    #[error("Template protection failed: {0}")]
    TemplateProtectionFailure(String),

    #[error("Key derivation failed: {0}")]
    KeyDerivationFailure(String),

    #[error("Biometric data format error: {0}")]
    DataFormatError(String),

    #[error("Privacy violation detected: {0}")]
    PrivacyViolation(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum BiometricModality {
    Face,
    Fingerprint,
    Voice,
    Iris,
    Palmprint,
    HandGeometry,
    Multimodal(Vec<BiometricModality>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BiometricChallenge {
    pub challenge_id: Ulid,
    pub modality: BiometricModality,
    pub challenge_data: Vec<u8>,
    pub created_at: u64,
    pub expires_at: u64,
    pub nonce: [u8; 32],
    pub anti_replay_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BiometricResponse {
    pub challenge_id: Ulid,
    pub biometric_data: Vec<u8>,
    pub liveness_proof: LivenessProof,
    pub timestamp: u64,
    pub device_attestation: Option<DeviceAttestation>,
    pub anti_replay_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivenessProof {
    pub proof_type: LivenessType,
    pub proof_data: Vec<u8>,
    pub confidence_score: f64,
    pub metadata: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LivenessType {
    ActiveChallenge,
    PassiveDetection,
    HardwareBacked,
    Multimodal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceAttestation {
    pub platform: String,
    pub attestation_data: Vec<u8>,
    pub signature: Vec<u8>,
    pub certificate_chain: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BiometricTemplate {
    pub template_id: Ulid,
    pub user_id: Ulid,
    pub org_id: u64,
    pub modality: BiometricModality,
    pub protected_template: Vec<u8>,
    pub transform_parameters: TransformParameters,
    pub quality_metrics: QualityMetrics,
    pub created_at: u64,
    pub updated_at: u64,
    pub version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformParameters {
    pub transform_id: String,
    pub parameters: Vec<u8>,
    pub salt: [u8; 32],
    pub iteration_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityMetrics {
    pub overall_quality: f64,
    pub uniformity: f64,
    pub sharpness: f64,
    pub contrast: f64,
    pub modality_specific: Vec<(String, f64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchingResult {
    pub is_match: bool,
    pub confidence_score: f64,
    pub match_score: f64,
    pub decision_threshold: f64,
    pub template_id: Ulid,
    pub matching_metadata: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BiometricKey {
    pub key_id: Ulid,
    pub derived_key: Vec<u8>,
    pub key_strength: u32,
    pub derivation_method: String,
    pub template_id: Ulid,
}

impl BiometricChallenge {
    pub fn new(modality: BiometricModality, timeout_secs: u64) -> Result<Self> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut nonce = [0u8; 32];
        use rand_core::{OsRng, RngCore};
        OsRng.fill_bytes(&mut nonce);

        let anti_replay_token = format!("{}-{}", Ulid::new(), hex::encode(&nonce[..16]));

        let challenge_data = Self::generate_challenge_data(&modality)?;

        Ok(Self {
            challenge_id: Ulid::new(),
            modality,
            challenge_data,
            created_at: now,
            expires_at: now + timeout_secs,
            nonce,
            anti_replay_token,
        })
    }

    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now > self.expires_at
    }

    fn generate_challenge_data(modality: &BiometricModality) -> Result<Vec<u8>> {
        use rand_core::{OsRng, RngCore};
        let mut rng = OsRng;

        match modality {
            BiometricModality::Face => {
                // Generate random head pose challenge
                let mut challenge = vec![0u8; 16];
                rng.fill_bytes(&mut challenge);
                // First 4 bytes: yaw angle (-45 to 45 degrees)
                // Next 4 bytes: pitch angle (-30 to 30 degrees)
                // Next 4 bytes: roll angle (-15 to 15 degrees)
                // Last 4 bytes: sequence timing
                Ok(challenge)
            },
            BiometricModality::Voice => {
                // Generate random passphrase challenge
                let phrases = [
                    b"The quick brown fox jumps over the lazy dog",
                    b"Pack my box with five dozen liquor jugs!xxx",
                    b"How vexingly quick daft zebras jump xxxxxxx",
                    b"Waltz, bad nymph, for quick jigs vex xxxxxx",
                ];
                let phrase = phrases[rng.next_u32() as usize % phrases.len()];
                let mut challenge = phrase.to_vec();
                // Add random timing requirements
                let mut timing = [0u8; 8];
                rng.fill_bytes(&mut timing);
                challenge.extend_from_slice(&timing);
                Ok(challenge)
            },
            BiometricModality::Fingerprint => {
                // Generate finger position challenge
                let mut challenge = vec![0u8; 8];
                rng.fill_bytes(&mut challenge);
                // Specify required finger positions and pressure
                Ok(challenge)
            },
            BiometricModality::Iris => {
                // Generate gaze direction challenge
                let mut challenge = vec![0u8; 12];
                rng.fill_bytes(&mut challenge);
                Ok(challenge)
            },
            BiometricModality::Multimodal(modalities) => {
                let mut combined_challenge = Vec::new();
                for modality in modalities {
                    let sub_challenge = Self::generate_challenge_data(modality)?;
                    combined_challenge.extend_from_slice(&sub_challenge);
                }
                Ok(combined_challenge)
            },
            _ => {
                // Generic random challenge
                let mut challenge = vec![0u8; 32];
                rng.fill_bytes(&mut challenge);
                Ok(challenge)
            }
        }
    }
}

pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}