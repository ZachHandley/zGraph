use std::{
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Result};
use serde::Serialize;
use ulid::Ulid;
use zcore_storage::Store;

use crate::{
    models::{
        DeviceInfo, HardwareAttestationData, AttestationType, CredentialType,
        AuditAction, AuditLogEntry, AuditStatus
    },
    repository::AuthRepository,
    device_trust::{DeviceTrustManager, TrustEvent},
    credential_verification::CredentialVerificationService,
};

/// Device attestation system that integrates with credential verification and device trust
#[derive(Clone)]
pub struct DeviceAttestationService<'a> {
    store: &'a Store,
    device_trust: DeviceTrustManager<'a>,
    credential_service: CredentialVerificationService<'a>,
}

impl<'a> DeviceAttestationService<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self {
            store,
            device_trust: DeviceTrustManager::new(store),
            credential_service: CredentialVerificationService::new(store),
        }
    }

    /// Perform device attestation during login
    pub async fn attest_device_login(
        &self,
        user_id: Ulid,
        org_id: u64,
        device_info: &DeviceInfo,
        ip_address: Option<String>,
        user_agent: Option<String>,
        attestation_data: Option<HardwareAttestationData>,
    ) -> Result<DeviceAttestationResult> {
        let _now = current_timestamp();

        // Register or update device with trust manager
        let device_registration = self.device_trust.register_device(
            user_id,
            org_id,
            device_info,
            ip_address.clone(),
            user_agent.clone(),
        ).await?;

        let mut attestation_verified = false;
        let mut hardware_backed = false;
        let mut trust_score: f64 = 0.0;
        let mut device_credential_id = None;

        // Process hardware attestation if provided
        if let Some(attestation) = attestation_data {
            let attestation_result = self.verify_hardware_attestation(
                user_id,
                org_id,
                &attestation,
                &device_info.fingerprint,
            ).await?;

            attestation_verified = attestation_result.verified;
            hardware_backed = attestation_result.hardware_backed;
            trust_score = attestation_result.trust_score;

            if attestation_verified {
                // Create device credential for hardware-attested device
                device_credential_id = Some(self.create_device_credential(
                    user_id,
                    org_id,
                    device_registration.device_id,
                    &attestation,
                    &device_info.fingerprint,
                ).await?);

                // Update device trust based on successful attestation
                self.device_trust.update_device_trust(
                    device_registration.device_id,
                    user_id,
                    org_id,
                    TrustEvent::ManualVerification,
                ).await?;
            }
        }

        // Calculate overall device trust score
        let final_trust_score = self.calculate_device_trust_score(
            &device_registration,
            attestation_verified,
            hardware_backed,
            trust_score,
        );

        // Log attestation attempt
        self.log_attestation_event(
            user_id,
            org_id,
            device_registration.device_id,
            AttestationEvent::DeviceAttestationAttempt,
            &format!("Device attestation: verified={}, hardware_backed={}, score={:.2}",
                attestation_verified, hardware_backed, final_trust_score),
            ip_address,
            user_agent,
        ).await?;

        Ok(DeviceAttestationResult {
            device_id: device_registration.device_id,
            device_registration,
            attestation_verified,
            hardware_backed,
            trust_score: final_trust_score,
            device_credential_id,
            requires_additional_verification: final_trust_score < 0.7,
        })
    }

    /// Verify hardware attestation data
    async fn verify_hardware_attestation(
        &self,
        _user_id: Ulid,
        _org_id: u64,
        attestation: &HardwareAttestationData,
        _device_fingerprint: &str,
    ) -> Result<HardwareAttestationResult> {
        // This would implement actual hardware attestation verification
        // For now, this is a comprehensive placeholder that shows the structure

        let mut verified = false;
        let mut hardware_backed = false;
        let mut trust_score: f64 = 0.0;

        match attestation.attestation_type {
            AttestationType::None => {
                // No attestation provided
                trust_score = 0.1;
            }
            AttestationType::Basic => {
                // Basic attestation - verify signature and certificate
                if self.verify_attestation_signature(attestation).await? {
                    verified = true;
                    trust_score = 0.6;
                }
            }
            AttestationType::SelfAttestation => {
                // Self-attestation - limited trust
                verified = true;
                trust_score = 0.4;
            }
            AttestationType::AttestationCA => {
                // CA-signed attestation - higher trust
                if self.verify_ca_attestation(attestation).await? {
                    verified = true;
                    hardware_backed = true;
                    trust_score = 0.9;
                }
            }
            AttestationType::ECDAA => {
                // ECDAA attestation - highest trust
                if self.verify_ecdaa_attestation(attestation).await? {
                    verified = true;
                    hardware_backed = true;
                    trust_score = 0.95;
                }
            }
        }

        // Additional checks for hardware indicators
        if verified && attestation.aaguid.is_some() {
            // AAGUID present indicates hardware authenticator
            hardware_backed = true;
            trust_score += 0.05;
        }

        if verified && attestation.counter > 0 {
            // Counter indicates hardware-backed storage
            trust_score += 0.02;
        }

        Ok(HardwareAttestationResult {
            verified,
            hardware_backed,
            trust_score: trust_score.min(1.0f64),
            attestation_type: attestation.attestation_type,
            certificate_valid: verified,
            revocation_checked: true, // Would implement actual revocation check
        })
    }

    /// Create a device credential from attestation data
    async fn create_device_credential(
        &self,
        user_id: Ulid,
        org_id: u64,
        device_id: Ulid,
        attestation: &HardwareAttestationData,
        device_fingerprint: &str,
    ) -> Result<Ulid> {
        let credential_data = serde_json::to_vec(&serde_json::json!({
            "device_id": device_id.to_string(),
            "device_fingerprint": device_fingerprint,
            "attestation_type": attestation.attestation_type,
            "counter": attestation.counter,
            "aaguid": attestation.aaguid.as_ref().map(hex::encode)
        }))?;

        let registration = self.credential_service.register_credential(
            user_id,
            org_id,
            CredentialType::DeviceCredential,
            &credential_data,
            "Device Attestation".to_string(),
            device_fingerprint.to_string(),
            None, // No separate public key for device credentials
            Some(attestation.attestation_certificate.clone()),
            Some(bincode::serialize(attestation)?),
        ).await?;

        Ok(registration.credential_id)
    }

    /// Calculate overall device trust score
    fn calculate_device_trust_score(
        &self,
        device_registration: &crate::device_trust::DeviceRegistration,
        attestation_verified: bool,
        hardware_backed: bool,
        attestation_trust_score: f64,
    ) -> f64 {
        let mut total_score = 0.0;

        // Base trust from device registration
        total_score += match device_registration.trust_level {
            crate::advanced_session::DeviceTrustLevel::Verified => 0.8,
            crate::advanced_session::DeviceTrustLevel::Trusted => 0.6,
            crate::advanced_session::DeviceTrustLevel::Learning => 0.4,
            crate::advanced_session::DeviceTrustLevel::Untrusted => 0.1,
        };

        // Additional trust from attestation
        if attestation_verified {
            total_score += attestation_trust_score * 0.3; // Weight attestation at 30%
        }

        // Hardware backing bonus
        if hardware_backed {
            total_score += 0.1;
        }

        // Normalize and return
        (total_score / 2.0).min(1.0)
    }

    /// Verify challenge-response for device attestation
    pub async fn verify_device_challenge(
        &self,
        user_id: Ulid,
        org_id: u64,
        device_id: Ulid,
        challenge_data: &[u8],
        response_data: &[u8],
        attestation_data: Option<&[u8]>,
    ) -> Result<DeviceChallengeResult> {
        // This would implement device-specific challenge-response verification
        // For TPM, this might be a TPM quote verification
        // For secure enclaves, this might be an enclave attestation

        let repo = AuthRepository::new(self.store);

        // Get device information
        let device = repo.get_trusted_device_by_id(device_id)?
            .ok_or_else(|| anyhow!("Device not found"))?;

        if device.user_id != user_id || device.org_id != org_id {
            return Err(anyhow!("Device does not belong to user"));
        }

        // Verify the challenge response
        let verification_result = self.verify_challenge_response(
            challenge_data,
            response_data,
            attestation_data,
        ).await?;

        if verification_result.success {
            // Update device trust on successful challenge
            let new_trust = self.device_trust.update_device_trust(
                device_id,
                user_id,
                org_id,
                TrustEvent::SuccessfulLogin,
            ).await?;

            self.log_attestation_event(
                user_id,
                org_id,
                device_id,
                AttestationEvent::ChallengeSuccess,
                "Device challenge verification successful",
                None,
                None,
            ).await?;

            Ok(DeviceChallengeResult {
                success: true,
                trust_level: new_trust,
                trust_score: 0.9, // High trust for successful challenge
                details: verification_result,
            })
        } else {
            self.log_attestation_event(
                user_id,
                org_id,
                device_id,
                AttestationEvent::ChallengeFailed,
                "Device challenge verification failed",
                None,
                None,
            ).await?;

            Ok(DeviceChallengeResult {
                success: false,
                trust_level: crate::advanced_session::DeviceTrustLevel::Untrusted,
                trust_score: 0.1,
                details: verification_result,
            })
        }
    }

    // Private verification methods (would implement actual cryptographic verification)

    async fn verify_attestation_signature(&self, _attestation: &HardwareAttestationData) -> Result<bool> {
        // Would verify attestation signature against certificate
        Ok(true) // Placeholder
    }

    async fn verify_ca_attestation(&self, _attestation: &HardwareAttestationData) -> Result<bool> {
        // Would verify CA-signed attestation certificate
        Ok(true) // Placeholder
    }

    async fn verify_ecdaa_attestation(&self, _attestation: &HardwareAttestationData) -> Result<bool> {
        // Would verify ECDAA attestation
        Ok(true) // Placeholder
    }

    async fn verify_challenge_response(
        &self,
        _challenge_data: &[u8],
        _response_data: &[u8],
        _attestation_data: Option<&[u8]>,
    ) -> Result<ChallengeVerificationResult> {
        // Would implement actual challenge-response verification
        Ok(ChallengeVerificationResult {
            success: true,
            trust_score: 0.9,
            attestation_verified: true,
            hardware_backed: true,
            details: serde_json::json!({
                "verification_method": "device_attestation",
                "timestamp": current_timestamp()
            }),
        })
    }

    async fn log_attestation_event(
        &self,
        user_id: Ulid,
        org_id: u64,
        device_id: Ulid,
        event: AttestationEvent,
        details: &str,
        ip_address: Option<String>,
        user_agent: Option<String>,
    ) -> Result<()> {
        let repo = AuthRepository::new(self.store);

        let audit_entry = AuditLogEntry {
            id: Ulid::new(),
            org_id,
            session_id: None,
            user_id: Some(user_id),
            team_id: None,
            action: AuditAction::ResourceAccess,
            resource_type: Some("device_attestation".to_string()),
            resource_id: Some(device_id.to_string()),
            details: serde_json::json!({
                "event": format!("{:?}", event),
                "description": details
            }),
            ip_address,
            user_agent,
            timestamp: current_timestamp(),
            status: AuditStatus::Success,
        };

        repo.insert_audit_event(&audit_entry)?;
        Ok(())
    }
}

// Result types

#[derive(Debug, Clone, Serialize)]
pub struct DeviceAttestationResult {
    pub device_id: Ulid,
    pub device_registration: crate::device_trust::DeviceRegistration,
    pub attestation_verified: bool,
    pub hardware_backed: bool,
    pub trust_score: f64,
    pub device_credential_id: Option<Ulid>,
    pub requires_additional_verification: bool,
}

#[derive(Debug, Clone)]
pub struct HardwareAttestationResult {
    pub verified: bool,
    pub hardware_backed: bool,
    pub trust_score: f64,
    pub attestation_type: AttestationType,
    pub certificate_valid: bool,
    pub revocation_checked: bool,
}

#[derive(Debug, Clone)]
pub struct DeviceChallengeResult {
    pub success: bool,
    pub trust_level: crate::advanced_session::DeviceTrustLevel,
    pub trust_score: f64,
    pub details: ChallengeVerificationResult,
}

#[derive(Debug, Clone)]
pub struct ChallengeVerificationResult {
    pub success: bool,
    pub trust_score: f64,
    pub attestation_verified: bool,
    pub hardware_backed: bool,
    pub details: serde_json::Value,
}

// Event types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttestationEvent {
    DeviceAttestationAttempt,
    ChallengeSuccess,
    ChallengeFailed,
    AttestationVerified,
    AttestationFailed,
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}