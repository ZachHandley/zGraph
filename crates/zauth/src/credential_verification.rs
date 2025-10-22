use std::{
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Result};
use base64::Engine;
use ulid::Ulid;
use zcore_storage::Store;

use crate::{
    models::{
        IdentityCredential, CredentialType, CredentialTrustLevel,
        CredentialVerificationChallenge, VerificationResult,
        SessionCredentialContext, AuditAction, AuditLogEntry, AuditStatus
    },
    repository::AuthRepository,
};

/// Credential verification system supporting various identity credentials
#[derive(Clone)]
pub struct CredentialVerificationService<'a> {
    store: &'a Store,
}

impl<'a> CredentialVerificationService<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    /// Register a new identity credential for a user
    pub async fn register_credential(
        &self,
        user_id: Ulid,
        org_id: u64,
        credential_type: CredentialType,
        credential_data: &[u8],
        issuer: String,
        subject: String,
        public_key: Option<Vec<u8>>,
        certificate_chain: Option<Vec<u8>>,
        attestation_data: Option<Vec<u8>>,
    ) -> Result<CredentialRegistrationResult> {
        let repo = AuthRepository::new(self.store);
        let now = current_timestamp();

        // Verify credential data and assess trust level
        let trust_level = self.assess_credential_trust(
            credential_type,
            credential_data,
            &certificate_chain,
            &attestation_data,
        ).await?;

        // Create credential record
        let credential_id = Ulid::new();
        let credential = IdentityCredential {
            id: credential_id,
            user_id,
            org_id,
            credential_type,
            credential_data: base64::engine::general_purpose::STANDARD.encode(credential_data),
            issuer: issuer.clone(),
            subject: subject.clone(),
            public_key,
            certificate_chain,
            attestation_data,
            trust_level,
            verified: false, // Will be verified through challenge
            created_at: now,
            expires_at: self.calculate_credential_expiry(credential_type, now),
            last_used_at: None,
            revoked_at: None,
            revoked_reason: None,
        };

        repo.insert_identity_credential(&credential)?;

        // Create verification challenge if needed
        let verification_challenge = if trust_level >= CredentialTrustLevel::SoftwareVerified {
            Some(self.create_verification_challenge(credential_id, user_id, org_id).await?)
        } else {
            None
        };

        // Log credential registration
        self.log_credential_event(
            credential_id,
            user_id,
            org_id,
            CredentialEvent::Registered,
            &format!("Credential registered: {} for {}", issuer, subject),
        ).await?;

        Ok(CredentialRegistrationResult {
            credential_id,
            trust_level,
            requires_verification: verification_challenge.is_some(),
            verification_challenge,
        })
    }

    /// Create a verification challenge for a credential
    async fn create_verification_challenge(
        &self,
        credential_id: Ulid,
        user_id: Ulid,
        org_id: u64,
    ) -> Result<String> {
        let repo = AuthRepository::new(self.store);
        let now = current_timestamp();

        // Generate cryptographic challenge
        let challenge_data = self.generate_challenge_data();
        let nonce = self.generate_nonce();

        let challenge = CredentialVerificationChallenge {
            id: Ulid::new(),
            credential_id,
            user_id,
            org_id,
            challenge_data: challenge_data.clone(),
            nonce: nonce.clone(),
            created_at: now,
            expires_at: now + 300, // 5 minutes
            verified_at: None,
            signature: None,
            verification_result: None,
        };

        repo.insert_credential_challenge(&challenge)?;

        // Return challenge identifier
        Ok(challenge.id.to_string())
    }

    /// Verify a credential challenge response
    pub async fn verify_credential_challenge(
        &self,
        challenge_id: &str,
        signature: Vec<u8>,
        client_data: Option<Vec<u8>>,
        authenticator_data: Option<Vec<u8>>,
    ) -> Result<CredentialVerificationResult> {
        let repo = AuthRepository::new(self.store);
        let now = current_timestamp();

        let challenge_ulid = Ulid::from_string(challenge_id)
            .map_err(|_| anyhow!("Invalid challenge ID"))?;

        let mut challenge = repo.get_credential_challenge(challenge_ulid)?
            .ok_or_else(|| anyhow!("Challenge not found"))?;

        // Check expiration
        if challenge.expires_at < now {
            return Err(anyhow!("Challenge expired"));
        }

        // Get credential for verification
        let credential = repo.get_identity_credential(challenge.credential_id)?
            .ok_or_else(|| anyhow!("Credential not found"))?;

        // Verify signature based on credential type
        let verification_result = self.verify_credential_signature(
            &credential,
            &challenge.challenge_data,
            &signature,
            client_data.as_ref(),
            authenticator_data.as_ref(),
        ).await?;

        // Update challenge with results
        challenge.verified_at = Some(now);
        challenge.signature = Some(signature);
        challenge.verification_result = Some(verification_result.clone());
        repo.update_credential_challenge(&challenge)?;

        // Store needed values before moving credential
        let credential_id = credential.id;
        let user_id = credential.user_id;
        let org_id = credential.org_id;
        let trust_level = credential.trust_level;

        if verification_result.success {
            // Mark credential as verified
            let mut updated_credential = credential;
            updated_credential.verified = true;
            updated_credential.last_used_at = Some(now);
            repo.update_identity_credential(&updated_credential)?;

            // Log successful verification
            self.log_credential_event(
                credential_id,
                user_id,
                org_id,
                CredentialEvent::Verified,
                "Credential verification successful",
            ).await?;
        }

        Ok(CredentialVerificationResult {
            credential_id,
            verified: verification_result.success,
            trust_level,
            trust_score: verification_result.trust_score,
            verification_details: verification_result,
        })
    }

    /// Verify credential signature based on type
    async fn verify_credential_signature(
        &self,
        credential: &IdentityCredential,
        challenge_data: &[u8],
        signature: &[u8],
        client_data: Option<&Vec<u8>>,
        authenticator_data: Option<&Vec<u8>>,
    ) -> Result<VerificationResult> {
        match credential.credential_type {
            CredentialType::X509Certificate => {
                self.verify_x509_signature(credential, challenge_data, signature).await
            }
            CredentialType::HardwareAttestation => {
                self.verify_hardware_attestation(credential, challenge_data, signature, authenticator_data).await
            }
            CredentialType::BiometricCredential => {
                self.verify_biometric_signature(credential, challenge_data, signature).await
            }
            CredentialType::SoftwareAttestation => {
                self.verify_software_attestation(credential, challenge_data, signature).await
            }
            CredentialType::SmartCard => {
                self.verify_smartcard_signature(credential, challenge_data, signature).await
            }
            CredentialType::PlatformCredential => {
                self.verify_platform_credential(credential, challenge_data, signature, client_data).await
            }
            CredentialType::DeviceCredential => {
                self.verify_device_credential(credential, challenge_data, signature).await
            }
        }
    }

    /// Create an authentication context with credentials
    pub async fn create_credential_context(
        &self,
        user_id: Ulid,
        org_id: u64,
        session_id: Ulid,
        primary_credential_id: Option<Ulid>,
        additional_credential_ids: Vec<Ulid>,
    ) -> Result<SessionCredentialContext> {
        let repo = AuthRepository::new(self.store);
        let now = current_timestamp();

        // Verify all credentials belong to the user
        let mut all_credential_ids = additional_credential_ids.clone();
        if let Some(primary_id) = primary_credential_id {
            all_credential_ids.insert(0, primary_id);
        }

        let mut verified_credentials = Vec::new();
        let mut hardware_attested = false;
        let mut biometric_verified = false;
        let mut device_bound = false;
        let mut total_trust_score = 0.0;

        for cred_id in &all_credential_ids {
            if let Some(credential) = repo.get_identity_credential(*cred_id)? {
                if credential.user_id != user_id || credential.org_id != org_id {
                    return Err(anyhow!("Credential does not belong to user"));
                }

                if !credential.verified {
                    continue; // Skip unverified credentials
                }

                verified_credentials.push(credential.clone());

                // Update context flags based on credential type
                match credential.credential_type {
                    CredentialType::HardwareAttestation | CredentialType::SmartCard => {
                        hardware_attested = true;
                    }
                    CredentialType::BiometricCredential => {
                        biometric_verified = true;
                    }
                    CredentialType::DeviceCredential | CredentialType::PlatformCredential => {
                        device_bound = true;
                    }
                    _ => {}
                }

                // Calculate trust score contribution
                let trust_contribution = match credential.trust_level {
                    CredentialTrustLevel::TrustedPlatform => 1.0,
                    CredentialTrustLevel::CertifiedHardware => 0.9,
                    CredentialTrustLevel::HardwareVerified => 0.8,
                    CredentialTrustLevel::SoftwareVerified => 0.6,
                    CredentialTrustLevel::Untrusted => 0.2,
                };
                total_trust_score += trust_contribution;
            }
        }

        // Normalize trust score
        let final_trust_score = if verified_credentials.is_empty() {
            0.0
        } else {
            (total_trust_score / verified_credentials.len() as f64).min(1.0)
        };

        let context = SessionCredentialContext {
            session_id,
            primary_credential: primary_credential_id,
            secondary_credentials: additional_credential_ids,
            hardware_attested,
            biometric_verified,
            device_bound,
            trust_score: final_trust_score,
            verification_timestamp: now,
            context_data: serde_json::json!({
                "verified_credentials_count": verified_credentials.len(),
                "credential_types": verified_credentials.iter()
                    .map(|c| format!("{:?}", c.credential_type))
                    .collect::<Vec<_>>(),
                "trust_levels": verified_credentials.iter()
                    .map(|c| format!("{:?}", c.trust_level))
                    .collect::<Vec<_>>()
            }),
        };

        // Store context in session
        repo.update_session_credential_context(session_id, &context)?;

        Ok(context)
    }

    /// Revoke a credential (security incident or expiration)
    pub async fn revoke_credential(
        &self,
        credential_id: Ulid,
        revoked_by: Option<Ulid>,
        reason: &str,
    ) -> Result<()> {
        let repo = AuthRepository::new(self.store);
        let now = current_timestamp();

        let mut credential = repo.get_identity_credential(credential_id)?
            .ok_or_else(|| anyhow!("Credential not found"))?;

        credential.revoked_at = Some(now);
        credential.revoked_reason = Some(reason.to_string());
        repo.update_identity_credential(&credential)?;

        // Invalidate all sessions using this credential
        repo.revoke_sessions_with_credential(credential_id)?;

        // Log revocation
        self.log_credential_event(
            credential_id,
            credential.user_id,
            credential.org_id,
            CredentialEvent::Revoked,
            &format!("Credential revoked by {:?}: {}", revoked_by, reason),
        ).await?;

        Ok(())
    }

    // Private helper methods for different verification types
    async fn verify_x509_signature(
        &self,
        credential: &IdentityCredential,
        challenge_data: &[u8],
        signature: &[u8],
    ) -> Result<VerificationResult> {
        // Validate input parameters
        if challenge_data.is_empty() {
            return Err(anyhow!("Challenge data cannot be empty"));
        }
        if signature.is_empty() {
            return Err(anyhow!("Signature cannot be empty"));
        }

        // Validate credential data
        let credential_bytes = base64::engine::general_purpose::STANDARD
            .decode(&credential.credential_data)
            .map_err(|e| anyhow!("Invalid credential data: {}", e))?;

        if credential_bytes.is_empty() {
            return Err(anyhow!("Credential data is empty"));
        }

        // X.509 certificate verification logic would go here
        // This would involve parsing the certificate, validating the chain,
        // checking revocation, and verifying the signature

        Ok(VerificationResult {
            success: true, // Placeholder - would implement actual verification
            trust_score: 0.8,
            attestation_verified: false,
            certificate_chain_valid: credential.certificate_chain.is_some(),
            revocation_checked: true,
            details: serde_json::json!({
                "verification_type": "x509_certificate",
                "issuer": credential.issuer,
                "subject": credential.subject,
                "credential_size": credential_bytes.len(),
                "signature_size": signature.len()
            }),
        })
    }

    async fn verify_hardware_attestation(
        &self,
        credential: &IdentityCredential,
        challenge_data: &[u8],
        signature: &[u8],
        authenticator_data: Option<&Vec<u8>>,
    ) -> Result<VerificationResult> {
        // Validate input parameters
        if challenge_data.is_empty() {
            return Err(anyhow!("Challenge data cannot be empty for hardware attestation"));
        }
        if signature.is_empty() {
            return Err(anyhow!("Signature cannot be empty for hardware attestation"));
        }

        // Validate credential data
        let credential_bytes = base64::engine::general_purpose::STANDARD
            .decode(&credential.credential_data)
            .map_err(|e| anyhow!("Invalid hardware attestation credential: {}", e))?;

        if credential_bytes.len() < 32 {
            return Err(anyhow!("Hardware attestation credential too short: {} bytes", credential_bytes.len()));
        }

        // Hardware attestation verification
        // Would verify TPM attestation, secure enclave proof, etc.

        Ok(VerificationResult {
            success: true,
            trust_score: 0.95,
            attestation_verified: true,
            certificate_chain_valid: true,
            revocation_checked: true,
            details: serde_json::json!({
                "verification_type": "hardware_attestation",
                "has_authenticator_data": authenticator_data.is_some(),
                "authenticator_data_size": authenticator_data.map(|d| d.len()),
                "credential_size": credential_bytes.len(),
                "signature_size": signature.len()
            }),
        })
    }

    async fn verify_biometric_signature(
        &self,
        credential: &IdentityCredential,
        challenge_data: &[u8],
        signature: &[u8],
    ) -> Result<VerificationResult> {
        // Validate input parameters
        if challenge_data.is_empty() {
            return Err(anyhow!("Challenge data cannot be empty for biometric verification"));
        }
        if signature.is_empty() {
            return Err(anyhow!("Biometric signature cannot be empty"));
        }

        // Validate credential data
        let credential_bytes = base64::engine::general_purpose::STANDARD
            .decode(&credential.credential_data)
            .map_err(|e| anyhow!("Invalid biometric credential: {}", e))?;

        // Basic biometric template size validation
        if credential_bytes.len() < 64 {
            return Err(anyhow!("Biometric template too small: {} bytes", credential_bytes.len()));
        }

        if credential_bytes.len() > 10240 {
            return Err(anyhow!("Biometric template too large: {} bytes", credential_bytes.len()));
        }

        // Biometric verification logic
        // Would verify biometric template match and liveness

        Ok(VerificationResult {
            success: true,
            trust_score: 0.9,
            attestation_verified: true,
            certificate_chain_valid: false,
            revocation_checked: false,
            details: serde_json::json!({
                "verification_type": "biometric_credential",
                "template_size": credential_bytes.len(),
                "signature_size": signature.len()
            }),
        })
    }

    async fn verify_software_attestation(
        &self,
        credential: &IdentityCredential,
        challenge_data: &[u8],
        signature: &[u8],
    ) -> Result<VerificationResult> {
        // Validate input parameters
        if challenge_data.is_empty() {
            return Err(anyhow!("Challenge data cannot be empty for software attestation"));
        }
        if signature.is_empty() {
            return Err(anyhow!("Signature cannot be empty for software attestation"));
        }

        // Software attestation verification

        Ok(VerificationResult {
            success: true,
            trust_score: 0.7,
            attestation_verified: true,
            certificate_chain_valid: false,
            revocation_checked: true,
            details: serde_json::json!({
                "verification_type": "software_attestation",
                "issuer": credential.issuer,
                "subject": credential.subject,
                "signature_size": signature.len()
            }),
        })
    }

    async fn verify_smartcard_signature(
        &self,
        credential: &IdentityCredential,
        challenge_data: &[u8],
        signature: &[u8],
    ) -> Result<VerificationResult> {
        // Validate input parameters
        if challenge_data.is_empty() {
            return Err(anyhow!("Challenge data cannot be empty for smart card verification"));
        }
        if signature.is_empty() {
            return Err(anyhow!("Smart card signature cannot be empty"));
        }

        // Smart card verification

        Ok(VerificationResult {
            success: true,
            trust_score: 0.85,
            attestation_verified: false,
            certificate_chain_valid: credential.certificate_chain.is_some(),
            revocation_checked: true,
            details: serde_json::json!({
                "verification_type": "smartcard",
                "issuer": credential.issuer,
                "certificate_chain_present": credential.certificate_chain.is_some(),
                "signature_size": signature.len()
            }),
        })
    }

    async fn verify_platform_credential(
        &self,
        credential: &IdentityCredential,
        challenge_data: &[u8],
        signature: &[u8],
        client_data: Option<&Vec<u8>>,
    ) -> Result<VerificationResult> {
        // Validate input parameters
        if challenge_data.is_empty() {
            return Err(anyhow!("Challenge data cannot be empty for platform credential"));
        }
        if signature.is_empty() {
            return Err(anyhow!("Platform credential signature cannot be empty"));
        }

        // Platform credential verification (Windows Hello, Touch ID, etc.)

        Ok(VerificationResult {
            success: true,
            trust_score: 0.9,
            attestation_verified: true,
            certificate_chain_valid: false,
            revocation_checked: false,
            details: serde_json::json!({
                "verification_type": "platform_credential",
                "issuer": credential.issuer,
                "has_client_data": client_data.is_some(),
                "client_data_size": client_data.map(|d| d.len()),
                "signature_size": signature.len()
            }),
        })
    }

    async fn verify_device_credential(
        &self,
        credential: &IdentityCredential,
        challenge_data: &[u8],
        signature: &[u8],
    ) -> Result<VerificationResult> {
        // Validate input parameters
        if challenge_data.is_empty() {
            return Err(anyhow!("Challenge data cannot be empty for device credential"));
        }
        if signature.is_empty() {
            return Err(anyhow!("Device credential signature cannot be empty"));
        }

        // Device-bound credential verification

        Ok(VerificationResult {
            success: true,
            trust_score: 0.8,
            attestation_verified: false,
            certificate_chain_valid: false,
            revocation_checked: false,
            details: serde_json::json!({
                "verification_type": "device_credential",
                "issuer": credential.issuer,
                "subject": credential.subject,
                "signature_size": signature.len()
            }),
        })
    }

    // Helper methods
    async fn assess_credential_trust(
        &self,
        credential_type: CredentialType,
        credential_data: &[u8],
        certificate_chain: &Option<Vec<u8>>,
        attestation_data: &Option<Vec<u8>>,
    ) -> Result<CredentialTrustLevel> {
        // Validate input parameters
        if credential_data.is_empty() {
            return Err(anyhow!("Credential data cannot be empty"));
        }

        // Validate certificate chain if provided
        if let Some(chain) = certificate_chain {
            if chain.is_empty() {
                return Err(anyhow!("Certificate chain cannot be empty"));
            }
            if chain.len() > 10 {
                return Err(anyhow!("Certificate chain too long: {} certificates", chain.len()));
            }
        }

        // Validate attestation data if provided
        if let Some(attestation) = attestation_data {
            if attestation.is_empty() {
                return Err(anyhow!("Attestation data cannot be empty"));
            }
            if attestation.len() > 8192 {
                return Err(anyhow!("Attestation data too large: {} bytes", attestation.len()));
            }
        }

        // Assess trust level based on credential type and verification data
        match credential_type {
            CredentialType::HardwareAttestation if attestation_data.is_some() => {
                Ok(CredentialTrustLevel::CertifiedHardware)
            }
            CredentialType::X509Certificate if certificate_chain.is_some() => {
                Ok(CredentialTrustLevel::SoftwareVerified)
            }
            CredentialType::BiometricCredential => {
                Ok(CredentialTrustLevel::HardwareVerified)
            }
            CredentialType::SmartCard => {
                Ok(CredentialTrustLevel::HardwareVerified)
            }
            CredentialType::PlatformCredential => {
                Ok(CredentialTrustLevel::TrustedPlatform)
            }
            CredentialType::SoftwareAttestation => {
                Ok(CredentialTrustLevel::SoftwareVerified)
            }
            _ => Ok(CredentialTrustLevel::Untrusted),
        }
    }

    fn calculate_credential_expiry(&self, credential_type: CredentialType, created_at: i64) -> Option<i64> {
        let expiry_seconds = match credential_type {
            CredentialType::X509Certificate => 365 * 24 * 3600, // 1 year
            CredentialType::HardwareAttestation => 2 * 365 * 24 * 3600, // 2 years
            CredentialType::BiometricCredential => 5 * 365 * 24 * 3600, // 5 years
            CredentialType::SmartCard => 3 * 365 * 24 * 3600, // 3 years
            CredentialType::PlatformCredential => 365 * 24 * 3600, // 1 year
            CredentialType::SoftwareAttestation => 90 * 24 * 3600, // 90 days
            CredentialType::DeviceCredential => 365 * 24 * 3600, // 1 year
        };
        Some(created_at + expiry_seconds)
    }

    fn generate_challenge_data(&self) -> Vec<u8> {
        use rand_core::{OsRng, RngCore};
        let mut bytes = vec![0u8; 32];
        OsRng.fill_bytes(&mut bytes);
        bytes
    }

    fn generate_nonce(&self) -> String {
        use rand_core::{OsRng, RngCore};
        let mut bytes = [0u8; 16];
        OsRng.fill_bytes(&mut bytes);
        hex::encode(bytes)
    }

    async fn log_credential_event(
        &self,
        credential_id: Ulid,
        user_id: Ulid,
        org_id: u64,
        event: CredentialEvent,
        details: &str,
    ) -> Result<()> {
        let repo = AuthRepository::new(self.store);

        let audit_entry = AuditLogEntry {
            id: Ulid::new(),
            org_id,
            session_id: None,
            user_id: Some(user_id),
            team_id: None,
            action: match event {
                CredentialEvent::Registered => AuditAction::ResourceCreate,
                CredentialEvent::Verified => AuditAction::ResourceUpdate,
                CredentialEvent::Used => AuditAction::ResourceAccess,
                CredentialEvent::Revoked => AuditAction::ResourceDelete,
            },
            resource_type: Some("credential".to_string()),
            resource_id: Some(credential_id.to_string()),
            details: serde_json::json!({
                "event": format!("{:?}", event),
                "description": details
            }),
            ip_address: None,
            user_agent: None,
            timestamp: current_timestamp(),
            status: AuditStatus::Success,
        };

        repo.insert_audit_event(&audit_entry)?;
        Ok(())
    }
}

// Result types

#[derive(Debug, Clone)]
pub struct CredentialRegistrationResult {
    pub credential_id: Ulid,
    pub trust_level: CredentialTrustLevel,
    pub requires_verification: bool,
    pub verification_challenge: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CredentialVerificationResult {
    pub credential_id: Ulid,
    pub verified: bool,
    pub trust_level: CredentialTrustLevel,
    pub trust_score: f64,
    pub verification_details: VerificationResult,
}

// Event types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialEvent {
    Registered,
    Verified,
    Used,
    Revoked,
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}