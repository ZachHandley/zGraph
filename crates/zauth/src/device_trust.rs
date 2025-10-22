use std::{
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use zcore_storage::Store;

use crate::{
    models::{AuditAction, AuditLogEntry, AuditStatus, DeviceInfo, TrustedDevice},
    repository::AuthRepository,
    advanced_session::DeviceTrustLevel,
};

/// Device trust management system with verification flows and automatic trust building
#[derive(Clone)]
pub struct DeviceTrustManager<'a> {
    store: &'a Store,
}

impl<'a> DeviceTrustManager<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    /// Register a new device with initial trust assessment
    pub async fn register_device(
        &self,
        user_id: Ulid,
        org_id: u64,
        device_info: &DeviceInfo,
        ip_address: Option<String>,
        user_agent: Option<String>,
    ) -> Result<DeviceRegistration> {
        let repo = AuthRepository::new(self.store);
        let now = current_timestamp();

        // Check if device is already registered
        if let Some(existing) = repo.get_trusted_device(user_id, &device_info.fingerprint)? {
            return Ok(DeviceRegistration {
                device_id: existing.id,
                trust_level: if existing.trusted_until > now {
                    DeviceTrustLevel::Trusted
                } else {
                    DeviceTrustLevel::Untrusted
                },
                verification_required: false,
                verification_token: None,
            });
        }

        // Perform initial trust assessment
        let initial_trust = self.assess_initial_trust(device_info, &ip_address, &user_agent).await;

        // Create trusted device record
        let device_id = Ulid::new();
        let device_name = self.generate_device_name(device_info);
        let trusted_until = if initial_trust == DeviceTrustLevel::Trusted {
            now + 86400 * 30 // 30 days for initially trusted devices
        } else {
            now + 86400 * 7 // 7 days for learning devices
        };

        let trusted_device = TrustedDevice {
            id: device_id,
            user_id,
            org_id,
            device_fingerprint: device_info.fingerprint.clone(),
            device_name,
            trusted_until,
            created_at: now,
            last_seen_at: now,
            ip_address: ip_address.clone(),
            user_agent: user_agent.clone(),
        };

        repo.insert_trusted_device(&trusted_device)?;

        // Log device registration
        self.log_device_event(
            device_id,
            user_id,
            org_id,
            DeviceEvent::Registered,
            &format!("Device registered with initial trust level: {:?}", initial_trust),
            ip_address,
            user_agent,
        ).await?;

        // Determine if verification is required
        let verification_required = matches!(initial_trust, DeviceTrustLevel::Untrusted);
        let verification_token = if verification_required {
            Some(self.create_verification_token(device_id, user_id).await?)
        } else {
            None
        };

        Ok(DeviceRegistration {
            device_id,
            trust_level: initial_trust,
            verification_required,
            verification_token,
        })
    }

    /// Assess initial trust level for a new device
    async fn assess_initial_trust(
        &self,
        device_info: &DeviceInfo,
        ip_address: &Option<String>,
        user_agent: &Option<String>,
    ) -> DeviceTrustLevel {
        let mut trust_score = 0.0;

        // Known device types get higher trust
        match device_info.device_type {
            crate::models::DeviceType::Desktop => trust_score += 0.3,
            crate::models::DeviceType::Mobile => trust_score += 0.2,
            crate::models::DeviceType::Tablet => trust_score += 0.2,
            crate::models::DeviceType::Api => trust_score += 0.1,
            crate::models::DeviceType::Unknown => trust_score += 0.0,
        }

        // Complete device information increases trust
        if device_info.browser.is_some() && device_info.os.is_some() {
            trust_score += 0.2;
        }

        // Modern browsers/OS versions get higher trust
        if let Some(ref browser) = device_info.browser {
            if ["Chrome", "Firefox", "Safari", "Edge"].contains(&browser.as_str()) {
                trust_score += 0.1;
            }
        }

        // Check for suspicious patterns
        if let Some(ref ua) = user_agent {
            if ua.contains("bot") || ua.contains("crawler") || ua.contains("spider") {
                trust_score -= 0.5;
            }
        }

        // IP address reputation (would integrate with threat intelligence)
        if let Some(ref ip) = ip_address {
            if self.is_known_good_ip(ip).await {
                trust_score += 0.2;
            } else if self.is_suspicious_ip(ip).await {
                trust_score -= 0.3;
            }
        }

        // Convert score to trust level
        if trust_score >= 0.7 {
            DeviceTrustLevel::Trusted
        } else if trust_score >= 0.3 {
            DeviceTrustLevel::Learning
        } else {
            DeviceTrustLevel::Untrusted
        }
    }

    /// Generate a human-readable device name
    fn generate_device_name(&self, device_info: &DeviceInfo) -> String {
        let mut parts = Vec::new();

        if let Some(ref browser) = device_info.browser {
            parts.push(browser.clone());
        }

        if let Some(ref os) = device_info.os {
            parts.push(format!("on {}", os));
        }

        match device_info.device_type {
            crate::models::DeviceType::Desktop => parts.push("Desktop".to_string()),
            crate::models::DeviceType::Mobile => parts.push("Mobile".to_string()),
            crate::models::DeviceType::Tablet => parts.push("Tablet".to_string()),
            crate::models::DeviceType::Api => parts.push("API Client".to_string()),
            crate::models::DeviceType::Unknown => parts.push("Unknown Device".to_string()),
        }

        if parts.is_empty() {
            format!("Device {}", &device_info.fingerprint[..8])
        } else {
            parts.join(" ")
        }
    }

    /// Create a device verification token
    async fn create_verification_token(&self, device_id: Ulid, user_id: Ulid) -> Result<String> {
        let repo = AuthRepository::new(self.store);
        let now = current_timestamp();

        let verification = DeviceVerification {
            id: Ulid::new(),
            device_id,
            user_id,
            token: self.generate_verification_token(),
            created_at: now,
            expires_at: now + 3600, // 1 hour
            verified_at: None,
            attempts: 0,
            max_attempts: 3,
        };

        repo.insert_device_verification(&verification)?;
        Ok(verification.token)
    }

    /// Generate a secure verification token
    fn generate_verification_token(&self) -> String {
        use rand_core::{OsRng, RngCore};
        let mut bytes = [0u8; 16];
        OsRng.fill_bytes(&mut bytes);
        hex::encode(bytes)
    }

    /// Verify a device using the verification token
    pub async fn verify_device(
        &self,
        user_id: Ulid,
        org_id: u64,
        verification_token: &str,
        device_name: Option<String>,
    ) -> Result<DeviceVerificationResult> {
        let repo = AuthRepository::new(self.store);
        let now = current_timestamp();

        // Find verification record
        let mut verification = repo.get_device_verification_by_token(verification_token)?
            .ok_or_else(|| anyhow!("Invalid verification token"))?;

        // Check expiration
        if verification.expires_at < now {
            return Err(anyhow!("Verification token expired"));
        }

        // Check attempt limits
        if verification.attempts >= verification.max_attempts {
            return Err(anyhow!("Maximum verification attempts exceeded"));
        }

        // Verify user matches
        if verification.user_id != user_id {
            verification.attempts += 1;
            repo.update_device_verification(&verification)?;
            return Err(anyhow!("Verification token does not match user"));
        }

        // Mark verification as successful
        verification.verified_at = Some(now);
        repo.update_device_verification(&verification)?;

        // Update trusted device
        let mut device = repo.get_trusted_device_by_id(verification.device_id)?
            .ok_or_else(|| anyhow!("Device not found"))?;

        if let Some(name) = device_name {
            device.device_name = name;
        }
        device.trusted_until = now + 86400 * 90; // 90 days for verified devices
        repo.update_trusted_device(&device)?;

        // Log verification success
        self.log_device_event(
            device.id,
            user_id,
            org_id,
            DeviceEvent::Verified,
            "Device verified successfully",
            device.ip_address.clone(),
            device.user_agent.clone(),
        ).await?;

        Ok(DeviceVerificationResult {
            device_id: device.id,
            device_name: device.device_name.clone(),
            trust_level: DeviceTrustLevel::Verified,
            trusted_until: device.trusted_until,
        })
    }

    /// Update device trust based on usage patterns and security events
    pub async fn update_device_trust(
        &self,
        device_id: Ulid,
        user_id: Ulid,
        org_id: u64,
        trust_event: TrustEvent,
    ) -> Result<DeviceTrustLevel> {
        let repo = AuthRepository::new(self.store);
        let now = current_timestamp();

        let mut device = repo.get_trusted_device_by_id(device_id)?
            .ok_or_else(|| anyhow!("Device not found"))?;

        let current_trust = if device.trusted_until > now {
            DeviceTrustLevel::Trusted
        } else {
            DeviceTrustLevel::Untrusted
        };

        let new_trust = self.calculate_trust_adjustment(current_trust, &trust_event);

        // Update device trust duration based on new trust level
        device.trusted_until = match new_trust {
            DeviceTrustLevel::Verified => now + 86400 * 90,   // 90 days
            DeviceTrustLevel::Trusted => now + 86400 * 30,    // 30 days
            DeviceTrustLevel::Learning => now + 86400 * 7,     // 7 days
            DeviceTrustLevel::Untrusted => now,               // Immediately untrusted
        };

        device.last_seen_at = now;
        repo.update_trusted_device(&device)?;

        // Log trust change
        self.log_device_event(
            device_id,
            user_id,
            org_id,
            DeviceEvent::TrustUpdated,
            &format!("Trust updated: {:?} -> {:?} due to {:?}", current_trust, new_trust, trust_event),
            device.ip_address.clone(),
            device.user_agent.clone(),
        ).await?;

        Ok(new_trust)
    }

    /// Calculate trust level adjustment based on security events
    fn calculate_trust_adjustment(&self, current: DeviceTrustLevel, event: &TrustEvent) -> DeviceTrustLevel {
        match event {
            TrustEvent::SuccessfulLogin => {
                match current {
                    DeviceTrustLevel::Untrusted => DeviceTrustLevel::Learning,
                    DeviceTrustLevel::Learning => DeviceTrustLevel::Trusted,
                    other => other, // No change for already trusted devices
                }
            }
            TrustEvent::SuspiciousActivity => {
                match current {
                    DeviceTrustLevel::Verified => DeviceTrustLevel::Trusted,
                    DeviceTrustLevel::Trusted => DeviceTrustLevel::Learning,
                    _ => DeviceTrustLevel::Untrusted,
                }
            }
            TrustEvent::SecurityBreach => DeviceTrustLevel::Untrusted,
            TrustEvent::ManualVerification => DeviceTrustLevel::Verified,
            TrustEvent::RegularUsage => {
                // Gradual trust building through regular usage
                match current {
                    DeviceTrustLevel::Untrusted => DeviceTrustLevel::Learning,
                    DeviceTrustLevel::Learning => DeviceTrustLevel::Trusted,
                    other => other,
                }
            }
        }
    }

    /// Revoke trust for a device (security incident response)
    pub async fn revoke_device_trust(
        &self,
        device_id: Ulid,
        user_id: Ulid,
        org_id: u64,
        reason: &str,
        revoked_by: Option<Ulid>,
    ) -> Result<()> {
        let repo = AuthRepository::new(self.store);
        let now = current_timestamp();

        // Update device to untrusted
        let mut device = repo.get_trusted_device_by_id(device_id)?
            .ok_or_else(|| anyhow!("Device not found"))?;

        device.trusted_until = now; // Immediately untrust
        repo.update_trusted_device(&device)?;

        // Revoke all active sessions for this device
        let active_sessions = repo.get_active_sessions_for_device(org_id, &device.device_fingerprint)?;
        for session in active_sessions {
            repo.revoke_session_advanced(session.id, revoked_by, &format!("Device trust revoked: {}", reason))?;
        }

        // Log revocation
        self.log_device_event(
            device_id,
            user_id,
            org_id,
            DeviceEvent::TrustRevoked,
            &format!("Trust revoked by {:?}: {}", revoked_by, reason),
            device.ip_address.clone(),
            device.user_agent.clone(),
        ).await?;

        Ok(())
    }

    /// Get comprehensive device trust information
    pub async fn get_device_trust_info(&self, user_id: Ulid, device_fingerprint: &str) -> Result<DeviceTrustInfo> {
        let repo = AuthRepository::new(self.store);
        let now = current_timestamp();

        let device = repo.get_trusted_device(user_id, device_fingerprint)?
            .ok_or_else(|| anyhow!("Device not found"))?;

        let trust_level = if device.trusted_until > now {
            DeviceTrustLevel::Trusted
        } else {
            DeviceTrustLevel::Untrusted
        };

        let recent_events = repo.get_device_events(device.id, 10).unwrap_or_default();
        let active_sessions = repo.get_active_sessions_for_device(device.org_id, device_fingerprint)?.len();

        Ok(DeviceTrustInfo {
            device_id: device.id,
            device_name: device.device_name,
            device_fingerprint: device.device_fingerprint,
            trust_level,
            trusted_until: device.trusted_until,
            created_at: device.created_at,
            last_seen_at: device.last_seen_at,
            active_sessions: active_sessions as u32,
            recent_events,
            location_info: self.get_device_location_info(&device.ip_address).await,
        })
    }

    /// Get location information for device (if GeoIP available)
    async fn get_device_location_info(&self, ip_address: &Option<String>) -> Option<LocationInfo> {
        // This would integrate with GeoIP service
        // For now, return placeholder
        ip_address.as_ref().map(|_| LocationInfo {
            country: "Unknown".to_string(),
            region: None,
            city: None,
            timezone: None,
        })
    }

    /// Check if IP is known to be good (internal networks, known corporate IPs, etc.)
    async fn is_known_good_ip(&self, _ip: &str) -> bool {
        // Would check against whitelist of known good IPs
        false
    }

    /// Check if IP is known to be suspicious
    async fn is_suspicious_ip(&self, _ip: &str) -> bool {
        // Would check against threat intelligence feeds
        false
    }

    /// Log device-related events for audit trail
    async fn log_device_event(
        &self,
        device_id: Ulid,
        user_id: Ulid,
        org_id: u64,
        event: DeviceEvent,
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
            action: match event {
                DeviceEvent::Registered => AuditAction::ResourceCreate,
                DeviceEvent::Verified => AuditAction::ResourceUpdate,
                DeviceEvent::TrustUpdated => AuditAction::ResourceUpdate,
                DeviceEvent::TrustRevoked => AuditAction::ResourceDelete,
            },
            resource_type: Some("device".to_string()),
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

/// Device registration result
#[derive(Debug, Clone, Serialize)]
pub struct DeviceRegistration {
    pub device_id: Ulid,
    pub trust_level: DeviceTrustLevel,
    pub verification_required: bool,
    pub verification_token: Option<String>,
}

/// Device verification result
#[derive(Debug, Clone)]
pub struct DeviceVerificationResult {
    pub device_id: Ulid,
    pub device_name: String,
    pub trust_level: DeviceTrustLevel,
    pub trusted_until: i64,
}

/// Comprehensive device trust information
#[derive(Debug, Clone)]
pub struct DeviceTrustInfo {
    pub device_id: Ulid,
    pub device_name: String,
    pub device_fingerprint: String,
    pub trust_level: DeviceTrustLevel,
    pub trusted_until: i64,
    pub created_at: i64,
    pub last_seen_at: i64,
    pub active_sessions: u32,
    pub recent_events: Vec<DeviceEventRecord>,
    pub location_info: Option<LocationInfo>,
}

/// Location information for device
#[derive(Debug, Clone)]
pub struct LocationInfo {
    pub country: String,
    pub region: Option<String>,
    pub city: Option<String>,
    pub timezone: Option<String>,
}

/// Events that affect device trust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrustEvent {
    SuccessfulLogin,
    SuspiciousActivity,
    SecurityBreach,
    ManualVerification,
    RegularUsage,
}

/// Device-related events for audit trail
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeviceEvent {
    Registered,
    Verified,
    TrustUpdated,
    TrustRevoked,
}

/// Device verification record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceVerification {
    pub id: Ulid,
    pub device_id: Ulid,
    pub user_id: Ulid,
    pub token: String,
    pub created_at: i64,
    pub expires_at: i64,
    pub verified_at: Option<i64>,
    pub attempts: u32,
    pub max_attempts: u32,
}

/// Device event record for audit trail
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceEventRecord {
    pub id: Ulid,
    pub device_id: Ulid,
    pub event_type: String,
    pub description: String,
    pub timestamp: i64,
    pub ip_address: Option<String>,
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}