use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use ulid::Ulid;
use zcore_storage::Store;

use crate::models::{
    DeviceInfo, DeviceType, SessionRecord, SessionStatus, SessionLimit,
    AuditLogEntry, AuditAction, AuditStatus,
};
use crate::repository::AuthRepository;

#[derive(Debug, Clone)]
pub struct SessionService<'a> {
    repo: AuthRepository<'a>,
}

impl<'a> SessionService<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self {
            repo: AuthRepository::new(store),
        }
    }

    /// Create a new session with device tracking
    pub fn create_session(
        &self,
        user_id: Ulid,
        org_id: u64,
        environment_id: Ulid,
        device_info: DeviceInfo,
        ip_address: Option<String>,
        user_agent: Option<String>,
        ttl_secs: i64,
    ) -> Result<SessionRecord> {
        let now = current_timestamp();
        let session_id = Ulid::new();

        // Check session limits
        self.enforce_session_limits(user_id, org_id, &device_info)?;

        let session = SessionRecord {
            id: session_id,
            user_id,
            org_id,
            environment_id,
            device_info: device_info.clone(),
            ip_address: ip_address.clone(),
            user_agent: user_agent.clone(),
            created_at: now,
            last_activity_at: now,
            expires_at: now + ttl_secs,
            status: SessionStatus::Active,
            revoked_at: None,
            revoked_by: None,
            revoked_reason: None,
        };

        self.repo.insert_enhanced_session(&session)?;

        // Log session creation
        self.log_audit_event(
            org_id,
            Some(session_id),
            Some(user_id),
            None,
            AuditAction::SessionCreate,
            None,
            None,
            serde_json::json!({
                "device_fingerprint": device_info.fingerprint,
                "device_type": device_info.device_type,
                "trusted_device": device_info.trusted,
            }),
            ip_address,
            user_agent,
            AuditStatus::Success,
        )?;

        Ok(session)
    }

    /// Update session activity timestamp
    pub fn update_session_activity(&self, session_id: &Ulid) -> Result<()> {
        let now = current_timestamp();
        self.repo.update_session_activity(session_id, now)
    }

    /// Get active session by ID
    pub fn get_session(&self, session_id: &Ulid) -> Result<Option<SessionRecord>> {
        self.repo.get_enhanced_session(session_id)
    }

    /// Get all active sessions for a user
    pub fn get_user_sessions(&self, user_id: Ulid, org_id: u64) -> Result<Vec<SessionRecord>> {
        self.repo.get_user_sessions(user_id, org_id)
    }

    /// Get sessions by device fingerprint
    pub fn get_device_sessions(&self, device_fingerprint: &str, org_id: u64) -> Result<Vec<SessionRecord>> {
        self.repo.get_device_sessions(device_fingerprint, org_id)
    }

    /// Revoke a session
    pub fn revoke_session(
        &self,
        session_id: &Ulid,
        revoked_by: Option<Ulid>,
        reason: Option<String>,
    ) -> Result<()> {
        let now = current_timestamp();

        if let Some(session) = self.get_session(session_id)? {
            self.repo.revoke_session(session_id, now, revoked_by, reason.clone())?;

            // Log session revocation
            self.log_audit_event(
                session.org_id,
                Some(*session_id),
                Some(session.user_id),
                None,
                AuditAction::SessionRevoke,
                None,
                None,
                serde_json::json!({
                    "revoked_by": revoked_by,
                    "reason": reason,
                }),
                session.ip_address,
                session.user_agent,
                AuditStatus::Success,
            )?;
        }

        Ok(())
    }

    /// Revoke all sessions for a user
    pub fn revoke_user_sessions(
        &self,
        user_id: Ulid,
        org_id: u64,
        revoked_by: Option<Ulid>,
        reason: Option<String>,
    ) -> Result<u32> {
        let sessions = self.get_user_sessions(user_id, org_id)?;
        let mut revoked_count = 0;

        for session in sessions {
            if session.status == SessionStatus::Active {
                self.revoke_session(&session.id, revoked_by, reason.clone())?;
                revoked_count += 1;
            }
        }

        Ok(revoked_count)
    }

    /// Revoke all sessions except the current one
    pub fn revoke_other_sessions(
        &self,
        user_id: Ulid,
        org_id: u64,
        except_session_id: &Ulid,
        revoked_by: Option<Ulid>,
        reason: Option<String>,
    ) -> Result<u32> {
        let sessions = self.get_user_sessions(user_id, org_id)?;
        let mut revoked_count = 0;

        for session in sessions {
            if session.status == SessionStatus::Active && session.id != *except_session_id {
                self.revoke_session(&session.id, revoked_by, reason.clone())?;
                revoked_count += 1;
            }
        }

        Ok(revoked_count)
    }

    /// Clean up expired sessions
    pub fn cleanup_expired_sessions(&self, org_id: u64) -> Result<u32> {
        let now = current_timestamp();
        let expired_sessions = self.repo.get_expired_sessions(org_id, now)?;
        let mut cleaned_count = 0;

        for session in expired_sessions {
            self.repo.update_session_status(&session.id, SessionStatus::Expired)?;

            // Log session expiration
            self.log_audit_event(
                session.org_id,
                Some(session.id),
                Some(session.user_id),
                None,
                AuditAction::SessionExpire,
                None,
                None,
                serde_json::json!({
                    "expired_at": now,
                    "device_fingerprint": session.device_info.fingerprint,
                }),
                session.ip_address,
                session.user_agent,
                AuditStatus::Success,
            )?;

            cleaned_count += 1;
        }

        Ok(cleaned_count)
    }

    /// Mark session as suspicious
    pub fn mark_suspicious(
        &self,
        session_id: &Ulid,
        reason: &str,
    ) -> Result<()> {
        if let Some(session) = self.get_session(session_id)? {
            self.repo.update_session_status(session_id, SessionStatus::Suspicious)?;

            // Log suspicious activity
            self.log_audit_event(
                session.org_id,
                Some(*session_id),
                Some(session.user_id),
                None,
                AuditAction::SessionRevoke,
                None,
                None,
                serde_json::json!({
                    "marked_suspicious": true,
                    "reason": reason,
                }),
                session.ip_address,
                session.user_agent,
                AuditStatus::Warning,
            )?;
        }

        Ok(())
    }

    /// Validate session limits and constraints
    fn enforce_session_limits(
        &self,
        user_id: Ulid,
        org_id: u64,
        device_info: &DeviceInfo,
    ) -> Result<()> {
        // Get user's session limits (from user profile or team settings)
        let limits = self.get_effective_session_limits(user_id, org_id)?;

        if let Some(limits) = limits {
            // Check trusted device requirement
            if limits.trusted_device_only && !device_info.trusted {
                return Err(anyhow!("Only trusted devices are allowed for this user"));
            }

            // Check concurrent session limit
            if let Some(max_concurrent) = limits.max_concurrent_sessions {
                let active_sessions = self.get_user_sessions(user_id, org_id)?;
                let active_count = active_sessions.iter()
                    .filter(|s| s.status == SessionStatus::Active)
                    .count() as u32;

                if active_count >= max_concurrent {
                    return Err(anyhow!("Maximum concurrent sessions exceeded"));
                }
            }

            // Check sessions per device limit
            if let Some(max_per_device) = limits.max_sessions_per_device {
                let device_sessions = self.get_device_sessions(&device_info.fingerprint, org_id)?;
                let active_device_sessions = device_sessions.iter()
                    .filter(|s| s.status == SessionStatus::Active)
                    .count() as u32;

                if active_device_sessions >= max_per_device {
                    return Err(anyhow!("Maximum sessions per device exceeded"));
                }
            }

            // TODO: Implement geo-restriction checks
            if let Some(_geo_restriction) = &limits.geo_restriction {
                // Implement IP geolocation and restriction logic
            }
        }

        Ok(())
    }

    /// Get effective session limits for a user (considering team settings)
    fn get_effective_session_limits(&self, _user_id: Ulid, _org_id: u64) -> Result<Option<SessionLimit>> {
        // TODO: Implement logic to get session limits from user profile and team settings
        // For now, return None (no limits)
        Ok(None)
    }

    /// Generate device fingerprint from user agent and other factors
    pub fn generate_device_fingerprint(
        user_agent: &str,
        ip_address: &str,
        additional_headers: &HashMap<String, String>,
    ) -> String {
        use sha2::{Sha256, Digest};

        let mut hasher = Sha256::new();
        hasher.update(user_agent.as_bytes());
        hasher.update(ip_address.as_bytes());

        // Add additional headers for fingerprinting
        let mut header_keys: Vec<_> = additional_headers.keys().collect();
        header_keys.sort();

        for key in header_keys {
            if let Some(value) = additional_headers.get(key) {
                hasher.update(key.as_bytes());
                hasher.update(value.as_bytes());
            }
        }

        format!("{:x}", hasher.finalize())
    }

    /// Parse device information from user agent
    pub fn parse_device_info(
        user_agent: Option<&str>,
        fingerprint: String,
    ) -> DeviceInfo {
        let (platform, browser, browser_version, os, os_version, device_type) =
            if let Some(ua) = user_agent {
                parse_user_agent(ua)
            } else {
                (None, None, None, None, None, DeviceType::Unknown)
            };

        DeviceInfo {
            fingerprint,
            platform,
            browser,
            browser_version,
            os,
            os_version,
            device_type,
            trusted: false, // Default to untrusted, set separately
        }
    }

    /// Log audit event
    fn log_audit_event(
        &self,
        org_id: u64,
        session_id: Option<Ulid>,
        user_id: Option<Ulid>,
        team_id: Option<Ulid>,
        action: AuditAction,
        resource_type: Option<String>,
        resource_id: Option<String>,
        details: serde_json::Value,
        ip_address: Option<String>,
        user_agent: Option<String>,
        status: AuditStatus,
    ) -> Result<()> {
        let audit_entry = AuditLogEntry {
            id: Ulid::new(),
            org_id,
            session_id,
            user_id,
            team_id,
            action,
            resource_type,
            resource_id,
            details,
            ip_address,
            user_agent,
            timestamp: current_timestamp(),
            status,
        };

        self.repo.insert_audit_log(&audit_entry)
    }
}

/// Parse user agent string to extract device information
fn parse_user_agent(user_agent: &str) -> (Option<String>, Option<String>, Option<String>, Option<String>, Option<String>, DeviceType) {
    let ua = user_agent.to_lowercase();

    // Detect device type
    let device_type = if ua.contains("mobile") || ua.contains("android") || ua.contains("iphone") {
        DeviceType::Mobile
    } else if ua.contains("tablet") || ua.contains("ipad") {
        DeviceType::Tablet
    } else if ua.contains("curl") || ua.contains("wget") || ua.contains("python") {
        DeviceType::Api
    } else {
        DeviceType::Desktop
    };

    // Basic browser detection
    let (browser, browser_version) = if ua.contains("chrome") {
        (Some("Chrome".to_string()), extract_version(&ua, "chrome"))
    } else if ua.contains("firefox") {
        (Some("Firefox".to_string()), extract_version(&ua, "firefox"))
    } else if ua.contains("safari") && !ua.contains("chrome") {
        (Some("Safari".to_string()), extract_version(&ua, "safari"))
    } else if ua.contains("edge") {
        (Some("Edge".to_string()), extract_version(&ua, "edge"))
    } else {
        (None, None)
    };

    // Basic OS detection
    let (os, os_version) = if ua.contains("windows") {
        (Some("Windows".to_string()), None)
    } else if ua.contains("mac os") {
        (Some("macOS".to_string()), None)
    } else if ua.contains("linux") {
        (Some("Linux".to_string()), None)
    } else if ua.contains("android") {
        (Some("Android".to_string()), extract_version(&ua, "android"))
    } else if ua.contains("ios") || ua.contains("iphone") || ua.contains("ipad") {
        (Some("iOS".to_string()), None)
    } else {
        (None, None)
    };

    let platform = os.clone();

    (platform, browser, browser_version, os, os_version, device_type)
}

/// Extract version from user agent string
fn extract_version(ua: &str, software: &str) -> Option<String> {
    if let Some(start) = ua.find(&format!("{}/", software)) {
        let version_start = start + software.len() + 1;
        if let Some(version_part) = ua.get(version_start..) {
            if let Some(end) = version_part.find(' ') {
                return Some(version_part[..end].to_string());
            }
        }
    }
    None
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use zcore_storage::Store;

    fn temp_service() -> SessionService<'static> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("session_test");
        let store = Store::open(&path).unwrap();
        let _ = dir.keep();
        let leaked: &'static Store = Box::leak(Box::new(store));
        SessionService::new(leaked)
    }

    #[test]
    fn test_device_fingerprint_generation() {
        let user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36";
        let ip = "192.168.1.1";
        let headers = HashMap::new();

        let fingerprint = SessionService::generate_device_fingerprint(user_agent, ip, &headers);
        assert!(!fingerprint.is_empty());
        assert_eq!(fingerprint.len(), 64); // SHA256 hex string
    }

    #[test]
    fn test_user_agent_parsing() {
        let ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36";
        let fingerprint = "test_fingerprint".to_string();

        let device_info = SessionService::parse_device_info(Some(ua), fingerprint);

        assert_eq!(device_info.device_type, DeviceType::Desktop);
        assert_eq!(device_info.browser, Some("Chrome".to_string()));
        assert_eq!(device_info.os, Some("Windows".to_string()));
    }

    #[test]
    fn test_mobile_user_agent_parsing() {
        let ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15";
        let fingerprint = "test_fingerprint".to_string();

        let device_info = SessionService::parse_device_info(Some(ua), fingerprint);

        assert_eq!(device_info.device_type, DeviceType::Mobile);
        assert_eq!(device_info.os, Some("iOS".to_string()));
    }
}