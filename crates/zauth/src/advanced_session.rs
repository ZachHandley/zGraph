use std::{
    net::IpAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Result};
use dashmap::DashMap;
use leaky_bucket::RateLimiter;
use serde::Serialize;
use ulid::Ulid;
// Simple user agent parsing without external dependencies
use zcore_storage::Store;

use crate::{
    models::{
        AuditAction, AuditLogEntry, AuditStatus, DeviceInfo, DeviceType, SessionRecord,
        SessionStatus, UserRecord,
    },
    repository::AuthRepository,
};

/// Advanced session management with device tracking, security policies, and threat detection
#[derive(Clone)]
pub struct AdvancedSessionManager<'a> {
    store: &'a Store,
    // Removed user_agent_parser to simplify dependencies
    rate_limiters: Arc<DashMap<String, RateLimiter>>,
    device_trust_cache: Arc<DashMap<String, DeviceTrustLevel>>,
    suspicious_ips: Arc<DashMap<String, SuspiciousIpInfo>>,
    geoip_database: Option<Arc<maxminddb::Reader<Vec<u8>>>>,
}

impl<'a> AdvancedSessionManager<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self {
            store,
            rate_limiters: Arc::new(DashMap::new()),
            device_trust_cache: Arc::new(DashMap::new()),
            suspicious_ips: Arc::new(DashMap::new()),
            geoip_database: Self::load_geoip_database(),
        }
    }

    /// Load GeoIP database from filesystem if available
    fn load_geoip_database() -> Option<Arc<maxminddb::Reader<Vec<u8>>>> {
        // Look for GeoLite2-Country.mmdb in common locations
        let possible_paths = [
            "/usr/share/GeoIP/GeoLite2-Country.mmdb",
            "/var/lib/GeoIP/GeoLite2-Country.mmdb",
            "./data/GeoLite2-Country.mmdb",
            "/opt/GeoIP/GeoLite2-Country.mmdb",
        ];

        for path in &possible_paths {
            if let Ok(reader) = maxminddb::Reader::open_readfile(path) {
                tracing::info!("Loaded GeoIP database from {}", path);
                return Some(Arc::new(reader));
            }
        }

        tracing::warn!("No GeoIP database found, geographic restrictions disabled");
        None
    }

    /// Create a new session with comprehensive device tracking and security validation
    pub async fn create_session(
        &self,
        user: &UserRecord,
        environment_id: Ulid,
        ip_address: Option<String>,
        user_agent: Option<String>,
        policies: &SessionPolicies,
    ) -> Result<SessionRecord> {
        let repo = AuthRepository::new(self.store);
        let now = current_timestamp();

        // Extract device information from user agent
        let device_info = self.extract_device_info(&user_agent)?;

        // Validate IP and geographic restrictions
        if let Some(ref ip) = ip_address {
            self.validate_ip_restrictions(ip, &policies.geo_restrictions).await?;
        }

        // Check rate limits for this IP/user combination
        self.check_rate_limits(&ip_address, user.id, &policies.rate_limits).await?;

        // Validate concurrent session limits
        self.validate_session_limits(user.id, user.org_id, &device_info.fingerprint, policies).await?;

        // Create session record
        let session_id = Ulid::new();
        let session = SessionRecord {
            id: session_id,
            user_id: user.id,
            org_id: user.org_id,
            environment_id,
            device_info,
            ip_address: ip_address.clone(),
            user_agent: user_agent.clone(),
            created_at: now,
            last_activity_at: now,
            expires_at: now + policies.session_timeout_secs,
            status: SessionStatus::Active,
            revoked_at: None,
            revoked_by: None,
            revoked_reason: None,
        };

        // Store session
        repo.insert_session_record(&session)?;

        // Log session creation
        self.log_audit_event(AuditLogEntry {
            id: Ulid::new(),
            org_id: user.org_id,
            session_id: Some(session_id),
            user_id: Some(user.id),
            team_id: None,
            action: AuditAction::SessionCreate,
            resource_type: Some("session".to_string()),
            resource_id: Some(session_id.to_string()),
            details: serde_json::json!({
                "device_fingerprint": session.device_info.fingerprint,
                "device_type": session.device_info.device_type,
                "ip_address": ip_address,
                "trusted_device": session.device_info.trusted
            }),
            ip_address,
            user_agent,
            timestamp: now,
            status: AuditStatus::Success,
        }).await?;

        // Update device trust based on usage patterns
        self.update_device_trust(&session.device_info.fingerprint, user.id, true).await;

        Ok(session)
    }

    /// Extract comprehensive device information from user agent and generate fingerprint
    fn extract_device_info(&self, user_agent: &Option<String>) -> Result<DeviceInfo> {
        let mut device_info = DeviceInfo {
            fingerprint: String::new(),
            platform: None,
            browser: None,
            browser_version: None,
            os: None,
            os_version: None,
            device_type: DeviceType::Unknown,
            trusted: false,
        };

        if let Some(ua_string) = user_agent {
            // Simple user agent parsing
            device_info.browser = self.extract_browser(ua_string);
            device_info.os = self.extract_os(ua_string);
            device_info.device_type = self.classify_device_type_simple(ua_string);

            // Generate device fingerprint
            device_info.fingerprint = self.generate_device_fingerprint(&device_info, ua_string);
        } else {
            // API access without user agent
            device_info.device_type = DeviceType::Api;
            device_info.fingerprint = format!("api-{}", Ulid::new());
        }

        Ok(device_info)
    }

    /// Simple browser extraction from user agent
    fn extract_browser(&self, user_agent: &str) -> Option<String> {
        let ua_lower = user_agent.to_lowercase();
        if ua_lower.contains("chrome") && !ua_lower.contains("edg") {
            Some("Chrome".to_string())
        } else if ua_lower.contains("firefox") {
            Some("Firefox".to_string())
        } else if ua_lower.contains("safari") && !ua_lower.contains("chrome") {
            Some("Safari".to_string())
        } else if ua_lower.contains("edg") {
            Some("Edge".to_string())
        } else if ua_lower.contains("opera") {
            Some("Opera".to_string())
        } else {
            None
        }
    }

    /// Simple OS extraction from user agent
    fn extract_os(&self, user_agent: &str) -> Option<String> {
        let ua_lower = user_agent.to_lowercase();
        if ua_lower.contains("windows") {
            Some("Windows".to_string())
        } else if ua_lower.contains("mac os x") || ua_lower.contains("macos") {
            Some("macOS".to_string())
        } else if ua_lower.contains("linux") && !ua_lower.contains("android") {
            Some("Linux".to_string())
        } else if ua_lower.contains("android") {
            Some("Android".to_string())
        } else if ua_lower.contains("iphone") || ua_lower.contains("ipad") {
            Some("iOS".to_string())
        } else {
            None
        }
    }

    /// Generate a deterministic device fingerprint based on device characteristics
    fn generate_device_fingerprint(&self, device_info: &DeviceInfo, user_agent: &str) -> String {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(user_agent.as_bytes());

        if let Some(ref browser) = device_info.browser {
            hasher.update(browser.as_bytes());
        }
        if let Some(ref os) = device_info.os {
            hasher.update(os.as_bytes());
        }

        let hash = hasher.finalize();
        format!("fp_{}", hex::encode(&hash[..16]))
    }

    /// Classify device type based on user agent analysis
    fn classify_device_type_simple(&self, user_agent: &str) -> DeviceType {
        let ua_lower = user_agent.to_lowercase();

        if ua_lower.contains("mobile") && !ua_lower.contains("tablet") {
            DeviceType::Mobile
        } else if ua_lower.contains("tablet") || ua_lower.contains("ipad") {
            DeviceType::Tablet
        } else if ua_lower.contains("bot") || ua_lower.contains("crawler") || ua_lower.contains("spider") {
            DeviceType::Api
        } else if ua_lower.contains("windows") || ua_lower.contains("mac") || ua_lower.contains("linux") {
            DeviceType::Desktop
        } else {
            DeviceType::Unknown
        }
    }

    /// Validate IP address against geographic and security restrictions
    async fn validate_ip_restrictions(&self, ip: &str, restrictions: &GeoRestrictions) -> Result<()> {
        let ip_addr: IpAddr = ip.parse().map_err(|_| anyhow!("Invalid IP address"))?;

        // Check if IP is in blocked list
        if restrictions.blocked_ips.contains(&ip.to_string()) {
            return Err(anyhow!("IP address is blocked"));
        }

        // Check geographic restrictions if GeoIP database is available
        if let Some(ref geoip) = self.geoip_database {
            if let Ok(country) = geoip.lookup::<maxminddb::geoip2::Country>(ip_addr) {
                if let Some(country_code) = country.country.and_then(|c| c.iso_code) {
                    if !restrictions.allowed_countries.is_empty()
                        && !restrictions.allowed_countries.contains(&country_code.to_string()) {
                        return Err(anyhow!("Access not allowed from country: {}", country_code));
                    }
                }
            }
        }

        // Check for suspicious IP activity
        if let Some(suspicious_info) = self.suspicious_ips.get(ip) {
            if suspicious_info.risk_score > restrictions.max_risk_score {
                return Err(anyhow!("IP address flagged as high risk"));
            }
        }

        Ok(())
    }

    /// Check rate limits for authentication attempts
    async fn check_rate_limits(&self, ip: &Option<String>, user_id: Ulid, limits: &RateLimits) -> Result<()> {
        if let Some(ip) = ip {
            // Check IP-based rate limit
            let ip_key = format!("ip:{}", ip);
            let ip_limiter = self.rate_limiters.entry(ip_key.clone()).or_insert_with(|| {
                RateLimiter::builder()
                    .max(limits.max_attempts_per_ip as usize)
                    .refill(1)
                    .interval(Duration::from_secs(limits.window_secs))
                    .build()
            });

            if !ip_limiter.try_acquire(1) {
                self.flag_suspicious_ip(ip, "Rate limit exceeded").await;
                return Err(anyhow!("Too many requests from IP address"));
            }
        }

        // Check user-based rate limit
        let user_key = format!("user:{}", user_id);
        let user_limiter = self.rate_limiters.entry(user_key.clone()).or_insert_with(|| {
            RateLimiter::builder()
                .max(limits.max_attempts_per_user as usize)
                .refill(1)
                .interval(Duration::from_secs(limits.window_secs))
                .build()
        });

        if !user_limiter.try_acquire(1) {
            return Err(anyhow!("Too many requests for user"));
        }

        Ok(())
    }

    /// Validate concurrent session limits and device restrictions
    async fn validate_session_limits(
        &self,
        user_id: Ulid,
        org_id: u64,
        device_fingerprint: &str,
        policies: &SessionPolicies,
    ) -> Result<()> {
        let repo = AuthRepository::new(self.store);

        // Get active sessions for user
        let active_sessions = repo.get_active_sessions_for_user(org_id, user_id)?;

        // Check maximum concurrent sessions
        if let Some(max_sessions) = policies.session_limits.max_concurrent_sessions {
            if active_sessions.len() >= max_sessions as usize {
                // Find oldest session to revoke
                if let Some(oldest_session) = active_sessions.iter().min_by_key(|s| s.last_activity_at) {
                    self.revoke_session(oldest_session.id, Some(user_id), "Session limit exceeded").await?;
                }
            }
        }

        // Check device-specific session limits
        if let Some(max_per_device) = policies.session_limits.max_sessions_per_device {
            let device_sessions = active_sessions.iter()
                .filter(|s| s.device_info.fingerprint == device_fingerprint)
                .count();

            if device_sessions >= max_per_device as usize {
                return Err(anyhow!("Too many sessions for this device"));
            }
        }

        // Check trusted device requirement
        if policies.session_limits.trusted_device_only {
            let trust_level = self.get_device_trust_level(device_fingerprint, user_id).await;
            if !matches!(trust_level, DeviceTrustLevel::Trusted | DeviceTrustLevel::Verified) {
                return Err(anyhow!("Session creation requires trusted device"));
            }
        }

        Ok(())
    }

    /// Update device trust level based on usage patterns and security events
    async fn update_device_trust(&self, device_fingerprint: &str, user_id: Ulid, positive_event: bool) {
        let mut trust_level = self.get_device_trust_level(device_fingerprint, user_id).await;

        match (&trust_level, positive_event) {
            (DeviceTrustLevel::Untrusted, true) => {
                trust_level = DeviceTrustLevel::Learning;
            }
            (DeviceTrustLevel::Learning, true) => {
                // After several successful authentications, promote to trusted
                // This would typically involve tracking authentication count
                trust_level = DeviceTrustLevel::Trusted;
            }
            (DeviceTrustLevel::Trusted, true) => {
                // Keep trusted status or potentially promote to verified
                trust_level = DeviceTrustLevel::Trusted;
            }
            (DeviceTrustLevel::Verified, true) => {
                // Keep verified status
                trust_level = DeviceTrustLevel::Verified;
            }
            (_, false) => {
                // Suspicious activity detected, demote trust level
                trust_level = match trust_level {
                    DeviceTrustLevel::Verified => DeviceTrustLevel::Trusted,
                    DeviceTrustLevel::Trusted => DeviceTrustLevel::Learning,
                    _ => DeviceTrustLevel::Untrusted,
                };
            }
        }

        self.device_trust_cache.insert(
            format!("{}:{}", device_fingerprint, user_id),
            trust_level,
        );
    }

    /// Get current device trust level for a device/user combination
    async fn get_device_trust_level(&self, device_fingerprint: &str, user_id: Ulid) -> DeviceTrustLevel {
        let key = format!("{}:{}", device_fingerprint, user_id);

        if let Some(trust_level) = self.device_trust_cache.get(&key) {
            *trust_level
        } else {
            // Load from database or default to untrusted
            let repo = AuthRepository::new(self.store);
            if let Ok(Some(_device)) = repo.get_trusted_device(user_id, device_fingerprint) {
                DeviceTrustLevel::Trusted
            } else {
                DeviceTrustLevel::Untrusted
            }
        }
    }

    /// Flag an IP address as suspicious
    async fn flag_suspicious_ip(&self, ip: &str, reason: &str) {
        let now = current_timestamp();
        let mut info = self.suspicious_ips.entry(ip.to_string()).or_insert_with(|| {
            SuspiciousIpInfo {
                first_seen: now,
                last_incident: now,
                incident_count: 0,
                risk_score: 0.0,
                reasons: Vec::new(),
            }
        });

        info.last_incident = now;
        info.incident_count += 1;
        info.risk_score = (info.risk_score + 0.1).min(1.0);
        info.reasons.push(format!("{}: {}", now, reason));

        // Keep only recent reasons (last 10)
        if info.reasons.len() > 10 {
            let len = info.reasons.len();
            info.reasons.drain(0..len - 10);
        }

        let current_risk_score = info.risk_score;
        tracing::warn!(
            "Suspicious activity from IP {}: {} (risk score: {:.2})",
            ip, reason, current_risk_score
        );
    }

    /// Revoke a session with audit logging
    pub async fn revoke_session(&self, session_id: Ulid, revoked_by: Option<Ulid>, reason: &str) -> Result<()> {
        let repo = AuthRepository::new(self.store);
        let now = current_timestamp();

        // Get session for audit logging
        let session = repo.get_session_record(session_id)?
            .ok_or_else(|| anyhow!("Session not found"))?;

        // Update session status
        repo.revoke_session(&session_id, now, revoked_by, Some(reason.to_string()))?;

        // Log session revocation
        self.log_audit_event(AuditLogEntry {
            id: Ulid::new(),
            org_id: session.org_id,
            session_id: Some(session_id),
            user_id: Some(session.user_id),
            team_id: None,
            action: AuditAction::SessionRevoke,
            resource_type: Some("session".to_string()),
            resource_id: Some(session_id.to_string()),
            details: serde_json::json!({
                "reason": reason,
                "revoked_by": revoked_by,
                "session_duration": now - session.created_at
            }),
            ip_address: session.ip_address,
            user_agent: session.user_agent,
            timestamp: now,
            status: AuditStatus::Success,
        }).await?;

        Ok(())
    }

    /// Validate session and detect potential hijacking
    pub async fn validate_session(&self, session_id: Ulid, current_ip: Option<String>, current_user_agent: Option<String>) -> Result<SessionValidation> {
        let repo = AuthRepository::new(self.store);

        let session = repo.get_session_record(session_id)?
            .ok_or_else(|| anyhow!("Session not found"))?;

        let now = current_timestamp();
        let mut validation = SessionValidation {
            valid: true,
            warnings: Vec::new(),
            requires_reauth: false,
            risk_score: 0.0,
        };

        // Check session expiration
        if session.expires_at < now {
            validation.valid = false;
            validation.warnings.push("Session expired".to_string());
            return Ok(validation);
        }

        // Check session status
        if session.status != SessionStatus::Active {
            validation.valid = false;
            validation.warnings.push(format!("Session status: {:?}", session.status));
            return Ok(validation);
        }

        // Detect potential session hijacking
        self.detect_session_anomalies(&session, &current_ip, &current_user_agent, &mut validation).await;

        // Update last activity
        if validation.valid {
            repo.update_session_activity(&session_id, now)?;
        }

        Ok(validation)
    }

    /// Detect session anomalies that might indicate hijacking or suspicious activity
    async fn detect_session_anomalies(
        &self,
        session: &SessionRecord,
        current_ip: &Option<String>,
        current_user_agent: &Option<String>,
        validation: &mut SessionValidation,
    ) {
        // IP address change detection
        if let (Some(ref original_ip), Some(ref current_ip)) = (&session.ip_address, current_ip) {
            if original_ip != current_ip {
                validation.warnings.push("IP address changed during session".to_string());
                validation.risk_score += 0.3;

                // Check if both IPs are from same geographic region
                if let Some(ref geoip) = self.geoip_database {
                    if self.ips_different_countries(original_ip, current_ip, geoip).await {
                        validation.warnings.push("Session IP changed to different country".to_string());
                        validation.risk_score += 0.5;
                        validation.requires_reauth = true;
                    }
                }
            }
        }

        // User agent change detection
        if let (Some(ref original_ua), Some(ref current_ua)) = (&session.user_agent, current_user_agent) {
            if original_ua != current_ua {
                validation.warnings.push("User agent changed during session".to_string());
                validation.risk_score += 0.2;

                // Analyze severity of user agent change
                if self.significant_user_agent_change(original_ua, current_ua) {
                    validation.warnings.push("Significant user agent change detected".to_string());
                    validation.risk_score += 0.4;
                    validation.requires_reauth = true;
                }
            }
        }

        // Session duration anomaly
        let now = current_timestamp();
        let session_duration = now - session.created_at;
        if session_duration > 86400 * 7 { // 7 days
            validation.warnings.push("Unusually long session duration".to_string());
            validation.risk_score += 0.1;
        }

        // High risk score requires reauth
        if validation.risk_score >= 0.7 {
            validation.requires_reauth = true;
        }
    }

    /// Check if two IP addresses are from different countries
    async fn ips_different_countries(&self, ip1: &str, ip2: &str, geoip: &maxminddb::Reader<Vec<u8>>) -> bool {
        let parse_country = |ip: &str| -> Option<String> {
            let ip_addr: IpAddr = ip.parse().ok()?;
            let country = geoip.lookup::<maxminddb::geoip2::Country>(ip_addr).ok()?;
            country.country?.iso_code.map(|c| c.to_string())
        };

        match (parse_country(ip1), parse_country(ip2)) {
            (Some(country1), Some(country2)) => country1 != country2,
            _ => false, // Can't determine, assume same country
        }
    }

    /// Analyze if user agent change is significant (different browser/OS)
    fn significant_user_agent_change(&self, original: &str, current: &str) -> bool {
        // Simple user agent comparison
        let orig_browser = self.extract_browser(original);
        let curr_browser = self.extract_browser(current);
        let orig_os = self.extract_os(original);
        let curr_os = self.extract_os(current);

        // Different browser family or OS family indicates potential hijacking
        orig_browser != curr_browser || orig_os != curr_os
    }

    /// Clean up expired sessions and update device trust metrics
    pub async fn cleanup_expired_sessions(&self, org_id: u64) -> Result<CleanupSummary> {
        let repo = AuthRepository::new(self.store);
        let now = current_timestamp();

        let expired_sessions = repo.get_expired_sessions(org_id, now)?;
        let mut summary = CleanupSummary {
            sessions_cleaned: 0,
            devices_updated: 0,
            suspicious_ips_flagged: 0,
        };

        for session in expired_sessions {
            // Mark session as expired
            repo.update_session_status(&session.id, SessionStatus::Expired)?;
            summary.sessions_cleaned += 1;

            // Log expiration
            self.log_audit_event(AuditLogEntry {
                id: Ulid::new(),
                org_id: session.org_id,
                session_id: Some(session.id),
                user_id: Some(session.user_id),
                team_id: None,
                action: AuditAction::SessionExpire,
                resource_type: Some("session".to_string()),
                resource_id: Some(session.id.to_string()),
                details: serde_json::json!({
                    "expired_at": now,
                    "session_duration": now - session.created_at
                }),
                ip_address: session.ip_address,
                user_agent: session.user_agent,
                timestamp: now,
                status: AuditStatus::Success,
            }).await?;
        }

        Ok(summary)
    }

    /// Log audit event (placeholder - would integrate with actual audit system)
    async fn log_audit_event(&self, event: AuditLogEntry) -> Result<()> {
        let repo = AuthRepository::new(self.store);
        repo.insert_audit_event(&event)?;
        tracing::info!(
            "Audit: {} by user {:?} on {:?} from {:?}",
            event.action as u8,
            event.user_id,
            event.resource_id,
            event.ip_address
        );
        Ok(())
    }
}

/// Device trust levels for security decisions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum DeviceTrustLevel {
    Untrusted,  // New or suspicious device
    Learning,   // Device building trust through usage
    Trusted,    // Device trusted through usage patterns
    Verified,   // Device explicitly verified by user
}

/// Session validation result with security indicators
#[derive(Debug, Clone)]
pub struct SessionValidation {
    pub valid: bool,
    pub warnings: Vec<String>,
    pub requires_reauth: bool,
    pub risk_score: f64,
}

/// Session security policies configuration
#[derive(Debug, Clone)]
pub struct SessionPolicies {
    pub session_limits: SessionLimits,
    pub geo_restrictions: GeoRestrictions,
    pub rate_limits: RateLimits,
    pub session_timeout_secs: i64,
    pub device_verification_required: bool,
}

#[derive(Debug, Clone)]
pub struct SessionLimits {
    pub max_concurrent_sessions: Option<u32>,
    pub max_sessions_per_device: Option<u32>,
    pub trusted_device_only: bool,
}

#[derive(Debug, Clone)]
pub struct GeoRestrictions {
    pub allowed_countries: Vec<String>,
    pub blocked_ips: Vec<String>,
    pub max_risk_score: f64,
}

#[derive(Debug, Clone)]
pub struct RateLimits {
    pub max_attempts_per_ip: u32,
    pub max_attempts_per_user: u32,
    pub window_secs: u64,
}

/// Information about suspicious IP addresses
#[derive(Debug, Clone)]
pub struct SuspiciousIpInfo {
    pub first_seen: i64,
    pub last_incident: i64,
    pub incident_count: u32,
    pub risk_score: f64,
    pub reasons: Vec<String>,
}

/// Session cleanup summary
#[derive(Debug, Clone)]
pub struct CleanupSummary {
    pub sessions_cleaned: u32,
    pub devices_updated: u32,
    pub suspicious_ips_flagged: u32,
}

/// Default session policies for development/testing
impl Default for SessionPolicies {
    fn default() -> Self {
        Self {
            session_limits: SessionLimits {
                max_concurrent_sessions: Some(10),
                max_sessions_per_device: Some(3),
                trusted_device_only: false,
            },
            geo_restrictions: GeoRestrictions {
                allowed_countries: Vec::new(),
                blocked_ips: Vec::new(),
                max_risk_score: 0.8,
            },
            rate_limits: RateLimits {
                max_attempts_per_ip: 10,
                max_attempts_per_user: 5,
                window_secs: 300, // 5 minutes
            },
            session_timeout_secs: 3600 * 8, // 8 hours
            device_verification_required: false,
        }
    }
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}