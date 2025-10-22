use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Result};
use bloom::BloomFilter;
use dashmap::DashMap;
use ulid::Ulid;
use zcore_storage::Store;

use crate::{
    models::{AuditAction, AuditLogEntry, AuditStatus, SessionRecord},
    repository::AuthRepository,
    advanced_session::SessionValidation,
};

/// Session security monitoring with hijacking detection, brute force protection, and threat analysis
#[derive(Clone)]
pub struct SessionSecurityMonitor<'a> {
    store: &'a Store,
    failed_attempts: Arc<DashMap<String, FailureTracker>>,
    session_fingerprints: Arc<DashMap<Ulid, SessionFingerprint>>,
    suspicious_patterns: Arc<DashMap<String, PatternTracker>>,
    bloom_filter: Arc<BloomFilter>,
    threat_intel: Arc<ThreatIntelligence>,
}

impl<'a> SessionSecurityMonitor<'a> {
    pub fn new(store: &'a Store) -> Self {
        // Initialize bloom filter for tracking seen session patterns
        let bloom_filter = Arc::new(BloomFilter::with_rate(0.01, 10000));

        Self {
            store,
            failed_attempts: Arc::new(DashMap::new()),
            session_fingerprints: Arc::new(DashMap::new()),
            suspicious_patterns: Arc::new(DashMap::new()),
            bloom_filter,
            threat_intel: Arc::new(ThreatIntelligence::new()),
        }
    }

    /// Monitor authentication attempts for brute force patterns
    pub async fn monitor_auth_attempt(
        &self,
        ip_address: Option<String>,
        user_email: Option<String>,
        user_agent: Option<String>,
        success: bool,
    ) -> Result<SecurityAssessment> {
        let now = current_timestamp();
        let mut assessment = SecurityAssessment {
            risk_level: RiskLevel::Low,
            blocked: false,
            reasons: Vec::new(),
            required_actions: Vec::new(),
        };

        // Track IP-based attempts
        if let Some(ref ip) = ip_address {
            let ip_key = format!("ip:{}", ip);
            self.track_failure_attempt(&ip_key, success, &mut assessment).await;

            // Use bloom filter to detect repeated IP patterns (simplified for now)
            // Note: bloom_filter usage would require proper trait implementation
            // For now, we'll use a simple pattern detection
            if ip.contains("192.168.1.100") || ip.contains("10.0.0.50") {
                assessment.risk_level = RiskLevel::Medium;
                assessment.reasons.push("Suspicious IP address pattern detected".to_string());
            }

            // Check threat intelligence for known bad IPs
            if self.threat_intel.is_malicious_ip(ip).await {
                assessment.risk_level = RiskLevel::Critical;
                assessment.blocked = true;
                assessment.reasons.push("IP address in threat intelligence database".to_string());
            }
        }

        // Track user-based attempts
        if let Some(ref email) = user_email {
            let user_key = format!("user:{}", email);
            self.track_failure_attempt(&user_key, success, &mut assessment).await;
        }

        // Analyze user agent patterns
        if let Some(ref ua) = user_agent {
            self.analyze_user_agent_patterns(ua, &mut assessment).await;
        }

        // Log security event
        self.log_security_event(SecurityEvent {
            event_type: if success { SecurityEventType::AuthSuccess } else { SecurityEventType::AuthFailure },
            ip_address: ip_address.clone(),
            user_agent: user_agent.clone(),
            user_email: user_email.clone(),
            risk_level: assessment.risk_level,
            details: serde_json::json!({
                "blocked": assessment.blocked,
                "reasons": assessment.reasons
            }),
            timestamp: now,
        }).await?;

        Ok(assessment)
    }

    /// Track authentication failures and detect brute force patterns
    async fn track_failure_attempt(&self, key: &str, success: bool, assessment: &mut SecurityAssessment) {
        let now = current_timestamp();
        let mut tracker = self.failed_attempts.entry(key.to_string()).or_insert_with(|| {
            FailureTracker {
                attempts: 0,
                first_attempt: now,
                last_attempt: now,
                success_count: 0,
                failure_count: 0,
                blocked_until: None,
            }
        });

        tracker.last_attempt = now;

        if success {
            tracker.success_count += 1;
            // Reset failure count on successful auth
            tracker.failure_count = 0;
        } else {
            tracker.failure_count += 1;
            tracker.attempts += 1;

            // Progressive blocking based on failure count
            if tracker.failure_count >= 20 {
                tracker.blocked_until = Some(now + 3600 * 24); // 24 hours
                assessment.risk_level = RiskLevel::Critical;
                assessment.blocked = true;
                assessment.reasons.push("Too many failed attempts - blocked for 24 hours".to_string());
            } else if tracker.failure_count >= 10 {
                tracker.blocked_until = Some(now + 3600); // 1 hour
                assessment.risk_level = RiskLevel::High;
                assessment.blocked = true;
                assessment.reasons.push("Too many failed attempts - blocked for 1 hour".to_string());
            } else if tracker.failure_count >= 5 {
                tracker.blocked_until = Some(now + 300); // 5 minutes
                assessment.risk_level = RiskLevel::Medium;
                assessment.blocked = true;
                assessment.reasons.push("Too many failed attempts - blocked for 5 minutes".to_string());
            }
        }

        // Check if currently blocked
        if let Some(blocked_until) = tracker.blocked_until {
            if blocked_until > now {
                assessment.blocked = true;
                assessment.reasons.push("Currently blocked due to previous failures".to_string());
            } else {
                tracker.blocked_until = None;
            }
        }
    }

    /// Analyze user agent patterns for suspicious activity
    async fn analyze_user_agent_patterns(&self, user_agent: &str, assessment: &mut SecurityAssessment) {
        // Check for automated tools/bots
        let suspicious_patterns = [
            "bot", "crawler", "spider", "scraper", "automated",
            "curl", "wget", "postman", "httpie", "python-requests",
        ];

        for pattern in &suspicious_patterns {
            if user_agent.to_lowercase().contains(pattern) {
                assessment.risk_level = RiskLevel::Medium;
                assessment.reasons.push(format!("Suspicious user agent pattern: {}", pattern));
            }
        }

        // Check for empty or very short user agents
        if user_agent.len() < 10 {
            assessment.risk_level = RiskLevel::Medium;
            assessment.reasons.push("Unusually short user agent".to_string());
        }

        // Track unique user agent patterns
        let ua_hash = format!("{:x}", md5::compute(user_agent.as_bytes()));
        let pattern_key = format!("ua:{}", ua_hash);

        let mut pattern_tracker = self.suspicious_patterns.entry(pattern_key).or_insert_with(|| {
            PatternTracker {
                first_seen: current_timestamp(),
                count: 0,
                unique_ips: HashSet::new(),
                risk_score: 0.0,
            }
        });

        pattern_tracker.count += 1;
        if pattern_tracker.count > 100 {
            assessment.risk_level = RiskLevel::High;
            assessment.reasons.push("User agent seen in high volume attacks".to_string());
        }
    }

    /// Create session fingerprint for hijacking detection
    pub async fn create_session_fingerprint(&self, session_id: Ulid, session: &SessionRecord) -> Result<()> {
        let fingerprint = SessionFingerprint {
            session_id,
            user_id: session.user_id,
            original_ip: session.ip_address.clone(),
            original_user_agent: session.user_agent.clone(),
            device_fingerprint: session.device_info.fingerprint.clone(),
            created_at: session.created_at,
            last_validated: session.created_at,
            validation_failures: 0,
            geographic_region: self.get_geographic_region(&session.ip_address).await,
        };

        self.session_fingerprints.insert(session_id, fingerprint);
        Ok(())
    }

    /// Validate session against hijacking patterns
    pub async fn validate_session_security(&self, session_id: Ulid, current_request: &SessionRequest) -> Result<SessionValidation> {
        let mut validation = SessionValidation {
            valid: true,
            warnings: Vec::new(),
            requires_reauth: false,
            risk_score: 0.0,
        };

        let fingerprint = self.session_fingerprints.get(&session_id)
            .ok_or_else(|| anyhow!("Session fingerprint not found"))?;

        // IP address validation
        if let (Some(ref original_ip), Some(ref current_ip)) = (&fingerprint.original_ip, &current_request.ip_address) {
            if original_ip != current_ip {
                validation.warnings.push("IP address changed during session".to_string());
                validation.risk_score += 0.3;

                // Check geographic distance
                let original_region = &fingerprint.geographic_region;
                let current_region = self.get_geographic_region(&current_request.ip_address).await;

                if let (Some(orig), Some(curr)) = (original_region, &current_region) {
                    if orig.country != curr.country {
                        validation.warnings.push("Session moved to different country".to_string());
                        validation.risk_score += 0.5;
                        validation.requires_reauth = true;
                    } else if orig.region != curr.region {
                        validation.warnings.push("Session moved to different region".to_string());
                        validation.risk_score += 0.2;
                    }
                }
            }
        }

        // User agent validation
        if let (Some(ref original_ua), Some(ref current_ua)) = (&fingerprint.original_user_agent, &current_request.user_agent) {
            if original_ua != current_ua {
                validation.warnings.push("User agent changed during session".to_string());
                validation.risk_score += 0.2;

                // Check for significant changes (different browser/OS)
                if self.is_significant_ua_change(original_ua, current_ua) {
                    validation.warnings.push("Significant user agent change detected".to_string());
                    validation.risk_score += 0.4;
                    validation.requires_reauth = true;
                }
            }
        }

        // Timing analysis
        let now = current_timestamp();
        let session_duration = now - fingerprint.created_at;

        if session_duration > 86400 * 3 { // 3 days
            validation.warnings.push("Very long session duration".to_string());
            validation.risk_score += 0.1;
        }

        // Check for impossible travel
        if let Some(impossible_travel) = self.detect_impossible_travel(&fingerprint, current_request).await {
            validation.warnings.push(impossible_travel);
            validation.risk_score += 0.6;
            validation.requires_reauth = true;
        }

        // Update fingerprint
        let mut fingerprint_mut = self.session_fingerprints.get_mut(&session_id).unwrap();
        fingerprint_mut.last_validated = now;

        if validation.risk_score > 0.3 {
            fingerprint_mut.validation_failures += 1;
        }

        // Too many validation failures indicate persistent anomalies
        if fingerprint_mut.validation_failures > 5 {
            validation.requires_reauth = true;
            validation.warnings.push("Multiple session anomalies detected".to_string());
        }

        // High risk score requires reauth
        if validation.risk_score >= 0.7 {
            validation.requires_reauth = true;
        }

        Ok(validation)
    }

    /// Detect impossible travel between session locations
    async fn detect_impossible_travel(&self, fingerprint: &SessionFingerprint, current_request: &SessionRequest) -> Option<String> {
        // This would require geographic coordinate calculation
        // For now, simplified version checking timing and country changes

        if let (Some(ref orig_ip), Some(ref curr_ip)) = (&fingerprint.original_ip, &current_request.ip_address) {
            if orig_ip != curr_ip {
                let time_diff = current_timestamp() - fingerprint.last_validated;

                // If country changed in less than 1 hour, flag as impossible travel
                if time_diff < 3600 {
                    let orig_region = &fingerprint.geographic_region;
                    let curr_region = self.get_geographic_region(&current_request.ip_address).await;

                    if let (Some(orig), Some(curr)) = (orig_region, curr_region) {
                        if orig.country != curr.country {
                            return Some(format!(
                                "Impossible travel: {} to {} in {} minutes",
                                orig.country, curr.country, time_diff / 60
                            ));
                        }
                    }
                }
            }
        }

        None
    }

    /// Check if user agent change is significant (different browser/OS family)
    fn is_significant_ua_change(&self, original: &str, current: &str) -> bool {
        // Simplified user agent analysis
        let orig_lower = original.to_lowercase();
        let curr_lower = current.to_lowercase();

        // Check for browser family changes
        let browsers = ["chrome", "firefox", "safari", "edge", "opera"];
        let orig_browser = browsers.iter().find(|&&b| orig_lower.contains(b));
        let curr_browser = browsers.iter().find(|&&b| curr_lower.contains(b));

        if orig_browser != curr_browser {
            return true;
        }

        // Check for OS family changes
        let operating_systems = ["windows", "mac", "linux", "android", "ios"];
        let orig_os = operating_systems.iter().find(|&&os| orig_lower.contains(os));
        let curr_os = operating_systems.iter().find(|&&os| curr_lower.contains(os));

        orig_os != curr_os
    }

    /// Get geographic region information for IP address
    async fn get_geographic_region(&self, ip_address: &Option<String>) -> Option<GeographicRegion> {
        // This would integrate with GeoIP service
        // For now, return placeholder
        ip_address.as_ref().map(|_| GeographicRegion {
            country: "US".to_string(),
            region: Some("CA".to_string()),
            city: Some("San Francisco".to_string()),
        })
    }

    /// Analyze session patterns for anomaly detection
    pub async fn analyze_session_patterns(&self, org_id: u64) -> Result<SecurityAnalysis> {
        let repo = AuthRepository::new(self.store);
        let now = current_timestamp();
        let window_start = now - 3600 * 24; // Last 24 hours

        let recent_sessions = repo.get_sessions_in_timeframe(org_id, window_start, now)?;

        let mut analysis = SecurityAnalysis {
            total_sessions: recent_sessions.len(),
            suspicious_sessions: 0,
            blocked_ips: 0,
            anomalies: Vec::new(),
            risk_score: 0.0,
            recommendations: Vec::new(),
        };

        // Analyze IP distribution
        let mut ip_counts: HashMap<String, u32> = HashMap::new();
        let mut user_agent_counts: HashMap<String, u32> = HashMap::new();

        for session in &recent_sessions {
            if let Some(ref ip) = session.ip_address {
                *ip_counts.entry(ip.clone()).or_insert(0) += 1;
            }
            if let Some(ref ua) = session.user_agent {
                *user_agent_counts.entry(ua.clone()).or_insert(0) += 1;
            }
        }

        // Detect IP anomalies
        for (ip, count) in ip_counts {
            if count > 20 { // More than 20 sessions from single IP
                analysis.anomalies.push(format!("High session count from IP {}: {} sessions", ip, count));
                analysis.suspicious_sessions += count;
            }
        }

        // Detect user agent anomalies
        for (_ua, count) in user_agent_counts {
            if count > 50 { // Same user agent for many sessions
                analysis.anomalies.push(format!("High session count for user agent: {} sessions", count));
            }
        }

        // Calculate overall risk score
        analysis.risk_score = (analysis.suspicious_sessions as f64) / (analysis.total_sessions as f64).max(1.0);

        // Generate recommendations
        if analysis.risk_score > 0.1 {
            analysis.recommendations.push("Consider enabling stricter rate limiting".to_string());
        }
        if analysis.risk_score > 0.2 {
            analysis.recommendations.push("Review and strengthen device verification policies".to_string());
        }
        if analysis.risk_score > 0.3 {
            analysis.recommendations.push("Consider implementing additional authentication factors".to_string());
        }

        Ok(analysis)
    }

    /// Clean up expired tracking data
    pub async fn cleanup_expired_data(&self) -> Result<()> {
        let now = current_timestamp();
        let expiry_threshold = now - 3600 * 24 * 7; // 7 days

        // Clean up old failure trackers
        self.failed_attempts.retain(|_, tracker| {
            tracker.last_attempt > expiry_threshold
        });

        // Clean up old session fingerprints
        self.session_fingerprints.retain(|_, fingerprint| {
            fingerprint.last_validated > expiry_threshold
        });

        // Clean up old pattern trackers
        self.suspicious_patterns.retain(|_, pattern| {
            pattern.first_seen > expiry_threshold
        });

        Ok(())
    }

    /// Log security events for audit and analysis
    async fn log_security_event(&self, event: SecurityEvent) -> Result<()> {
        let repo = AuthRepository::new(self.store);

        let audit_entry = AuditLogEntry {
            id: Ulid::new(),
            org_id: 0, // Would be passed from context
            session_id: None,
            user_id: None,
            team_id: None,
            action: match event.event_type {
                SecurityEventType::AuthSuccess => AuditAction::LoginSuccess,
                SecurityEventType::AuthFailure => AuditAction::LoginFailure,
                SecurityEventType::SuspiciousActivity => AuditAction::ResourceAccess,
                SecurityEventType::SecurityBreach => AuditAction::ResourceAccess,
            },
            resource_type: Some("security".to_string()),
            resource_id: None,
            details: event.details.clone(),
            ip_address: event.ip_address.clone(),
            user_agent: event.user_agent.clone(),
            timestamp: event.timestamp,
            status: match event.risk_level {
                RiskLevel::Low => AuditStatus::Success,
                RiskLevel::Medium => AuditStatus::Warning,
                RiskLevel::High => AuditStatus::Blocked,
                RiskLevel::Critical => AuditStatus::Blocked,
            },
        };

        repo.insert_audit_event(&audit_entry)?;

        tracing::info!(
            "Security event: {:?} - Risk: {:?} - IP: {:?}",
            event.event_type, event.risk_level, event.ip_address
        );

        Ok(())
    }
}

/// Security assessment result for authentication attempts
#[derive(Debug, Clone)]
pub struct SecurityAssessment {
    pub risk_level: RiskLevel,
    pub blocked: bool,
    pub reasons: Vec<String>,
    pub required_actions: Vec<String>,
}

/// Risk levels for security events
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

/// Session request information for validation
#[derive(Debug, Clone)]
pub struct SessionRequest {
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub timestamp: i64,
}

/// Session fingerprint for hijacking detection
#[derive(Debug, Clone)]
pub struct SessionFingerprint {
    pub session_id: Ulid,
    pub user_id: Ulid,
    pub original_ip: Option<String>,
    pub original_user_agent: Option<String>,
    pub device_fingerprint: String,
    pub created_at: i64,
    pub last_validated: i64,
    pub validation_failures: u32,
    pub geographic_region: Option<GeographicRegion>,
}

/// Geographic region information
#[derive(Debug, Clone)]
pub struct GeographicRegion {
    pub country: String,
    pub region: Option<String>,
    pub city: Option<String>,
}

/// Failure tracking for brute force detection
#[derive(Debug, Clone)]
pub struct FailureTracker {
    pub attempts: u32,
    pub first_attempt: i64,
    pub last_attempt: i64,
    pub success_count: u32,
    pub failure_count: u32,
    pub blocked_until: Option<i64>,
}

/// Pattern tracking for suspicious activity detection
#[derive(Debug, Clone)]
pub struct PatternTracker {
    pub first_seen: i64,
    pub count: u32,
    pub unique_ips: HashSet<String>,
    pub risk_score: f64,
}

/// Security event for logging and analysis
#[derive(Debug, Clone)]
pub struct SecurityEvent {
    pub event_type: SecurityEventType,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub user_email: Option<String>,
    pub risk_level: RiskLevel,
    pub details: serde_json::Value,
    pub timestamp: i64,
}

/// Types of security events
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecurityEventType {
    AuthSuccess,
    AuthFailure,
    SuspiciousActivity,
    SecurityBreach,
}

/// Security analysis summary
#[derive(Debug, Clone)]
pub struct SecurityAnalysis {
    pub total_sessions: usize,
    pub suspicious_sessions: u32,
    pub blocked_ips: u32,
    pub anomalies: Vec<String>,
    pub risk_score: f64,
    pub recommendations: Vec<String>,
}

/// Threat intelligence service (simplified)
#[derive(Debug)]
pub struct ThreatIntelligence {
    known_bad_ips: HashSet<String>,
}

impl ThreatIntelligence {
    pub fn new() -> Self {
        // In production, this would load from external threat feeds
        // For now, include some common malicious IP patterns
        let mut known_bad_ips = HashSet::new();

        // Add some known malicious IP ranges for demonstration
        known_bad_ips.insert("192.168.1.100".to_string()); // Example malicious IP
        known_bad_ips.insert("10.0.0.50".to_string());     // Example malicious IP

        Self {
            known_bad_ips,
        }
    }

    /// Add a malicious IP to the threat intelligence database
    pub fn add_malicious_ip(&mut self, ip: String) {
        self.known_bad_ips.insert(ip);
    }

    /// Get the current list of known bad IPs
    pub fn get_known_bad_ips(&self) -> &HashSet<String> {
        &self.known_bad_ips
    }

    pub async fn is_malicious_ip(&self, ip: &str) -> bool {
        // Check against known bad IPs database
        self.known_bad_ips.contains(ip)
    }
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}