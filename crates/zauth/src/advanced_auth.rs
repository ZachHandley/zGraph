use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use hex;
use sha2::{Digest, Sha256};
use ulid::Ulid;
use zcore_storage::Store;

use crate::{
    models::DeviceInfo,
    repository::AuthRepository,
    advanced_session::{AdvancedSessionManager, SessionPolicies},
    device_trust::{DeviceTrustManager, DeviceRegistration, TrustEvent},
    session_security::{SessionSecurityMonitor, RiskLevel, SessionRequest},
    AuthConfig, LoginTokens,
};

/// Comprehensive advanced authentication service that integrates all session management components
#[derive(Clone)]
pub struct AdvancedAuthService<'a> {
    store: &'a Store,
    config: AuthConfig,
    session_manager: AdvancedSessionManager<'a>,
    device_trust_manager: DeviceTrustManager<'a>,
    security_monitor: SessionSecurityMonitor<'a>,
    default_policies: SessionPolicies,
}

impl<'a> AdvancedAuthService<'a> {
    pub fn new(store: &'a Store, config: AuthConfig) -> Self {
        Self {
            store,
            config,
            session_manager: AdvancedSessionManager::new(store),
            device_trust_manager: DeviceTrustManager::new(store),
            security_monitor: SessionSecurityMonitor::new(store),
            default_policies: SessionPolicies::default(),
        }
    }

    /// Extract device information from user agent
    fn extract_device_info(&self, user_agent: &Option<String>) -> Result<DeviceInfo> {
        let mut device_info = DeviceInfo {
            fingerprint: String::new(),
            platform: None,
            browser: None,
            browser_version: None,
            os: None,
            os_version: None,
            device_type: crate::models::DeviceType::Unknown,
            trusted: false,
        };

        if let Some(ua_string) = user_agent {
            // Simple user agent parsing (in production, would use the user-agent-parser crate)
            if ua_string.contains("Chrome") {
                device_info.browser = Some("Chrome".to_string());
            } else if ua_string.contains("Firefox") {
                device_info.browser = Some("Firefox".to_string());
            } else if ua_string.contains("Safari") {
                device_info.browser = Some("Safari".to_string());
            }

            if ua_string.contains("Windows") {
                device_info.os = Some("Windows".to_string());
            } else if ua_string.contains("Mac") {
                device_info.os = Some("macOS".to_string());
            } else if ua_string.contains("Linux") {
                device_info.os = Some("Linux".to_string());
            }

            if ua_string.contains("Mobile") {
                device_info.device_type = crate::models::DeviceType::Mobile;
            } else {
                device_info.device_type = crate::models::DeviceType::Desktop;
            }

            // Generate device fingerprint
            let mut hasher = Sha256::new();
            hasher.update(ua_string.as_bytes());
            let hash = hasher.finalize();
            device_info.fingerprint = format!("fp_{}", hex::encode(&hash[..16]));
        } else {
            device_info.device_type = crate::models::DeviceType::Api;
            device_info.fingerprint = format!("api-{}", Ulid::new());
        }

        Ok(device_info)
    }

    /// Enhanced login with comprehensive security monitoring and device tracking
    pub async fn enhanced_login(
        &self,
        org_id: u64,
        email: &str,
        password: &str,
        env_slug: Option<String>,
        ip_address: Option<String>,
        user_agent: Option<String>,
        session_policies: Option<SessionPolicies>,
    ) -> Result<EnhancedLoginResult> {
        let repo = AuthRepository::new(self.store);

        // Step 1: Security assessment of the login attempt
        let security_assessment = self.security_monitor.monitor_auth_attempt(
            ip_address.clone(),
            Some(email.to_string()),
            user_agent.clone(),
            false, // Will update to true if successful
        ).await?;

        // Block if security assessment indicates high risk
        if security_assessment.blocked {
            return Ok(EnhancedLoginResult {
                success: false,
                tokens: None,
                device_registration: None,
                security_info: EnhancedSecurityInfo {
                    risk_level: security_assessment.risk_level,
                    blocked: true,
                    reasons: security_assessment.reasons,
                    required_actions: security_assessment.required_actions,
                    device_trust_required: false,
                    verification_token: None,
                },
            });
        }

        // Step 2: Authenticate user credentials
        let user = match repo.get_user_by_email(org_id, &email.to_lowercase())? {
            Some(user) => user,
            None => {
                // Update security monitor with failed attempt
                self.security_monitor.monitor_auth_attempt(
                    ip_address,
                    Some(email.to_string()),
                    user_agent,
                    false,
                ).await?;

                return Err(anyhow!("Invalid credentials"));
            }
        };

        if !crate::hash::verify_password(&user.password_hash, password)? {
            // Update security monitor with failed attempt
            self.security_monitor.monitor_auth_attempt(
                ip_address,
                Some(email.to_string()),
                user_agent,
                false,
            ).await?;

            return Err(anyhow!("Invalid credentials"));
        }

        // Update security monitor with successful authentication
        self.security_monitor.monitor_auth_attempt(
            ip_address.clone(),
            Some(email.to_string()),
            user_agent.clone(),
            true,
        ).await?;

        // Step 3: Device registration and trust assessment
        let device_info = self.extract_device_info(&user_agent)?;
        let device_registration = self.device_trust_manager.register_device(
            user.id,
            org_id,
            &device_info,
            ip_address.clone(),
            user_agent.clone(),
        ).await?;

        // Step 4: Determine environment
        let (live_env, sandbox_env) = repo.ensure_default_environments(org_id)?;
        let target_env = match env_slug.as_deref() {
            Some("sandbox") => sandbox_env,
            Some("live") | None => live_env,
            Some(other) => repo
                .get_environment_by_slug(org_id, other)?
                .ok_or_else(|| anyhow!("Unknown environment"))?,
        };

        // Step 5: Apply session policies
        let policies = session_policies.unwrap_or_else(|| self.default_policies.clone());

        // Check if device verification is required
        let verification_required = device_registration.verification_required
            || policies.device_verification_required
            || matches!(security_assessment.risk_level, RiskLevel::High | RiskLevel::Critical);

        if verification_required && device_registration.verification_token.is_none() {
            // Device verification is required but no token was generated
            // This means the device needs explicit verification
            return Ok(EnhancedLoginResult {
                success: false,
                tokens: None,
                device_registration: Some(device_registration),
                security_info: EnhancedSecurityInfo {
                    risk_level: security_assessment.risk_level,
                    blocked: false,
                    reasons: security_assessment.reasons,
                    required_actions: vec!["Device verification required".to_string()],
                    device_trust_required: true,
                    verification_token: None,
                },
            });
        }

        // Step 6: Create advanced session
        let session = self.session_manager.create_session(
            &user,
            target_env.id,
            ip_address.clone(),
            user_agent.clone(),
            &policies,
        ).await?;

        // Step 7: Create session fingerprint for hijacking detection
        self.security_monitor.create_session_fingerprint(session.id, &session).await?;

        // Step 8: Generate JWT tokens
        let now = current_timestamp();
        let access_exp = now + self.config.access_ttl().as_secs() as i64;
        let refresh_exp = now + self.config.refresh_ttl().as_secs() as i64;

        let access_claims = crate::jwt::AccessClaims {
            sub: user.id.to_string(),
            org: org_id,
            env: target_env.id.to_string(),
            roles: user.roles.clone(),
            labels: user.labels.clone(),
            sid: session.id.to_string(),
            iat: now,
            exp: access_exp,
            typ: "access".into(),
        };

        let refresh_claims = crate::jwt::RefreshClaims {
            sub: user.id.to_string(),
            org: org_id,
            sid: session.id.to_string(),
            env: target_env.id.to_string(),
            iat: now,
            exp: refresh_exp,
            typ: "refresh".into(),
        };

        let keys = self.config.jwt_keys();
        let access_token = keys.encode_access(&access_claims)?;
        let refresh_token = keys.encode_refresh(&refresh_claims)?;

        // Step 9: Store refresh token session
        let session_payload = bincode::serialize(&refresh_claims)?;
        repo.insert_session(session.id, &session_payload)?;

        // Step 10: Update device trust
        self.device_trust_manager.update_device_trust(
            device_registration.device_id,
            user.id,
            org_id,
            TrustEvent::SuccessfulLogin,
        ).await?;

        Ok(EnhancedLoginResult {
            success: true,
            tokens: Some(LoginTokens {
                access_token,
                refresh_token,
                access_expires_at: access_exp,
                refresh_expires_at: refresh_exp,
                environment: crate::EnvironmentSummary::from(target_env),
            }),
            device_registration: Some(device_registration.clone()),
            security_info: EnhancedSecurityInfo {
                risk_level: security_assessment.risk_level,
                blocked: false,
                reasons: security_assessment.reasons,
                required_actions: security_assessment.required_actions,
                device_trust_required: false,
                verification_token: device_registration.verification_token,
            },
        })
    }

    /// Validate session with comprehensive security checks
    pub async fn validate_session(
        &self,
        session_id: Ulid,
        current_ip: Option<String>,
        current_user_agent: Option<String>,
    ) -> Result<SessionValidationResult> {
        // Basic session validation
        let basic_validation = self.session_manager.validate_session(
            session_id,
            current_ip.clone(),
            current_user_agent.clone(),
        ).await?;

        if !basic_validation.valid {
            return Ok(SessionValidationResult {
                valid: false,
                warnings: basic_validation.warnings,
                requires_reauth: true,
                risk_score: basic_validation.risk_score,
                security_events: Vec::new(),
            });
        }

        // Advanced security validation
        let security_validation = self.security_monitor.validate_session_security(
            session_id,
            &SessionRequest {
                ip_address: current_ip,
                user_agent: current_user_agent,
                timestamp: current_timestamp(),
            },
        ).await?;

        let mut warnings = basic_validation.warnings;
        warnings.extend(security_validation.warnings);

        Ok(SessionValidationResult {
            valid: security_validation.valid,
            warnings,
            requires_reauth: basic_validation.requires_reauth || security_validation.requires_reauth,
            risk_score: (basic_validation.risk_score + security_validation.risk_score) / 2.0,
            security_events: Vec::new(), // Would be populated with recent security events
        })
    }

    /// Verify a device using verification token
    pub async fn verify_device(
        &self,
        user_id: Ulid,
        org_id: u64,
        verification_token: &str,
        device_name: Option<String>,
    ) -> Result<crate::device_trust::DeviceVerificationResult> {
        self.device_trust_manager.verify_device(
            user_id,
            org_id,
            verification_token,
            device_name,
        ).await
    }

    /// Get comprehensive security analysis for an organization
    pub async fn get_security_analysis(&self, org_id: u64) -> Result<ComprehensiveSecurityAnalysis> {
        let session_analysis = self.security_monitor.analyze_session_patterns(org_id).await?;
        let cleanup_summary = self.session_manager.cleanup_expired_sessions(org_id).await?;

        Ok(ComprehensiveSecurityAnalysis {
            session_analysis,
            cleanup_summary,
            timestamp: current_timestamp(),
        })
    }

    /// Perform security cleanup and maintenance
    pub async fn security_maintenance(&self) -> Result<MaintenanceSummary> {
        // Clean up expired tracking data
        self.security_monitor.cleanup_expired_data().await?;

        Ok(MaintenanceSummary {
            cleanup_completed: true,
            timestamp: current_timestamp(),
        })
    }

    /// Get device trust information
    pub async fn get_device_trust_info(
        &self,
        user_id: Ulid,
        device_fingerprint: &str,
    ) -> Result<crate::device_trust::DeviceTrustInfo> {
        self.device_trust_manager.get_device_trust_info(user_id, device_fingerprint).await
    }

    /// Revoke device trust (security incident response)
    pub async fn revoke_device_trust(
        &self,
        device_id: Ulid,
        user_id: Ulid,
        org_id: u64,
        reason: &str,
        revoked_by: Option<Ulid>,
    ) -> Result<()> {
        self.device_trust_manager.revoke_device_trust(
            device_id,
            user_id,
            org_id,
            reason,
            revoked_by,
        ).await
    }

    /// Revoke session with comprehensive audit logging
    pub async fn revoke_session(
        &self,
        session_id: Ulid,
        revoked_by: Option<Ulid>,
        reason: &str,
    ) -> Result<()> {
        self.session_manager.revoke_session(session_id, revoked_by, reason).await
    }
}

/// Enhanced login result with comprehensive security information
#[derive(Debug, Clone)]
pub struct EnhancedLoginResult {
    pub success: bool,
    pub tokens: Option<LoginTokens>,
    pub device_registration: Option<DeviceRegistration>,
    pub security_info: EnhancedSecurityInfo,
}

/// Enhanced security information for login attempts
#[derive(Debug, Clone)]
pub struct EnhancedSecurityInfo {
    pub risk_level: RiskLevel,
    pub blocked: bool,
    pub reasons: Vec<String>,
    pub required_actions: Vec<String>,
    pub device_trust_required: bool,
    pub verification_token: Option<String>,
}

/// Session validation result with security analysis
#[derive(Debug, Clone)]
pub struct SessionValidationResult {
    pub valid: bool,
    pub warnings: Vec<String>,
    pub requires_reauth: bool,
    pub risk_score: f64,
    pub security_events: Vec<String>,
}

/// Comprehensive security analysis combining multiple data sources
#[derive(Debug, Clone)]
pub struct ComprehensiveSecurityAnalysis {
    pub session_analysis: crate::session_security::SecurityAnalysis,
    pub cleanup_summary: crate::advanced_session::CleanupSummary,
    pub timestamp: i64,
}

/// Maintenance summary for security operations
#[derive(Debug, Clone)]
pub struct MaintenanceSummary {
    pub cleanup_completed: bool,
    pub timestamp: i64,
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}