//! Two-Factor Authentication policy enforcement

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use zcore_storage::Store;
use znotifications::{NotificationService, TemplateContext, EmailOptions};

use crate::models::{
    OrgTwoFactorPolicy, TwoFactorSettings, ChallengeType, AuditLogEntry,
    AuditAction, AuditStatus,
};
use crate::repository::AuthRepository;

/// Service for managing and enforcing 2FA policies
pub struct TwoFactorPolicyService<'a> {
    repo: AuthRepository<'a>,
    notification_service: Option<NotificationService>,
}

impl<'a> TwoFactorPolicyService<'a> {
    /// Create a new 2FA policy service
    pub fn new(store: &'a Store, notification_service: Option<NotificationService>) -> Self {
        Self {
            repo: AuthRepository::new(store),
            notification_service,
        }
    }

    /// Create or update organization 2FA policy
    pub async fn update_org_policy(
        &self,
        org_id: u64,
        policy: &OrgTwoFactorPolicy,
        updated_by: &Ulid,
    ) -> Result<()> {
        // Validate policy
        self.validate_policy(policy)?;

        let mut policy_to_save = policy.clone();
        policy_to_save.updated_at = current_timestamp();
        policy_to_save.updated_by = *updated_by;

        self.repo.update_org_2fa_policy(&policy_to_save)?;

        // Log the policy change
        let audit_entry = AuditLogEntry {
            id: Ulid::new(),
            org_id,
            session_id: None,
            user_id: Some(*updated_by),
            team_id: None,
            action: AuditAction::ResourceUpdate,
            resource_type: Some("2fa_policy".to_string()),
            resource_id: Some(org_id.to_string()),
            details: serde_json::json!({
                "policy": policy_to_save,
                "action": "update_2fa_policy"
            }),
            ip_address: None,
            user_agent: None,
            timestamp: current_timestamp(),
            status: AuditStatus::Success,
        };

        self.repo.insert_audit_log(&audit_entry)?;

        // If 2FA is now required, notify users who don't have it enabled
        if policy.required {
            self.notify_users_2fa_required(org_id).await?;
        }

        Ok(())
    }

    /// Get organization 2FA policy
    pub fn get_org_policy(&self, org_id: u64) -> Result<OrgTwoFactorPolicy> {
        Ok(self.repo.get_org_2fa_policy(org_id)?
            .unwrap_or_else(|| OrgTwoFactorPolicy::default()))
    }

    /// Check if a user complies with the organization's 2FA policy
    pub fn check_user_compliance(
        &self,
        user_id: &Ulid,
        org_id: u64,
    ) -> Result<ComplianceStatus> {
        let policy = self.get_org_policy(org_id)?;
        let settings = self.repo.get_2fa_settings(user_id, org_id)?;

        if !policy.required {
            return Ok(ComplianceStatus {
                compliant: true,
                required: false,
                enabled_methods: settings.map(|s| self.get_enabled_methods(&s)).unwrap_or_default(),
                missing_requirements: Vec::new(),
                grace_period_ends: None,
            });
        }

        let Some(settings) = settings else {
            return Ok(ComplianceStatus {
                compliant: false,
                required: true,
                enabled_methods: Vec::new(),
                missing_requirements: vec!["No 2FA methods enabled".to_string()],
                grace_period_ends: Some(self.calculate_grace_period_end(user_id, org_id, &policy)?),
            });
        };

        let enabled_methods = self.get_enabled_methods(&settings);
        let mut missing_requirements = Vec::new();

        // Check if user has at least one allowed method enabled
        let has_allowed_method = enabled_methods.iter()
            .any(|method| policy.allowed_methods.contains(method));

        if !has_allowed_method {
            missing_requirements.push("Must enable at least one allowed 2FA method".to_string());
        }

        // Check backup codes requirement
        if policy.require_backup_codes && !settings.backup_codes_enabled {
            missing_requirements.push("Backup codes required".to_string());
        }

        // Check device limits
        let totp_count = self.repo.list_totp_secrets(user_id, org_id)?.len() as u32;
        if totp_count > policy.max_totp_devices {
            missing_requirements.push(format!(
                "Too many TOTP devices ({} > {})",
                totp_count, policy.max_totp_devices
            ));
        }

        let webauthn_count = self.repo.list_webauthn_credentials(user_id, org_id)?.len() as u32;
        if webauthn_count > policy.max_webauthn_devices {
            missing_requirements.push(format!(
                "Too many WebAuthn devices ({} > {})",
                webauthn_count, policy.max_webauthn_devices
            ));
        }

        let compliant = missing_requirements.is_empty();
        let grace_period_ends = if compliant {
            None
        } else {
            Some(self.calculate_grace_period_end(user_id, org_id, &policy)?)
        };

        Ok(ComplianceStatus {
            compliant,
            required: true,
            enabled_methods,
            missing_requirements,
            grace_period_ends,
        })
    }

    /// Enforce 2FA policy for a user (check compliance and take action)
    pub async fn enforce_policy(
        &self,
        user_id: &Ulid,
        org_id: u64,
    ) -> Result<EnforcementAction> {
        let compliance = self.check_user_compliance(user_id, org_id)?;

        if compliance.compliant || !compliance.required {
            return Ok(EnforcementAction::Allow);
        }

        // Check if grace period has expired
        if let Some(grace_period_ends) = compliance.grace_period_ends {
            let now = current_timestamp();
            if now > grace_period_ends {
                // Grace period expired - require 2FA setup
                self.log_policy_enforcement(user_id, org_id, "grace_period_expired").await?;
                return Ok(EnforcementAction::Require2FA);
            } else {
                // Still in grace period - warn user
                let days_remaining = (grace_period_ends - now) / 86400;
                self.send_grace_period_warning(user_id, org_id, days_remaining).await?;
                return Ok(EnforcementAction::Warn(days_remaining));
            }
        }

        Ok(EnforcementAction::Require2FA)
    }

    /// Get all users who don't comply with 2FA policy
    pub fn get_non_compliant_users(&self, _org_id: u64) -> Result<Vec<NonCompliantUser>> {
        let policy = self.get_org_policy(_org_id)?;
        if !policy.required {
            return Ok(Vec::new());
        }

        // This is a simplified implementation - in practice, you'd want to
        // paginate through users and check compliance
        let non_compliant = Vec::new();

        // TODO: Implement user listing and compliance checking
        // For now, return empty list as this would require iterating through all users

        Ok(non_compliant)
    }

    /// Send policy update notifications to organization administrators
    pub async fn notify_policy_update(
        &self,
        _org_id: u64,
        _policy: &OrgTwoFactorPolicy,
        _updated_by: &Ulid,
    ) -> Result<()> {
        if let Some(ref notification_service) = self.notification_service {
            // TODO: Get org admins and send notifications
            // This would require a method to get organization administrators
            let _ = notification_service;
        }
        Ok(())
    }

    // Private helper methods

    fn validate_policy(&self, policy: &OrgTwoFactorPolicy) -> Result<()> {
        if policy.grace_period_days > 365 {
            return Err(anyhow!("Grace period cannot exceed 365 days"));
        }

        if policy.max_totp_devices == 0 || policy.max_totp_devices > 10 {
            return Err(anyhow!("TOTP device limit must be between 1 and 10"));
        }

        if policy.max_webauthn_devices == 0 || policy.max_webauthn_devices > 20 {
            return Err(anyhow!("WebAuthn device limit must be between 1 and 20"));
        }

        if policy.session_timeout_minutes < 5 || policy.session_timeout_minutes > 1440 {
            return Err(anyhow!("Session timeout must be between 5 minutes and 24 hours"));
        }

        if policy.allowed_methods.is_empty() {
            return Err(anyhow!("At least one 2FA method must be allowed"));
        }

        Ok(())
    }

    fn get_enabled_methods(&self, settings: &TwoFactorSettings) -> Vec<ChallengeType> {
        let mut methods = Vec::new();
        if settings.totp_enabled {
            methods.push(ChallengeType::Totp);
        }
        if settings.sms_enabled {
            methods.push(ChallengeType::Sms);
        }
        if settings.email_enabled {
            methods.push(ChallengeType::Email);
        }
        if settings.webauthn_enabled {
            methods.push(ChallengeType::WebAuthn);
        }
        if settings.backup_codes_enabled {
            methods.push(ChallengeType::BackupCode);
        }
        methods
    }

    fn calculate_grace_period_end(
        &self,
        user_id: &Ulid,
        org_id: u64,
        policy: &OrgTwoFactorPolicy,
    ) -> Result<i64> {
        // Try to get user creation date, fall back to policy creation date
        if let Some(user) = self.repo.get_user_by_id(org_id, user_id)? {
            Ok(user.created_at + (policy.grace_period_days as i64 * 86400))
        } else {
            // Fallback to current time + grace period
            Ok(current_timestamp() + (policy.grace_period_days as i64 * 86400))
        }
    }

    async fn notify_users_2fa_required(&self, _org_id: u64) -> Result<()> {
        if let Some(ref notification_service) = self.notification_service {
            // TODO: Get all users without 2FA and send notifications
            // This would require iterating through organization users
            let _ = notification_service;
        }
        Ok(())
    }

    async fn send_grace_period_warning(
        &self,
        user_id: &Ulid,
        org_id: u64,
        days_remaining: i64,
    ) -> Result<()> {
        if let Some(ref notification_service) = self.notification_service {
            if let Some(user) = self.repo.get_user_by_id(org_id, user_id)? {
                let context = TemplateContext::new()
                    .insert_str("user_name", &user.email) // TODO: Use actual name
                    .insert_str("user_email", &user.email)
                    .insert_str("org_name", "ZRUSTDB") // TODO: Get org name
                    .insert_number("days_remaining", days_remaining as f64);

                let _ = notification_service.send_email_notification(
                    org_id,
                    "2fa_grace_period_warning",
                    &user.email,
                    context,
                    EmailOptions::default(),
                ).await;
            }
        }
        Ok(())
    }

    async fn log_policy_enforcement(
        &self,
        user_id: &Ulid,
        org_id: u64,
        action: &str,
    ) -> Result<()> {
        let audit_entry = AuditLogEntry {
            id: Ulid::new(),
            org_id,
            session_id: None,
            user_id: Some(*user_id),
            team_id: None,
            action: AuditAction::ResourceAccess,
            resource_type: Some("2fa_policy".to_string()),
            resource_id: Some(org_id.to_string()),
            details: serde_json::json!({
                "action": action,
                "enforcement": "2fa_policy"
            }),
            ip_address: None,
            user_agent: None,
            timestamp: current_timestamp(),
            status: AuditStatus::Success,
        };

        self.repo.insert_audit_log(&audit_entry)
    }
}

/// User compliance status with 2FA policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceStatus {
    pub compliant: bool,
    pub required: bool,
    pub enabled_methods: Vec<ChallengeType>,
    pub missing_requirements: Vec<String>,
    pub grace_period_ends: Option<i64>,
}

/// Action to take for policy enforcement
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnforcementAction {
    Allow,
    Warn(i64), // Days remaining in grace period
    Require2FA,
}

/// Information about a non-compliant user
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonCompliantUser {
    pub user_id: Ulid,
    pub email: String,
    pub enabled_methods: Vec<ChallengeType>,
    pub missing_requirements: Vec<String>,
    pub grace_period_ends: Option<i64>,
}

/// Policy enforcement statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyStats {
    pub total_users: u32,
    pub compliant_users: u32,
    pub non_compliant_users: u32,
    pub grace_period_users: u32,
    pub enabled_methods_breakdown: std::collections::HashMap<ChallengeType, u32>,
}

/// Utility functions
fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}