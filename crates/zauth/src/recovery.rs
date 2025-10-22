//! Device recovery and trust management for 2FA

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use base64::Engine;
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use zcore_storage::Store;
use znotifications::{NotificationService, TemplateContext, EmailOptions, SmsOptions};

use crate::hash::{hash_password, verify_password};
use crate::models::{TrustedDevice, AuditLogEntry, AuditAction, AuditStatus};
use crate::repository::AuthRepository;

/// Service for managing device trust and recovery mechanisms
pub struct RecoveryService<'a> {
    repo: AuthRepository<'a>,
    notification_service: Option<NotificationService>,
    rng: SystemRandom,
}

impl<'a> RecoveryService<'a> {
    /// Create a new recovery service
    pub fn new(store: &'a Store, notification_service: Option<NotificationService>) -> Self {
        Self {
            repo: AuthRepository::new(store),
            notification_service,
            rng: SystemRandom::new(),
        }
    }

    /// Trust a device for a user
    pub fn trust_device(
        &self,
        user_id: &Ulid,
        org_id: u64,
        device_fingerprint: &str,
        device_name: &str,
        remember_days: u32,
        ip_address: Option<String>,
        user_agent: Option<String>,
    ) -> Result<TrustedDevice> {
        let now = current_timestamp();
        let trusted_until = now + (remember_days as i64 * 86400);

        let trusted_device = TrustedDevice {
            id: Ulid::new(),
            user_id: *user_id,
            org_id,
            device_fingerprint: device_fingerprint.to_string(),
            device_name: device_name.to_string(),
            trusted_until,
            created_at: now,
            last_seen_at: now,
            ip_address: ip_address.clone(),
            user_agent: user_agent.clone(),
        };

        self.repo.insert_trusted_device(&trusted_device)?;

        // Log device trust
        let audit_entry = AuditLogEntry {
            id: Ulid::new(),
            org_id,
            session_id: None,
            user_id: Some(*user_id),
            team_id: None,
            action: AuditAction::ResourceCreate,
            resource_type: Some("trusted_device".to_string()),
            resource_id: Some(trusted_device.id.to_string()),
            details: serde_json::json!({
                "device_name": device_name,
                "device_fingerprint": device_fingerprint,
                "trusted_until": trusted_until,
                "action": "trust_device"
            }),
            ip_address,
            user_agent,
            timestamp: now,
            status: AuditStatus::Success,
        };

        self.repo.insert_audit_log(&audit_entry)?;

        Ok(trusted_device)
    }

    /// Check if a device is trusted
    pub fn is_device_trusted(
        &self,
        user_id: &Ulid,
        org_id: u64,
        device_fingerprint: &str,
    ) -> Result<bool> {
        let trusted_devices = self.list_trusted_devices(user_id, org_id)?;
        let now = current_timestamp();

        for device in trusted_devices {
            if device.device_fingerprint == device_fingerprint && device.trusted_until > now {
                // Update last seen time
                let mut updated_device = device;
                updated_device.last_seen_at = now;
                self.repo.update_trusted_device(&updated_device)?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Remove device trust
    pub fn untrust_device(
        &self,
        user_id: &Ulid,
        org_id: u64,
        device_id: &Ulid,
    ) -> Result<()> {
        // Verify the device belongs to the user
        if let Some(device) = self.repo.get_trusted_device_by_id(*device_id)? {
            if device.user_id != *user_id || device.org_id != org_id {
                return Err(anyhow!("Device does not belong to user"));
            }
        } else {
            return Err(anyhow!("Trusted device not found"));
        }

        self.repo.delete_trusted_device(device_id)?;

        // Log device untrust
        let audit_entry = AuditLogEntry {
            id: Ulid::new(),
            org_id,
            session_id: None,
            user_id: Some(*user_id),
            team_id: None,
            action: AuditAction::ResourceDelete,
            resource_type: Some("trusted_device".to_string()),
            resource_id: Some(device_id.to_string()),
            details: serde_json::json!({
                "action": "untrust_device"
            }),
            ip_address: None,
            user_agent: None,
            timestamp: current_timestamp(),
            status: AuditStatus::Success,
        };

        self.repo.insert_audit_log(&audit_entry)?;

        Ok(())
    }

    /// List trusted devices for a user
    pub fn list_trusted_devices(
        &self,
        user_id: &Ulid,
        org_id: u64,
    ) -> Result<Vec<TrustedDevice>> {
        let devices = self.repo.list_trusted_devices(user_id, org_id)?;
        let now = current_timestamp();

        // Filter out expired devices
        Ok(devices.into_iter()
            .filter(|device| device.trusted_until > now)
            .collect())
    }

    /// Clean up expired trusted devices
    pub fn cleanup_expired_devices(&self, org_id: u64) -> Result<u32> {
        let count = self.repo.cleanup_expired_trusted_devices(org_id)?;
        Ok(count)
    }

    /// Initiate account recovery process
    pub async fn initiate_account_recovery(
        &self,
        org_id: u64,
        email: &str,
        recovery_method: RecoveryMethod,
    ) -> Result<Ulid> {
        // Find user by email
        let user = self.repo.get_user_by_email(org_id, email)?
            .ok_or_else(|| anyhow!("User not found"))?;

        let settings = self.repo.get_2fa_settings(&user.id, org_id)?;

        match recovery_method {
            RecoveryMethod::Email => {
                self.send_recovery_email(&user.id, org_id, email).await
            }
            RecoveryMethod::Sms => {
                if let Some(settings) = settings {
                    if let Some(phone_number) = settings.phone_number {
                        self.send_recovery_sms(&user.id, org_id, &phone_number).await
                    } else {
                        Err(anyhow!("No verified phone number for SMS recovery"))
                    }
                } else {
                    Err(anyhow!("SMS recovery not available"))
                }
            }
            RecoveryMethod::SecurityQuestions => {
                // TODO: Implement security questions
                Err(anyhow!("Security questions not implemented"))
            }
        }
    }

    /// Send recovery email
    async fn send_recovery_email(
        &self,
        user_id: &Ulid,
        org_id: u64,
        email: &str,
    ) -> Result<Ulid> {
        // Generate recovery token
        let mut token_bytes = [0u8; 32];
        self.rng.fill(&mut token_bytes)?;
        let recovery_token = base64::engine::general_purpose::STANDARD.encode(&token_bytes);

        let recovery_id = Ulid::new();
        let expires_at = current_timestamp() + 3600; // 1 hour

        // Store recovery request
        let recovery_request = RecoveryRequest {
            id: recovery_id,
            user_id: *user_id,
            org_id,
            recovery_type: RecoveryMethod::Email,
            token_hash: hash_password(&recovery_token)?,
            contact: email.to_string(),
            expires_at,
            created_at: current_timestamp(),
            used: false,
            used_at: None,
        };

        self.repo.insert_recovery_request(&recovery_request)?;

        // Send recovery email
        if let Some(ref notification_service) = self.notification_service {
            let recovery_url = format!("https://app.example.com/auth/recover?token={}", recovery_token);

            let context = TemplateContext::new()
                .insert_str("user_name", email) // TODO: Use actual name
                .insert_str("user_email", email)
                .insert_str("org_name", "ZRUSTDB") // TODO: Get org name
                .insert_str("recovery_url", &recovery_url)
                .insert_number("expires_in_hours", 1.0);

            let _ = notification_service.send_email_notification(
                org_id,
                "account_recovery",
                email,
                context,
                EmailOptions::default(),
            ).await;
        }

        Ok(recovery_id)
    }

    /// Send recovery SMS
    async fn send_recovery_sms(
        &self,
        user_id: &Ulid,
        org_id: u64,
        phone_number: &str,
    ) -> Result<Ulid> {
        // Generate recovery code
        let mut code_bytes = [0u8; 3];
        self.rng.fill(&mut code_bytes)?;
        let recovery_code = format!("{:06}",
            u32::from_be_bytes([0, code_bytes[0], code_bytes[1], code_bytes[2]]) % 1000000
        );

        let recovery_id = Ulid::new();
        let expires_at = current_timestamp() + 600; // 10 minutes

        // Store recovery request
        let recovery_request = RecoveryRequest {
            id: recovery_id,
            user_id: *user_id,
            org_id,
            recovery_type: RecoveryMethod::Sms,
            token_hash: hash_password(&recovery_code)?,
            contact: phone_number.to_string(),
            expires_at,
            created_at: current_timestamp(),
            used: false,
            used_at: None,
        };

        self.repo.insert_recovery_request(&recovery_request)?;

        // Send recovery SMS
        if let Some(ref notification_service) = self.notification_service {
            let context = TemplateContext::new()
                .insert_str("org_name", "ZRUSTDB") // TODO: Get org name
                .insert_str("recovery_code", &recovery_code)
                .insert_number("expires_in", 10.0);

            let _ = notification_service.send_sms_notification(
                org_id,
                "account_recovery_sms",
                phone_number,
                context,
                SmsOptions::default(),
            ).await;
        }

        Ok(recovery_id)
    }

    /// Verify recovery token/code
    pub fn verify_recovery(
        &self,
        recovery_id: &Ulid,
        token_or_code: &str,
    ) -> Result<RecoveryVerificationResult> {
        let mut recovery_request = self.repo.get_recovery_request(recovery_id)?
            .ok_or_else(|| anyhow!("Recovery request not found"))?;

        if recovery_request.used {
            return Err(anyhow!("Recovery token already used"));
        }

        if recovery_request.expires_at < current_timestamp() {
            return Err(anyhow!("Recovery token expired"));
        }

        if !verify_password(&recovery_request.token_hash, token_or_code)? {
            return Err(anyhow!("Invalid recovery token"));
        }

        // Mark as used
        recovery_request.used = true;
        recovery_request.used_at = Some(current_timestamp());
        self.repo.update_recovery_request(&recovery_request)?;

        // Generate temporary access token for 2FA bypass
        let mut temp_token_bytes = [0u8; 32];
        self.rng.fill(&mut temp_token_bytes)?;
        let temp_access_token = base64::engine::general_purpose::STANDARD.encode(&temp_token_bytes);

        // Store temporary access token (valid for 15 minutes)
        let temp_access = TempAccess {
            token_hash: hash_password(&temp_access_token)?,
            user_id: recovery_request.user_id,
            org_id: recovery_request.org_id,
            expires_at: current_timestamp() + 900, // 15 minutes
            purpose: "2fa_recovery".to_string(),
        };

        self.repo.insert_temp_access(&temp_access)?;

        Ok(RecoveryVerificationResult {
            user_id: recovery_request.user_id,
            org_id: recovery_request.org_id,
            temp_access_token,
            expires_at: temp_access.expires_at,
        })
    }

    /// Disable 2FA using recovery access
    pub async fn disable_2fa_with_recovery(
        &self,
        temp_access_token: &str,
        new_password: Option<String>,
    ) -> Result<()> {
        // Verify temporary access token
        let temp_access = self.verify_temp_access(temp_access_token)?;

        // Disable all 2FA methods
        if let Some(mut settings) = self.repo.get_2fa_settings(&temp_access.user_id, temp_access.org_id)? {
            settings.totp_enabled = false;
            settings.sms_enabled = false;
            settings.email_enabled = false;
            settings.webauthn_enabled = false;
            settings.backup_codes_enabled = false;
            settings.default_method = None;
            settings.updated_at = current_timestamp();

            self.repo.update_2fa_settings(&settings)?;
        }

        // Clear all 2FA-related data
        self.repo.clear_totp_secrets(&temp_access.user_id, temp_access.org_id)?;
        self.repo.clear_backup_codes(&temp_access.user_id, temp_access.org_id)?;
        self.repo.clear_webauthn_credentials(&temp_access.user_id, temp_access.org_id)?;

        // Update password if provided
        if let Some(password) = new_password {
            if let Some(mut user) = self.repo.get_user_by_id(temp_access.org_id, &temp_access.user_id)? {
                user.password_hash = crate::hash::hash_password(&password)?;
                user.updated_at = current_timestamp();
                self.repo.update_user(&user)?;
            }
        }

        // Revoke temporary access
        self.repo.revoke_temp_access(temp_access_token)?;

        // Send security notification
        if let Some(ref notification_service) = self.notification_service {
            if let Some(user) = self.repo.get_user_by_id(temp_access.org_id, &temp_access.user_id)? {
                let context = TemplateContext::new()
                    .insert_str("user_name", &user.email) // TODO: Use actual name
                    .insert_str("user_email", &user.email)
                    .insert_str("org_name", "ZRUSTDB"); // TODO: Get org name

                let _ = notification_service.send_email_notification(
                    temp_access.org_id,
                    "2fa_disabled_recovery",
                    &user.email,
                    context,
                    EmailOptions::default(),
                ).await;
            }
        }

        Ok(())
    }

    /// Verify temporary access token
    fn verify_temp_access(&self, token: &str) -> Result<TempAccess> {
        self.repo.get_temp_access_by_token(token)?
            .ok_or_else(|| anyhow!("Invalid or expired recovery token"))
    }
}

/// Recovery methods available
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RecoveryMethod {
    Email,
    Sms,
    SecurityQuestions,
}

/// Recovery request record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryRequest {
    pub id: Ulid,
    pub user_id: Ulid,
    pub org_id: u64,
    pub recovery_type: RecoveryMethod,
    pub token_hash: String,
    pub contact: String, // Email or phone number
    pub expires_at: i64,
    pub created_at: i64,
    pub used: bool,
    pub used_at: Option<i64>,
}

/// Temporary access token for recovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TempAccess {
    pub token_hash: String,
    pub user_id: Ulid,
    pub org_id: u64,
    pub expires_at: i64,
    pub purpose: String,
}

/// Result of recovery verification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryVerificationResult {
    pub user_id: Ulid,
    pub org_id: u64,
    pub temp_access_token: String,
    pub expires_at: i64,
}

/// Extended repository methods for recovery that are not yet implemented in main repository
impl<'a> AuthRepository<'a> {
    /// List trusted devices for a user
    pub fn list_trusted_devices(&self, user_id: &Ulid, org_id: u64) -> Result<Vec<TrustedDevice>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, zcore_storage::COL_TRUSTED_DEVICES)?;
        let (start, end) = zcore_storage::prefix_range(&[
            b"trusted_device",
            &org_id.to_be_bytes(),
            user_id.to_string().as_bytes(),
        ]);

        let mut devices = Vec::new();
        let it = tbl.range(start.as_slice()..end.as_slice())?;
        for (_, val) in it {
            let device: TrustedDevice = bincode::deserialize(&val)?;
            devices.push(device);
        }

        devices.sort_by(|a, b| b.last_seen_at.cmp(&a.last_seen_at));
        Ok(devices)
    }

    /// Delete a trusted device
    pub fn delete_trusted_device(&self, device_id: &Ulid) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, zcore_storage::COL_TRUSTED_DEVICES)?;

            // Find and remove the device
            let mut key_to_remove = None;
            {
                let it = tbl.iter()?;
                for item in it {
                    let (key, val) = item?;
                    let device: TrustedDevice = bincode::deserialize(&val)?;
                    if device.id == *device_id {
                        key_to_remove = Some(key.to_vec());
                        break;
                    }
                }
            }

            if let Some(key) = key_to_remove {
                tbl.remove(key.as_slice())?;
            }
        }
        w.commit(self.store)?;
        Ok(())
    }

    /// Clean up expired trusted devices
    pub fn cleanup_expired_trusted_devices(&self, _org_id: u64) -> Result<u32> {
        // TODO: Implement cleanup of expired devices
        // This would scan through devices and remove expired ones
        Ok(0)
    }

    /// Placeholder methods for recovery requests and temp access
    pub fn insert_recovery_request(&self, _request: &RecoveryRequest) -> Result<()> {
        // TODO: Implement recovery request storage
        Ok(())
    }

    pub fn get_recovery_request(&self, _id: &Ulid) -> Result<Option<RecoveryRequest>> {
        // TODO: Implement recovery request retrieval
        Ok(None)
    }

    pub fn update_recovery_request(&self, _request: &RecoveryRequest) -> Result<()> {
        // TODO: Implement recovery request update
        Ok(())
    }

    pub fn insert_temp_access(&self, _access: &TempAccess) -> Result<()> {
        // TODO: Implement temp access storage
        Ok(())
    }

    pub fn get_temp_access_by_token(&self, _token: &str) -> Result<Option<TempAccess>> {
        // TODO: Implement temp access retrieval
        Ok(None)
    }

    pub fn revoke_temp_access(&self, _token: &str) -> Result<()> {
        // TODO: Implement temp access revocation
        Ok(())
    }
}

/// Utility functions
fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}