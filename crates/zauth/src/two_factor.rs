//! Two-Factor Authentication service implementation

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use data_encoding::BASE32;
use qrcode::QrCode;
use ring::rand::{SecureRandom, SystemRandom};
use serde::Serialize;
use totp_rs::{Algorithm, Secret, TOTP};
use ulid::Ulid;
use webauthn_rs::{
    prelude::*,
    Webauthn, WebauthnBuilder,
};
use zcore_storage::Store;
use znotifications::{NotificationService, TemplateContext, EmailOptions, SmsOptions};

use crate::hash::{hash_password, verify_password};
use crate::models::*;
use crate::repository::AuthRepository;

/// Two-Factor Authentication service
pub struct TwoFactorService<'a> {
    repo: AuthRepository<'a>,
    notification_service: Option<NotificationService>,
    _webauthn: Webauthn,
    rng: SystemRandom,
}

impl<'a> TwoFactorService<'a> {
    /// Create a new 2FA service
    pub fn new(
        store: &'a Store,
        notification_service: Option<NotificationService>,
        rp_origin: &str,
        rp_id: &str,
    ) -> Result<Self> {
        let webauthn = WebauthnBuilder::new(rp_id, &Url::parse(rp_origin)?)?
            .build()?;

        Ok(Self {
            repo: AuthRepository::new(store),
            notification_service,
            _webauthn: webauthn,
            rng: SystemRandom::new(),
        })
    }

    /// Generate TOTP setup for a user
    pub fn generate_totp_setup(
        &self,
        user_id: &Ulid,
        org_id: u64,
        issuer: &str,
        account_name: &str,
    ) -> Result<TotpSetup> {
        // Generate random secret
        let mut secret_bytes = [0u8; 20];
        self.rng.fill(&mut secret_bytes)?;
        let secret = BASE32.encode(&secret_bytes);

        let totp = TOTP::new(
            Algorithm::SHA1,
            6,
            1,
            30,
            Secret::Raw(secret_bytes.to_vec()).to_bytes().unwrap(),
            Some(issuer.to_string()),
            account_name.to_string(),
        )?;

        // Store unverified TOTP secret
        let totp_secret = TotpSecret {
            id: Ulid::new(),
            user_id: *user_id,
            org_id,
            secret: secret.clone(),
            algorithm: TotpAlgorithm::Sha1,
            digits: 6,
            period: 30,
            issuer: issuer.to_string(),
            account_name: account_name.to_string(),
            verified: false,
            created_at: current_timestamp(),
            verified_at: None,
            last_used_at: None,
        };

        self.repo.insert_totp_secret(&totp_secret)?;

        // Generate QR code
        let qr_code_url = totp.get_url();
        let qr_code = QrCode::new(&qr_code_url)?;
        let qr_image = qr_code.render::<image::Luma<u8>>().build();

        // Convert QR code to PNG bytes
        let mut qr_png_bytes = Vec::new();
        qr_image.write_to(
            &mut std::io::Cursor::new(&mut qr_png_bytes),
            image::ImageFormat::Png,
        )?;

        Ok(TotpSetup {
            secret_id: totp_secret.id,
            secret,
            qr_code_url,
            qr_code_png: qr_png_bytes,
            backup_codes: Vec::new(), // Generated after verification
        })
    }

    /// Verify TOTP setup and enable 2FA
    pub async fn verify_totp_setup(
        &self,
        user_id: &Ulid,
        org_id: u64,
        secret_id: &Ulid,
        code: &str,
    ) -> Result<Vec<String>> {
        // Get unverified TOTP secret
        let mut totp_secret = self.repo.get_totp_secret(secret_id)?
            .ok_or_else(|| anyhow!("TOTP secret not found"))?;

        if totp_secret.user_id != *user_id || totp_secret.org_id != org_id {
            return Err(anyhow!("TOTP secret does not belong to user"));
        }

        if totp_secret.verified {
            return Err(anyhow!("TOTP already verified"));
        }

        // Verify the code
        let totp = TOTP::new(
            Algorithm::SHA1,
            6,
            1,
            30,
            Secret::Encoded(totp_secret.secret.clone()).to_bytes().unwrap(),
            Some(totp_secret.issuer.clone()),
            totp_secret.account_name.clone(),
        )?;

        if !totp.check_current(code)? {
            return Err(anyhow!("Invalid TOTP code"));
        }

        // Mark TOTP as verified
        totp_secret.verified = true;
        totp_secret.verified_at = Some(current_timestamp());
        totp_secret.last_used_at = Some(current_timestamp());

        self.repo.update_totp_secret(&totp_secret)?;

        // Generate backup codes
        let backup_codes = self.generate_backup_codes(user_id, org_id)?;

        // Update user's 2FA settings
        let mut settings = self.repo.get_2fa_settings(user_id, org_id)?
            .unwrap_or_else(|| TwoFactorSettings {
                user_id: *user_id,
                org_id,
                totp_enabled: false,
                sms_enabled: false,
                email_enabled: false,
                webauthn_enabled: false,
                backup_codes_enabled: false,
                default_method: None,
                phone_number: None,
                phone_verified: false,
                recovery_email: None,
                recovery_email_verified: false,
                updated_at: current_timestamp(),
            });

        settings.totp_enabled = true;
        settings.backup_codes_enabled = true;
        if settings.default_method.is_none() {
            settings.default_method = Some(ChallengeType::Totp);
        }
        settings.updated_at = current_timestamp();

        self.repo.update_2fa_settings(&settings)?;

        // Send setup notification email
        if let Some(ref notification_service) = self.notification_service {
            if let Some(user) = self.repo.get_user_by_id(org_id, user_id)? {
                let context = TemplateContext::new()
                    .insert_str("user_name", &user.email) // TODO: Use actual name
                    .insert_str("user_email", &user.email)
                    .insert_str("org_name", "ZRUSTDB") // TODO: Get org name
                    .insert("backup_codes", &backup_codes)?;

                let _ = notification_service.send_email_notification(
                    org_id,
                    "2fa_setup",
                    &user.email,
                    context,
                    EmailOptions::default(),
                ).await;
            }
        }

        Ok(backup_codes)
    }

    /// Generate backup codes for a user
    pub fn generate_backup_codes(&self, user_id: &Ulid, org_id: u64) -> Result<Vec<String>> {
        // Clear existing backup codes
        self.repo.clear_backup_codes(user_id, org_id)?;

        let mut codes = Vec::new();
        for _ in 0..10 {
            // Generate 8-character alphanumeric code
            let mut code_bytes = [0u8; 6];
            self.rng.fill(&mut code_bytes)?;
            let code = code_bytes.iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>()
                .to_uppercase();

            // Store hashed code
            let backup_code = BackupCode {
                id: Ulid::new(),
                user_id: *user_id,
                org_id,
                code: hash_password(&code)?,
                used: false,
                used_at: None,
                created_at: current_timestamp(),
            };

            self.repo.insert_backup_code(&backup_code)?;
            codes.push(format!("{}-{}", &code[..4], &code[4..]));
        }

        Ok(codes)
    }

    /// Initiate SMS 2FA setup
    pub async fn initiate_sms_setup(
        &self,
        user_id: &Ulid,
        org_id: u64,
        phone_number: &str,
    ) -> Result<Ulid> {
        // Validate phone number format
        if !is_valid_phone_number(phone_number) {
            return Err(anyhow!("Invalid phone number format"));
        }

        // Generate verification code
        let mut code_bytes = [0u8; 3];
        self.rng.fill(&mut code_bytes)?;
        let verification_code = format!("{:06}",
            u32::from_be_bytes([0, code_bytes[0], code_bytes[1], code_bytes[2]]) % 1000000
        );

        let verification = SmsVerification {
            id: Ulid::new(),
            user_id: *user_id,
            org_id,
            phone_number: phone_number.to_string(),
            verification_code: hash_password(&verification_code)?,
            verified: false,
            expires_at: current_timestamp() + 600, // 10 minutes
            created_at: current_timestamp(),
            verified_at: None,
            attempts: 0,
            max_attempts: 3,
        };

        self.repo.insert_sms_verification(&verification)?;

        // Send SMS
        if let Some(ref notification_service) = self.notification_service {
            let context = TemplateContext::new()
                .insert_str("org_name", "ZRUSTDB") // TODO: Get org name
                .insert_str("verification_code", &verification_code)
                .insert_number("expires_in", 10.0);

            let _ = notification_service.send_sms_notification(
                org_id,
                "2fa_code_sms",
                phone_number,
                context,
                SmsOptions::default(),
            ).await;
        }

        Ok(verification.id)
    }

    /// Verify SMS setup
    pub fn verify_sms_setup(
        &self,
        user_id: &Ulid,
        org_id: u64,
        verification_id: &Ulid,
        code: &str,
    ) -> Result<()> {
        let mut verification = self.repo.get_sms_verification(verification_id)?
            .ok_or_else(|| anyhow!("SMS verification not found"))?;

        if verification.user_id != *user_id || verification.org_id != org_id {
            return Err(anyhow!("SMS verification does not belong to user"));
        }

        if verification.verified {
            return Err(anyhow!("SMS already verified"));
        }

        if verification.expires_at < current_timestamp() {
            return Err(anyhow!("Verification code expired"));
        }

        if verification.attempts >= verification.max_attempts {
            return Err(anyhow!("Too many verification attempts"));
        }

        verification.attempts += 1;
        self.repo.update_sms_verification(&verification)?;

        if !verify_password(&verification.verification_code, code)? {
            return Err(anyhow!("Invalid verification code"));
        }

        // Mark as verified
        verification.verified = true;
        verification.verified_at = Some(current_timestamp());
        self.repo.update_sms_verification(&verification)?;

        // Update 2FA settings
        let mut settings = self.repo.get_2fa_settings(user_id, org_id)?
            .unwrap_or_else(|| TwoFactorSettings {
                user_id: *user_id,
                org_id,
                totp_enabled: false,
                sms_enabled: false,
                email_enabled: false,
                webauthn_enabled: false,
                backup_codes_enabled: false,
                default_method: None,
                phone_number: None,
                phone_verified: false,
                recovery_email: None,
                recovery_email_verified: false,
                updated_at: current_timestamp(),
            });

        settings.sms_enabled = true;
        settings.phone_number = Some(verification.phone_number.clone());
        settings.phone_verified = true;
        if settings.default_method.is_none() {
            settings.default_method = Some(ChallengeType::Sms);
        }
        settings.updated_at = current_timestamp();

        self.repo.update_2fa_settings(&settings)?;

        Ok(())
    }

    /// Create a 2FA challenge for login
    pub async fn create_challenge(
        &self,
        user_id: &Ulid,
        org_id: u64,
        preferred_method: Option<ChallengeType>,
    ) -> Result<TwoFactorChallenge> {
        let settings = self.repo.get_2fa_settings(user_id, org_id)?
            .ok_or_else(|| anyhow!("2FA not enabled for user"))?;

        // Determine challenge method
        let challenge_type = preferred_method
            .or(settings.default_method)
            .unwrap_or(ChallengeType::Totp);

        // Validate method is enabled
        match challenge_type {
            ChallengeType::Totp if !settings.totp_enabled => {
                return Err(anyhow!("TOTP not enabled"));
            }
            ChallengeType::Sms if !settings.sms_enabled => {
                return Err(anyhow!("SMS not enabled"));
            }
            ChallengeType::Email if !settings.email_enabled => {
                return Err(anyhow!("Email 2FA not enabled"));
            }
            _ => {}
        }

        let challenge_id = Ulid::new();
        let expires_at = current_timestamp() + 300; // 5 minutes

        let mut challenge_data = serde_json::json!({
            "challenge_id": challenge_id.to_string()
        });

        // Send challenge based on method
        match challenge_type {
            ChallengeType::Sms => {
                if let Some(ref phone_number) = settings.phone_number {
                    // Generate SMS code
                    let mut code_bytes = [0u8; 3];
                    self.rng.fill(&mut code_bytes)?;
                    let sms_code = format!("{:06}",
                        u32::from_be_bytes([0, code_bytes[0], code_bytes[1], code_bytes[2]]) % 1000000
                    );

                    challenge_data["sms_code_hash"] = serde_json::Value::String(hash_password(&sms_code)?);

                    // Send SMS
                    if let Some(ref notification_service) = self.notification_service {
                        let context = TemplateContext::new()
                            .insert_str("org_name", "ZRUSTDB") // TODO: Get org name
                            .insert_str("verification_code", &sms_code)
                            .insert_number("expires_in", 5.0);

                        let _ = notification_service.send_sms_notification(
                            org_id,
                            "2fa_code_sms",
                            phone_number,
                            context,
                            SmsOptions::default(),
                        ).await;
                    }
                }
            }
            _ => {
                // TOTP, Email, WebAuthn don't need server-side challenge data
            }
        }

        let challenge = TwoFactorChallenge {
            id: challenge_id,
            user_id: *user_id,
            org_id,
            challenge_type,
            challenge_data: serde_json::to_string(&challenge_data)?,
            verified: false,
            expires_at,
            created_at: current_timestamp(),
            verified_at: None,
            attempts: 0,
            max_attempts: 3,
        };

        self.repo.insert_2fa_challenge(&challenge)?;

        Ok(challenge)
    }

    /// Verify a 2FA challenge
    pub fn verify_challenge(
        &self,
        challenge_id: &Ulid,
        code: &str,
    ) -> Result<bool> {
        let mut challenge = self.repo.get_2fa_challenge(challenge_id)?
            .ok_or_else(|| anyhow!("Challenge not found"))?;

        if challenge.verified {
            return Err(anyhow!("Challenge already verified"));
        }

        if challenge.expires_at < current_timestamp() {
            return Err(anyhow!("Challenge expired"));
        }

        if challenge.attempts >= challenge.max_attempts {
            return Err(anyhow!("Too many verification attempts"));
        }

        challenge.attempts += 1;
        self.repo.update_2fa_challenge(&challenge)?;

        let verified = match challenge.challenge_type {
            ChallengeType::Totp => {
                self.verify_totp_code(&challenge.user_id, challenge.org_id, code)?
            }
            ChallengeType::Sms => {
                let challenge_data: serde_json::Value = serde_json::from_str(&challenge.challenge_data)?;
                if let Some(hash) = challenge_data["sms_code_hash"].as_str() {
                    verify_password(hash, code)?
                } else {
                    false
                }
            }
            ChallengeType::BackupCode => {
                self.verify_backup_code(&challenge.user_id, challenge.org_id, code)?
            }
            _ => false,
        };

        if verified {
            challenge.verified = true;
            challenge.verified_at = Some(current_timestamp());
            self.repo.update_2fa_challenge(&challenge)?;
        }

        Ok(verified)
    }

    /// Verify TOTP code
    fn verify_totp_code(&self, user_id: &Ulid, org_id: u64, code: &str) -> Result<bool> {
        let totp_secrets = self.repo.list_totp_secrets(user_id, org_id)?;

        for mut secret in totp_secrets {
            if !secret.verified {
                continue;
            }

            let totp = TOTP::new(
                Algorithm::SHA1,
                secret.digits as usize,
                1,
                secret.period,
                Secret::Encoded(secret.secret.clone()).to_bytes().unwrap(),
                Some(secret.issuer.clone()),
                secret.account_name.clone(),
            )?;

            if totp.check_current(code)? {
                secret.last_used_at = Some(current_timestamp());
                self.repo.update_totp_secret(&secret)?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Verify backup code
    fn verify_backup_code(&self, user_id: &Ulid, org_id: u64, code: &str) -> Result<bool> {
        let backup_codes = self.repo.list_backup_codes(user_id, org_id)?;

        for mut backup_code in backup_codes {
            if backup_code.used {
                continue;
            }

            // Remove formatting from input code
            let clean_code = code.replace('-', "").to_uppercase();

            if verify_password(&backup_code.code, &clean_code)? {
                backup_code.used = true;
                backup_code.used_at = Some(current_timestamp());
                self.repo.update_backup_code(&backup_code)?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Get 2FA status for a user
    pub fn get_2fa_status(&self, user_id: &Ulid, org_id: u64) -> Result<TwoFactorStatus> {
        let settings = self.repo.get_2fa_settings(user_id, org_id)?;
        let org_policy = self.repo.get_org_2fa_policy(org_id)?;

        let enabled_methods = settings.as_ref().map(|s| {
            let mut methods = Vec::new();
            if s.totp_enabled { methods.push(ChallengeType::Totp); }
            if s.sms_enabled { methods.push(ChallengeType::Sms); }
            if s.email_enabled { methods.push(ChallengeType::Email); }
            if s.webauthn_enabled { methods.push(ChallengeType::WebAuthn); }
            if s.backup_codes_enabled { methods.push(ChallengeType::BackupCode); }
            methods
        }).unwrap_or_default();

        let is_required = org_policy.map(|p| p.required).unwrap_or(false);
        let has_any_method = !enabled_methods.is_empty();

        Ok(TwoFactorStatus {
            enabled: has_any_method,
            required: is_required,
            enabled_methods,
            default_method: settings.as_ref().and_then(|s| s.default_method),
            phone_verified: settings.as_ref().map(|s| s.phone_verified).unwrap_or(false),
            backup_codes_remaining: self.repo.count_unused_backup_codes(user_id, org_id)?,
        })
    }

    /// Disable 2FA method
    pub fn disable_2fa_method(
        &self,
        user_id: &Ulid,
        org_id: u64,
        method: ChallengeType,
    ) -> Result<()> {
        let mut settings = self.repo.get_2fa_settings(user_id, org_id)?
            .ok_or_else(|| anyhow!("2FA settings not found"))?;

        match method {
            ChallengeType::Totp => {
                settings.totp_enabled = false;
                // Clear TOTP secrets
                self.repo.clear_totp_secrets(user_id, org_id)?;
            }
            ChallengeType::Sms => {
                settings.sms_enabled = false;
                settings.phone_number = None;
                settings.phone_verified = false;
            }
            ChallengeType::Email => {
                settings.email_enabled = false;
            }
            ChallengeType::WebAuthn => {
                settings.webauthn_enabled = false;
                // Clear WebAuthn credentials
                self.repo.clear_webauthn_credentials(user_id, org_id)?;
            }
            ChallengeType::BackupCode => {
                settings.backup_codes_enabled = false;
                // Clear backup codes
                self.repo.clear_backup_codes(user_id, org_id)?;
            }
        }

        // Update default method if it was the disabled one
        if settings.default_method == Some(method) {
            settings.default_method = None;
            // Find a new default method
            if settings.totp_enabled {
                settings.default_method = Some(ChallengeType::Totp);
            } else if settings.sms_enabled {
                settings.default_method = Some(ChallengeType::Sms);
            } else if settings.webauthn_enabled {
                settings.default_method = Some(ChallengeType::WebAuthn);
            }
        }

        settings.updated_at = current_timestamp();
        self.repo.update_2fa_settings(&settings)?;

        Ok(())
    }
}

/// TOTP setup information
#[derive(Debug, Clone, Serialize)]
pub struct TotpSetup {
    pub secret_id: Ulid,
    pub secret: String,
    pub qr_code_url: String,
    #[serde(with = "base64_serde")]
    pub qr_code_png: Vec<u8>,
    pub backup_codes: Vec<String>,
}

/// 2FA status for a user
#[derive(Debug, Clone, Serialize)]
pub struct TwoFactorStatus {
    pub enabled: bool,
    pub required: bool,
    pub enabled_methods: Vec<ChallengeType>,
    pub default_method: Option<ChallengeType>,
    pub phone_verified: bool,
    pub backup_codes_remaining: u32,
}

// Serde module for base64 encoding
mod base64_serde {
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
    }

    pub fn _deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD.decode(&s)
            .map_err(serde::de::Error::custom)
    }
}

/// Utility functions
fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn is_valid_phone_number(phone: &str) -> bool {
    // Basic phone validation
    let cleaned = phone.chars().filter(|c| c.is_ascii_digit() || *c == '+').collect::<String>();
    cleaned.len() >= 10 && cleaned.len() <= 16
}