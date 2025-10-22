//! WebAuthn/FIDO2 hardware security key support

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use webauthn_rs::{
    prelude::*,
    Webauthn, WebauthnBuilder,
};
use zcore_storage::{Store, TBL_WEBAUTHN_CREDENTIALS};

use crate::models::{WebAuthnCredential, TwoFactorSettings, ChallengeType};
use crate::repository::AuthRepository;

/// WebAuthn service for FIDO2 hardware security key support
pub struct WebAuthnService<'a> {
    repo: AuthRepository<'a>,
    webauthn: Webauthn,
}

impl<'a> WebAuthnService<'a> {
    /// Create a new WebAuthn service
    pub fn new(
        store: &'a Store,
        rp_origin: &str,
        rp_id: &str,
        rp_name: &str,
    ) -> Result<Self> {
        let webauthn = WebauthnBuilder::new(rp_id, &Url::parse(rp_origin)?)?
            .rp_name(rp_name)
            .build()?;

        Ok(Self {
            repo: AuthRepository::new(store),
            webauthn,
        })
    }

    /// Start WebAuthn registration process
    pub fn start_registration(
        &self,
        user_id: &Ulid,
        org_id: u64,
        user_email: &str,
        display_name: &str,
        exclude_credentials: Option<Vec<CredentialID>>,
    ) -> Result<(CreationChallengeResponse, PasskeyRegistration)> {
        let user_unique_id = Uuid::new_v4();
        let existing_creds = match exclude_credentials {
            Some(creds) => creds,
            None => self.get_user_credential_ids(user_id, org_id)?,
        };

        let (ccr, reg_state) = self.webauthn.start_passkey_registration(
            user_unique_id,
            user_email,
            display_name,
            Some(existing_creds),
        )?;

        Ok((ccr, reg_state))
    }

    /// Finish WebAuthn registration process
    pub fn finish_registration(
        &self,
        user_id: &Ulid,
        org_id: u64,
        reg: &RegisterPublicKeyCredential,
        reg_state: &PasskeyRegistration,
        credential_name: &str,
    ) -> Result<WebAuthnCredential> {
        let passkey = self.webauthn.finish_passkey_registration(reg, reg_state)?;

        let credential = WebAuthnCredential {
            id: Ulid::new(),
            user_id: *user_id,
            org_id,
            credential_id: passkey.cred_id().to_vec(),
            public_key: serde_json::to_vec(&passkey)?,
            aaguid: vec![], // AAGUID not accessible in new API
            counter: 0, // Passkeys typically don't use counters
            name: credential_name.to_string(),
            backup_eligible: false, // Default value for new API
            backup_state: false, // Default value for new API
            created_at: current_timestamp(),
            last_used_at: None,
        };

        self.repo.insert_webauthn_credential(&credential)?;

        // Update user's 2FA settings to enable WebAuthn
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

        settings.webauthn_enabled = true;
        if settings.default_method.is_none() {
            settings.default_method = Some(ChallengeType::WebAuthn);
        }
        settings.updated_at = current_timestamp();

        self.repo.update_2fa_settings(&settings)?;

        Ok(credential)
    }

    /// Start WebAuthn authentication process
    pub fn start_authentication(
        &self,
        user_id: &Ulid,
        org_id: u64,
    ) -> Result<(RequestChallengeResponse, PasskeyAuthentication)> {
        let credentials = self.get_user_passkeys(user_id, org_id)?;

        if credentials.is_empty() {
            return Err(anyhow!("No WebAuthn credentials found for user"));
        }

        let (rcr, auth_state) = self.webauthn.start_passkey_authentication(&credentials)?;

        Ok((rcr, auth_state))
    }

    /// Finish WebAuthn authentication process
    pub fn finish_authentication(
        &self,
        _user_id: &Ulid,
        _org_id: u64,
        auth: &PublicKeyCredential,
        auth_state: &PasskeyAuthentication,
    ) -> Result<AuthenticationResult> {
        let result = self.webauthn.finish_passkey_authentication(auth, auth_state)?;

        // Note: WebAuthn-rs 0.5.2+ handles credential updates internally

        Ok(result)
    }

    /// Get user's WebAuthn credentials
    pub fn get_user_credentials(&self, user_id: &Ulid, org_id: u64) -> Result<Vec<WebAuthnCredential>> {
        self.repo.list_webauthn_credentials(user_id, org_id)
    }

    /// Get credential IDs for exclusion during registration
    fn get_user_credential_ids(&self, user_id: &Ulid, org_id: u64) -> Result<Vec<CredentialID>> {
        let credentials = self.repo.list_webauthn_credentials(user_id, org_id)?;
        Ok(credentials.iter().map(|c| CredentialID::from(c.credential_id.clone())).collect())
    }

    /// Get user's passkeys for authentication
    fn get_user_passkeys(&self, user_id: &Ulid, org_id: u64) -> Result<Vec<Passkey>> {
        let credentials = self.repo.list_webauthn_credentials(user_id, org_id)?;
        let mut passkeys = Vec::new();

        for credential in credentials {
            let passkey: Passkey = serde_json::from_slice(&credential.public_key)?;
            passkeys.push(passkey);
        }

        Ok(passkeys)
    }

    /// Remove a WebAuthn credential
    pub fn remove_credential(
        &self,
        user_id: &Ulid,
        org_id: u64,
        credential_id: &Ulid,
    ) -> Result<()> {
        // Verify the credential belongs to the user
        if let Some(credential) = self.repo.get_webauthn_credential(credential_id)? {
            if credential.user_id != *user_id || credential.org_id != org_id {
                return Err(anyhow!("Credential does not belong to user"));
            }
        } else {
            return Err(anyhow!("Credential not found"));
        }

        self.repo.delete_webauthn_credential(credential_id)?;

        // Check if this was the last WebAuthn credential
        let remaining_credentials = self.repo.list_webauthn_credentials(user_id, org_id)?;
        if remaining_credentials.is_empty() {
            // Disable WebAuthn in 2FA settings
            if let Some(mut settings) = self.repo.get_2fa_settings(user_id, org_id)? {
                settings.webauthn_enabled = false;
                if settings.default_method == Some(ChallengeType::WebAuthn) {
                    // Find a new default method
                    settings.default_method = None;
                    if settings.totp_enabled {
                        settings.default_method = Some(ChallengeType::Totp);
                    } else if settings.sms_enabled {
                        settings.default_method = Some(ChallengeType::Sms);
                    }
                }
                settings.updated_at = current_timestamp();
                self.repo.update_2fa_settings(&settings)?;
            }
        }

        Ok(())
    }

    /// Update credential name
    pub fn update_credential_name(
        &self,
        user_id: &Ulid,
        org_id: u64,
        credential_id: &Ulid,
        new_name: &str,
    ) -> Result<()> {
        if let Some(mut credential) = self.repo.get_webauthn_credential(credential_id)? {
            if credential.user_id != *user_id || credential.org_id != org_id {
                return Err(anyhow!("Credential does not belong to user"));
            }

            credential.name = new_name.to_string();
            self.repo.update_webauthn_credential(&credential)?;
        } else {
            return Err(anyhow!("Credential not found"));
        }

        Ok(())
    }

    /// Get WebAuthn registration options for a user
    pub fn get_registration_info(&self, org_id: u64) -> Result<WebAuthnRegistrationInfo> {
        let org_policy = self.repo.get_org_2fa_policy(org_id)?;
        let max_credentials = org_policy.map(|p| p.max_webauthn_devices).unwrap_or(5);

        Ok(WebAuthnRegistrationInfo {
            max_credentials,
            supported_algorithms: vec!["ES256".to_string(), "RS256".to_string()],
            user_verification: "preferred".to_string(),
            authenticator_attachment: None, // Allow both platform and cross-platform
            require_resident_key: false,
        })
    }
}

/// WebAuthn registration information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebAuthnRegistrationInfo {
    pub max_credentials: u32,
    pub supported_algorithms: Vec<String>,
    pub user_verification: String,
    pub authenticator_attachment: Option<String>,
    pub require_resident_key: bool,
}

/// Extended repository methods for WebAuthn
impl<'a> AuthRepository<'a> {
    /// Insert a WebAuthn credential
    pub fn insert_webauthn_credential(&self, credential: &WebAuthnCredential) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, TBL_WEBAUTHN_CREDENTIALS)?;
            let key = encode_key(&[
                b"webauthn_credential",
                &credential.org_id.to_be_bytes(),
                credential.user_id.to_string().as_bytes(),
                credential.id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(credential)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    /// Update a WebAuthn credential
    pub fn update_webauthn_credential(&self, credential: &WebAuthnCredential) -> Result<()> {
        self.insert_webauthn_credential(credential) // Same as insert for this implementation
    }

    /// Get a WebAuthn credential by ID
    pub fn get_webauthn_credential(&self, credential_id: &Ulid) -> Result<Option<WebAuthnCredential>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, TBL_WEBAUTHN_CREDENTIALS)?;

        // Scan to find credential since we don't know the org_id/user_id
        let mut it = tbl.iter()?;
        while let Some(Ok((_, val))) = it.next() {
            let credential: WebAuthnCredential = bincode::deserialize(&val)?;
            if credential.id == *credential_id {
                return Ok(Some(credential));
            }
        }
        Ok(None)
    }

    /// Get a WebAuthn credential by credential ID
    pub fn get_webauthn_credential_by_id(&self, cred_id: &[u8]) -> Result<Option<WebAuthnCredential>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, TBL_WEBAUTHN_CREDENTIALS)?;

        // Scan to find credential by credential_id
        let mut it = tbl.iter()?;
        while let Some(Ok((_, val))) = it.next() {
            let credential: WebAuthnCredential = bincode::deserialize(&val)?;
            if credential.credential_id == cred_id {
                return Ok(Some(credential));
            }
        }
        Ok(None)
    }

    /// List WebAuthn credentials for a user
    pub fn list_webauthn_credentials(&self, user_id: &Ulid, org_id: u64) -> Result<Vec<WebAuthnCredential>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, TBL_WEBAUTHN_CREDENTIALS)?;
        let (start, end) = zcore_storage::prefix_range(&[
            b"webauthn_credential",
            &org_id.to_be_bytes(),
            user_id.to_string().as_bytes(),
        ]);

        let mut credentials = Vec::new();
        let it = tbl.range(start.as_slice()..end.as_slice())?;
        for (_, val) in it {
            let credential: WebAuthnCredential = bincode::deserialize(&val)?;
            credentials.push(credential);
        }

        credentials.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(credentials)
    }

    /// Delete a WebAuthn credential
    pub fn delete_webauthn_credential(&self, credential_id: &Ulid) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, TBL_WEBAUTHN_CREDENTIALS)?;

            // Find and remove the credential
            let mut key_to_remove = None;
            {
                let it = tbl.iter()?;
                for item in it {
                    let (key, val) = item?;
                    let credential: WebAuthnCredential = bincode::deserialize(&val)?;
                    if credential.id == *credential_id {
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
}

/// Utility functions
fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// Re-export needed items from zcore_storage
use zcore_storage::encode_key;