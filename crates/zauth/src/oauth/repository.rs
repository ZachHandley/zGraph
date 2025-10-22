use super::models::{
    OAuthAccountLink, OAuthProfile, OAuthProvider, OAuthSession, OAuthState, UnifiedUserProfile,
};
use anyhow::Result;
use ulid::Ulid;
use zcore_storage::{encode_key, Store};

#[derive(Clone)]
pub struct OAuthRepository<'a> {
    store: &'a Store,
}

impl<'a> std::fmt::Debug for OAuthRepository<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthRepository")
            .field("store", &"<Store>")
            .finish()
    }
}

impl<'a> OAuthRepository<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &'a Store {
        self.store
    }

    /// Insert or update OAuth profile
    pub fn upsert_oauth_profile(&self, profile: &OAuthProfile) -> Result<()> {
        let mut w = self.store.begin_write()?;

        // Profile key: oauth_profile:{org_id}:{profile_id}
        let profile_key = encode_key(&[
            b"oauth_profile",
            &profile.org_id.to_be_bytes(),
            profile.id.as_bytes(),
        ]);

        // Provider-user lookup key: oauth_provider_user:{org_id}:{provider}:{provider_user_id}
        let provider_user_key = encode_key(&[
            b"oauth_provider_user",
            &profile.org_id.to_be_bytes(),
            profile.provider.as_str().as_bytes(),
            profile.provider_user_id.as_bytes(),
        ]);

        let payload = bincode::serialize(profile)?;

        // Store profile
        w.set("oauth_profiles", profile_key, payload);

        // Store provider-user lookup
        w.set("oauth_provider_user", provider_user_key, profile.id.as_bytes().to_vec());

        // Store email lookup if email exists
        if let Some(email) = &profile.email {
            let email_key = encode_key(&[
                b"oauth_profile_email",
                &profile.org_id.to_be_bytes(),
                email.to_lowercase().as_bytes(),
            ]);
            w.set("oauth_profile_email", email_key, profile.id.as_bytes().to_vec());
        }

        w.commit(self.store)?;
        Ok(())
    }

    /// Get OAuth profile by ID
    pub fn get_oauth_profile(&self, org_id: u64, profile_id: &str) -> Result<Option<OAuthProfile>> {
        let r = self.store.begin_read()?;

        let key = encode_key(&[
            b"oauth_profile",
            &org_id.to_be_bytes(),
            profile_id.as_bytes(),
        ]);

        if let Some(val) = r.get(self.store, "oauth_profiles", &key)? {
            let profile: OAuthProfile = bincode::deserialize(&val)?;
            return Ok(Some(profile));
        }
        Ok(None)
    }

    /// Get OAuth profile by provider and provider user ID
    pub fn get_oauth_profile_by_provider_user(
        &self,
        org_id: u64,
        provider: &OAuthProvider,
        provider_user_id: &str,
    ) -> Result<Option<OAuthProfile>> {
        let r = self.store.begin_read()?;

        let provider_user_key = encode_key(&[
            b"oauth_provider_user",
            &org_id.to_be_bytes(),
            provider.as_str().as_bytes(),
            provider_user_id.as_bytes(),
        ]);

        if let Some(profile_id_val) = r.get(self.store, "oauth_provider_user", &provider_user_key)? {
            let profile_id = String::from_utf8(profile_id_val)?;
            return self.get_oauth_profile(org_id, &profile_id);
        }
        Ok(None)
    }

    /// Get OAuth profile by email
    pub fn get_oauth_profile_by_email(
        &self,
        org_id: u64,
        email: &str,
    ) -> Result<Option<OAuthProfile>> {
        let r = self.store.begin_read()?;

        let email_key = encode_key(&[
            b"oauth_profile_email",
            &org_id.to_be_bytes(),
            email.to_lowercase().as_bytes(),
        ]);

        if let Some(profile_id_val) = r.get(self.store, "oauth_profile_email", &email_key)? {
            let profile_id = String::from_utf8(profile_id_val)?;
            return self.get_oauth_profile(org_id, &profile_id);
        }
        Ok(None)
    }

    /// Insert OAuth account link
    pub fn insert_account_link(&self, link: &OAuthAccountLink) -> Result<()> {
        let mut w = self.store.begin_write()?;

        // Link key: oauth_link:{org_id}:{link_id}
        let link_key = encode_key(&[
            b"oauth_link",
            &link.org_id.to_be_bytes(),
            link.id.to_string().as_bytes(),
        ]);

        let payload = bincode::serialize(link)?;
        w.set("oauth_account_links", link_key, payload);

        w.commit(self.store)?;
        Ok(())
    }

    /// Get account links for user
    pub fn get_account_links_for_user(
        &self,
        org_id: u64,
        user_id: &Ulid,
    ) -> Result<Vec<OAuthAccountLink>> {
        let r = self.store.begin_read()?;

        let prefix = encode_key(&[b"oauth_link", &org_id.to_be_bytes()]);
        let results = r.scan_prefix(self.store, "oauth_account_links", &prefix)?;
        let mut links = Vec::new();

        for (_, val) in results {
            let link: OAuthAccountLink = bincode::deserialize(&val)?;
            if link.user_id == *user_id {
                links.push(link);
            }
        }

        Ok(links)
    }

    /// Get account link by OAuth profile
    pub fn get_account_link_by_oauth_profile(
        &self,
        org_id: u64,
        oauth_profile_id: &str,
    ) -> Result<Option<OAuthAccountLink>> {
        let r = self.store.begin_read()?;

        let prefix = encode_key(&[b"oauth_link", &org_id.to_be_bytes()]);
        let results = r.scan_prefix(self.store, "oauth_account_links", &prefix)?;

        for (_, val) in results {
            let link: OAuthAccountLink = bincode::deserialize(&val)?;
            if link.oauth_profile_id == oauth_profile_id {
                return Ok(Some(link));
            }
        }

        Ok(None)
    }

    /// Delete account link
    pub fn delete_account_link(&self, org_id: u64, link_id: &Ulid) -> Result<()> {
        let mut w = self.store.begin_write()?;

        let link_key = encode_key(&[
            b"oauth_link",
            &org_id.to_be_bytes(),
            link_id.to_string().as_bytes(),
        ]);
        w.delete("oauth_account_links", link_key);

        w.commit(self.store)?;
        Ok(())
    }

    /// Store OAuth state for CSRF protection
    pub fn store_oauth_state(&self, state: &OAuthState) -> Result<()> {
        let mut w = self.store.begin_write()?;

        let state_key = encode_key(&[b"oauth_state", state.state_id.as_bytes()]);
        let payload = bincode::serialize(state)?;
        w.set("oauth_states", state_key, payload);

        w.commit(self.store)?;
        Ok(())
    }

    /// Get and consume OAuth state
    pub fn consume_oauth_state(&self, state_id: &str) -> Result<Option<OAuthState>> {
        let state_key = encode_key(&[b"oauth_state", state_id.as_bytes()]);

        // First read the state
        let state = {
            let r = self.store.begin_read()?;
            if let Some(val) = r.get(self.store, "oauth_states", &state_key)? {
                Some(bincode::deserialize::<OAuthState>(&val)?)
            } else {
                None
            }
        };

        // If state exists, delete it (consume)
        if state.is_some() {
            let mut w = self.store.begin_write()?;
            w.delete("oauth_states", state_key);
            w.commit(self.store)?;
        }

        Ok(state)
    }

    /// Store OAuth session
    pub fn store_oauth_session(&self, session: &OAuthSession) -> Result<()> {
        let mut w = self.store.begin_write()?;

        let session_key = encode_key(&[b"oauth_session", session.session_id.as_bytes()]);
        let payload = bincode::serialize(session)?;
        w.set("oauth_sessions", session_key, payload);

        w.commit(self.store)?;
        Ok(())
    }

    /// Get OAuth session
    pub fn get_oauth_session(&self, session_id: &str) -> Result<Option<OAuthSession>> {
        let r = self.store.begin_read()?;

        let session_key = encode_key(&[b"oauth_session", session_id.as_bytes()]);

        if let Some(val) = r.get(self.store, "oauth_sessions", &session_key)? {
            let session: OAuthSession = bincode::deserialize(&val)?;
            return Ok(Some(session));
        }
        Ok(None)
    }

    /// Delete OAuth session
    pub fn delete_oauth_session(&self, session_id: &str) -> Result<()> {
        let mut w = self.store.begin_write()?;

        let session_key = encode_key(&[b"oauth_session", session_id.as_bytes()]);
        w.delete("oauth_sessions", session_key);

        w.commit(self.store)?;
        Ok(())
    }

    /// Get unified user profile
    pub fn get_unified_user_profile(
        &self,
        org_id: u64,
        user_id: &Ulid,
    ) -> Result<Option<UnifiedUserProfile>> {
        use crate::repository::AuthRepository;

        let auth_repo = AuthRepository::new(self.store);

        // Get base user record
        let Some(user) = auth_repo.get_user_by_id(org_id, user_id)? else {
            return Ok(None);
        };

        // Get OAuth account links
        let account_links = self.get_account_links_for_user(org_id, user_id)?;

        // Get OAuth profiles for linked accounts
        let mut oauth_profiles = Vec::new();
        for link in &account_links {
            if let Some(profile) = self.get_oauth_profile(org_id, &link.oauth_profile_id)? {
                oauth_profiles.push(profile);
            }
        }

        // Determine primary name and avatar
        let primary_name = oauth_profiles
            .iter()
            .find_map(|p| p.name.as_ref())
            .unwrap_or(&user.email)
            .clone();

        let primary_avatar = oauth_profiles
            .iter()
            .find_map(|p| p.avatar_url.as_ref())
            .cloned();

        Ok(Some(UnifiedUserProfile {
            user_id: user.id,
            org_id: user.org_id,
            email: user.email,
            primary_name,
            primary_avatar,
            oauth_profiles,
            account_links,
            created_at: user.created_at,
            updated_at: user.updated_at,
        }))
    }

    /// Clean expired OAuth states and sessions
    pub fn cleanup_expired_oauth_data(&self, current_time: i64) -> Result<()> {
        // First, find expired items
        let mut expired_state_keys = Vec::new();
        let mut expired_session_keys = Vec::new();

        {
            let r = self.store.begin_read()?;

            // Find expired states
            let state_prefix = encode_key(&[b"oauth_state"]);
            let state_results = r.scan_prefix(self.store, "oauth_states", &state_prefix)?;
            for (key, val) in state_results {
                let state: OAuthState = bincode::deserialize(&val)?;
                if state.expires_at < current_time {
                    expired_state_keys.push(key);
                }
            }

            // Find expired sessions
            let session_prefix = encode_key(&[b"oauth_session"]);
            let session_results = r.scan_prefix(self.store, "oauth_sessions", &session_prefix)?;
            for (key, val) in session_results {
                let session: OAuthSession = bincode::deserialize(&val)?;
                if session.expires_at < current_time {
                    expired_session_keys.push(key);
                }
            }
        }

        // Now delete expired items
        if !expired_state_keys.is_empty() || !expired_session_keys.is_empty() {
            let mut w = self.store.begin_write()?;

            for key in expired_state_keys {
                w.delete("oauth_states", key);
            }

            for key in expired_session_keys {
                w.delete("oauth_sessions", key);
            }

            w.commit(self.store)?;
        }

        Ok(())
    }
}