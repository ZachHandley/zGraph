use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use ulid::Ulid;

use crate::models::{
    EnvironmentRecord, UserRecord, SessionRecord, SessionStatus, TeamRecord, TeamMemberRecord,
    TeamInvitation, TeamRole, RoleDelegation, AuditLogEntry,
    // 2FA models
    TotpSecret, BackupCode, TwoFactorChallenge, TwoFactorSettings,
    OrgTwoFactorPolicy, TrustedDevice, SmsVerification,
    // Credential models
    IdentityCredential, CredentialVerificationChallenge, SessionCredentialContext,
    IdentityAssertionRecord,
};
use crate::device_trust::{DeviceVerification, DeviceEventRecord};
use zcore_storage::{
    encode_key, prefix_range, Store, COL_ENVIRONMENTS, COL_SESSIONS, COL_USERS, COL_USER_EMAIL,
    COL_TOTP_SECRETS, COL_BACKUP_CODES, COL_SMS_VERIFICATIONS, COL_2FA_CHALLENGES,
    COL_2FA_SETTINGS, COL_WEBAUTHN_CREDENTIALS, COL_ORG_2FA_POLICIES, COL_TRUSTED_DEVICES,
    COL_ENHANCED_SESSIONS, COL_TEAMS, COL_TEAM_MEMBERS, COL_TEAM_INVITATIONS, COL_TEAM_NAME_INDEX,
    COL_ROLE_DELEGATIONS, COL_AUDIT_LOGS, COL_SESSION_RECORDS,
};

// Local collection names not yet migrated to zcore_storage

// Advanced session management collections (not in zcore_storage yet)
const COL_DEVICE_VERIFICATIONS: &str = "device_verifications";
const COL_DEVICE_EVENTS: &str = "device_events";

// Credential management collections
const COL_IDENTITY_CREDENTIALS: &str = "identity_credentials";
const COL_CREDENTIAL_CHALLENGES: &str = "credential_challenges";
const COL_SESSION_CREDENTIAL_CONTEXTS: &str = "session_credential_contexts";
const COL_IDENTITY_ASSERTIONS: &str = "identity_assertions";


// Table definitions for credential management
const TBL_IDENTITY_CREDENTIALS: &str = COL_IDENTITY_CREDENTIALS;
const TBL_CREDENTIAL_CHALLENGES: &str = COL_CREDENTIAL_CHALLENGES;
const TBL_SESSION_CREDENTIAL_CONTEXTS: &str = COL_SESSION_CREDENTIAL_CONTEXTS;
const TBL_IDENTITY_ASSERTIONS: &str = COL_IDENTITY_ASSERTIONS;

const LIVE_SLUG: &str = "live";
const SANDBOX_SLUG: &str = "sandbox";

#[derive(Clone)]
pub struct AuthRepository<'a> {
    pub store: &'a Store,
}

impl<'a> std::fmt::Debug for AuthRepository<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthRepository")
            .field("store", &"<Store>")
            .finish()
    }
}

impl<'a> AuthRepository<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    pub fn get_user_by_email(&self, org_id: u64, email: &str) -> Result<Option<UserRecord>> {
        let r = self.store.begin_read()?;
        let email_table = r.open_table(self.store, COL_USER_EMAIL)?;
        let key = encode_key(&[b"user_email", &org_id.to_be_bytes(), email.as_bytes()]);
        if let Some(user_id) = email_table.get(key.as_slice())? {
            let user_id = user_id.value();
            let users = r.open_table(self.store, COL_USERS)?;
            if let Some(val) = users.get(user_id)? {
                let user: UserRecord = bincode::deserialize(val.value())?;
                return Ok(Some(user));
            }
        }
        Ok(None)
    }

    pub fn insert_user(&self, user: &UserRecord) -> Result<()> {
        let mut w = self.store.begin_write()?;

        let email_key = encode_key(&[
            b"user_email",
            &user.org_id.to_be_bytes(),
            user.email.as_bytes(),
        ]);
        let user_key = encode_key(&[
            b"users",
            &user.org_id.to_be_bytes(),
            user.id.to_string().as_bytes(),
        ]);

        // Check if email already exists
        {
            let email_tbl = w.open_table(self.store, COL_USER_EMAIL)?;
            if email_tbl.get(email_key.as_slice())?.is_some() {
                return Err(anyhow!("email already registered"));
            }
        }

        // Insert user and email index
        {
            let mut user_tbl = w.open_table(self.store, COL_USERS)?;
            let payload = bincode::serialize(user)?;
            user_tbl.insert(user_key.as_slice(), payload.as_slice())?;
        }
        {
            let mut email_tbl = w.open_table(self.store, COL_USER_EMAIL)?;
            email_tbl.insert(email_key.as_slice(), user_key.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    pub fn update_user(&self, user: &UserRecord) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut user_tbl = w.open_table(self.store, COL_USERS)?;
            let user_key = encode_key(&[
                b"users",
                &user.org_id.to_be_bytes(),
                user.id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(user)?;
            user_tbl.insert(user_key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    pub fn get_user_by_id(&self, org_id: u64, user_id: &Ulid) -> Result<Option<UserRecord>> {
        let r = self.store.begin_read()?;
        let user_tbl = r.open_table(self.store, COL_USERS)?;
        let key = encode_key(&[
            b"users",
            &org_id.to_be_bytes(),
            user_id.to_string().as_bytes(),
        ]);
        if let Some(val) = user_tbl.get(key.as_slice())? {
            let user: UserRecord = bincode::deserialize(val.value())?;
            return Ok(Some(user));
        }
        Ok(None)
    }

    pub fn list_environments(&self, org_id: u64) -> Result<Vec<EnvironmentRecord>> {
        let r = self.store.begin_read()?;
        let env_tbl = r.open_table(self.store, COL_ENVIRONMENTS)?;
        let (start, end) = prefix_range(&[b"env", &org_id.to_be_bytes()]);
        let it = env_tbl.range(start.as_slice()..end.as_slice())?;
        let mut out = Vec::new();
        for (_, val) in it {
            let record: EnvironmentRecord = bincode::deserialize(&val)?;
            out.push(record);
        }
        out.sort_by(|a, b| a.slug.cmp(&b.slug));
        Ok(out)
    }

    pub fn ensure_default_environments(
        &self,
        org_id: u64,
    ) -> Result<(EnvironmentRecord, EnvironmentRecord)> {
        let existing = self.list_environments(org_id)?;
        let live = existing.iter().find(|env| env.slug == LIVE_SLUG).cloned();
        let sandbox = existing
            .iter()
            .find(|env| env.slug == SANDBOX_SLUG)
            .cloned();
        if let (Some(live_val), Some(sandbox_val)) = (live.as_ref(), sandbox.as_ref()) {
            return Ok((live_val.clone(), sandbox_val.clone()));
        }
        let now = current_timestamp();
        let live_rec = live.clone().unwrap_or_else(|| EnvironmentRecord {
            id: Ulid::new(),
            org_id,
            slug: LIVE_SLUG.into(),
            display_name: "Live".into(),
            auto_clear_policy: crate::models::AutoClearPolicy::Never,
            created_at: now,
        });
        let sandbox_rec = sandbox.clone().unwrap_or_else(|| EnvironmentRecord {
            id: Ulid::new(),
            org_id,
            slug: SANDBOX_SLUG.into(),
            display_name: "Sandbox".into(),
            auto_clear_policy: crate::models::AutoClearPolicy::OnDeploy,
            created_at: now,
        });
        let mut w = self.store.begin_write()?;
        {
            let mut env_tbl = w.open_table(self.store, COL_ENVIRONMENTS)?;
            if live.is_none() {
                let key = encode_key(&[
                    b"env",
                    &org_id.to_be_bytes(),
                    live_rec.id.to_string().as_bytes(),
                ]);
                env_tbl.insert(key.as_slice(), bincode::serialize(&live_rec)?.as_slice())?;
            }
            if sandbox.is_none() {
                let key = encode_key(&[
                    b"env",
                    &org_id.to_be_bytes(),
                    sandbox_rec.id.to_string().as_bytes(),
                ]);
                env_tbl.insert(key.as_slice(), bincode::serialize(&sandbox_rec)?.as_slice())?;
            }
        }
        w.commit(self.store)?;
        Ok((live_rec, sandbox_rec))
    }

    pub fn get_environment_by_slug(
        &self,
        org_id: u64,
        slug: &str,
    ) -> Result<Option<EnvironmentRecord>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_ENVIRONMENTS)?;
        let (start, end) = prefix_range(&[b"env", &org_id.to_be_bytes()]);
        let it = tbl.range(start.as_slice()..end.as_slice())?;
        for (_, val) in it {
            let env: EnvironmentRecord = bincode::deserialize(&val)?;
            if env.slug == slug {
                return Ok(Some(env));
            }
        }
        Ok(None)
    }

    pub fn get_environment_by_id(
        &self,
        org_id: u64,
        id: &Ulid,
    ) -> Result<Option<EnvironmentRecord>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_ENVIRONMENTS)?;
        let key = encode_key(&[b"env", &org_id.to_be_bytes(), id.to_string().as_bytes()]);
        if let Some(val) = tbl.get(key.as_slice())? {
            let env: EnvironmentRecord = bincode::deserialize(val.value())?;
            return Ok(Some(env));
        }
        Ok(None)
    }

    pub fn insert_session(&self, session_id: Ulid, payload: &[u8]) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_SESSIONS)?;
            let key = encode_key(&[b"sess", session_id.to_string().as_bytes()]);
            tbl.insert(key.as_slice(), payload)?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    pub fn get_session(&self, session_id: &Ulid) -> Result<Option<Vec<u8>>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_SESSIONS)?;
        let key = encode_key(&[b"sess", session_id.to_string().as_bytes()]);
        if let Some(val) = tbl.get(key.as_slice())? {
            return Ok(Some(val.value().to_vec()));
        }
        Ok(None)
    }

    pub fn delete_session(&self, session_id: &Ulid) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_SESSIONS)?;
            let key = encode_key(&[b"sess", session_id.to_string().as_bytes()]);
            tbl.remove(key.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    // Enhanced Session Management Methods

    pub fn insert_enhanced_session(&self, session: &SessionRecord) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_ENHANCED_SESSIONS)?;
            let key = encode_key(&[
                b"enhanced_sess",
                &session.org_id.to_be_bytes(),
                session.id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(session)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    pub fn get_enhanced_session(&self, session_id: &Ulid) -> Result<Option<SessionRecord>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_ENHANCED_SESSIONS)?;

        // We need to scan since we don't know the org_id
        let mut it = tbl.iter()?;
        while let Some(Ok((_key, val))) = it.next() {
            let session: SessionRecord = bincode::deserialize(&val)?;
            if session.id == *session_id {
                return Ok(Some(session));
            }
        }
        Ok(None)
    }

    pub fn update_session_activity(&self, session_id: &Ulid, timestamp: i64) -> Result<()> {
        if let Some(mut session) = self.get_enhanced_session(session_id)? {
            session.last_activity_at = timestamp;
            self.insert_enhanced_session(&session)?;
        }
        Ok(())
    }

    pub fn get_user_sessions(&self, user_id: Ulid, org_id: u64) -> Result<Vec<SessionRecord>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_ENHANCED_SESSIONS)?;
        let (start, end) = prefix_range(&[b"enhanced_sess", &org_id.to_be_bytes()]);

        let mut sessions = Vec::new();
        let it = tbl.range(start.as_slice()..end.as_slice())?;
        for (_, val) in it {
            let session: SessionRecord = bincode::deserialize(&val)?;
            if session.user_id == user_id {
                sessions.push(session);
            }
        }

        sessions.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(sessions)
    }

    pub fn get_device_sessions(&self, device_fingerprint: &str, org_id: u64) -> Result<Vec<SessionRecord>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_ENHANCED_SESSIONS)?;
        let (start, end) = prefix_range(&[b"enhanced_sess", &org_id.to_be_bytes()]);

        let mut sessions = Vec::new();
        let it = tbl.range(start.as_slice()..end.as_slice())?;
        for (_, val) in it {
            let session: SessionRecord = bincode::deserialize(&val)?;
            if session.device_info.fingerprint == device_fingerprint {
                sessions.push(session);
            }
        }

        sessions.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(sessions)
    }

    pub fn revoke_session(&self, session_id: &Ulid, revoked_at: i64, revoked_by: Option<Ulid>, reason: Option<String>) -> Result<()> {
        if let Some(mut session) = self.get_enhanced_session(session_id)? {
            session.status = SessionStatus::Revoked;
            session.revoked_at = Some(revoked_at);
            session.revoked_by = revoked_by;
            session.revoked_reason = reason;
            self.insert_enhanced_session(&session)?;
        }
        Ok(())
    }

    pub fn update_session_status(&self, session_id: &Ulid, status: SessionStatus) -> Result<()> {
        if let Some(mut session) = self.get_enhanced_session(session_id)? {
            session.status = status;
            self.insert_enhanced_session(&session)?;
        }
        Ok(())
    }

    pub fn get_expired_sessions(&self, org_id: u64, current_time: i64) -> Result<Vec<SessionRecord>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_ENHANCED_SESSIONS)?;
        let (start, end) = prefix_range(&[b"enhanced_sess", &org_id.to_be_bytes()]);

        let mut expired_sessions = Vec::new();
        let it = tbl.range(start.as_slice()..end.as_slice())?;
        for (_, val) in it {
            let session: SessionRecord = bincode::deserialize(&val)?;
            if session.status == SessionStatus::Active && session.expires_at < current_time {
                expired_sessions.push(session);
            }
        }

        Ok(expired_sessions)
    }

    // Team Management Methods

    pub fn insert_team(&self, team: &TeamRecord) -> Result<()> {
        let mut w = self.store.begin_write()?;
        let team_key = encode_key(&[
            b"team",
            &team.org_id.to_be_bytes(),
            team.id.to_string().as_bytes(),
        ]);

        let name_key = encode_key(&[
            b"team_name",
            &team.org_id.to_be_bytes(),
            team.name.as_bytes(),
        ]);

        // Insert team
        {
            let mut team_tbl = w.open_table(self.store, COL_TEAMS)?;
            let payload = bincode::serialize(team)?;
            team_tbl.insert(team_key.as_slice(), payload.as_slice())?;
        }

        // Insert name index
        {
            let mut name_tbl = w.open_table(self.store, COL_TEAM_NAME_INDEX)?;
            name_tbl.insert(name_key.as_slice(), team.id.to_string().as_bytes())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    pub fn update_team(&self, team: &TeamRecord) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_TEAMS)?;
            let key = encode_key(&[
                b"team",
                &team.org_id.to_be_bytes(),
                team.id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(team)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    pub fn get_team_by_id(&self, org_id: u64, team_id: &Ulid) -> Result<Option<TeamRecord>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_TEAMS)?;
        let key = encode_key(&[
            b"team",
            &org_id.to_be_bytes(),
            team_id.to_string().as_bytes(),
        ]);

        if let Some(val) = tbl.get(key.as_slice())? {
            let team: TeamRecord = bincode::deserialize(val.value())?;
            return Ok(Some(team));
        }
        Ok(None)
    }

    pub fn get_team_by_name(&self, org_id: u64, name: &str) -> Result<Option<TeamRecord>> {
        let r = self.store.begin_read()?;
        let name_tbl = r.open_table(self.store, COL_TEAM_NAME_INDEX)?;
        let name_key = encode_key(&[
            b"team_name",
            &org_id.to_be_bytes(),
            name.as_bytes(),
        ]);

        if let Some(team_id_bytes) = name_tbl.get(name_key.as_slice())? {
            let team_id_str = std::str::from_utf8(team_id_bytes.value())?;
            let team_id = Ulid::from_string(team_id_str)?;
            return self.get_team_by_id(org_id, &team_id);
        }
        Ok(None)
    }

    pub fn get_teams_by_org(&self, org_id: u64) -> Result<Vec<TeamRecord>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_TEAMS)?;
        let (start, end) = prefix_range(&[b"team", &org_id.to_be_bytes()]);

        let mut teams = Vec::new();
        let it = tbl.range(start.as_slice()..end.as_slice())?;
        for (_, val) in it {
            let team: TeamRecord = bincode::deserialize(&val)?;
            teams.push(team);
        }

        teams.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(teams)
    }

    pub fn get_child_teams(&self, org_id: u64, parent_id: &Ulid) -> Result<Vec<TeamRecord>> {
        let teams = self.get_teams_by_org(org_id)?;
        Ok(teams.into_iter()
            .filter(|team| team.parent_team_id == Some(*parent_id))
            .collect())
    }

    pub fn delete_team(&self, team_id: &Ulid) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_TEAMS)?;
            // We need to scan to find the team since we don't have org_id
            let mut key_to_remove = None;
            {
                let it = tbl.iter()?;
                for item in it {
                    let (key, val) = item?;
                    let team: TeamRecord = bincode::deserialize(&val)?;
                    if team.id == *team_id {
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

    // Team Member Methods

    pub fn insert_team_member(&self, member: &TeamMemberRecord) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_TEAM_MEMBERS)?;
            let key = encode_key(&[
                b"team_member",
                member.team_id.to_string().as_bytes(),
                member.user_id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(member)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    pub fn update_team_member(&self, member: &TeamMemberRecord) -> Result<()> {
        self.insert_team_member(member) // Same as insert for this implementation
    }

    pub fn get_team_member(&self, team_id: &Ulid, user_id: &Ulid) -> Result<Option<TeamMemberRecord>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_TEAM_MEMBERS)?;
        let key = encode_key(&[
            b"team_member",
            team_id.to_string().as_bytes(),
            user_id.to_string().as_bytes(),
        ]);

        if let Some(val) = tbl.get(key.as_slice())? {
            let member: TeamMemberRecord = bincode::deserialize(val.value())?;
            return Ok(Some(member));
        }
        Ok(None)
    }

    pub fn get_team_members(&self, team_id: &Ulid) -> Result<Vec<TeamMemberRecord>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_TEAM_MEMBERS)?;
        let (start, end) = prefix_range(&[b"team_member", team_id.to_string().as_bytes()]);

        let mut members = Vec::new();
        let it = tbl.range(start.as_slice()..end.as_slice())?;
        for (_, val) in it {
            let member: TeamMemberRecord = bincode::deserialize(&val)?;
            members.push(member);
        }

        Ok(members)
    }

    pub fn get_team_members_by_role(&self, team_id: &Ulid, role: &TeamRole) -> Result<Vec<TeamMemberRecord>> {
        let members = self.get_team_members(team_id)?;
        Ok(members.into_iter()
            .filter(|member| member.role == *role)
            .collect())
    }

    pub fn get_user_teams(&self, org_id: u64, user_id: &Ulid) -> Result<Vec<(TeamRecord, TeamMemberRecord)>> {
        let r = self.store.begin_read()?;
        let member_tbl = r.open_table(self.store, COL_TEAM_MEMBERS)?;

        let mut user_teams = Vec::new();
        let mut it = member_tbl.iter()?;
        while let Some(Ok((_, val))) = it.next() {
            let member: TeamMemberRecord = bincode::deserialize(&val)?;
            if member.user_id == *user_id {
                if let Some(team) = self.get_team_by_id(org_id, &member.team_id)? {
                    user_teams.push((team, member));
                }
            }
        }

        Ok(user_teams)
    }

    pub fn remove_team_member(&self, team_id: &Ulid, user_id: &Ulid) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_TEAM_MEMBERS)?;
            let key = encode_key(&[
                b"team_member",
                team_id.to_string().as_bytes(),
                user_id.to_string().as_bytes(),
            ]);
            tbl.remove(key.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    pub fn delete_team_members(&self, team_id: &Ulid) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_TEAM_MEMBERS)?;
            let (start, end) = prefix_range(&[b"team_member", team_id.to_string().as_bytes()]);

            let keys_to_remove: Vec<_> = {
                let mut keys = Vec::new();
                let it = tbl.range(start.as_slice()..end.as_slice())?;
                for (key, _) in it {
                    keys.push(key.to_vec());
                }
                keys
            };

            for key in keys_to_remove {
                tbl.remove(key.as_slice())?;
            }
        }
        w.commit(self.store)?;
        Ok(())
    }

    // Team Invitation Methods

    pub fn insert_team_invitation(&self, invitation: &TeamInvitation) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_TEAM_INVITATIONS)?;
            let key = encode_key(&[
                b"team_invitation",
                invitation.id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(invitation)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    pub fn update_team_invitation(&self, invitation: &TeamInvitation) -> Result<()> {
        self.insert_team_invitation(invitation) // Same as insert for this implementation
    }

    pub fn get_team_invitation(&self, invitation_id: &Ulid) -> Result<Option<TeamInvitation>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_TEAM_INVITATIONS)?;
        let key = encode_key(&[
            b"team_invitation",
            invitation_id.to_string().as_bytes(),
        ]);

        if let Some(val) = tbl.get(key.as_slice())? {
            let invitation: TeamInvitation = bincode::deserialize(val.value())?;
            return Ok(Some(invitation));
        }
        Ok(None)
    }

    pub fn delete_team_invitations(&self, team_id: &Ulid) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_TEAM_INVITATIONS)?;

            let keys_to_remove: Vec<_> = {
                let mut keys = Vec::new();
                let mut it = tbl.iter()?;
                while let Some(Ok((key, val))) = it.next() {
                    let invitation: TeamInvitation = bincode::deserialize(&val)?;
                    if invitation.team_id == *team_id {
                        keys.push(key.to_vec());
                    }
                }
                keys
            };

            for key in keys_to_remove {
                tbl.remove(key.as_slice())?;
            }
        }
        w.commit(self.store)?;
        Ok(())
    }

    // Role Delegation Methods

    pub fn insert_role_delegation(&self, delegation: &RoleDelegation) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_ROLE_DELEGATIONS)?;
            let key = encode_key(&[
                b"role_delegation",
                &delegation.org_id.to_be_bytes(),
                delegation.id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(delegation)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    pub fn update_role_delegation(&self, delegation: &RoleDelegation) -> Result<()> {
        self.insert_role_delegation(delegation) // Same as insert for this implementation
    }

    pub fn get_role_delegation(&self, delegation_id: &Ulid) -> Result<Option<RoleDelegation>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_ROLE_DELEGATIONS)?;

        // Scan to find delegation since we don't know the org_id
        let mut it = tbl.iter()?;
        while let Some(Ok((_, val))) = it.next() {
            let delegation: RoleDelegation = bincode::deserialize(&val)?;
            if delegation.id == *delegation_id {
                return Ok(Some(delegation));
            }
        }
        Ok(None)
    }

    pub fn get_user_delegations(&self, user_id: &Ulid, org_id: u64) -> Result<Vec<RoleDelegation>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_ROLE_DELEGATIONS)?;
        let (start, end) = prefix_range(&[b"role_delegation", &org_id.to_be_bytes()]);

        let mut delegations = Vec::new();
        let it = tbl.range(start.as_slice()..end.as_slice())?;
        for (_, val) in it {
            let delegation: RoleDelegation = bincode::deserialize(&val)?;
            if delegation.delegatee_id == *user_id {
                delegations.push(delegation);
            }
        }

        delegations.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(delegations)
    }

    pub fn get_org_delegations(&self, org_id: u64) -> Result<Vec<RoleDelegation>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_ROLE_DELEGATIONS)?;
        let (start, end) = prefix_range(&[b"role_delegation", &org_id.to_be_bytes()]);

        let mut delegations = Vec::new();
        let it = tbl.range(start.as_slice()..end.as_slice())?;
        for (_, val) in it {
            let delegation: RoleDelegation = bincode::deserialize(&val)?;
            delegations.push(delegation);
        }

        delegations.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(delegations)
    }

    // Audit Log Methods

    pub fn insert_audit_log(&self, entry: &AuditLogEntry) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_AUDIT_LOGS)?;
            let key = encode_key(&[
                b"audit",
                &entry.org_id.to_be_bytes(),
                &entry.timestamp.to_be_bytes(),
                entry.id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(entry)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    pub fn get_audit_logs(&self, org_id: u64, limit: Option<usize>, offset: Option<usize>) -> Result<Vec<AuditLogEntry>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_AUDIT_LOGS)?;
        let (start, end) = prefix_range(&[b"audit", &org_id.to_be_bytes()]);

        let mut logs = Vec::new();
        let it = tbl.range(start.as_slice()..end.as_slice())?;
        let mut count = 0;
        let skip = offset.unwrap_or(0);
        let take = limit.unwrap_or(1000);

        for (_, val) in it {
            if count < skip {
                count += 1;
                continue;
            }
            if logs.len() >= take {
                break;
            }

            let log_entry: AuditLogEntry = bincode::deserialize(&val)?;
            logs.push(log_entry);
            count += 1;
        }

        // Sort by timestamp descending (newest first)
        logs.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(logs)
    }

    // Two-Factor Authentication Methods

    // TOTP Secret Methods
    pub fn insert_totp_secret(&self, secret: &TotpSecret) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_TOTP_SECRETS)?;
            let key = encode_key(&[
                b"totp_secret",
                &secret.org_id.to_be_bytes(),
                secret.user_id.to_string().as_bytes(),
                secret.id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(secret)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    pub fn update_totp_secret(&self, secret: &TotpSecret) -> Result<()> {
        self.insert_totp_secret(secret) // Same as insert for this implementation
    }

    pub fn get_totp_secret(&self, secret_id: &Ulid) -> Result<Option<TotpSecret>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_TOTP_SECRETS)?;

        // Scan to find secret since we don't know the org_id/user_id
        let mut it = tbl.iter()?;
        while let Some(Ok((_, val))) = it.next() {
            let secret: TotpSecret = bincode::deserialize(&val)?;
            if secret.id == *secret_id {
                return Ok(Some(secret));
            }
        }
        Ok(None)
    }

    pub fn list_totp_secrets(&self, user_id: &Ulid, org_id: u64) -> Result<Vec<TotpSecret>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_TOTP_SECRETS)?;
        let (start, end) = prefix_range(&[
            b"totp_secret",
            &org_id.to_be_bytes(),
            user_id.to_string().as_bytes(),
        ]);

        let mut secrets = Vec::new();
        let it = tbl.range(start.as_slice()..end.as_slice())?;
        for (_, val) in it {
            let secret: TotpSecret = bincode::deserialize(&val)?;
            secrets.push(secret);
        }

        secrets.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(secrets)
    }

    pub fn clear_totp_secrets(&self, user_id: &Ulid, org_id: u64) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_TOTP_SECRETS)?;
            let (start, end) = prefix_range(&[
                b"totp_secret",
                &org_id.to_be_bytes(),
                user_id.to_string().as_bytes(),
            ]);

            let keys_to_remove: Vec<_> = {
                let mut keys = Vec::new();
                let it = tbl.range(start.as_slice()..end.as_slice())?;
                for (key, _) in it {
                    keys.push(key.to_vec());
                }
                keys
            };

            for key in keys_to_remove {
                tbl.remove(key.as_slice())?;
            }
        }
        w.commit(self.store)?;
        Ok(())
    }

    // Backup Code Methods
    pub fn insert_backup_code(&self, code: &BackupCode) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_BACKUP_CODES)?;
            let key = encode_key(&[
                b"backup_code",
                &code.org_id.to_be_bytes(),
                code.user_id.to_string().as_bytes(),
                code.id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(code)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    pub fn update_backup_code(&self, code: &BackupCode) -> Result<()> {
        self.insert_backup_code(code) // Same as insert for this implementation
    }

    pub fn list_backup_codes(&self, user_id: &Ulid, org_id: u64) -> Result<Vec<BackupCode>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_BACKUP_CODES)?;
        let (start, end) = prefix_range(&[
            b"backup_code",
            &org_id.to_be_bytes(),
            user_id.to_string().as_bytes(),
        ]);

        let mut codes = Vec::new();
        let it = tbl.range(start.as_slice()..end.as_slice())?;
        for (_, val) in it {
            let code: BackupCode = bincode::deserialize(&val)?;
            codes.push(code);
        }

        codes.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        Ok(codes)
    }

    pub fn count_unused_backup_codes(&self, user_id: &Ulid, org_id: u64) -> Result<u32> {
        let codes = self.list_backup_codes(user_id, org_id)?;
        Ok(codes.iter().filter(|code| !code.used).count() as u32)
    }

    pub fn clear_backup_codes(&self, user_id: &Ulid, org_id: u64) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_BACKUP_CODES)?;
            let (start, end) = prefix_range(&[
                b"backup_code",
                &org_id.to_be_bytes(),
                user_id.to_string().as_bytes(),
            ]);

            let keys_to_remove: Vec<_> = {
                let mut keys = Vec::new();
                let it = tbl.range(start.as_slice()..end.as_slice())?;
                for (key, _) in it {
                    keys.push(key.to_vec());
                }
                keys
            };

            for key in keys_to_remove {
                tbl.remove(key.as_slice())?;
            }
        }
        w.commit(self.store)?;
        Ok(())
    }

    // 2FA Challenge Methods
    pub fn insert_2fa_challenge(&self, challenge: &TwoFactorChallenge) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_2FA_CHALLENGES)?;
            let key = encode_key(&[
                b"2fa_challenge",
                challenge.id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(challenge)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    pub fn update_2fa_challenge(&self, challenge: &TwoFactorChallenge) -> Result<()> {
        self.insert_2fa_challenge(challenge) // Same as insert for this implementation
    }

    pub fn get_2fa_challenge(&self, challenge_id: &Ulid) -> Result<Option<TwoFactorChallenge>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_2FA_CHALLENGES)?;
        let key = encode_key(&[
            b"2fa_challenge",
            challenge_id.to_string().as_bytes(),
        ]);

        if let Some(val) = tbl.get(key.as_slice())? {
            let challenge: TwoFactorChallenge = bincode::deserialize(val.value())?;
            return Ok(Some(challenge));
        }
        Ok(None)
    }

    // 2FA Settings Methods
    pub fn get_2fa_settings(&self, user_id: &Ulid, org_id: u64) -> Result<Option<TwoFactorSettings>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_2FA_SETTINGS)?;
        let key = encode_key(&[
            b"2fa_settings",
            &org_id.to_be_bytes(),
            user_id.to_string().as_bytes(),
        ]);

        if let Some(val) = tbl.get(key.as_slice())? {
            let settings: TwoFactorSettings = bincode::deserialize(val.value())?;
            return Ok(Some(settings));
        }
        Ok(None)
    }

    pub fn update_2fa_settings(&self, settings: &TwoFactorSettings) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_2FA_SETTINGS)?;
            let key = encode_key(&[
                b"2fa_settings",
                &settings.org_id.to_be_bytes(),
                settings.user_id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(settings)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    // Organization 2FA Policy Methods
    pub fn get_org_2fa_policy(&self, org_id: u64) -> Result<Option<OrgTwoFactorPolicy>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_ORG_2FA_POLICIES)?;
        let key = encode_key(&[
            b"org_2fa_policy",
            &org_id.to_be_bytes(),
        ]);

        if let Some(val) = tbl.get(key.as_slice())? {
            let policy: OrgTwoFactorPolicy = bincode::deserialize(val.value())?;
            return Ok(Some(policy));
        }
        Ok(None)
    }

    pub fn update_org_2fa_policy(&self, policy: &OrgTwoFactorPolicy) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_ORG_2FA_POLICIES)?;
            let key = encode_key(&[
                b"org_2fa_policy",
                &policy.org_id.to_be_bytes(),
            ]);
            let payload = bincode::serialize(policy)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    // SMS Verification Methods
    pub fn insert_sms_verification(&self, verification: &SmsVerification) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_SMS_VERIFICATIONS)?;
            let key = encode_key(&[
                b"sms_verification",
                verification.id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(verification)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    pub fn update_sms_verification(&self, verification: &SmsVerification) -> Result<()> {
        self.insert_sms_verification(verification) // Same as insert for this implementation
    }

    pub fn get_sms_verification(&self, verification_id: &Ulid) -> Result<Option<SmsVerification>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_SMS_VERIFICATIONS)?;
        let key = encode_key(&[
            b"sms_verification",
            verification_id.to_string().as_bytes(),
        ]);

        if let Some(val) = tbl.get(key.as_slice())? {
            let verification: SmsVerification = bincode::deserialize(val.value())?;
            return Ok(Some(verification));
        }
        Ok(None)
    }

    // WebAuthn Credential Methods (placeholder)
    pub fn clear_webauthn_credentials(&self, user_id: &Ulid, org_id: u64) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_WEBAUTHN_CREDENTIALS)?;
            let (start, end) = prefix_range(&[
                b"webauthn_credential",
                &org_id.to_be_bytes(),
                user_id.to_string().as_bytes(),
            ]);

            let keys_to_remove: Vec<_> = {
                let mut keys = Vec::new();
                let it = tbl.range(start.as_slice()..end.as_slice())?;
                for (key, _) in it {
                    keys.push(key.to_vec());
                }
                keys
            };

            for key in keys_to_remove {
                tbl.remove(key.as_slice())?;
            }
        }
        w.commit(self.store)?;
        Ok(())
    }

    // Advanced Session Management Methods

    /// Insert a session record for advanced session tracking
    pub fn insert_session_record(&self, session: &SessionRecord) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_SESSION_RECORDS)?;
            let key = encode_key(&[
                b"session_record",
                &session.org_id.to_be_bytes(),
                session.id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(session)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    /// Get a session record by ID
    pub fn get_session_record(&self, session_id: Ulid) -> Result<Option<SessionRecord>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_SESSION_RECORDS)?;

        // Scan to find session since we don't know the org_id
        let mut it = tbl.iter()?;
        while let Some(Ok((_, val))) = it.next() {
            let session: SessionRecord = bincode::deserialize(&val)?;
            if session.id == session_id {
                return Ok(Some(session));
            }
        }
        Ok(None)
    }

    /// Get active sessions for a user
    pub fn get_active_sessions_for_user(&self, org_id: u64, user_id: Ulid) -> Result<Vec<SessionRecord>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_SESSION_RECORDS)?;
        let (start, end) = prefix_range(&[b"session_record", &org_id.to_be_bytes()]);

        let mut sessions = Vec::new();
        let it = tbl.range(start.as_slice()..end.as_slice())?;
        for (_, val) in it {
            let session: SessionRecord = bincode::deserialize(&val)?;
            if session.user_id == user_id && session.status == SessionStatus::Active {
                sessions.push(session);
            }
        }

        Ok(sessions)
    }

    /// Get active sessions for a device
    pub fn get_active_sessions_for_device(&self, org_id: u64, device_fingerprint: &str) -> Result<Vec<SessionRecord>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_SESSION_RECORDS)?;
        let (start, end) = prefix_range(&[b"session_record", &org_id.to_be_bytes()]);

        let mut sessions = Vec::new();
        let it = tbl.range(start.as_slice()..end.as_slice())?;
        for (_, val) in it {
            let session: SessionRecord = bincode::deserialize(&val)?;
            if session.device_info.fingerprint == device_fingerprint && session.status == SessionStatus::Active {
                sessions.push(session);
            }
        }

        Ok(sessions)
    }

    /// Get sessions within a timeframe for security analysis
    pub fn get_sessions_in_timeframe(&self, org_id: u64, start_time: i64, end_time: i64) -> Result<Vec<SessionRecord>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_SESSION_RECORDS)?;
        let (prefix_start, prefix_end) = prefix_range(&[b"session_record", &org_id.to_be_bytes()]);

        let mut sessions = Vec::new();
        let it = tbl.range(prefix_start.as_slice()..prefix_end.as_slice())?;
        for (_, val) in it {
            let session: SessionRecord = bincode::deserialize(&val)?;
            if session.created_at >= start_time && session.created_at <= end_time {
                sessions.push(session);
            }
        }

        Ok(sessions)
    }

    /// Revoke a session (advanced version)
    pub fn revoke_session_advanced(&self, session_id: Ulid, revoked_by: Option<Ulid>, reason: &str) -> Result<()> {
        if let Some(mut session) = self.get_session_record(session_id)? {
            session.status = SessionStatus::Revoked;
            session.revoked_at = Some(current_timestamp());
            session.revoked_by = revoked_by;
            session.revoked_reason = Some(reason.to_string());
            self.insert_session_record(&session)?;
        }
        Ok(())
    }

    // Trusted Device Methods

    /// Insert a trusted device record
    pub fn insert_trusted_device(&self, device: &TrustedDevice) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_TRUSTED_DEVICES)?;
            let key = encode_key(&[
                b"trusted_device",
                &device.org_id.to_be_bytes(),
                device.user_id.to_string().as_bytes(),
                device.id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(device)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    /// Get a trusted device by user and fingerprint
    pub fn get_trusted_device(&self, user_id: Ulid, device_fingerprint: &str) -> Result<Option<TrustedDevice>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_TRUSTED_DEVICES)?;

        // Scan to find device since we need to match by fingerprint
        let mut it = tbl.iter()?;
        while let Some(Ok((_, val))) = it.next() {
            let device: TrustedDevice = bincode::deserialize(&val)?;
            if device.user_id == user_id && device.device_fingerprint == device_fingerprint {
                return Ok(Some(device));
            }
        }
        Ok(None)
    }

    /// Get a trusted device by ID
    pub fn get_trusted_device_by_id(&self, device_id: Ulid) -> Result<Option<TrustedDevice>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_TRUSTED_DEVICES)?;

        // Scan to find device since we don't know the org_id/user_id
        let mut it = tbl.iter()?;
        while let Some(Ok((_, val))) = it.next() {
            let device: TrustedDevice = bincode::deserialize(&val)?;
            if device.id == device_id {
                return Ok(Some(device));
            }
        }
        Ok(None)
    }

    /// Update a trusted device
    pub fn update_trusted_device(&self, device: &TrustedDevice) -> Result<()> {
        self.insert_trusted_device(device) // Same as insert for this implementation
    }

    // Device Verification Methods

    /// Insert a device verification record
    pub fn insert_device_verification(&self, verification: &DeviceVerification) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_DEVICE_VERIFICATIONS)?;
            let key = encode_key(&[
                b"device_verification",
                verification.id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(verification)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    /// Get device verification by token
    pub fn get_device_verification_by_token(&self, token: &str) -> Result<Option<DeviceVerification>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_DEVICE_VERIFICATIONS)?;

        // Scan to find verification by token
        let mut it = tbl.iter()?;
        while let Some(Ok((_, val))) = it.next() {
            let verification: DeviceVerification = bincode::deserialize(&val)?;
            if verification.token == token {
                return Ok(Some(verification));
            }
        }
        Ok(None)
    }

    /// Update device verification
    pub fn update_device_verification(&self, verification: &DeviceVerification) -> Result<()> {
        self.insert_device_verification(verification) // Same as insert for this implementation
    }

    // Device Event Methods

    /// Insert a device event record
    pub fn insert_device_event(&self, event: &DeviceEventRecord) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, COL_DEVICE_EVENTS)?;
            let key = encode_key(&[
                b"device_event",
                event.device_id.to_string().as_bytes(),
                &event.timestamp.to_be_bytes(),
                event.id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(event)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    /// Get recent device events
    pub fn get_device_events(&self, device_id: Ulid, limit: usize) -> Result<Vec<DeviceEventRecord>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, COL_DEVICE_EVENTS)?;
        let (start, end) = prefix_range(&[b"device_event", device_id.to_string().as_bytes()]);

        let mut events = Vec::new();
        let it = tbl.range(start.as_slice()..end.as_slice())?;
        for (_, val) in it {
            if events.len() >= limit {
                break;
            }
            let event: DeviceEventRecord = bincode::deserialize(&val)?;
            events.push(event);
        }

        // Sort by timestamp descending (newest first)
        events.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(events)
    }

    // Audit Event Methods (renamed to avoid conflict)

    /// Insert an audit event
    pub fn insert_audit_event(&self, entry: &AuditLogEntry) -> Result<()> {
        self.insert_audit_log(entry) // Delegate to existing method
    }

    // Credential Management Methods

    /// Insert a new identity credential
    pub fn insert_identity_credential(&self, credential: &IdentityCredential) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, TBL_IDENTITY_CREDENTIALS)?;
            let key = encode_key(&[
                b"identity_credential",
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

    /// Get an identity credential by ID
    pub fn get_identity_credential(&self, credential_id: Ulid) -> Result<Option<IdentityCredential>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, TBL_IDENTITY_CREDENTIALS)?;

        // Since we don't know org_id and user_id, we need to scan
        let mut it = tbl.iter()?;
        while let Some(Ok((_, val))) = it.next() {
            let credential: IdentityCredential = bincode::deserialize(&val)?;
            if credential.id == credential_id {
                return Ok(Some(credential));
            }
        }
        Ok(None)
    }

    /// Update an identity credential
    pub fn update_identity_credential(&self, credential: &IdentityCredential) -> Result<()> {
        self.insert_identity_credential(credential) // Same as insert
    }

    /// Get all credentials for a user
    pub fn get_user_credentials(&self, user_id: Ulid, org_id: u64) -> Result<Vec<IdentityCredential>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, TBL_IDENTITY_CREDENTIALS)?;
        let (start, end) = prefix_range(&[
            b"identity_credential",
            &org_id.to_be_bytes(),
            user_id.to_string().as_bytes(),
        ]);

        let mut credentials = Vec::new();
        let it = tbl.range(start.as_slice()..end.as_slice())?;
        for (_, val) in it {
            let credential: IdentityCredential = bincode::deserialize(&val)?;
            // Only include non-revoked credentials
            if credential.revoked_at.is_none() {
                credentials.push(credential);
            }
        }

        credentials.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(credentials)
    }

    /// Insert a credential verification challenge
    pub fn insert_credential_challenge(&self, challenge: &CredentialVerificationChallenge) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, TBL_CREDENTIAL_CHALLENGES)?;
            let key = encode_key(&[
                b"credential_challenge",
                challenge.id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(challenge)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    /// Get a credential verification challenge
    pub fn get_credential_challenge(&self, challenge_id: Ulid) -> Result<Option<CredentialVerificationChallenge>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, TBL_CREDENTIAL_CHALLENGES)?;
        let key = encode_key(&[
            b"credential_challenge",
            challenge_id.to_string().as_bytes(),
        ]);

        if let Some(val) = tbl.get(key.as_slice())? {
            let challenge: CredentialVerificationChallenge = bincode::deserialize(val.value())?;
            Ok(Some(challenge))
        } else {
            Ok(None)
        }
    }

    /// Update a credential verification challenge
    pub fn update_credential_challenge(&self, challenge: &CredentialVerificationChallenge) -> Result<()> {
        self.insert_credential_challenge(challenge) // Same as insert
    }

    /// Update session credential context
    pub fn update_session_credential_context(&self, session_id: Ulid, context: &SessionCredentialContext) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, TBL_SESSION_CREDENTIAL_CONTEXTS)?;
            let key = encode_key(&[
                b"session_credential_context",
                session_id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(context)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    /// Get session credential context
    pub fn get_session_credential_context(&self, session_id: Ulid) -> Result<Option<SessionCredentialContext>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, TBL_SESSION_CREDENTIAL_CONTEXTS)?;
        let key = encode_key(&[
            b"session_credential_context",
            session_id.to_string().as_bytes(),
        ]);

        if let Some(val) = tbl.get(key.as_slice())? {
            let context: SessionCredentialContext = bincode::deserialize(val.value())?;
            Ok(Some(context))
        } else {
            Ok(None)
        }
    }

    /// Revoke sessions that use a specific credential
    pub fn revoke_sessions_with_credential(&self, credential_id: Ulid) -> Result<()> {
        // This would scan all session credential contexts and revoke sessions that use the credential
        // For now, this is a placeholder implementation
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, TBL_SESSION_CREDENTIAL_CONTEXTS)?;
        let mut sessions_to_revoke = Vec::new();

        let mut it = tbl.iter()?;
        while let Some(Ok((_, val))) = it.next() {
            let context: SessionCredentialContext = bincode::deserialize(&val)?;
            if context.primary_credential == Some(credential_id) ||
               context.secondary_credentials.contains(&credential_id) {
                sessions_to_revoke.push(context.session_id);
            }
        }
        drop(r);

        // Revoke the identified sessions
        for session_id in sessions_to_revoke {
            // This would call the session revocation method
            // For now, just remove the credential context
            let mut w = self.store.begin_write()?;
            {
                let mut tbl = w.open_table(self.store, TBL_SESSION_CREDENTIAL_CONTEXTS)?;
                let key = encode_key(&[
                    b"session_credential_context",
                    session_id.to_string().as_bytes(),
                ]);
                tbl.remove(key.as_slice())?;
            }
            w.commit(self.store)?;
        }

        Ok(())
    }

    /// Insert an identity assertion record
    pub fn insert_identity_assertion(&self, assertion: &IdentityAssertionRecord) -> Result<()> {
        let mut w = self.store.begin_write()?;
        {
            let mut tbl = w.open_table(self.store, TBL_IDENTITY_ASSERTIONS)?;
            let key = encode_key(&[
                b"identity_assertion",
                &assertion.org_id.to_be_bytes(),
                assertion.user_id.to_string().as_bytes(),
                &assertion.created_at.to_be_bytes(),
                assertion.id.to_string().as_bytes(),
            ]);
            let payload = bincode::serialize(assertion)?;
            tbl.insert(key.as_slice(), payload.as_slice())?;
        }
        w.commit(self.store)?;
        Ok(())
    }

    /// Get an identity assertion by ID
    pub fn get_identity_assertion(&self, assertion_id: Ulid) -> Result<Option<IdentityAssertionRecord>> {
        let r = self.store.begin_read()?;
        let tbl = r.open_table(self.store, TBL_IDENTITY_ASSERTIONS)?;

        // Scan to find assertion since we don't know org_id and user_id
        let mut it = tbl.iter()?;
        while let Some(Ok((_, val))) = it.next() {
            let assertion: IdentityAssertionRecord = bincode::deserialize(&val)?;
            if assertion.id == assertion_id {
                return Ok(Some(assertion));
            }
        }
        Ok(None)
    }

    /// Update an identity assertion
    pub fn update_identity_assertion(&self, assertion: &IdentityAssertionRecord) -> Result<()> {
        self.insert_identity_assertion(assertion) // Same as insert
    }
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
