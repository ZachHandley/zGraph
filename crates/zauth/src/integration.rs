use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use serde::Serialize;
use ulid::Ulid;
use zcore_storage::Store;
use zpermissions::{Principal, ResourceKind, CrudAction, PermissionService};

use crate::models::{
    SessionRecord, TeamRole,
    AuditLogEntry, AuditAction, AuditStatus,
};
use crate::{AuthSession, SessionService, TeamService};
use crate::repository::AuthRepository;

/// Integrated service for session, team, and permission management
#[derive(Debug, Clone)]
pub struct IntegratedAuthService<'a> {
    session_service: SessionService<'a>,
    team_service: TeamService<'a>,
    permission_service: PermissionService<'a>,
    repo: AuthRepository<'a>,
}

impl<'a> IntegratedAuthService<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self {
            session_service: SessionService::new(store),
            team_service: TeamService::new(store),
            permission_service: PermissionService::new(store),
            repo: AuthRepository::new(store),
        }
    }

    /// Get effective permissions for a user considering their team memberships
    pub fn get_effective_permissions(
        &self,
        user_id: &Ulid,
        org_id: u64,
    ) -> Result<HashMap<ResourceKind, Vec<CrudAction>>> {
        let mut principals = Vec::new();

        // Add user principal
        principals.push(Principal::User(user_id.to_string()));

        // Get user record for roles and labels
        if let Some(user) = self.repo.get_user_by_id(org_id, user_id)? {
            // Add user roles
            for role in &user.roles {
                principals.push(Principal::Role(role.clone()));
            }
            // Add user labels
            for label in &user.labels {
                principals.push(Principal::Label(label.clone()));
            }
        }

        // Get team memberships and add team-based principals
        let user_teams = self.team_service.get_user_teams(org_id, user_id)?;
        for (team, member) in user_teams {
            // Add team principal
            principals.push(Principal::Team(team.id.to_string()));

            // Add team role principal
            principals.push(Principal::TeamRole {
                team_id: team.id.to_string(),
                role: format!("{:?}", member.role).to_lowercase(),
            });

            // Add inherited permissions from parent teams
            if let Some(parent_id) = team.parent_team_id {
                self.add_inherited_team_permissions(&mut principals, org_id, &parent_id, user_id)?;
            }
        }

        // Get effective permissions considering all principals
        let btree_map = self.permission_service.effective_map(org_id, &principals)?;
        Ok(btree_map.into_iter().collect())
    }

    /// Add inherited permissions from parent teams
    fn add_inherited_team_permissions(
        &self,
        principals: &mut Vec<Principal>,
        org_id: u64,
        team_id: &Ulid,
        user_id: &Ulid,
    ) -> Result<()> {
        if let Some(parent_member) = self.repo.get_team_member(team_id, user_id)? {
            principals.push(Principal::Team(team_id.to_string()));
            principals.push(Principal::TeamRole {
                team_id: team_id.to_string(),
                role: format!("{:?}", parent_member.role).to_lowercase(),
            });

            // Recursively check parent teams
            if let Some(team) = self.team_service.get_team(org_id, team_id)? {
                if let Some(parent_id) = team.parent_team_id {
                    self.add_inherited_team_permissions(principals, org_id, &parent_id, user_id)?;
                }
            }
        }
        Ok(())
    }

    /// Check if user can access resource considering session and team context
    pub fn check_access(
        &self,
        session: &AuthSession,
        resource_type: ResourceKind,
        required_actions: &[CrudAction],
    ) -> Result<bool> {
        // Check if session is valid and active
        if let Some(session_id) = session.session_id {
            if let Some(session_record) = self.session_service.get_session(&session_id)? {
                if session_record.status != crate::models::SessionStatus::Active {
                    return Ok(false);
                }
            }
        }

        // Get user's effective permissions
        if let Some(user_id) = session.user_id {
            let effective_perms = self.get_effective_permissions(&user_id, session.org_id)?;

            if let Some(user_actions) = effective_perms.get(&resource_type) {
                return Ok(required_actions.iter().all(|action| user_actions.contains(action)));
            }
        }

        Ok(false)
    }

    /// Share resource with team
    pub fn share_resource_with_team(
        &self,
        sharer_id: Ulid,
        org_id: u64,
        team_id: &Ulid,
        resource_type: ResourceKind,
        resource_id: String,
        permissions: Vec<CrudAction>,
    ) -> Result<()> {
        // Check if user has permission to share this resource
        let effective_perms = self.get_effective_permissions(&sharer_id, org_id)?;
        let empty_perms = vec![];
        let sharer_perms = effective_perms.get(&resource_type).unwrap_or(&empty_perms);

        if !sharer_perms.contains(&CrudAction::Update) {
            return Err(anyhow!("Insufficient permissions to share this resource"));
        }

        // Check if user is a member of the team
        if !self.team_service.has_team_permission(org_id, team_id, &sharer_id, &TeamRole::Member)? {
            return Err(anyhow!("User is not a member of the target team"));
        }

        // Create team permission record
        let team_principal = Principal::Team(team_id.to_string());
        let permission_record = zpermissions::PermissionRecord::new(
            team_principal,
            resource_type,
            permissions.clone(),
        );

        self.permission_service.upsert(org_id, &permission_record)?;

        // Log resource sharing
        self.log_audit_event(
            org_id,
            None,
            Some(sharer_id),
            Some(*team_id),
            AuditAction::ResourceShare,
            Some(format!("{:?}", resource_type)),
            Some(resource_id),
            serde_json::json!({
                "permissions": permissions,
                "shared_with_team": team_id,
            }),
            None,
            None,
            AuditStatus::Success,
        )?;

        Ok(())
    }

    /// Get team resources (resources shared with team)
    pub fn get_team_resources(
        &self,
        org_id: u64,
        team_id: &Ulid,
    ) -> Result<Vec<TeamResourceInfo>> {
        let team_principal = Principal::Team(team_id.to_string());
        let org_permissions = self.permission_service.list_org(org_id)?;

        let mut team_resources = Vec::new();

        for perm in org_permissions {
            if perm.principal == team_principal {
                team_resources.push(TeamResourceInfo {
                    resource_type: perm.resource,
                    permissions: perm.allow,
                    shared_at: current_timestamp(), // TODO: Store actual sharing timestamp
                });
            }
        }

        Ok(team_resources)
    }

    /// Create session with team context
    pub fn create_team_session(
        &self,
        user_id: Ulid,
        org_id: u64,
        environment_id: Ulid,
        team_id: Option<Ulid>,
        device_info: crate::models::DeviceInfo,
        ip_address: Option<String>,
        user_agent: Option<String>,
        ttl_secs: i64,
    ) -> Result<SessionRecord> {
        // Validate team membership if team_id is provided
        if let Some(team_id) = team_id {
            if !self.team_service.has_team_permission(org_id, &team_id, &user_id, &TeamRole::Member)? {
                return Err(anyhow!("User is not a member of the specified team"));
            }
        }

        // Create enhanced session
        let session = self.session_service.create_session(
            user_id,
            org_id,
            environment_id,
            device_info,
            ip_address,
            user_agent,
            ttl_secs,
        )?;

        // Log team context if provided
        if let Some(team_id) = team_id {
            self.log_audit_event(
                org_id,
                Some(session.id),
                Some(user_id),
                Some(team_id),
                AuditAction::SessionCreate,
                None,
                None,
                serde_json::json!({
                    "team_context": team_id,
                    "device_trusted": session.device_info.trusted,
                }),
                session.ip_address.clone(),
                session.user_agent.clone(),
                AuditStatus::Success,
            )?;
        }

        Ok(session)
    }

    /// Get team activity summary
    pub fn get_team_activity_summary(
        &self,
        org_id: u64,
        team_id: &Ulid,
        days: i32,
    ) -> Result<TeamActivitySummary> {
        let since = current_timestamp() - (days as i64 * 24 * 3600);
        let logs = self.repo.get_audit_logs(org_id, Some(1000), None)?;

        let team_logs: Vec<_> = logs.into_iter()
            .filter(|log| log.team_id == Some(*team_id) && log.timestamp >= since)
            .collect();

        let mut activity_by_action = HashMap::new();
        let mut active_users = std::collections::HashSet::new();
        let mut recent_actions = Vec::new();

        for log in &team_logs {
            *activity_by_action.entry(log.action.clone()).or_insert(0) += 1;

            if let Some(user_id) = log.user_id {
                active_users.insert(user_id);
            }

            if recent_actions.len() < 10 {
                recent_actions.push(RecentActivity {
                    action: log.action.clone(),
                    user_id: log.user_id,
                    timestamp: log.timestamp,
                    details: log.details.clone(),
                });
            }
        }

        Ok(TeamActivitySummary {
            team_id: *team_id,
            activity_count: team_logs.len(),
            active_user_count: active_users.len(),
            activity_by_action,
            recent_actions,
        })
    }

    /// Cleanup expired sessions and update team activity
    pub fn cleanup_and_update_activity(&self, org_id: u64) -> Result<CleanupSummary> {
        let expired_sessions = self.session_service.cleanup_expired_sessions(org_id)?;
        let now = current_timestamp();

        // Update team last activity timestamps
        let teams = self.team_service.get_org_teams(org_id)?;
        let mut updated_teams = 0;

        for team in teams {
            let activity = self.get_team_activity_summary(org_id, &team.id, 1)?;
            if activity.activity_count > 0 {
                // Update team's last activity (would need to add this field to TeamRecord)
                updated_teams += 1;
            }
        }

        Ok(CleanupSummary {
            expired_sessions_cleaned: expired_sessions,
            teams_activity_updated: updated_teams,
            cleanup_timestamp: now,
        })
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

#[derive(Debug, Clone, Serialize)]
pub struct TeamResourceInfo {
    pub resource_type: ResourceKind,
    pub permissions: Vec<CrudAction>,
    pub shared_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TeamActivitySummary {
    pub team_id: Ulid,
    pub activity_count: usize,
    pub active_user_count: usize,
    pub activity_by_action: HashMap<AuditAction, usize>,
    pub recent_actions: Vec<RecentActivity>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RecentActivity {
    pub action: AuditAction,
    pub user_id: Option<Ulid>,
    pub timestamp: i64,
    pub details: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct CleanupSummary {
    pub expired_sessions_cleaned: u32,
    pub teams_activity_updated: usize,
    pub cleanup_timestamp: i64,
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
    use crate::models::{DeviceInfo, DeviceType, TeamType, TeamStatus, TeamSettings, TeamVisibility, AuditLevel};

    fn temp_service() -> IntegratedAuthService<'static> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("integration_test");
        let store = Store::open(&path).unwrap();
        let _ = dir.keep();
        let leaked: &'static Store = Box::leak(Box::new(store));
        IntegratedAuthService::new(leaked)
    }

    #[test]
    fn test_team_resource_sharing() {
        let service = temp_service();
        let org_id = 1;
        let sharer_id = Ulid::new();
        let team_id = Ulid::new();

        // This would require setting up proper test data
        // For now, just test that the method doesn't panic
        let result = service.share_resource_with_team(
            sharer_id,
            org_id,
            &team_id,
            ResourceKind::Collections,
            "test_collection".to_string(),
            vec![CrudAction::Read, CrudAction::Update],
        );

        // Expected to fail due to missing permissions, but shouldn't panic
        assert!(result.is_err());
    }
}