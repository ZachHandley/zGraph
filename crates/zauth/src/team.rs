use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use serde::Serialize;
use ulid::Ulid;
use zcore_storage::Store;

use crate::models::{
    TeamRecord, TeamType, TeamStatus, TeamSettings,
    TeamMemberRecord, TeamRole, MemberStatus, TeamInvitation, InvitationStatus,
    AuditLogEntry, AuditAction, AuditStatus,
};
use crate::repository::AuthRepository;

#[derive(Debug, Clone)]
pub struct TeamService<'a> {
    repo: AuthRepository<'a>,
}

impl<'a> TeamService<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self {
            repo: AuthRepository::new(store),
        }
    }

    /// Create a new team
    pub fn create_team(
        &self,
        org_id: u64,
        creator_id: Ulid,
        name: String,
        description: Option<String>,
        parent_team_id: Option<Ulid>,
        team_type: TeamType,
        settings: TeamSettings,
    ) -> Result<TeamRecord> {
        // Validate team name uniqueness within org
        if self.repo.get_team_by_name(org_id, &name)?.is_some() {
            return Err(anyhow!("Team name already exists"));
        }

        // Validate parent team exists if specified
        if let Some(parent_id) = parent_team_id {
            if self.repo.get_team_by_id(org_id, &parent_id)?.is_none() {
                return Err(anyhow!("Parent team does not exist"));
            }
        }

        let now = current_timestamp();
        let team_id = Ulid::new();

        let team = TeamRecord {
            id: team_id,
            org_id,
            name: name.clone(),
            description,
            parent_team_id,
            team_type,
            status: TeamStatus::Active,
            settings,
            created_at: now,
            updated_at: now,
            created_by: creator_id,
        };

        self.repo.insert_team(&team)?;

        // Add creator as team owner
        let owner_member = TeamMemberRecord {
            team_id,
            user_id: creator_id,
            role: TeamRole::Owner,
            permissions: vec!["*".to_string()], // Full permissions
            joined_at: now,
            invited_by: None,
            status: MemberStatus::Active,
        };

        self.repo.insert_team_member(&owner_member)?;

        // Log team creation
        self.log_audit_event(
            org_id,
            None,
            Some(creator_id),
            Some(team_id),
            AuditAction::TeamCreate,
            Some("team".to_string()),
            Some(team_id.to_string()),
            serde_json::json!({
                "team_name": name,
                "team_type": team_type,
                "parent_team_id": parent_team_id,
            }),
            None,
            None,
            AuditStatus::Success,
        )?;

        Ok(team)
    }

    /// Update team settings
    pub fn update_team(
        &self,
        org_id: u64,
        team_id: &Ulid,
        updater_id: Ulid,
        name: Option<String>,
        description: Option<String>,
        settings: Option<TeamSettings>,
    ) -> Result<TeamRecord> {
        // Check permissions
        self.ensure_team_permission(org_id, team_id, &updater_id, &TeamRole::Admin)?;

        let mut team = self.repo.get_team_by_id(org_id, team_id)?
            .ok_or_else(|| anyhow!("Team not found"))?;

        let mut changes = serde_json::Map::new();

        if let Some(new_name) = name {
            if new_name != team.name {
                // Check name uniqueness
                if self.repo.get_team_by_name(org_id, &new_name)?.is_some() {
                    return Err(anyhow!("Team name already exists"));
                }
                changes.insert("name".to_string(), serde_json::json!({
                    "old": team.name,
                    "new": new_name.clone()
                }));
                team.name = new_name;
            }
        }

        if let Some(new_description) = description {
            changes.insert("description".to_string(), serde_json::json!({
                "old": team.description,
                "new": new_description.clone()
            }));
            team.description = Some(new_description);
        }

        if let Some(new_settings) = settings {
            changes.insert("settings".to_string(), serde_json::json!({
                "old": team.settings,
                "new": new_settings
            }));
            team.settings = new_settings;
        }

        team.updated_at = current_timestamp();
        self.repo.update_team(&team)?;

        // Log team update
        self.log_audit_event(
            org_id,
            None,
            Some(updater_id),
            Some(*team_id),
            AuditAction::TeamUpdate,
            Some("team".to_string()),
            Some(team_id.to_string()),
            serde_json::json!({ "changes": changes }),
            None,
            None,
            AuditStatus::Success,
        )?;

        Ok(team)
    }

    /// Delete/archive a team
    pub fn delete_team(
        &self,
        org_id: u64,
        team_id: &Ulid,
        deleter_id: Ulid,
        permanent: bool,
    ) -> Result<()> {
        // Check permissions (only owners can delete)
        self.ensure_team_permission(org_id, team_id, &deleter_id, &TeamRole::Owner)?;

        if permanent {
            // Remove all team members
            self.repo.delete_team_members(team_id)?;
            // Remove all invitations
            self.repo.delete_team_invitations(team_id)?;
            // Remove team
            self.repo.delete_team(team_id)?;
        } else {
            // Archive team
            let mut team = self.repo.get_team_by_id(org_id, team_id)?
                .ok_or_else(|| anyhow!("Team not found"))?;
            team.status = TeamStatus::Archived;
            team.updated_at = current_timestamp();
            self.repo.update_team(&team)?;
        }

        // Log team deletion
        self.log_audit_event(
            org_id,
            None,
            Some(deleter_id),
            Some(*team_id),
            AuditAction::TeamDelete,
            Some("team".to_string()),
            Some(team_id.to_string()),
            serde_json::json!({ "permanent": permanent }),
            None,
            None,
            AuditStatus::Success,
        )?;

        Ok(())
    }

    /// Invite user to team
    pub fn invite_user(
        &self,
        org_id: u64,
        team_id: &Ulid,
        inviter_id: Ulid,
        invitee_email: String,
        role: TeamRole,
        message: Option<String>,
        expires_in_hours: Option<u32>,
    ) -> Result<TeamInvitation> {
        // Check permissions
        self.ensure_team_permission(org_id, team_id, &inviter_id, &TeamRole::Admin)?;

        // Check if user is already a member
        if let Some(user) = self.repo.get_user_by_email(org_id, &invitee_email)? {
            if self.repo.get_team_member(team_id, &user.id)?.is_some() {
                return Err(anyhow!("User is already a team member"));
            }
        }

        let now = current_timestamp();
        let expires_at = now + (expires_in_hours.unwrap_or(72) as i64 * 3600); // Default 72 hours

        let invitation = TeamInvitation {
            id: Ulid::new(),
            team_id: *team_id,
            inviter_id,
            invitee_email: invitee_email.clone(),
            role,
            message,
            expires_at,
            created_at: now,
            accepted_at: None,
            declined_at: None,
            status: InvitationStatus::Pending,
        };

        self.repo.insert_team_invitation(&invitation)?;

        // Log invitation
        self.log_audit_event(
            org_id,
            None,
            Some(inviter_id),
            Some(*team_id),
            AuditAction::TeamInvite,
            Some("team_invitation".to_string()),
            Some(invitation.id.to_string()),
            serde_json::json!({
                "invitee_email": invitee_email,
                "role": role,
                "expires_at": expires_at,
            }),
            None,
            None,
            AuditStatus::Success,
        )?;

        Ok(invitation)
    }

    /// Accept team invitation
    pub fn accept_invitation(
        &self,
        invitation_id: &Ulid,
        user_id: Ulid,
    ) -> Result<TeamMemberRecord> {
        let mut invitation = self.repo.get_team_invitation(invitation_id)?
            .ok_or_else(|| anyhow!("Invitation not found"))?;

        if invitation.status != InvitationStatus::Pending {
            return Err(anyhow!("Invitation is not pending"));
        }

        if invitation.expires_at < current_timestamp() {
            return Err(anyhow!("Invitation has expired"));
        }

        // Verify user email matches invitation
        let user = self.repo.get_user_by_id(invitation.team_id.to_string().parse::<u64>().unwrap_or(0), &user_id)?
            .ok_or_else(|| anyhow!("User not found"))?;

        if user.email.to_lowercase() != invitation.invitee_email.to_lowercase() {
            return Err(anyhow!("Email mismatch"));
        }

        // Create team member record
        let member = TeamMemberRecord {
            team_id: invitation.team_id,
            user_id,
            role: invitation.role.clone(),
            permissions: self.get_default_permissions(&invitation.role),
            joined_at: current_timestamp(),
            invited_by: Some(invitation.inviter_id),
            status: MemberStatus::Active,
        };

        self.repo.insert_team_member(&member)?;

        // Update invitation status
        invitation.status = InvitationStatus::Accepted;
        invitation.accepted_at = Some(current_timestamp());
        self.repo.update_team_invitation(&invitation)?;

        // Log join event
        self.log_audit_event(
            user.org_id,
            None,
            Some(user_id),
            Some(invitation.team_id),
            AuditAction::TeamJoin,
            Some("team_member".to_string()),
            Some(format!("{}:{}", invitation.team_id, user_id)),
            serde_json::json!({
                "role": invitation.role,
                "invited_by": invitation.inviter_id,
            }),
            None,
            None,
            AuditStatus::Success,
        )?;

        Ok(member)
    }

    /// Remove member from team
    pub fn remove_member(
        &self,
        org_id: u64,
        team_id: &Ulid,
        member_user_id: &Ulid,
        remover_id: Ulid,
    ) -> Result<()> {
        // Check permissions
        let remover_member = self.repo.get_team_member(team_id, &remover_id)?
            .ok_or_else(|| anyhow!("Remover is not a team member"))?;

        let member_to_remove = self.repo.get_team_member(team_id, member_user_id)?
            .ok_or_else(|| anyhow!("Member not found"))?;

        // Check if remover has permission to remove this member
        if !self.can_remove_member(&remover_member.role, &member_to_remove.role) {
            return Err(anyhow!("Insufficient permissions to remove this member"));
        }

        // Prevent removing the last owner
        if member_to_remove.role == TeamRole::Owner {
            let owners = self.repo.get_team_members_by_role(team_id, &TeamRole::Owner)?;
            if owners.len() <= 1 {
                return Err(anyhow!("Cannot remove the last team owner"));
            }
        }

        self.repo.remove_team_member(team_id, member_user_id)?;

        // Log removal
        self.log_audit_event(
            org_id,
            None,
            Some(remover_id),
            Some(*team_id),
            AuditAction::TeamLeave,
            Some("team_member".to_string()),
            Some(format!("{}:{}", team_id, member_user_id)),
            serde_json::json!({
                "removed_user_id": member_user_id,
                "removed_role": member_to_remove.role,
                "removed_by": remover_id,
            }),
            None,
            None,
            AuditStatus::Success,
        )?;

        Ok(())
    }

    /// Update member role
    pub fn update_member_role(
        &self,
        org_id: u64,
        team_id: &Ulid,
        member_user_id: &Ulid,
        new_role: TeamRole,
        updater_id: Ulid,
    ) -> Result<()> {
        // Check permissions
        self.ensure_team_permission(org_id, team_id, &updater_id, &TeamRole::Admin)?;

        let mut member = self.repo.get_team_member(team_id, member_user_id)?
            .ok_or_else(|| anyhow!("Member not found"))?;

        let old_role = member.role.clone();

        // Prevent changing the last owner's role
        if old_role == TeamRole::Owner && new_role != TeamRole::Owner {
            let owners = self.repo.get_team_members_by_role(team_id, &TeamRole::Owner)?;
            if owners.len() <= 1 {
                return Err(anyhow!("Cannot change the role of the last team owner"));
            }
        }

        member.role = new_role.clone();
        member.permissions = self.get_default_permissions(&new_role);
        self.repo.update_team_member(&member)?;

        // Log role change
        self.log_audit_event(
            org_id,
            None,
            Some(updater_id),
            Some(*team_id),
            AuditAction::PermissionGrant,
            Some("team_member".to_string()),
            Some(format!("{}:{}", team_id, member_user_id)),
            serde_json::json!({
                "old_role": old_role,
                "new_role": new_role,
                "updated_by": updater_id,
            }),
            None,
            None,
            AuditStatus::Success,
        )?;

        Ok(())
    }

    /// Get team by ID
    pub fn get_team(&self, org_id: u64, team_id: &Ulid) -> Result<Option<TeamRecord>> {
        self.repo.get_team_by_id(org_id, team_id)
    }

    /// Get all teams for an organization
    pub fn get_org_teams(&self, org_id: u64) -> Result<Vec<TeamRecord>> {
        self.repo.get_teams_by_org(org_id)
    }

    /// Get teams user is a member of
    pub fn get_user_teams(&self, org_id: u64, user_id: &Ulid) -> Result<Vec<(TeamRecord, TeamMemberRecord)>> {
        self.repo.get_user_teams(org_id, user_id)
    }

    /// Get team members
    pub fn get_team_members(&self, team_id: &Ulid) -> Result<Vec<TeamMemberRecord>> {
        self.repo.get_team_members(team_id)
    }

    /// Get team hierarchy (parent and child teams)
    pub fn get_team_hierarchy(&self, org_id: u64, team_id: &Ulid) -> Result<TeamHierarchy> {
        let team = self.repo.get_team_by_id(org_id, team_id)?
            .ok_or_else(|| anyhow!("Team not found"))?;

        let parent = if let Some(parent_id) = team.parent_team_id {
            self.repo.get_team_by_id(org_id, &parent_id)?
        } else {
            None
        };

        let children = self.repo.get_child_teams(org_id, team_id)?;

        Ok(TeamHierarchy {
            team,
            parent,
            children,
        })
    }

    /// Check if user has specific team permission
    pub fn has_team_permission(
        &self,
        _org_id: u64,
        team_id: &Ulid,
        user_id: &Ulid,
        required_role: &TeamRole,
    ) -> Result<bool> {
        let member = match self.repo.get_team_member(team_id, user_id)? {
            Some(member) => member,
            None => return Ok(false),
        };

        Ok(self.role_satisfies_requirement(&member.role, required_role))
    }

    /// Ensure user has team permission (throws error if not)
    fn ensure_team_permission(
        &self,
        org_id: u64,
        team_id: &Ulid,
        user_id: &Ulid,
        required_role: &TeamRole,
    ) -> Result<()> {
        if !self.has_team_permission(org_id, team_id, user_id, required_role)? {
            return Err(anyhow!("Insufficient team permissions"));
        }
        Ok(())
    }

    /// Check if a role satisfies the requirement
    fn role_satisfies_requirement(&self, user_role: &TeamRole, required_role: &TeamRole) -> bool {
        let role_hierarchy = vec![
            TeamRole::Guest,
            TeamRole::Viewer,
            TeamRole::Member,
            TeamRole::Admin,
            TeamRole::Owner,
        ];

        let user_level = role_hierarchy.iter().position(|r| r == user_role).unwrap_or(0);
        let required_level = role_hierarchy.iter().position(|r| r == required_role).unwrap_or(0);

        user_level >= required_level
    }

    /// Check if one role can remove another
    fn can_remove_member(&self, remover_role: &TeamRole, target_role: &TeamRole) -> bool {
        match remover_role {
            TeamRole::Owner => true, // Owners can remove anyone
            TeamRole::Admin => !matches!(target_role, TeamRole::Owner), // Admins can't remove owners
            _ => false, // Others can't remove members
        }
    }

    /// Get default permissions for a role
    fn get_default_permissions(&self, role: &TeamRole) -> Vec<String> {
        match role {
            TeamRole::Owner => vec!["*".to_string()],
            TeamRole::Admin => vec![
                "team:read".to_string(),
                "team:write".to_string(),
                "members:read".to_string(),
                "members:write".to_string(),
                "invitations:create".to_string(),
                "invitations:revoke".to_string(),
            ],
            TeamRole::Member => vec![
                "team:read".to_string(),
                "members:read".to_string(),
                "resources:read".to_string(),
                "resources:write".to_string(),
            ],
            TeamRole::Viewer => vec![
                "team:read".to_string(),
                "members:read".to_string(),
                "resources:read".to_string(),
            ],
            TeamRole::Guest => vec![
                "team:read".to_string(),
            ],
        }
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
pub struct TeamHierarchy {
    pub team: TeamRecord,
    pub parent: Option<TeamRecord>,
    pub children: Vec<TeamRecord>,
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

    fn temp_service() -> TeamService<'static> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("team_test");
        let store = Store::open(&path).unwrap();
        let _ = dir.keep();
        let leaked: &'static Store = Box::leak(Box::new(store));
        TeamService::new(leaked)
    }

    #[test]
    fn test_role_hierarchy() {
        let service = temp_service();

        assert!(service.role_satisfies_requirement(&TeamRole::Owner, &TeamRole::Admin));
        assert!(service.role_satisfies_requirement(&TeamRole::Admin, &TeamRole::Member));
        assert!(service.role_satisfies_requirement(&TeamRole::Member, &TeamRole::Viewer));
        assert!(service.role_satisfies_requirement(&TeamRole::Viewer, &TeamRole::Guest));

        assert!(!service.role_satisfies_requirement(&TeamRole::Guest, &TeamRole::Viewer));
        assert!(!service.role_satisfies_requirement(&TeamRole::Member, &TeamRole::Admin));
    }

    #[test]
    fn test_can_remove_member() {
        let service = temp_service();

        assert!(service.can_remove_member(&TeamRole::Owner, &TeamRole::Admin));
        assert!(service.can_remove_member(&TeamRole::Owner, &TeamRole::Owner));
        assert!(service.can_remove_member(&TeamRole::Admin, &TeamRole::Member));
        assert!(!service.can_remove_member(&TeamRole::Admin, &TeamRole::Owner));
        assert!(!service.can_remove_member(&TeamRole::Member, &TeamRole::Admin));
    }
}