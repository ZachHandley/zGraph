use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use zcore_storage::Store;

use crate::models::{
    RoleDelegation, DelegationScope, DelegationCondition, DelegationStatus, ConditionType,
    AuditLogEntry, AuditAction, AuditStatus,
};
use crate::repository::AuthRepository;

/// Service for managing role delegation and inheritance
#[derive(Debug, Clone)]
pub struct DelegationService<'a> {
    repo: AuthRepository<'a>,
}

impl<'a> DelegationService<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self {
            repo: AuthRepository::new(store),
        }
    }

    /// Create a new role delegation
    pub fn create_delegation(
        &self,
        delegator_id: Ulid,
        delegatee_id: Ulid,
        org_id: u64,
        role: String,
        scope: DelegationScope,
        conditions: Vec<DelegationCondition>,
        expires_at: Option<i64>,
    ) -> Result<RoleDelegation> {
        // Validate that delegator has the role they're trying to delegate
        self.validate_delegator_permissions(&delegator_id, org_id, &role, &scope)?;

        // Check for existing active delegation
        if let Some(_existing) = self.get_active_delegation(&delegatee_id, org_id, &role, &scope)? {
            return Err(anyhow!("Active delegation already exists for this role and scope"));
        }

        let now = current_timestamp();
        let delegation_id = Ulid::new();

        let delegation = RoleDelegation {
            id: delegation_id,
            delegator_id,
            delegatee_id,
            org_id,
            role: role.clone(),
            scope: scope.clone(),
            conditions,
            expires_at,
            created_at: now,
            status: DelegationStatus::Active,
        };

        self.repo.insert_role_delegation(&delegation)?;

        // Log delegation creation
        self.log_audit_event(
            org_id,
            None,
            Some(delegator_id),
            None,
            AuditAction::RoleDelegate,
            Some("role_delegation".to_string()),
            Some(delegation_id.to_string()),
            serde_json::json!({
                "delegatee_id": delegatee_id,
                "role": role,
                "scope": scope,
                "expires_at": expires_at,
            }),
            None,
            None,
            AuditStatus::Success,
        )?;

        Ok(delegation)
    }

    /// Revoke a role delegation
    pub fn revoke_delegation(
        &self,
        delegation_id: &Ulid,
        revoker_id: Ulid,
    ) -> Result<()> {
        let mut delegation = self.repo.get_role_delegation(delegation_id)?
            .ok_or_else(|| anyhow!("Delegation not found"))?;

        // Check if revoker has permission to revoke
        if delegation.delegator_id != revoker_id {
            // Check if revoker is an admin or has higher privileges
            self.validate_revocation_permissions(&revoker_id, &delegation)?;
        }

        delegation.status = DelegationStatus::Revoked;
        self.repo.update_role_delegation(&delegation)?;

        // Log delegation revocation
        self.log_audit_event(
            delegation.org_id,
            None,
            Some(revoker_id),
            None,
            AuditAction::RoleDelegationRevoke,
            Some("role_delegation".to_string()),
            Some(delegation_id.to_string()),
            serde_json::json!({
                "delegation_id": delegation_id,
                "original_delegator": delegation.delegator_id,
                "delegatee": delegation.delegatee_id,
            }),
            None,
            None,
            AuditStatus::Success,
        )?;

        Ok(())
    }

    /// Get effective roles for a user considering delegations
    pub fn get_effective_roles(
        &self,
        user_id: &Ulid,
        org_id: u64,
        context: &DelegationContext,
    ) -> Result<Vec<EffectiveRole>> {
        let mut effective_roles = Vec::new();

        // Get user's direct roles
        if let Some(user) = self.repo.get_user_by_id(org_id, user_id)? {
            for role in &user.roles {
                effective_roles.push(EffectiveRole {
                    role: role.clone(),
                    source: RoleSource::Direct,
                    scope: DelegationScope::Global,
                    expires_at: None,
                    delegation_id: None,
                });
            }
        }

        // Get delegated roles
        let delegations = self.repo.get_user_delegations(user_id, org_id)?;
        let now = current_timestamp();

        for delegation in delegations {
            // Check if delegation is active and not expired
            if delegation.status != DelegationStatus::Active {
                continue;
            }

            if let Some(expires_at) = delegation.expires_at {
                if expires_at <= now {
                    // Mark as expired
                    self.expire_delegation(&delegation.id)?;
                    continue;
                }
            }

            // Check delegation conditions
            if self.check_delegation_conditions(&delegation, context)? {
                effective_roles.push(EffectiveRole {
                    role: delegation.role.clone(),
                    source: RoleSource::Delegated {
                        delegator_id: delegation.delegator_id,
                    },
                    scope: delegation.scope.clone(),
                    expires_at: delegation.expires_at,
                    delegation_id: Some(delegation.id),
                });
            }
        }

        Ok(effective_roles)
    }

    /// Check if delegation conditions are met
    fn check_delegation_conditions(
        &self,
        delegation: &RoleDelegation,
        context: &DelegationContext,
    ) -> Result<bool> {
        for condition in &delegation.conditions {
            if !self.evaluate_condition(condition, context)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Evaluate a single delegation condition
    fn evaluate_condition(
        &self,
        condition: &DelegationCondition,
        context: &DelegationContext,
    ) -> Result<bool> {
        match condition.condition_type {
            ConditionType::TimeRange => {
                // Parse time range from condition value
                if let Ok(time_range) = serde_json::from_str::<TimeRange>(&condition.value) {
                    let now = current_timestamp();
                    return Ok(now >= time_range.start && now <= time_range.end);
                }
                Ok(false)
            }
            ConditionType::IpAddress => {
                if let Some(ip) = &context.ip_address {
                    // Support CIDR notation and exact matches
                    return Ok(self.ip_matches(&condition.value, ip));
                }
                Ok(false)
            }
            ConditionType::UserAgent => {
                if let Some(ua) = &context.user_agent {
                    return Ok(ua.contains(&condition.value));
                }
                Ok(false)
            }
            ConditionType::GeographicLocation => {
                // Would integrate with IP geolocation service
                // For now, return true
                Ok(true)
            }
            ConditionType::ResourceAccess => {
                // Check if user is accessing specific resources
                if let Some(resource_access) = &context.resource_access {
                    if let Ok(required_access) = serde_json::from_str::<ResourceAccess>(&condition.value) {
                        return Ok(resource_access.resource_type == required_access.resource_type
                            && resource_access.resource_id == required_access.resource_id);
                    }
                }
                Ok(false)
            }
        }
    }

    /// Check if IP address matches condition (supports CIDR)
    fn ip_matches(&self, condition: &str, user_ip: &str) -> bool {
        // Simple exact match for now
        // In production, would use CIDR parsing
        if condition == user_ip {
            return true;
        }

        // Check for wildcard patterns
        if condition.ends_with("*") {
            let prefix = &condition[..condition.len() - 1];
            return user_ip.starts_with(prefix);
        }

        false
    }

    /// Get active delegation for specific role and scope
    fn get_active_delegation(
        &self,
        user_id: &Ulid,
        org_id: u64,
        role: &str,
        scope: &DelegationScope,
    ) -> Result<Option<RoleDelegation>> {
        let delegations = self.repo.get_user_delegations(user_id, org_id)?;

        for delegation in delegations {
            if delegation.status == DelegationStatus::Active
                && delegation.role == role
                && delegation.scope == *scope
            {
                return Ok(Some(delegation));
            }
        }

        Ok(None)
    }

    /// Validate that delegator has permission to delegate
    fn validate_delegator_permissions(
        &self,
        delegator_id: &Ulid,
        org_id: u64,
        role: &str,
        scope: &DelegationScope,
    ) -> Result<()> {
        // Get delegator's current roles
        if let Some(user) = self.repo.get_user_by_id(org_id, delegator_id)? {
            // Check if user has the role they're trying to delegate
            if !user.roles.contains(&role.to_string()) {
                return Err(anyhow!("Delegator does not have the role being delegated"));
            }

            // Additional scope-based validation
            match scope {
                DelegationScope::Team(team_id) => {
                    // Check if delegator is admin/owner of the team
                    if let Some(member) = self.repo.get_team_member(team_id, delegator_id)? {
                        if !matches!(member.role, crate::models::TeamRole::Admin | crate::models::TeamRole::Owner) {
                            return Err(anyhow!("Insufficient team permissions to delegate role"));
                        }
                    } else {
                        return Err(anyhow!("Delegator is not a member of the target team"));
                    }
                }
                DelegationScope::Environment(_) => {
                    // Check environment-specific permissions
                    // For now, require admin role
                    if !user.roles.contains(&"admin".to_string()) {
                        return Err(anyhow!("Admin role required for environment-scoped delegation"));
                    }
                }
                DelegationScope::Resource { .. } => {
                    // Check resource-specific permissions
                    // Implementation depends on resource type
                }
                DelegationScope::Global => {
                    // Require admin role for global delegation
                    if !user.roles.contains(&"admin".to_string()) {
                        return Err(anyhow!("Admin role required for global delegation"));
                    }
                }
            }
        } else {
            return Err(anyhow!("Delegator not found"));
        }

        Ok(())
    }

    /// Validate permission to revoke delegation
    fn validate_revocation_permissions(
        &self,
        revoker_id: &Ulid,
        delegation: &RoleDelegation,
    ) -> Result<()> {
        if let Some(user) = self.repo.get_user_by_id(delegation.org_id, revoker_id)? {
            // Admin can revoke any delegation
            if user.roles.contains(&"admin".to_string()) {
                return Ok(());
            }

            // Team admins can revoke team-scoped delegations
            if let DelegationScope::Team(team_id) = &delegation.scope {
                if let Some(member) = self.repo.get_team_member(team_id, revoker_id)? {
                    if matches!(member.role, crate::models::TeamRole::Admin | crate::models::TeamRole::Owner) {
                        return Ok(());
                    }
                }
            }
        }

        Err(anyhow!("Insufficient permissions to revoke this delegation"))
    }

    /// Mark delegation as expired
    fn expire_delegation(&self, delegation_id: &Ulid) -> Result<()> {
        if let Some(mut delegation) = self.repo.get_role_delegation(delegation_id)? {
            delegation.status = DelegationStatus::Expired;
            self.repo.update_role_delegation(&delegation)?;
        }
        Ok(())
    }

    /// Cleanup expired delegations
    pub fn cleanup_expired_delegations(&self, org_id: u64) -> Result<u32> {
        let delegations = self.repo.get_org_delegations(org_id)?;
        let now = current_timestamp();
        let mut expired_count = 0;

        for delegation in delegations {
            if delegation.status == DelegationStatus::Active {
                if let Some(expires_at) = delegation.expires_at {
                    if expires_at <= now {
                        self.expire_delegation(&delegation.id)?;
                        expired_count += 1;
                    }
                }
            }
        }

        Ok(expired_count)
    }

    /// Get delegation statistics for an organization
    pub fn get_delegation_stats(&self, org_id: u64) -> Result<DelegationStats> {
        let delegations = self.repo.get_org_delegations(org_id)?;

        let mut stats = DelegationStats {
            total_delegations: delegations.len(),
            active_delegations: 0,
            expired_delegations: 0,
            revoked_delegations: 0,
            delegations_by_role: HashMap::new(),
            delegations_by_scope: HashMap::new(),
        };

        for delegation in delegations {
            match delegation.status {
                DelegationStatus::Active => stats.active_delegations += 1,
                DelegationStatus::Expired => stats.expired_delegations += 1,
                DelegationStatus::Revoked => stats.revoked_delegations += 1,
                DelegationStatus::Suspended => {} // Count as inactive
            }

            *stats.delegations_by_role.entry(delegation.role).or_insert(0) += 1;

            let scope_key = match delegation.scope {
                DelegationScope::Global => "global".to_string(),
                DelegationScope::Team(_) => "team".to_string(),
                DelegationScope::Environment(_) => "environment".to_string(),
                DelegationScope::Resource { .. } => "resource".to_string(),
            };
            *stats.delegations_by_scope.entry(scope_key).or_insert(0) += 1;
        }

        Ok(stats)
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegationContext {
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub resource_access: Option<ResourceAccess>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceAccess {
    pub resource_type: String,
    pub resource_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    pub start: i64,
    pub end: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct EffectiveRole {
    pub role: String,
    pub source: RoleSource,
    pub scope: DelegationScope,
    pub expires_at: Option<i64>,
    pub delegation_id: Option<Ulid>,
}

#[derive(Debug, Clone, Serialize)]
pub enum RoleSource {
    Direct,
    Delegated { delegator_id: Ulid },
}

#[derive(Debug, Clone, Serialize)]
pub struct DelegationStats {
    pub total_delegations: usize,
    pub active_delegations: usize,
    pub expired_delegations: usize,
    pub revoked_delegations: usize,
    pub delegations_by_role: HashMap<String, usize>,
    pub delegations_by_scope: HashMap<String, usize>,
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

    fn temp_service() -> DelegationService<'static> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("delegation_test");
        let store = Store::open(&path).unwrap();
        let _ = dir.keep();
        let leaked: &'static Store = Box::leak(Box::new(store));
        DelegationService::new(leaked)
    }

    #[test]
    fn test_ip_matching() {
        let service = temp_service();

        assert!(service.ip_matches("192.168.1.100", "192.168.1.100"));
        assert!(service.ip_matches("192.168.1.*", "192.168.1.100"));
        assert!(!service.ip_matches("192.168.2.*", "192.168.1.100"));
    }

    #[test]
    fn test_time_range_condition() {
        let service = temp_service();
        let now = current_timestamp();

        let context = DelegationContext {
            ip_address: None,
            user_agent: None,
            resource_access: None,
            timestamp: now,
        };

        let condition = DelegationCondition {
            condition_type: ConditionType::TimeRange,
            value: serde_json::to_string(&TimeRange {
                start: now - 3600,
                end: now + 3600,
            }).unwrap(),
        };

        assert!(service.evaluate_condition(&condition, &context).unwrap());
    }
}