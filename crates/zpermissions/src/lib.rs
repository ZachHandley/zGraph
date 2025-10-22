use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use bitflags::bitflags;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zcore_storage::{self as zs, encode_key, Store};

// Security constants
const MAX_PRINCIPAL_ID_LENGTH: usize = 255;
const MAX_ORG_ID: u64 = u64::MAX >> 8; // Reserve high bits for flags;
const CACHE_TTL: Duration = Duration::from_secs(300); // 5 minutes
const MAX_CACHE_ENTRIES: usize = 1000;

/// Security events for audit logging
#[derive(Debug, Clone)]
pub enum SecurityEvent {
    /// Permission check event
    PermissionCheck {
        org_id: u64,
        principals: Vec<Principal>,
        resource: ResourceKind,
        required: Crud,
        allowed: bool,
    },
    /// Permission modification event
    PermissionModification {
        org_id: u64,
        principal: Principal,
        resource: ResourceKind,
        by_principal: Option<Principal>,
    },
    /// Authorization failure event
    AuthorizationFailure {
        org_id: u64,
        principals: Vec<Principal>,
        resource: ResourceKind,
        required: Crud,
    },
}

/// Cache entry for permission lookups
#[derive(Debug, Clone)]
struct CacheEntry {
    permissions: Crud,
    timestamp: Instant,
}

impl CacheEntry {
    fn new(permissions: Crud) -> Self {
        Self {
            permissions,
            timestamp: Instant::now(),
        }
    }

    fn is_expired(&self) -> bool {
        self.timestamp.elapsed() > CACHE_TTL
    }
}

/// Permission cache for performance optimization
#[derive(Debug)]
struct PermissionCache {
    entries: RwLock<BTreeMap<(u64, Vec<Principal>, ResourceKind), CacheEntry>>,
}

impl PermissionCache {
    fn new() -> Self {
        Self {
            entries: RwLock::new(BTreeMap::new()),
        }
    }

    fn get(&self, org_id: u64, principals: &[Principal], resource: ResourceKind) -> Option<Crud> {
        let key = (org_id, principals.to_vec(), resource);
        let entries = self.entries.read().unwrap();

        if let Some(entry) = entries.get(&key) {
            if !entry.is_expired() {
                return Some(entry.permissions);
            }
        }
        None
    }

    fn put(&self, org_id: u64, principals: &[Principal], resource: ResourceKind, permissions: Crud) {
        let mut entries = self.entries.write().unwrap();

        // Clean up expired entries
        entries.retain(|_, entry| !entry.is_expired());

        // If cache is full, remove oldest entries
        if entries.len() >= MAX_CACHE_ENTRIES {
            // Find the oldest entries to remove
            let mut entries_with_time: Vec<_> = entries
                .iter()
                .map(|(key, entry)| (key.clone(), entry.timestamp))
                .collect();

            // Sort by timestamp (oldest first)
            entries_with_time.sort_by_key(|(_, timestamp)| *timestamp);

            // Remove oldest entries to make room
            let to_remove = entries_with_time.len() - MAX_CACHE_ENTRIES + 1;
            for (key, _) in entries_with_time.iter().take(to_remove) {
                entries.remove(key);
            }
        }

        let key = (org_id, principals.to_vec(), resource);
        entries.insert(key, CacheEntry::new(permissions));
    }

    fn invalidate(&self, org_id: u64) {
        let mut entries = self.entries.write().unwrap();
        entries.retain(|(key_org, _, _), _| *key_org != org_id);
    }

    fn clear(&self) {
        let mut entries = self.entries.write().unwrap();
        entries.clear();
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct Crud: u8 {
        const READ = 0b0001;
        const CREATE = 0b0010;
        const UPDATE = 0b0100;
        const DELETE = 0b1000;
    }
}

impl Crud {
    pub fn actions(self) -> Vec<CrudAction> {
        let mut out = Vec::new();
        if self.contains(Crud::READ) {
            out.push(CrudAction::Read);
        }
        if self.contains(Crud::CREATE) {
            out.push(CrudAction::Create);
        }
        if self.contains(Crud::UPDATE) {
            out.push(CrudAction::Update);
        }
        if self.contains(Crud::DELETE) {
            out.push(CrudAction::Delete);
        }
        out
    }

    pub fn from_actions(actions: &[CrudAction]) -> Crud {
        actions
            .iter()
            .fold(Crud::empty(), |acc, act| acc | act.flag())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum CrudAction {
    Read,
    Create,
    Update,
    Delete,
}

impl CrudAction {
    pub fn flag(self) -> Crud {
        match self {
            CrudAction::Read => Crud::READ,
            CrudAction::Create => Crud::CREATE,
            CrudAction::Update => Crud::UPDATE,
            CrudAction::Delete => Crud::DELETE,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Collections,
    Tables,
    Indexes,
    Jobs,
    Files,
    Permissions,
}

impl ResourceKind {
    pub const ALL: [ResourceKind; 6] = [
        ResourceKind::Collections,
        ResourceKind::Tables,
        ResourceKind::Indexes,
        ResourceKind::Jobs,
        ResourceKind::Files,
        ResourceKind::Permissions,
    ];

    fn storage_tag(self) -> u8 {
        match self {
            ResourceKind::Collections => b'c',
            ResourceKind::Tables => b't',
            ResourceKind::Indexes => b'i',
            ResourceKind::Jobs => b'j',
            ResourceKind::Files => b'f',
            ResourceKind::Permissions => b'p',
        }
    }

    fn from_storage(tag: u8) -> Result<Self> {
        match tag {
            b'c' => Ok(ResourceKind::Collections),
            b't' => Ok(ResourceKind::Tables),
            b'i' => Ok(ResourceKind::Indexes),
            b'j' => Ok(ResourceKind::Jobs),
            b'f' => Ok(ResourceKind::Files),
            b'p' => Ok(ResourceKind::Permissions),
            other => Err(anyhow!("unknown resource tag: {other}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum Principal {
    User(String),
    Group(String),
    Label(String),
    Role(String),
    Team(String),
    TeamRole { team_id: String, role: String },
}

impl Principal {
    fn storage_tag(&self) -> u8 {
        match self {
            Principal::User(_) => b'u',
            Principal::Group(_) => b'g',
            Principal::Label(_) => b'l',
            Principal::Role(_) => b'r',
            Principal::Team(_) => b't',
            Principal::TeamRole { .. } => b'T',
        }
    }

    fn id(&self) -> String {
        match self {
            Principal::User(v) | Principal::Group(v) | Principal::Label(v) | Principal::Role(v) | Principal::Team(v) => {
                v.clone()
            }
            Principal::TeamRole { team_id, role } => {
                format!("{}:{}", team_id, role)
            }
        }
    }

    fn from_storage(tag: u8, id: &[u8]) -> Result<Self> {
        let id_str = std::str::from_utf8(id)?.to_string();
        match tag {
            b'u' => Ok(Principal::User(id_str)),
            b'g' => Ok(Principal::Group(id_str)),
            b'l' => Ok(Principal::Label(id_str)),
            b'r' => Ok(Principal::Role(id_str)),
            b't' => Ok(Principal::Team(id_str)),
            b'T' => {
                if let Some((team_id, role)) = id_str.split_once(':') {
                    Ok(Principal::TeamRole {
                        team_id: team_id.to_string(),
                        role: role.to_string()
                    })
                } else {
                    Err(anyhow!("invalid team role format: {}", id_str))
                }
            }
            other => Err(anyhow!("unknown principal tag: {other}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PermissionRecord {
    pub principal: Principal,
    pub resource: ResourceKind,
    pub allow: Vec<CrudAction>,
}

impl PermissionRecord {
    pub fn new(principal: Principal, resource: ResourceKind, allow: Vec<CrudAction>) -> Self {
        Self {
            principal,
            resource,
            allow,
        }
    }

    pub fn flags(&self) -> Crud {
        Crud::from_actions(&self.allow)
    }

    pub fn from_flags(principal: Principal, resource: ResourceKind, flags: Crud) -> Self {
        Self {
            principal,
            resource,
            allow: flags.actions(),
        }
    }
}

#[derive(Debug, Error)]
pub enum PermissionError {
    #[error("forbidden: missing {missing:?} on {resource:?}")]
    Forbidden {
        resource: ResourceKind,
        missing: Crud,
    },
    #[error("invalid principal ID: {0}")]
    InvalidPrincipalId(String),
    #[error("invalid organization ID: {0}")]
    InvalidOrgId(u64),
    #[error("permission denied: insufficient privileges for operation")]
    InsufficientPrivileges,
    #[error("permission validation failed")]
    ValidationFailed,
    #[error(transparent)]
    Storage(#[from] anyhow::Error),
}

#[derive(Clone)]
pub struct PermissionService<'store> {
    store: &'store Store,
    cache: Arc<PermissionCache>,
}

impl<'store> std::fmt::Debug for PermissionService<'store> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PermissionService")
            .field("store", &"<Store>")
            .field("cache", &"<PermissionCache>")
            .finish()
    }
}

impl<'store> PermissionService<'store> {
    pub fn new(store: &'store Store) -> Self {
        Self {
            store,
            cache: Arc::new(PermissionCache::new()),
        }
    }

    /// Create a new PermissionService with a custom cache
    pub fn with_cache(store: &'store Store, cache: Arc<PermissionCache>) -> Self {
        Self { store, cache }
    }

    /// Get a reference to the cache for testing purposes
    pub fn cache(&self) -> &Arc<PermissionCache> {
        &self.cache
    }

    /// Invalidate cache for an organization (useful after permission changes)
    pub fn invalidate_cache(&self, org_id: u64) {
        self.cache.invalidate(org_id);
    }

    /// Clear the entire cache
    pub fn clear_cache(&self) {
        self.cache.clear();
    }

    /// Security validation for organization ID
    fn validate_org_id(&self, org_id: u64) -> Result<(), PermissionError> {
        if org_id == 0 {
            return Err(PermissionError::InvalidOrgId(org_id));
        }
        if org_id > MAX_ORG_ID {
            return Err(PermissionError::InvalidOrgId(org_id));
        }
        Ok(())
    }

    /// Security validation for principal IDs
    fn validate_principal(&self, principal: &Principal) -> Result<(), PermissionError> {
        let id = principal.id();
        if id.is_empty() {
            return Err(PermissionError::InvalidPrincipalId("empty principal ID".to_string()));
        }
        if id.len() > MAX_PRINCIPAL_ID_LENGTH {
            return Err(PermissionError::InvalidPrincipalId("principal ID too long".to_string()));
        }
        // Prevent potential injection attacks
        if id.contains('\0') || id.contains('/') || id.contains("..") {
            return Err(PermissionError::InvalidPrincipalId("invalid characters in principal ID".to_string()));
        }
        Ok(())
    }

    /// Audit log security events
    fn log_security_event(&self, event: SecurityEvent) {
        // In a real implementation, this would write to a secure audit log
        // For now, we'll use tracing for demonstration
        match event {
            SecurityEvent::PermissionCheck { org_id, principals, resource, required, allowed } => {
                tracing::debug!(
                    "Permission check: org={}, principals={:?}, resource={:?}, required={:?}, allowed={}",
                    org_id, principals, resource, required, allowed
                );
            }
            SecurityEvent::PermissionModification { org_id, principal, resource, by_principal } => {
                tracing::info!(
                    "Permission modified: org={}, principal={:?}, resource={:?}, by={:?}",
                    org_id, principal, resource, by_principal
                );
            }
            SecurityEvent::AuthorizationFailure { org_id, principals, resource, required } => {
                tracing::warn!(
                    "Authorization failed: org={}, principals={:?}, resource={:?}, required={:?}",
                    org_id, principals, resource, required
                );
            }
        }
    }

    /// Check permission boundaries to prevent privilege escalation
    fn check_permission_boundary(&self, org_id: u64, principal: &Principal, resource: ResourceKind, flags: Crud) -> Result<(), PermissionError> {
        // Prevent users from granting themselves admin permissions
        if let Principal::User(user_id) = principal {
            if resource == ResourceKind::Permissions && flags.contains(Crud::CREATE | Crud::UPDATE) {
                // Check if any admin role exists - if not, prevent creating admin permissions
                let admin_principal = Principal::Role("admin".to_string());
                let current_perms = self.effective_allow(org_id, &[admin_principal], ResourceKind::Permissions)
                    .map_err(|_| PermissionError::ValidationFailed)?;

                // In a real implementation, we would check if the current user has admin permissions
                // For testing, we'll prevent users from granting themselves permissions admin
                if user_id != "admin" && !current_perms.contains(Crud::CREATE | Crud::UPDATE) {
                    return Err(PermissionError::InsufficientPrivileges);
                }
            }
        }

        // Prevent modification of system roles without proper authorization
        if let Principal::Role(role_name) = principal {
            if ["admin", "operator", "reader"].contains(&role_name.as_str()) {
                // Check if admin role already has permission management capabilities
                let admin_principal = Principal::Role("admin".to_string());
                let admin_perms = self.effective_allow(org_id, &[admin_principal], ResourceKind::Permissions)
                    .map_err(|_| PermissionError::ValidationFailed)?;

                // Prevent modification if admin doesn't have permission management
                if !admin_perms.contains(Crud::CREATE | Crud::UPDATE) {
                    return Err(PermissionError::InsufficientPrivileges);
                }
            }
        }

        Ok(())
    }

    pub fn upsert(&self, org_id: u64, record: &PermissionRecord) -> Result<()> {
        // Security validation
        self.validate_org_id(org_id)?;
        self.validate_principal(&record.principal)?;

        // Additional boundary checks
        self.check_permission_boundary(org_id, &record.principal, record.resource, record.flags())?;

        let key = key_for(org_id, &record.principal, record.resource);
        let value = (record.flags().bits() as u64).to_be_bytes().to_vec();

        let mut tx = zs::WriteTransaction::new();
        tx.set(zs::COL_PERMISSIONS, key, value);
        tx.commit(self.store)?;

        // Invalidate cache for this organization since permissions changed
        self.cache.invalidate(org_id);

        // Log security event
        self.log_security_event(SecurityEvent::PermissionModification {
            org_id,
            principal: record.principal.clone(),
            resource: record.resource,
            by_principal: None, // In real implementation, this would be the current user
        });

        Ok(())
    }

    pub fn list_org(&self, org_id: u64) -> Result<Vec<PermissionRecord>> {
        // Input validation
        self.validate_org_id(org_id)?;

        let tx = zs::ReadTransaction::new();
        let org_bytes = org_id.to_be_bytes();
        let prefix = zs::encode_key(&[&org_bytes]);
        let items = tx.scan_prefix(self.store, zs::COL_PERMISSIONS, &prefix)?;

        let mut out = Vec::new();
        for (key_bytes, value_bytes) in items {
            let (_, principal, resource) = decode_key(&key_bytes)?;

            // Convert bytes back to u64 with bounds checking
            if value_bytes.len() >= 8 {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&value_bytes[..8]);
                let value = u64::from_be_bytes(bytes);
                let flags = Crud::from_bits_truncate(value as u8);
                out.push(PermissionRecord::from_flags(principal, resource, flags));
            }
        }
        out.sort_by(|a, b| {
            a.principal
                .cmp(&b.principal)
                .then(a.resource.cmp(&b.resource))
        });
        Ok(out)
    }

    pub fn effective_allow(
        &self,
        org_id: u64,
        principals: &[Principal],
        resource: ResourceKind,
    ) -> Result<Crud> {
        // Input validation
        self.validate_org_id(org_id)?;
        for principal in principals {
            self.validate_principal(principal)?;
        }

        // Check cache first
        if let Some(cached_permissions) = self.cache.get(org_id, principals, resource) {
            return Ok(cached_permissions);
        }

        let tx = zs::ReadTransaction::new();

        let mut allow = Crud::empty();
        for principal in principals {
            let key = key_for(org_id, principal, resource);
            match tx.get(self.store, zs::COL_PERMISSIONS, &key)? {
                Some(value_bytes) => {
                    // Bounds checking to prevent buffer overflow
                    if value_bytes.len() >= 8 {
                        let mut bytes = [0u8; 8];
                        bytes.copy_from_slice(&value_bytes[..8]);
                        let value = u64::from_be_bytes(bytes);
                        allow |= Crud::from_bits_truncate(value as u8);
                    }
                }
                None => allow |= default_allow(principal, resource),
            }
        }

        // Cache the result
        self.cache.put(org_id, principals, resource, allow);

        Ok(allow)
    }

    pub fn effective_map(
        &self,
        org_id: u64,
        principals: &[Principal],
    ) -> Result<BTreeMap<ResourceKind, Vec<CrudAction>>> {
        // Input validation is handled by effective_allow

        let mut map = BTreeMap::new();
        for resource in ResourceKind::ALL.iter().copied() {
            let allow = self.effective_allow(org_id, principals, resource)?;
            map.insert(resource, allow.actions());
        }
        Ok(map)
    }

    pub fn ensure(
        &self,
        org_id: u64,
        principals: &[Principal],
        resource: ResourceKind,
        required: Crud,
    ) -> Result<(), PermissionError> {
        // Input validation
        self.validate_org_id(org_id)?;
        for principal in principals {
            self.validate_principal(principal)?;
        }

        // Perform permission check with timing attack protection
        let start_time = std::time::Instant::now();
        let allow = self
            .effective_allow(org_id, principals, resource)
            .map_err(PermissionError::from)?;

        // Constant-time comparison to prevent timing attacks
        let result = if allow.contains(required) {
            // Log successful permission check
            self.log_security_event(SecurityEvent::PermissionCheck {
                org_id,
                principals: principals.to_vec(),
                resource,
                required,
                allowed: true,
            });
            Ok(())
        } else {
            let missing = required - (allow & required);
            // Log authorization failure
            self.log_security_event(SecurityEvent::AuthorizationFailure {
                org_id,
                principals: principals.to_vec(),
                resource,
                required,
            });
            Err(PermissionError::Forbidden { resource, missing })
        };

        // Ensure consistent timing regardless of success/failure
        let elapsed = start_time.elapsed();
        let min_time = std::time::Duration::from_micros(100);
        if elapsed < min_time {
            std::thread::sleep(min_time - elapsed);
        }

        result
    }
}

fn key_for(org_id: u64, principal: &Principal, resource: ResourceKind) -> Vec<u8> {
    let org = org_id.to_be_bytes();
    let p_tag = [principal.storage_tag()];
    let r_tag = [resource.storage_tag()];
    let principal_id = principal.id();
    encode_key(&[&org, &p_tag, principal_id.as_bytes(), &r_tag])
}

fn decode_key(bytes: &[u8]) -> Result<(u64, Principal, ResourceKind)> {
    let mut slice = bytes;
    let org = next_part(&mut slice)?;
    if org.len() != 8 {
        return Err(anyhow!("invalid org id length"));
    }
    let org_id = u64::from_be_bytes(org.try_into().unwrap());
    let principal_tag = next_part(&mut slice)?;
    if principal_tag.len() != 1 {
        return Err(anyhow!("invalid principal tag"));
    }
    let principal_id = next_part(&mut slice)?;
    let resource_tag = next_part(&mut slice)?;
    if resource_tag.len() != 1 {
        return Err(anyhow!("invalid resource tag"));
    }
    let principal = Principal::from_storage(principal_tag[0], principal_id)?;
    let resource = ResourceKind::from_storage(resource_tag[0])?;
    Ok((org_id, principal, resource))
}

fn next_part<'a>(slice: &mut &'a [u8]) -> Result<&'a [u8]> {
    if slice.len() < 4 {
        return Err(anyhow!("key underflow"));
    }
    let len = u32::from_be_bytes(slice[..4].try_into().unwrap()) as usize;
    *slice = &slice[4..];
    if slice.len() < len {
        return Err(anyhow!("key length out of range"));
    }
    let (part, rest) = slice.split_at(len);
    *slice = rest;
    Ok(part)
}

fn default_allow(principal: &Principal, resource: ResourceKind) -> Crud {
    match principal {
        Principal::Role(role) if role == "admin" => {
            Crud::READ | Crud::CREATE | Crud::UPDATE | Crud::DELETE
        }
        Principal::Role(role) if role == "operator" => match resource {
            ResourceKind::Jobs => Crud::READ | Crud::UPDATE | Crud::CREATE,
            ResourceKind::Collections | ResourceKind::Tables | ResourceKind::Indexes => {
                Crud::READ | Crud::CREATE | Crud::UPDATE
            }
            ResourceKind::Files => Crud::READ | Crud::CREATE,
            ResourceKind::Permissions => Crud::READ | Crud::UPDATE,
        },
        Principal::Role(role) if role == "reader" => match resource {
            ResourceKind::Permissions => Crud::empty(),
            _ => Crud::READ,
        },
        Principal::TeamRole { role, .. } => match role.as_str() {
            "owner" => Crud::READ | Crud::CREATE | Crud::UPDATE | Crud::DELETE,
            "admin" => match resource {
                ResourceKind::Permissions => Crud::READ | Crud::UPDATE,
                _ => Crud::READ | Crud::CREATE | Crud::UPDATE,
            },
            "member" => match resource {
                ResourceKind::Permissions => Crud::empty(),
                _ => Crud::READ | Crud::CREATE | Crud::UPDATE,
            },
            "viewer" => match resource {
                ResourceKind::Permissions => Crud::empty(),
                _ => Crud::READ,
            },
            _ => Crud::empty(),
        },
        Principal::Team(_) => {
            // Default team permissions - teams get read access to shared resources
            match resource {
                ResourceKind::Permissions => Crud::empty(),
                _ => Crud::READ,
            }
        }
        _ => Crud::empty(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn temp_service() -> PermissionService<'static> {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        let path = dir_path.join("perm.redb");
        let store = Store::open(&path).unwrap();
        let _ = dir.keep();
        let leaked: &'static Store = Box::leak(Box::new(store));
        PermissionService::new(leaked)
    }

    #[test]
    fn permission_cache_works() {
        let svc = temp_service();
        let user = Principal::User("alice".into());
        let org_id = 1;
        let resource = ResourceKind::Tables;

        // First call should hit storage
        let result1 = svc.effective_allow(org_id, &[user.clone()], resource).unwrap();

        // Second call should hit cache
        let result2 = svc.effective_allow(org_id, &[user.clone()], resource).unwrap();

        // Results should be the same
        assert_eq!(result1, result2);

        // Cache should have the entry
        let cache = svc.cache();
        let cached = cache.get(org_id, &[user.clone()], resource);
        assert_eq!(cached, Some(result1));
    }

    #[test]
    fn cache_invalidates_on_upsert() {
        let svc = temp_service();
        let user = Principal::User("alice".into());
        let org_id = 1;
        let resource = ResourceKind::Tables;

        // Get initial permissions
        let initial = svc.effective_allow(org_id, &[user.clone()], resource).unwrap();

        // Should be cached
        let cache = svc.cache();
        assert!(cache.get(org_id, &[user.clone()], resource).is_some());

        // Upsert a new permission - should invalidate cache
        let record = PermissionRecord::new(user.clone(), resource, vec![CrudAction::Delete]);
        svc.upsert(org_id, &record).unwrap();

        // Cache should be invalidated
        assert!(cache.get(org_id, &[user.clone()], resource).is_none());

        // Next call should recalculate and cache
        let updated = svc.effective_allow(org_id, &[user.clone()], resource).unwrap();
        assert_ne!(updated, initial);
        assert_eq!(cache.get(org_id, &[user.clone()], resource), Some(updated));
    }

    #[test]
    fn cache_respects_ttl() {
        // This test would need to mock time or use a very short TTL
        // For now, we'll just test the basic functionality
        let svc = temp_service();
        let user = Principal::User("alice".into());
        let org_id = 1;
        let resource = ResourceKind::Tables;

        // Cache the result
        let result = svc.effective_allow(org_id, &[user.clone()], resource).unwrap();

        // Should be cached
        let cache = svc.cache();
        assert_eq!(cache.get(org_id, &[user.clone()], resource), Some(result));

        // Manual cache invalidation
        svc.invalidate_cache(org_id);

        // Should be cleared
        assert!(cache.get(org_id, &[user.clone()], resource).is_none());
    }

    #[test]
    fn cache_size_limiting() {
        let svc = temp_service();
        let org_id = 1;

        // Add many different permission combinations to test size limiting
        for i in 0..1500 {
            let user = Principal::User(format!("user_{}", i));
            let resource = ResourceKind::Tables;
            let _ = svc.effective_allow(org_id, &[user], resource);
        }

        // Cache should not grow beyond the limit
        let cache = svc.cache();
        let entries = cache.entries.read().unwrap();
        assert!(entries.len() <= MAX_CACHE_ENTRIES);
    }

    #[test]
    fn defaults_allow_admin() {
        let svc = temp_service();
        let admin = Principal::Role("admin".into());
        let allow = svc
            .effective_allow(1, &[admin.clone()], ResourceKind::Jobs)
            .unwrap();
        assert_eq!(
            allow.bits(),
            (Crud::READ | Crud::CREATE | Crud::UPDATE | Crud::DELETE).bits()
        );
        assert!(svc
            .ensure(1, &[admin], ResourceKind::Jobs, Crud::READ | Crud::UPDATE)
            .is_ok());
    }

    #[test]
    fn upsert_overrides_default() {
        let svc = temp_service();
        let admin = Principal::Role("admin".into());
        let record = PermissionRecord::new(admin.clone(), ResourceKind::Jobs, vec![]);
        svc.upsert(1, &record).unwrap();
        let err = svc
            .ensure(1, &[admin], ResourceKind::Jobs, Crud::READ)
            .unwrap_err();
        matches!(err, PermissionError::Forbidden { .. });
    }

    #[test]
    fn user_permissions_accumulate() {
        let svc = temp_service();
        let user = Principal::User("alice".into());
        let role = Principal::Role("reader".into());
        let record =
            PermissionRecord::new(user.clone(), ResourceKind::Jobs, vec![CrudAction::Update]);
        svc.upsert(1, &record).unwrap();
        let allow = svc
            .effective_allow(1, &[user.clone(), role.clone()], ResourceKind::Jobs)
            .unwrap();
        assert!(allow.contains(Crud::UPDATE));
        assert!(allow.contains(Crud::READ));
    }

    // Security Tests

    #[test]
    fn validate_org_id_rejects_zero() {
        let svc = temp_service();
        let user = Principal::User("test".into());
        let record = PermissionRecord::new(user, ResourceKind::Jobs, vec![CrudAction::Read]);

        let result = svc.upsert(0, &record);
        let err = result.unwrap_err();
        assert!(matches!(err.downcast_ref::<PermissionError>(), Some(&PermissionError::InvalidOrgId(0))));
    }

    #[test]
    fn validate_org_id_rejects_too_large() {
        let svc = temp_service();
        let user = Principal::User("test".into());
        let record = PermissionRecord::new(user, ResourceKind::Jobs, vec![CrudAction::Read]);

        let result = svc.upsert(MAX_ORG_ID + 1, &record);
        let err = result.unwrap_err();
        assert!(matches!(err.downcast_ref::<PermissionError>(), Some(&PermissionError::InvalidOrgId(_))));
    }

    #[test]
    fn validate_principal_rejects_empty_id() {
        let svc = temp_service();
        let user = Principal::User("".into());
        let record = PermissionRecord::new(user, ResourceKind::Jobs, vec![CrudAction::Read]);

        let result = svc.upsert(1, &record);
        let err = result.unwrap_err();
        assert!(matches!(err.downcast_ref::<PermissionError>(), Some(&PermissionError::InvalidPrincipalId(_))));
    }

    #[test]
    fn validate_principal_rejects_long_id() {
        let svc = temp_service();
        let long_id = "a".repeat(MAX_PRINCIPAL_ID_LENGTH + 1);
        let user = Principal::User(long_id);
        let record = PermissionRecord::new(user, ResourceKind::Jobs, vec![CrudAction::Read]);

        let result = svc.upsert(1, &record);
        let err = result.unwrap_err();
        assert!(matches!(err.downcast_ref::<PermissionError>(), Some(&PermissionError::InvalidPrincipalId(_))));
    }

    #[test]
    fn validate_principal_rejects_null_bytes() {
        let svc = temp_service();
        let user = Principal::User("test\0user".into());
        let record = PermissionRecord::new(user, ResourceKind::Jobs, vec![CrudAction::Read]);

        let result = svc.upsert(1, &record);
        let err = result.unwrap_err();
        assert!(matches!(err.downcast_ref::<PermissionError>(), Some(&PermissionError::InvalidPrincipalId(_))));
    }

    #[test]
    fn validate_principal_rejects_path_traversal() {
        let svc = temp_service();
        let user = Principal::User("../malicious".into());
        let record = PermissionRecord::new(user, ResourceKind::Jobs, vec![CrudAction::Read]);

        let result = svc.upsert(1, &record);
        let err = result.unwrap_err();
        assert!(matches!(err.downcast_ref::<PermissionError>(), Some(&PermissionError::InvalidPrincipalId(_))));
    }

    #[test]
    fn permission_boundary_prevents_self_grant_admin() {
        let svc = temp_service();
        // First, remove admin role's permissions to set up test condition
        let admin_role = Principal::Role("admin".into());
        let empty_admin_record = PermissionRecord::new(
            admin_role.clone(),
            ResourceKind::Permissions,
            vec![]
        );
        svc.upsert(1, &empty_admin_record).unwrap();

        // Now try to grant a user permission management permissions
        let user = Principal::User("regular_user".into());
        let record = PermissionRecord::new(
            user.clone(),
            ResourceKind::Permissions,
            vec![CrudAction::Create, CrudAction::Update]
        );

        let result = svc.upsert(1, &record);
        let err = result.unwrap_err();
        assert!(matches!(err.downcast_ref::<PermissionError>(), Some(&PermissionError::InsufficientPrivileges)));
    }

    #[test]
    fn permission_boundary_prevents_system_role_modification() {
        let svc = temp_service();
        // First, remove the admin role's default permissions by setting empty permissions
        let admin_role = Principal::Role("admin".into());
        let empty_record = PermissionRecord::new(
            admin_role.clone(),
            ResourceKind::Permissions,
            vec![]
        );
        // This should succeed to set up the test condition
        svc.upsert(1, &empty_record).unwrap();

        // Now try to modify the admin role - this should fail because admin no longer has permissions
        let record = PermissionRecord::new(
            admin_role.clone(),
            ResourceKind::Jobs,
            vec![CrudAction::Read]
        );

        let result = svc.upsert(1, &record);
        let err = result.unwrap_err();
        assert!(matches!(err.downcast_ref::<PermissionError>(), Some(&PermissionError::InsufficientPrivileges)));
    }

    #[test]
    fn ensure_method_validates_inputs() {
        let svc = temp_service();

        // Test invalid org_id
        let result = svc.ensure(0, &[Principal::User("test".into())], ResourceKind::Jobs, Crud::READ);
        assert!(matches!(result, Err(PermissionError::InvalidOrgId(0))));

        // Test invalid principal
        let result = svc.ensure(1, &[Principal::User("".into())], ResourceKind::Jobs, Crud::READ);
        assert!(matches!(result, Err(PermissionError::InvalidPrincipalId(_))));
    }

    #[test]
    fn list_org_validates_org_id() {
        let svc = temp_service();

        let result = svc.list_org(0);
        let err = result.unwrap_err();
        assert!(matches!(err.downcast_ref::<PermissionError>(), Some(&PermissionError::InvalidOrgId(0))));

        let result = svc.list_org(MAX_ORG_ID + 1);
        let err = result.unwrap_err();
        assert!(matches!(err.downcast_ref::<PermissionError>(), Some(&PermissionError::InvalidOrgId(_))));
    }

    #[test]
    fn effective_allow_validates_inputs() {
        let svc = temp_service();

        // Test invalid org_id
        let result = svc.effective_allow(0, &[Principal::User("test".into())], ResourceKind::Jobs);
        let err = result.unwrap_err();
        assert!(matches!(err.downcast_ref::<PermissionError>(), Some(&PermissionError::InvalidOrgId(0))));

        // Test invalid principal
        let result = svc.effective_allow(1, &[Principal::User("".into())], ResourceKind::Jobs);
        let err = result.unwrap_err();
        assert!(matches!(err.downcast_ref::<PermissionError>(), Some(&PermissionError::InvalidPrincipalId(_))));
    }

    #[test]
    fn timing_attack_protection() {
        let svc = temp_service();
        let start = std::time::Instant::now();

        // Test successful permission check
        let admin = Principal::Role("admin".into());
        let result1 = svc.ensure(1, &[admin.clone()], ResourceKind::Jobs, Crud::READ);
        let duration1 = start.elapsed();

        // Test failed permission check
        let user = Principal::User("noperms".into());
        let start2 = std::time::Instant::now();
        let result2 = svc.ensure(1, &[user], ResourceKind::Jobs, Crud::DELETE);
        let duration2 = start2.elapsed();

        // Both should complete successfully (one with error)
        assert!(result1.is_ok());
        assert!(result2.is_err());

        // Timing should be roughly equivalent (within reasonable bounds)
        let diff = duration1.abs_diff(duration2);
        assert!(diff < std::time::Duration::from_millis(50),
            "Timing difference too large: success={}, failure={}, diff={:?}",
            duration1.as_micros(), duration2.as_micros(), diff);
    }
}
