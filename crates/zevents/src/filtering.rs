//! Advanced event filtering capabilities
//!
//! This module provides sophisticated filtering mechanisms for database change events,
//! allowing for targeted subscriptions based on table, operation, user, and organization
//! scope with support for permission-based filtering.

use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use crate::{Event, EventType, EventData};
use anyhow::Result;

/// Advanced event filter with multiple criteria
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventFilter {
    /// Filter by table names (empty = all tables)
    pub tables: Option<HashSet<String>>,
    /// Filter by operation types (INSERT, UPDATE, DELETE, etc.)
    pub operations: Option<HashSet<String>>,
    /// Filter by user IDs (empty = all users)
    pub users: Option<HashSet<u64>>,
    /// Filter by organization IDs (empty = all organizations)
    pub organizations: Option<HashSet<u64>>,
    /// Filter by event types
    pub event_types: Option<HashSet<EventType>>,
    /// Filter by connection IDs
    pub connections: Option<HashSet<String>>,
    /// Filter by transaction IDs
    pub transactions: Option<HashSet<String>>,
    /// Custom attribute filters (key-value pairs)
    pub attributes: Option<HashMap<String, String>>,
    /// Include only events where user has specific permissions
    pub required_permissions: Option<Vec<String>>,
}

impl EventFilter {
    pub fn new() -> Self {
        Self {
            tables: None,
            operations: None,
            users: None,
            organizations: None,
            event_types: None,
            connections: None,
            transactions: None,
            attributes: None,
            required_permissions: None,
        }
    }

    /// Filter for specific tables
    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.tables = Some(tables.into_iter().collect());
        self
    }

    /// Filter for specific operations
    pub fn with_operations(mut self, operations: Vec<String>) -> Self {
        self.operations = Some(operations.into_iter().collect());
        self
    }

    /// Filter for specific users
    pub fn with_users(mut self, users: Vec<u64>) -> Self {
        self.users = Some(users.into_iter().collect());
        self
    }

    /// Filter for specific organizations
    pub fn with_organizations(mut self, organizations: Vec<u64>) -> Self {
        self.organizations = Some(organizations.into_iter().collect());
        self
    }

    /// Filter for specific event types
    pub fn with_event_types(mut self, event_types: Vec<EventType>) -> Self {
        self.event_types = Some(event_types.into_iter().collect());
        self
    }

    /// Filter for specific connections
    pub fn with_connections(mut self, connections: Vec<String>) -> Self {
        self.connections = Some(connections.into_iter().collect());
        self
    }

    /// Filter for specific transactions
    pub fn with_transactions(mut self, transactions: Vec<String>) -> Self {
        self.transactions = Some(transactions.into_iter().collect());
        self
    }

    /// Filter by custom attributes
    pub fn with_attributes(mut self, attributes: HashMap<String, String>) -> Self {
        self.attributes = Some(attributes);
        self
    }

    /// Require specific permissions
    pub fn with_permissions(mut self, permissions: Vec<String>) -> Self {
        self.required_permissions = Some(permissions);
        self
    }

    /// Check if an event matches this filter
    pub fn matches(&self, event: &Event) -> bool {
        // Filter by organization
        if let Some(ref orgs) = self.organizations {
            if !orgs.contains(&event.org_id) {
                return false;
            }
        }

        // Filter by event type
        if let Some(ref types) = self.event_types {
            if !types.contains(&event.event_type) {
                return false;
            }
        }

        // Filter by custom attributes
        if let Some(ref required_attrs) = self.attributes {
            for (key, value) in required_attrs {
                if !event.metadata.attributes.get(key).map_or(false, |v| v == value) {
                    return false;
                }
            }
        }

        // Event-specific filtering
        match &event.data {
            EventData::DatabaseChange {
                table_name,
                operation,
                user_id,
                connection_id,
                transaction_id,
                ..
            } => {
                // Filter by table
                if let Some(ref tables) = self.tables {
                    if !tables.contains(table_name) {
                        return false;
                    }
                }

                // Filter by operation
                if let Some(ref operations) = self.operations {
                    if !operations.contains(operation) {
                        return false;
                    }
                }

                // Filter by user
                if let Some(ref users) = self.users {
                    match user_id {
                        Some(uid) => {
                            if !users.contains(uid) {
                                return false;
                            }
                        }
                        None => return false, // No user ID but filter requires specific users
                    }
                }

                // Filter by connection
                if let Some(ref connections) = self.connections {
                    match connection_id {
                        Some(cid) => {
                            if !connections.contains(cid) {
                                return false;
                            }
                        }
                        None => return false, // No connection ID but filter requires specific connections
                    }
                }

                // Filter by transaction
                if let Some(ref transactions) = self.transactions {
                    match transaction_id {
                        Some(tid) => {
                            if !transactions.contains(tid) {
                                return false;
                            }
                        }
                        None => return false, // No transaction ID but filter requires specific transactions
                    }
                }
            }
            EventData::User { user_id, .. } => {
                if let Some(ref users) = self.users {
                    match user_id {
                        Some(uid) => {
                            if !users.contains(uid) {
                                return false;
                            }
                        }
                        None => return false,
                    }
                }
            }
            EventData::Connection { connection_id, user_id, .. } => {
                if let Some(ref connections) = self.connections {
                    if !connections.contains(connection_id) {
                        return false;
                    }
                }

                if let Some(ref users) = self.users {
                    match user_id {
                        Some(uid) => {
                            if !users.contains(uid) {
                                return false;
                            }
                        }
                        None => return false,
                    }
                }
            }
            EventData::Transaction { transaction_id, .. } => {
                if let Some(ref transactions) = self.transactions {
                    if !transactions.contains(transaction_id) {
                        return false;
                    }
                }
            }
            _ => {
                // For other event types, only apply general filters (org, event_type, attributes)
                // which are already checked above
            }
        }

        true
    }

    /// Create a filter for database changes only
    pub fn database_changes() -> Self {
        Self::new().with_event_types(vec![
            EventType::RowInserted,
            EventType::RowUpdated,
            EventType::RowDeleted,
            EventType::SchemaChanged,
        ])
    }

    /// Create a filter for user activity only
    pub fn user_activity() -> Self {
        Self::new().with_event_types(vec![
            EventType::UserCreated,
            EventType::UserUpdated,
            EventType::UserDeleted,
            EventType::UserLoggedIn,
            EventType::UserLoggedOut,
        ])
    }

    /// Create a filter for system events only
    pub fn system_events() -> Self {
        Self::new().with_event_types(vec![
            EventType::SystemStarted,
            EventType::SystemStopped,
            EventType::ConfigurationChanged,
            EventType::HealthCheckFailed,
            EventType::MemoryPressure,
            EventType::DiskSpaceWarning,
        ])
    }
}

impl Default for EventFilter {
    fn default() -> Self {
        Self::new()
    }
}

/// Permission-based event filter
#[derive(Debug, Clone)]
pub struct PermissionFilter {
    user_id: u64,
    permissions: HashSet<String>,
    organization_access: HashSet<u64>,
    table_access: HashMap<u64, HashSet<String>>, // org_id -> table names
}

impl PermissionFilter {
    pub fn new(
        user_id: u64,
        permissions: Vec<String>,
        organization_access: Vec<u64>,
        table_access: HashMap<u64, Vec<String>>,
    ) -> Self {
        Self {
            user_id,
            permissions: permissions.into_iter().collect(),
            organization_access: organization_access.into_iter().collect(),
            table_access: table_access
                .into_iter()
                .map(|(org, tables)| (org, tables.into_iter().collect()))
                .collect(),
        }
    }

    /// Check if user can see this event based on permissions
    pub fn can_access(&self, event: &Event) -> bool {
        // Check organization access
        if !self.organization_access.contains(&event.org_id) {
            return false;
        }

        // Check event-specific permissions
        match &event.data {
            EventData::DatabaseChange { table_name, .. } => {
                // Check table-level permissions
                if let Some(allowed_tables) = self.table_access.get(&event.org_id) {
                    if !allowed_tables.contains(table_name) {
                        return false;
                    }
                }

                // Check operation permissions (simplified - could be more granular)
                if !self.permissions.contains("read") && !self.permissions.contains("admin") {
                    return false;
                }
            }
            EventData::User { user_id: Some(uid), .. } => {
                // Users can see their own events, admins can see all
                if *uid != self.user_id && !self.permissions.contains("admin") {
                    return false;
                }
            }
            EventData::System { .. } => {
                // Only admins and operators can see system events
                if !self.permissions.contains("admin") && !self.permissions.contains("operator") {
                    return false;
                }
            }
            _ => {
                // Default: require at least read permission
                if !self.permissions.contains("read") && !self.permissions.contains("admin") {
                    return false;
                }
            }
        }

        true
    }
}

/// Composite filter that combines multiple filtering strategies
pub struct CompositeFilter {
    event_filter: Option<EventFilter>,
    permission_filter: Option<PermissionFilter>,
    custom_predicates: Vec<Box<dyn Fn(&Event) -> bool + Send + Sync>>,
}

impl std::fmt::Debug for CompositeFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeFilter")
            .field("event_filter", &self.event_filter)
            .field("permission_filter", &self.permission_filter)
            .field("custom_predicates", &format!("{} predicates", self.custom_predicates.len()))
            .finish()
    }
}

impl Clone for CompositeFilter {
    fn clone(&self) -> Self {
        Self {
            event_filter: self.event_filter.clone(),
            permission_filter: self.permission_filter.clone(),
            custom_predicates: Vec::new(), // Cannot clone closures, so we create empty vec
        }
    }
}

impl CompositeFilter {
    pub fn new() -> Self {
        Self {
            event_filter: None,
            permission_filter: None,
            custom_predicates: Vec::new(),
        }
    }

    pub fn with_event_filter(mut self, filter: EventFilter) -> Self {
        self.event_filter = Some(filter);
        self
    }

    pub fn with_permission_filter(mut self, filter: PermissionFilter) -> Self {
        self.permission_filter = Some(filter);
        self
    }

    pub fn matches(&self, event: &Event) -> bool {
        // Apply event filter
        if let Some(ref filter) = self.event_filter {
            if !filter.matches(event) {
                return false;
            }
        }

        // Apply permission filter
        if let Some(ref filter) = self.permission_filter {
            if !filter.can_access(event) {
                return false;
            }
        }

        // Apply custom predicates
        for predicate in &self.custom_predicates {
            if !predicate(event) {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Event, EventData, EventMetadata};
    use chrono::Utc;
    use std::collections::HashMap;

    #[test]
    fn test_basic_event_filtering() {
        let filter = EventFilter::new()
            .with_tables(vec!["users".to_string()])
            .with_operations(vec!["INSERT".to_string()]);

        let event = Event::new(
            EventType::RowInserted,
            EventData::DatabaseChange {
                org_id: 1,
                table_name: "users".to_string(),
                operation: "INSERT".to_string(),
                transaction_id: None,
                user_id: None,
                connection_id: None,
                old_values: None,
                new_values: None,
                where_clause: None,
                rows_affected: 1,
                schema_changes: None,
                timestamp_ms: Utc::now().timestamp_millis(),
            },
            1,
        );

        assert!(filter.matches(&event));

        let non_matching_event = Event::new(
            EventType::RowUpdated,
            EventData::DatabaseChange {
                org_id: 1,
                table_name: "products".to_string(),
                operation: "UPDATE".to_string(),
                transaction_id: None,
                user_id: None,
                connection_id: None,
                old_values: None,
                new_values: None,
                where_clause: None,
                rows_affected: 1,
                schema_changes: None,
                timestamp_ms: Utc::now().timestamp_millis(),
            },
            1,
        );

        assert!(!filter.matches(&non_matching_event));
    }

    #[test]
    fn test_permission_filtering() {
        let perm_filter = PermissionFilter::new(
            100,
            vec!["read".to_string()],
            vec![1],
            [(1, vec!["users".to_string()])].into_iter().collect(),
        );

        let allowed_event = Event::new(
            EventType::RowInserted,
            EventData::DatabaseChange {
                org_id: 1,
                table_name: "users".to_string(),
                operation: "INSERT".to_string(),
                transaction_id: None,
                user_id: None,
                connection_id: None,
                old_values: None,
                new_values: None,
                where_clause: None,
                rows_affected: 1,
                schema_changes: None,
                timestamp_ms: Utc::now().timestamp_millis(),
            },
            1,
        );

        assert!(perm_filter.can_access(&allowed_event));

        let denied_event = Event::new(
            EventType::RowInserted,
            EventData::DatabaseChange {
                org_id: 1,
                table_name: "admin_logs".to_string(),
                operation: "INSERT".to_string(),
                transaction_id: None,
                user_id: None,
                connection_id: None,
                old_values: None,
                new_values: None,
                where_clause: None,
                rows_affected: 1,
                schema_changes: None,
                timestamp_ms: Utc::now().timestamp_millis(),
            },
            1,
        );

        assert!(!perm_filter.can_access(&denied_event));
    }

    #[test]
    fn test_predefined_filters() {
        let db_filter = EventFilter::database_changes();
        assert!(db_filter.event_types.as_ref().unwrap().contains(&EventType::RowInserted));
        assert!(!db_filter.event_types.as_ref().unwrap().contains(&EventType::UserLoggedIn));

        let user_filter = EventFilter::user_activity();
        assert!(user_filter.event_types.as_ref().unwrap().contains(&EventType::UserLoggedIn));
        assert!(!user_filter.event_types.as_ref().unwrap().contains(&EventType::RowInserted));
    }
}