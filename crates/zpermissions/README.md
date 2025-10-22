# zpermissions

A comprehensive role-based access control (RBAC) and authorization library for ZRUSTDB, providing fine-grained permission management across all system resources.

## Overview

The `zpermissions` crate implements a flexible, storage-backed permission system that enables secure access control for multi-tenant database operations. It provides CRUD-level permissions for various resource types with support for multiple principal types including users, roles, groups, labels, teams, and team roles.

## Features

- **Fine-grained CRUD Permissions**: Read, Create, Update, Delete access control
- **Multiple Principal Types**: Users, Roles, Groups, Labels, Teams, and Team Roles
- **Comprehensive Resource Coverage**: Collections, Tables, Indexes, Jobs, Files, Permissions
- **Hierarchical Authorization**: Role-based defaults with explicit overrides
- **Organization-scoped Access**: Multi-tenant permission isolation
- **Storage-backed Persistence**: Uses zcore-storage for durable permission records
- **Efficient Binary Encoding**: Optimized storage keys and bitflag-based permissions
- **Default Role System**: Built-in admin, operator, reader, and team roles

## Core Types

### CRUD Permissions

```rust
use zpermissions::{Crud, CrudAction};

// Bitflag-based permissions for efficient storage and operations
let perms = Crud::READ | Crud::CREATE | Crud::UPDATE;

// Convert to/from action vectors
let actions = perms.actions(); // Vec<CrudAction>
let rebuilt = Crud::from_actions(&actions);
```

### Principal Types

```rust
use zpermissions::Principal;

// Different types of security principals
let user = Principal::User("alice".to_string());
let role = Principal::Role("admin".to_string());
let group = Principal::Group("engineers".to_string());
let label = Principal::Label("contractor".to_string());
let team = Principal::Team("backend-team".to_string());
let team_role = Principal::TeamRole {
    team_id: "backend-team".to_string(),
    role: "owner".to_string()
};
```

### Resource Types

```rust
use zpermissions::ResourceKind;

// All supported resource types
ResourceKind::Collections   // NoSQL collections
ResourceKind::Tables       // SQL tables
ResourceKind::Indexes      // Vector and SQL indexes
ResourceKind::Jobs         // Background jobs
ResourceKind::Files        // File storage
ResourceKind::Permissions  // Permission management
```

## Usage Examples

### Basic Permission Management

```rust
use zpermissions::{PermissionService, PermissionRecord, Principal, ResourceKind, CrudAction};
use zcore_storage::Store;

// Initialize service with storage backend
let store = Store::open("permissions.redb")?;
let perms = PermissionService::new(&store);
let org_id = 1;

// Grant user specific permissions
let user = Principal::User("alice".to_string());
let record = PermissionRecord::new(
    user.clone(),
    ResourceKind::Tables,
    vec![CrudAction::Read, CrudAction::Create]
);
perms.upsert(org_id, &record)?;

// Check effective permissions
let principals = vec![user];
let effective = perms.effective_allow(org_id, &principals, ResourceKind::Tables)?;
assert!(effective.contains(Crud::READ | Crud::CREATE));
```

### Authorization Checking

```rust
use zpermissions::{Crud, PermissionError};

// Ensure user has required permissions
let required = Crud::READ | Crud::UPDATE;
match perms.ensure(org_id, &principals, ResourceKind::Collections, required) {
    Ok(()) => {
        // User has permission, proceed with operation
        println!("Access granted");
    }
    Err(PermissionError::Forbidden { resource, missing }) => {
        // Missing permissions, deny access
        println!("Access denied: missing {:?} on {:?}", missing, resource);
    }
    Err(PermissionError::Storage(e)) => {
        // Storage error
        println!("Permission check failed: {}", e);
    }
}
```

### Role-based Permissions

```rust
// Default role permissions are automatically applied
let admin = Principal::Role("admin".to_string());
let reader = Principal::Role("reader".to_string());

// Admin gets full access to all resources
let admin_perms = perms.effective_allow(org_id, &[admin], ResourceKind::Tables)?;
assert_eq!(admin_perms, Crud::READ | Crud::CREATE | Crud::UPDATE | Crud::DELETE);

// Reader gets read-only access (except permissions)
let reader_perms = perms.effective_allow(org_id, &[reader], ResourceKind::Tables)?;
assert_eq!(reader_perms, Crud::READ);
```

### Multi-Principal Authorization

```rust
// Combine multiple principals for cumulative permissions
let user = Principal::User("bob".to_string());
let role = Principal::Role("operator".to_string());
let label = Principal::Label("trusted".to_string());

let combined_principals = vec![user, role, label];
let effective = perms.effective_allow(org_id, &combined_principals, ResourceKind::Jobs)?;

// Effective permissions are the union of all principal permissions
```

### Team-based Permissions

```rust
// Team roles provide hierarchical access within teams
let team_owner = Principal::TeamRole {
    team_id: "data-team".to_string(),
    role: "owner".to_string()
};

let team_member = Principal::TeamRole {
    team_id: "data-team".to_string(),
    role: "member".to_string()
};

// Owners get full access, members get read/create/update
let owner_perms = perms.effective_allow(org_id, &[team_owner], ResourceKind::Collections)?;
let member_perms = perms.effective_allow(org_id, &[team_member], ResourceKind::Collections)?;
```

### Organization-wide Permission Listing

```rust
// List all permissions for an organization
let all_permissions = perms.list_org(org_id)?;
for record in all_permissions {
    println!("Principal: {:?}, Resource: {:?}, Actions: {:?}",
             record.principal, record.resource, record.allow);
}

// Get effective permission map for a user
let permission_map = perms.effective_map(org_id, &principals)?;
for (resource, actions) in permission_map {
    println!("Resource: {:?}, Allowed: {:?}", resource, actions);
}
```

## Default Permission Model

### Built-in Roles

- **admin**: Full access (CRUD) to all resources
- **operator**: Read/Create/Update access to most resources, limited permission management
- **reader**: Read-only access to all resources except permissions

### Team Roles

- **owner**: Full access within team scope
- **admin**: Full access except delete permissions on most resources
- **member**: Read/Create/Update access, no permission management
- **viewer**: Read-only access, no permission management

### Resource-specific Defaults

Some roles have resource-specific permission patterns:
- **operator** cannot delete most resources but can manage jobs
- **reader** has no access to permission management
- Team roles respect resource boundaries and permission hierarchies

## Storage Architecture

### Backend Storage
The permissions system uses **zcore-storage** which has been migrated from **redb** to **fjall** LSM-tree for improved performance and scalability. The zcore-storage crate provides redb-style compatibility wrappers to maintain API compatibility while leveraging fjall's advanced features.

### Key Encoding
Permissions are stored using composite binary keys in fjall partitions:
```
[org_id(8)] + [principal_tag(1)] + [principal_id(variable)] + [resource_tag(1)]
```

### Value Encoding
Permission values are stored as 8-byte big-endian integers representing bitflags, optimized for fjall's LSM-tree structure.

### Storage Collections
- **Partition**: `permissions` - Dedicated fjall partition for permission records
- **MVCC Support**: Multi-version concurrency control for concurrent access
- **ACID Transactions**: Full transaction support with optimistic concurrency control
- **Range Queries**: Efficient prefix scans for organization-wide permission listing

### Performance Benefits
- **LSM-tree Architecture**: Excellent write performance and compression
- **Multi-writer Support**: Unlimited concurrent writers with MVCC
- **Optimized Queries**: Single key lookups and batched operations
- **Scalable Storage**: Handles high-volume permission data efficiently

## Integration with ZRUSTDB

The zpermissions crate is tightly integrated with the ZRUSTDB server:

### HTTP API Integration
Every protected endpoint checks permissions using `ensure_permission()`:

```rust
let principals = principals_for(&auth_session);
ensure_permission(
    state.permissions,
    auth.org_id,
    &principals,
    ResourceKind::Tables,
    Crud::CREATE,
)?;
```

### Principal Resolution
The server automatically resolves user sessions to principals:
- User ID from authentication
- Assigned roles and labels
- Team memberships and team roles

### Permission API Endpoints
- `GET /v1/permissions` - List effective permissions
- `POST /v1/permissions` - Create/update permission records

## Error Handling

```rust
use zpermissions::PermissionError;

match permission_result {
    Err(PermissionError::Forbidden { resource, missing }) => {
        // Handle access denied
        return Err(axum::http::StatusCode::FORBIDDEN);
    }
    Err(PermissionError::Storage(e)) => {
        // Handle storage error
        tracing::error!("Permission storage error: {}", e);
        return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    }
    Ok(()) => {
        // Permission granted, continue
    }
}
```

## Performance Considerations

- **Bitflag Operations**: CRUD permissions use efficient bitwise operations
- **Cached Lookups**: Permission checks are single storage lookups
- **Batch Processing**: Multiple principals resolved in single operation
- **Compact Storage**: Binary-encoded keys and values minimize storage overhead
- **Prefix Scanning**: Efficient organization-wide permission enumeration

## Testing

The crate includes comprehensive tests covering:
- Default role behavior
- Permission override functionality
- Multi-principal accumulation
- Storage key encoding/decoding
- Error handling scenarios

Run tests with:
```bash
cargo test
```

## Dependencies

- `anyhow` - Error handling
- `bitflags` - Efficient permission flags
- `serde` - Serialization support
- `thiserror` - Structured error types
- `zcore-storage` - Persistent storage backend

## License

Licensed under MIT OR Apache-2.0