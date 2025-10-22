use hashbrown::{HashMap, HashSet};
use serde::{Deserialize, Serialize};

use crate::types::RowId;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Permission {
    Read,
    Write,
    Delete,
    Admin,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Subject {
    User(String),
    Role(String),
}

#[derive(Debug)]
pub struct PermissionManager {
    user_roles: HashMap<String, HashSet<String>>, // user -> roles
    table_acls: HashMap<String, Vec<AclEntry>>,   // table -> grants
    row_acls: HashMap<(String, RowId), Vec<AclEntry>>, // (table,row) -> grants
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AclEntry {
    pub subject: Subject,
    pub perm: Permission,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct AclDump {
    pub user_roles: Vec<(String, Vec<String>)>,
    pub table_acls: Vec<(String, Vec<AclEntry>)>,
    pub row_acls: Vec<((String, RowId), Vec<AclEntry>)>,
}

impl Default for PermissionManager {
    fn default() -> Self {
        let mut pm = Self {
            user_roles: HashMap::new(),
            table_acls: HashMap::new(),
            row_acls: HashMap::new(),
        };
        // By default, edge store will be created with open perms via ensure_edge_store_initialized.
        pm
    }
}

impl PermissionManager {
    pub fn add_role(&mut self, user: &str, role: &str) {
        self.user_roles
            .entry(user.to_string())
            .or_default()
            .insert(role.to_string());
    }

    pub fn grant_table(&mut self, table: &str, subject: Subject, perm: Permission) {
        self.table_acls
            .entry(table.to_string())
            .or_default()
            .push(AclEntry { subject, perm });
    }

    pub fn grant_row(&mut self, table: &str, row: RowId, subject: Subject, perm: Permission) {
        self.row_acls
            .entry((table.to_string(), row))
            .or_default()
            .push(AclEntry { subject, perm });
    }

    pub fn check(&self, user: &str, table: &str, row: Option<RowId>, perm: Permission) -> bool {
        // If the table has no ACL entries at all, treat it as open (allow).
        if self.is_open(table) {
            return true;
        }
        // Admin implies everything
        if self.has_grant(user, table, row, &Permission::Admin) {
            return true;
        }
        self.has_grant(user, table, row, &perm)
    }

    pub fn is_open(&self, table: &str) -> bool {
        !self.table_acls.contains_key(table)
            && !self
                .row_acls
                .keys()
                .any(|(t, _)| t.as_str() == table)
    }

    fn has_grant(&self, user: &str, table: &str, row: Option<RowId>, perm: &Permission) -> bool {
        // Row-level grants first
        if let Some(rid) = row {
            if let Some(entries) = self.row_acls.get(&(table.to_string(), rid)) {
                if entries.iter().any(|e| self.entry_applies(user, e, perm)) {
                    return true;
                }
            }
        }
        // Table-level grants
        if let Some(entries) = self.table_acls.get(table) {
            if entries.iter().any(|e| self.entry_applies(user, e, perm)) {
                return true;
            }
        }
        false
    }

    fn entry_applies(&self, user: &str, entry: &AclEntry, perm: &Permission) -> bool {
        if &entry.perm != perm {
            return false;
        }
        match &entry.subject {
            Subject::User(u) => u == user,
            Subject::Role(r) => self
                .user_roles
                .get(user)
                .map(|rs| rs.contains(r))
                .unwrap_or(false),
        }
    }

    pub fn dump(&self) -> AclDump {
        AclDump {
            user_roles: self
                .user_roles
                .iter()
                .map(|(u, rs)| (u.clone(), rs.iter().cloned().collect()))
                .collect(),
            table_acls: self
                .table_acls
                .iter()
                .map(|(t, es)| (t.clone(), es.clone()))
                .collect(),
            row_acls: self
                .row_acls
                .iter()
                .map(|(k, es)| (k.clone(), es.clone()))
                .collect(),
        }
    }

    pub fn restore(&mut self, dump: AclDump) {
        self.user_roles.clear();
        self.table_acls.clear();
        self.row_acls.clear();
        for (u, rs) in dump.user_roles {
            self.user_roles
                .entry(u)
                .or_default()
                .extend(rs.into_iter());
        }
        for (t, es) in dump.table_acls {
            self.table_acls.insert(t, es);
        }
        for (k, es) in dump.row_acls {
            self.row_acls.insert(k, es);
        }
    }

    pub fn ensure_edge_store_initialized(&mut self) {
        // Provide a default permissive ACL for the edges pseudo-table if none present
        if !self.table_acls.contains_key(crate::graph::EDGE_TABLE_NAME) {
            self.grant_table(
                crate::graph::EDGE_TABLE_NAME,
                Subject::Role("admin".into()),
                Permission::Admin,
            );
            self.grant_table(
                crate::graph::EDGE_TABLE_NAME,
                Subject::Role("writer".into()),
                Permission::Write,
            );
            self.grant_table(
                crate::graph::EDGE_TABLE_NAME,
                Subject::Role("reader".into()),
                Permission::Read,
            );
        }
    }
}
