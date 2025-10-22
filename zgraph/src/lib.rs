pub mod error;
pub mod types;
pub mod schema;
pub mod index;
pub mod permissions;
pub mod graph;
pub mod cypher;
pub mod fjall_db;
pub mod query;

#[cfg(feature = "sparql")]
pub mod sparql;

// Re-export common types
pub use permissions::Permission;

use hashbrown::HashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::error::ZgResult;
use crate::graph::{EdgeId, EdgeStore, NodeRef};
use crate::index::{Metric, VectorIndexKind};
use crate::permissions::{PermissionManager, Subject};
use crate::schema::{TableSchema, VectorSpec};
use crate::types::{Row, RowId, Value};

#[derive(Default)]
pub struct Database {
    inner: RwLock<DbInner>,
}

#[derive(Default, Serialize, Deserialize)]
struct DbDump {
    tables: HashMap<String, TableDump>,
    edges: Vec<graph::EdgeDump>,
    acls: permissions::AclDump,
}

#[derive(Default)]
struct DbInner {
    tables: HashMap<String, Table>,
    edges: EdgeStore,
    perms: PermissionManager,
}

#[derive(Serialize, Deserialize)]
struct TableDump {
    schema: TableSchema,
    rows: Vec<(RowId, Row)>,
}

struct Table {
    schema: TableSchema,
    rows: HashMap<RowId, Row>,
    next_id: RowId,
    vector_index: Option<VectorIndexKind>,
}

impl Table {
    fn new(schema: TableSchema) -> Self {
        let vector_index = schema
            .vector
            .as_ref()
            .map(|vs| VectorIndexKind::new(vs.metric, vs.dims));
        Self {
            schema,
            rows: HashMap::new(),
            next_id: 1,
            vector_index,
        }
    }
}

impl Database {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_table(&self, name: &str, schema: TableSchema) -> ZgResult<()> {
        let mut inner = self.inner.write();
        if inner.tables.contains_key(name) {
            return Err(crate::error::ZgError::AlreadyExists(format!(
                "table '{}' exists",
                name
            )));
        }
        inner.tables.insert(name.to_string(), Table::new(schema));
        Ok(())
    }

    pub fn add_role(&self, user: &str, role: &str) {
        let mut inner = self.inner.write();
        inner.perms.add_role(user, role);
    }

    pub fn drop_table(&self, name: &str) -> ZgResult<()> {
        let mut inner = self.inner.write();
        inner.tables.remove(name);
        Ok(())
    }

    pub fn grant_table_permission(
        &self,
        table: &str,
        subject: Subject,
        perm: Permission,
    ) -> ZgResult<()> {
        let mut inner = self.inner.write();
        inner.perms.grant_table(table, subject, perm);
        Ok(())
    }

    pub fn grant_row_permission(
        &self,
        table: &str,
        row_id: RowId,
        subject: Subject,
        perm: Permission,
    ) -> ZgResult<()> {
        let mut inner = self.inner.write();
        inner.perms.grant_row(table, row_id, subject, perm);
        Ok(())
    }

    pub fn insert_row(&self, user: &str, table: &str, mut row: Row) -> ZgResult<RowId> {
        let mut inner = self.inner.write();
        let t = inner
            .tables
            .get_mut(table)
            .ok_or_else(|| crate::error::ZgError::NotFound(format!("table '{}'", table)))?;
        if !inner
            .perms
            .check(user, table, None, Permission::Write)
        {
            return Err(crate::error::ZgError::Forbidden("write denied".into()));
        }
        let id = t.next_id;
        t.next_id += 1;
        // If there's a vector column declared, index it when present.
        if let Some(vs) = &t.schema.vector {
            if let Some(Value::Vector(vec)) = row.get(&vs.column) {
                if let Some(idx) = &mut t.vector_index {
                    idx.upsert(id, vec.as_slice());
                }
            }
        }
        t.rows.insert(id, row);
        Ok(id)
    }

    pub fn get_row(&self, user: &str, table: &str, id: RowId) -> ZgResult<Option<Row>> {
        let inner = self.inner.read();
        let t = match inner.tables.get(table) {
            Some(t) => t,
            None => return Ok(None),
        };
        if !inner.perms.check(user, table, Some(id), Permission::Read) {
            return Ok(None);
        }
        Ok(t.rows.get(&id).cloned())
    }

    pub fn select_where<F>(&self, user: &str, table: &str, mut pred: F) -> ZgResult<Vec<(RowId, Row)>>
    where
        F: FnMut(&Row) -> bool,
    {
        let inner = self.inner.read();
        let t = inner
            .tables
            .get(table)
            .ok_or_else(|| crate::error::ZgError::NotFound(format!("table '{}'", table)))?;
        let mut out = Vec::new();
        for (id, r) in &t.rows {
            if !inner
                .perms
                .check(user, table, Some(*id), Permission::Read)
            {
                continue;
            }
            if pred(r) {
                out.push((*id, r.clone()));
            }
        }
        Ok(out)
    }

    pub fn knn(
        &self,
        user: &str,
        table: &str,
        query: &[f32],
        k: usize,
    ) -> ZgResult<Vec<(RowId, f32)>> {
        let inner = self.inner.read();
        let t = inner
            .tables
            .get(table)
            .ok_or_else(|| crate::error::ZgError::NotFound(format!("table '{}'", table)))?;
        if !inner.perms.check(user, table, None, Permission::Read) {
            return Err(crate::error::ZgError::Forbidden("read denied".into()));
        }
        let idx = t
            .vector_index
            .as_ref()
            .ok_or_else(|| crate::error::ZgError::Invalid("no vector index on table".into()))?;
        let mut res = idx.knn(query, k);
        // Filter by row-level read permission.
        res.retain(|(rid, _)| inner.perms.check(user, table, Some(*rid), Permission::Read));
        Ok(res)
    }

    pub fn add_edge(
        &self,
        user: &str,
        from: NodeRef,
        rel_type: &str,
        to: NodeRef,
        props: HashMap<String, Value>,
    ) -> ZgResult<EdgeId> {
        let mut inner = self.inner.write();
        // Require write on the edges store and read on endpoints.
        if !inner.perms.check(user, &from.table, Some(from.id), Permission::Read)
            || !inner.perms.check(user, &to.table, Some(to.id), Permission::Read)
        {
            return Err(crate::error::ZgError::Forbidden(
                "read denied on endpoint".into(),
            ));
        }
        inner
            .perms
            .ensure_edge_store_initialized();
        if !inner
            .perms
            .check(user, graph::EDGE_TABLE_NAME, None, Permission::Write)
        {
            return Err(crate::error::ZgError::Forbidden("write denied on edges".into()));
        }
        Ok(inner.edges.add_edge(from, rel_type.to_string(), to, props))
    }

    pub fn neighbors(
        &self,
        user: &str,
        node: &NodeRef,
        rel_type: Option<&str>,
    ) -> ZgResult<Vec<(EdgeId, NodeRef)>> {
        let inner = self.inner.read();
        if !inner
            .perms
            .check(user, &node.table, Some(node.id), Permission::Read)
        {
            return Err(crate::error::ZgError::Forbidden("read denied".into()));
        }
        let res = inner.edges.neighbors(node, rel_type);
        Ok(res)
    }

    pub fn save_to_path(&self, path: &str) -> ZgResult<()> {
        let inner = self.inner.read();
        let mut dump = DbDump::default();
        // tables
        for (name, t) in &inner.tables {
            let mut rows = Vec::with_capacity(t.rows.len());
            for (id, r) in &t.rows {
                rows.push((*id, r.clone()));
            }
            dump.tables.insert(
                name.clone(),
                TableDump {
                    schema: t.schema.clone(),
                    rows,
                },
            );
        }
        // edges
        dump.edges = inner.edges.dump();
        // acls
        dump.acls = inner.perms.dump();
        let s = serde_json::to_string_pretty(&dump).unwrap();
        std::fs::write(path, s)?;
        Ok(())
    }

    pub fn load_from_path(path: &str) -> ZgResult<Self> {
        let data = std::fs::read_to_string(path)?;
        let dump: DbDump = serde_json::from_str(&data)?;
        let mut inner = DbInner {
            tables: HashMap::new(),
            edges: EdgeStore::default(),
            perms: PermissionManager::default(),
        };

        // restore tables and rebuild vector indices
        for (name, td) in dump.tables.into_iter() {
            let mut t = Table::new(td.schema);
            for (id, row) in td.rows.into_iter() {
                if let Some(vs) = &t.schema.vector {
                    if let Some(Value::Vector(vec)) = row.get(&vs.column) {
                        if let Some(idx) = &mut t.vector_index {
                            idx.upsert(id, vec.as_slice());
                        }
                    }
                }
                t.rows.insert(id, row);
                t.next_id = t.next_id.max(id + 1);
            }
            inner.tables.insert(name, t);
        }
        // restore edges
        inner.edges.restore(dump.edges);
        // restore acls
        inner.perms.restore(dump.acls);
        Ok(Self {
            inner: RwLock::new(inner),
        })
    }

    /// Execute a SQL query using the integrated zexec-engine
    pub fn execute_sql(&self, sql: &str) -> crate::error::ZgResult<crate::query::QueryResult> {
        use crate::query::QueryExecutor;
        <Self as QueryExecutor>::execute_sql(self, sql)
    }

    /// Execute a SQL query as a specific user (for permission checking)
    pub fn execute_sql_as_user(&self, sql: &str, user: &str) -> crate::error::ZgResult<crate::query::QueryResult> {
        use crate::query::QueryExecutor;
        <Self as QueryExecutor>::execute_sql_as_user(self, sql, user)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_knn() {
        let db = Database::new();
        let schema = TableSchema::new("docs").with_vector("embedding", Some(3), Metric::L2);
        db.create_table("docs", schema).unwrap();

        // By default, table is open (no ACLs) so inserts/reads allowed
        let mut r1 = Row::default();
        r1.insert("title".into(), Value::Text("a".into()));
        r1.insert("embedding".into(), Value::Vector(vec![0.0, 0.0, 0.0]));
        let mut r2 = Row::default();
        r2.insert("title".into(), Value::Text("b".into()));
        r2.insert("embedding".into(), Value::Vector(vec![1.0, 0.0, 0.0]));
        let mut r3 = Row::default();
        r3.insert("title".into(), Value::Text("c".into()));
        r3.insert("embedding".into(), Value::Vector(vec![0.0, 1.0, 0.0]));

        let id1 = db.insert_row("admin", "docs", r1).unwrap();
        let id2 = db.insert_row("admin", "docs", r2).unwrap();
        let id3 = db.insert_row("admin", "docs", r3).unwrap();
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);

        let res = db.knn("admin", "docs", &[0.2, 0.0, 0.0], 2).unwrap();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].0, 2);
    }
}
