use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::error::ZgResult;
use crate::graph::{EdgeId, EdgeStore, NodeRef};
use crate::permissions::{Permission, PermissionManager, Subject};
use crate::schema::TableSchema;
use crate::types::{Row, RowId, Value};

use zcore_catalog as zcat;
use zcore_storage as zs;

#[derive(Debug)]
pub struct FjDatabase {
    pub(crate) store: zs::Store,
    pub(crate) org_id: u64,
    pub(crate) edges: RwLock<EdgeStore>,
    pub(crate) perms: RwLock<PermissionManager>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Cell {
    Int(i64),
    Float(f64),
    Text(String),
    Vector(Vec<f32>),
}

impl FjDatabase {
    pub fn open(path: &str, org_id: u64) -> ZgResult<Self> {
        let store = zs::Store::open(path)?;
        Ok(Self {
            store,
            org_id,
            edges: RwLock::new(EdgeStore::default()),
            perms: RwLock::new(PermissionManager::default()),
        })
    }

    pub fn create_table(&self, name: &str, schema: TableSchema) -> ZgResult<()> {
        let catalog = zcat::Catalog::new(&self.store);
        let mut cols = Vec::new();
        cols.push(zcat::ColumnDef::primary_key("id".to_string(), zcat::ColumnType::Integer));
        if let Some(vs) = schema.vector {
            cols.push(zcat::ColumnDef::not_null(vs.column, zcat::ColumnType::Vector));
        }
        let def = zcat::TableDef::new(name.to_string(), cols);
        catalog.create_table(self.org_id, &def)?;
        Ok(())
    }

    pub fn drop_table(&self, name: &str) -> ZgResult<()> {
        let catalog = zcat::Catalog::new(&self.store);
        catalog.drop_table(self.org_id, name)?;
        Ok(())
    }

    pub fn grant_table_permission(&self, table: &str, subject: Subject, perm: Permission) -> ZgResult<()> {
        let mut p = self.perms.write();
        p.grant_table(table, subject, perm);
        Ok(())
    }
    pub fn grant_row_permission(&self, table: &str, row_id: RowId, subject: Subject, perm: Permission) -> ZgResult<()> {
        let mut p = self.perms.write();
        p.grant_row(table, row_id, subject, perm);
        Ok(())
    }
    pub fn add_role(&self, user: &str, role: &str) { self.perms.write().add_role(user, role); }

    pub fn insert_row(&self, user: &str, table: &str, row: Row) -> ZgResult<RowId> {
        let p = self.perms.read();
        if !p.check(user, table, None, Permission::Write) { return Err(crate::error::ZgError::Forbidden("write denied".into())); }
        drop(p);
        let catalog = zcat::Catalog::new(&self.store);
        let tdef = catalog
            .get_table(self.org_id, table)?
            .ok_or_else(|| crate::error::ZgError::NotFound(format!("table '{}'", table)))?;
        let cells = self.row_to_cells(&tdef, &row)?;
        let mut w = zs::WriteTransaction::new();
        let row_id = zs::next_seq(&self.store, table.as_bytes())?;
        let key = zs::encode_key(&[b"rows", &self.org_id.to_be_bytes(), table.as_bytes(), &row_id.to_be_bytes()]);
        let bytes = bincode::serialize(&cells).unwrap();
        w.set(zs::COL_ROWS, key, bytes);
        w.commit(&self.store)?;
        Ok(row_id)
    }

    pub fn get_row(&self, user: &str, table: &str, id: RowId) -> ZgResult<Option<Row>> {
        let p = self.perms.read();
        if !p.check(user, table, Some(id), Permission::Read) { return Ok(None); }
        drop(p);
        let catalog = zcat::Catalog::new(&self.store);
        let tdef = match catalog.get_table(self.org_id, table)? { Some(t) => t, None => return Ok(None) };
        let key = zs::encode_key(&[b"rows", &self.org_id.to_be_bytes(), table.as_bytes(), &id.to_be_bytes()]);
        let tx = zs::ReadTransaction::new();
        if let Some(bytes) = tx.get(&self.store, zs::COL_ROWS, &key)? {
            let cells: Vec<Cell> = bincode::deserialize(&bytes).unwrap_or_default();
            let row = self.cells_to_row(&tdef, &cells);
            return Ok(Some(row));
        }
        Ok(None)
    }

    pub fn select_where<F>(&self, user: &str, table: &str, mut pred: F) -> ZgResult<Vec<(RowId, Row)>>
    where F: FnMut(&Row) -> bool {
        let catalog = zcat::Catalog::new(&self.store);
        let tdef = catalog.get_table(self.org_id, table)?.ok_or_else(|| crate::error::ZgError::NotFound(format!("table '{}'", table)))?;
        let tx = zs::ReadTransaction::new();
        let prefix = zs::encode_key(&[b"rows", &self.org_id.to_be_bytes(), table.as_bytes()]);
        let items = tx.scan_prefix(&self.store, zs::COL_ROWS, &prefix)?;
        let p = self.perms.read();
        let mut out = Vec::new();
        for (key, bytes) in items {
            let rid = parse_row_id_from_key(&key);
            if !p.check(user, table, Some(rid), Permission::Read) { continue; }
            let cells: Vec<Cell> = bincode::deserialize(&bytes).unwrap_or_default();
            let row = self.cells_to_row(&tdef, &cells);
            if pred(&row) { out.push((rid, row)); }
        }
        Ok(out)
    }

    pub fn knn(&self, user: &str, table: &str, query: &[f32], k: usize) -> ZgResult<Vec<(RowId, f32)>> {
        let p = self.perms.read();
        if !p.check(user, table, None, Permission::Read) { return Err(crate::error::ZgError::Forbidden("read denied".into())); }
        drop(p);
        let catalog = zcat::Catalog::new(&self.store);
        let tdef = catalog.get_table(self.org_id, table)?.ok_or_else(|| crate::error::ZgError::NotFound(format!("table '{}'", table)))?;
        let vcol = tdef.columns.iter().find(|c| matches!(c.ty, zcat::ColumnType::Vector)).ok_or_else(|| crate::error::ZgError::Invalid("no vector column".into()))?.name.clone();
        let res = zann_hnsw::search(&self.store, &catalog, self.org_id, table, &vcol, query, k, 64)
            .map_err(|e| crate::error::ZgError::Invalid(format!("knn error: {}", e)))?;
        let p = self.perms.read();
        let out: Vec<(RowId, f32)> = res.into_iter().filter(|(rid, _)| p.check(user, table, Some(*rid), Permission::Read)).collect();
        Ok(out)
    }

    pub fn add_edge(&self, user: &str, from: NodeRef, rel_type: &str, to: NodeRef, props: std::collections::HashMap<String, Value>) -> ZgResult<EdgeId> {
        let p = self.perms.read();
        if !p.check(user, &from.table, Some(from.id), Permission::Read) || !p.check(user, &to.table, Some(to.id), Permission::Read) {
            return Err(crate::error::ZgError::Forbidden("read denied on endpoint".into()));
        }
        drop(p);
        let mut perms = self.perms.write();
        perms.ensure_edge_store_initialized();
        if !perms.check(user, crate::graph::EDGE_TABLE_NAME, None, Permission::Write) {
            return Err(crate::error::ZgError::Forbidden("write denied on edges".into()));
        }
        drop(perms);
        let mut es = self.edges.write();
        Ok(es.add_edge(from, rel_type.to_string(), to, props))
    }

    pub fn neighbors(&self, user: &str, node: &NodeRef, rel_type: Option<&str>) -> ZgResult<Vec<(EdgeId, NodeRef)>> {
        let p = self.perms.read();
        if !p.check(user, &node.table, Some(node.id), Permission::Read) { return Err(crate::error::ZgError::Forbidden("read denied".into())); }
        drop(p);
        let es = self.edges.read();
        Ok(es.neighbors(node, rel_type))
    }

    pub(crate) fn row_to_cells(&self, tdef: &zcat::TableDef, r: &Row) -> ZgResult<Vec<Cell>> {
        let mut cells = Vec::with_capacity(tdef.columns.len());
        for col in &tdef.columns {
            let c = match col.ty {
                zcat::ColumnType::Integer | zcat::ColumnType::Int => {
                    match r.get(&col.name) {
                        Some(Value::Int(i)) => Cell::Int(i),
                        Some(Value::Float(f)) => Cell::Int(f as i64),
                        Some(Value::Text(s)) => Cell::Int(s.parse::<i64>().unwrap_or_default()),
                        _ => Cell::Int(0),
                    }
                }
                zcat::ColumnType::Float => match r.get(&col.name) {
                    Some(Value::Float(f)) => Cell::Float(f),
                    Some(Value::Int(i)) => Cell::Float(i as f64),
                    _ => Cell::Float(0.0),
                },
                zcat::ColumnType::Text => match r.get(&col.name) {
                    Some(Value::Text(s)) => Cell::Text(s.clone()),
                    _ => Cell::Text(String::new()),
                },
                zcat::ColumnType::Vector => match r.get(&col.name) {
                    Some(Value::Vector(v)) => Cell::Vector(v.clone()),
                    _ => Cell::Vector(Vec::new()),
                },
                zcat::ColumnType::Decimal => Cell::Float(0.0),
            };
            cells.push(c);
        }
        Ok(cells)
    }

    pub(crate) fn cells_to_row(&self, tdef: &zcat::TableDef, cells: &[Cell]) -> Row {
        let mut row = Row::default();
        for (i, col) in tdef.columns.iter().enumerate() {
            if let Some(c) = cells.get(i) {
                match (col.ty.clone(), c) {
                    (zcat::ColumnType::Integer | zcat::ColumnType::Int, Cell::Int(v)) => { row.insert(col.name.clone(), Value::Int(*v)); }
                    (zcat::ColumnType::Float, Cell::Float(v)) => { row.insert(col.name.clone(), Value::Float(*v)); }
                    (zcat::ColumnType::Text, Cell::Text(v)) => { row.insert(col.name.clone(), Value::Text(v.clone())); }
                    (zcat::ColumnType::Vector, Cell::Vector(v)) => { row.insert(col.name.clone(), Value::Vector(v.clone())); }
                    _ => {}
                }
            }
        }
        row
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

pub(crate) fn parse_row_id_from_key(key: &[u8]) -> RowId {
    if key.len() >= 8 {
        let n = key.len();
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&key[n - 8..]);
        u64::from_be_bytes(arr)
    } else { 0 }
}
