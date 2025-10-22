use std::collections::HashMap;

use crate::error::{ZgError, ZgResult};
use crate::graph::NodeRef;
use crate::index::{self, Metric};
use crate::types::{Row, RowId, Value};

#[derive(Debug, Clone)]
pub struct Query {
    pub left_var: String,
    pub left_label: String,
    pub rel_type: String,
    pub right_var: String,
    pub right_label: String,
    pub filters: Vec<Filter>,
    pub order: Option<OrderByDistance>,
    pub limit: Option<usize>,
    pub returns: Vec<ReturnItem>,
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub var: String,
    pub key: String,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub struct OrderByDistance {
    pub var: String,
    pub field: String,
    pub vector: Vec<f32>,
    pub metric: Metric,
}

#[derive(Debug, Clone)]
pub enum ReturnItem {
    Var(String),
}

#[derive(Debug, Clone)]
pub struct ResultRow {
    pub vars: HashMap<String, (NodeRef, Row)>,
    pub distance: Option<f32>,
}

// Simple filter matching function
fn filters_match(filters: &[Filter], bindings: &[(String, &Row)]) -> bool {
    for filter in filters {
        let mut found = false;
        for (var_name, row) in bindings {
            if var_name == &filter.var {
                if let Some(value) = row.get(&filter.key) {
                    if value == &filter.value {
                        found = true;
                        break;
                    }
                }
            }
        }
        if !found {
            return false;
        }
    }
    true
}

// Super-minimal parser for patterns like:
// MATCH (a:docs)-[r:REL]->(b:meta) WHERE a.org_id=1 AND b.status=2 RETURN a,b ORDER BY distance(a.embedding,[0,0,0],'l2') LIMIT 10
pub fn parse(query: &str) -> ZgResult<Query> {
    let mut s = query.trim();
    // MATCH
    if !s.to_ascii_lowercase().starts_with("match ") {
        return Err(ZgError::Invalid("expected MATCH".into()));
    }
    s = &s[6..];
    // Pattern: (a:Label)-[r:TYPE]->(b:Label)
    let (left_var, left_label, rel_type, right_var, right_label, rest) = parse_pattern(s)?;
    s = rest.trim();

    // Optional WHERE
    let mut filters = Vec::new();
    if s.to_ascii_lowercase().starts_with("where ") {
        let idx = s.find(|c: char| c == 'r' || c == 'R') // start of RETURN
            .or_else(|| s.find(|c: char| c == 'o' || c == 'O')) // start of ORDER
            .or_else(|| s.find(|c: char| c == 'l' || c == 'L')) // start of LIMIT
            .unwrap_or_else(|| s.len());
        let (where_part, rest2) = s.split_at(idx);
        filters = parse_where(&where_part[6..])?; // skip WHERE
        s = rest2.trim();
    }

    // Optional RETURN
    let mut returns = Vec::new();
    if s.to_ascii_lowercase().starts_with("return ") {
        let idx = s.find(|c: char| c == 'o' || c == 'O')
            .or_else(|| s.find(|c: char| c == 'l' || c == 'L'))
            .unwrap_or_else(|| s.len());
        let (ret_part, rest2) = s.split_at(idx);
        returns = parse_return(&ret_part[7..])?; // skip RETURN
        s = rest2.trim();
    }

    // Optional ORDER BY distance(...)
    let mut order = None;
    if s.to_ascii_lowercase().starts_with("order by ") {
        s = &s[9..];
        order = Some(parse_order_distance(s)?);
        // consume until LIMIT or end
        if let Some(i) = s.to_ascii_lowercase().find("limit ") {
            s = &s[i..];
        } else {
            s = "";
        }
    }

    // Optional LIMIT
    let mut limit = None;
    if s.to_ascii_lowercase().starts_with("limit ") {
        let n = s[6..].trim();
        let n = n
            .split_whitespace()
            .next()
            .ok_or_else(|| ZgError::Invalid("expected limit value".into()))?;
        limit = Some(n.parse::<usize>().map_err(|_| ZgError::Invalid("bad limit".into()))?);
    }

    if returns.is_empty() {
        returns = vec![ReturnItem::Var(left_var.clone()), ReturnItem::Var(right_var.clone())];
    }

    Ok(Query {
        left_var,
        left_label,
        rel_type,
        right_var,
        right_label,
        filters,
        order,
        limit,
        returns,
    })
}

fn parse_pattern(s: &str) -> ZgResult<(String, String, String, String, String, &str)> {
    // Expect (a:Label)-[r:TYPE]->(b:Label)
    let s = s.trim();
    if !s.starts_with('(') {
        return Err(ZgError::Invalid("expected (".into()));
    }
    let close = s.find(')').ok_or_else(|| ZgError::Invalid("bad pattern".into()))?;
    let inside = &s[1..close];
    let (left_var, left_label) = parse_node(inside)?;
    let rest = &s[close + 1..];
    let rest = rest.trim_start();
    if !rest.starts_with("-[") {
        return Err(ZgError::Invalid("expected -[".into()));
    }
    let rest2 = &rest[2..];
    let rb = rest2.find(']').ok_or_else(|| ZgError::Invalid("bad rel".into()))?;
    let rel_inside = &rest2[1..rb]; // skip leading var name like r:TYPE
    // allow r:TYPE or just TYPE
    let rel_type = if let Some((_rvar, rty)) = rel_inside.split_once(':') {
        rty.trim().to_string()
    } else {
        rel_inside.trim().to_string()
    };
    let rest3 = &rest2[rb + 1..];
    let rest3 = rest3.trim_start();
    if !rest3.starts_with("->(") {
        return Err(ZgError::Invalid("expected ->(".into()));
    }
    let rest4 = &rest3[2..];
    let close2 = rest4.find(')').ok_or_else(|| ZgError::Invalid("bad right node".into()))?;
    let inside2 = &rest4[1..close2];
    let (right_var, right_label) = parse_node(inside2)?;
    let rest5 = &rest4[close2 + 1..];
    Ok((left_var, left_label, rel_type, right_var, right_label, rest5))
}

fn parse_node(s: &str) -> ZgResult<(String, String)> {
    // a:Label
    if let Some((var, label)) = s.split_once(':') {
        Ok((var.trim().to_string(), label.trim().to_string()))
    } else {
        Err(ZgError::Invalid("node must be var:Label".into()))
    }
}

fn parse_where(s: &str) -> ZgResult<Vec<Filter>> {
    // a.k=1 AND b.name='x'
    let mut out = Vec::new();
    for part in s.split("AND") {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (lhs, rhs) = part
            .split_once('=')
            .ok_or_else(|| ZgError::Invalid("expected = in WHERE".into()))?;
        let (var, key) = lhs
            .trim()
            .split_once('.')
            .ok_or_else(|| ZgError::Invalid("expected var.key in WHERE".into()))?;
        let value = parse_literal(rhs.trim())?;
        out.push(Filter {
            var: var.trim().to_string(),
            key: key.trim().to_string(),
            value,
        });
    }
    Ok(out)
}

fn parse_return(s: &str) -> ZgResult<Vec<ReturnItem>> {
    let mut out = Vec::new();
    for v in s.split(',') {
        let v = v.trim();
        if v.is_empty() {
            continue;
        }
        out.push(ReturnItem::Var(v.to_string()));
    }
    Ok(out)
}

fn parse_order_distance(s: &str) -> ZgResult<OrderByDistance> {
    // distance(a.embedding, [..], 'l2')
    let sl = s.trim_start().to_ascii_lowercase();
    if !sl.starts_with("distance(") {
        return Err(ZgError::Invalid("only distance() supported in ORDER BY".into()));
    }
    let inner = &s[s.find('(').unwrap() + 1..];
    let close = inner.find(')').ok_or_else(|| ZgError::Invalid("bad distance()".into()))?;
    let args = &inner[..close];
    let mut parts = args.split(',');
    let varfield = parts
        .next()
        .ok_or_else(|| ZgError::Invalid("missing var.field".into()))?
        .trim();
    let (var, field) = varfield
        .split_once('.')
        .ok_or_else(|| ZgError::Invalid("expected var.field".into()))?;
    let vec_str = parts
        .next()
        .ok_or_else(|| ZgError::Invalid("missing vector".into()))?
        .trim();
    let vector = parse_vec(vec_str)?;
    let metric_str = parts
        .next()
        .ok_or_else(|| ZgError::Invalid("missing metric".into()))?
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .to_ascii_lowercase();
    let metric = match metric_str.as_str() {
        "l2" => Metric::L2,
        "cosine" => Metric::Cosine,
        "dot" | "inner" | "innerproduct" => Metric::Dot,
        _ => return Err(ZgError::Invalid("unknown metric".into())),
    };
    Ok(OrderByDistance {
        var: var.trim().to_string(),
        field: field.trim().to_string(),
        vector,
        metric,
    })
}

fn parse_vec(s: &str) -> ZgResult<Vec<f32>> {
    let s = s.trim();
    if !s.starts_with('[') || !s.ends_with(']') {
        return Err(ZgError::Invalid("vector must be [..]".into()));
    }
    let inner = &s[1..s.len() - 1];
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for n in inner.split(',') {
        let v: f32 = n.trim().parse().map_err(|_| ZgError::Invalid("bad vector value".into()))?;
        out.push(v);
    }
    Ok(out)
}

fn parse_literal(s: &str) -> ZgResult<Value> {
    let s = s.trim();
    if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        return Ok(Value::Text(s[1..s.len() - 1].to_string()));
    }
    if let Ok(i) = s.parse::<i64>() {
        return Ok(Value::Int(i));
    }
    if let Ok(f) = s.parse::<f64>() {
        return Ok(Value::Float(f));
    }
    if let Ok(b) = s.parse::<bool>() {
        return Ok(Value::Bool(b));
    }
    Err(ZgError::Invalid("unsupported literal".into()))
}

impl crate::Database {
    pub fn cypher(&self, user: &str, q: &str) -> ZgResult<Vec<ResultRow>> {
        let q = parse(q)?;
        let inner = self.inner.read();
        let lt = inner
            .tables
            .get(&q.left_label)
            .ok_or_else(|| ZgError::NotFound(format!("table '{}'", q.left_label)))?;
        let rt = inner
            .tables
            .get(&q.right_label)
            .ok_or_else(|| ZgError::NotFound(format!("table '{}'", q.right_label)))?;

        // Enumerate left nodes the user can read
        let mut rows = Vec::new();
        for (lid, lrow) in lt.rows.iter() {
            if !inner
                .perms
                .check(user, &q.left_label, Some(*lid), crate::permissions::Permission::Read)
            {
                continue;
            }
            // neighbors from left to right
            let nn = inner
                .edges
                .neighbors(&NodeRef { table: q.left_label.clone(), id: *lid }, Some(&q.rel_type));
            for (_eid, nref) in nn.into_iter() {
                if nref.table != q.right_label {
                    continue;
                }
                if !inner.perms.check(
                    user,
                    &nref.table,
                    Some(nref.id),
                    crate::permissions::Permission::Read,
                ) {
                    continue;
                }
                if let Some(rrow) = rt.rows.get(&nref.id) {
                    // apply filters
                    if !filters_match(&q.filters, &[(q.left_var.clone(), lrow), (q.right_var.clone(), rrow)]) {
                        continue;
                    }
                    let mut vars = HashMap::new();
                    vars.insert(
                        q.left_var.clone(),
                        (
                            NodeRef { table: q.left_label.clone(), id: *lid },
                            lrow.clone(),
                        ),
                    );
                    vars.insert(q.right_var.clone(), (nref.clone(), rrow.clone()));
                    rows.push(ResultRow { vars, distance: None });
                }
            }
        }

        // ORDER BY distance
        if let Some(ord) = &q.order {
            for r in rows.iter_mut() {
                if let Some((_, row)) = r.vars.get(&ord.var) {
                    if let Some(Value::Vector(v)) = row.get(&ord.field) {
                        let d = match ord.metric {
                            Metric::L2 => l2(v.as_slice(), &ord.vector),
                            Metric::Cosine => cosine(v.as_slice(), &ord.vector),
                            Metric::Dot => -dot(v.as_slice(), &ord.vector),
                        };
                        r.distance = Some(d);
                    }
                }
            }
            rows.sort_by(|a, b| a
                .distance
                .unwrap_or(f32::INFINITY)
                .partial_cmp(&b.distance.unwrap_or(f32::INFINITY))
                .unwrap());
        }

        if let Some(lim) = q.limit {
            rows.truncate(lim);
        }
        Ok(rows)
    }
}

// Fjall implementation
impl crate::fjall_db::FjDatabase {
    pub fn cypher(&self, user: &str, q: &str) -> ZgResult<Vec<ResultRow>> {
        let q = parse(q)?;
        let catalog = zcore_catalog::Catalog::new(&self.store);
        let lt = catalog
            .get_table(self.org_id, &q.left_label)?
            .ok_or_else(|| ZgError::NotFound(format!("table '{}'", q.left_label)))?;
        let rt = catalog
            .get_table(self.org_id, &q.right_label)?
            .ok_or_else(|| ZgError::NotFound(format!("table '{}'", q.right_label)))?;

        // Enumerate left nodes via storage scan
        let tx = zcore_storage::ReadTransaction::new();
        let prefix = zcore_storage::encode_key(&[b"rows", &self.org_id.to_be_bytes(), q.left_label.as_bytes()]);
        let items = tx.scan_prefix(&self.store, zcore_storage::COL_ROWS, &prefix)?;
        let p = self.perms.read();
        let mut rows = Vec::new();
        for (key, bytes) in items {
            let lid = super::fjall_db::parse_row_id_from_key(&key);
            if !p.check(user, &q.left_label, Some(lid), crate::permissions::Permission::Read) {
                continue;
            }
            let lcells: Vec<crate::fjall_db::Cell> = bincode::deserialize(&bytes).unwrap_or_default();
            let lrow = self.cells_to_row(&lt, &lcells);
            // neighbors from left to right (edges are in-memory)
            let nn = self
                .edges
                .read()
                .neighbors(&NodeRef { table: q.left_label.clone(), id: lid }, Some(&q.rel_type));
            for (_eid, nref) in nn.into_iter() {
                if nref.table != q.right_label { continue; }
                if !p.check(user, &nref.table, Some(nref.id), crate::permissions::Permission::Read) {
                    continue;
                }
                // read right row
                let rkey = zcore_storage::encode_key(&[b"rows", &self.org_id.to_be_bytes(), q.right_label.as_bytes(), &nref.id.to_be_bytes()]);
                let tx2 = zcore_storage::ReadTransaction::new();
                if let Some(rbytes) = tx2.get(&self.store, zcore_storage::COL_ROWS, &rkey)? {
                    let rcells: Vec<crate::fjall_db::Cell> = bincode::deserialize(&rbytes).unwrap_or_default();
                    let rrow = self.cells_to_row(&rt, &rcells);
                    // apply filters
                    if !filters_match(&q.filters, &[(q.left_var.clone(), &lrow), (q.right_var.clone(), &rrow)]) { continue; }
                    let mut vars = std::collections::HashMap::new();
                    vars.insert(q.left_var.clone(), (NodeRef { table: q.left_label.clone(), id: lid }, lrow.clone()));
                    vars.insert(q.right_var.clone(), (nref.clone(), rrow.clone()));
                    rows.push(ResultRow { vars, distance: None });
                }
            }
        }
        drop(p);

        // ORDER BY distance
        if let Some(ord) = &q.order {
            for r in rows.iter_mut() {
                if let Some((_, row)) = r.vars.get(&ord.var) {
                    if let Some(Value::Vector(v)) = row.get(&ord.field) {
                        let d = match ord.metric {
                            Metric::L2 => l2(v.as_slice(), &ord.vector),
                            Metric::Cosine => cosine(v.as_slice(), &ord.vector),
                            Metric::Dot => -dot(v.as_slice(), &ord.vector),
                        };
                        r.distance = Some(d);
                    }
                }
            }
            rows.sort_by(|a, b| a.distance.unwrap_or(f32::INFINITY).partial_cmp(&b.distance.unwrap_or(f32::INFINITY)).unwrap());
        }

        if let Some(lim) = q.limit { rows.truncate(lim); }
        Ok(rows)
    }
}

fn l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum::<f32>().sqrt()
}
fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}
fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let num = dot(a, b);
    let na = (a.iter().map(|x| x * x).sum::<f32>()).sqrt();
    let nb = (b.iter().map(|x| x * x).sum::<f32>()).sqrt();
    1.0 - (num / (na * nb + 1e-12))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::TableSchema;

    #[test]
    fn parse_and_exec_minimal() {
        let db = crate::Database::new();
        db.create_table("docs", TableSchema::new("docs").with_vector("embedding", Some(3), Metric::L2))
            .unwrap();
        let mut a = Row::default();
        a.insert("title".into(), Value::Text("a".into()));
        a.insert("embedding".into(), Value::Vector(vec![0.0, 0.0, 0.0]));
        let aid = db.insert_row("u", "docs", a).unwrap();
        let mut b = Row::default();
        b.insert("title".into(), Value::Text("b".into()));
        b.insert("embedding".into(), Value::Vector(vec![1.0, 0.0, 0.0]));
        let bid = db.insert_row("u", "docs", b).unwrap();
        db.add_edge(
            "u",
            NodeRef { table: "docs".into(), id: aid },
            "similar",
            NodeRef { table: "docs".into(), id: bid },
            HashMap::new(),
        )
        .unwrap();

        let q = "MATCH (a:docs)-[:similar]->(b:docs) WHERE a.title='a' RETURN a,b ORDER BY distance(b.embedding, [0.2,0,0], 'l2') LIMIT 1";
        let res = db.cypher("u", q).unwrap();
        assert_eq!(res.len(), 1);
        let r = &res[0];
        assert!(r.vars.contains_key("a"));
        assert!(r.vars.contains_key("b"));
        assert!(r.distance.is_some());
    }
}
