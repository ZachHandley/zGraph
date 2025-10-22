use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

use crate::types::{RowId, Value};

pub const EDGE_TABLE_NAME: &str = "__edges";

pub type EdgeId = u64;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NodeRef {
    pub table: String,
    pub id: RowId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Edge {
    pub id: EdgeId,
    pub from: NodeRef,
    pub rel_type: String,
    pub to: NodeRef,
    pub props: HashMap<String, Value>,
}

#[derive(Default, Debug)]
pub struct EdgeStore {
    edges: HashMap<EdgeId, Edge>,
    next_id: EdgeId,
    // forward adjacency: (table,id) -> edge ids
    fwd: HashMap<(String, RowId), Vec<EdgeId>>,
    // reverse adjacency (dst -> incoming edge ids)
    rev: HashMap<(String, RowId), Vec<EdgeId>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeDump {
    pub id: EdgeId,
    pub from: NodeRef,
    pub rel_type: String,
    pub to: NodeRef,
    pub props: HashMap<String, Value>,
}

impl Default for NodeRef {
    fn default() -> Self {
        Self {
            table: String::new(),
            id: 0,
        }
    }
}

impl EdgeStore {
    pub fn add_edge(
        &mut self,
        from: NodeRef,
        rel_type: String,
        to: NodeRef,
        props: HashMap<String, Value>,
    ) -> EdgeId {
        let id = self.next_id;
        self.next_id += 1;
        let e = Edge {
            id,
            from: from.clone(),
            rel_type,
            to,
            props,
        };
        self.edges.insert(id, e);
        self.fwd
            .entry((from.table, from.id))
            .or_default()
            .push(id);
        // also populate reverse adjacency
        let to_ref = self.edges.get(&id).unwrap().to.clone();
        self.rev
            .entry((to_ref.table, to_ref.id))
            .or_default()
            .push(id);
        id
    }

    pub fn neighbors(&self, node: &NodeRef, rel_type: Option<&str>) -> Vec<(EdgeId, NodeRef)> {
        let mut out = Vec::new();
        if let Some(eids) = self.fwd.get(&(node.table.clone(), node.id)) {
            for eid in eids {
                if let Some(e) = self.edges.get(eid) {
                    if rel_type.map(|t| t == e.rel_type).unwrap_or(true) {
                        out.push((*eid, e.to.clone()));
                    }
                }
            }
        }
        out
    }

    pub fn incoming(&self, node: &NodeRef, rel_type: Option<&str>) -> Vec<(EdgeId, NodeRef)> {
        let mut out = Vec::new();
        if let Some(eids) = self.rev.get(&(node.table.clone(), node.id)) {
            for eid in eids {
                if let Some(e) = self.edges.get(eid) {
                    if rel_type.map(|t| t == e.rel_type).unwrap_or(true) {
                        out.push((*eid, e.from.clone()));
                    }
                }
            }
        }
        out
    }

    pub fn dump(&self) -> Vec<EdgeDump> {
        self.edges
            .values()
            .cloned()
            .map(|e| EdgeDump {
                id: e.id,
                from: e.from,
                rel_type: e.rel_type,
                to: e.to,
                props: e.props,
            })
            .collect()
    }

    pub fn restore(&mut self, dump: Vec<EdgeDump>) {
        self.edges.clear();
        self.fwd.clear();
        self.rev.clear();
        self.next_id = 0;
        for e in dump.into_iter() {
            let id = e.id;
            self.next_id = self.next_id.max(id + 1);
            let from = e.from.clone();
            self.fwd
                .entry((from.table.clone(), from.id))
                .or_default()
                .push(id);
            let to = e.to.clone();
            self.rev
                .entry((to.table.clone(), to.id))
                .or_default()
                .push(id);
            self.edges.insert(
                id,
                Edge {
                    id,
                    from: e.from,
                    rel_type: e.rel_type,
                    to: e.to,
                    props: e.props,
                },
            );
        }
    }
}
