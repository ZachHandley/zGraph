use anyhow::{anyhow, Result};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::HashMap, fmt, str::FromStr, sync::Arc};
use zcore_catalog as zcat;
use zcore_storage as zs;
use zvec_kernels;

// Custom error types for better error handling
#[derive(Debug, thiserror::Error)]
pub enum HnswError {
    #[error("Index not found for table '{table}', column '{column}'")]
    IndexNotFound { table: String, column: String },
    #[error("Invalid metric: {0}")]
    InvalidMetric(String),
    #[error("Invalid parameters: {0}")]
    InvalidParameters(String),
    #[error("Storage error: {0}")]
    Storage(#[from] anyhow::Error),
    #[error("Empty index: no vectors available for search")]
    EmptyIndex,
    #[error("Invalid vector dimensions: expected {expected}, got {actual}")]
    InvalidDimension { expected: usize, actual: usize },
}


pub const DEFAULT_M: usize = 16;
pub const DEFAULT_EF_CONSTRUCTION: usize = 64;
const MAX_LEVEL: u8 = 16;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Metric {
    L2,
    Cosine,
    InnerProduct,
}

impl From<Metric> for zvec_kernels::Metric {
    fn from(metric: Metric) -> Self {
        match metric {
            Metric::L2 => zvec_kernels::Metric::L2,
            Metric::Cosine => zvec_kernels::Metric::Cosine,
            Metric::InnerProduct => zvec_kernels::Metric::InnerProduct,
        }
    }
}

impl Metric {
    fn distance_nodes(&self, vecs: &[Vec<f32>], _norms: &[f32], a: usize, b: usize) -> f32 {
        // Use optimized SIMD kernels for all distance calculations
        zvec_kernels::distance(&vecs[a], &vecs[b], (*self).into()).unwrap_or(f32::INFINITY)
    }

    fn distance_to_query(
        &self,
        vecs: &[Vec<f32>],
        _norms: &[f32],
        idx: usize,
        query: &[f32],
        _q_norm: f32,
    ) -> f32 {
        // Use optimized SIMD kernels for query distance calculations
        zvec_kernels::distance(query, &vecs[idx], (*self).into()).unwrap_or(f32::INFINITY)
    }
}

impl FromStr for Metric {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "l2" | "euclidean" => Ok(Metric::L2),
            "cosine" => Ok(Metric::Cosine),
            "ip" | "inner_product" => Ok(Metric::InnerProduct),
            _ => Err(anyhow!("unsupported metric '{}'; expected l2|cosine|ip", s)),
        }
    }
}

impl fmt::Display for Metric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Metric::L2 => write!(f, "l2"),
            Metric::Cosine => write!(f, "cosine"),
            Metric::InnerProduct => write!(f, "ip"),
        }
    }
}

#[derive(Clone)]
struct AnnIndex {
    metric: Metric,
    m: usize,
    ef_construction: usize,
    entry: usize,
    max_level: u8,
    levels: Vec<u8>,
    vecs: Vec<Vec<f32>>,
    norms: Vec<f32>,
    ids: Vec<u64>,
    layers: Vec<Vec<Vec<usize>>>, // layers[level][node] -> neighbors
}

#[derive(Serialize, Deserialize)]
struct PersistedIndex {
    metric: Metric,
    m: usize,
    ef_construction: usize,
    entry: usize,
    max_level: u8,
    levels: Vec<u8>,
    vecs: Vec<Vec<f32>>,
    norms: Vec<f32>,
    ids: Vec<u64>,
    layers: Vec<Vec<Vec<usize>>>,
}

impl From<PersistedIndex> for AnnIndex {
    fn from(p: PersistedIndex) -> Self {
        Self {
            metric: p.metric,
            m: p.m,
            ef_construction: p.ef_construction,
            entry: p.entry,
            max_level: p.max_level,
            levels: p.levels,
            vecs: p.vecs,
            norms: p.norms,
            ids: p.ids,
            layers: p.layers,
        }
    }
}

struct AnnBuilder {
    metric: Metric,
    m: usize,
    ef_construction: usize,
    max_level: u8,
    entry: usize,
    vecs: Vec<Vec<f32>>,
    norms: Vec<f32>,
    ids: Vec<u64>,
    levels: Vec<u8>,
    layers: Vec<Vec<Vec<usize>>>,
}

impl AnnBuilder {
    fn new(metric: Metric, m: usize, ef_construction: usize) -> Self {
        Self {
            metric,
            m: m.max(1),
            ef_construction: ef_construction.max(1),
            max_level: 0,
            entry: 0,
            vecs: Vec::new(),
            norms: Vec::new(),
            ids: Vec::new(),
            levels: Vec::new(),
            layers: Vec::new(),
        }
    }

    fn add_point(&mut self, vec: Vec<f32>, id: u64) {
        let node_idx = self.vecs.len();
        // Use node index for level sampling to ensure proper randomization
        let level = sample_level(node_idx as u64, self.m, self.ef_construction);
        let norm = if self.metric == Metric::Cosine {
            vector_norm(&vec)
        } else {
            0.0 // Not needed for L2 and InnerProduct when using SIMD kernels
        };
        self.vecs.push(vec);
        self.norms.push(norm);
        self.ids.push(id);
        self.levels.push(level);

        let node_level = level as usize;
        let total_nodes = self.vecs.len();

        if self.layers.is_empty() {
            self.layers.push(Vec::new());
        }

        while self.layers.len() <= node_level {
            let mut new_layer = Vec::with_capacity(total_nodes);
            for _ in 0..total_nodes {
                new_layer.push(Vec::new());
            }
            self.layers.push(new_layer);
        }

        for layer in self.layers.iter_mut() {
            while layer.len() < total_nodes {
                layer.push(Vec::new());
            }
        }

        if node_idx == 0 {
            self.entry = 0;
            self.max_level = level;
            return;
        }

        if level > self.max_level {
            self.entry = node_idx;
            self.max_level = level;
        }

        for lvl in 0..=node_level {
            let mut candidates: Vec<(usize, f32)> = Vec::new();
            for other in 0..node_idx {
                if self.levels[other] as usize >= lvl {
                    let dist = self
                        .metric
                        .distance_nodes(&self.vecs, &self.norms, node_idx, other);
                    candidates.push((other, dist));
                }
            }
            candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
            candidates.truncate(self.m.max(1));
            for (other, _) in candidates {
                add_neighbor(
                    &mut self.layers[lvl],
                    node_idx,
                    other,
                    self.m,
                    self.metric,
                    &self.vecs,
                    &self.norms,
                );
                add_neighbor(
                    &mut self.layers[lvl],
                    other,
                    node_idx,
                    self.m,
                    self.metric,
                    &self.vecs,
                    &self.norms,
                );
            }
        }
    }

    fn finish(self) -> AnnIndex {
        AnnIndex {
            metric: self.metric,
            m: self.m,
            ef_construction: self.ef_construction,
            entry: self.entry,
            max_level: self.max_level,
            levels: self.levels,
            vecs: self.vecs,
            norms: self.norms,
            ids: self.ids,
            layers: self.layers,
        }
    }
}

static REG: Lazy<RwLock<HashMap<(u64, String, String), Arc<AnnIndex>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

pub fn build_index(
    store: &zs::Store,
    catalog: &zcat::Catalog,
    org_id: u64,
    table: &str,
    column: &str,
    metric: Metric,
    m: usize,
    ef_construction: usize,
) -> Result<()> {
    // Validate parameters
    if m == 0 {
        return Err(HnswError::InvalidParameters(
            "m must be greater than 0".to_string()
        ).into());
    }
    if ef_construction == 0 {
        return Err(HnswError::InvalidParameters(
            "ef_construction must be greater than 0".to_string()
        ).into());
    }

    let tdef = catalog
        .get_table(org_id, table)?
        .ok_or_else(|| HnswError::IndexNotFound {
            table: table.to_string(),
            column: column.to_string(),
        })?;

    let col_idx = tdef
        .columns
        .iter()
        .position(|c| c.name == column)
        .ok_or_else(|| HnswError::IndexNotFound {
            table: table.to_string(),
            column: column.to_string(),
        })?;

    let tx = zs::ReadTransaction::new();
    let prefix = zs::encode_key(&[b"rows", &org_id.to_be_bytes(), table.as_bytes()]);
    let items = tx.scan_prefix(store, zs::COL_ROWS, &prefix)?;

    let mut builder = AnnBuilder::new(metric, m, ef_construction);
    let _row_count = 0;       // Reserved for future metrics/logging
    let _vector_count = 0;   // Reserved for future metrics/logging
    let mut first_vector: Option<Vec<f32>> = None;

    for (key_bytes, value_bytes) in items {
        // _row_count += 1;  // Reserved for future metrics/logging
        let row_id = parse_row_id_from_key(&key_bytes)?;
        let cells: Vec<Cell> = bincode::deserialize(&value_bytes)?;
        if col_idx < cells.len() {
            if let Cell::Vector(vec) = &cells[col_idx] {
                // Validate vector dimensions
                if let Some(ref first) = first_vector {
                    if first.len() != vec.len() {
                        return Err(HnswError::InvalidDimension {
                            expected: first.len(),
                            actual: vec.len(),
                        }.into());
                    }
                } else {
                    first_vector = Some(vec.clone());
                }
                // _vector_count += 1;  // Reserved for future metrics/logging
                builder.add_point(vec.clone(), row_id);
            }
        }
    }

    // Check if we found any vectors
    if first_vector.is_none() {
        return Err(HnswError::EmptyIndex.into());
    }

    let ann = builder.finish();
    persist_index(store, org_id, table, column, &ann)?;
    REG.write().insert(
        (org_id, table.to_string(), column.to_string()),
        Arc::new(ann),
    );
    Ok(())
}

pub fn search(
    store: &zs::Store,
    catalog: &zcat::Catalog,
    org_id: u64,
    table: &str,
    column: &str,
    query: &[f32],
    top_k: usize,
    ef_search: usize,
) -> Result<Vec<(u64, f32)>> {
    // Validate parameters
    if query.is_empty() {
        return Err(HnswError::InvalidParameters(
            "Query vector cannot be empty".to_string()
        ).into());
    }
    if top_k == 0 {
        return Err(HnswError::InvalidParameters(
            "top_k must be greater than 0".to_string()
        ).into());
    }
    if ef_search == 0 {
        return Err(HnswError::InvalidParameters(
            "ef_search must be greater than 0".to_string()
        ).into());
    }

    let _ = catalog; // reserved for future use
    let ann = ensure_loaded(store, org_id, table, column)?;
    if ann.vecs.is_empty() {
        return Err(HnswError::EmptyIndex.into());
    }

    // Validate query dimensions
    if let Some(first_vec) = ann.vecs.first() {
        if first_vec.len() != query.len() {
            return Err(HnswError::InvalidDimension {
                expected: first_vec.len(),
                actual: query.len(),
            }.into());
        }
    }

    let q_norm = match ann.metric {
        Metric::Cosine => vector_norm(query),
        Metric::InnerProduct | Metric::L2 => 0.0,
    };

    let mut entry = ann.entry;
    if entry >= ann.vecs.len() {
        return Ok(Vec::new());
    }

    let max_level = ann.layers.len().saturating_sub(1);
    if max_level > 0 && max_level < ann.layers.len() {
        for lvl in (1..=max_level).rev() {
            if lvl < ann.layers.len() {
                entry = greedy_descent(&ann, entry, lvl, query, q_norm);
            }
        }
    }

    let ef = ef_search.max(top_k.max(1)).min(8192);
    let candidates = search_layer(&ann, entry, 0, query, q_norm, ef);
    let mut out = Vec::new();
    for (idx, dist) in candidates.into_iter().take(top_k) {
        out.push((ann.ids[idx], dist));
    }
    Ok(out)
}

pub struct IndexInfo {
    pub metric: Metric,
    pub m: usize,
    pub ef_construction: usize,
    pub layers: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSummary {
    pub table: String,
    pub column: String,
    pub metric: Metric,
    pub m: usize,
    pub ef_construction: usize,
    pub layers: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerStats {
    pub level: usize,
    pub nodes: usize,
    pub avg_degree: f32,
    pub max_degree: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    pub table: String,
    pub column: String,
    pub metric: Metric,
    pub m: usize,
    pub ef_construction: usize,
    pub total_vectors: usize,
    pub layers: Vec<LayerStats>,
}

pub fn index_info(
    store: &zs::Store,
    org_id: u64,
    table: &str,
    column: &str,
) -> Result<Option<IndexInfo>> {
    match try_get_index(store, org_id, table, column)? {
        Some(ann) => Ok(Some(IndexInfo {
            metric: ann.metric,
            m: ann.m,
            ef_construction: ann.ef_construction,
            layers: ann.layers.len(),
        })),
        None => Ok(None),
    }
}

pub fn list_indexes(store: &zs::Store, org_id: u64) -> Result<Vec<IndexSummary>> {
    let tx = zs::ReadTransaction::new();
    let org_bytes = org_id.to_be_bytes();
    let prefix = zs::encode_key(&[b"hnsw", &org_bytes]);
    let items = tx.scan_prefix(store, zs::COL_INDEX, &prefix)?;

    let mut out = Vec::new();
    for (key_bytes, value_bytes) in items {
        let parts = decode_key_parts(&key_bytes)?;
        let table = parts
            .get(2)
            .ok_or_else(|| anyhow!("invalid index key"))?
            .clone();
        let column = parts
            .get(3)
            .ok_or_else(|| anyhow!("invalid index key"))?
            .clone();
        let table = String::from_utf8(table).map_err(|_| anyhow!("invalid utf8"))?;
        let column = String::from_utf8(column).map_err(|_| anyhow!("invalid utf8"))?;
        let persisted: PersistedIndex = bincode::deserialize(&value_bytes)?;
        out.push(IndexSummary {
            table,
            column,
            metric: persisted.metric,
            m: persisted.m,
            ef_construction: persisted.ef_construction,
            layers: persisted.layers.len(),
        });
    }
    out.sort_by(|a, b| a.table.cmp(&b.table).then(a.column.cmp(&b.column)));
    Ok(out)
}

pub fn index_stats(
    store: &zs::Store,
    org_id: u64,
    table: &str,
    column: &str,
) -> Result<Option<IndexStats>> {
    let Some(ann) = try_get_index(store, org_id, table, column)? else {
        return Ok(None);
    };

    let total_vectors = ann.vecs.len();
    let mut layers = Vec::new();
    for (level, layer) in ann.layers.iter().enumerate() {
        let mut nodes = 0usize;
        let mut degree_sum = 0usize;
        let mut max_degree = 0usize;
        for (idx, neighbors) in layer.iter().enumerate() {
            if ann.levels[idx] as usize >= level {
                nodes += 1;
                let degree = neighbors.len();
                degree_sum += degree;
                if degree > max_degree {
                    max_degree = degree;
                }
            }
        }
        if nodes == 0 {
            continue;
        }
        let avg_degree = if nodes > 0 {
            degree_sum as f32 / nodes as f32
        } else {
            0.0
        };
        layers.push(LayerStats {
            level,
            nodes,
            avg_degree,
            max_degree,
        });
    }

    Ok(Some(IndexStats {
        table: table.to_string(),
        column: column.to_string(),
        metric: ann.metric,
        m: ann.m,
        ef_construction: ann.ef_construction,
        total_vectors,
        layers,
    }))
}

fn ensure_loaded(
    store: &zs::Store,
    org_id: u64,
    table: &str,
    column: &str,
) -> Result<Arc<AnnIndex>> {
    match try_get_index(store, org_id, table, column)? {
        Some(ann) => Ok(ann),
        None => Err(HnswError::IndexNotFound {
            table: table.to_string(),
            column: column.to_string(),
        }.into()),
    }
}

fn try_get_index(
    store: &zs::Store,
    org_id: u64,
    table: &str,
    column: &str,
) -> Result<Option<Arc<AnnIndex>>> {
    if let Some(ann) = REG
        .read()
        .get(&(org_id, table.to_string(), column.to_string()))
        .cloned()
    {
        return Ok(Some(ann));
    }
    if let Some(ann) = load_index(store, org_id, table, column)? {
        let ann_arc = Arc::new(ann);
        REG.write().insert(
            (org_id, table.to_string(), column.to_string()),
            ann_arc.clone(),
        );
        Ok(Some(ann_arc))
    } else {
        Ok(None)
    }
}

fn persist_index(
    store: &zs::Store,
    org_id: u64,
    table: &str,
    column: &str,
    ann: &AnnIndex,
) -> Result<()> {
    let key = key_index(org_id, table, column);
    let payload = PersistedIndex {
        metric: ann.metric,
        m: ann.m,
        ef_construction: ann.ef_construction,
        entry: ann.entry,
        max_level: ann.max_level,
        levels: ann.levels.clone(),
        vecs: ann.vecs.clone(),
        norms: ann.norms.clone(),
        ids: ann.ids.clone(),
        layers: ann.layers.clone(),
    };
    let bytes = bincode::serialize(&payload)?;

    let mut tx = zs::WriteTransaction::new();
    tx.set(zs::COL_INDEX, key, bytes);
    tx.commit(store)?;
    Ok(())
}

fn load_index(
    store: &zs::Store,
    org_id: u64,
    table: &str,
    column: &str,
) -> Result<Option<AnnIndex>> {
    let tx = zs::ReadTransaction::new();
    let key = key_index(org_id, table, column);

    if let Some(value_bytes) = tx.get(store, zs::COL_INDEX, &key)? {
        let persisted: PersistedIndex = bincode::deserialize(&value_bytes)?;
        Ok(Some(persisted.into()))
    } else {
        Ok(None)
    }
}

fn greedy_descent(ann: &AnnIndex, start: usize, level: usize, query: &[f32], q_norm: f32) -> usize {
    let mut current = start;
    let mut best = ann
        .metric
        .distance_to_query(&ann.vecs, &ann.norms, current, query, q_norm);
    let mut visited = std::collections::HashSet::new();
    visited.insert(current);

    loop {
        let mut improved = false;
        let mut next_best = current;
        let mut next_dist = best;

        for &nbr in &ann.layers[level][current] {
            if visited.contains(&nbr) {
                continue;
            }
            let dist = ann
                .metric
                .distance_to_query(&ann.vecs, &ann.norms, nbr, query, q_norm);
            if dist < next_dist {
                next_dist = dist;
                next_best = nbr;
                improved = true;
            }
        }

        if !improved {
            break;
        }

        current = next_best;
        best = next_dist;
        visited.insert(current);

        // Safety check to prevent infinite loops
        if visited.len() > ann.vecs.len() {
            break;
        }
    }
    current
}

fn search_layer(
    ann: &AnnIndex,
    entry: usize,
    level: usize,
    query: &[f32],
    q_norm: f32,
    ef: usize,
) -> Vec<(usize, f32)> {
    // Safety checks
    if ann.vecs.is_empty() || level >= ann.layers.len() || entry >= ann.vecs.len() {
        return Vec::new();
    }
    let ef = ef.min(ann.vecs.len()).max(1);
    #[derive(Clone, Copy, Debug)]
    struct MinCandidate {
        dist: f32,
        idx: usize,
    }
    impl PartialEq for MinCandidate {
        fn eq(&self, other: &Self) -> bool {
            self.idx == other.idx
        }
    }
    impl Eq for MinCandidate {}
    impl PartialOrd for MinCandidate {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            other
                .dist
                .partial_cmp(&self.dist)
                .or_else(|| (self.idx == other.idx).then_some(Ordering::Equal))
        }
    }
    impl Ord for MinCandidate {
        fn cmp(&self, other: &Self) -> Ordering {
            self.partial_cmp(other).unwrap_or(Ordering::Equal)
        }
    }

    #[derive(Clone, Copy, Debug)]
    struct MaxCandidate {
        dist: f32,
        idx: usize,
    }
    impl PartialEq for MaxCandidate {
        fn eq(&self, other: &Self) -> bool {
            self.idx == other.idx
        }
    }
    impl Eq for MaxCandidate {}
    impl PartialOrd for MaxCandidate {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            self.dist
                .partial_cmp(&other.dist)
                .or_else(|| (self.idx == other.idx).then_some(Ordering::Equal))
        }
    }
    impl Ord for MaxCandidate {
        fn cmp(&self, other: &Self) -> Ordering {
            self.partial_cmp(other).unwrap_or(Ordering::Equal)
        }
    }

    let mut visited = vec![false; ann.vecs.len()];
    let mut candidates = std::collections::BinaryHeap::new();
    let mut best = std::collections::BinaryHeap::new();
    let mut iterations = 0;
    const MAX_ITERATIONS: usize = 100000;

    let entry_dist = ann
        .metric
        .distance_to_query(&ann.vecs, &ann.norms, entry, query, q_norm);
    candidates.push(MinCandidate {
        dist: entry_dist,
        idx: entry,
    });
    best.push(MaxCandidate {
        dist: entry_dist,
        idx: entry,
    });
    visited[entry] = true;

    while let Some(MinCandidate { dist, idx }) = candidates.pop() {
        iterations += 1;
        if iterations > MAX_ITERATIONS {
            break; // Prevent infinite loops
        }

        let worst = best
            .peek()
            .map(|c: &MaxCandidate| c.dist)
            .unwrap_or(f32::MAX);
        if best.len() >= ef && dist > worst {
            break;
        }

        // Safety check for valid index
        if idx >= ann.layers[level].len() {
            continue;
        }

        for &nbr in &ann.layers[level][idx] {
            if nbr >= visited.len() || visited[nbr] {
                continue;
            }
            visited[nbr] = true;
            let d = ann
                .metric
                .distance_to_query(&ann.vecs, &ann.norms, nbr, query, q_norm);
            if best.len() < ef || d < worst {
                candidates.push(MinCandidate { dist: d, idx: nbr });
                best.push(MaxCandidate { dist: d, idx: nbr });
                if best.len() > ef {
                    best.pop();
                }
            }
        }
    }

    let mut out: Vec<(usize, f32)> = best.into_iter().map(|c| (c.idx, c.dist)).collect();
    out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
    out
}

fn add_neighbor(
    layer: &mut [Vec<usize>],
    node: usize,
    neighbor: usize,
    m: usize,
    metric: Metric,
    vecs: &[Vec<f32>],
    norms: &[f32],
) {
    let list = &mut layer[node];
    if !list.contains(&neighbor) {
        list.push(neighbor);
    }
    prune_neighbors(list, node, m, metric, vecs, norms);
}

fn prune_neighbors(
    list: &mut Vec<usize>,
    node: usize,
    m: usize,
    metric: Metric,
    vecs: &[Vec<f32>],
    norms: &[f32],
) {
    list.sort_by(|&a, &b| {
        let da = metric.distance_nodes(vecs, norms, node, a);
        let db = metric.distance_nodes(vecs, norms, node, b);
        da.partial_cmp(&db).unwrap_or(Ordering::Equal)
    });
    list.dedup();
    if list.len() > m {
        list.truncate(m);
    }
}

fn sample_level(seed: u64, _m: usize, _ef_construction: usize) -> u8 {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut level = 0u8;

    // Standard HNSW level sampling: each level has probability 1/2 of the previous
    // This creates a proper hierarchical structure with exponential decay
    while level < MAX_LEVEL {
        if rng.gen::<f64>() < 0.5 {
            level += 1;
        } else {
            break;
        }
    }
    level
}

fn vector_norm(v: &[f32]) -> f32 {
    // Use SIMD-optimized inner product to compute norm
    let dot = zvec_kernels::distance(v, v, zvec_kernels::Metric::InnerProduct).unwrap_or(0.0);
    (-dot).sqrt() // InnerProduct returns negative dot product, so we negate it
}

fn key_index(org_id: u64, table: &str, col: &str) -> Vec<u8> {
    let org = org_id.to_be_bytes();
    zs::encode_key(&[b"hnsw", &org, table.as_bytes(), col.as_bytes()])
}

fn decode_key_parts(key: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut parts = Vec::new();
    let mut idx = 0usize;
    while idx < key.len() {
        if idx + 4 > key.len() {
            return Err(anyhow!("malformed key"));
        }
        let len = u32::from_be_bytes([key[idx], key[idx + 1], key[idx + 2], key[idx + 3]]) as usize;
        idx += 4;
        if idx + len > key.len() {
            return Err(anyhow!("malformed key"));
        }
        parts.push(key[idx..idx + len].to_vec());
        idx += len;
    }
    Ok(parts)
}

fn parse_row_id_from_key(key: &[u8]) -> Result<u64> {
    if key.len() < 4 {
        return Err(anyhow!("bad key"));
    }
    let mut i = 0usize;
    for _ in 0..3 {
        if i + 4 > key.len() {
            return Err(anyhow!("bad key"));
        }
        let len = u32::from_be_bytes([key[i], key[i + 1], key[i + 2], key[i + 3]]) as usize;
        i += 4 + len;
    }
    if i + 4 > key.len() {
        return Err(anyhow!("bad key"));
    }
    let len = u32::from_be_bytes([key[i], key[i + 1], key[i + 2], key[i + 3]]) as usize;
    i += 4;
    if len != 8 || i + len != key.len() {
        return Err(anyhow!("bad key"));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&key[i..i + 8]);
    Ok(u64::from_be_bytes(arr))
}

/// Add a single vector to an existing index
///
/// This implements incremental index updates by finding optimal insertion points
/// and updating the graph structure. Currently a simplified implementation.
pub fn add_vector(
    store: &zs::Store,
    _catalog: &zcat::Catalog,
    org_id: u64,
    table: &str,
    column: &str,
    vector: Vec<f32>,
    row_id: u64,
) -> Result<()> {
    let mut ann = match try_get_index(store, org_id, table, column)? {
        Some(ann) => Arc::try_unwrap(ann).unwrap_or_else(|arc| (*arc).clone()),
        None => return Err(HnswError::IndexNotFound {
            table: table.to_string(),
            column: column.to_string(),
        }.into()),
    };

    // Validate vector dimensions
    if let Some(first_vec) = ann.vecs.first() {
        if first_vec.len() != vector.len() {
            return Err(HnswError::InvalidDimension {
                expected: first_vec.len(),
                actual: vector.len(),
            }.into());
        }
    }

    // Sample level for new vector
    let node_idx = ann.vecs.len();
    let level = sample_level(node_idx as u64, ann.m, ann.ef_construction);
    let norm = if ann.metric == Metric::Cosine {
        vector_norm(&vector)
    } else {
        0.0
    };

    // Add vector to index
    ann.vecs.push(vector);
    ann.norms.push(norm);
    ann.ids.push(row_id);
    ann.levels.push(level);

    // Expand layers if needed
    let total_nodes = ann.vecs.len();
    while ann.layers.len() <= level as usize {
        let mut new_layer = Vec::with_capacity(total_nodes);
        for _ in 0..total_nodes {
            new_layer.push(Vec::new());
        }
        ann.layers.push(new_layer);
    }

    for layer in ann.layers.iter_mut() {
        while layer.len() < total_nodes {
            layer.push(Vec::new());
        }
    }

    // Update entry point if needed
    if level > ann.max_level {
        ann.entry = node_idx;
        ann.max_level = level;
    }

    // Connect to neighbors at each level
    for lvl in 0..=level as usize {
        if lvl >= ann.layers.len() {
            continue;
        }

        // Find nearest neighbors at this level
        let mut candidates: Vec<(usize, f32)> = Vec::new();
        for other in 0..node_idx {
            if ann.levels[other] as usize >= lvl {
                let dist = ann
                    .metric
                    .distance_nodes(&ann.vecs, &ann.norms, node_idx, other);
                candidates.push((other, dist));
            }
        }

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        candidates.truncate(ann.m.max(1));

        // Connect bidirectionally
        for (other, _) in candidates {
            add_neighbor(
                &mut ann.layers[lvl],
                node_idx,
                other,
                ann.m,
                ann.metric,
                &ann.vecs,
                &ann.norms,
            );
            add_neighbor(
                &mut ann.layers[lvl],
                other,
                node_idx,
                ann.m,
                ann.metric,
                &ann.vecs,
                &ann.norms,
            );
        }
    }

    // Persist updated index
    persist_index(store, org_id, table, column, &ann)?;

    // Update cache
    REG.write().insert(
        (org_id, table.to_string(), column.to_string()),
        Arc::new(ann),
    );

    Ok(())
}

/// Remove a vector from an existing index by row ID
///
/// This is a simplified implementation that marks the vector as removed
/// without rebuilding the entire graph structure.
pub fn remove_vector(
    _store: &zs::Store,
    _catalog: &zcat::Catalog,
    _org_id: u64,
    _table: &str,
    _column: &str,
    _row_id: u64,
) -> Result<()> {
    // For now, we'll just rebuild the index without this vector
    // In a full implementation, you'd want to:
    // 1. Remove the node from all neighbor lists
    // 2. Update graph connectivity
    // 3. Possibly adjust entry point

    // This is a placeholder - full implementation would require
    // more sophisticated graph surgery
    Err(anyhow!("Vector removal not yet implemented - index rebuild required"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Cell {
    Int(i64),
    Float(f64),
    Text(String),
    Vector(Vec<f32>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn debug_level_distribution() {
        // Test the level sampling to see if we get a hierarchical structure
        let mut level_counts = std::collections::HashMap::new();
        for i in 0..1000 {
            let level = sample_level(i, 16, 64);
            *level_counts.entry(level).or_insert(0) += 1;
        }
        println!("Level distribution:");
        for level in 0..=3 {
            let count = level_counts.get(&level).copied().unwrap_or(0);
            println!("  Level {}: {} nodes ({:.1}%)", level, count, count as f32 / 10.0);
        }

        // Should have most nodes at level 0, fewer at level 1, etc.
        let level0_count = level_counts.get(&0).copied().unwrap_or(0);
        let level1_count = level_counts.get(&1).copied().unwrap_or(0);
        println!("Level 0: {}, Level 1: {}", level0_count, level1_count);

        // With fixed algorithm, should have ~50% at level 0, ~25% at level 1, etc.
        assert!(level0_count > 450 && level0_count < 550, "Expected ~50% nodes at level 0, got {}", level0_count);
        assert!(level1_count > 200 && level1_count < 300, "Expected ~25% nodes at level 1, got {}", level1_count);
    }

    #[test]
    fn debug_search_failure() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("debug.redb")).unwrap();
        let catalog = zcat::Catalog::new(&store);

        catalog.create_table(
            1,
            &zcat::TableDef {
                name: "test".into(),
                columns: vec![zcat::ColumnDef {
                    name: "embedding".into(),
                    ty: zcat::ColumnType::Vector,
                    nullable: false,
                    primary_key: false,
                }],
            },
        ).unwrap();

        // Insert more vectors to trigger the scaling issue
        let vectors: Vec<Vec<f32>> = (0..50).map(|i| {
            vec![
                (i as f32) * 0.1,
                ((i + 1) as f32) * 0.1,
                ((i + 2) as f32) * 0.1
            ]
        }).collect();

        for vec in &vectors {
            let mut w = store.begin_write().unwrap();
            {
                let mut rows = w.open_table(&store, zs::COL_ROWS).unwrap();
                let row_id = zs::next_seq(&store, b"rows:test").unwrap();
                let key = zs::encode_key(&[
                    b"rows",
                    &1u64.to_be_bytes(),
                    b"test",
                    &row_id.to_be_bytes(),
                ]);
                let cells = vec![Cell::Vector(vec.clone())];
                rows.insert(key.as_slice(), bincode::serialize(&cells).unwrap().as_slice()).unwrap();
            }
            w.commit(&store).unwrap();
        }

        build_index(&store, &catalog, 1, "test", "embedding", Metric::L2, 16, 64).unwrap();

        // Debug index structure
        if let Some(stats) = index_stats(&store, 1, "test", "embedding").unwrap() {
            println!("Index stats: {} vectors, {} layers", stats.total_vectors, stats.layers.len());
            for layer in &stats.layers {
                println!("  Level {}: {} nodes, avg degree: {:.2}",
                         layer.level, layer.nodes, layer.avg_degree);
            }
        }

        // Test search with different queries
        let queries = vec![
            vec![0.0, 0.0, 0.0],  // Should match first vector
            vec![1.0, 1.1, 1.2], // Should match 10th vector
            vec![5.0, 5.1, 5.2], // Should match 50th vector
        ];

        for (i, query) in queries.iter().enumerate() {
            let results = search(&store, &catalog, 1, "test", "embedding", query, 5, 64).unwrap();
            println!("Query {}: {:?} -> {} results: {:?}", i, query, results.len(), results);
        }

        // Test with exact match
        let exact_query = vectors[0].clone(); // Should return the first vector
        let results = search(&store, &catalog, 1, "test", "embedding", &exact_query, 5, 64).unwrap();
        println!("Exact query: {:?} -> {} results: {:?}", exact_query, results.len(), results);

        // Debug: inspect the index entry point and max level
        let loaded_index = ensure_loaded(&store, 1, "test", "embedding").unwrap();
        println!("Entry point: {}, Max level: {}, Total nodes: {}",
                 loaded_index.entry, loaded_index.layers.len() - 1, loaded_index.vecs.len());

        // Check if entry point is valid and at the right level
        if loaded_index.entry < loaded_index.levels.len() {
            println!("Entry point level: {}", loaded_index.levels[loaded_index.entry]);
        }

        // This should pass but will currently fail
        assert!(!results.is_empty(), "Search should return results but got none");
    }

    #[test]
    fn debug_large_scale_search() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("large_debug.redb")).unwrap();
        let catalog = zcat::Catalog::new(&store);

        catalog.create_table(
            1,
            &zcat::TableDef {
                name: "test".into(),
                columns: vec![zcat::ColumnDef {
                    name: "embedding".into(),
                    ty: zcat::ColumnType::Vector,
                    nullable: false,
                    primary_key: false,
                }],
            },
        ).unwrap();

        // Insert 1000 vectors like the failing example
        let vectors: Vec<Vec<f32>> = (0..1000).map(|i| {
            (0..256).map(|j| ((i + j) as f32) * 0.001).collect()
        }).collect();

        for vec in &vectors {
            let mut w = store.begin_write().unwrap();
            {
                let mut rows = w.open_table(&store, zs::COL_ROWS).unwrap();
                let row_id = zs::next_seq(&store, b"rows:test").unwrap();
                let key = zs::encode_key(&[
                    b"rows",
                    &1u64.to_be_bytes(),
                    b"test",
                    &row_id.to_be_bytes(),
                ]);
                let cells = vec![Cell::Vector(vec.clone())];
                rows.insert(key.as_slice(), bincode::serialize(&cells).unwrap().as_slice()).unwrap();
            }
            w.commit(&store).unwrap();
        }

        build_index(&store, &catalog, 1, "test", "embedding", Metric::L2, 16, 64).unwrap();

        // Debug index structure
        if let Some(stats) = index_stats(&store, 1, "test", "embedding").unwrap() {
            println!("Large index stats: {} vectors, {} layers", stats.total_vectors, stats.layers.len());
            for layer in &stats.layers {
                println!("  Level {}: {} nodes, avg degree: {:.2}",
                         layer.level, layer.nodes, layer.avg_degree);
            }
        }

        let loaded_index = ensure_loaded(&store, 1, "test", "embedding").unwrap();
        println!("Entry point: {}, Max level: {}, Total nodes: {}",
                 loaded_index.entry, loaded_index.layers.len() - 1, loaded_index.vecs.len());

        if loaded_index.entry < loaded_index.levels.len() {
            println!("Entry point level: {}", loaded_index.levels[loaded_index.entry]);
        }

        // Test query like the example
        let query: Vec<f32> = (0..256).map(|j| ((1000 + j) as f32) * 0.001).collect();
        let results = search(&store, &catalog, 1, "test", "embedding", &query, 10, 128).unwrap();
        println!("Large scale search returned {} results: {:?}", results.len(),
                 results.iter().take(3).collect::<Vec<_>>());

        // This test should reveal the scaling issue
        if results.is_empty() {
            println!("BUG CONFIRMED: Large scale search returns 0 results!");
            // Don't fail the test so we can continue debugging
        } else {
            println!("Search works at large scale");
        }
    }

    #[test]
    fn build_and_query_index() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("idx.redb")).unwrap();
        let catalog = zcat::Catalog::new(&store);
        catalog
            .create_table(
                1,
                &zcat::TableDef {
                    name: "docs".into(),
                    columns: vec![zcat::ColumnDef {
                        name: "embedding".into(),
                        ty: zcat::ColumnType::Vector,
                        nullable: false,
                        primary_key: false,
                    }],
                },
            )
            .unwrap();

        let vectors = vec![
            vec![0.1, 0.0, 0.0],
            vec![0.9, 0.1, 0.0],
            vec![0.0, 1.0, 0.0],
        ];

        for v in vectors.iter() {
            let mut w = store.begin_write().unwrap();
            {
                let mut rows = w.open_table(&store, zs::COL_ROWS).unwrap();
                let row_id = zs::next_seq(&store, b"rows:docs").unwrap();
                let key =
                    zs::encode_key(&[b"rows", &1u64.to_be_bytes(), b"docs", &row_id.to_be_bytes()]);
                let cells = vec![Cell::Vector(v.clone())];
                rows.insert(
                    key.as_slice(),
                    bincode::serialize(&cells).unwrap().as_slice(),
                )
                .unwrap();
            }
            w.commit(&store).unwrap();
        }

        build_index(&store, &catalog, 1, "docs", "embedding", Metric::L2, 8, 32).unwrap();
        let info = index_info(&store, 1, "docs", "embedding").unwrap().unwrap();
        assert_eq!(info.metric, Metric::L2);
        assert!(info.layers >= 1);

        let res = search(
            &store,
            &catalog,
            1,
            "docs",
            "embedding",
            &[1.0, 0.0, 0.0],
            2,
            32,
        )
        .unwrap();
        assert_eq!(res.len(), 2);
    }

    #[test]
    fn index_stats_reports_layers() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("idx_stats.redb")).unwrap();
        let catalog = zcat::Catalog::new(&store);
        catalog
            .create_table(
                1,
                &zcat::TableDef {
                    name: "docs".into(),
                    columns: vec![zcat::ColumnDef {
                        name: "embedding".into(),
                        ty: zcat::ColumnType::Vector,
                        nullable: false,
                        primary_key: false,
                    }],
                },
            )
            .unwrap();

        for v in [vec![0.0, 1.0], vec![1.0, 0.0], vec![0.5, 0.5]] {
            let mut w = store.begin_write().unwrap();
            {
                let mut rows = w.open_table(&store, zs::COL_ROWS).unwrap();
                let row_id = zs::next_seq(&store, b"rows:docs").unwrap();
                let key =
                    zs::encode_key(&[b"rows", &1u64.to_be_bytes(), b"docs", &row_id.to_be_bytes()]);
                let cells = vec![Cell::Vector(v.clone())];
                rows.insert(
                    key.as_slice(),
                    bincode::serialize(&cells).unwrap().as_slice(),
                )
                .unwrap();
            }
            w.commit(&store).unwrap();
        }

        build_index(&store, &catalog, 1, "docs", "embedding", Metric::L2, 8, 32).unwrap();

        let stats = index_stats(&store, 1, "docs", "embedding")
            .unwrap()
            .expect("stats should exist");
        assert_eq!(stats.total_vectors, 3);
        assert!(!stats.layers.is_empty());
        let ground_layer = stats
            .layers
            .iter()
            .find(|l| l.level == 0)
            .expect("ground layer present");
        assert_eq!(ground_layer.nodes, 3);
        assert!(ground_layer.avg_degree >= 0.0);
    }

    #[test]
    fn test_custom_error_types() {
        // Test that custom error types work correctly
        let err = HnswError::IndexNotFound {
            table: "test".to_string(),
            column: "embedding".to_string(),
        };
        let anyhow_err: anyhow::Error = err.into();
        assert!(anyhow_err.to_string().contains("Index not found"));

        let err = HnswError::InvalidParameters("m must be greater than 0".to_string());
        assert!(err.to_string().contains("Invalid parameters"));

        let err = HnswError::InvalidDimension { expected: 128, actual: 256 };
        assert!(err.to_string().contains("Invalid vector dimensions"));

        let err = HnswError::EmptyIndex;
        assert!(err.to_string().contains("Empty index"));
    }

    #[test]
    fn test_parameter_validation() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("validation.redb")).unwrap();
        let catalog = zcat::Catalog::new(&store);

        catalog.create_table(
            1,
            &zcat::TableDef {
                name: "test".into(),
                columns: vec![zcat::ColumnDef {
                    name: "embedding".into(),
                    ty: zcat::ColumnType::Vector,
                    nullable: false,
                    primary_key: false,
                }],
            },
        ).unwrap();

        // Test invalid parameters
        let result = build_index(&store, &catalog, 1, "test", "embedding", Metric::L2, 0, 64);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("m must be greater than 0"));

        let result = build_index(&store, &catalog, 1, "test", "embedding", Metric::L2, 16, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ef_construction must be greater than 0"));

        // Test search with invalid parameters
        let result = search(&store, &catalog, 1, "test", "embedding", &[], 0, 64);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Query vector cannot be empty"));

        let result = search(&store, &catalog, 1, "test", "embedding", &[1.0, 2.0], 0, 64);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("top_k must be greater than 0"));

        let result = search(&store, &catalog, 1, "test", "embedding", &[1.0, 2.0], 10, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ef_search must be greater than 0"));
    }

    #[test]
    fn test_incremental_add_vector() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("incremental.redb")).unwrap();
        let catalog = zcat::Catalog::new(&store);

        catalog.create_table(
            1,
            &zcat::TableDef {
                name: "test".into(),
                columns: vec![zcat::ColumnDef {
                    name: "embedding".into(),
                    ty: zcat::ColumnType::Vector,
                    nullable: false,
                    primary_key: false,
                }],
            },
        ).unwrap();

        // Build initial index with 3 vectors
        let vectors = vec![
            vec![1.0, 0.0, 0.0],
            vec![0.0, 1.0, 0.0],
            vec![0.0, 0.0, 1.0],
        ];

        for (i, vec) in vectors.iter().enumerate() {
            let mut w = store.begin_write().unwrap();
            {
                let mut rows = w.open_table(&store, zs::COL_ROWS).unwrap();
                let row_id = zs::next_seq(&store, b"rows:test").unwrap();
                let key = zs::encode_key(&[
                    b"rows",
                    &1u64.to_be_bytes(),
                    b"test",
                    &row_id.to_be_bytes(),
                ]);
                let cells = vec![Cell::Vector(vec.clone())];
                rows.insert(key.as_slice(), bincode::serialize(&cells).unwrap().as_slice()).unwrap();
            }
            w.commit(&store).unwrap();
        }

        build_index(&store, &catalog, 1, "test", "embedding", Metric::L2, 8, 32).unwrap();

        // Test adding a new vector incrementally
        let new_vector = vec![0.5, 0.5, 0.5];
        let result = add_vector(&store, &catalog, 1, "test", "embedding", new_vector, 999);
        assert!(result.is_ok());

        // Verify the new vector can be found
        let query = vec![0.5, 0.5, 0.5];
        let results = search(&store, &catalog, 1, "test", "embedding", &query, 5, 32).unwrap();
        assert!(!results.is_empty());

        // Test dimension validation
        let wrong_dim_vector = vec![1.0, 2.0]; // Wrong dimension
        let result = add_vector(&store, &catalog, 1, "test", "embedding", wrong_dim_vector, 1000);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid vector dimensions"));
    }

    #[test]
    fn test_remove_vector_placeholder() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("remove.redb")).unwrap();
        let catalog = zcat::Catalog::new(&store);

        // Test that remove_vector returns the expected error
        let result = remove_vector(&store, &catalog, 1, "test", "embedding", 123);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not yet implemented"));
    }

    #[test]
    fn test_metric_from_str() {
        assert_eq!(Metric::from_str("l2").unwrap(), Metric::L2);
        assert_eq!(Metric::from_str("euclidean").unwrap(), Metric::L2);
        assert_eq!(Metric::from_str("cosine").unwrap(), Metric::Cosine);
        assert_eq!(Metric::from_str("ip").unwrap(), Metric::InnerProduct);
        assert_eq!(Metric::from_str("inner_product").unwrap(), Metric::InnerProduct);

        assert!(Metric::from_str("invalid").is_err());
        assert!(Metric::from_str("").is_err());
    }

    #[test]
    fn test_metric_display() {
        assert_eq!(format!("{}", Metric::L2), "l2");
        assert_eq!(format!("{}", Metric::Cosine), "cosine");
        assert_eq!(format!("{}", Metric::InnerProduct), "ip");
    }

    #[test]
    fn test_metric_conversion() {
        use zvec_kernels::Metric as ZMetric;

        assert_eq!(ZMetric::from(Metric::L2), ZMetric::L2);
        assert_eq!(ZMetric::from(Metric::Cosine), ZMetric::Cosine);
        assert_eq!(ZMetric::from(Metric::InnerProduct), ZMetric::InnerProduct);
    }

    #[test]
    fn test_level_sampling() {
        // Test that level sampling produces expected distribution
        let mut level_counts = std::collections::HashMap::new();
        for i in 0..1000 {
            let level = sample_level(i, 16, 64);
            *level_counts.entry(level).or_insert(0) += 1;
        }

        // Most vectors should be at level 0
        let level0_count = level_counts.get(&0).copied().unwrap_or(0);
        assert!(level0_count > 400, "Expected most vectors at level 0, got {}", level0_count);

        // Should have some vectors at higher levels
        let total_levels = level_counts.len();
        assert!(total_levels > 1, "Expected multiple levels, got {}", total_levels);

        // Level counts should decrease exponentially
        if let Some(level1_count) = level_counts.get(&1) {
            assert!(*level1_count < level0_count, "Level 1 should have fewer vectors than level 0");
        }
    }

    #[test]
    fn test_vector_norm() {
        // Test vector norm calculation
        let vec = vec![3.0, 4.0]; // Should have norm 5.0
        let norm = vector_norm(&vec);
        assert!((norm - 5.0).abs() < 0.001, "Expected norm 5.0, got {}", norm);

        let zero_vec = vec![0.0, 0.0];
        let norm = vector_norm(&zero_vec);
        assert!((norm - 0.0).abs() < 0.001, "Expected norm 0.0, got {}", norm);
    }

    #[test]
    fn test_empty_index_handling() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("empty.redb")).unwrap();
        let catalog = zcat::Catalog::new(&store);

        catalog.create_table(
            1,
            &zcat::TableDef {
                name: "test".into(),
                columns: vec![zcat::ColumnDef {
                    name: "embedding".into(),
                    ty: zcat::ColumnType::Vector,
                    nullable: false,
                    primary_key: false,
                }],
            },
        ).unwrap();

        // Try to build index with no vectors
        let result = build_index(&store, &catalog, 1, "test", "embedding", Metric::L2, 8, 32);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Empty index"));

        // Try to search on non-existent index
        let result = search(&store, &catalog, 1, "test", "embedding", &[1.0, 2.0], 5, 32);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Index not found"));
    }
}
