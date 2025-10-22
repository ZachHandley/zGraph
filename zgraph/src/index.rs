use serde::{Deserialize, Serialize};

use crate::types::RowId;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Metric {
    L2,
    Cosine,
    Dot,
}

fn l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(x, y)| {
            let d = x - y;
            d * d
        })
        .sum::<f32>()
        .sqrt()
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let num = dot(a, b);
    let na = (a.iter().map(|x| x * x).sum::<f32>()).sqrt();
    let nb = (b.iter().map(|x| x * x).sum::<f32>()).sqrt();
    // Smaller is better for distances: return 1 - cosine similarity
    1.0 - (num / (na * nb + 1e-12))
}

fn dist(metric: Metric, a: &[f32], b: &[f32]) -> f32 {
    match metric {
        Metric::L2 => l2(a, b),
        Metric::Cosine => cosine(a, b),
        Metric::Dot => -dot(a, b), // higher dot -> closer; use negative to keep "lower is better"
    }
}

pub trait VectorIndex {
    fn dims(&self) -> usize;
    fn len(&self) -> usize;
    fn upsert(&mut self, id: RowId, vec: &[f32]);
    fn remove(&mut self, id: RowId);
    fn knn(&self, query: &[f32], k: usize) -> Vec<(RowId, f32)>;
}

pub enum VectorIndexKind {
    Brute(BruteForceIndex),
    #[cfg(feature = "hnsw")]
    Hnsw(HnswIndex),
}

impl VectorIndexKind {
    pub fn new(metric: Metric, dims: Option<usize>) -> Self {
        let d = dims.unwrap_or(0);
        #[cfg(feature = "hnsw")]
        {
            return VectorIndexKind::Hnsw(HnswIndex::new(metric, d));
        }
        #[allow(unreachable_code)]
        VectorIndexKind::Brute(BruteForceIndex::new(metric, d))
    }

    pub fn dims(&self) -> usize {
        match self {
            VectorIndexKind::Brute(i) => i.dims(),
            #[cfg(feature = "hnsw")]
            VectorIndexKind::Hnsw(i) => i.dims(),
        }
    }
    pub fn len(&self) -> usize {
        match self {
            VectorIndexKind::Brute(i) => i.len(),
            #[cfg(feature = "hnsw")]
            VectorIndexKind::Hnsw(i) => i.len(),
        }
    }
    pub fn upsert(&mut self, id: RowId, vec: &[f32]) {
        match self {
            VectorIndexKind::Brute(i) => i.upsert(id, vec),
            #[cfg(feature = "hnsw")]
            VectorIndexKind::Hnsw(i) => i.upsert(id, vec),
        }
    }
    pub fn remove(&mut self, id: RowId) {
        match self {
            VectorIndexKind::Brute(i) => i.remove(id),
            #[cfg(feature = "hnsw")]
            VectorIndexKind::Hnsw(i) => i.remove(id),
        }
    }
    pub fn knn(&self, query: &[f32], k: usize) -> Vec<(RowId, f32)> {
        match self {
            VectorIndexKind::Brute(i) => i.knn(query, k),
            #[cfg(feature = "hnsw")]
            VectorIndexKind::Hnsw(i) => i.knn(query, k),
        }
    }
}

#[derive(Clone)]
pub struct BruteForceIndex {
    metric: Metric,
    dims: usize,
    data: Vec<(RowId, Vec<f32>)>,
}

impl BruteForceIndex {
    pub fn new(metric: Metric, dims: usize) -> Self {
        Self {
            metric,
            dims,
            data: Vec::new(),
        }
    }
}

impl VectorIndex for BruteForceIndex {
    fn dims(&self) -> usize {
        self.dims
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn upsert(&mut self, id: RowId, vec: &[f32]) {
        if self.dims == 0 {
            self.dims = vec.len();
        }
        if vec.len() != self.dims {
            return;
        }
        if let Some(pos) = self.data.iter().position(|(rid, _)| *rid == id) {
            self.data[pos] = (id, vec.to_vec());
        } else {
            self.data.push((id, vec.to_vec()));
        }
    }

    fn remove(&mut self, id: RowId) {
        if let Some(pos) = self.data.iter().position(|(rid, _)| *rid == id) {
            self.data.swap_remove(pos);
        }
    }

    fn knn(&self, query: &[f32], k: usize) -> Vec<(RowId, f32)> {
        let mut v: Vec<(RowId, f32)> = self
            .data
            .iter()
            .map(|(rid, vec)| (*rid, dist(self.metric, query, vec)))
            .collect();
        v.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        v.truncate(k);
        v
    }
}

#[cfg(feature = "hnsw")]
pub struct HnswIndex {
    metric: Metric,
    dims: usize,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
    ann: AnnIndex,
}

#[cfg(feature = "hnsw")]
impl HnswIndex {
    pub fn new(metric: Metric, dims: usize) -> Self {
        Self {
            metric,
            dims,
            m: 16,
            ef_construction: 64,
            ef_search: 64,
            ann: AnnIndex::new(metric),
        }
    }
}

#[cfg(feature = "hnsw")]
impl VectorIndex for HnswIndex {
    fn dims(&self) -> usize {
        self.dims
    }
    fn len(&self) -> usize {
        self.ann.len()
    }
    fn upsert(&mut self, id: RowId, vec: &[f32]) {
        if self.dims == 0 {
            self.dims = vec.len();
        }
        if vec.len() != self.dims {
            return;
        }
        self.ann.add_point(vec.to_vec(), id, self.m, self.ef_construction);
    }
    fn remove(&mut self, _id: RowId) {
        // Not supported in this minimal wrapper.
    }
    fn knn(&self, query: &[f32], k: usize) -> Vec<(RowId, f32)> {
        self.ann.search(query, k, self.ef_search)
    }
}

#[cfg(feature = "hnsw")]
struct AnnIndex {
    metric: Metric,
    entry: usize,
    max_level: u8,
    levels: Vec<u8>,
    vecs: Vec<Vec<f32>>,
    ids: Vec<RowId>,
    layers: Vec<Vec<Vec<usize>>>,
}

#[cfg(feature = "hnsw")]
impl AnnIndex {
    fn new(metric: Metric) -> Self {
        Self {
            metric,
            entry: 0,
            max_level: 0,
            levels: Vec::new(),
            vecs: Vec::new(),
            ids: Vec::new(),
            layers: Vec::new(),
        }
    }

    fn len(&self) -> usize {
        self.vecs.len()
    }

    fn add_point(&mut self, vec: Vec<f32>, id: RowId, m: usize, _ef_construction: usize) {
        let node_idx = self.vecs.len();
        let level = sample_level(node_idx as u64);
        self.vecs.push(vec);
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
                    let dist = dist(self.metric, &self.vecs[node_idx], &self.vecs[other]);
                    candidates.push((other, dist));
                }
            }
            candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            candidates.truncate(m.max(1));
            for (other, _) in candidates {
                add_neighbor(&mut self.layers[lvl], node_idx, other, m, self.metric, &self.vecs);
                add_neighbor(&mut self.layers[lvl], other, node_idx, m, self.metric, &self.vecs);
            }
        }
    }

    fn search(&self, query: &[f32], top_k: usize, ef_search: usize) -> Vec<(RowId, f32)> {
        if self.vecs.is_empty() {
            return Vec::new();
        }
        // Greedy descent from top level
        let mut curr = self.entry;
        for lvl in (1..=self.max_level).rev() {
            curr = greedy_descent(self, curr, lvl as usize, query);
        }
        // Search layer 0
        let results = search_layer(self, curr, 0, query, ef_search.max(top_k));
        let mut out: Vec<(RowId, f32)> = results
            .into_iter()
            .map(|(idx, d)| (self.ids[idx], d))
            .collect();
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        out.truncate(top_k);
        out
    }
}

#[cfg(feature = "hnsw")]
fn greedy_descent(ann: &AnnIndex, start: usize, level: usize, query: &[f32]) -> usize {
    let mut current = start;
    let mut best = dist(ann.metric, &ann.vecs[current], query);
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
            let d = dist(ann.metric, &ann.vecs[nbr], query);
            if d < next_dist {
                next_dist = d;
                next_best = nbr;
                improved = true;
            }
        }
        if !improved { break; }
        current = next_best;
        best = next_dist;
        visited.insert(current);
        if visited.len() > ann.vecs.len() { break; }
    }
    current
}

#[cfg(feature = "hnsw")]
fn search_layer(
    ann: &AnnIndex,
    entry: usize,
    level: usize,
    query: &[f32],
    ef: usize,
) -> Vec<(usize, f32)> {
    if ann.vecs.is_empty() || level >= ann.layers.len() || entry >= ann.vecs.len() { return Vec::new(); }
    let ef = ef.min(ann.vecs.len()).max(1);
    #[derive(Clone, Copy)]
    struct MinC{ dist:f32, idx:usize }
    impl PartialEq for MinC { fn eq(&self, o:&Self)->bool{ self.idx==o.idx } }
    impl Eq for MinC {}
    impl PartialOrd for MinC { fn partial_cmp(&self,o:&Self)->Option<std::cmp::Ordering>{ o.dist.partial_cmp(&self.dist) } }
    impl Ord for MinC { fn cmp(&self,o:&Self)->std::cmp::Ordering{ self.partial_cmp(o).unwrap() } }
    #[derive(Clone, Copy)]
    struct MaxC{ dist:f32, idx:usize }
    impl PartialEq for MaxC { fn eq(&self, o:&Self)->bool{ self.idx==o.idx } }
    impl Eq for MaxC {}
    impl PartialOrd for MaxC { fn partial_cmp(&self,o:&Self)->Option<std::cmp::Ordering>{ self.dist.partial_cmp(&o.dist) } }
    impl Ord for MaxC { fn cmp(&self,o:&Self)->std::cmp::Ordering{ self.partial_cmp(o).unwrap() } }
    let mut visited = vec![false; ann.vecs.len()];
    let mut candidates = std::collections::BinaryHeap::new();
    let mut best = std::collections::BinaryHeap::new();
    let entry_dist = dist(ann.metric, &ann.vecs[entry], query);
    candidates.push(MinC{dist:entry_dist, idx:entry});
    best.push(MaxC{dist:entry_dist, idx:entry});
    visited[entry]=true;
    while let Some(MinC{dist: current_dist, idx}) = candidates.pop() {
        let worst = best.peek().map(|c:&MaxC| c.dist).unwrap_or(f32::MAX);
        if best.len() >= ef && current_dist > worst { break; }
        for &nbr in &ann.layers[level][idx] {
            if nbr>=visited.len() || visited[nbr] { continue; }
            visited[nbr]=true;
            let d = dist(ann.metric, &ann.vecs[nbr], query);
            if best.len() < ef || d < worst {
                candidates.push(MinC{dist:d, idx:nbr});
                best.push(MaxC{dist:d, idx:nbr});
                if best.len() > ef { best.pop(); }
            }
        }
    }
    let mut out: Vec<(usize,f32)> = best.into_iter().map(|c| (c.idx, c.dist)).collect();
    out.sort_by(|a,b| a.1.partial_cmp(&b.1).unwrap());
    out
}

#[cfg(feature = "hnsw")]
fn add_neighbor(layer: &mut [Vec<usize>], node: usize, neighbor: usize, m: usize, metric: Metric, vecs: &[Vec<f32>]) {
    let list = &mut layer[node];
    if !list.contains(&neighbor) { list.push(neighbor); }
    prune_neighbors(list, node, m, metric, vecs);
}

#[cfg(feature = "hnsw")]
fn prune_neighbors(list: &mut Vec<usize>, node: usize, m: usize, metric: Metric, vecs: &[Vec<f32>]) {
    list.sort_by(|&a,&b| {
        let da = dist(metric, &vecs[node], &vecs[a]);
        let db = dist(metric, &vecs[node], &vecs[b]);
        da.partial_cmp(&db).unwrap()
    });
    list.dedup();
    if list.len() > m { list.truncate(m); }
}

#[cfg(feature = "hnsw")]
fn sample_level(mut seed: u64) -> u8 {
    let mut level = 0u8;
    while level < 16 {
        // xorshift64*
        seed ^= seed >> 12;
        seed ^= seed << 25;
        seed ^= seed >> 27;
        let bit = seed & 1;
        if bit == 1 { level += 1; } else { break; }
    }
    level
}
