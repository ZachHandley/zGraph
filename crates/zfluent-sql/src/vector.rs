/*!
Vector-specific query operations for similarity search.

This module provides specialized functionality for vector similarity queries,
integrating with ZRUSTDB's vector indexing capabilities (HNSW) and distance metrics.
*/

use crate::{
    query::{QueryParameterValue, query_states},
    select::SelectQueryBuilder,
};
use tracing::debug;
use zann_hnsw::{Metric as HnswMetric};
use zvec_kernels::Metric as KernelMetric;

/// Vector query builder for similarity operations
pub struct VectorQuery {
    /// Target vector for similarity search
    query_vector: Vec<f32>,
    /// Distance metric to use
    metric: VectorDistance,
    /// Number of nearest neighbors to return
    k: usize,
    /// Similarity threshold (optional)
    threshold: Option<f32>,
    /// HNSW search parameter (ef_search)
    ef_search: Option<usize>,
    /// Whether to include distance scores in results
    include_scores: bool,
    /// Additional filters to apply
    filters: Vec<(String, String, QueryParameterValue)>,
}

/// Supported vector distance metrics
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VectorDistance {
    /// Euclidean (L2) distance - <->
    L2,
    /// Cosine similarity (converted to distance) - <=>
    Cosine,
    /// Inner product distance - <#>
    InnerProduct,
    /// Manhattan (L1) distance
    Manhattan,
}

impl VectorQuery {
    /// Create a new vector similarity query
    pub fn new() -> Self {
        Self {
            query_vector: Vec::new(),
            metric: VectorDistance::L2,
            k: 10,
            threshold: None,
            ef_search: None,
            include_scores: false,
            filters: Vec::new(),
        }
    }

    /// Create a new vector query with target vector
    pub fn for_vector(target: Vec<f32>) -> Self {
        Self {
            query_vector: target,
            metric: VectorDistance::L2,
            k: 10,
            threshold: None,
            ef_search: None,
            include_scores: false,
            filters: Vec::new(),
        }
    }

    /// Find vectors similar to the target vector
    pub fn similar_to(mut self, target: &[f32]) -> Self {
        debug!("Setting target vector with {} dimensions", target.len());
        self.query_vector = target.to_vec();
        self
    }

    /// Specify the distance metric to use
    pub fn with_distance(mut self, metric: VectorDistance) -> Self {
        debug!("Setting distance metric: {:?}", metric);
        self.metric = metric;
        self
    }

    /// Set the number of similar vectors to return (K in KNN)
    pub fn limit(mut self, count: usize) -> Self {
        debug!("Setting K (nearest neighbors): {}", count);
        self.k = count;
        self
    }

    /// Set similarity threshold for filtering results
    pub fn with_threshold(mut self, threshold: f32) -> Self {
        debug!("Setting similarity threshold: {}", threshold);
        self.threshold = Some(threshold);
        self
    }

    /// Set the search accuracy parameter (ef_search for HNSW)
    pub fn with_accuracy(mut self, ef_search: usize) -> Self {
        debug!("Setting HNSW ef_search: {}", ef_search);
        self.ef_search = Some(ef_search);
        self
    }

    /// Include distance scores in results
    pub fn with_scores(mut self) -> Self {
        debug!("Enabling distance scores in results");
        self.include_scores = true;
        self
    }

    /// Filter results based on metadata conditions
    pub fn filter_by<T>(mut self, column: &str, value: T) -> Self
    where
        T: Into<QueryParameterValue>,
    {
        debug!("Adding filter: {} = <value>", column);
        self.filters.push((column.to_string(), "=".to_string(), value.into()));
        self
    }

    /// Filter results with custom operator
    pub fn filter_where<T>(mut self, column: &str, operator: &str, value: T) -> Self
    where
        T: Into<QueryParameterValue>,
    {
        debug!("Adding filter: {} {} <value>", column, operator);
        self.filters.push((column.to_string(), operator.to_string(), value.into()));
        self
    }

    /// Get the target vector
    pub fn target_vector(&self) -> &[f32] {
        &self.query_vector
    }

    /// Get the distance metric
    pub fn metric(&self) -> VectorDistance {
        self.metric
    }

    /// Get the K value (number of nearest neighbors)
    pub fn k(&self) -> usize {
        self.k
    }

    /// Get the similarity threshold
    pub fn threshold(&self) -> Option<f32> {
        self.threshold
    }

    /// Get the HNSW ef_search parameter
    pub fn ef_search(&self) -> Option<usize> {
        self.ef_search
    }

    /// Check if scores should be included
    pub fn includes_scores(&self) -> bool {
        self.include_scores
    }

    /// Get the filters
    pub fn filters(&self) -> &[(String, String, QueryParameterValue)] {
        &self.filters
    }
}

impl Default for VectorQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorDistance {
    /// Convert to SQL distance operator
    pub fn to_sql_operator(&self) -> &'static str {
        match self {
            VectorDistance::L2 => "<->",
            VectorDistance::Cosine => "<=>",
            VectorDistance::InnerProduct => "<#>",
            VectorDistance::Manhattan => "<|>", // Custom operator for Manhattan
        }
    }

    /// Convert to HNSW metric
    pub fn to_hnsw_metric(&self) -> HnswMetric {
        match self {
            VectorDistance::L2 => HnswMetric::L2,
            VectorDistance::Cosine => HnswMetric::Cosine,
            VectorDistance::InnerProduct => HnswMetric::InnerProduct,
            VectorDistance::Manhattan => HnswMetric::L2, // Fallback to L2 for HNSW
        }
    }

    /// Convert to kernel metric
    pub fn to_kernel_metric(&self) -> KernelMetric {
        match self {
            VectorDistance::L2 => KernelMetric::L2,
            VectorDistance::Cosine => KernelMetric::Cosine,
            VectorDistance::InnerProduct => KernelMetric::InnerProduct,
            VectorDistance::Manhattan => KernelMetric::L2, // Fallback to L2
        }
    }

    /// Format vector array for SQL
    pub fn format_vector_array(&self, vector: &[f32]) -> String {
        let vector_str = vector.iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(",");
        format!("ARRAY[{}]", vector_str)
    }
}

impl Default for VectorDistance {
    fn default() -> Self {
        VectorDistance::L2
    }
}

/// Vector query result with optional distance scores
#[derive(Debug)]
pub struct VectorQueryResult {
    /// The retrieved vectors
    pub vectors: Vec<Vec<f32>>,
    /// Distance scores (if requested)
    pub scores: Option<Vec<f32>>,
    /// Associated metadata
    pub metadata: Vec<serde_json::Value>,
}

/// Extension trait for adding vector operations to regular queries
pub trait VectorQueryExt {
    /// Order results by vector similarity
    fn order_by_vector_distance(self, column: &str, target: &[f32], metric: VectorDistance) -> Self;

    /// Add vector similarity as a computed column
    fn with_vector_distance(self, column: &str, target: &[f32], metric: VectorDistance, alias: &str) -> Self;
}

/// Vector query builder trait for KNN and similarity search operations
pub trait VectorQueryBuilder {
    /// K-nearest neighbors query
    fn knn(self, vector_column: &str, query_vector: Vec<f32>, k: usize) -> Self;

    /// Similarity search with threshold
    fn similarity_search(self, vector_column: &str, query_vector: Vec<f32>, threshold: f32) -> Self;

    /// Add distance column to SELECT
    fn vector_distance(self, vector_column: &str, query_vector: Vec<f32>) -> Self;

    /// Cosine distance/similarity
    fn cosine_distance(self, vector_column: &str, query_vector: Vec<f32>) -> Self;

    /// L2 (Euclidean) distance
    fn l2_distance(self, vector_column: &str, query_vector: Vec<f32>) -> Self;

    /// Inner product similarity
    fn inner_product(self, vector_column: &str, query_vector: Vec<f32>) -> Self;

    /// Set HNSW ef_search parameter
    fn with_hnsw_ef_search(self, ef_search: usize) -> Self;

    /// Create HNSW index (DDL operation)
    fn create_hnsw_index(self, column: &str, m: Option<usize>, ef_construction: Option<usize>) -> Self;
}

/// Implementation of VectorQueryBuilder for SelectQueryBuilder in ColumnsSelected state
impl<'q> VectorQueryBuilder for SelectQueryBuilder<'q, query_states::ColumnsSelected> {
    /// K-nearest neighbors query - orders by distance and limits results
    fn knn(mut self, vector_column: &str, query_vector: Vec<f32>, k: usize) -> Self {
        debug!("Adding KNN query: {} nearest neighbors for column {}", k, vector_column);

        // Add vector distance ORDER BY
        self = self.order_by_distance(vector_column, query_vector);

        // Set limit to k
        self = self.limit(k as u64);

        self
    }

    /// Similarity search with threshold - orders by distance
    fn similarity_search(self, vector_column: &str, query_vector: Vec<f32>, _threshold: f32) -> Self {
        debug!("Adding similarity search with threshold {} for column {}", _threshold, vector_column);

        // Add ORDER BY for distance (threshold filtering would be done at application level)
        self.order_by_distance(vector_column, query_vector)
    }

    /// Add distance ordering (distance column must be added during table selection phase)
    fn vector_distance(self, vector_column: &str, query_vector: Vec<f32>) -> Self {
        debug!("Adding vector distance ordering for {}", vector_column);

        // Note: To add distance as a column, use the TableSelected state with select_expr
        // For ColumnsSelected state, we can only add ordering
        self.order_by_distance(vector_column, query_vector)
    }

    /// Cosine distance/similarity query
    fn cosine_distance(mut self, vector_column: &str, query_vector: Vec<f32>) -> Self {
        debug!("Adding cosine distance ordering for column {}", vector_column);

        let vector_str = query_vector.iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(",");

        // Use cosine distance operator <=>
        let order_expr = format!("{} <=> ARRAY[{}]", vector_column, vector_str);
        self = self.order_by_raw(&order_expr);

        self
    }

    /// L2 (Euclidean) distance query
    fn l2_distance(mut self, vector_column: &str, query_vector: Vec<f32>) -> Self {
        debug!("Adding L2 distance ordering for column {}", vector_column);

        // Use the existing order_by_distance which uses L2 (<->)
        self = self.order_by_distance(vector_column, query_vector);

        self
    }

    /// Inner product similarity query
    fn inner_product(mut self, vector_column: &str, query_vector: Vec<f32>) -> Self {
        debug!("Adding inner product ordering for column {}", vector_column);

        let vector_str = query_vector.iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(",");

        // Use inner product operator <#>
        let order_expr = format!("{} <#> ARRAY[{}]", vector_column, vector_str);
        self = self.order_by_raw(&order_expr);

        self
    }

    /// Set HNSW ef_search parameter (would generate SET command)
    fn with_hnsw_ef_search(self, ef_search: usize) -> Self {
        debug!("Setting HNSW ef_search parameter: {}", ef_search);

        // This would typically generate a SET hnsw.ef_search = <value> command
        // For now, we just log it - actual implementation would need query context modification

        self
    }

    /// Create HNSW index (DDL operation)
    fn create_hnsw_index(self, column: &str, m: Option<usize>, ef_construction: Option<usize>) -> Self {
        debug!("Creating HNSW index for column {} with m={:?}, ef_construction={:?}",
               column, m, ef_construction);

        // This would generate a CREATE INDEX statement with HNSW options
        // For now, we just log it - actual implementation would need DDL execution

        self
    }
}

/// Implementation of VectorQueryExt for SelectQueryBuilder in ColumnsSelected state
impl<'q> VectorQueryExt for SelectQueryBuilder<'q, query_states::ColumnsSelected> {
    /// Order results by vector similarity using specified metric
    fn order_by_vector_distance(mut self, column: &str, target: &[f32], metric: VectorDistance) -> Self {
        debug!("Adding vector distance ordering: {} with metric {:?}", column, metric);

        let vector_str = target.iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let order_expr = format!("{} {} ARRAY[{}]", column, metric.to_sql_operator(), vector_str);
        self = self.order_by_raw(&order_expr);

        self
    }

    /// Add vector similarity ordering (distance column must be added during table selection phase)
    fn with_vector_distance(self, column: &str, target: &[f32], metric: VectorDistance, _alias: &str) -> Self {
        debug!("Adding vector distance ordering: {} with metric {:?}", column, metric);

        // Note: To add distance as a column, use the TableSelected state with select_expr
        // For ColumnsSelected state, we can only add ordering
        self.order_by_vector_distance(column, target, metric)
    }
}

/// Hybrid query builder that combines vector operations with traditional filtering
pub struct HybridQuery {
    vector_query: VectorQuery,
    traditional_filters: Vec<(String, String, QueryParameterValue)>,
}

impl HybridQuery {
    /// Create a new hybrid query
    pub fn new() -> Self {
        Self {
            vector_query: VectorQuery::new(),
            traditional_filters: Vec::new(),
        }
    }

    /// Set the vector similarity component
    pub fn with_vector_search(mut self, query: VectorQuery) -> Self {
        self.vector_query = query;
        self
    }

    /// Add traditional WHERE filter
    pub fn where_<T>(mut self, column: &str, operator: &str, value: T) -> Self
    where
        T: Into<QueryParameterValue>,
    {
        self.traditional_filters.push((column.to_string(), operator.to_string(), value.into()));
        self
    }

    /// Convert to SelectQueryBuilder with both vector and traditional filters applied
    pub fn to_select_builder<'q>(self, table: &str, vector_column: &str) -> SelectQueryBuilder<'q, query_states::ColumnsSelected> {
        let mut builder = SelectQueryBuilder::new()
            .from(table)
            .select_all();

        // Apply vector search
        builder = builder.knn(vector_column, self.vector_query.query_vector, self.vector_query.k);

        // Apply traditional filters (would need WHERE clause implementation)
        // For now, this is a placeholder

        builder
    }
}

impl Default for HybridQuery {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_query_creation() {
        let query = VectorQuery::new();
        assert_eq!(query.k(), 10);
        assert_eq!(query.metric(), VectorDistance::L2);
        assert!(!query.includes_scores());
        assert!(query.threshold().is_none());
    }

    #[test]
    fn test_vector_query_configuration() {
        let target = vec![1.0, 2.0, 3.0];
        let query = VectorQuery::for_vector(target.clone())
            .with_distance(VectorDistance::Cosine)
            .limit(5)
            .with_threshold(0.8)
            .with_scores()
            .with_accuracy(200);

        assert_eq!(query.target_vector(), &target);
        assert_eq!(query.metric(), VectorDistance::Cosine);
        assert_eq!(query.k(), 5);
        assert_eq!(query.threshold(), Some(0.8));
        assert_eq!(query.ef_search(), Some(200));
        assert!(query.includes_scores());
    }

    #[test]
    fn test_vector_distance_sql_operators() {
        assert_eq!(VectorDistance::L2.to_sql_operator(), "<->");
        assert_eq!(VectorDistance::Cosine.to_sql_operator(), "<=>");
        assert_eq!(VectorDistance::InnerProduct.to_sql_operator(), "<#>");
        assert_eq!(VectorDistance::Manhattan.to_sql_operator(), "<|>");
    }

    #[test]
    fn test_vector_distance_format_array() {
        let vector = vec![1.0, 2.5, -0.3];
        let metric = VectorDistance::L2;
        let formatted = metric.format_vector_array(&vector);
        assert_eq!(formatted, "ARRAY[1,2.5,-0.3]");
    }

    #[test]
    fn test_vector_query_filters() {
        let query = VectorQuery::new()
            .filter_by("category", "tech")
            .filter_where("score", ">", 0.7_f64);

        assert_eq!(query.filters().len(), 2);
        assert_eq!(query.filters()[0].0, "category");
        assert_eq!(query.filters()[1].0, "score");
        assert_eq!(query.filters()[1].1, ">");
    }

    #[test]
    fn test_hybrid_query_creation() {
        let vector_query = VectorQuery::for_vector(vec![1.0, 2.0, 3.0])
            .with_distance(VectorDistance::Cosine)
            .limit(10);

        let hybrid = HybridQuery::new()
            .with_vector_search(vector_query)
            .where_("category", "=", "tech")
            .where_("status", "=", "active");

        assert_eq!(hybrid.traditional_filters.len(), 2);
        assert_eq!(hybrid.vector_query.metric(), VectorDistance::Cosine);
        assert_eq!(hybrid.vector_query.k(), 10);
    }

    #[test]
    fn test_select_query_builder_knn() {
        let query_vector = vec![0.1, 0.2, 0.3];
        let builder = SelectQueryBuilder::new()
            .from("embeddings")
            .select(&["id", "content", "embedding"])
            .knn("embedding", query_vector.clone(), 5);

        let sql = builder.build().unwrap().to_sql();

        // Should contain KNN elements
        assert!(sql.contains("FROM embeddings"));
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("embedding <->"));
        assert!(sql.contains("ARRAY[0.1,0.2,0.3]"));
        assert!(sql.contains("LIMIT 5"));
    }

    #[test]
    fn test_select_query_builder_vector_distance() {
        let query_vector = vec![1.0, 2.0];
        let builder = SelectQueryBuilder::new()
            .from("docs")
            .select(&["id", "content"])
            .vector_distance("embedding", query_vector.clone());

        let sql = builder.build().unwrap().to_sql();

        // Should contain distance column
        assert!(sql.contains("embedding <-> ARRAY[1,2] AS distance"));
    }

    #[test]
    fn test_select_query_builder_cosine_distance() {
        let query_vector = vec![0.5, 0.7];
        let builder = SelectQueryBuilder::new()
            .from("vectors")
            .select(&["id"])
            .cosine_distance("vec", query_vector);

        let sql = builder.build().unwrap().to_sql();

        // Should use cosine distance operator
        assert!(sql.contains("vec <=> ARRAY[0.5,0.7]"));
    }

    #[test]
    fn test_select_query_builder_inner_product() {
        let query_vector = vec![0.3, 0.4, 0.5];
        let builder = SelectQueryBuilder::new()
            .from("products")
            .select(&["id", "name"])
            .inner_product("feature_vector", query_vector);

        let sql = builder.build().unwrap().to_sql();

        // Should use inner product operator
        assert!(sql.contains("feature_vector <#> ARRAY[0.3,0.4,0.5]"));
    }

    #[test]
    fn test_vector_query_ext_with_distance() {
        let target = vec![1.0, 2.0, 3.0];
        let builder = SelectQueryBuilder::new()
            .from("embeddings")
            .select(&["id", "text"])
            .with_vector_distance("embedding", &target, VectorDistance::Cosine, "similarity_score");

        let sql = builder.build().unwrap().to_sql();

        // Should contain the distance column with alias
        assert!(sql.contains("embedding <=> ARRAY[1,2,3] AS similarity_score"));
    }
}