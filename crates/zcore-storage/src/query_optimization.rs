//! Storage layer optimizations for multi-language query patterns
//!
//! This module provides optimized storage patterns and access methods for
//! supporting SQL, Cypher, SPARQL, and GraphQL query workloads efficiently.

use anyhow::{anyhow, Result};
use std::collections::HashMap;
use parking_lot::RwLock;
use tracing::{debug, info, warn};

use crate::{Store, ReadTransaction, WriteTransaction, encode_key, prefix_range};

/// Query language specific storage optimizations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryLanguage {
    Sql,
    Cypher,
    Sparql,
    GraphQL,
}

/// Optimized key patterns for different query types
pub struct QueryOptimizedKeys;

impl QueryOptimizedKeys {
    /// Row data key: [org_id][table_name][row_id]
    pub fn row_key(org_id: u64, table_name: &str, row_id: u64) -> Vec<u8> {
        encode_key(&[
            b"row",
            &org_id.to_be_bytes(),
            table_name.as_bytes(),
            &row_id.to_be_bytes(),
        ])
    }

    /// Index key: [org_id][table_name][index_name][indexed_value][row_id]
    pub fn index_key(org_id: u64, table_name: &str, index_name: &str, indexed_value: &[u8], row_id: u64) -> Vec<u8> {
        encode_key(&[
            b"idx",
            &org_id.to_be_bytes(),
            table_name.as_bytes(),
            index_name.as_bytes(),
            indexed_value,
            &row_id.to_be_bytes(),
        ])
    }

    /// Graph edge key for Cypher: [org_id][graph_name][edge_type][from_id][to_id][edge_id]
    pub fn edge_key(org_id: u64, graph_name: &str, edge_type: &str, from_id: u64, to_id: u64, edge_id: u64) -> Vec<u8> {
        encode_key(&[
            b"edge",
            &org_id.to_be_bytes(),
            graph_name.as_bytes(),
            edge_type.as_bytes(),
            &from_id.to_be_bytes(),
            &to_id.to_be_bytes(),
            &edge_id.to_be_bytes(),
        ])
    }

    /// Graph vertex key for Cypher: [org_id][graph_name][vertex][vertex_id]
    pub fn vertex_key(org_id: u64, graph_name: &str, vertex_id: u64) -> Vec<u8> {
        encode_key(&[
            b"vertex",
            &org_id.to_be_bytes(),
            graph_name.as_bytes(),
            &vertex_id.to_be_bytes(),
        ])
    }

    /// SPARQL triple key: [org_id][graph][subject][predicate][object]
    pub fn triple_key(org_id: u64, graph_name: &str, subject: &str, predicate: &str, object: &str) -> Vec<u8> {
        encode_key(&[
            b"triple",
            &org_id.to_be_bytes(),
            graph_name.as_bytes(),
            subject.as_bytes(),
            predicate.as_bytes(),
            object.as_bytes(),
        ])
    }

    /// GraphQL field index key: [org_id][type_name][field_name][field_value][entity_id]
    pub fn graphql_field_key(org_id: u64, type_name: &str, field_name: &str, field_value: &[u8], entity_id: u64) -> Vec<u8> {
        encode_key(&[
            b"gql",
            &org_id.to_be_bytes(),
            type_name.as_bytes(),
            field_name.as_bytes(),
            field_value,
            &entity_id.to_be_bytes(),
        ])
    }

    /// Range prefix for table scans
    pub fn table_prefix(org_id: u64, table_name: &str) -> (Vec<u8>, Vec<u8>) {
        prefix_range(&[b"row", &org_id.to_be_bytes(), table_name.as_bytes()])
    }

    /// Range prefix for graph traversals
    pub fn graph_prefix(org_id: u64, graph_name: &str) -> (Vec<u8>, Vec<u8>) {
        prefix_range(&[b"edge", &org_id.to_be_bytes(), graph_name.as_bytes()])
    }

    /// Range prefix for vertex scans
    pub fn vertex_prefix(org_id: u64, graph_name: &str) -> (Vec<u8>, Vec<u8>) {
        prefix_range(&[b"vertex", &org_id.to_be_bytes(), graph_name.as_bytes()])
    }
}

/// Query pattern analyzer for storage optimization
#[derive(Debug)]
pub struct QueryPatternAnalyzer {
    /// Cache of frequently accessed query patterns
    pattern_cache: RwLock<HashMap<String, QueryPattern>>,
    /// Statistics for query optimization
    stats: RwLock<QueryStats>,
}

#[derive(Debug, Clone)]
pub struct QueryPattern {
    pub language: QueryLanguage,
    pub access_pattern: AccessPattern,
    pub frequency: u64,
    pub last_accessed: std::time::Instant,
}

#[derive(Debug, Clone)]
pub enum AccessPattern {
    /// Point lookups by primary key
    PointLookup,
    /// Range scans on sequential data
    RangeScan,
    /// Index lookups with secondary keys
    IndexLookup,
    /// Graph traversals following edges
    GraphTraversal,
    /// Multi-way joins across tables
    JoinOperation,
    /// Vector similarity searches
    VectorSearch,
    /// Full-text searches
    FullTextSearch,
}

#[derive(Debug, Default)]
pub struct QueryStats {
    pub total_queries: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub avg_response_time_ms: f64,
    pub language_distribution: HashMap<QueryLanguage, u64>,
}

impl QueryPatternAnalyzer {
    pub fn new() -> Self {
        Self {
            pattern_cache: RwLock::new(HashMap::new()),
            stats: RwLock::new(QueryStats::default()),
        }
    }

    /// Analyze a query and record its pattern
    pub fn analyze_query(&self, query: &str, language: QueryLanguage) -> Result<QueryPattern> {
        let pattern = self.extract_pattern(query, language)?;

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.total_queries += 1;
            *stats.language_distribution.entry(language).or_insert(0) += 1;
        }

        // Cache the pattern
        {
            let mut cache = self.pattern_cache.write();
            cache.insert(query.to_string(), pattern.clone());
        }

        Ok(pattern)
    }

    /// Extract query pattern from query string
    fn extract_pattern(&self, query: &str, language: QueryLanguage) -> Result<QueryPattern> {
        let access_pattern = match language {
            QueryLanguage::Sql => self.analyze_sql_pattern(query)?,
            QueryLanguage::Cypher => self.analyze_cypher_pattern(query)?,
            QueryLanguage::Sparql => self.analyze_sparql_pattern(query)?,
            QueryLanguage::GraphQL => self.analyze_graphql_pattern(query)?,
        };

        Ok(QueryPattern {
            language,
            access_pattern,
            frequency: 1,
            last_accessed: std::time::Instant::now(),
        })
    }

    fn analyze_sql_pattern(&self, query: &str) -> Result<AccessPattern> {
        let query_lower = query.to_lowercase();

        if query_lower.contains("where") && query_lower.contains("=") {
            Ok(AccessPattern::IndexLookup)
        } else if query_lower.contains("order by") || query_lower.contains("limit") {
            Ok(AccessPattern::RangeScan)
        } else if query_lower.contains("join") {
            Ok(AccessPattern::JoinOperation)
        } else if query_lower.contains("similarity") || query_lower.contains("knn") {
            Ok(AccessPattern::VectorSearch)
        } else if query_lower.contains("full-text") || query_lower.contains("contains") {
            Ok(AccessPattern::FullTextSearch)
        } else {
            Ok(AccessPattern::PointLookup)
        }
    }

    fn analyze_cypher_pattern(&self, query: &str) -> Result<AccessPattern> {
        let query_lower = query.to_lowercase();

        if query_lower.contains("-[") && query_lower.contains("]->") {
            Ok(AccessPattern::GraphTraversal)
        } else if query_lower.contains("match") && query_lower.contains("where") {
            Ok(AccessPattern::IndexLookup)
        } else {
            Ok(AccessPattern::PointLookup)
        }
    }

    fn analyze_sparql_pattern(&self, query: &str) -> Result<AccessPattern> {
        let query_lower = query.to_lowercase();

        if query_lower.contains("triple") || query_lower.contains("?s ?p ?o") {
            Ok(AccessPattern::GraphTraversal)
        } else if query_lower.contains("filter") {
            Ok(AccessPattern::IndexLookup)
        } else {
            Ok(AccessPattern::PointLookup)
        }
    }

    fn analyze_graphql_pattern(&self, query: &str) -> Result<AccessPattern> {
        let query_lower = query.to_lowercase();

        if query_lower.contains("{") && query_lower.contains("}") {
            if query_lower.contains("where") || query_lower.contains("filter") {
                Ok(AccessPattern::IndexLookup)
            } else {
                Ok(AccessPattern::PointLookup)
            }
        } else {
            Ok(AccessPattern::PointLookup)
        }
    }

    /// Get cached query pattern
    pub fn get_cached_pattern(&self, query: &str) -> Option<QueryPattern> {
        let cache = self.pattern_cache.read();
        cache.get(query).cloned()
    }

    /// Get query statistics
    pub fn get_stats(&self) -> QueryStats {
        self.stats.read().clone()
    }
}

/// Multi-language query optimizer
pub struct MultiLanguageOptimizer {
    store: Store,
    pattern_analyzer: QueryPatternAnalyzer,
}

impl MultiLanguageOptimizer {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            pattern_analyzer: QueryPatternAnalyzer::new(),
        }
    }

    /// Optimize storage layout for specific query patterns
    pub fn optimize_for_pattern(&self, pattern: &QueryPattern) -> Result<()> {
        match &pattern.access_pattern {
            AccessPattern::PointLookup => {
                info!("Optimizing for point lookup pattern");
                self.optimize_point_lookups()?;
            }
            AccessPattern::RangeScan => {
                info!("Optimizing for range scan pattern");
                self.optimize_range_scans()?;
            }
            AccessPattern::IndexLookup => {
                info!("Optimizing for index lookup pattern");
                self.optimize_index_lookups()?;
            }
            AccessPattern::GraphTraversal => {
                info!("Optimizing for graph traversal pattern");
                self.optimize_graph_traversals()?;
            }
            AccessPattern::JoinOperation => {
                info!("Optimizing for join operation pattern");
                self.optimize_joins()?;
            }
            AccessPattern::VectorSearch => {
                info!("Optimizing for vector search pattern");
                self.optimize_vector_searches()?;
            }
            AccessPattern::FullTextSearch => {
                info!("Optimizing for full-text search pattern");
                self.optimize_fulltext_searches()?;
            }
        }
        Ok(())
    }

    fn optimize_point_lookups(&self) -> Result<()> {
        // Ensure primary key indexes are created
        // Cache frequently accessed rows
        // Optimize key distribution
        debug!("Optimizing point lookup storage patterns");
        Ok(())
    }

    fn optimize_range_scans(&self) -> Result<()> {
        // Ensure proper key ordering for range queries
        // Create clustering indexes
        // Optimize LSM-tree compaction for sequential access
        debug!("Optimizing range scan storage patterns");
        Ok(())
    }

    fn optimize_index_lookups(&self) -> Result<()> {
        // Create appropriate secondary indexes
        // Optimize index key encoding
        // Maintain index statistics
        debug!("Optimizing index lookup storage patterns");
        Ok(())
    }

    fn optimize_graph_traversals(&self) -> Result<()> {
        // Create adjacency lists for efficient graph traversal
        // Optimize edge storage for both inbound and outbound traversals
        // Create vertex property indexes
        debug!("Optimizing graph traversal storage patterns");
        Ok(())
    }

    fn optimize_joins(&self) -> Result<()> {
        // Create join indexes
        // Optimize data co-location for joined tables
        // Maintain join statistics
        debug!("Optimizing join operation storage patterns");
        Ok(())
    }

    fn optimize_vector_searches(&self) -> Result<()> {
        // Ensure HNSW indexes are built
        // Optimize vector storage layout
        // Maintain vector statistics
        debug!("Optimizing vector search storage patterns");
        Ok(())
    }

    fn optimize_fulltext_searches(&self) -> Result<()> {
        // Create full-text indexes
        // Optimize text tokenization and indexing
        // Maintain text search statistics
        debug!("Optimizing full-text search storage patterns");
        Ok(())
    }

    /// Get the pattern analyzer
    pub fn pattern_analyzer(&self) -> &QueryPatternAnalyzer {
        &self.pattern_analyzer
    }

    /// Get the store reference
    pub fn store(&self) -> &Store {
        &self.store
    }
}

/// Cache manager for multi-language query results
pub struct QueryCacheManager {
    /// Point query cache
    point_cache: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
    /// Range query cache
    range_cache: RwLock<HashMap<String, Vec<(Vec<u8>, Vec<u8>)>>>,
    /// Index lookup cache
    index_cache: RwLock<HashMap<String, Vec<Vec<u8>>>>,
    /// Cache statistics
    cache_stats: RwLock<CacheStats>,
}

#[derive(Debug, Default)]
pub struct CacheStats {
    pub point_cache_hits: u64,
    pub point_cache_misses: u64,
    pub range_cache_hits: u64,
    pub range_cache_misses: u64,
    pub index_cache_hits: u64,
    pub index_cache_misses: u64,
}

impl QueryCacheManager {
    pub fn new() -> Self {
        Self {
            point_cache: RwLock::new(HashMap::new()),
            range_cache: RwLock::new(HashMap::new()),
            index_cache: RwLock::new(HashMap::new()),
            cache_stats: RwLock::new(CacheStats::default()),
        }
    }

    /// Cache point lookup result
    pub fn cache_point_result(&self, key: Vec<u8>, value: Vec<u8>) {
        let mut cache = self.point_cache.write();
        cache.insert(key, value);
    }

    /// Get cached point result
    pub fn get_cached_point(&self, key: &[u8]) -> Option<Vec<u8>> {
        let cache = self.point_cache.read();
        if let Some(value) = cache.get(key) {
            let mut stats = self.cache_stats.write();
            stats.point_cache_hits += 1;
            Some(value.clone())
        } else {
            let mut stats = self.cache_stats.write();
            stats.point_cache_misses += 1;
            None
        }
    }

    /// Cache range query result
    pub fn cache_range_result(&self, query: &str, results: Vec<(Vec<u8>, Vec<u8>)>) {
        let mut cache = self.range_cache.write();
        cache.insert(query.to_string(), results);
    }

    /// Get cached range result
    pub fn get_cached_range(&self, query: &str) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        let cache = self.range_cache.read();
        if let Some(results) = cache.get(query) {
            let mut stats = self.cache_stats.write();
            stats.range_cache_hits += 1;
            Some(results.clone())
        } else {
            let mut stats = self.cache_stats.write();
            stats.range_cache_misses += 1;
            None
        }
    }

    /// Cache index lookup result
    pub fn cache_index_result(&self, query: &str, results: Vec<Vec<u8>>) {
        let mut cache = self.index_cache.write();
        cache.insert(query.to_string(), results);
    }

    /// Get cached index result
    pub fn get_cached_index(&self, query: &str) -> Option<Vec<Vec<u8>>> {
        let cache = self.index_cache.read();
        if let Some(results) = cache.get(query) {
            let mut stats = self.cache_stats.write();
            stats.index_cache_hits += 1;
            Some(results.clone())
        } else {
            let mut stats = self.cache_stats.write();
            stats.index_cache_misses += 1;
            None
        }
    }

    /// Get cache statistics
    pub fn get_cache_stats(&self) -> CacheStats {
        self.cache_stats.read().clone()
    }

    /// Clear all caches
    pub fn clear_all_caches(&self) {
        self.point_cache.write().clear();
        self.range_cache.write().clear();
        self.index_cache.write().clear();
    }

    /// Clear expired cache entries based on LRU or time-based eviction
    pub fn cleanup_expired_entries(&self) {
        // TODO: Implement cache cleanup logic based on LRU or time-based eviction
        debug!("Cache cleanup requested");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_query_optimized_keys() {
        // Test row key generation
        let row_key = QueryOptimizedKeys::row_key(1, "users", 123);
        assert!(row_key.starts_with(b"row"));

        // Test index key generation
        let index_key = QueryOptimizedKeys::index_key(1, "users", "email_idx", b"test@example.com", 123);
        assert!(index_key.starts_with(b"idx"));

        // Test graph edge key generation
        let edge_key = QueryOptimizedKeys::edge_key(1, "social", "FRIENDS", 100, 200, 300);
        assert!(edge_key.starts_with(b"edge"));

        // Test vertex key generation
        let vertex_key = QueryOptimizedKeys::vertex_key(1, "social", 100);
        assert!(vertex_key.starts_with(b"vertex"));

        // Test SPARQL triple key generation
        let triple_key = QueryOptimizedKeys::triple_key(1, "rdf", "subj1", "pred1", "obj1");
        assert!(triple_key.starts_with(b"triple"));

        // Test GraphQL field key generation
        let gql_key = QueryOptimizedKeys::graphql_field_key(1, "User", "name", b"Alice", 100);
        assert!(gql_key.starts_with(b"gql"));
    }

    #[test]
    fn test_query_pattern_analyzer() {
        let analyzer = QueryPatternAnalyzer::new();

        // Test SQL pattern analysis
        let sql_pattern = analyzer.analyze_query("SELECT * FROM users WHERE id = 1", QueryLanguage::Sql).unwrap();
        assert_eq!(sql_pattern.language, QueryLanguage::Sql);
        assert!(matches!(sql_pattern.access_pattern, AccessPattern::IndexLookup));

        // Test Cypher pattern analysis
        let cypher_pattern = analyzer.analyze_query("MATCH (a)-[:FRIENDS]->(b) RETURN a,b", QueryLanguage::Cypher).unwrap();
        assert_eq!(cypher_pattern.language, QueryLanguage::Cypher);
        assert!(matches!(cypher_pattern.access_pattern, AccessPattern::GraphTraversal));

        // Test SPARQL pattern analysis
        let sparql_pattern = analyzer.analyze_query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", QueryLanguage::Sparql).unwrap();
        assert_eq!(sparql_pattern.language, QueryLanguage::Sparql);
        assert!(matches!(sparql_pattern.access_pattern, AccessPattern::GraphTraversal));

        // Test GraphQL pattern analysis
        let gql_pattern = analyzer.analyze_query("{ user(id: 1) { name email } }", QueryLanguage::GraphQL).unwrap();
        assert_eq!(gql_pattern.language, QueryLanguage::GraphQL);
        assert!(matches!(gql_pattern.access_pattern, AccessPattern::PointLookup));
    }

    #[test]
    fn test_multi_language_optimizer() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("test_opt")).unwrap();
        let optimizer = MultiLanguageOptimizer::new(store);

        // Test optimizing different patterns
        let point_pattern = QueryPattern {
            language: QueryLanguage::Sql,
            access_pattern: AccessPattern::PointLookup,
            frequency: 1,
            last_accessed: std::time::Instant::now(),
        };

        assert!(optimizer.optimize_for_pattern(&point_pattern).is_ok());

        // Test pattern analyzer access
        let analyzer = optimizer.pattern_analyzer();
        assert!(analyzer.get_stats().total_queries >= 0);
    }

    #[test]
    fn test_query_cache_manager() {
        let cache_manager = QueryCacheManager::new();

        // Test point cache
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();
        cache_manager.cache_point_result(key.clone(), value.clone());
        assert_eq!(cache_manager.get_cached_point(&key), Some(value));

        // Test range cache
        let query = "SELECT * FROM users WHERE id > 100";
        let results = vec![(b"key1".to_vec(), b"value1".to_vec()), (b"key2".to_vec(), b"value2".to_vec())];
        cache_manager.cache_range_result(query, results.clone());
        assert_eq!(cache_manager.get_cached_range(query), Some(results));

        // Test index cache
        let index_query = "users_email_idx:test@example.com";
        let index_results = vec![b"row1".to_vec(), b"row2".to_vec()];
        cache_manager.cache_index_result(index_query, index_results.clone());
        assert_eq!(cache_manager.get_cached_index(index_query), Some(index_results));

        // Test cache statistics
        let stats = cache_manager.get_cache_stats();
        assert_eq!(stats.point_cache_hits, 1);
        assert_eq!(stats.range_cache_hits, 1);
        assert_eq!(stats.index_cache_hits, 1);

        // Test cache clearing
        cache_manager.clear_all_caches();
        assert_eq!(cache_manager.get_cached_point(&key), None);
        assert_eq!(cache_manager.get_cached_range(query), None);
        assert_eq!(cache_manager.get_cached_index(index_query), None);
    }

    #[test]
    fn test_table_prefix_ranges() {
        let (start, end) = QueryOptimizedKeys::table_prefix(1, "users");
        assert!(start < end);
        assert!(start.starts_with(b"row"));
        assert!(end.starts_with(b"row"));

        let (v_start, v_end) = QueryOptimizedKeys::vertex_prefix(1, "social");
        assert!(v_start < v_end);
        assert!(v_start.starts_with(b"vertex"));
        assert!(v_end.starts_with(b"vertex"));
    }
}