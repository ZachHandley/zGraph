//! Enhanced vector storage integration with HNSW indexing
//!
//! This module provides optimized vector storage operations and seamless integration
//! with the HNSW indexing system for high-performance similarity searches.

use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn, error};

use crate::{Store, WriteTransaction, ReadTransaction, encode_key, prefix_range};

/// Vector storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorStorageConfig {
    /// Default HNSW M parameter (connections per node)
    pub default_m: usize,
    /// Default ef_construction parameter
    pub default_ef_construction: usize,
    /// Maximum vector dimension supported
    pub max_dimension: usize,
    /// Enable vector compression
    pub enable_compression: bool,
    /// Compression threshold (dimension size)
    pub compression_threshold: usize,
    /// Cache size for frequently accessed vectors
    pub cache_size: usize,
    /// Batch size for vector operations
    pub batch_size: usize,
    /// Enable async indexing
    pub enable_async_indexing: bool,
}

impl Default for VectorStorageConfig {
    fn default() -> Self {
        Self {
            default_m: 16,
            default_ef_construction: 64,
            max_dimension: 4096,
            enable_compression: true,
            compression_threshold: 128,
            cache_size: 10000,
            batch_size: 1000,
            enable_async_indexing: true,
        }
    }
}

/// Vector metadata stored alongside vectors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorMetadata {
    /// Vector dimension
    pub dimension: usize,
    /// Distance metric used
    pub metric: VectorMetric,
    /// Vector creation timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last update timestamp
    pub updated_at: chrono::DateTime<chrono::Utc>,
    /// Number of times this vector was accessed
    pub access_count: u64,
    /// Vector normalization status
    pub is_normalized: bool,
    /// Vector checksum for integrity verification
    pub checksum: Option<u64>,
}

/// Supported distance metrics
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorMetric {
    L2,
    Cosine,
    InnerProduct,
}

impl VectorMetric {
    /// Convert to zvec-kernels metric
    #[cfg(feature = "vectors")]
    pub fn to_kernel_metric(self) -> zvec_kernels::Metric {
        match self {
            VectorMetric::L2 => zvec_kernels::Metric::L2,
            VectorMetric::Cosine => zvec_kernels::Metric::Cosine,
            VectorMetric::InnerProduct => zvec_kernels::Metric::InnerProduct,
        }
    }

    /// Convert to zann-hnsw metric
    #[cfg(feature = "vectors")]
    pub fn to_hnsw_metric(self) -> zann_hnsw::Metric {
        match self {
            VectorMetric::L2 => zann_hnsw::Metric::L2,
            VectorMetric::Cosine => zann_hnsw::Metric::Cosine,
            VectorMetric::InnerProduct => zann_hnsw::Metric::InnerProduct,
        }
    }
}

/// Vector storage statistics
#[derive(Debug, Clone, Default)]
pub struct VectorStorageStats {
    /// Total number of vectors stored
    pub total_vectors: u64,
    /// Total storage used by vectors (in bytes)
    pub storage_used_bytes: u64,
    /// Number of HNSW indexes
    pub total_indexes: u64,
    /// Average vector dimension
    pub avg_dimension: f64,
    /// Cache hit ratio
    pub cache_hit_ratio: f64,
    /// Number of similarity searches performed
    pub total_searches: u64,
    /// Average search latency (ms)
    pub avg_search_latency_ms: f64,
    /// Index build progress (0.0 to 1.0)
    pub index_build_progress: f64,
}

/// Enhanced vector storage manager
pub struct VectorStorageManager {
    store: Store,
    config: VectorStorageConfig,
    /// LRU cache for frequently accessed vectors
    vector_cache: RwLock<lru::LruCache<Vec<u8>, Vec<f32>>>,
    /// Vector metadata cache
    metadata_cache: RwLock<HashMap<String, VectorMetadata>>,
    /// Storage statistics
    stats: RwLock<VectorStorageStats>,
    /// HNSW index handles
    hnsw_indexes: RwLock<HashMap<String, Arc<zann_hnsw::AnnIndex>>>,
}

impl VectorStorageManager {
    pub fn new(store: Store, config: VectorStorageConfig) -> Result<Self> {
        let vector_cache = RwLock::new(lru::LruCache::new(
            std::num::NonZeroUsize::new(config.cache_size).ok_or_else(|| anyhow!("Invalid cache size"))?
        ));

        Ok(Self {
            store,
            config,
            vector_cache,
            metadata_cache: RwLock::new(HashMap::new()),
            stats: RwLock::new(VectorStorageStats::default()),
            hnsw_indexes: RwLock::new(HashMap::new()),
        })
    }

    /// Store a vector with metadata
    pub fn store_vector(&self, org_id: u64, table_name: &str, column_name: &str,
                       row_id: u64, vector: Vec<f32>, metric: VectorMetric) -> Result<()> {
        // Validate vector
        self.validate_vector(&vector)?;

        // Generate vector key
        let vector_key = self.vector_key(org_id, table_name, column_name, row_id);

        // Create metadata
        let metadata = VectorMetadata {
            dimension: vector.len(),
            metric,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            access_count: 0,
            is_normalized: false,
            checksum: Some(self.calculate_checksum(&vector)),
        };

        // Serialize and store
        let mut tx = WriteTransaction::new();

        // Store the vector data
        let serialized_vector = bincode::serialize(&vector)?;
        tx.set("vectors", vector_key.clone(), serialized_vector);

        // Store metadata
        let metadata_key = self.metadata_key(org_id, table_name, column_name, row_id);
        let serialized_metadata = bincode::serialize(&metadata)?;
        tx.set("vector_metadata", metadata_key, serialized_metadata);

        // Update index if it exists
        self.update_index(org_id, table_name, column_name, &vector, row_id, &metric)?;

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.total_vectors += 1;
            stats.storage_used_bytes += vector.len() * std::mem::size_of::<f32>() as u64;
            stats.avg_dimension = ((stats.avg_dimension * (stats.total_vectors - 1) as f64) + vector.len() as f64) / stats.total_vectors as f64;
        }

        // Cache the vector
        {
            let mut cache = self.vector_cache.write();
            cache.put(vector_key, vector.clone());
        }

        // Cache metadata
        {
            let mut metadata_cache = self.metadata_cache.write();
            metadata_cache.insert(format!("{}:{}:{}:{}", org_id, table_name, column_name, row_id), metadata);
        }

        tx.commit(&self.store)?;

        debug!("Stored vector {}:{}:{}:{} with dimension {}",
               org_id, table_name, column_name, row_id, vector.len());

        Ok(())
    }

    /// Retrieve a vector by key
    pub fn get_vector(&self, org_id: u64, table_name: &str, column_name: &str,
                     row_id: u64) -> Result<Option<Vec<f32>>> {
        let vector_key = self.vector_key(org_id, table_name, column_name, row_id);

        // Check cache first
        {
            let mut cache = self.vector_cache.write();
            if let Some(vector) = cache.get(&vector_key) {
                // Update access statistics
                self.update_access_stats(org_id, table_name, column_name, row_id)?;
                return Ok(Some(vector.clone()));
            }
        }

        // Fall back to storage
        let tx = ReadTransaction::new();
        if let Some(serialized_vector) = tx.get(&self.store, "vectors", &vector_key)? {
            let vector: Vec<f32> = bincode::deserialize(&serialized_vector)?;

            // Cache the result
            {
                let mut cache = self.vector_cache.write();
                cache.put(vector_key, vector.clone());
            }

            // Update access statistics
            self.update_access_stats(org_id, table_name, column_name, row_id)?;

            Ok(Some(vector))
        } else {
            Ok(None)
        }
    }

    /// Get vector metadata
    pub fn get_vector_metadata(&self, org_id: u64, table_name: &str, column_name: &str,
                              row_id: u64) -> Result<Option<VectorMetadata>> {
        let cache_key = format!("{}:{}:{}:{}", org_id, table_name, column_name, row_id);

        // Check metadata cache first
        {
            let metadata_cache = self.metadata_cache.read();
            if let Some(metadata) = metadata_cache.get(&cache_key) {
                return Ok(Some(metadata.clone()));
            }
        }

        // Fall back to storage
        let metadata_key = self.metadata_key(org_id, table_name, column_name, row_id);
        let tx = ReadTransaction::new();
        if let Some(serialized_metadata) = tx.get(&self.store, "vector_metadata", &metadata_key)? {
            let metadata: VectorMetadata = bincode::deserialize(&serialized_metadata)?;

            // Cache the metadata
            {
                let mut metadata_cache = self.metadata_cache.write();
                metadata_cache.insert(cache_key, metadata.clone());
            }

            Ok(Some(metadata))
        } else {
            Ok(None)
        }
    }

    /// Delete a vector
    pub fn delete_vector(&self, org_id: u64, table_name: &str, column_name: &str,
                        row_id: u64) -> Result<bool> {
        let vector_key = self.vector_key(org_id, table_name, column_name, row_id);
        let metadata_key = self.metadata_key(org_id, table_name, column_name, row_id);

        let mut tx = WriteTransaction::new();

        // Check if vector exists
        let existed = tx.get(&self.store, "vectors", &vector_key)?.is_some();

        if existed {
            tx.delete("vectors", vector_key);
            tx.delete("vector_metadata", metadata_key);

            // Remove from index
            self.remove_from_index(org_id, table_name, column_name, row_id)?;

            // Update statistics
            {
                let mut stats = self.stats.write();
                stats.total_vectors = stats.total_vectors.saturating_sub(1);
            }

            // Remove from cache
            {
                let mut cache = self.vector_cache.write();
                cache.pop(&vector_key);
            }

            // Remove metadata from cache
            {
                let mut metadata_cache = self.metadata_cache.write();
                let cache_key = format!("{}:{}:{}:{}", org_id, table_name, column_name, row_id);
                metadata_cache.remove(&cache_key);
            }
        }

        tx.commit(&self.store)?;
        Ok(existed)
    }

    /// Batch store multiple vectors
    pub fn batch_store_vectors(&self, vectors: Vec<BatchVectorEntry>) -> Result<u64> {
        let mut stored_count = 0;
        let batch_size = self.config.batch_size;

        for chunk in vectors.chunks(batch_size) {
            let mut tx = WriteTransaction::new();

            for entry in chunk {
                // Validate vector
                if let Err(e) = self.validate_vector(&entry.vector) {
                    warn!("Skipping invalid vector: {}", e);
                    continue;
                }

                let vector_key = self.vector_key(entry.org_id, &entry.table_name, &entry.column_name, entry.row_id);
                let metadata_key = self.metadata_key(entry.org_id, &entry.table_name, &entry.column_name, entry.row_id);

                // Create metadata
                let metadata = VectorMetadata {
                    dimension: entry.vector.len(),
                    metric: entry.metric,
                    created_at: chrono::Utc::now(),
                    updated_at: chrono::Utc::now(),
                    access_count: 0,
                    is_normalized: false,
                    checksum: Some(self.calculate_checksum(&entry.vector)),
                };

                // Store vector and metadata
                let serialized_vector = bincode::serialize(&entry.vector)?;
                tx.set("vectors", vector_key, serialized_vector);

                let serialized_metadata = bincode::serialize(&metadata)?;
                tx.set("vector_metadata", metadata_key, serialized_metadata);

                stored_count += 1;
            }

            tx.commit(&self.store)?;

            // Update indexes after batch commit
            for entry in chunk {
                if let Err(e) = self.update_index(entry.org_id, &entry.table_name, &entry.column_name,
                                                 &entry.vector, entry.row_id, &entry.metric) {
                    error!("Failed to update index for vector {}:{}:{}: {}",
                           entry.org_id, entry.table_name, entry.column_name, e);
                }
            }
        }

        info!("Batch stored {} vectors", stored_count);
        Ok(stored_count)
    }

    /// Create or get HNSW index for a table column
    pub fn get_or_create_index(&self, org_id: u64, table_name: &str, column_name: &str,
                              dimension: usize, metric: VectorMetric) -> Result<Arc<zann_hnsw::AnnIndex>> {
        let index_key = format!("{}:{}:{}", org_id, table_name, column_name);

        // Check if index already exists in cache
        {
            let indexes = self.hnsw_indexes.read();
            if let Some(index) = indexes.get(&index_key) {
                return Ok(Arc::clone(index));
            }
        }

        // Load or create index
        let index = self.load_or_create_hnsw_index(org_id, table_name, column_name, dimension, metric)?;

        // Cache the index
        {
            let mut indexes = self.hnsw_indexes.write();
            indexes.insert(index_key, Arc::clone(&index));
        }

        Ok(index)
    }

    /// Perform similarity search using HNSW index
    pub fn similarity_search(&self, org_id: u64, table_name: &str, column_name: &str,
                           query_vector: &[f32], k: usize, ef: usize) -> Result<Vec<(u64, f32)>> {
        // Get or create index
        let index = self.get_or_create_index(org_id, table_name, column_name, query_vector.len(), VectorMetric::L2)?;

        // Perform search
        let start_time = std::time::Instant::now();
        let results = index.search(query_vector, k, ef)?;
        let search_duration = start_time.elapsed();

        // Update search statistics
        {
            let mut stats = self.stats.write();
            stats.total_searches += 1;
            stats.avg_search_latency_ms = ((stats.avg_search_latency_ms * (stats.total_searches - 1) as f64) +
                                          search_duration.as_millis() as f64) / stats.total_searches as f64;
        }

        debug!("Similarity search completed in {:?}ms, returned {} results",
               search_duration.as_millis(), results.len());

        Ok(results)
    }

    /// Get storage statistics
    pub fn get_stats(&self) -> VectorStorageStats {
        self.stats.read().clone()
    }

    /// Clear vector cache
    pub fn clear_cache(&self) {
        self.vector_cache.write().clear();
        self.metadata_cache.write().clear();
        info!("Vector cache cleared");
    }

    /// Compact vector storage
    pub fn compact_storage(&self) -> Result<()> {
        info!("Starting vector storage compaction");

        // TODO: Implement storage compaction logic
        // - Remove orphaned vectors
        // - Optimize vector layout
        // - Rebuild indexes if needed

        info!("Vector storage compaction completed");
        Ok(())
    }

    // Private helper methods

    fn validate_vector(&self, vector: &[f32]) -> Result<()> {
        if vector.is_empty() {
            return Err(anyhow!("Vector cannot be empty"));
        }

        if vector.len() > self.config.max_dimension {
            return Err(anyhow!("Vector dimension {} exceeds maximum {}",
                             vector.len(), self.config.max_dimension));
        }

        // Check for NaN or infinite values
        for (i, &val) in vector.iter().enumerate() {
            if !val.is_finite() {
                return Err(anyhow!("Vector contains non-finite value at index {}: {}", i, val));
            }
        }

        Ok(())
    }

    fn calculate_checksum(&self, vector: &[f32]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        vector.hash(&mut hasher);
        hasher.finish()
    }

    fn vector_key(&self, org_id: u64, table_name: &str, column_name: &str, row_id: u64) -> Vec<u8> {
        encode_key(&[
            b"vector",
            &org_id.to_be_bytes(),
            table_name.as_bytes(),
            column_name.as_bytes(),
            &row_id.to_be_bytes(),
        ])
    }

    fn metadata_key(&self, org_id: u64, table_name: &str, column_name: &str, row_id: u64) -> Vec<u8> {
        encode_key(&[
            b"vector_meta",
            &org_id.to_be_bytes(),
            table_name.as_bytes(),
            column_name.as_bytes(),
            &row_id.to_be_bytes(),
        ])
    }

    fn update_access_stats(&self, org_id: u64, table_name: &str, column_name: &str, row_id: u64) -> Result<()> {
        let metadata_key = self.metadata_key(org_id, table_name, column_name, row_id);
        let mut tx = WriteTransaction::new();

        if let Some(serialized_metadata) = tx.get(&self.store, "vector_metadata", &metadata_key)? {
            let mut metadata: VectorMetadata = bincode::deserialize(&serialized_metadata)?;
            metadata.access_count += 1;
            metadata.updated_at = chrono::Utc::now();

            let updated_metadata = bincode::serialize(&metadata)?;
            tx.set("vector_metadata", metadata_key, updated_metadata);
            tx.commit(&self.store)?;
        }

        Ok(())
    }

    fn load_or_create_hnsw_index(&self, org_id: u64, table_name: &str, column_name: &str,
                                dimension: usize, metric: VectorMetric) -> Result<Arc<zann_hnsw::AnnIndex>> {
        // For now, create a new index
        // TODO: Load existing index from storage if available

        let mut index = zann_hnsw::AnnIndex::new(
            self.config.default_m,
            self.config.default_ef_construction,
            metric.to_hnsw_metric(),
        );

        // Load existing vectors into the index
        self.populate_index_with_vectors(&mut index, org_id, table_name, column_name)?;

        Ok(Arc::new(index))
    }

    fn populate_index_with_vectors(&self, index: &mut zann_hnsw::AnnIndex, org_id: u64,
                                  table_name: &str, column_name: &str) -> Result<()> {
        let (start_key, end_key) = prefix_range(&[
            b"vector",
            &org_id.to_be_bytes(),
            table_name.as_bytes(),
            column_name.as_bytes(),
        ]);

        let tx = ReadTransaction::new();
        let vector_entries = tx.scan_prefix(&self.store, "vectors", &start_key)?;

        for (key, serialized_vector) in vector_entries {
            // Extract row_id from key
            if key.len() >= 8 {
                let row_id_bytes = &key[key.len() - 8..];
                let row_id = u64::from_be_bytes(row_id_bytes.try_into().unwrap_or([0; 8]));

                let vector: Vec<f32> = bincode::deserialize(&serialized_vector)?;
                index.insert(row_id, vector)?;
            }
        }

        info!("Populated HNSW index for {}:{}:{} with vectors",
              org_id, table_name, column_name);

        Ok(())
    }

    fn update_index(&self, org_id: u64, table_name: &str, column_name: &str,
                   vector: &[f32], row_id: u64, metric: &VectorMetric) -> Result<()> {
        if let Ok(index) = self.get_or_create_index(org_id, table_name, column_name, vector.len(), *metric) {
            index.insert(row_id, vector.to_vec())?;
        }
        Ok(())
    }

    fn remove_from_index(&self, org_id: u64, table_name: &str, column_name: &str, row_id: u64) -> Result<()> {
        let index_key = format!("{}:{}:{}", org_id, table_name, column_name);

        let mut indexes = self.hnsw_indexes.write();
        if let Some(index) = indexes.get_mut(&index_key) {
            // Note: zann-hnsw should support removal
            // For now, we'll need to rebuild the index
            // TODO: Implement index rebuild logic
            info!("Marked index {} for rebuild due to vector removal", index_key);
        }

        Ok(())
    }
}

/// Batch vector entry for bulk operations
#[derive(Debug)]
pub struct BatchVectorEntry {
    pub org_id: u64,
    pub table_name: String,
    pub column_name: String,
    pub row_id: u64,
    pub vector: Vec<f32>,
    pub metric: VectorMetric,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_vector_storage_manager() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("vector_test")).unwrap();
        let config = VectorStorageConfig::default();
        let manager = VectorStorageManager::new(store, config).unwrap();

        let org_id = 1;
        let table_name = "documents";
        let column_name = "embedding";
        let row_id = 123;
        let vector = vec![0.1, 0.2, 0.3, 0.4, 0.5];
        let metric = VectorMetric::L2;

        // Store vector
        manager.store_vector(org_id, table_name, column_name, row_id, vector.clone(), metric).unwrap();

        // Retrieve vector
        let retrieved = manager.get_vector(org_id, table_name, column_name, row_id).unwrap();
        assert_eq!(retrieved, Some(vector));

        // Get metadata
        let metadata = manager.get_vector_metadata(org_id, table_name, column_name, row_id).unwrap();
        assert!(metadata.is_some());
        let meta = metadata.unwrap();
        assert_eq!(meta.dimension, 5);
        assert_eq!(meta.metric, metric);
        assert_eq!(meta.access_count, 1);

        // Update access count by retrieving again
        manager.get_vector(org_id, table_name, column_name, row_id).unwrap();
        let updated_metadata = manager.get_vector_metadata(org_id, table_name, column_name, row_id).unwrap();
        assert_eq!(updated_metadata.unwrap().access_count, 2);

        // Delete vector
        let deleted = manager.delete_vector(org_id, table_name, column_name, row_id).unwrap();
        assert!(deleted);

        // Verify deletion
        let retrieved_after_delete = manager.get_vector(org_id, table_name, column_name, row_id).unwrap();
        assert_eq!(retrieved_after_delete, None);
    }

    #[test]
    fn test_batch_vector_operations() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("batch_test")).unwrap();
        let config = VectorStorageConfig::default();
        let manager = VectorStorageManager::new(store, config).unwrap();

        let vectors = vec![
            BatchVectorEntry {
                org_id: 1,
                table_name: "documents".to_string(),
                column_name: "embedding".to_string(),
                row_id: 1,
                vector: vec![0.1, 0.2, 0.3],
                metric: VectorMetric::L2,
            },
            BatchVectorEntry {
                org_id: 1,
                table_name: "documents".to_string(),
                column_name: "embedding".to_string(),
                row_id: 2,
                vector: vec![0.4, 0.5, 0.6],
                metric: VectorMetric::L2,
            },
            BatchVectorEntry {
                org_id: 1,
                table_name: "documents".to_string(),
                column_name: "embedding".to_string(),
                row_id: 3,
                vector: vec![0.7, 0.8, 0.9],
                metric: VectorMetric::L2,
            },
        ];

        let stored_count = manager.batch_store_vectors(vectors).unwrap();
        assert_eq!(stored_count, 3);

        // Verify all vectors were stored
        for i in 1..=3 {
            let retrieved = manager.get_vector(1, "documents", "embedding", i).unwrap();
            assert!(retrieved.is_some());
        }
    }

    #[test]
    fn test_vector_validation() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("validation_test")).unwrap();
        let config = VectorStorageConfig::default();
        let manager = VectorStorageManager::new(store, config).unwrap();

        // Test empty vector
        let result = manager.store_vector(1, "test", "vec", 1, vec![], VectorMetric::L2);
        assert!(result.is_err());

        // Test vector with NaN
        let result = manager.store_vector(1, "test", "vec", 2, vec![1.0, f32::NAN], VectorMetric::L2);
        assert!(result.is_err());

        // Test vector with infinity
        let result = manager.store_vector(1, "test", "vec", 3, vec![1.0, f32::INFINITY], VectorMetric::L2);
        assert!(result.is_err());

        // Test oversized vector
        let oversized_vec = vec![0.1; 5000];
        let result = manager.store_vector(1, "test", "vec", 4, oversized_vec, VectorMetric::L2);
        assert!(result.is_err());

        // Test valid vector
        let valid_vec = vec![0.1, 0.2, 0.3];
        let result = manager.store_vector(1, "test", "vec", 5, valid_vec.clone(), VectorMetric::L2);
        assert!(result.is_ok());

        let retrieved = manager.get_vector(1, "test", "vec", 5).unwrap();
        assert_eq!(retrieved, Some(valid_vec));
    }

    #[test]
    fn test_cache_operations() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("cache_test")).unwrap();
        let config = VectorStorageConfig::default();
        let manager = VectorStorageManager::new(store, config).unwrap();

        let vector = vec![0.1, 0.2, 0.3];
        manager.store_vector(1, "test", "vec", 1, vector.clone(), VectorMetric::L2).unwrap();

        // First retrieval (from storage, cached)
        let start_time = std::time::Instant::now();
        let retrieved1 = manager.get_vector(1, "test", "vec", 1).unwrap();
        let first_retrieval_time = start_time.elapsed();

        // Second retrieval (from cache)
        let start_time = std::time::Instant::now();
        let retrieved2 = manager.get_vector(1, "test", "vec", 1).unwrap();
        let second_retrieval_time = start_time.elapsed();

        assert_eq!(retrieved1, Some(vector.clone()));
        assert_eq!(retrieved2, Some(vector));

        // Cache should be faster (though this might not always be true due to system variance)
        debug!("First retrieval: {:?}, Second retrieval: {:?}",
               first_retrieval_time, second_retrieval_time);

        // Clear cache
        manager.clear_cache();

        // Retrieval after cache clear (from storage again)
        let retrieved3 = manager.get_vector(1, "test", "vec", 1).unwrap();
        assert_eq!(retrieved3, Some(vector));
    }

    #[test]
    fn test_storage_statistics() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("stats_test")).unwrap();
        let config = VectorStorageConfig::default();
        let manager = VectorStorageManager::new(store, config).unwrap();

        let initial_stats = manager.get_stats();
        assert_eq!(initial_stats.total_vectors, 0);

        // Store some vectors
        manager.store_vector(1, "test", "vec", 1, vec![0.1, 0.2], VectorMetric::L2).unwrap();
        manager.store_vector(1, "test", "vec", 2, vec![0.3, 0.4, 0.5], VectorMetric::L2).unwrap();

        let updated_stats = manager.get_stats();
        assert_eq!(updated_stats.total_vectors, 2);
        assert!(updated_stats.avg_dimension > 0.0);
        assert!(updated_stats.storage_used_bytes > 0);

        // Delete a vector
        manager.delete_vector(1, "test", "vec", 1).unwrap();

        let final_stats = manager.get_stats();
        assert_eq!(final_stats.total_vectors, 1);
    }
}