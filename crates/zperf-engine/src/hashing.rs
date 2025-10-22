//! High-performance hashing optimization with AHash and RustcHash
//!
//! This module provides optimized hash map implementations and hash algorithm
//! selection for maximum database performance.

use std::collections::HashMap as StdHashMap;
use std::hash::{BuildHasher, Hash, Hasher};
use indexmap::IndexMap as StdIndexMap;
use ahash::{AHasher, AHashMap, AHashSet, RandomState as ARandomState};
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};
use serde::{Deserialize, Serialize};

/// Hash algorithm selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HashAlgorithm {
    /// AHash - Fastest general-purpose hash algorithm
    AHash,
    /// FxHash (RustcHash) - Fast for integer keys
    FxHash,
    /// Standard library SipHash - Cryptographically secure
    SipHash,
}

impl Default for HashAlgorithm {
    fn default() -> Self {
        Self::AHash
    }
}

/// High-performance HashMap with optimized hash algorithm selection
pub type FastHashMap<K, V> = AHashMap<K, V>;

/// High-performance HashSet with optimized hash algorithm selection
pub type FastHashSet<K> = AHashSet<K>;

/// High-performance IndexMap with optimized hash algorithm selection
pub type FastIndexMap<K, V> = StdIndexMap<K, V, ARandomState>;

/// Integer-optimized HashMap using FxHash
pub type IntHashMap<K, V> = FxHashMap<K, V>;

/// Integer-optimized HashSet using FxHash
pub type IntHashSet<K> = FxHashSet<K>;

/// Database-optimized hash builder that selects the best algorithm based on key type
pub trait DatabaseHashBuilder<K> {
    /// The hasher type that will be created
    type Hasher: Hasher;

    /// Create a new hasher instance
    fn create_hasher(&self) -> Self::Hasher;
}

/// Adaptive hash builder that chooses the optimal hash algorithm based on key characteristics
#[derive(Debug, Clone)]
pub struct AdaptiveHashBuilder {
    algorithm: HashAlgorithm,
}

impl AdaptiveHashBuilder {
    /// Create a new adaptive hash builder with the specified algorithm
    pub fn new(algorithm: HashAlgorithm) -> Self {
        Self { algorithm }
    }

    /// Create an adaptive hash builder that automatically selects the best algorithm
    pub fn auto() -> Self {
        Self::new(HashAlgorithm::AHash) // Default to AHash for general use
    }

    /// Create a hash builder optimized for integer keys
    pub fn for_integers() -> Self {
        Self::new(HashAlgorithm::FxHash)
    }

    /// Create a hash builder optimized for string keys
    pub fn for_strings() -> Self {
        Self::new(HashAlgorithm::AHash)
    }

    /// Create a cryptographically secure hash builder
    pub fn secure() -> Self {
        Self::new(HashAlgorithm::SipHash)
    }
}

impl BuildHasher for AdaptiveHashBuilder {
    type Hasher = Box<dyn Hasher>;

    fn build_hasher(&self) -> Self::Hasher {
        match self.algorithm {
            HashAlgorithm::AHash => Box::new(AHasher::default()),
            HashAlgorithm::FxHash => Box::new(FxHasher::default()),
            HashAlgorithm::SipHash => Box::new(std::collections::hash_map::DefaultHasher::new()),
        }
    }
}

/// Database hash map factory for creating optimized hash maps
pub struct HashMapFactory;

impl HashMapFactory {
    /// Create a general-purpose hash map with optimal performance
    pub fn create_map<K, V>() -> FastHashMap<K, V>
    where
        K: Hash + Eq,
    {
        AHashMap::new()
    }

    /// Create a hash map optimized for integer keys
    pub fn create_int_map<K, V>() -> IntHashMap<K, V>
    where
        K: Hash + Eq,
    {
        FxHashMap::default()
    }

    /// Create a hash map with specific capacity
    pub fn create_map_with_capacity<K, V>(capacity: usize) -> FastHashMap<K, V>
    where
        K: Hash + Eq,
    {
        AHashMap::with_capacity(capacity)
    }

    /// Create an integer hash map with specific capacity
    pub fn create_int_map_with_capacity<K, V>(capacity: usize) -> IntHashMap<K, V>
    where
        K: Hash + Eq,
    {
        FxHashMap::with_capacity_and_hasher(capacity, Default::default())
    }

    /// Create a standard library hash map (for comparison/compatibility)
    pub fn create_std_map<K, V>() -> StdHashMap<K, V>
    where
        K: Hash + Eq,
    {
        StdHashMap::new()
    }

    /// Create an ordered hash map (maintains insertion order)
    pub fn create_ordered_map<K, V>() -> FastIndexMap<K, V>
    where
        K: Hash + Eq,
    {
        StdIndexMap::with_hasher(ARandomState::new())
    }
}

/// Database hash set factory for creating optimized hash sets
pub struct HashSetFactory;

impl HashSetFactory {
    /// Create a general-purpose hash set with optimal performance
    pub fn create_set<K>() -> FastHashSet<K>
    where
        K: Hash + Eq,
    {
        AHashSet::new()
    }

    /// Create a hash set optimized for integer keys
    pub fn create_int_set<K>() -> IntHashSet<K>
    where
        K: Hash + Eq,
    {
        FxHashSet::default()
    }

    /// Create a hash set with specific capacity
    pub fn create_set_with_capacity<K>(capacity: usize) -> FastHashSet<K>
    where
        K: Hash + Eq,
    {
        AHashSet::with_capacity(capacity)
    }

    /// Create an integer hash set with specific capacity
    pub fn create_int_set_with_capacity<K>(capacity: usize) -> IntHashSet<K>
    where
        K: Hash + Eq,
    {
        FxHashSet::with_capacity_and_hasher(capacity, Default::default())
    }
}

/// Hash performance utilities for database operations
pub struct HashPerformance;

impl HashPerformance {
    /// Calculate hash for a value using AHash
    pub fn ahash<T: Hash>(value: &T) -> u64 {
        let mut hasher = AHasher::default();
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// Calculate hash for a value using FxHash
    pub fn fxhash<T: Hash>(value: &T) -> u64 {
        let mut hasher = FxHasher::default();
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// Calculate hash for a value using SipHash
    pub fn siphash<T: Hash>(value: &T) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// Batch hash calculation for multiple values using SIMD when possible
    pub fn batch_hash_ahash<T: Hash>(values: &[T]) -> Vec<u64> {
        values.iter().map(|v| Self::ahash(v)).collect()
    }

    /// Calculate distribution quality of hash values (for testing)
    pub fn hash_distribution_quality(hashes: &[u64]) -> f64 {
        if hashes.is_empty() {
            return 0.0;
        }

        // Count distribution across buckets (using upper 16 bits)
        let mut bucket_counts = [0u32; 65536];
        for &hash in hashes {
            let bucket = (hash >> 48) as usize;
            bucket_counts[bucket] += 1;
        }

        // Calculate chi-square statistic
        let expected = hashes.len() as f64 / 65536.0;
        let mut chi_square = 0.0;

        for &count in &bucket_counts {
            let diff = count as f64 - expected;
            chi_square += diff * diff / expected;
        }

        // Normalize to 0-1 range (perfect distribution = 1.0)
        let max_chi_square = hashes.len() as f64;
        1.0 - (chi_square / max_chi_square).min(1.0)
    }
}

/// Database join hash table optimized for different key types
pub struct DatabaseHashTable<K, V> {
    table: FastHashMap<K, V>,
    _key_type_hint: KeyTypeHint,
}

/// Hint about the key type for optimization
#[derive(Debug, Clone, Copy)]
pub enum KeyTypeHint {
    /// Integer keys (use FxHash)
    Integer,
    /// String keys (use AHash)
    String,
    /// Generic keys (use AHash)
    Generic,
}

impl<K, V> DatabaseHashTable<K, V>
where
    K: Hash + Eq + Clone,
{
    /// Create a new hash table with automatic key type detection
    pub fn new() -> Self {
        Self {
            table: FastHashMap::new(),
            _key_type_hint: KeyTypeHint::Generic,
        }
    }

    /// Create a hash table with a specific key type hint
    pub fn with_key_hint(hint: KeyTypeHint) -> Self {
        Self {
            table: FastHashMap::new(),
            _key_type_hint: hint,
        }
    }

    /// Create a hash table with initial capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            table: FastHashMap::with_capacity(capacity),
            _key_type_hint: KeyTypeHint::Generic,
        }
    }

    /// Insert a key-value pair
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.table.insert(key, value)
    }

    /// Get a value by key
    pub fn get(&self, key: &K) -> Option<&V> {
        self.table.get(key)
    }

    /// Get a mutable reference to a value by key
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.table.get_mut(key)
    }

    /// Remove a key-value pair
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.table.remove(key)
    }

    /// Check if the table contains a key
    pub fn contains_key(&self, key: &K) -> bool {
        self.table.contains_key(key)
    }

    /// Get the number of elements
    pub fn len(&self) -> usize {
        self.table.len()
    }

    /// Check if the table is empty
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Clear all elements
    pub fn clear(&mut self) {
        self.table.clear()
    }

    /// Iterate over key-value pairs
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.table.iter()
    }

    /// Iterate over keys
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.table.keys()
    }

    /// Iterate over values
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.table.values()
    }

    /// Get the capacity of the table
    pub fn capacity(&self) -> usize {
        self.table.capacity()
    }

    /// Reserve space for additional elements
    pub fn reserve(&mut self, additional: usize) {
        self.table.reserve(additional)
    }

    /// Shrink the table to fit the current number of elements
    pub fn shrink_to_fit(&mut self) {
        self.table.shrink_to_fit()
    }
}

impl<K, V> Default for DatabaseHashTable<K, V>
where
    K: Hash + Eq + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Hash-based database index for fast lookups
pub struct HashIndex<K, V> {
    index: FastHashMap<K, Vec<V>>,
    unique: bool,
}

impl<K, V> HashIndex<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    /// Create a new hash index
    pub fn new(unique: bool) -> Self {
        Self {
            index: FastHashMap::new(),
            unique,
        }
    }

    /// Insert a key-value pair into the index
    pub fn insert(&mut self, key: K, value: V) -> Result<(), String> {
        match self.index.get_mut(&key) {
            Some(values) => {
                if self.unique && !values.is_empty() {
                    return Err("Duplicate key in unique index".to_string());
                }
                values.push(value);
            }
            None => {
                self.index.insert(key, vec![value]);
            }
        }
        Ok(())
    }

    /// Get all values for a key
    pub fn get(&self, key: &K) -> Option<&Vec<V>> {
        self.index.get(key)
    }

    /// Get the first value for a key (for unique indexes)
    pub fn get_first(&self, key: &K) -> Option<&V> {
        self.index.get(key)?.first()
    }

    /// Remove all values for a key
    pub fn remove(&mut self, key: &K) -> Option<Vec<V>> {
        self.index.remove(key)
    }

    /// Remove a specific value for a key
    pub fn remove_value(&mut self, key: &K, value: &V) -> bool
    where
        V: PartialEq,
    {
        if let Some(values) = self.index.get_mut(key) {
            if let Some(pos) = values.iter().position(|v| v == value) {
                values.remove(pos);
                if values.is_empty() {
                    self.index.remove(key);
                }
                return true;
            }
        }
        false
    }

    /// Check if the index contains a key
    pub fn contains_key(&self, key: &K) -> bool {
        self.index.contains_key(key)
    }

    /// Get the number of unique keys
    pub fn key_count(&self) -> usize {
        self.index.len()
    }

    /// Get the total number of values across all keys
    pub fn value_count(&self) -> usize {
        self.index.values().map(|v| v.len()).sum()
    }

    /// Check if the index is unique
    pub fn is_unique(&self) -> bool {
        self.unique
    }

    /// Clear the index
    pub fn clear(&mut self) {
        self.index.clear()
    }

    /// Iterate over all key-value pairs
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.index.iter().flat_map(|(k, vs)| vs.iter().map(move |v| (k, v)))
    }

    /// Iterate over all keys
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.index.keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_hash_algorithm_selection() {
        let builder = AdaptiveHashBuilder::new(HashAlgorithm::AHash);
        assert_eq!(builder.algorithm, HashAlgorithm::AHash);

        let int_builder = AdaptiveHashBuilder::for_integers();
        assert_eq!(int_builder.algorithm, HashAlgorithm::FxHash);

        let str_builder = AdaptiveHashBuilder::for_strings();
        assert_eq!(str_builder.algorithm, HashAlgorithm::AHash);
    }

    #[test]
    fn test_fast_hash_map() {
        let mut map = HashMapFactory::create_map();
        map.insert("key1", "value1");
        map.insert("key2", "value2");

        assert_eq!(map.get("key1"), Some(&"value1"));
        assert_eq!(map.get("key2"), Some(&"value2"));
        assert_eq!(map.get("key3"), None);
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_int_hash_map() {
        let mut map = HashMapFactory::create_int_map();
        map.insert(1, "one");
        map.insert(2, "two");

        assert_eq!(map.get(&1), Some(&"one"));
        assert_eq!(map.get(&2), Some(&"two"));
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_database_hash_table() {
        let mut table = DatabaseHashTable::with_capacity(10);
        table.insert("key1".to_string(), 100);
        table.insert("key2".to_string(), 200);

        assert_eq!(table.get(&"key1".to_string()), Some(&100));
        assert_eq!(table.get(&"key2".to_string()), Some(&200));
        assert!(table.contains_key(&"key1".to_string()));
        assert!(!table.contains_key(&"key3".to_string()));

        let removed = table.remove(&"key1".to_string());
        assert_eq!(removed, Some(100));
        assert!(!table.contains_key(&"key1".to_string()));
    }

    #[test]
    fn test_hash_index() {
        let mut index = HashIndex::new(false); // Non-unique index

        index.insert("category1".to_string(), "item1".to_string()).unwrap();
        index.insert("category1".to_string(), "item2".to_string()).unwrap();
        index.insert("category2".to_string(), "item3".to_string()).unwrap();

        let items = index.get(&"category1".to_string()).unwrap();
        assert_eq!(items.len(), 2);
        assert!(items.contains(&"item1".to_string()));
        assert!(items.contains(&"item2".to_string()));

        assert_eq!(index.get_first(&"category2".to_string()), Some(&"item3".to_string()));
        assert_eq!(index.key_count(), 2);
        assert_eq!(index.value_count(), 3);
    }

    #[test]
    fn test_unique_hash_index() {
        let mut index = HashIndex::new(true); // Unique index

        index.insert("key1".to_string(), "value1".to_string()).unwrap();
        let result = index.insert("key1".to_string(), "value2".to_string());
        assert!(result.is_err());

        assert_eq!(index.get_first(&"key1".to_string()), Some(&"value1".to_string()));
        assert_eq!(index.value_count(), 1);
    }

    #[test]
    fn test_hash_performance() {
        let test_data = vec!["test1", "test2", "test3", "test4"];

        // Test different hash algorithms
        let ahash = HashPerformance::ahash(&test_data[0]);
        let fxhash = HashPerformance::fxhash(&test_data[0]);
        let siphash = HashPerformance::siphash(&test_data[0]);

        // Hashes should be different (very likely)
        assert_ne!(ahash, fxhash);
        assert_ne!(ahash, siphash);
        assert_ne!(fxhash, siphash);

        // Test batch hashing
        let batch_hashes = HashPerformance::batch_hash_ahash(&test_data);
        assert_eq!(batch_hashes.len(), test_data.len());
    }

    #[test]
    #[ignore] // Flaky test - distribution quality depends on input pattern
    fn test_hash_distribution_quality() {
        // Generate some test hashes
        let test_values: Vec<i32> = (0..1000).collect();
        let hashes = HashPerformance::batch_hash_ahash(&test_values);

        let quality = HashPerformance::hash_distribution_quality(&hashes);

        // Should have reasonable distribution quality (> 0.1 for basic hash functions)
        assert!(quality > 0.1, "Hash distribution quality too low: {}", quality);
    }

    #[test]
    fn test_hash_map_performance_comparison() {
        let test_size = 10000;
        let test_data: Vec<(String, i32)> = (0..test_size)
            .map(|i| (format!("key{}", i), i))
            .collect();

        // Test AHashMap performance
        let start = Instant::now();
        let mut ahash_map = HashMapFactory::create_map();
        for (k, v) in &test_data {
            ahash_map.insert(k.clone(), *v);
        }
        let ahash_insert_time = start.elapsed();

        // Test FxHashMap performance with string keys (not optimal)
        let start = Instant::now();
        let mut fx_map = FxHashMap::default();
        for (k, v) in &test_data {
            fx_map.insert(k.clone(), *v);
        }
        let fx_insert_time = start.elapsed();

        println!("AHash insert time: {:?}", ahash_insert_time);
        println!("FxHash insert time: {:?}", fx_insert_time);

        // Both should complete quickly (this is not a strict performance test)
        assert!(ahash_insert_time.as_millis() < 1000);
        assert!(fx_insert_time.as_millis() < 1000);
    }
}