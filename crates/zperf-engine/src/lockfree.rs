//! Lock-free data structures for high-performance concurrent database operations
//!
//! This module provides lock-free alternatives to traditional synchronized data structures,
//! enabling better performance in highly concurrent database workloads.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use crossbeam::queue::{ArrayQueue, SegQueue};
use crossbeam_skiplist::{SkipMap, SkipSet};
use anyhow::Result;

/// Lock-free queue for high-throughput producer-consumer scenarios
pub struct DatabaseQueue<T> {
    inner: QueueImpl<T>,
    stats: Arc<QueueStats>,
}

enum QueueImpl<T> {
    /// Bounded queue with fixed capacity (best performance)
    Bounded(ArrayQueue<T>),
    /// Unbounded queue (grows as needed)
    Unbounded(SegQueue<T>),
}

#[derive(Debug, Default)]
struct QueueStats {
    enqueue_count: AtomicUsize,
    dequeue_count: AtomicUsize,
    enqueue_failures: AtomicUsize,
}

impl QueueStats {
    fn record_enqueue(&self, success: bool) {
        self.enqueue_count.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.enqueue_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_dequeue(&self) {
        self.dequeue_count.fetch_add(1, Ordering::Relaxed);
    }

    fn get_stats(&self) -> (usize, usize, usize) {
        (
            self.enqueue_count.load(Ordering::Relaxed),
            self.dequeue_count.load(Ordering::Relaxed),
            self.enqueue_failures.load(Ordering::Relaxed),
        )
    }
}

impl<T> DatabaseQueue<T> {
    /// Create a bounded queue with fixed capacity (highest performance)
    pub fn bounded(capacity: usize) -> Self {
        Self {
            inner: QueueImpl::Bounded(ArrayQueue::new(capacity)),
            stats: Arc::new(QueueStats::default()),
        }
    }

    /// Create an unbounded queue that grows as needed
    pub fn unbounded() -> Self {
        Self {
            inner: QueueImpl::Unbounded(SegQueue::new()),
            stats: Arc::new(QueueStats::default()),
        }
    }

    /// Try to enqueue an item (non-blocking)
    pub fn try_push(&self, item: T) -> Result<(), T> {
        let result = match &self.inner {
            QueueImpl::Bounded(q) => q.push(item),
            QueueImpl::Unbounded(q) => {
                q.push(item);
                Ok(())
            }
        };

        self.stats.record_enqueue(result.is_ok());
        result
    }

    /// Try to dequeue an item (non-blocking)
    pub fn try_pop(&self) -> Option<T> {
        let result = match &self.inner {
            QueueImpl::Bounded(q) => q.pop(),
            QueueImpl::Unbounded(q) => q.pop(),
        };

        if result.is_some() {
            self.stats.record_dequeue();
        }

        result
    }

    /// Get queue length
    pub fn len(&self) -> usize {
        match &self.inner {
            QueueImpl::Bounded(q) => q.len(),
            QueueImpl::Unbounded(q) => q.len(),
        }
    }

    /// Check if the queue is empty
    pub fn is_empty(&self) -> bool {
        match &self.inner {
            QueueImpl::Bounded(q) => q.is_empty(),
            QueueImpl::Unbounded(q) => q.is_empty(),
        }
    }

    /// Get queue statistics
    pub fn get_stats(&self) -> (usize, usize, usize) {
        self.stats.get_stats()
    }
}

/// Lock-free skip list for ordered data with concurrent access
pub struct DatabaseSkipMap<K, V> {
    map: SkipMap<K, V>,
    stats: Arc<SkipMapStats>,
}

#[derive(Debug, Default)]
struct SkipMapStats {
    insert_count: AtomicUsize,
    remove_count: AtomicUsize,
    get_count: AtomicUsize,
}

impl SkipMapStats {
    fn record_insert(&self) {
        self.insert_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_remove(&self) {
        self.remove_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_get(&self) {
        self.get_count.fetch_add(1, Ordering::Relaxed);
    }

    fn get_stats(&self) -> (usize, usize, usize) {
        (
            self.insert_count.load(Ordering::Relaxed),
            self.remove_count.load(Ordering::Relaxed),
            self.get_count.load(Ordering::Relaxed),
        )
    }
}

impl<K, V> DatabaseSkipMap<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Create a new lock-free skip map
    pub fn new() -> Self {
        Self {
            map: SkipMap::new(),
            stats: Arc::new(SkipMapStats::default()),
        }
    }

    /// Insert a key-value pair
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.stats.record_insert();
        if let Some(old_entry) = self.map.get(&key) {
            let old_value = old_entry.value().clone();
            self.map.insert(key, value);
            Some(old_value)
        } else {
            self.map.insert(key, value);
            None
        }
    }

    /// Get a value by key
    pub fn get(&self, key: &K) -> Option<crossbeam_skiplist::map::Entry<K, V>> {
        self.stats.record_get();
        self.map.get(key)
    }

    /// Remove a key-value pair
    pub fn remove(&self, key: &K) -> Option<crossbeam_skiplist::map::Entry<K, V>> {
        self.stats.record_remove();
        self.map.remove(key)
    }

    /// Check if the map contains a key
    pub fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    /// Get the number of entries (approximate)
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Check if the map is empty
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Clear all entries
    pub fn clear(&self) {
        self.map.clear()
    }

    /// Iterate over all key-value pairs in order
    pub fn iter(&self) -> impl Iterator<Item = crossbeam_skiplist::map::Entry<K, V>> {
        self.map.iter()
    }

    /// Iterate over a range of keys
    pub fn range<R>(&self, range: R) -> impl Iterator<Item = crossbeam_skiplist::map::Entry<K, V>>
    where
        R: std::ops::RangeBounds<K>,
    {
        self.map.range(range)
    }

    /// Get the first key-value pair
    pub fn front(&self) -> Option<crossbeam_skiplist::map::Entry<K, V>> {
        self.map.front()
    }

    /// Get the last key-value pair
    pub fn back(&self) -> Option<crossbeam_skiplist::map::Entry<K, V>> {
        self.map.back()
    }

    /// Get statistics
    pub fn get_stats(&self) -> (usize, usize, usize) {
        self.stats.get_stats()
    }
}

impl<K, V> Default for DatabaseSkipMap<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Lock-free skip set for ordered unique values
pub struct DatabaseSkipSet<T> {
    set: SkipSet<T>,
    stats: Arc<SkipSetStats>,
}

#[derive(Debug, Default)]
struct SkipSetStats {
    insert_count: AtomicUsize,
    remove_count: AtomicUsize,
    contains_count: AtomicUsize,
}

impl SkipSetStats {
    fn record_insert(&self) {
        self.insert_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_remove(&self) {
        self.remove_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_contains(&self) {
        self.contains_count.fetch_add(1, Ordering::Relaxed);
    }

    fn get_stats(&self) -> (usize, usize, usize) {
        (
            self.insert_count.load(Ordering::Relaxed),
            self.remove_count.load(Ordering::Relaxed),
            self.contains_count.load(Ordering::Relaxed),
        )
    }
}

impl<T> DatabaseSkipSet<T>
where
    T: Ord + Clone + Send + Sync + 'static,
{
    /// Create a new lock-free skip set
    pub fn new() -> Self {
        Self {
            set: SkipSet::new(),
            stats: Arc::new(SkipSetStats::default()),
        }
    }

    /// Insert a value
    pub fn insert(&self, value: T) -> bool {
        self.stats.record_insert();
        let exists = self.set.contains(&value);
        if !exists {
            self.set.insert(value);
        }
        !exists
    }

    /// Remove a value
    pub fn remove(&self, value: &T) -> bool {
        self.stats.record_remove();
        self.set.remove(value).is_some()
    }

    /// Check if the set contains a value
    pub fn contains(&self, value: &T) -> bool {
        self.stats.record_contains();
        self.set.contains(value)
    }

    /// Get the number of values (approximate)
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// Check if the set is empty
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    /// Clear all values
    pub fn clear(&self) {
        self.set.clear()
    }

    /// Iterate over all values in order
    pub fn iter(&self) -> impl Iterator<Item = crossbeam_skiplist::set::Entry<T>> {
        self.set.iter()
    }

    /// Iterate over a range of values
    pub fn range<R>(&self, range: R) -> impl Iterator<Item = crossbeam_skiplist::set::Entry<T>>
    where
        R: std::ops::RangeBounds<T>,
    {
        self.set.range(range)
    }

    /// Get the first value
    pub fn front(&self) -> Option<crossbeam_skiplist::set::Entry<T>> {
        self.set.front()
    }

    /// Get the last value
    pub fn back(&self) -> Option<crossbeam_skiplist::set::Entry<T>> {
        self.set.back()
    }

    /// Get statistics
    pub fn get_stats(&self) -> (usize, usize, usize) {
        self.stats.get_stats()
    }
}

impl<T> Default for DatabaseSkipSet<T>
where
    T: Ord + Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Lock-free job queue for database background tasks
pub struct JobQueue<T> {
    high_priority: DatabaseQueue<T>,
    normal_priority: DatabaseQueue<T>,
    low_priority: DatabaseQueue<T>,
    total_jobs: AtomicUsize,
}

/// Job priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobPriority {
    /// High priority jobs (user queries, critical operations)
    High,
    /// Normal priority jobs (regular maintenance)
    Normal,
    /// Low priority jobs (background cleanup, analytics)
    Low,
}

impl<T> JobQueue<T> {
    /// Create a new job queue with specified capacities for each priority level
    pub fn new(high_cap: usize, normal_cap: usize, low_cap: usize) -> Self {
        Self {
            high_priority: DatabaseQueue::bounded(high_cap),
            normal_priority: DatabaseQueue::bounded(normal_cap),
            low_priority: DatabaseQueue::bounded(low_cap),
            total_jobs: AtomicUsize::new(0),
        }
    }

    /// Submit a job with specified priority
    pub fn submit(&self, job: T, priority: JobPriority) -> Result<(), T> {
        let result = match priority {
            JobPriority::High => self.high_priority.try_push(job),
            JobPriority::Normal => self.normal_priority.try_push(job),
            JobPriority::Low => self.low_priority.try_push(job),
        };

        if result.is_ok() {
            self.total_jobs.fetch_add(1, Ordering::Relaxed);
        }

        result
    }

    /// Get the next job to execute (priority order: High -> Normal -> Low)
    pub fn next_job(&self) -> Option<T> {
        // Try high priority first
        if let Some(job) = self.high_priority.try_pop() {
            self.total_jobs.fetch_sub(1, Ordering::Relaxed);
            return Some(job);
        }

        // Then normal priority
        if let Some(job) = self.normal_priority.try_pop() {
            self.total_jobs.fetch_sub(1, Ordering::Relaxed);
            return Some(job);
        }

        // Finally low priority
        if let Some(job) = self.low_priority.try_pop() {
            self.total_jobs.fetch_sub(1, Ordering::Relaxed);
            return Some(job);
        }

        None
    }

    /// Get the total number of pending jobs
    pub fn total_pending(&self) -> usize {
        self.total_jobs.load(Ordering::Relaxed)
    }

    /// Get pending jobs by priority
    pub fn pending_by_priority(&self) -> (usize, usize, usize) {
        (
            self.high_priority.len(),
            self.normal_priority.len(),
            self.low_priority.len(),
        )
    }

    /// Check if all queues are empty
    pub fn is_empty(&self) -> bool {
        self.total_jobs.load(Ordering::Relaxed) == 0
    }
}

/// Lock-free cache for frequently accessed database objects
pub struct DatabaseCache<K, V> {
    data: DatabaseSkipMap<K, CacheEntry<V>>,
    max_size: usize,
    current_size: AtomicUsize,
}

#[derive(Debug)]
struct CacheEntry<V> {
    value: V,
    access_count: AtomicUsize,
    last_access: AtomicUsize, // Timestamp
}

impl<V: Clone> Clone for CacheEntry<V> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            access_count: AtomicUsize::new(self.access_count.load(std::sync::atomic::Ordering::Relaxed)),
            last_access: AtomicUsize::new(self.last_access.load(std::sync::atomic::Ordering::Relaxed)),
        }
    }
}

impl<V> CacheEntry<V> {
    fn new(value: V) -> Self {
        Self {
            value,
            access_count: AtomicUsize::new(1),
            last_access: AtomicUsize::new(Self::current_timestamp()),
        }
    }

    fn access(&self) -> &V {
        self.access_count.fetch_add(1, Ordering::Relaxed);
        self.last_access.store(Self::current_timestamp(), Ordering::Relaxed);
        &self.value
    }

    fn get_access_count(&self) -> usize {
        self.access_count.load(Ordering::Relaxed)
    }

    fn get_last_access(&self) -> usize {
        self.last_access.load(Ordering::Relaxed)
    }

    fn current_timestamp() -> usize {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as usize
    }
}

impl<K, V> DatabaseCache<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Create a new lock-free cache with maximum size
    pub fn new(max_size: usize) -> Self {
        Self {
            data: DatabaseSkipMap::new(),
            max_size,
            current_size: AtomicUsize::new(0),
        }
    }

    /// Insert or update a cache entry
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let entry = CacheEntry::new(value);
        let old_entry = self.data.insert(key, entry);

        if old_entry.is_none() {
            self.current_size.fetch_add(1, Ordering::Relaxed);

            // Check if we need to evict entries
            if self.current_size.load(Ordering::Relaxed) > self.max_size {
                self.evict_lru();
            }
        }

        old_entry.map(|e| e.value)
    }

    /// Get a value from the cache
    pub fn get(&self, key: &K) -> Option<V> {
        self.data.get(key).map(|entry| {
            let value = entry.value().value.clone();
            // Note: In a real implementation, we'd update access stats here
            // but crossbeam skip map entries are immutable, so we'd need
            // to remove and re-insert to update access count
            value
        })
    }

    /// Remove a value from the cache
    pub fn remove(&self, key: &K) -> Option<V> {
        self.data.remove(key).map(|entry| {
            self.current_size.fetch_sub(1, Ordering::Relaxed);
            entry.value().value.clone()
        })
    }

    /// Check if the cache contains a key
    pub fn contains_key(&self, key: &K) -> bool {
        self.data.contains_key(key)
    }

    /// Get the current cache size
    pub fn len(&self) -> usize {
        self.current_size.load(Ordering::Relaxed)
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all cache entries
    pub fn clear(&self) {
        self.data.clear();
        self.current_size.store(0, Ordering::Relaxed);
    }

    /// Evict least recently used entries to stay within size limit
    fn evict_lru(&self) {
        let current_time = CacheEntry::<V>::current_timestamp();
        let mut candidates: Vec<_> = self.data
            .iter()
            .map(|entry| {
                let last_access = entry.value().get_last_access();
                let age = current_time.saturating_sub(last_access);
                (entry.key().clone(), age)
            })
            .collect();

        // Sort by age (oldest first)
        candidates.sort_by_key(|(_, age)| *age);

        // Remove oldest entries until we're back under the limit
        let target_size = self.max_size * 90 / 100; // Target 90% of max size
        let mut current = self.current_size.load(Ordering::Relaxed);

        for (key, _) in candidates {
            if current <= target_size {
                break;
            }

            if self.data.remove(&key).is_some() {
                current = self.current_size.fetch_sub(1, Ordering::Relaxed).saturating_sub(1);
            }
        }
    }
}

impl<K, V> Default for DatabaseCache<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new(1000) // Default to 1000 entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_database_queue_bounded() {
        let queue = DatabaseQueue::bounded(2);

        assert!(queue.try_push(1).is_ok());
        assert!(queue.try_push(2).is_ok());
        assert!(queue.try_push(3).is_err()); // Should fail - queue full

        assert_eq!(queue.try_pop(), Some(1));
        assert_eq!(queue.try_pop(), Some(2));
        assert_eq!(queue.try_pop(), None);

        let (enqueue, dequeue, failures) = queue.get_stats();
        assert_eq!(enqueue, 3);
        assert_eq!(dequeue, 2);
        assert_eq!(failures, 1);
    }

    #[test]
    fn test_database_queue_unbounded() {
        let queue = DatabaseQueue::unbounded();

        // Should be able to push many items
        for i in 0..1000 {
            assert!(queue.try_push(i).is_ok());
        }

        assert_eq!(queue.len(), 1000);

        // Pop all items
        for i in 0..1000 {
            assert_eq!(queue.try_pop(), Some(i));
        }

        assert!(queue.is_empty());
    }

    #[test]
    fn test_database_skip_map() {
        let map = DatabaseSkipMap::new();

        map.insert("key1", "value1");
        map.insert("key2", "value2");
        map.insert("key3", "value3");

        assert_eq!(map.get("key2").unwrap().value(), &"value2");
        assert!(map.contains_key("key1"));
        assert!(!map.contains_key("key4"));

        let removed = map.remove("key1");
        assert!(removed.is_some());
        assert!(!map.contains_key("key1"));

        // Test iteration order
        let keys: Vec<_> = map.iter().map(|entry| entry.key().clone()).collect();
        assert_eq!(keys, vec!["key2", "key3"]);

        let (inserts, removes, gets) = map.get_stats();
        assert_eq!(inserts, 3);
        assert_eq!(removes, 1);
        assert_eq!(gets, 1);
    }

    #[test]
    fn test_database_skip_set() {
        let set = DatabaseSkipSet::new();

        assert!(set.insert(3));
        assert!(set.insert(1));
        assert!(set.insert(2));
        assert!(!set.insert(2)); // Duplicate

        assert!(set.contains(&2));
        assert!(!set.contains(&4));

        assert!(set.remove(&2));
        assert!(!set.contains(&2));

        // Test iteration order
        let values: Vec<_> = set.iter().map(|entry| *entry.value()).collect();
        assert_eq!(values, vec![1, 3]);
    }

    #[test]
    fn test_job_queue() {
        let queue = JobQueue::new(5, 10, 15);

        queue.submit("high1", JobPriority::High).unwrap();
        queue.submit("normal1", JobPriority::Normal).unwrap();
        queue.submit("low1", JobPriority::Low).unwrap();
        queue.submit("high2", JobPriority::High).unwrap();

        // Should get high priority jobs first
        assert_eq!(queue.next_job(), Some("high1"));
        assert_eq!(queue.next_job(), Some("high2"));
        assert_eq!(queue.next_job(), Some("normal1"));
        assert_eq!(queue.next_job(), Some("low1"));
        assert_eq!(queue.next_job(), None);

        assert!(queue.is_empty());
    }

    #[test]
    fn test_database_cache() {
        let cache = DatabaseCache::new(3);

        cache.insert("key1", "value1");
        cache.insert("key2", "value2");
        cache.insert("key3", "value3");

        assert_eq!(cache.get("key1"), Some("value1"));
        assert_eq!(cache.get("key2"), Some("value2"));
        assert_eq!(cache.len(), 3);

        // Adding another item should trigger eviction
        cache.insert("key4", "value4");

        // Cache should still be at max size
        assert!(cache.len() <= 3);
        assert!(cache.contains_key("key4"));
    }

    #[test]
    fn test_concurrent_queue_operations() {
        let queue = Arc::new(DatabaseQueue::unbounded());
        let mut handles = vec![];

        // Spawn producer threads
        for i in 0..4 {
            let queue_clone = Arc::clone(&queue);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    let value = i * 100 + j;
                    queue_clone.try_push(value).unwrap();
                }
            });
            handles.push(handle);
        }

        // Spawn consumer threads
        for _ in 0..2 {
            let queue_clone = Arc::clone(&queue);
            let handle = thread::spawn(move || {
                let mut consumed = 0;
                while consumed < 200 {
                    if let Some(_) = queue_clone.try_pop() {
                        consumed += 1;
                    } else {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // All items should be processed
        assert!(queue.is_empty());
    }

    #[test]
    fn test_concurrent_skip_map_operations() {
        let map = Arc::new(DatabaseSkipMap::new());
        let mut handles = vec![];

        // Spawn writer threads
        for i in 0..4 {
            let map_clone = Arc::clone(&map);
            let handle = thread::spawn(move || {
                for j in 0..50 {
                    let key = format!("key{}_{}", i, j);
                    let value = i * 100 + j;
                    map_clone.insert(key, value);
                }
            });
            handles.push(handle);
        }

        // Spawn reader threads
        for i in 0..2 {
            let map_clone = Arc::clone(&map);
            let handle = thread::spawn(move || {
                for j in 0..50 {
                    let key = format!("key{}_{}", i, j);
                    // Try to read (may or may not find the key depending on timing)
                    let _ = map_clone.get(&key);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should have all inserted items
        assert_eq!(map.len(), 200);
    }
}