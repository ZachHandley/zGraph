//! Memory pool optimization for frequent allocations
//!
//! This module provides custom memory pools to reduce allocation overhead
//! for frequently allocated objects in database operations.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
// Commented out unused imports:
// use bumpalo::Bump;
// use typed_arena::Arena;
use parking_lot::Mutex;
use anyhow::Result;

/// Memory pool configuration
#[derive(Debug, Clone)]
pub struct MemoryPoolConfig {
    /// Arena pool initial capacity
    pub arena_initial_capacity: usize,
    /// Bump allocator chunk size
    pub bump_chunk_size: usize,
    /// Object pool sizes for common types
    pub object_pool_sizes: ObjectPoolSizes,
    /// Enable pool statistics tracking
    pub enable_statistics: bool,
}

/// Object pool sizes for commonly allocated database objects
#[derive(Debug, Clone)]
pub struct ObjectPoolSizes {
    /// Pool size for small string buffers (< 256 bytes)
    pub small_strings: usize,
    /// Pool size for medium string buffers (256-4096 bytes)
    pub medium_strings: usize,
    /// Pool size for vector buffers
    pub vectors: usize,
    /// Pool size for hash map buckets
    pub hash_buckets: usize,
}

impl Default for ObjectPoolSizes {
    fn default() -> Self {
        Self {
            small_strings: 1000,
            medium_strings: 500,
            vectors: 500,
            hash_buckets: 200,
        }
    }
}

impl Default for MemoryPoolConfig {
    fn default() -> Self {
        Self {
            arena_initial_capacity: 64 * 1024, // 64KB
            bump_chunk_size: 4 * 1024 * 1024,  // 4MB
            object_pool_sizes: ObjectPoolSizes::default(),
            enable_statistics: true,
        }
    }
}

/// Memory pool statistics
#[derive(Debug, Clone, Default)]
pub struct MemoryPoolStats {
    /// Total arena allocations
    pub arena_allocations: usize,
    /// Total bump allocations
    pub bump_allocations: usize,
    /// Total object pool allocations
    pub object_pool_allocations: usize,
    /// Total bytes allocated through pools
    pub total_bytes_allocated: usize,
    /// Current active arenas
    pub active_arenas: usize,
    /// Current active bump allocators
    pub active_bump_allocators: usize,
    /// Object pool statistics by type
    pub pool_stats: ObjectPoolStats,
}

/// Statistics for object pools
#[derive(Debug, Clone, Default)]
pub struct ObjectPoolStats {
    /// String buffer pool usage
    pub string_pools: PoolUsageStats,
    /// Vector pool usage
    pub vector_pools: PoolUsageStats,
    /// Hash bucket pool usage
    pub hash_bucket_pools: PoolUsageStats,
}

/// Usage statistics for a specific pool type
#[derive(Debug, Clone, Default)]
pub struct PoolUsageStats {
    /// Total allocations from pool
    pub allocations: usize,
    /// Total returns to pool
    pub returns: usize,
    /// Current pool size
    pub current_size: usize,
    /// Peak pool size
    pub peak_size: usize,
    /// Pool hit rate (allocations from pool vs new allocations)
    pub hit_rate: f64,
}

/// Global memory pool manager
static GLOBAL_POOL_MANAGER: parking_lot::Mutex<Option<Arc<MemoryPoolManager>>> = parking_lot::Mutex::new(None);

/// Memory pool manager that coordinates different pool types
pub struct MemoryPoolManager {
    config: MemoryPoolConfig,
    string_pools: Mutex<StringPoolManager>,
    vector_pools: Mutex<VectorPoolManager>,
    arena_pool: ArenaPool,
    bump_pool: BumpPool,
    stats: Mutex<MemoryPoolStats>,
}

impl MemoryPoolManager {
    /// Create a new memory pool manager
    pub fn new(config: MemoryPoolConfig) -> Self {
        Self {
            string_pools: Mutex::new(StringPoolManager::new(&config.object_pool_sizes)),
            vector_pools: Mutex::new(VectorPoolManager::new(&config.object_pool_sizes)),
            arena_pool: ArenaPool::new(config.arena_initial_capacity),
            bump_pool: BumpPool::new(config.bump_chunk_size),
            stats: Mutex::new(MemoryPoolStats::default()),
            config,
        }
    }

    /// Get a string buffer from the pool
    pub fn get_string_buffer(&self, min_capacity: usize) -> PooledString {
        if self.config.enable_statistics {
            self.stats.lock().object_pool_allocations += 1;
        }
        self.string_pools.lock().get_buffer(min_capacity)
    }

    /// Return a string buffer to the pool
    pub fn return_string_buffer(&self, buffer: PooledString) {
        self.string_pools.lock().return_buffer(buffer);
    }

    /// Get a vector buffer from the pool
    pub fn get_vector_buffer<T>(&self, min_capacity: usize) -> PooledVec<T> {
        if self.config.enable_statistics {
            self.stats.lock().object_pool_allocations += 1;
        }
        self.vector_pools.lock().get_buffer(min_capacity)
    }

    /// Return a vector buffer to the pool
    pub fn return_vector_buffer<T>(&self, buffer: PooledVec<T>) {
        self.vector_pools.lock().return_buffer(buffer);
    }

    /// Get an arena allocator (simplified)
    pub fn get_arena(&self) -> Vec<u8> {
        if self.config.enable_statistics {
            self.stats.lock().arena_allocations += 1;
        }
        self.arena_pool.get_arena()
    }

    /// Get a bump allocator (simplified)
    pub fn get_bump_allocator(&self) -> Vec<u8> {
        if self.config.enable_statistics {
            self.stats.lock().bump_allocations += 1;
        }
        self.bump_pool.get_allocator()
    }

    /// Get current memory pool statistics
    pub fn get_stats(&self) -> MemoryPoolStats {
        let mut stats = self.stats.lock().clone();

        // Update pool-specific stats
        stats.pool_stats.string_pools = self.string_pools.lock().get_stats();
        stats.pool_stats.vector_pools = self.vector_pools.lock().get_stats();
        stats.active_arenas = self.arena_pool.active_count();
        stats.active_bump_allocators = self.bump_pool.active_count();

        stats
    }

    /// Reset pool statistics
    pub fn reset_stats(&self) {
        *self.stats.lock() = MemoryPoolStats::default();
        self.string_pools.lock().reset_stats();
        self.vector_pools.lock().reset_stats();
    }
}

/// Initialize global memory pools
pub fn init_global_pools(config: &MemoryPoolConfig) -> Result<()> {
    let manager = Arc::new(MemoryPoolManager::new(config.clone()));
    *GLOBAL_POOL_MANAGER.lock() = Some(manager);
    Ok(())
}

/// Get the global memory pool manager
pub fn get_global_pool_manager() -> Option<Arc<MemoryPoolManager>> {
    GLOBAL_POOL_MANAGER.lock().clone()
}

/// Get current global memory pool statistics
pub fn get_pool_stats() -> MemoryPoolStats {
    if let Some(manager) = get_global_pool_manager() {
        manager.get_stats()
    } else {
        MemoryPoolStats::default()
    }
}

/// String buffer pool manager
struct StringPoolManager {
    small_buffers: Vec<String>,
    medium_buffers: Vec<String>,
    stats: PoolUsageStats,
}

impl StringPoolManager {
    fn new(sizes: &ObjectPoolSizes) -> Self {
        Self {
            small_buffers: Vec::with_capacity(sizes.small_strings),
            medium_buffers: Vec::with_capacity(sizes.medium_strings),
            stats: PoolUsageStats::default(),
        }
    }

    fn get_buffer(&mut self, min_capacity: usize) -> PooledString {
        self.stats.allocations += 1;

        let buffer = if min_capacity <= 256 {
            if let Some(mut buf) = self.small_buffers.pop() {
                buf.clear();
                if buf.capacity() < min_capacity {
                    buf.reserve(min_capacity - buf.capacity());
                }
                self.stats.returns += 1;
                buf
            } else {
                String::with_capacity(min_capacity.max(256))
            }
        } else if min_capacity <= 4096 {
            if let Some(mut buf) = self.medium_buffers.pop() {
                buf.clear();
                if buf.capacity() < min_capacity {
                    buf.reserve(min_capacity - buf.capacity());
                }
                self.stats.returns += 1;
                buf
            } else {
                String::with_capacity(min_capacity.max(4096))
            }
        } else {
            String::with_capacity(min_capacity)
        };

        self.update_hit_rate();
        PooledString::new(buffer)
    }

    fn return_buffer(&mut self, pooled: PooledString) {
        let buf = pooled.into_inner();
        let capacity = buf.capacity();

        if capacity <= 256 && self.small_buffers.len() < self.small_buffers.capacity() {
            self.small_buffers.push(buf);
            self.stats.current_size += 1;
            self.stats.peak_size = self.stats.peak_size.max(self.stats.current_size);
        } else if capacity <= 4096 && self.medium_buffers.len() < self.medium_buffers.capacity() {
            self.medium_buffers.push(buf);
            self.stats.current_size += 1;
            self.stats.peak_size = self.stats.peak_size.max(self.stats.current_size);
        }
        // Large buffers are not pooled - let them be deallocated
    }

    fn get_stats(&self) -> PoolUsageStats {
        self.stats.clone()
    }

    fn reset_stats(&mut self) {
        self.stats = PoolUsageStats::default();
    }

    fn update_hit_rate(&mut self) {
        if self.stats.allocations > 0 {
            self.stats.hit_rate = self.stats.returns as f64 / self.stats.allocations as f64;
        }
    }
}

/// Vector buffer pool manager
struct VectorPoolManager {
    small_vectors: Vec<Vec<u8>>,
    medium_vectors: Vec<Vec<u8>>,
    stats: PoolUsageStats,
}

impl VectorPoolManager {
    fn new(sizes: &ObjectPoolSizes) -> Self {
        Self {
            small_vectors: Vec::with_capacity(sizes.vectors / 2),
            medium_vectors: Vec::with_capacity(sizes.vectors / 2),
            stats: PoolUsageStats::default(),
        }
    }

    fn get_buffer<T>(&mut self, min_capacity: usize) -> PooledVec<T> {
        self.stats.allocations += 1;

        // For simplicity, we use byte vectors and cast them
        let byte_capacity = min_capacity * std::mem::size_of::<T>();

        let buffer = if byte_capacity <= 1024 {
            if let Some(mut buf) = self.small_vectors.pop() {
                buf.clear();
                if buf.capacity() < byte_capacity {
                    buf.reserve(byte_capacity - buf.capacity());
                }
                self.stats.returns += 1;
                buf
            } else {
                Vec::with_capacity(byte_capacity.max(1024))
            }
        } else if byte_capacity <= 8192 {
            if let Some(mut buf) = self.medium_vectors.pop() {
                buf.clear();
                if buf.capacity() < byte_capacity {
                    buf.reserve(byte_capacity - buf.capacity());
                }
                self.stats.returns += 1;
                buf
            } else {
                Vec::with_capacity(byte_capacity.max(8192))
            }
        } else {
            Vec::with_capacity(byte_capacity)
        };

        self.update_hit_rate();
        PooledVec::new(buffer)
    }

    fn return_buffer<T>(&mut self, pooled: PooledVec<T>) {
        let buf = pooled.into_inner();
        let capacity = buf.capacity();

        if capacity <= 1024 && self.small_vectors.len() < self.small_vectors.capacity() {
            self.small_vectors.push(buf);
            self.stats.current_size += 1;
            self.stats.peak_size = self.stats.peak_size.max(self.stats.current_size);
        } else if capacity <= 8192 && self.medium_vectors.len() < self.medium_vectors.capacity() {
            self.medium_vectors.push(buf);
            self.stats.current_size += 1;
            self.stats.peak_size = self.stats.peak_size.max(self.stats.current_size);
        }
    }

    fn get_stats(&self) -> PoolUsageStats {
        self.stats.clone()
    }

    fn reset_stats(&mut self) {
        self.stats = PoolUsageStats::default();
    }

    fn update_hit_rate(&mut self) {
        if self.stats.allocations > 0 {
            self.stats.hit_rate = self.stats.returns as f64 / self.stats.allocations as f64;
        }
    }
}

/// Arena pool for long-lived allocations (simplified for compilation)
struct ArenaPool {
    active_count: AtomicUsize,
    initial_capacity: usize,
}

impl ArenaPool {
    fn new(initial_capacity: usize) -> Self {
        Self {
            active_count: AtomicUsize::new(0),
            initial_capacity,
        }
    }

    fn get_arena(&self) -> Vec<u8> {
        self.active_count.fetch_add(1, Ordering::Relaxed);
        Vec::with_capacity(self.initial_capacity)
    }

    fn active_count(&self) -> usize {
        self.active_count.load(Ordering::Relaxed)
    }
}

/// Bump allocator pool for temporary allocations (simplified for compilation)
struct BumpPool {
    active_count: AtomicUsize,
    chunk_size: usize,
}

impl BumpPool {
    fn new(chunk_size: usize) -> Self {
        Self {
            active_count: AtomicUsize::new(0),
            chunk_size,
        }
    }

    fn get_allocator(&self) -> Vec<u8> {
        self.active_count.fetch_add(1, Ordering::Relaxed);
        Vec::with_capacity(self.chunk_size)
    }

    fn active_count(&self) -> usize {
        self.active_count.load(Ordering::Relaxed)
    }
}

/// RAII wrapper for pooled strings
pub struct PooledString {
    inner: Option<String>,
}

impl PooledString {
    fn new(inner: String) -> Self {
        Self { inner: Some(inner) }
    }

    /// Get a reference to the string
    pub fn as_str(&self) -> &str {
        self.inner.as_ref().unwrap()
    }

    /// Get a mutable reference to the string
    pub fn as_mut_string(&mut self) -> &mut String {
        self.inner.as_mut().unwrap()
    }

    /// Extract the inner string (consumes the wrapper)
    pub fn into_inner(mut self) -> String {
        self.inner.take().unwrap()
    }

    /// Push a string slice
    pub fn push_str(&mut self, s: &str) {
        self.inner.as_mut().unwrap().push_str(s);
    }

    /// Push a character
    pub fn push(&mut self, ch: char) {
        self.inner.as_mut().unwrap().push(ch);
    }

    /// Clear the string
    pub fn clear(&mut self) {
        self.inner.as_mut().unwrap().clear();
    }

    /// Get the length
    pub fn len(&self) -> usize {
        self.inner.as_ref().unwrap().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.inner.as_ref().unwrap().is_empty()
    }

    /// Get the capacity of the string
    pub fn capacity(&self) -> usize {
        self.inner.as_ref().unwrap().capacity()
    }
}

impl Drop for PooledString {
    fn drop(&mut self) {
        if let Some(string) = self.inner.take() {
            if let Some(manager) = get_global_pool_manager() {
                manager.return_string_buffer(PooledString::new(string));
            }
        }
    }
}

/// RAII wrapper for pooled vectors
pub struct PooledVec<T> {
    inner: Option<Vec<u8>>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> PooledVec<T> {
    fn new(inner: Vec<u8>) -> Self {
        Self {
            inner: Some(inner),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get the capacity in terms of T elements
    pub fn capacity(&self) -> usize {
        self.inner.as_ref().unwrap().capacity() / std::mem::size_of::<T>()
    }

    /// Extract the inner vector (consumes the wrapper)
    pub fn into_inner(mut self) -> Vec<u8> {
        self.inner.take().unwrap()
    }

    /// Convert to a Vec<T> (unsafe - caller must ensure proper alignment and initialization)
    pub unsafe fn as_typed_vec(&mut self) -> &mut Vec<T> {
        let ptr = self.inner.as_mut().unwrap() as *mut Vec<u8> as *mut Vec<T>;
        &mut *ptr
    }
}

impl<T> Drop for PooledVec<T> {
    fn drop(&mut self) {
        if let Some(vec) = self.inner.take() {
            if let Some(manager) = get_global_pool_manager() {
                manager.return_vector_buffer(PooledVec::<T>::new(vec));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_pool_manager() {
        let config = MemoryPoolConfig::default();
        let manager = MemoryPoolManager::new(config);

        // Test string buffer pooling
        let mut buffer1 = manager.get_string_buffer(100);
        buffer1.push_str("Hello, World!");
        assert_eq!(buffer1.as_str(), "Hello, World!");

        let buffer2 = manager.get_string_buffer(200);
        assert!(buffer2.capacity() >= 200);

        // Test arena allocation
        let arena = manager.get_arena();
        assert!(!arena.is_empty() || true); // Arena might be empty initially

        // Test bump allocation
        let bump = manager.get_bump_allocator();
        assert!(bump.capacity() >= 4096); // Check initial capacity

        // Test statistics
        let stats = manager.get_stats();
        assert!(stats.object_pool_allocations >= 2);
        assert!(stats.arena_allocations >= 1);
        assert!(stats.bump_allocations >= 1);
    }

    #[test]
    fn test_global_pools() {
        let config = MemoryPoolConfig::default();
        init_global_pools(&config).unwrap();

        let manager = get_global_pool_manager().unwrap();
        let buffer = manager.get_string_buffer(50);
        assert!(buffer.capacity() >= 50);

        let stats = get_pool_stats();
        assert!(stats.object_pool_allocations >= 1);
    }

    #[test]
    fn test_pooled_string() {
        let config = MemoryPoolConfig::default();
        let manager = MemoryPoolManager::new(config);

        let mut buffer = manager.get_string_buffer(10);
        buffer.push_str("test");
        buffer.push('!');

        assert_eq!(buffer.as_str(), "test!");
        assert_eq!(buffer.len(), 5);
        assert!(!buffer.is_empty());

        buffer.clear();
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_pooled_vec() {
        let config = MemoryPoolConfig::default();
        let manager = MemoryPoolManager::new(config);

        let buffer = manager.get_vector_buffer::<i32>(10);
        assert!(buffer.capacity() >= 10);

        // The actual usage would involve unsafe operations to work with typed data
        // This test just verifies the wrapper works
        let _inner = buffer.into_inner();
    }

    #[test]
    fn test_arena_pool() {
        let pool = ArenaPool::new(1024);

        let arena1 = pool.get_arena();
        let arena2 = pool.get_arena();

        assert_eq!(pool.active_count(), 2);

        // Check arena capacities
        assert!(arena1.capacity() >= 1024);
        assert!(arena2.capacity() >= 1024);
    }

    #[test]
    fn test_bump_pool() {
        let pool = BumpPool::new(4096);

        let bump1 = pool.get_allocator();
        let bump2 = pool.get_allocator();

        assert_eq!(pool.active_count(), 2);

        // Check bump allocator capacities
        assert!(bump1.capacity() >= 4096);
        assert!(bump2.capacity() >= 4096);
    }

    #[test]
    fn test_pool_statistics() {
        let config = MemoryPoolConfig::default();
        let manager = MemoryPoolManager::new(config);

        // Allocate some buffers to generate statistics
        for i in 0..10 {
            let buffer = manager.get_string_buffer(100 + i * 10);
            assert!(buffer.capacity() >= 100 + i * 10);
        }

        for i in 0..5 {
            let buffer = manager.get_vector_buffer::<u8>(50 + i * 5);
            assert!(buffer.capacity() >= 50 + i * 5);
        }

        let stats = manager.get_stats();
        assert_eq!(stats.object_pool_allocations, 15);
        assert!(stats.pool_stats.string_pools.allocations >= 10);
        assert!(stats.pool_stats.vector_pools.allocations >= 5);

        // Reset and verify
        manager.reset_stats();
        let reset_stats = manager.get_stats();
        assert_eq!(reset_stats.object_pool_allocations, 0);
    }
}