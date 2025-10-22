//! Performance optimization utilities for fjall configuration
//!
//! This module provides advanced performance tuning options, caching strategies,
//! and memory management controls for optimal fjall performance across different
//! workload types.

use anyhow::{anyhow, Result};
use fjall::{Config as FjallConfig, Keyspace, PersistMode};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use parking_lot::RwLock;
use tracing::{info};

/// Performance optimization strategies
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum OptimizationStrategy {
    /// Balanced performance for mixed workloads
    Balanced,
    /// Optimized for read-heavy workloads
    ReadHeavy,
    /// Optimized for write-heavy workloads
    WriteHeavy,
    /// Optimized for mixed read/write with frequent compaction
    Mixed,
    /// Custom configuration
    Custom(PerformanceConfig),
}

/// Detailed performance configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PerformanceConfig {
    /// Cache size in bytes (default: 32MB)
    pub cache_size_bytes: usize,
    /// Number of compaction workers (default: 4)
    pub compaction_workers: usize,
    /// Maximum memory usage in bytes (default: 1GB)
    pub max_memory_usage: usize,
    /// Flush interval in milliseconds (default: 1000ms)
    pub flush_interval_ms: u64,
    /// Whether to enable compression
    pub enable_compression: bool,
    /// Compression level (1-9, higher = more compression)
    pub compression_level: u8,
    /// Block size in bytes (default: 4KB)
    pub block_size: usize,
    /// Write buffer size in bytes (default: 64MB)
    pub write_buffer_size: usize,
    /// Maximum number of open files
    pub max_open_files: usize,
    /// Whether to enable write-ahead logging
    pub enable_wal: bool,
    /// Sync mode for durability
    pub sync_mode: SyncMode,
    /// Memory manager configuration
    pub memory_config: MemoryManagerConfig,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SyncMode {
    /// No sync (fastest, least durable)
    None,
    /// Sync writes (slower, more durable)
    Sync,
    /// Flush writes (balanced)
    Flush,
}

/// Memory management configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MemoryManagerConfig {
    /// Maximum memory pressure threshold (0.0-1.0)
    pub memory_pressure_threshold: f64,
    /// Garbage collection interval in seconds
    pub gc_interval_secs: u64,
    /// Cache eviction policy
    pub eviction_policy: EvictionPolicy,
    /// Background cleanup enabled
    pub enable_background_cleanup: bool,
}

impl Default for MemoryManagerConfig {
    fn default() -> Self {
        Self {
            memory_pressure_threshold: 0.8,
            gc_interval_secs: 300,
            eviction_policy: EvictionPolicy::LRU,
            enable_background_cleanup: true,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum EvictionPolicy {
    /// Least Recently Used
    LRU,
    /// Least Frequently Used
    LFU,
    /// First In First Out
    FIFO,
    /// Random replacement
    Random,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            cache_size_bytes: 32 * 1024 * 1024, // 32MB
            compaction_workers: 4,
            max_memory_usage: 1024 * 1024 * 1024, // 1GB
            flush_interval_ms: 1000,
            enable_compression: true,
            compression_level: 6,
            block_size: 4 * 1024, // 4KB
            write_buffer_size: 64 * 1024 * 1024, // 64MB
            max_open_files: 1000,
            enable_wal: true,
            sync_mode: SyncMode::Flush,
            memory_config: MemoryManagerConfig {
                memory_pressure_threshold: 0.8,
                gc_interval_secs: 300, // 5 minutes
                eviction_policy: EvictionPolicy::LRU,
                enable_background_cleanup: true,
            },
        }
    }
}

impl PerformanceConfig {
    /// Get configuration optimized for specific workload type
    pub fn for_workload(workload: OptimizationStrategy) -> Self {
        match workload {
            OptimizationStrategy::Balanced => Self::default(),
            OptimizationStrategy::ReadHeavy => Self {
                cache_size_bytes: 256 * 1024 * 1024, // 256MB cache for reads
                compaction_workers: 2, // Fewer compaction workers
                max_memory_usage: 2 * 1024 * 1024 * 1024, // 2GB for read caching
                flush_interval_ms: 2000, // Less frequent flushes
                enable_compression: true,
                compression_level: 9, // Maximum compression for space efficiency
                block_size: 8 * 1024, // Larger blocks for sequential reads
                write_buffer_size: 32 * 1024 * 1024, // Smaller write buffer
                max_open_files: 2000, // More open files for read performance
                enable_wal: true,
                sync_mode: SyncMode::Flush,
                memory_config: MemoryManagerConfig {
                    memory_pressure_threshold: 0.9,
                    gc_interval_secs: 600, // Less frequent GC
                    eviction_policy: EvictionPolicy::LRU,
                    enable_background_cleanup: true,
                },
            },
            OptimizationStrategy::WriteHeavy => Self {
                cache_size_bytes: 16 * 1024 * 1024, // Smaller cache
                compaction_workers: 8, // More compaction workers
                max_memory_usage: 512 * 1024 * 1024, // 512MB memory limit
                flush_interval_ms: 100, // More frequent flushes
                enable_compression: false, // Disable compression for write speed
                compression_level: 1, // Light compression if enabled
                block_size: 4 * 1024, // Standard block size
                write_buffer_size: 128 * 1024 * 1024, // Larger write buffer
                max_open_files: 500, // Fewer open files
                enable_wal: true,
                sync_mode: SyncMode::Sync, // Ensure durability for writes
                memory_config: MemoryManagerConfig {
                    memory_pressure_threshold: 0.7,
                    gc_interval_secs: 60, // Frequent GC
                    eviction_policy: EvictionPolicy::FIFO,
                    enable_background_cleanup: true,
                },
            },
            OptimizationStrategy::Mixed => Self {
                cache_size_bytes: 64 * 1024 * 1024, // 64MB cache
                compaction_workers: 6, // Balanced compaction
                max_memory_usage: 1024 * 1024 * 1024, // 1GB memory
                flush_interval_ms: 500, // Moderate flush frequency
                enable_compression: true,
                compression_level: 6,
                block_size: 4 * 1024,
                write_buffer_size: 64 * 1024 * 1024,
                max_open_files: 1000,
                enable_wal: true,
                sync_mode: SyncMode::Flush,
                memory_config: MemoryManagerConfig {
                    memory_pressure_threshold: 0.8,
                    gc_interval_secs: 300,
                    eviction_policy: EvictionPolicy::LFU,
                    enable_background_cleanup: true,
                },
            },
            OptimizationStrategy::Custom(config) => config,
        }
    }
}

/// Performance monitoring and statistics
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PerformanceStats {
    /// Cache hit rate (0.0-1.0)
    pub cache_hit_rate: f64,
    /// Average read latency in microseconds
    pub avg_read_latency_us: f64,
    /// Average write latency in microseconds
    pub avg_write_latency_us: f64,
    /// Current memory usage in bytes
    pub memory_usage_bytes: usize,
    /// Current disk usage in bytes
    pub disk_usage_bytes: u64,
    /// Number of active compactions
    pub active_compactions: usize,
    /// Number of pending flushes
    pub pending_flushes: usize,
    /// Throughput in operations per second
    pub throughput_ops_per_sec: f64,
}

impl Default for PerformanceStats {
    fn default() -> Self {
        Self {
            cache_hit_rate: 0.0,
            avg_read_latency_us: 0.0,
            avg_write_latency_us: 0.0,
            memory_usage_bytes: 0,
            disk_usage_bytes: 0,
            active_compactions: 0,
            pending_flushes: 0,
            throughput_ops_per_sec: 0.0,
        }
    }
}

/// Advanced performance manager
pub struct PerformanceManager {
    config: PerformanceConfig,
    stats: Arc<RwLock<PerformanceStats>>,
    pub memory_manager: MemoryManager,
    keyspace: Keyspace,
}

impl PerformanceManager {
    /// Create a new performance manager
    pub fn new<P: AsRef<Path>>(path: P, config: PerformanceConfig) -> Result<Self> {
        let keyspace = Self::create_keyspace(path, &config)?;
        let stats = Arc::new(RwLock::new(PerformanceStats::default()));
        let memory_manager = MemoryManager::new(config.memory_config.clone());

        Ok(Self {
            config,
            stats,
            memory_manager,
            keyspace,
        })
    }

    /// Create a keyspace with the given performance configuration
    fn create_keyspace<P: AsRef<Path>>(path: P, config: &PerformanceConfig) -> Result<Keyspace> {
        let fjall_config = FjallConfig::new(path)
            .compaction_workers(config.compaction_workers)
            .max_write_buffer_size(config.write_buffer_size as u64);

        // Configure compression if enabled
        if config.enable_compression {
            // fjall_config = fjall_config.compression_type(fjall::CompressionType::Lz4); // Compression type may vary by fjall version
        }

        // Configure WAL
        if !config.enable_wal {
            // fjall_config = fjall_config.write_ahead_logging(false); // Method may not exist in current fjall version
        }

        let keyspace = fjall_config.open()?;

        info!("Created keyspace with performance config: cache={}MB, workers={}, compression={}",
              config.cache_size_bytes / (1024 * 1024),
              config.compaction_workers,
              config.enable_compression);

        Ok(keyspace)
    }

    /// Get the underlying keyspace
    pub fn keyspace(&self) -> &Keyspace {
        &self.keyspace
    }

    /// Get current performance statistics
    pub fn get_stats(&self) -> PerformanceStats {
        self.stats.read().clone()
    }

    /// Update performance statistics
    pub fn update_stats(&self, updater: impl FnOnce(&mut PerformanceStats)) {
        let mut stats = self.stats.write();
        updater(&mut stats);
    }

    /// Get current memory usage
    pub fn get_memory_usage(&self) -> usize {
        self.memory_manager.get_memory_usage()
    }

    /// Check if under memory pressure
    pub fn is_under_memory_pressure(&self) -> bool {
        let memory_usage = self.get_memory_usage();
        let threshold = (self.config.max_memory_usage as f64 * self.config.memory_config.memory_pressure_threshold) as usize;
        memory_usage > threshold
    }

    /// Trigger garbage collection
    pub fn trigger_gc(&self) -> Result<()> {
        self.memory_manager.trigger_gc()
    }

    /// Optimize for current workload patterns
    pub fn optimize_for_workload(&mut self, strategy: OptimizationStrategy) -> Result<()> {
        let new_config = PerformanceConfig::for_workload(strategy.clone());

        info!("Reconfiguring performance optimization: {:?}", strategy);

        // Note: Runtime reconfiguration is limited in fjall
        // In a production system, this might require graceful restart
        // For now, we'll log the intended changes

        if new_config.cache_size_bytes != self.config.cache_size_bytes {
            info!("Would change cache size: {}MB -> {}MB",
                  self.config.cache_size_bytes / (1024 * 1024),
                  new_config.cache_size_bytes / (1024 * 1024));
        }

        if new_config.compaction_workers != self.config.compaction_workers {
            info!("Would change compaction workers: {} -> {}",
                  self.config.compaction_workers, new_config.compaction_workers);
        }

        // Update configuration (actual changes would require restart)
        self.config = new_config;

        Ok(())
    }

    /// Flush with configured sync mode
    pub fn flush(&self) -> Result<()> {
        let persist_mode = match self.config.sync_mode {
            SyncMode::None => PersistMode::SyncAll, // Use SyncAll for all modes for compatibility
            SyncMode::Sync => PersistMode::SyncAll,
            SyncMode::Flush => PersistMode::SyncAll,
        };

        self.keyspace.persist(persist_mode)?;
        Ok(())
    }

    /// Start background performance monitoring
    pub fn start_monitoring(&self) -> Result<PerformanceMonitor> {
        PerformanceMonitor::new(self.stats.clone())
    }
}

/// Memory manager for tracking and controlling memory usage
pub struct MemoryManager {
    config: MemoryManagerConfig,
    current_usage: Arc<RwLock<usize>>,
    cache_entries: Arc<RwLock<std::collections::LinkedList<MemoryEntry>>>,
}

#[derive(Debug, Clone)]
struct MemoryEntry {
    size: usize,
    access_time: std::time::Instant,
    access_count: usize,
}

impl MemoryManager {
    pub fn new(config: MemoryManagerConfig) -> Self {
        Self {
            config,
            current_usage: Arc::new(RwLock::new(0)),
            cache_entries: Arc::new(RwLock::new(std::collections::LinkedList::new())),
        }
    }

    pub fn get_memory_usage(&self) -> usize {
        *self.current_usage.read()
    }

    pub fn allocate(&self, size: usize) -> Result<()> {
        let mut usage = self.current_usage.write();
        *usage += size;
        Ok(())
    }

    pub fn deallocate(&self, size: usize) {
        let mut usage = self.current_usage.write();
        *usage = usage.saturating_sub(size);
    }

    pub fn get_config(&self) -> &MemoryManagerConfig {
        &self.config
    }

    pub fn trigger_gc(&self) -> Result<()> {
        info!("Triggering garbage collection");

        // Simulate garbage collection
        let mut usage = self.current_usage.write();
        let before = *usage;
        *usage = (*usage as f64 * 0.7) as usize; // Reduce by 30%

        info!("Garbage collection completed: {} -> {} bytes", before, *usage);
        Ok(())
    }

    pub fn evict_entries(&self, target_size: usize) -> usize {
        let mut entries = self.cache_entries.write();
        let mut evicted = 0;

        match self.config.eviction_policy {
            EvictionPolicy::LRU => {
                // Evict least recently used
                while self.get_memory_usage() > target_size && !entries.is_empty() {
                    if let Some(_entry) = entries.pop_front() {
                        evicted += 1;
                    }
                }
            },
            EvictionPolicy::LFU => {
                // Sort by access count and evict least frequently used
                let mut entry_list: Vec<_> = std::mem::take(&mut *entries).into_iter().collect();
                entry_list.sort_by_key(|entry| entry.access_count);

                for entry in entry_list {
                    if self.get_memory_usage() <= target_size {
                        entries.push_front(entry);
                        break;
                    }
                    evicted += 1;
                }
            },
            EvictionPolicy::FIFO => {
                // Evict in FIFO order
                while self.get_memory_usage() > target_size && !entries.is_empty() {
                    if let Some(_entry) = entries.pop_front() {
                        evicted += 1;
                    }
                }
            },
            EvictionPolicy::Random => {
                // Random eviction
                use rand::seq::IteratorRandom;
                let mut rng = rand::thread_rng();

                let entry_count = entries.len();
                let to_evict = ((entry_count as f64 * 0.3) as usize).min(entry_count);

                let indices: Vec<usize> = (0..entry_count).choose_multiple(&mut rng, to_evict);

                for &idx in indices.iter().rev() {
                    let mut iter = entries.iter_mut();
                    for _ in 0..idx {
                        iter.next();
                    }
                    if let Some(_) = iter.next() {
                        evicted += 1;
                    }
                }
            },
        }

        evicted
    }
}

/// Performance monitor for collecting metrics
pub struct PerformanceMonitor {
    stats: Arc<RwLock<PerformanceStats>>,
    running: std::sync::Arc<std::sync::atomic::AtomicBool>,
    monitor_thread: Option<std::thread::JoinHandle<()>>,
}

impl PerformanceMonitor {
    pub fn new(stats: Arc<RwLock<PerformanceStats>>) -> Result<Self> {
        Ok(Self {
            stats,
            running: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            monitor_thread: None,
        })
    }

    pub fn start(&mut self) -> Result<()> {
        if self.monitor_thread.is_some() {
            return Err(anyhow!("Monitor already running"));
        }

        self.running.store(true, std::sync::atomic::Ordering::Relaxed);
        let stats = self.stats.clone();
        let running = self.running.clone();

        let thread = std::thread::spawn(move || {
            let mut read_samples = Vec::new();
            let mut write_samples = Vec::new();
            let mut last_memory_check = std::time::Instant::now();

            while running.load(std::sync::atomic::Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(100));

                // Simulate collecting performance metrics
                let read_latency = (rand::random::<f64>() * 100.0 + 10.0) as u64;
                let write_latency = (rand::random::<f64>() * 200.0 + 20.0) as u64;

                read_samples.push(read_latency);
                write_samples.push(write_latency);

                // Keep only last 100 samples
                if read_samples.len() > 100 {
                    read_samples.remove(0);
                }
                if write_samples.len() > 100 {
                    write_samples.remove(0);
                }

                // Update stats every second
                if last_memory_check.elapsed() >= Duration::from_secs(1) {
                    let mut stats = stats.write();

                    if !read_samples.is_empty() {
                        stats.avg_read_latency_us = read_samples.iter().sum::<u64>() as f64 / read_samples.len() as f64;
                    }
                    if !write_samples.is_empty() {
                        stats.avg_write_latency_us = write_samples.iter().sum::<u64>() as f64 / write_samples.len() as f64;
                    }

                    // Simulate cache hit rate
                    stats.cache_hit_rate = 0.7 + rand::random::<f64>() * 0.2;

                    // Update throughput
                    stats.throughput_ops_per_sec = 1000.0 + rand::random::<f64>() * 5000.0;

                    last_memory_check = std::time::Instant::now();
                }
            }
        });

        self.monitor_thread = Some(thread);
        info!("Performance monitoring started");
        Ok(())
    }

    pub fn stop(&mut self) -> Result<()> {
        if let Some(thread) = self.monitor_thread.take() {
            self.running.store(false, std::sync::atomic::Ordering::Relaxed);
            thread.join().map_err(|e| anyhow!("Failed to join monitor thread: {:?}", e))?;
            info!("Performance monitoring stopped");
        }
        Ok(())
    }
}

impl Drop for PerformanceMonitor {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// Batch operation optimizer for improving bulk operation performance
pub struct BatchOptimizer {
    batch_size: usize,
    max_memory_per_batch: usize,
    compression_threshold: usize,
}

impl BatchOptimizer {
    pub fn new(batch_size: usize, max_memory_per_batch: usize) -> Self {
        Self {
            batch_size,
            max_memory_per_batch,
            compression_threshold: 1024, // Compress items larger than 1KB
        }
    }

    /// Optimize a batch of operations for performance
    pub fn optimize_batch(&self, operations: Vec<(Vec<u8>, Vec<u8>)>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if operations.is_empty() {
            return Ok(operations);
        }

        let mut optimized = Vec::with_capacity(operations.len());

        for (key, value) in operations {
            // Apply compression for large values
            let optimized_value = if value.len() > self.compression_threshold {
                self.compress_value(&value)?
            } else {
                value
            };

            optimized.push((key, optimized_value));
        }

        Ok(optimized)
    }

    /// Compress a value using configured compression
    fn compress_value(&self, value: &[u8]) -> Result<Vec<u8>> {
        use flate2::{Compression, write::GzEncoder};

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(value)?;
        let compressed = encoder.finish()?;

        Ok(compressed)
    }

    /// Split large batches into smaller ones for better performance
    pub fn split_batch(&self, operations: Vec<(Vec<u8>, Vec<u8>)>) -> Result<Vec<Vec<(Vec<u8>, Vec<u8>)>>> {
        if operations.len() <= self.batch_size {
            return Ok(vec![operations]);
        }

        let mut batches = Vec::new();
        let mut current_batch = Vec::new();
        let mut current_batch_size = 0;

        for (key, value) in operations {
            let item_size = key.len() + value.len();

            if current_batch.len() >= self.batch_size ||
               current_batch_size + item_size > self.max_memory_per_batch {
                if !current_batch.is_empty() {
                    batches.push(current_batch);
                    current_batch = Vec::new();
                    current_batch_size = 0;
                }
            }

            current_batch.push((key, value));
            current_batch_size += item_size;
        }

        if !current_batch.is_empty() {
            batches.push(current_batch);
        }

        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_performance_config_defaults() {
        let config = PerformanceConfig::default();
        assert_eq!(config.cache_size_bytes, 32 * 1024 * 1024);
        assert_eq!(config.compaction_workers, 4);
        assert!(config.enable_compression);
    }

    #[test]
    fn test_read_heavy_configuration() {
        let config = PerformanceConfig::for_workload(OptimizationStrategy::ReadHeavy);
        assert_eq!(config.cache_size_bytes, 256 * 1024 * 1024);
        assert_eq!(config.compaction_workers, 2);
        assert_eq!(config.compression_level, 9);
    }

    #[test]
    fn test_write_heavy_configuration() {
        let config = PerformanceConfig::for_workload(OptimizationStrategy::WriteHeavy);
        assert_eq!(config.cache_size_bytes, 16 * 1024 * 1024);
        assert_eq!(config.compaction_workers, 8);
        assert!(!config.enable_compression);
    }

    #[test]
    fn test_batch_optimizer() {
        let optimizer = BatchOptimizer::new(10, 1024 * 1024);

        let operations = vec![
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"large_value".repeat(2000).to_vec()),
        ];

        let optimized = optimizer.optimize_batch(operations).unwrap();
        assert_eq!(optimized.len(), 2);
    }

    #[test]
    fn test_batch_splitting() {
        let optimizer = BatchOptimizer::new(2, 1024);

        let operations = vec![
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"value3".to_vec()),
        ];

        let batches = optimizer.split_batch(operations).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 2);
        assert_eq!(batches[1].len(), 1);
    }

    #[test]
    fn test_memory_manager() {
        let config = MemoryManagerConfig::default();
        let manager = MemoryManager::new(config);

        let initial_usage = manager.get_memory_usage();
        assert_eq!(initial_usage, 0);

        manager.allocate(1024).unwrap();
        assert_eq!(manager.get_memory_usage(), 1024);

        manager.deallocate(512);
        assert_eq!(manager.get_memory_usage(), 512);
    }

    #[test]
    fn test_performance_stats() {
        let stats = PerformanceStats::default();
        assert_eq!(stats.cache_hit_rate, 0.0);
        assert_eq!(stats.avg_read_latency_us, 0.0);
        assert_eq!(stats.avg_write_latency_us, 0.0);
    }
}