//! Performance monitoring and metrics collection
//!
//! This module provides comprehensive performance monitoring for database operations,
//! including allocator statistics, SIMD usage, and operation profiling.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use anyhow::Result;
// Import types from other modules within the crate
// Note: These types are defined in other modules and should be available when needed

/// Performance monitoring configuration
#[derive(Debug, Clone)]
pub struct MonitoringConfig {
    /// Enable detailed operation tracking
    pub enable_operation_tracking: bool,
    /// Enable memory allocation tracking
    pub enable_memory_tracking: bool,
    /// Enable SIMD usage tracking
    pub enable_simd_tracking: bool,
    /// Metrics collection interval
    pub collection_interval: Duration,
    /// Maximum number of recent operations to track
    pub max_tracked_operations: usize,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enable_operation_tracking: true,
            enable_memory_tracking: true,
            enable_simd_tracking: true,
            collection_interval: Duration::from_secs(1),
            max_tracked_operations: 1000,
        }
    }
}

/// Performance metrics collector
#[derive(Debug)]
pub struct PerformanceMonitor {
    config: MonitoringConfig,
    operation_stats: Arc<RwLock<OperationStats>>,
    memory_stats: Arc<RwLock<MemoryMetrics>>,
    simd_stats: Arc<RwLock<SIMDMetrics>>,
    system_stats: Arc<RwLock<SystemMetrics>>,
    start_time: Instant,
}

impl PerformanceMonitor {
    /// Create a new performance monitor
    pub fn new(config: MonitoringConfig) -> Self {
        Self {
            config,
            operation_stats: Arc::new(RwLock::new(OperationStats::new())),
            memory_stats: Arc::new(RwLock::new(MemoryMetrics::new())),
            simd_stats: Arc::new(RwLock::new(SIMDMetrics::new())),
            system_stats: Arc::new(RwLock::new(SystemMetrics::new())),
            start_time: Instant::now(),
        }
    }

    /// Record a database operation
    pub fn record_operation(&self, operation: OperationType, duration: Duration, success: bool) {
        if !self.config.enable_operation_tracking {
            return;
        }

        let mut stats = self.operation_stats.write();
        stats.record_operation(operation, duration, success);
    }

    /// Record memory usage
    pub fn record_memory_usage(&self, allocated: usize, deallocated: usize) {
        if !self.config.enable_memory_tracking {
            return;
        }

        let mut stats = self.memory_stats.write();
        stats.record_allocation(allocated, deallocated);
    }

    /// Record SIMD operation usage
    pub fn record_simd_usage(&self, operation: SIMDOperationType, elements: usize, duration: Duration) {
        if !self.config.enable_simd_tracking {
            return;
        }

        let mut stats = self.simd_stats.write();
        stats.record_operation(operation, elements, duration);
    }

    /// Update system metrics
    pub fn update_system_metrics(&self) {
        let mut stats = self.system_stats.write();
        stats.update();
    }

    /// Get comprehensive performance report
    pub fn get_performance_report(&self) -> PerformanceReport {
        let operation_stats = self.operation_stats.read().clone();
        let memory_stats = self.memory_stats.read().clone();
        let simd_stats = self.simd_stats.read().clone();
        let system_stats = self.system_stats.read().clone();

        PerformanceReport {
            uptime: self.start_time.elapsed(),
            operations: operation_stats,
            memory: memory_stats,
            simd: simd_stats,
            system: system_stats,
        }
    }

    /// Reset all statistics
    pub fn reset_stats(&self) {
        self.operation_stats.write().reset();
        self.memory_stats.write().reset();
        self.simd_stats.write().reset();
        self.system_stats.write().reset();
    }

    /// Get operation statistics only
    pub fn get_operation_stats(&self) -> OperationStats {
        self.operation_stats.read().clone()
    }

    /// Get memory statistics only
    pub fn get_memory_stats(&self) -> MemoryMetrics {
        self.memory_stats.read().clone()
    }
}

/// Types of database operations to track
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationType {
    /// SQL SELECT query
    Select,
    /// SQL INSERT operation
    Insert,
    /// SQL UPDATE operation
    Update,
    /// SQL DELETE operation
    Delete,
    /// Vector similarity search
    VectorSearch,
    /// Index creation
    IndexCreation,
    /// Index lookup
    IndexLookup,
    /// Compression operation
    Compression,
    /// Decompression operation
    Decompression,
    /// Serialization
    Serialization,
    /// Deserialization
    Deserialization,
}

/// Types of SIMD operations to track
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SIMDOperationType {
    /// Vector distance calculation
    VectorDistance,
    /// Aggregation (sum, min, max)
    Aggregation,
    /// Filtering operations
    Filtering,
    /// Sorting operations
    Sorting,
}

/// Database operation statistics
#[derive(Debug, Clone)]
pub struct OperationStats {
    /// Total operations by type
    pub operation_counts: std::collections::HashMap<OperationType, u64>,
    /// Total duration by operation type
    pub operation_durations: std::collections::HashMap<OperationType, Duration>,
    /// Success rates by operation type
    pub success_counts: std::collections::HashMap<OperationType, u64>,
    /// Failure counts by operation type
    pub failure_counts: std::collections::HashMap<OperationType, u64>,
    /// Recent operation latencies (for percentile calculation)
    pub recent_latencies: std::collections::VecDeque<(OperationType, Duration)>,
    /// Peak operations per second
    pub peak_ops_per_second: f64,
    /// Current operations per second
    pub current_ops_per_second: f64,
    /// Last update time
    pub last_update: Instant,
}

impl OperationStats {
    fn new() -> Self {
        Self {
            operation_counts: std::collections::HashMap::new(),
            operation_durations: std::collections::HashMap::new(),
            success_counts: std::collections::HashMap::new(),
            failure_counts: std::collections::HashMap::new(),
            recent_latencies: std::collections::VecDeque::new(),
            peak_ops_per_second: 0.0,
            current_ops_per_second: 0.0,
            last_update: Instant::now(),
        }
    }

    fn record_operation(&mut self, operation: OperationType, duration: Duration, success: bool) {
        *self.operation_counts.entry(operation).or_insert(0) += 1;
        *self.operation_durations.entry(operation).or_insert(Duration::ZERO) += duration;

        if success {
            *self.success_counts.entry(operation).or_insert(0) += 1;
        } else {
            *self.failure_counts.entry(operation).or_insert(0) += 1;
        }

        // Track recent latencies for percentile calculations
        self.recent_latencies.push_back((operation, duration));
        if self.recent_latencies.len() > 1000 {
            self.recent_latencies.pop_front();
        }

        // Update operations per second
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update);
        if elapsed >= Duration::from_secs(1) {
            let ops_in_window = self.recent_latencies.len() as f64;
            self.current_ops_per_second = ops_in_window / elapsed.as_secs_f64();
            self.peak_ops_per_second = self.peak_ops_per_second.max(self.current_ops_per_second);
            self.last_update = now;
        }
    }

    fn reset(&mut self) {
        self.operation_counts.clear();
        self.operation_durations.clear();
        self.success_counts.clear();
        self.failure_counts.clear();
        self.recent_latencies.clear();
        self.peak_ops_per_second = 0.0;
        self.current_ops_per_second = 0.0;
        self.last_update = Instant::now();
    }

    /// Get average latency for an operation type
    pub fn average_latency(&self, operation: OperationType) -> Option<Duration> {
        let count = self.operation_counts.get(&operation)?;
        let total_duration = self.operation_durations.get(&operation)?;

        if *count > 0 {
            Some(*total_duration / (*count as u32))
        } else {
            None
        }
    }

    /// Get success rate for an operation type
    pub fn success_rate(&self, operation: OperationType) -> f64 {
        let successes = self.success_counts.get(&operation).unwrap_or(&0);
        let failures = self.failure_counts.get(&operation).unwrap_or(&0);
        let total = successes + failures;

        if total > 0 {
            *successes as f64 / total as f64
        } else {
            0.0
        }
    }

    /// Get 95th percentile latency for an operation type
    pub fn p95_latency(&self, operation: OperationType) -> Option<Duration> {
        let latencies: Vec<_> = self.recent_latencies
            .iter()
            .filter(|(op, _)| *op == operation)
            .map(|(_, duration)| *duration)
            .collect();

        if latencies.is_empty() {
            return None;
        }

        let mut sorted_latencies = latencies;
        sorted_latencies.sort();

        let index = (sorted_latencies.len() as f64 * 0.95) as usize;
        sorted_latencies.get(index.min(sorted_latencies.len() - 1)).copied()
    }
}

/// Memory usage metrics
#[derive(Debug)]
pub struct MemoryMetrics {
    /// Current bytes allocated
    pub current_allocated: AtomicU64,
    /// Peak bytes allocated
    pub peak_allocated: AtomicU64,
    /// Total allocations
    pub total_allocations: AtomicU64,
    /// Total deallocations
    pub total_deallocations: AtomicU64,
    /// Allocation rate (allocs per second)
    pub allocation_rate: f64,
    /// Average allocation size
    pub average_allocation_size: f64,
    /// Memory fragmentation estimate
    pub fragmentation_ratio: f64,
}

impl Clone for MemoryMetrics {
    fn clone(&self) -> Self {
        Self {
            current_allocated: AtomicU64::new(self.current_allocated.load(Ordering::Relaxed)),
            peak_allocated: AtomicU64::new(self.peak_allocated.load(Ordering::Relaxed)),
            total_allocations: AtomicU64::new(self.total_allocations.load(Ordering::Relaxed)),
            total_deallocations: AtomicU64::new(self.total_deallocations.load(Ordering::Relaxed)),
            allocation_rate: self.allocation_rate,
            average_allocation_size: self.average_allocation_size,
            fragmentation_ratio: self.fragmentation_ratio,
        }
    }
}

impl MemoryMetrics {
    fn new() -> Self {
        Self {
            current_allocated: AtomicU64::new(0),
            peak_allocated: AtomicU64::new(0),
            total_allocations: AtomicU64::new(0),
            total_deallocations: AtomicU64::new(0),
            allocation_rate: 0.0,
            average_allocation_size: 0.0,
            fragmentation_ratio: 0.0,
        }
    }

    fn record_allocation(&mut self, allocated: usize, deallocated: usize) {
        let current = self.current_allocated.load(Ordering::Relaxed);
        let new_current = current + allocated as u64 - deallocated as u64;
        self.current_allocated.store(new_current, Ordering::Relaxed);

        // Update peak
        let current_peak = self.peak_allocated.load(Ordering::Relaxed);
        if new_current > current_peak {
            self.peak_allocated.store(new_current, Ordering::Relaxed);
        }

        if allocated > 0 {
            self.total_allocations.fetch_add(1, Ordering::Relaxed);
        }
        if deallocated > 0 {
            self.total_deallocations.fetch_add(1, Ordering::Relaxed);
        }

        // Update average allocation size
        let total_allocs = self.total_allocations.load(Ordering::Relaxed);
        if total_allocs > 0 {
            self.average_allocation_size = current as f64 / total_allocs as f64;
        }
    }

    fn reset(&mut self) {
        self.current_allocated.store(0, Ordering::Relaxed);
        self.peak_allocated.store(0, Ordering::Relaxed);
        self.total_allocations.store(0, Ordering::Relaxed);
        self.total_deallocations.store(0, Ordering::Relaxed);
        self.allocation_rate = 0.0;
        self.average_allocation_size = 0.0;
        self.fragmentation_ratio = 0.0;
    }
}

/// SIMD operation metrics
#[derive(Debug, Clone)]
pub struct SIMDMetrics {
    /// SIMD operations by type
    pub operation_counts: std::collections::HashMap<SIMDOperationType, u64>,
    /// Total elements processed by SIMD
    pub elements_processed: std::collections::HashMap<SIMDOperationType, u64>,
    /// SIMD operation durations
    pub operation_durations: std::collections::HashMap<SIMDOperationType, Duration>,
    /// SIMD vs scalar performance ratios
    pub performance_ratios: std::collections::HashMap<SIMDOperationType, f64>,
}

impl SIMDMetrics {
    fn new() -> Self {
        Self {
            operation_counts: std::collections::HashMap::new(),
            elements_processed: std::collections::HashMap::new(),
            operation_durations: std::collections::HashMap::new(),
            performance_ratios: std::collections::HashMap::new(),
        }
    }

    fn record_operation(&mut self, operation: SIMDOperationType, elements: usize, duration: Duration) {
        *self.operation_counts.entry(operation).or_insert(0) += 1;
        *self.elements_processed.entry(operation).or_insert(0) += elements as u64;
        *self.operation_durations.entry(operation).or_insert(Duration::ZERO) += duration;

        // Calculate throughput (elements per second)
        if !duration.is_zero() {
            let throughput = elements as f64 / duration.as_secs_f64();
            self.performance_ratios.insert(operation, throughput);
        }
    }

    fn reset(&mut self) {
        self.operation_counts.clear();
        self.elements_processed.clear();
        self.operation_durations.clear();
        self.performance_ratios.clear();
    }

    /// Get average elements per operation
    pub fn average_elements_per_operation(&self, operation: SIMDOperationType) -> f64 {
        let count = self.operation_counts.get(&operation).unwrap_or(&0);
        let elements = self.elements_processed.get(&operation).unwrap_or(&0);

        if *count > 0 {
            *elements as f64 / *count as f64
        } else {
            0.0
        }
    }

    /// Get throughput (elements per second)
    pub fn throughput(&self, operation: SIMDOperationType) -> f64 {
        self.performance_ratios.get(&operation).copied().unwrap_or(0.0)
    }
}

/// System-level metrics
#[derive(Debug, Clone)]
pub struct SystemMetrics {
    /// CPU usage percentage
    pub cpu_usage: f64,
    /// Memory usage percentage
    pub memory_usage: f64,
    /// Number of active threads
    pub thread_count: usize,
    /// System load average
    pub load_average: f64,
    /// Last update time
    pub last_update: Instant,
}

impl SystemMetrics {
    fn new() -> Self {
        Self {
            cpu_usage: 0.0,
            memory_usage: 0.0,
            thread_count: 0,
            load_average: 0.0,
            last_update: Instant::now(),
        }
    }

    fn update(&mut self) {
        // In a real implementation, these would query the actual system
        // For now, we'll use placeholder values
        self.last_update = Instant::now();

        // These would be implemented using system-specific APIs
        // self.cpu_usage = get_cpu_usage();
        // self.memory_usage = get_memory_usage();
        // self.thread_count = get_thread_count();
        // self.load_average = get_load_average();
    }

    fn reset(&mut self) {
        self.cpu_usage = 0.0;
        self.memory_usage = 0.0;
        self.thread_count = 0;
        self.load_average = 0.0;
        self.last_update = Instant::now();
    }
}

/// Comprehensive performance report
#[derive(Debug, Clone)]
pub struct PerformanceReport {
    /// System uptime
    pub uptime: Duration,
    /// Operation statistics
    pub operations: OperationStats,
    /// Memory metrics
    pub memory: MemoryMetrics,
    /// SIMD metrics
    pub simd: SIMDMetrics,
    /// System metrics
    pub system: SystemMetrics,
}

impl PerformanceReport {
    /// Generate a human-readable summary
    pub fn summary(&self) -> String {
        let mut summary = String::new();

        summary.push_str(&format!("ZRUSTDB Performance Report\n"));
        summary.push_str(&format!("========================\n"));
        summary.push_str(&format!("Uptime: {:?}\n", self.uptime));
        summary.push_str(&format!("Current OPS: {:.2}\n", self.operations.current_ops_per_second));
        summary.push_str(&format!("Peak OPS: {:.2}\n", self.operations.peak_ops_per_second));

        summary.push_str(&format!("\nMemory Usage:\n"));
        summary.push_str(&format!("  Current: {} bytes\n", self.memory.current_allocated.load(Ordering::Relaxed)));
        summary.push_str(&format!("  Peak: {} bytes\n", self.memory.peak_allocated.load(Ordering::Relaxed)));
        summary.push_str(&format!("  Avg Allocation: {:.2} bytes\n", self.memory.average_allocation_size));

        summary.push_str(&format!("\nOperation Counts:\n"));
        for (op_type, count) in &self.operations.operation_counts {
            summary.push_str(&format!("  {:?}: {}\n", op_type, count));
        }

        summary.push_str(&format!("\nSIMD Operations:\n"));
        for (simd_type, count) in &self.simd.operation_counts {
            let throughput = self.simd.throughput(*simd_type);
            summary.push_str(&format!("  {:?}: {} ops ({:.2} elements/sec)\n", simd_type, count, throughput));
        }

        summary
    }

    /// Get key performance indicators
    pub fn get_kpis(&self) -> PerformanceKPIs {
        PerformanceKPIs {
            operations_per_second: self.operations.current_ops_per_second,
            average_latency_ms: self.operations.average_latency(OperationType::Select)
                .unwrap_or(Duration::ZERO)
                .as_millis() as f64,
            memory_efficiency: self.calculate_memory_efficiency(),
            simd_utilization: self.calculate_simd_utilization(),
            error_rate: self.calculate_error_rate(),
        }
    }

    fn calculate_memory_efficiency(&self) -> f64 {
        let current = self.memory.current_allocated.load(Ordering::Relaxed) as f64;
        let peak = self.memory.peak_allocated.load(Ordering::Relaxed) as f64;

        if peak > 0.0 {
            current / peak
        } else {
            1.0
        }
    }

    fn calculate_simd_utilization(&self) -> f64 {
        let total_simd_ops: u64 = self.simd.operation_counts.values().sum();
        let total_ops: u64 = self.operations.operation_counts.values().sum();

        if total_ops > 0 {
            total_simd_ops as f64 / total_ops as f64
        } else {
            0.0
        }
    }

    fn calculate_error_rate(&self) -> f64 {
        let total_successes: u64 = self.operations.success_counts.values().sum();
        let total_failures: u64 = self.operations.failure_counts.values().sum();
        let total = total_successes + total_failures;

        if total > 0 {
            total_failures as f64 / total as f64
        } else {
            0.0
        }
    }
}

/// Key Performance Indicators
#[derive(Debug, Clone)]
pub struct PerformanceKPIs {
    /// Operations per second
    pub operations_per_second: f64,
    /// Average latency in milliseconds
    pub average_latency_ms: f64,
    /// Memory efficiency (0.0 - 1.0)
    pub memory_efficiency: f64,
    /// SIMD utilization (0.0 - 1.0)
    pub simd_utilization: f64,
    /// Error rate (0.0 - 1.0)
    pub error_rate: f64,
}

/// Global performance monitor instance
static GLOBAL_MONITOR: parking_lot::RwLock<Option<Arc<PerformanceMonitor>>> = parking_lot::RwLock::new(None);

/// Initialize global performance monitoring
pub fn init_performance_monitoring(config: MonitoringConfig) -> Result<()> {
    let monitor = Arc::new(PerformanceMonitor::new(config));
    *GLOBAL_MONITOR.write() = Some(monitor);
    Ok(())
}

/// Get the global performance monitor
pub fn get_global_monitor() -> Option<Arc<PerformanceMonitor>> {
    GLOBAL_MONITOR.read().clone()
}

/// Initialize allocator monitoring (stub implementation)
pub fn init_allocator_monitoring() -> Result<()> {
    // This would initialize the allocator tracking system
    Ok(())
}

/// Get allocator statistics (stub implementation)
pub fn get_allocator_stats() -> crate::allocator::AllocatorStats {
    // This would return actual allocator statistics
    crate::allocator::AllocatorStats::default()
}

/// Record a timed operation for monitoring
pub fn record_operation<F, R>(operation: OperationType, f: F) -> R
where
    F: FnOnce() -> R,
{
    let start = Instant::now();
    let result = f();
    let duration = start.elapsed();

    if let Some(monitor) = get_global_monitor() {
        monitor.record_operation(operation, duration, true);
    }

    result
}

/// Record a timed operation that can fail
pub fn record_fallible_operation<F, R, E>(operation: OperationType, f: F) -> Result<R, E>
where
    F: FnOnce() -> Result<R, E>,
{
    let start = Instant::now();
    let result = f();
    let duration = start.elapsed();

    if let Some(monitor) = get_global_monitor() {
        monitor.record_operation(operation, duration, result.is_ok());
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_performance_monitor() {
        let config = MonitoringConfig::default();
        let monitor = PerformanceMonitor::new(config);

        // Record some operations
        monitor.record_operation(OperationType::Select, Duration::from_millis(10), true);
        monitor.record_operation(OperationType::Insert, Duration::from_millis(5), true);
        monitor.record_operation(OperationType::Select, Duration::from_millis(15), false);

        let stats = monitor.get_operation_stats();

        assert_eq!(stats.operation_counts[&OperationType::Select], 2);
        assert_eq!(stats.operation_counts[&OperationType::Insert], 1);
        assert_eq!(stats.success_counts[&OperationType::Select], 1);
        assert_eq!(stats.failure_counts[&OperationType::Select], 1);

        let avg_latency = stats.average_latency(OperationType::Select).unwrap();
        assert_eq!(avg_latency, Duration::from_millis(12)); // (10 + 15) / 2 = 12.5ms, truncated to 12ms

        let success_rate = stats.success_rate(OperationType::Select);
        assert_eq!(success_rate, 0.5); // 1 success out of 2 total
    }

    #[test]
    fn test_memory_metrics() {
        let mut metrics = MemoryMetrics::new();

        metrics.record_allocation(1024, 0);
        metrics.record_allocation(512, 0);
        metrics.record_allocation(0, 256);

        assert_eq!(metrics.current_allocated.load(Ordering::Relaxed), 1024 + 512 - 256);
        assert_eq!(metrics.total_allocations.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.total_deallocations.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_simd_metrics() {
        let mut metrics = SIMDMetrics::new();

        metrics.record_operation(SIMDOperationType::VectorDistance, 1000, Duration::from_millis(1));
        metrics.record_operation(SIMDOperationType::VectorDistance, 2000, Duration::from_millis(2));

        assert_eq!(metrics.operation_counts[&SIMDOperationType::VectorDistance], 2);
        assert_eq!(metrics.elements_processed[&SIMDOperationType::VectorDistance], 3000);

        let avg_elements = metrics.average_elements_per_operation(SIMDOperationType::VectorDistance);
        assert_eq!(avg_elements, 1500.0); // 3000 / 2 = 1500
    }

    #[test]
    fn test_operation_percentiles() {
        let mut stats = OperationStats::new();

        // Record latencies: 1, 2, 3, 4, 5 ms
        for i in 1..=5 {
            stats.record_operation(
                OperationType::Select,
                Duration::from_millis(i),
                true,
            );
        }

        let p95 = stats.p95_latency(OperationType::Select).unwrap();
        // 95th percentile of [1,2,3,4,5] should be 5ms (or close to it)
        assert!(p95 >= Duration::from_millis(4));
    }

    #[test]
    fn test_performance_report() {
        let config = MonitoringConfig::default();
        let monitor = PerformanceMonitor::new(config);

        monitor.record_operation(OperationType::Select, Duration::from_millis(10), true);
        monitor.record_memory_usage(1024, 0);

        let report = monitor.get_performance_report();
        let kpis = report.get_kpis();

        assert!(kpis.operations_per_second >= 0.0);
        assert!(kpis.memory_efficiency >= 0.0 && kpis.memory_efficiency <= 1.0);
        assert!(kpis.error_rate >= 0.0 && kpis.error_rate <= 1.0);

        let summary = report.summary();
        assert!(summary.contains("ZRUSTDB Performance Report"));
    }

    #[test]
    fn test_record_operation_macro() {
        init_performance_monitoring(MonitoringConfig::default()).unwrap();

        let result = record_operation(OperationType::Select, || {
            std::thread::sleep(Duration::from_millis(1));
            42
        });

        assert_eq!(result, 42);

        if let Some(monitor) = get_global_monitor() {
            let stats = monitor.get_operation_stats();
            assert!(stats.operation_counts.contains_key(&OperationType::Select));
        }
    }

    #[test]
    fn test_record_fallible_operation_macro() {
        init_performance_monitoring(MonitoringConfig::default()).unwrap();

        let success_result = record_fallible_operation(OperationType::Insert, || -> Result<i32, &str> {
            Ok(100)
        });
        assert!(success_result.is_ok());

        let error_result = record_fallible_operation(OperationType::Insert, || -> Result<i32, &str> {
            Err("test error")
        });
        assert!(error_result.is_err());

        if let Some(monitor) = get_global_monitor() {
            let stats = monitor.get_operation_stats();
            assert_eq!(stats.success_counts[&OperationType::Insert], 1);
            assert_eq!(stats.failure_counts[&OperationType::Insert], 1);
        }
    }
}