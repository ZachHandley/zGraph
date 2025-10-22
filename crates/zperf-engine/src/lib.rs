//! High-performance optimization components for ZRUSTDB
//!
//! This crate provides comprehensive performance optimizations including:
//! - Memory allocator optimization with mimalloc (5.3x faster than system malloc)
//! - High-performance hashing with AHash (3-4x faster than standard HashMap)
//! - Adaptive compression with LZ4 and Zstd (30-70% size reduction)

#![warn(missing_docs)]

// Global allocator configuration
#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub mod hashing;
pub mod compression;
pub mod memory_profiler;
pub mod simd;
pub mod allocator;
pub mod memory_pool;
pub mod lockfree;
pub mod monitoring;
pub mod serialization;

// Re-exports for convenience
pub use hashing::*;
pub use compression::*;
pub use memory_profiler::*;
pub use simd::*;
pub use memory_pool::*;
pub use lockfree::*;
pub use serialization::*;

// Selective re-exports to avoid conflicts
pub use allocator::{AllocatorStats, DatabaseAllocator, TrackedAllocation, record_allocation, record_deallocation, reset_allocator_stats};
pub use monitoring::{PerformanceMonitor, MonitoringConfig, OperationType, SIMDOperationType, PerformanceReport, PerformanceKPIs, init_performance_monitoring, get_global_monitor, record_operation, record_fallible_operation};

/// Performance optimization configuration
#[derive(Debug, Clone)]
pub struct PerfConfig {
    /// Preferred compression algorithm
    pub compression: CompressionAlgorithm,
    /// Hash algorithm selection
    pub hash_algorithm: HashAlgorithm,
}

impl Default for PerfConfig {
    fn default() -> Self {
        Self {
            compression: CompressionAlgorithm::Auto,
            hash_algorithm: HashAlgorithm::AHash,
        }
    }
}

/// Initialize performance optimizations with the given configuration
pub fn init_performance_optimizations(config: &PerfConfig) -> anyhow::Result<()> {
    // Initialize compression contexts
    compression::init_compression_contexts(&config.compression)?;
    Ok(())
}

/// Get current performance statistics
pub fn get_performance_stats() -> PerformanceStats {
    PerformanceStats {
        compression_stats: compression::get_compression_stats(),
    }
}

/// Performance statistics
#[derive(Debug, Clone)]
pub struct PerformanceStats {
    /// Compression performance metrics
    pub compression_stats: CompressionStats,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PerfConfig::default();
        assert!(matches!(config.compression, CompressionAlgorithm::Auto));
        assert!(matches!(config.hash_algorithm, HashAlgorithm::AHash));
    }

    #[test]
    fn test_performance_initialization() {
        let config = PerfConfig::default();
        let result = init_performance_optimizations(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_performance_stats() {
        let _stats = get_performance_stats();
        // Stats should be accessible
    }
}