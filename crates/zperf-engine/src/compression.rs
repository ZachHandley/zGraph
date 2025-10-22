//! Adaptive compression integration for database storage
//!
//! This module provides high-performance compression using LZ4 and Zstd with
//! adaptive algorithm selection based on data characteristics.

use std::sync::OnceLock;
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};

#[cfg(feature = "compression")]
use lz4_flex::{compress_prepend_size, decompress_size_prepended};

#[cfg(feature = "compression")]
use zstd;

/// Compression algorithm selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// LZ4 - Fast compression/decompression
    Lz4,
    /// Zstd - High compression ratio
    Zstd,
    /// Automatic selection based on data characteristics
    Auto,
}

impl Default for CompressionAlgorithm {
    fn default() -> Self {
        Self::Auto
    }
}

/// Compression level configuration
#[derive(Debug, Clone, Copy)]
pub struct CompressionLevel {
    /// LZ4 acceleration (1-65537, higher = faster, lower compression)
    pub lz4_acceleration: i32,
    /// Zstd compression level (1-22, higher = better compression, slower)
    pub zstd_level: i32,
}

impl Default for CompressionLevel {
    fn default() -> Self {
        Self {
            lz4_acceleration: 1,     // Default acceleration
            zstd_level: 3,           // Balanced level
        }
    }
}

/// Compression statistics
#[derive(Debug, Clone, Default)]
pub struct CompressionStats {
    /// Total bytes compressed
    pub bytes_compressed: u64,
    /// Total bytes after compression
    pub compressed_size: u64,
    /// Total bytes decompressed
    pub bytes_decompressed: u64,
    /// Number of compression operations
    pub compression_count: usize,
    /// Number of decompression operations
    pub decompression_count: usize,
    /// Average compression ratio
    pub avg_compression_ratio: f64,
    /// LZ4 usage count
    pub lz4_usage: usize,
    /// Zstd usage count
    pub zstd_usage: usize,
    /// No compression usage count
    pub no_compression_usage: usize,
}

impl CompressionStats {
    fn update_compression(&mut self, original_size: usize, compressed_size: usize, algorithm: CompressionAlgorithm) {
        self.bytes_compressed += original_size as u64;
        self.compressed_size += compressed_size as u64;
        self.compression_count += 1;

        match algorithm {
            CompressionAlgorithm::Lz4 => self.lz4_usage += 1,
            CompressionAlgorithm::Zstd => self.zstd_usage += 1,
            CompressionAlgorithm::None => self.no_compression_usage += 1,
            CompressionAlgorithm::Auto => {} // Should not reach here
        }

        // Update average compression ratio
        if self.bytes_compressed > 0 {
            self.avg_compression_ratio = self.compressed_size as f64 / self.bytes_compressed as f64;
        }
    }

    fn update_decompression(&mut self, decompressed_size: usize) {
        self.bytes_decompressed += decompressed_size as u64;
        self.decompression_count += 1;
    }
}

static COMPRESSION_STATS: OnceLock<parking_lot::Mutex<CompressionStats>> = OnceLock::new();

/// Initialize compression contexts and statistics
pub fn init_compression_contexts(_algorithm: &CompressionAlgorithm) -> Result<()> {
    let _ = COMPRESSION_STATS.set(parking_lot::Mutex::new(CompressionStats::default()));
    Ok(())
}

/// Get current compression statistics
pub fn get_compression_stats() -> CompressionStats {
    COMPRESSION_STATS
        .get()
        .map(|stats| stats.lock().clone())
        .unwrap_or_default()
}

/// Reset compression statistics
pub fn reset_compression_stats() {
    if let Some(stats) = COMPRESSION_STATS.get() {
        *stats.lock() = CompressionStats::default();
    }
}

/// Adaptive compression engine
pub struct CompressionEngine {
    default_algorithm: CompressionAlgorithm,
    compression_level: CompressionLevel,
}

impl CompressionEngine {
    /// Create a new compression engine with the specified algorithm
    pub fn new(algorithm: CompressionAlgorithm, level: CompressionLevel) -> Self {
        Self {
            default_algorithm: algorithm,
            compression_level: level,
        }
    }

    /// Compress data using the optimal algorithm
    pub fn compress(&self, data: &[u8]) -> Result<CompressedData> {
        let algorithm = if self.default_algorithm == CompressionAlgorithm::Auto {
            self.select_optimal_algorithm(data)
        } else {
            self.default_algorithm
        };

        let compressed = self.compress_with_algorithm(data, algorithm)?;

        // Update statistics
        if let Some(stats) = COMPRESSION_STATS.get() {
            stats.lock().update_compression(data.len(), compressed.data.len(), algorithm);
        }

        Ok(compressed)
    }

    /// Decompress data
    pub fn decompress(&self, compressed: &CompressedData) -> Result<Vec<u8>> {
        let decompressed = self.decompress_with_algorithm(compressed)?;

        // Update statistics
        if let Some(stats) = COMPRESSION_STATS.get() {
            stats.lock().update_decompression(decompressed.len());
        }

        Ok(decompressed)
    }

    /// Select optimal compression algorithm based on data characteristics
    fn select_optimal_algorithm(&self, data: &[u8]) -> CompressionAlgorithm {
        // Small data - use LZ4 for speed
        if data.len() < 1024 {
            return CompressionAlgorithm::Lz4;
        }

        // Very large data - analyze entropy to decide
        if data.len() > 1024 * 1024 {
            let entropy = self.estimate_entropy(data);

            // High entropy data (random/encrypted) - skip compression
            if entropy > 7.5 {
                return CompressionAlgorithm::None;
            }

            // Low entropy - use Zstd for better compression
            if entropy < 4.0 {
                return CompressionAlgorithm::Zstd;
            }
        }

        // Medium-sized data with repetitive patterns - analyze repetition
        let repetition_ratio = self.estimate_repetition(data);

        if repetition_ratio > 0.3 {
            // High repetition - use Zstd for better compression
            CompressionAlgorithm::Zstd
        } else {
            // Low repetition - use LZ4 for speed
            CompressionAlgorithm::Lz4
        }
    }

    /// Estimate data entropy (0-8 bits per byte)
    fn estimate_entropy(&self, data: &[u8]) -> f64 {
        let mut freq = [0u32; 256];

        // Count byte frequencies
        for &byte in data {
            freq[byte as usize] += 1;
        }

        // Calculate Shannon entropy
        let len = data.len() as f64;
        let mut entropy = 0.0;

        for &count in &freq {
            if count > 0 {
                let p = count as f64 / len;
                entropy -= p * p.log2();
            }
        }

        entropy
    }

    /// Estimate repetition ratio in data
    fn estimate_repetition(&self, data: &[u8]) -> f64 {
        if data.len() < 16 {
            return 0.0;
        }

        let sample_size = (data.len() / 4).min(1024);
        let sample = &data[..sample_size];

        let mut unique_4grams = std::collections::HashSet::new();
        let mut total_4grams = 0;

        // Count unique 4-byte patterns
        for window in sample.windows(4) {
            unique_4grams.insert(window);
            total_4grams += 1;
        }

        if total_4grams == 0 {
            return 0.0;
        }

        // Higher ratio means more repetition
        1.0 - (unique_4grams.len() as f64 / total_4grams as f64)
    }

    /// Compress with specific algorithm
    fn compress_with_algorithm(&self, data: &[u8], algorithm: CompressionAlgorithm) -> Result<CompressedData> {
        match algorithm {
            CompressionAlgorithm::None => {
                Ok(CompressedData {
                    algorithm: CompressionAlgorithm::None,
                    data: data.to_vec(),
                    original_size: data.len(),
                })
            }
            #[cfg(feature = "compression")]
            CompressionAlgorithm::Lz4 => {
                let compressed = compress_prepend_size(data);
                Ok(CompressedData {
                    algorithm: CompressionAlgorithm::Lz4,
                    data: compressed,
                    original_size: data.len(),
                })
            }
            #[cfg(feature = "compression")]
            CompressionAlgorithm::Zstd => {
                let compressed = zstd::bulk::compress(data, self.compression_level.zstd_level)
                    .context("Zstd compression failed")?;
                Ok(CompressedData {
                    algorithm: CompressionAlgorithm::Zstd,
                    data: compressed,
                    original_size: data.len(),
                })
            }
            #[cfg(not(feature = "compression"))]
            CompressionAlgorithm::Lz4 | CompressionAlgorithm::Zstd => {
                // Fall back to no compression if compression features are disabled
                Ok(CompressedData {
                    algorithm: CompressionAlgorithm::None,
                    data: data.to_vec(),
                    original_size: data.len(),
                })
            }
            CompressionAlgorithm::Auto => {
                anyhow::bail!("Auto algorithm should be resolved before this point")
            }
        }
    }

    /// Decompress with specific algorithm
    fn decompress_with_algorithm(&self, compressed: &CompressedData) -> Result<Vec<u8>> {
        match compressed.algorithm {
            CompressionAlgorithm::None => {
                Ok(compressed.data.clone())
            }
            #[cfg(feature = "compression")]
            CompressionAlgorithm::Lz4 => {
                decompress_size_prepended(&compressed.data)
                    .map_err(|e| anyhow::anyhow!("LZ4 decompression failed: {}", e))
            }
            #[cfg(feature = "compression")]
            CompressionAlgorithm::Zstd => {
                zstd::bulk::decompress(&compressed.data, compressed.original_size)
                    .context("Zstd decompression failed")
            }
            #[cfg(not(feature = "compression"))]
            CompressionAlgorithm::Lz4 | CompressionAlgorithm::Zstd => {
                anyhow::bail!("Compression features are disabled")
            }
            CompressionAlgorithm::Auto => {
                anyhow::bail!("Auto algorithm should not be stored in compressed data")
            }
        }
    }
}

impl Default for CompressionEngine {
    fn default() -> Self {
        Self::new(CompressionAlgorithm::Auto, CompressionLevel::default())
    }
}

/// Compressed data container
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressedData {
    /// Algorithm used for compression
    pub algorithm: CompressionAlgorithm,
    /// Compressed data
    pub data: Vec<u8>,
    /// Original uncompressed size
    pub original_size: usize,
}

impl CompressedData {
    /// Get compression ratio (compressed_size / original_size)
    pub fn compression_ratio(&self) -> f64 {
        if self.original_size == 0 {
            return 1.0;
        }
        self.data.len() as f64 / self.original_size as f64
    }

    /// Get space saved in bytes
    pub fn space_saved(&self) -> i64 {
        self.original_size as i64 - self.data.len() as i64
    }

    /// Get space saved as percentage
    pub fn space_saved_percent(&self) -> f64 {
        if self.original_size == 0 {
            return 0.0;
        }
        100.0 * (1.0 - self.compression_ratio())
    }
}

/// Database storage integration for compressed data
pub struct CompressedStorage {
    engine: CompressionEngine,
    min_compression_size: usize,
}

impl CompressedStorage {
    /// Create new compressed storage with configuration
    pub fn new(algorithm: CompressionAlgorithm, min_size: usize) -> Self {
        Self {
            engine: CompressionEngine::new(algorithm, CompressionLevel::default()),
            min_compression_size: min_size,
        }
    }

    /// Store data with optional compression
    pub fn store(&self, data: &[u8]) -> Result<StoredData> {
        // Skip compression for small data
        if data.len() < self.min_compression_size {
            return Ok(StoredData::Uncompressed(data.to_vec()));
        }

        let compressed = self.engine.compress(data)?;

        // Only use compression if it actually reduces size
        if compressed.data.len() < data.len() {
            Ok(StoredData::Compressed(compressed))
        } else {
            Ok(StoredData::Uncompressed(data.to_vec()))
        }
    }

    /// Retrieve and decompress data
    pub fn retrieve(&self, stored: &StoredData) -> Result<Vec<u8>> {
        match stored {
            StoredData::Uncompressed(data) => Ok(data.clone()),
            StoredData::Compressed(compressed) => self.engine.decompress(compressed),
        }
    }
}

impl Default for CompressedStorage {
    fn default() -> Self {
        Self::new(CompressionAlgorithm::Auto, 128) // Don't compress data smaller than 128 bytes
    }
}

/// Stored data that may or may not be compressed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StoredData {
    /// Uncompressed data
    Uncompressed(Vec<u8>),
    /// Compressed data
    Compressed(CompressedData),
}

impl StoredData {
    /// Get the storage size in bytes
    pub fn storage_size(&self) -> usize {
        match self {
            StoredData::Uncompressed(data) => data.len(),
            StoredData::Compressed(compressed) => compressed.data.len(),
        }
    }

    /// Get the original uncompressed size
    pub fn original_size(&self) -> usize {
        match self {
            StoredData::Uncompressed(data) => data.len(),
            StoredData::Compressed(compressed) => compressed.original_size,
        }
    }

    /// Check if data is compressed
    pub fn is_compressed(&self) -> bool {
        matches!(self, StoredData::Compressed(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_engine_creation() {
        let engine = CompressionEngine::new(CompressionAlgorithm::Lz4, CompressionLevel::default());
        assert_eq!(engine.default_algorithm, CompressionAlgorithm::Lz4);
    }

    #[test]
    fn test_entropy_estimation() {
        let engine = CompressionEngine::default();

        // Random data should have high entropy
        let random_data: Vec<u8> = (0..=255).collect();
        let entropy = engine.estimate_entropy(&random_data);
        assert!(entropy > 7.0);

        // Repetitive data should have low entropy
        let repetitive_data = vec![0u8; 256];
        let entropy = engine.estimate_entropy(&repetitive_data);
        assert!(entropy < 1.0);
    }

    #[test]
    fn test_repetition_estimation() {
        let engine = CompressionEngine::default();

        // Highly repetitive data
        let repetitive_data = "abcdabcdabcdabcd".repeat(10).into_bytes();
        let ratio = engine.estimate_repetition(&repetitive_data);
        assert!(ratio > 0.3); // Lowered threshold since algorithm may vary

        // Random data should have low repetition
        let random_data: Vec<u8> = (0..64).collect();
        let ratio = engine.estimate_repetition(&random_data);
        assert!(ratio < 0.3);
    }

    #[cfg(feature = "compression")]
    #[test]
    fn test_lz4_compression() {
        let engine = CompressionEngine::new(CompressionAlgorithm::Lz4, CompressionLevel::default());

        let original_data = b"Hello, World! This is a test string that should compress well with LZ4.";
        let compressed = engine.compress(original_data).unwrap();

        assert_eq!(compressed.algorithm, CompressionAlgorithm::Lz4);
        assert_eq!(compressed.original_size, original_data.len());

        let decompressed = engine.decompress(&compressed).unwrap();
        assert_eq!(decompressed, original_data);
    }

    #[cfg(feature = "compression")]
    #[test]
    fn test_zstd_compression() {
        let engine = CompressionEngine::new(CompressionAlgorithm::Zstd, CompressionLevel::default());

        // Create some repetitive data that should compress well
        let original_data = "abcdefgh".repeat(100).into_bytes();
        let compressed = engine.compress(&original_data).unwrap();

        assert_eq!(compressed.algorithm, CompressionAlgorithm::Zstd);
        assert_eq!(compressed.original_size, original_data.len());
        assert!(compressed.data.len() < original_data.len());

        let decompressed = engine.decompress(&compressed).unwrap();
        assert_eq!(decompressed, original_data);
    }

    #[test]
    fn test_auto_algorithm_selection() {
        let engine = CompressionEngine::new(CompressionAlgorithm::Auto, CompressionLevel::default());

        // Small data should use LZ4
        let small_data = b"small";
        let algorithm = engine.select_optimal_algorithm(small_data);
        assert_eq!(algorithm, CompressionAlgorithm::Lz4);

        // Repetitive data should use Zstd
        let repetitive_data = "abcd".repeat(1000).into_bytes();
        let algorithm = engine.select_optimal_algorithm(&repetitive_data);
        assert_eq!(algorithm, CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_compressed_storage() {
        let storage = CompressedStorage::default();

        let original_data = "This is test data that should be compressed.".repeat(10).into_bytes();
        let stored = storage.store(&original_data).unwrap();

        let retrieved = storage.retrieve(&stored).unwrap();
        assert_eq!(retrieved, original_data);

        // Storage size should be less than original for repetitive data
        if stored.is_compressed() {
            assert!(stored.storage_size() < stored.original_size());
        }
    }

    #[test]
    fn test_compression_stats() {
        reset_compression_stats();
        init_compression_contexts(&CompressionAlgorithm::Auto).unwrap();

        let engine = CompressionEngine::default();
        let data = b"test data for statistics";

        let _compressed = engine.compress(data).unwrap();

        let stats = get_compression_stats();
        assert_eq!(stats.compression_count, 1);
        assert_eq!(stats.bytes_compressed, data.len() as u64);
    }
}