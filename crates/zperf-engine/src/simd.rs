//! SIMD optimization components for database operations
//!
//! This module provides SIMD-accelerated implementations of common database operations
//! including sorting, filtering, aggregations, and vectorized computations.

use std::sync::OnceLock;
// anyhow::Result not needed in this module

#[cfg(feature = "simd")]
use wide::*;

/// Extension trait to add missing methods to wide vector types
#[cfg(feature = "simd")]
trait WideExt {
    /// Create vector from unaligned slice
    fn from_slice_unaligned(slice: &[Self::Element]) -> Self;

    /// Reduce to minimum value
    fn reduce_min(self) -> Self::Element;

    /// Reduce to maximum value
    fn reduce_max(self) -> Self::Element;

    /// Associated type for element
    type Element;
}

#[cfg(feature = "simd")]
impl WideExt for wide::i32x4 {
    type Element = i32;

    fn from_slice_unaligned(slice: &[i32]) -> Self {
        if slice.len() >= 4 {
            Self::new([slice[0], slice[1], slice[2], slice[3]])
        } else {
            let mut padded = [0i32; 4];
            padded[..slice.len()].copy_from_slice(slice);
            Self::new(padded)
        }
    }

    fn reduce_min(self) -> i32 {
        let arr = self.as_array_ref();
        arr.iter().copied().min().unwrap_or(0)
    }

    fn reduce_max(self) -> i32 {
        let arr = self.as_array_ref();
        arr.iter().copied().max().unwrap_or(0)
    }
}

#[cfg(feature = "simd")]
impl WideExt for wide::i32x8 {
    type Element = i32;

    fn from_slice_unaligned(slice: &[i32]) -> Self {
        if slice.len() >= 8 {
            Self::new([slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7]])
        } else {
            let mut padded = [0i32; 8];
            padded[..slice.len()].copy_from_slice(slice);
            Self::new(padded)
        }
    }

    fn reduce_min(self) -> i32 {
        let arr = self.as_array_ref();
        arr.iter().copied().min().unwrap_or(0)
    }

    fn reduce_max(self) -> i32 {
        let arr = self.as_array_ref();
        arr.iter().copied().max().unwrap_or(0)
    }
}

#[cfg(feature = "simd")]
impl WideExt for wide::f32x4 {
    type Element = f32;

    fn from_slice_unaligned(slice: &[f32]) -> Self {
        if slice.len() >= 4 {
            Self::new([slice[0], slice[1], slice[2], slice[3]])
        } else {
            let mut padded = [0.0f32; 4];
            padded[..slice.len()].copy_from_slice(slice);
            Self::new(padded)
        }
    }

    fn reduce_min(self) -> f32 {
        let arr = self.as_array_ref();
        arr.iter().copied().fold(f32::INFINITY, |a, b| a.min(b))
    }

    fn reduce_max(self) -> f32 {
        let arr = self.as_array_ref();
        arr.iter().copied().fold(f32::NEG_INFINITY, |a, b| a.max(b))
    }
}

#[cfg(feature = "simd")]
impl WideExt for wide::f32x8 {
    type Element = f32;

    fn from_slice_unaligned(slice: &[f32]) -> Self {
        if slice.len() >= 8 {
            Self::new([slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7]])
        } else {
            let mut padded = [0.0f32; 8];
            padded[..slice.len()].copy_from_slice(slice);
            Self::new(padded)
        }
    }

    fn reduce_min(self) -> f32 {
        let arr = self.as_array_ref();
        arr.iter().copied().fold(f32::INFINITY, |a, b| a.min(b))
    }

    fn reduce_max(self) -> f32 {
        let arr = self.as_array_ref();
        arr.iter().copied().fold(f32::NEG_INFINITY, |a, b| a.max(b))
    }
}

// Note: faster crate has compatibility issues, so we implement manual SIMD sorting

/// SIMD capabilities detected at runtime
#[derive(Debug, Clone, Copy)]
pub struct SIMDCapabilities {
    /// AVX2 support (256-bit vectors)
    pub avx2: bool,
    /// AVX support (256-bit vectors, older)
    pub avx: bool,
    /// SSE4.1 support (128-bit vectors)
    pub sse41: bool,
    /// SSE2 support (128-bit vectors, baseline)
    pub sse2: bool,
    /// FMA (Fused Multiply-Add) support
    pub fma: bool,
}

impl Default for SIMDCapabilities {
    fn default() -> Self {
        Self {
            avx2: false,
            avx: false,
            sse41: false,
            sse2: false,
            fma: false,
        }
    }
}

static SIMD_CAPABILITIES: OnceLock<SIMDCapabilities> = OnceLock::new();

/// Initialize and detect SIMD capabilities
pub fn init_simd_capabilities() {
    let capabilities = detect_simd_capabilities();
    let _ = SIMD_CAPABILITIES.set(capabilities);
}

/// Get detected SIMD capabilities
pub fn get_simd_capabilities() -> SIMDCapabilities {
    *SIMD_CAPABILITIES.get().unwrap_or(&SIMDCapabilities::default())
}

/// Detect available SIMD capabilities at runtime
fn detect_simd_capabilities() -> SIMDCapabilities {
    #[cfg(target_arch = "x86_64")]
    {
        use raw_cpuid::CpuId;
        let cpuid = CpuId::new();

        let mut capabilities = SIMDCapabilities::default();

        if let Some(finfo) = cpuid.get_feature_info() {
            capabilities.sse2 = finfo.has_sse2();
            capabilities.sse41 = finfo.has_sse41();
            capabilities.avx = finfo.has_avx();
            capabilities.fma = finfo.has_fma();
        }

        if let Some(einfo) = cpuid.get_extended_feature_info() {
            capabilities.avx2 = einfo.has_avx2();
        }

        capabilities
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        SIMDCapabilities::default()
    }
}

/// SIMD-optimized database operations
pub struct SIMDOperations;

impl SIMDOperations {
    /// SIMD-optimized integer sum aggregation
    pub fn sum_i32(values: &[i32]) -> i64 {
        #[cfg(feature = "simd")]
        {
            let capabilities = get_simd_capabilities();
            if capabilities.avx2 {
                return Self::sum_i32_avx2(values);
            } else if capabilities.sse41 {
                return Self::sum_i32_sse41(values);
            }
        }
        Self::sum_i32_scalar(values)
    }

    /// SIMD-optimized float sum aggregation
    pub fn sum_f32(values: &[f32]) -> f64 {
        #[cfg(feature = "simd")]
        {
            let capabilities = get_simd_capabilities();
            if capabilities.avx2 {
                return Self::sum_f32_avx2(values);
            } else if capabilities.sse41 {
                return Self::sum_f32_sse41(values);
            }
        }
        Self::sum_f32_scalar(values)
    }

    /// SIMD-optimized integer filtering (WHERE clause evaluation)
    pub fn filter_i32(values: &[i32], predicate: i32, op: ComparisonOp) -> Vec<usize> {
        #[cfg(feature = "simd")]
        {
            let capabilities = get_simd_capabilities();
            if capabilities.avx2 {
                return Self::filter_i32_avx2(values, predicate, op);
            } else if capabilities.sse41 {
                return Self::filter_i32_sse41(values, predicate, op);
            }
        }
        Self::filter_i32_scalar(values, predicate, op)
    }

    /// SIMD-optimized floating point filtering
    pub fn filter_f32(values: &[f32], predicate: f32, op: ComparisonOp) -> Vec<usize> {
        #[cfg(feature = "simd")]
        {
            let capabilities = get_simd_capabilities();
            if capabilities.avx2 {
                return Self::filter_f32_avx2(values, predicate, op);
            } else if capabilities.sse41 {
                return Self::filter_f32_sse41(values, predicate, op);
            }
        }
        Self::filter_f32_scalar(values, predicate, op)
    }

    /// SIMD-optimized quicksort for integers
    pub fn sort_i32(values: &mut [i32]) {
        #[cfg(feature = "simd")]
        {
            let capabilities = get_simd_capabilities();
            if capabilities.avx2 && values.len() >= 16 {
                Self::sort_i32_simd(values);
                return;
            }
        }
        values.sort_unstable();
    }

    /// SIMD-optimized quicksort for floats
    pub fn sort_f32(values: &mut [f32]) {
        #[cfg(feature = "simd")]
        {
            let capabilities = get_simd_capabilities();
            if capabilities.avx2 && values.len() >= 16 {
                Self::sort_f32_simd(values);
                return;
            }
        }
        values.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    }

    // AVX2 implementations (256-bit vectors)
    #[cfg(feature = "simd")]
    fn sum_i32_avx2(values: &[i32]) -> i64 {
        let mut sum = 0i64;
        let chunks = values.chunks_exact(8);
        let remainder = chunks.remainder();

        // Process 8 elements at a time with AVX2
        for chunk in chunks {
            let vec = i32x8::from([chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7]]);
            sum += (vec.to_array().iter().sum::<i32>()) as i64;
        }

        // Handle remainder
        sum + Self::sum_i32_scalar(remainder)
    }

    #[cfg(feature = "simd")]
    fn sum_f32_avx2(values: &[f32]) -> f64 {
        let mut sum = 0.0f64;
        let chunks = values.chunks_exact(8);
        let remainder = chunks.remainder();

        // Process 8 elements at a time with AVX2
        for chunk in chunks {
            let vec = f32x8::from_slice_unaligned(chunk);
            sum += vec.reduce_add() as f64;
        }

        // Handle remainder
        sum + Self::sum_f32_scalar(remainder)
    }

    #[cfg(feature = "simd")]
    fn filter_i32_avx2(values: &[i32], predicate: i32, op: ComparisonOp) -> Vec<usize> {
        let mut indices = Vec::new();
        let chunks = values.chunks_exact(8);
        let remainder = chunks.remainder();

        let pred_vec = i32x8::splat(predicate);

        // Process 8 elements at a time
        for (chunk_idx, chunk) in chunks.enumerate() {
            let vec = i32x8::from_slice_unaligned(chunk);
            let mask = match op {
                ComparisonOp::Equal => vec.cmp_eq(pred_vec),
                ComparisonOp::GreaterThan => vec.cmp_gt(pred_vec),
                ComparisonOp::LessThan => vec.cmp_lt(pred_vec),
                ComparisonOp::GreaterEqual => !vec.cmp_lt(pred_vec),
                ComparisonOp::LessEqual => !vec.cmp_gt(pred_vec),
            };

            // Extract matching indices
            for (i, &matches) in mask.as_array_ref().iter().enumerate() {
                if matches != 0 {
                    indices.push(chunk_idx * 8 + i);
                }
            }
        }

        // Handle remainder
        let remainder_indices = Self::filter_i32_scalar(remainder, predicate, op);
        for idx in remainder_indices {
            indices.push(values.len() - remainder.len() + idx);
        }

        indices
    }

    #[cfg(feature = "simd")]
    fn filter_f32_avx2(values: &[f32], predicate: f32, op: ComparisonOp) -> Vec<usize> {
        let mut indices = Vec::new();
        let chunks = values.chunks_exact(8);
        let remainder = chunks.remainder();

        let pred_vec = f32x8::splat(predicate);

        // Process 8 elements at a time
        for (chunk_idx, chunk) in chunks.enumerate() {
            let vec = f32x8::from_slice_unaligned(chunk);
            let mask = match op {
                ComparisonOp::Equal => vec.cmp_eq(pred_vec),
                ComparisonOp::GreaterThan => vec.cmp_gt(pred_vec),
                ComparisonOp::LessThan => vec.cmp_lt(pred_vec),
                ComparisonOp::GreaterEqual => !vec.cmp_lt(pred_vec),
                ComparisonOp::LessEqual => !vec.cmp_gt(pred_vec),
            };

            // Extract matching indices
            for (i, &matches) in mask.as_array_ref().iter().enumerate() {
                if matches != 0.0 {
                    indices.push(chunk_idx * 8 + i);
                }
            }
        }

        // Handle remainder
        let remainder_indices = Self::filter_f32_scalar(remainder, predicate, op);
        for idx in remainder_indices {
            indices.push(values.len() - remainder.len() + idx);
        }

        indices
    }

    // SSE4.1 implementations (128-bit vectors)
    #[cfg(feature = "simd")]
    fn sum_i32_sse41(values: &[i32]) -> i64 {
        let mut sum = 0i64;
        let chunks = values.chunks_exact(4);
        let remainder = chunks.remainder();

        // Process 4 elements at a time with SSE4.1
        for chunk in chunks {
            let vec = i32x4::from_slice_unaligned(chunk);
            sum += vec.reduce_add() as i64;
        }

        // Handle remainder
        sum + Self::sum_i32_scalar(remainder)
    }

    #[cfg(feature = "simd")]
    fn sum_f32_sse41(values: &[f32]) -> f64 {
        let mut sum = 0.0f64;
        let chunks = values.chunks_exact(4);
        let remainder = chunks.remainder();

        // Process 4 elements at a time with SSE4.1
        for chunk in chunks {
            let vec = f32x4::from_slice_unaligned(chunk);
            sum += vec.reduce_add() as f64;
        }

        // Handle remainder
        sum + Self::sum_f32_scalar(remainder)
    }

    #[cfg(feature = "simd")]
    fn filter_i32_sse41(values: &[i32], predicate: i32, op: ComparisonOp) -> Vec<usize> {
        let mut indices = Vec::new();
        let chunks = values.chunks_exact(4);
        let remainder = chunks.remainder();

        let pred_vec = i32x4::splat(predicate);

        // Process 4 elements at a time
        for (chunk_idx, chunk) in chunks.enumerate() {
            let vec = i32x4::from_slice_unaligned(chunk);
            let mask = match op {
                ComparisonOp::Equal => vec.cmp_eq(pred_vec),
                ComparisonOp::GreaterThan => vec.cmp_gt(pred_vec),
                ComparisonOp::LessThan => vec.cmp_lt(pred_vec),
                ComparisonOp::GreaterEqual => !vec.cmp_lt(pred_vec),
                ComparisonOp::LessEqual => !vec.cmp_gt(pred_vec),
            };

            // Extract matching indices
            for (i, &matches) in mask.as_array_ref().iter().enumerate() {
                if matches != 0 {
                    indices.push(chunk_idx * 4 + i);
                }
            }
        }

        // Handle remainder
        let remainder_indices = Self::filter_i32_scalar(remainder, predicate, op);
        for idx in remainder_indices {
            indices.push(values.len() - remainder.len() + idx);
        }

        indices
    }

    #[cfg(feature = "simd")]
    fn filter_f32_sse41(values: &[f32], predicate: f32, op: ComparisonOp) -> Vec<usize> {
        let mut indices = Vec::new();
        let chunks = values.chunks_exact(4);
        let remainder = chunks.remainder();

        let pred_vec = f32x4::splat(predicate);

        // Process 4 elements at a time
        for (chunk_idx, chunk) in chunks.enumerate() {
            let vec = f32x4::from_slice_unaligned(chunk);
            let mask = match op {
                ComparisonOp::Equal => vec.cmp_eq(pred_vec),
                ComparisonOp::GreaterThan => vec.cmp_gt(pred_vec),
                ComparisonOp::LessThan => vec.cmp_lt(pred_vec),
                ComparisonOp::GreaterEqual => !vec.cmp_lt(pred_vec),
                ComparisonOp::LessEqual => !vec.cmp_gt(pred_vec),
            };

            // Extract matching indices
            for (i, &matches) in mask.as_array_ref().iter().enumerate() {
                if matches != 0.0 {
                    indices.push(chunk_idx * 4 + i);
                }
            }
        }

        // Handle remainder
        let remainder_indices = Self::filter_f32_scalar(remainder, predicate, op);
        for idx in remainder_indices {
            indices.push(values.len() - remainder.len() + idx);
        }

        indices
    }

    // SIMD-optimized sorting
    #[cfg(feature = "simd")]
    fn sort_i32_simd(values: &mut [i32]) {
        // For now, fall back to standard sorting which is already highly optimized
        // TODO: Implement custom SIMD sorting network for small arrays
        values.sort_unstable();
    }

    #[cfg(feature = "simd")]
    fn sort_f32_simd(values: &mut [f32]) {
        // For now, fall back to standard sorting which is already highly optimized
        // TODO: Implement custom SIMD sorting network for small arrays
        values.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    }

    // Scalar fallback implementations
    fn sum_i32_scalar(values: &[i32]) -> i64 {
        values.iter().map(|&x| x as i64).sum()
    }

    fn sum_f32_scalar(values: &[f32]) -> f64 {
        values.iter().map(|&x| x as f64).sum()
    }

    fn filter_i32_scalar(values: &[i32], predicate: i32, op: ComparisonOp) -> Vec<usize> {
        values
            .iter()
            .enumerate()
            .filter_map(|(i, &value)| {
                let matches = match op {
                    ComparisonOp::Equal => value == predicate,
                    ComparisonOp::GreaterThan => value > predicate,
                    ComparisonOp::LessThan => value < predicate,
                    ComparisonOp::GreaterEqual => value >= predicate,
                    ComparisonOp::LessEqual => value <= predicate,
                };
                if matches { Some(i) } else { None }
            })
            .collect()
    }

    fn filter_f32_scalar(values: &[f32], predicate: f32, op: ComparisonOp) -> Vec<usize> {
        values
            .iter()
            .enumerate()
            .filter_map(|(i, &value)| {
                let matches = match op {
                    ComparisonOp::Equal => (value - predicate).abs() < f32::EPSILON,
                    ComparisonOp::GreaterThan => value > predicate,
                    ComparisonOp::LessThan => value < predicate,
                    ComparisonOp::GreaterEqual => value >= predicate,
                    ComparisonOp::LessEqual => value <= predicate,
                };
                if matches { Some(i) } else { None }
            })
            .collect()
    }
}

/// Comparison operations for SIMD filtering
#[derive(Debug, Clone, Copy)]
pub enum ComparisonOp {
    /// Equal (==)
    Equal,
    /// Greater than (>)
    GreaterThan,
    /// Less than (<)
    LessThan,
    /// Greater than or equal (>=)
    GreaterEqual,
    /// Less than or equal (<=)
    LessEqual,
}

/// SIMD-optimized aggregation functions
pub struct SIMDAggregation;

impl SIMDAggregation {
    /// Calculate min, max, sum, and count in a single SIMD pass
    pub fn aggregate_i32(values: &[i32]) -> AggregateResult<i32, i64> {
        if values.is_empty() {
            return AggregateResult::default();
        }

        #[cfg(feature = "simd")]
        {
            let capabilities = get_simd_capabilities();
            if capabilities.avx2 {
                return Self::aggregate_i32_avx2(values);
            } else if capabilities.sse41 {
                return Self::aggregate_i32_sse41(values);
            }
        }

        Self::aggregate_i32_scalar(values)
    }

    /// Calculate min, max, sum, and count for floats
    pub fn aggregate_f32(values: &[f32]) -> AggregateResult<f32, f64> {
        if values.is_empty() {
            return AggregateResult::default();
        }

        #[cfg(feature = "simd")]
        {
            let capabilities = get_simd_capabilities();
            if capabilities.avx2 {
                return Self::aggregate_f32_avx2(values);
            } else if capabilities.sse41 {
                return Self::aggregate_f32_sse41(values);
            }
        }

        Self::aggregate_f32_scalar(values)
    }

    #[cfg(feature = "simd")]
    fn aggregate_i32_avx2(values: &[i32]) -> AggregateResult<i32, i64> {
        let first = values[0];
        let mut min_vec = i32x8::splat(first);
        let mut max_vec = i32x8::splat(first);
        let mut sum = 0i64;

        let chunks = values.chunks_exact(8);
        let remainder = chunks.remainder();

        for chunk in chunks {
            let vec = i32x8::from_slice_unaligned(chunk);
            min_vec = min_vec.min(vec);
            max_vec = max_vec.max(vec);
            sum += vec.reduce_add() as i64;
        }

        let min_val = min_vec.reduce_min();
        let max_val = max_vec.reduce_max();

        // Handle remainder
        let remainder_agg = Self::aggregate_i32_scalar(remainder);

        AggregateResult {
            min: min_val.min(remainder_agg.min),
            max: max_val.max(remainder_agg.max),
            sum: sum + remainder_agg.sum,
            count: values.len(),
        }
    }

    #[cfg(feature = "simd")]
    fn aggregate_f32_avx2(values: &[f32]) -> AggregateResult<f32, f64> {
        let first = values[0];
        let mut min_vec = f32x8::splat(first);
        let mut max_vec = f32x8::splat(first);
        let mut sum = 0.0f64;

        let chunks = values.chunks_exact(8);
        let remainder = chunks.remainder();

        for chunk in chunks {
            let vec = f32x8::from_slice_unaligned(chunk);
            min_vec = min_vec.min(vec);
            max_vec = max_vec.max(vec);
            sum += vec.reduce_add() as f64;
        }

        let min_val = min_vec.reduce_min();
        let max_val = max_vec.reduce_max();

        // Handle remainder
        let remainder_agg = Self::aggregate_f32_scalar(remainder);

        AggregateResult {
            min: min_val.min(remainder_agg.min),
            max: max_val.max(remainder_agg.max),
            sum: sum + remainder_agg.sum,
            count: values.len(),
        }
    }

    #[cfg(feature = "simd")]
    fn aggregate_i32_sse41(values: &[i32]) -> AggregateResult<i32, i64> {
        let first = values[0];
        let mut min_vec = i32x4::splat(first);
        let mut max_vec = i32x4::splat(first);
        let mut sum = 0i64;

        let chunks = values.chunks_exact(4);
        let remainder = chunks.remainder();

        for chunk in chunks {
            let vec = i32x4::from_slice_unaligned(chunk);
            min_vec = min_vec.min(vec);
            max_vec = max_vec.max(vec);
            sum += vec.reduce_add() as i64;
        }

        let min_val = min_vec.reduce_min();
        let max_val = max_vec.reduce_max();

        // Handle remainder
        let remainder_agg = Self::aggregate_i32_scalar(remainder);

        AggregateResult {
            min: min_val.min(remainder_agg.min),
            max: max_val.max(remainder_agg.max),
            sum: sum + remainder_agg.sum,
            count: values.len(),
        }
    }

    #[cfg(feature = "simd")]
    fn aggregate_f32_sse41(values: &[f32]) -> AggregateResult<f32, f64> {
        let first = values[0];
        let mut min_vec = f32x4::splat(first);
        let mut max_vec = f32x4::splat(first);
        let mut sum = 0.0f64;

        let chunks = values.chunks_exact(4);
        let remainder = chunks.remainder();

        for chunk in chunks {
            let vec = f32x4::from_slice_unaligned(chunk);
            min_vec = min_vec.min(vec);
            max_vec = max_vec.max(vec);
            sum += vec.reduce_add() as f64;
        }

        let min_val = min_vec.reduce_min();
        let max_val = max_vec.reduce_max();

        // Handle remainder
        let remainder_agg = Self::aggregate_f32_scalar(remainder);

        AggregateResult {
            min: min_val.min(remainder_agg.min),
            max: max_val.max(remainder_agg.max),
            sum: sum + remainder_agg.sum,
            count: values.len(),
        }
    }

    fn aggregate_i32_scalar(values: &[i32]) -> AggregateResult<i32, i64> {
        if values.is_empty() {
            return AggregateResult::default();
        }

        let mut min_val = values[0];
        let mut max_val = values[0];
        let mut sum = 0i64;

        for &value in values {
            min_val = min_val.min(value);
            max_val = max_val.max(value);
            sum += value as i64;
        }

        AggregateResult {
            min: min_val,
            max: max_val,
            sum,
            count: values.len(),
        }
    }

    fn aggregate_f32_scalar(values: &[f32]) -> AggregateResult<f32, f64> {
        if values.is_empty() {
            return AggregateResult::default();
        }

        let mut min_val = values[0];
        let mut max_val = values[0];
        let mut sum = 0.0f64;

        for &value in values {
            min_val = min_val.min(value);
            max_val = max_val.max(value);
            sum += value as f64;
        }

        AggregateResult {
            min: min_val,
            max: max_val,
            sum,
            count: values.len(),
        }
    }
}

/// Result of SIMD aggregation operations
#[derive(Debug, Clone)]
pub struct AggregateResult<T, S> {
    /// Minimum value
    pub min: T,
    /// Maximum value
    pub max: T,
    /// Sum of all values
    pub sum: S,
    /// Count of values
    pub count: usize,
}

impl<T: Default, S: Default> Default for AggregateResult<T, S> {
    fn default() -> Self {
        Self {
            min: T::default(),
            max: T::default(),
            sum: S::default(),
            count: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_capabilities_detection() {
        init_simd_capabilities();
        let capabilities = get_simd_capabilities();

        // On x86_64, we should at least have SSE2
        #[cfg(target_arch = "x86_64")]
        assert!(capabilities.sse2);
    }

    #[test]
    fn test_simd_sum_i32() {
        let values = (0..1000).collect::<Vec<i32>>();
        let expected: i64 = values.iter().map(|&x| x as i64).sum();
        let result = SIMDOperations::sum_i32(&values);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_simd_sum_f32() {
        let values: Vec<f32> = (0..1000).map(|x| x as f32).collect();
        let expected: f64 = values.iter().map(|&x| x as f64).sum();
        let result = SIMDOperations::sum_f32(&values);
        assert!((result - expected).abs() < 1e-6);
    }

    #[test]
    fn test_simd_filter_i32() {
        let values: Vec<i32> = (0..100).collect();
        let indices = SIMDOperations::filter_i32(&values, 50, ComparisonOp::GreaterThan);

        // Should find indices 51-99
        assert_eq!(indices.len(), 49);
        assert_eq!(indices[0], 51);
        assert_eq!(indices[48], 99);
    }

    #[test]
    fn test_simd_aggregation() {
        let values: Vec<i32> = vec![1, 5, 3, 9, 2, 8, 4, 7, 6];
        let result = SIMDAggregation::aggregate_i32(&values);

        assert_eq!(result.min, 1);
        assert_eq!(result.max, 9);
        assert_eq!(result.sum, 45);
        assert_eq!(result.count, 9);
    }

    #[test]
    fn test_simd_sort() {
        let mut values: Vec<i32> = vec![9, 3, 7, 1, 5, 8, 2, 6, 4];
        SIMDOperations::sort_i32(&mut values);
        assert_eq!(values, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }
}