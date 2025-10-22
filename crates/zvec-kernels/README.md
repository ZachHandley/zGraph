# zvec-kernels

[![Crates.io](https://img.shields.io/crates/v/zvec-kernels)](https://crates.io/crates/zvec-kernels)
[![Documentation](https://img.shields.io/docsrs/zvec-kernels)](https://docs.rs/zvec-kernels)
[![Build Status](https://img.shields.io/github/workflow/status/zachhandley/zdb/zvec-kernels)](https://github.com/zachhandley/zdb/actions)

High-performance SIMD-optimized distance metric kernels for vector databases and similarity search applications. This crate provides the core computational engine for ZRUSTDB's vector operations with automatic CPU feature detection and optimal performance across different architectures.

## Purpose

zvec-kernels serves as the foundational layer for vector similarity computations in ZRUSTDB, providing:

- **Maximum Performance**: SIMD-accelerated distance calculations using AVX2/SSE4.1 instructions
- **Universal Compatibility**: Automatic fallback to scalar implementations for broader CPU support
- **Memory Safety**: Zero-cost abstractions with comprehensive bounds checking
- **Production Ready**: Extensive testing, error handling, and performance validation

## Status: ✅ PRODUCTION-READY

**Current Version**: 0.1.0
**Build Status**: ✅ Compiles successfully on x86_64
**Performance**: 3-4x speedup over scalar implementations
**Platforms**: x86_64 (ARM support planned)
**Dependencies**: Minimal core dependencies with optional feature flags

## Key Features

- **Multi-Tier SIMD Optimization**: Three-tier strategy (AVX2 → SSE4.1 → scalar) for optimal performance
- **Runtime Feature Detection**: Automatic selection of best implementation based on CPU capabilities
- **Comprehensive Distance Metrics**: L2 (Euclidean), Inner Product, and Cosine distance with consistent APIs
- **Batch Processing**: Optimized algorithms for processing multiple vectors with top-k selection
- **Production Ready**: Extensive testing, error handling, and memory safety guarantees
- **Cross-Platform Support**: Works on x86_64 with graceful degradation on other architectures

## Performance

The SIMD implementations provide significant speedups over scalar code with automatic optimization based on available CPU features:

### Performance Characteristics

- **L2 Distance**: 3-4x faster with SIMD acceleration
- **Inner Product**: 3-5x faster with FMA (Fused Multiply-Add) instructions
- **Cosine Distance**: 3-4x faster with optimized norm calculations
- **Batch Operations**: Near-linear scaling with vector count and top-k selection
- **Memory Throughput**: Up to 0.21 billion elements/second on modern CPUs
- **Compilation**: Clean build with no errors on x86_64 platforms

### Real-World Benchmark Results

Performance validation on x86_64 with AVX2 support (lower is better):

```
Dimension | SIMD L2 (ns/op) | Scalar L2 (ns/op) | Speedup | Throughput (Gelem/s)
----------|-----------------|-------------------|---------|---------------------
128       | 761             | 2,504             | 3.29x   | 0.17
256       | 1,447           | 4,956             | 3.43x   | 0.18
512       | 2,810           | 9,857             | 3.51x   | 0.18
1024      | 5,577           | 20,241            | 3.63x   | 0.18
1536      | 9,275           | 31,220            | 3.37x   | 0.17
```

### Distance Metric Comparison (512 dimensions)

```
Metric        | Time (ns/op) | Throughput (Gelem/s) | Notes
--------------|--------------|---------------------|------------------
L2            | 2,826        | 0.18                | Square root computation
InnerProduct  | 2,497        | 0.21                | Fastest, no sqrt
Cosine        | 5,371        | 0.10                | Requires norm calculations
```

## Usage

### Basic Distance Calculations

```rust
use zvec_kernels::{distance, Metric};

let a = vec![1.0, 2.0, 3.0];
let b = vec![4.0, 5.0, 6.0];

// L2 (Euclidean) distance
let l2_dist = distance(&a, &b, Metric::L2)?;

// Inner product (negated for distance semantics)
let ip_dist = distance(&a, &b, Metric::InnerProduct)?;

// Cosine distance
let cos_dist = distance(&a, &b, Metric::Cosine)?;
```

### Batch Processing

```rust
use zvec_kernels::{batch_distance, Metric};

let query = vec![1.0, 2.0, 3.0];
let vectors: Vec<&[f32]> = vec![
    &[1.0, 1.0, 1.0],
    &[2.0, 2.0, 2.0],
    &[3.0, 3.0, 3.0],
];

// Find top 2 closest vectors
let results = batch_distance(&query, &vectors, Metric::L2, 2)?;
// Returns: Vec<(index, distance)> sorted by distance
```

### CPU Feature Detection

Check available SIMD capabilities at runtime:

```rust
use zvec_kernels::simd_features;

let features = simd_features();
println!("AVX2 available: {}", features.avx2);
println!("SSE4.1 available: {}", features.sse41);
println!("Scalar fallback: {}", features.scalar_fallback);

// The library automatically uses the best available implementation
let result = distance(&vec_a, &vec_b, Metric::L2)?;
```

### Error Handling

All operations return `Result<T, E>` for proper error handling:

```rust
use zvec_kernels::{distance, Metric};

let a = vec![1.0, 2.0, 3.0];
let b = vec![4.0, 5.0]; // Different length!

match distance(&a, &b, Metric::L2) {
    Ok(dist) => println!("Distance: {}", dist),
    Err(e) => println!("Error: {}", e), // "dim mismatch: 3 vs 2"
}
```

## Architecture

### SIMD Implementation Strategy

The crate uses a three-tier optimization strategy:

1. **AVX2 Implementation**: Processes 8 floats per instruction using `_mm256_*` intrinsics
2. **SSE4.1 Implementation**: Processes 4 floats per instruction using `_mm_*` intrinsics
3. **Scalar Fallback**: Standard Rust loops for maximum compatibility

### Runtime Dispatch

```rust
// Automatic best-path selection
#[cfg(feature = "simd")]
{
    if is_x86_feature_detected!("avx2") {
        return Ok(unsafe { distance_avx2(a, b, metric) });
    }
    if is_x86_feature_detected!("sse4.1") {
        return Ok(unsafe { distance_sse41(a, b, metric) });
    }
}

// Fallback to scalar
Ok(distance_scalar(a, b, metric))
```

### Key Optimizations

- **FMA Instructions**: Uses fused multiply-add (`_mm256_fmadd_ps`) for improved accuracy and performance
- **Loop Unrolling**: Processes multiple SIMD vectors per iteration for better instruction-level parallelism
- **Multi-Accumulator Pattern**: Reduces dependency chains by using 4 separate accumulators
- **Optimized Horizontal Reduction**: Efficient lane-wise summation using `hadd` instructions
- **Memory Access Patterns**: Unaligned loads (`loadu`) for flexibility, cache-friendly chunk processing
- **Remainder Handling**: Scalar processing for non-SIMD-aligned vector tails

### Algorithm Details

#### AVX2 Implementation (256-bit, 8 floats/instruction)
```rust
// Example: L2 distance with AVX2
let mut acc1 = _mm256_setzero_ps();
let mut acc2 = _mm256_setzero_ps();

for chunk in vectors.chunks(32) { // Process 32 elements (4 vectors) at once
    let va1 = _mm256_loadu_ps(ptr_a);
    let vb1 = _mm256_loadu_ps(ptr_b);
    let diff1 = _mm256_sub_ps(va1, vb1);
    acc1 = _mm256_fmadd_ps(diff1, diff1, acc1); // FMA: diff² + acc

    // ... unrolled for acc2, acc3, acc4
}
```

#### Performance Strategy
- **Small vectors (<64 dims)**: Simple processing to avoid overhead
- **Large vectors (≥64 dims)**: Aggressive unrolling with multiple accumulators
- **Remainder elements**: Scalar processing for perfect accuracy

## Memory Safety

All SIMD operations are encapsulated in safe Rust interfaces:

- **Bounds Checking**: Automatic length validation for all inputs
- **Alignment Handling**: Uses unaligned loads (`loadu`) for flexibility
- **Safe Transmutation**: No raw pointer manipulation in public APIs
- **Error Handling**: Proper `Result<T, E>` types for all fallible operations

## Building

The crate requires Rust 1.70+ for full SIMD support:

```bash
# Build with SIMD optimizations (default)
cargo build --release

# Build without SIMD (scalar only)
cargo build --release --no-default-features

# Build with optional features
cargo build --release --features "simd zobservability"

# Run tests
cargo test

# Run examples
cargo run --example simd_demo
cargo run --example performance_validation
cargo run --example comprehensive_performance

# Run benchmarks
cargo bench kernel_performance
cargo bench batch_operations

# Run test benchmarks
cargo test benches -- --ignored
```

### Feature Flags

- `simd` (default): Enable SIMD optimizations (AVX2/SSE4.1)
- `zobservability`: Enable metrics collection with ZRUSTDB observability (optional)
- `perf-engine`: Enable advanced performance optimizations (temporarily disabled due to wide crate API issues)
- `mobile-optimizations`: Mobile-specific optimizations (planned)
- `power-profiling`: Power consumption profiling (planned)

## Known Issues and Limitations

### Current Limitations

1. **Platform Support**: Currently optimized for x86_64 only
   - ARM NEON support planned for mobile and Apple Silicon
   - Graceful degradation to scalar on unsupported platforms

2. **Dependency Issues**:
   - `zperf-engine` integration temporarily disabled due to wide crate API compatibility issues
   - `zobservability` metrics collection available as optional feature to avoid cyclic dependencies

3. **Distance Metrics**: Limited to L2, Inner Product, and Cosine
   - Manhattan (L1) distance planned
   - Chebyshev (L∞) distance planned
   - Custom metric support considered

4. **Storage Backend**: No direct storage dependencies
   - This is a compute-only crate focused on SIMD optimizations
   - Does not use redb or fjall directly - those are handled by higher-level crates
   - Designed as a pure computation library for integration with storage layers

### Performance Considerations

- **Small Vectors**: For vectors < 64 dimensions, overhead may reduce speedup
- **Memory Alignment**: Uses unaligned loads for flexibility (5-10% performance penalty)
- **Batch Processing**: Optimal performance with batch sizes > 100 vectors

## Integration with ZRUSTDB

zvec-kernels is designed as the computational foundation for ZRUSTDB's vector operations:

- **HNSW Integration**: Used by `zann-hnsw` for approximate nearest neighbor search
- **SQL Engine**: Powers `zexec-engine` KNN queries with `ORDER BY embedding <-> ARRAY[...] LIMIT k`
- **REST API**: Drives `zserver` vector search endpoints for direct client access
- **Batch Operations**: Enables efficient multi-vector processing in collection queries
- **Storage Agnostic**: Works with any storage backend (redb, fjall, or others) through higher-level integration

## Examples and Testing

The crate includes comprehensive examples and benchmarks:

```bash
# Run the SIMD demonstration
cargo run --example simd_demo

# Performance validation with real metrics
cargo run --example performance_validation

# Comprehensive benchmarks
cargo run --example comprehensive_benchmark

# Run all tests
cargo test

# Run ignored benchmark tests
cargo test benches -- --ignored --nocapture
```

### Available Examples

- `simd_demo.rs` - Feature detection and basic performance demonstration
- `performance_validation.rs` - Detailed performance analysis with real-world data
- `comprehensive_benchmark.rs` - Extensive benchmarking across dimensions and metrics
- `dimension_scaling.rs` - Performance scaling analysis
- `debug_performance.rs` - Debugging performance characteristics

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.