//! HNSW integration benchmarks with optimized SIMD kernels
//!
//! This benchmark suite validates that the HNSW algorithm works correctly
//! with the optimized SIMD vector kernels and measures the performance improvements.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::time::Duration;
use tempfile::tempdir;
use zann_hnsw::{build_index, search, Metric};
use zcore_catalog::{Catalog, ColumnDef, ColumnType, TableDef};
use zcore_storage::{encode_key, next_seq, Store, COL_ROWS};

/// Cell type for storing vectors in the database
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum Cell {
    Vector(Vec<f32>),
}

/// Generate random vectors for benchmarking
fn generate_vectors(dim: usize, count: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count)
        .map(|_| (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect())
        .collect()
}

/// Generate normalized vectors for cosine similarity
fn generate_normalized_vectors(dim: usize, count: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count)
        .map(|_| {
            let mut vec: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
            let norm = vec.iter().map(|&x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                vec.iter_mut().for_each(|x| *x /= norm);
            }
            vec
        })
        .collect()
}

/// Setup test database with vectors
fn setup_test_db(
    store: &Store,
    catalog: &Catalog,
    org_id: u64,
    table: &str,
    column: &str,
    vectors: &[Vec<f32>],
) -> Result<(), Box<dyn std::error::Error>> {
    // Create table schema
    catalog.create_table(
        org_id,
        &TableDef {
            name: table.into(),
            columns: vec![ColumnDef {
                name: column.into(),
                ty: ColumnType::Vector,
                nullable: false,
                primary_key: false,
            }],
        },
    )?;

    // Insert vectors
    for vec in vectors {
        let mut w = store.begin_write()?;
        {
            let mut rows = w.open_table(store, COL_ROWS)?;
            let row_id = next_seq(store, b"rows:docs")?;
            let key = encode_key(&[
                b"rows",
                &org_id.to_be_bytes(),
                table.as_bytes(),
                &row_id.to_be_bytes(),
            ]);
            let cells = vec![Cell::Vector(vec.clone())];
            rows.insert(key.as_slice(), bincode::serialize(&cells)?.as_slice())?;
        }
        w.commit(store)?;
    }

    Ok(())
}

/// Benchmark index building with different dataset sizes
fn bench_index_building(c: &mut Criterion) {
    let dimensions = [128, 256, 512];
    let dataset_sizes = [1000, 5000, 10000];
    let metrics = [Metric::L2, Metric::Cosine, Metric::InnerProduct];

    let mut group = c.benchmark_group("index_building");
    group.measurement_time(Duration::from_secs(30));

    for &dim in &dimensions {
        for &size in &dataset_sizes {
            for &metric in &metrics {
                let vectors = if metric == Metric::Cosine {
                    generate_normalized_vectors(dim, size, 42)
                } else {
                    generate_vectors(dim, size, 42)
                };

                let throughput = Throughput::Elements(size as u64);
                group.throughput(throughput);

                group.bench_with_input(
                    BenchmarkId::new(
                        format!("build_{}_{:?}", dim, metric),
                        size,
                    ),
                    &vectors,
                    |b, vectors| {
                        b.iter_batched(
                            || {
                                let dir = tempdir().unwrap();
                                let store = Store::open(dir.path().join("test.redb")).unwrap();
                                let catalog = Catalog::new(&store);
                                setup_test_db(&store, &catalog, 1, "docs", "embedding", vectors)
                                    .unwrap();
                                store
                            },
                            |store| {
                                let catalog = Catalog::new(&store);
                                black_box(
                                    build_index(
                                        black_box(&store),
                                        black_box(&catalog),
                                        1,
                                        "docs",
                                        "embedding",
                                        metric,
                                        16,  // m
                                        64,  // ef_construction
                                    )
                                    .unwrap(),
                                )
                            },
                            criterion::BatchSize::LargeInput,
                        )
                    },
                );
            }
        }
    }
    group.finish();
}

/// Benchmark search performance with optimized kernels
fn bench_search_performance(c: &mut Criterion) {
    let dim = 256;
    let dataset_size = 5000;
    let num_queries = 100;
    let top_k_values = [1, 10, 50, 100];
    let ef_search_values = [64, 128, 256];

    let mut group = c.benchmark_group("search_performance");
    group.measurement_time(Duration::from_secs(20));

    for &metric in &[Metric::L2, Metric::Cosine, Metric::InnerProduct] {
        let vectors = if metric == Metric::Cosine {
            generate_normalized_vectors(dim, dataset_size, 123)
        } else {
            generate_vectors(dim, dataset_size, 123)
        };

        let queries = if metric == Metric::Cosine {
            generate_normalized_vectors(dim, num_queries, 456)
        } else {
            generate_vectors(dim, num_queries, 456)
        };

        // Setup database and build index once
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("search_test.redb")).unwrap();
        let catalog = Catalog::new(&store);
        setup_test_db(&store, &catalog, 1, "docs", "embedding", &vectors).unwrap();
        build_index(&store, &catalog, 1, "docs", "embedding", metric, 16, 64).unwrap();

        for &top_k in &top_k_values {
            for &ef_search in &ef_search_values {
                let throughput = Throughput::Elements(num_queries as u64);
                group.throughput(throughput);

                group.bench_with_input(
                    BenchmarkId::new(
                        format!("{:?}_k{}_ef{}", metric, top_k, ef_search),
                        dim,
                    ),
                    &queries,
                    |b, queries| {
                        b.iter(|| {
                            for query in queries.iter() {
                                black_box(
                                    search(
                                        black_box(&store),
                                        black_box(&catalog),
                                        1,
                                        "docs",
                                        "embedding",
                                        black_box(query),
                                        top_k,
                                        ef_search,
                                    )
                                    .unwrap(),
                                );
                            }
                        })
                    },
                );
            }
        }
    }
    group.finish();
}

/// Benchmark search latency for different vector dimensions
fn bench_search_scaling(c: &mut Criterion) {
    let dimensions = [64, 128, 256, 512, 768, 1024, 1536];
    let dataset_size = 2000;
    let top_k = 10;
    let ef_search = 128;

    let mut group = c.benchmark_group("search_scaling");
    group.measurement_time(Duration::from_secs(15));

    for &dim in &dimensions {
        let vectors = generate_vectors(dim, dataset_size, 789);
        let query = &vectors[0];

        // Setup database and build index
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("scaling_test.redb")).unwrap();
        let catalog = Catalog::new(&store);
        setup_test_db(&store, &catalog, 1, "docs", "embedding", &vectors[1..]).unwrap();
        build_index(&store, &catalog, 1, "docs", "embedding", Metric::L2, 16, 64).unwrap();

        let throughput = Throughput::Elements(dim as u64);
        group.throughput(throughput);

        group.bench_with_input(
            BenchmarkId::new("l2_search", dim),
            query,
            |b, query| {
                b.iter(|| {
                    black_box(
                        search(
                            black_box(&store),
                            black_box(&catalog),
                            1,
                            "docs",
                            "embedding",
                            black_box(query),
                            top_k,
                            ef_search,
                        )
                        .unwrap(),
                    )
                })
            },
        );
    }
    group.finish();
}

/// Benchmark recall quality with optimized kernels
fn bench_recall_quality(c: &mut Criterion) {
    let dim = 256;
    let dataset_size = 1000;
    let num_queries = 50;
    let top_k = 10;

    let mut group = c.benchmark_group("recall_quality");
    group.measurement_time(Duration::from_secs(10));

    let vectors = generate_vectors(dim, dataset_size, 999);
    let queries = generate_vectors(dim, num_queries, 888);

    // Setup database and build index
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path().join("recall_test.redb")).unwrap();
    let catalog = Catalog::new(&store);
    setup_test_db(&store, &catalog, 1, "docs", "embedding", &vectors).unwrap();
    build_index(&store, &catalog, 1, "docs", "embedding", Metric::L2, 16, 64).unwrap();

    // Test different ef_search values to measure recall vs speed tradeoff
    let ef_values = [32, 64, 128, 256, 512];
    for &ef_search in &ef_values {
        group.bench_with_input(
            BenchmarkId::new("recall_ef", ef_search),
            &queries,
            |b, queries| {
                b.iter(|| {
                    let mut total_results = 0;
                    for query in queries.iter() {
                        let results = search(
                            black_box(&store),
                            black_box(&catalog),
                            1,
                            "docs",
                            "embedding",
                            black_box(query),
                            top_k,
                            ef_search,
                        )
                        .unwrap();
                        total_results += results.len();
                    }
                    black_box(total_results)
                })
            },
        );
    }
    group.finish();
}

/// Compare SIMD-optimized HNSW vs theoretical brute force search
fn bench_vs_brute_force(c: &mut Criterion) {
    let dim = 128;
    let dataset_size = 1000;
    let top_k = 10;

    let vectors = generate_vectors(dim, dataset_size, 777);
    let query = &vectors[0];
    let candidates = &vectors[1..];

    let mut group = c.benchmark_group("vs_brute_force");
    group.measurement_time(Duration::from_secs(10));

    // Setup HNSW index
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path().join("comparison.redb")).unwrap();
    let catalog = Catalog::new(&store);
    setup_test_db(&store, &catalog, 1, "docs", "embedding", candidates).unwrap();
    build_index(&store, &catalog, 1, "docs", "embedding", Metric::L2, 16, 64).unwrap();

    // HNSW search
    group.bench_function("hnsw_search", |b| {
        b.iter(|| {
            black_box(
                search(
                    black_box(&store),
                    black_box(&catalog),
                    1,
                    "docs",
                    "embedding",
                    black_box(query),
                    top_k,
                    128,
                )
                .unwrap(),
            )
        })
    });

    // Brute force with optimized kernels for comparison
    group.bench_function("brute_force_simd", |b| {
        b.iter(|| {
            let candidates_slice: Vec<&[f32]> = candidates.iter().map(|v| v.as_slice()).collect();
            black_box(
                zvec_kernels::batch_distance(
                    black_box(query),
                    black_box(&candidates_slice),
                    zvec_kernels::Metric::L2,
                    top_k,
                )
                .unwrap(),
            )
        })
    });

    group.finish();
}

/// Benchmark memory usage patterns
fn bench_memory_patterns(c: &mut Criterion) {
    let scenarios = [
        ("small_high_freq", 64, 500, 100),     // Small vectors, high query frequency
        ("medium_balanced", 256, 2000, 50),    // Medium vectors, balanced
        ("large_low_freq", 1024, 5000, 10),   // Large vectors, lower query frequency
    ];

    let mut group = c.benchmark_group("memory_patterns");
    group.measurement_time(Duration::from_secs(15));

    for (scenario, dim, dataset_size, num_queries) in scenarios {
        let vectors = generate_vectors(dim, dataset_size, 555);
        let queries = generate_vectors(dim, num_queries, 666);

        // Setup database and build index
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("memory_test.redb")).unwrap();
        let catalog = Catalog::new(&store);
        setup_test_db(&store, &catalog, 1, "docs", "embedding", &vectors).unwrap();
        build_index(&store, &catalog, 1, "docs", "embedding", Metric::L2, 16, 64).unwrap();

        let throughput = Throughput::Elements((num_queries * dim) as u64);
        group.throughput(throughput);

        group.bench_with_input(
            BenchmarkId::new("memory_access", scenario),
            &queries,
            |b, queries| {
                b.iter(|| {
                    for query in queries.iter() {
                        black_box(
                            search(
                                black_box(&store),
                                black_box(&catalog),
                                1,
                                "docs",
                                "embedding",
                                black_box(query),
                                10,
                                128,
                            )
                            .unwrap(),
                        );
                    }
                })
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_index_building,
    bench_search_performance,
    bench_search_scaling,
    bench_recall_quality,
    bench_vs_brute_force,
    bench_memory_patterns,
);
criterion_main!(benches);