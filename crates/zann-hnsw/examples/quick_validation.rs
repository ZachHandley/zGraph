//! Quick validation of HNSW performance

use std::time::Instant;
use tempfile::tempdir;
use zann_hnsw::{build_index, search, Metric};
use zcore_catalog::{Catalog, ColumnDef, ColumnType, TableDef};
use zcore_storage::{encode_key, next_seq, Store, COL_ROWS};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum Cell {
    Int(i64),
    Float(f64),
    Text(String),
    Vector(Vec<f32>),
}

fn setup_test_db(
    store: &Store,
    catalog: &Catalog,
    org_id: u64,
    table: &str,
    column: &str,
    vectors: &[Vec<f32>],
) -> Result<(), Box<dyn std::error::Error>> {
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== HNSW Performance Validation ===\n");

    let dim = 256;
    let dataset_size = 1000;
    let num_queries = 100;

    // Generate test data exactly like the working test
    println!("Generating {} vectors of dimension {}", dataset_size, dim);
    let vectors: Vec<Vec<f32>> = (0..dataset_size).map(|i| {
        (0..dim).map(|j| ((i + j) as f32) * 0.001).collect()
    }).collect();

    // Use exact query vectors from working test
    let queries: Vec<Vec<f32>> = (0..num_queries).map(|i| {
        (0..dim).map(|j| ((i + j) as f32) * 0.001 + 0.0001).collect()
    }).collect();

    // Setup database
    println!("Setting up database and inserting vectors...");
    let dir = tempdir()?;
    let store = Store::open(dir.path().join("hnsw_test.redb"))?;
    let catalog = Catalog::new(&store);

    let setup_start = Instant::now();
    setup_test_db(&store, &catalog, 1, "docs", "embedding", &vectors)?;
    let setup_time = setup_start.elapsed();
    println!("Database setup: {:.2}ms", setup_time.as_millis());

    // Build HNSW index
    println!("Building HNSW index...");
    let build_start = Instant::now();
    build_index(&store, &catalog, 1, "docs", "embedding", Metric::L2, 16, 64)?;
    let build_time = build_start.elapsed();
    println!("Index build time: {:.2}ms", build_time.as_millis());
    println!("Build throughput: {:.0} vectors/sec", dataset_size as f64 / build_time.as_secs_f64());

    // Test search performance
    println!("\nTesting search performance...");
    let search_start = Instant::now();
    let mut total_results = 0;

    for query in &queries {
        let results = search(&store, &catalog, 1, "docs", "embedding", query, 10, 128)?;
        total_results += results.len();
    }

    let search_time = search_start.elapsed();
    let avg_search_time = search_time.as_micros() / num_queries as u128;
    let search_throughput = num_queries as f64 / search_time.as_secs_f64();

    println!("Search completed:");
    println!("  Total queries: {}", num_queries);
    println!("  Average search time: {}μs", avg_search_time);
    println!("  Search throughput: {:.0} queries/sec", search_throughput);
    println!("  Total results found: {}", total_results);

    // Test different top-k values
    println!("\nTesting different top-k values:");
    for &k in &[1, 5, 10, 20, 50] {
        let start = Instant::now();
        for query in queries.iter().take(10) {
            let _results = search(&store, &catalog, 1, "docs", "embedding", query, k, 128)?;
        }
        let time = start.elapsed();
        let avg_time = time.as_micros() / 10;
        println!("  k={:<2}: {}μs per query", k, avg_time);
    }

    // Test different ef_search values
    println!("\nTesting different ef_search values:");
    for &ef in &[32, 64, 128, 256] {
        let start = Instant::now();
        for query in queries.iter().take(10) {
            let _results = search(&store, &catalog, 1, "docs", "embedding", query, 10, ef)?;
        }
        let time = start.elapsed();
        let avg_time = time.as_micros() / 10;
        println!("  ef={:<3}: {}μs per query", ef, avg_time);
    }

    Ok(())
}