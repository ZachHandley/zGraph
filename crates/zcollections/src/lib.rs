//! zcollections: NoSQL collections with declarative projections to SQL tables
//!
//! This crate provides NoSQL document collection functionality with automatic
//! projection to SQL tables. It enables flexible JSON document storage while
//! maintaining the performance and consistency benefits of relational storage.
//!
//! # Key Features
//! - Declarative projection mapping from JSON keys to table columns
//! - Automatic type conversion and validation during projection
//! - Transactional consistency between document and table storage
//! - Support for vector embeddings in JSON documents
//! - Flexible schema evolution through projection updates
//!
//! # Projection Model
//! Collections use projection configurations to map JSON document fields to
//! typed table columns, enabling seamless integration between NoSQL flexibility
//! and SQL performance. Vector fields are automatically converted to the
//! appropriate VECTOR column types.
//!
//! # Integration
//! Used by zserver for collection ingest endpoints and by zexec-engine for
//! projection-based queries. Integrates with zcore-catalog for table schema
//! validation and zcore-storage for persistent data management.

use anyhow::{anyhow, Result};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use zcore_catalog as zcat;
use zcore_storage as zs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Projection {
    pub table: String,
    // map table_column -> json_key (flat for MVP)
    pub columns: IndexMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionInfo {
    pub name: String,
    pub projection: Projection,
}

pub fn key_proj(org_id: u64, name: &str) -> Vec<u8> {
    let org = org_id.to_be_bytes();
    zs::encode_key(&[b"collections:proj", &org, name.as_bytes()])
}

pub fn put_projection(store: &zs::Store, org_id: u64, name: &str, proj: &Projection) -> Result<()> {
    let key = key_proj(org_id, name);
    let value = bincode::serialize(proj)?;

    let mut tx = zs::WriteTransaction::new();
    tx.set(zs::COL_CATALOG, key, value);
    tx.commit(store)?;
    Ok(())
}

pub fn get_projection(store: &zs::Store, org_id: u64, name: &str) -> Result<Option<Projection>> {
    let key = key_proj(org_id, name);
    let tx = zs::ReadTransaction::new();

    if let Some(value_bytes) = tx.get(store, zs::COL_CATALOG, &key)? {
        Ok(Some(bincode::deserialize(&value_bytes)?))
    } else {
        Ok(None)
    }
}

pub fn apply_and_insert(
    store: &zs::Store,
    catalog: &zcat::Catalog,
    org_id: u64,
    coll_name: &str,
    doc: &JsonValue,
) -> Result<()> {
    let proj =
        get_projection(store, org_id, coll_name)?.ok_or_else(|| anyhow!("collection not found"))?;
    let tdef = catalog
        .get_table(org_id, &proj.table)?
        .ok_or_else(|| anyhow!("target table not found"))?;
    // Build cells vector in table column order
    let mut cells: Vec<zexec_cell::Cell> = vec![zexec_cell::Cell::Int(0); tdef.columns.len()];
    for (col_idx, col) in tdef.columns.iter().enumerate() {
        let Some(json_key) = proj.columns.get(&col.name) else {
            return Err(anyhow!("missing mapping for col {}", col.name));
        };
        let v = doc
            .get(json_key)
            .ok_or_else(|| anyhow!("missing key {}", json_key))?;
        let cell = match col.ty {
            zcat::ColumnType::Int | zcat::ColumnType::Integer => {
                zexec_cell::Cell::Int(v.as_i64().ok_or_else(|| anyhow!("{} not int", json_key))?)
            }
            zcat::ColumnType::Float | zcat::ColumnType::Decimal => zexec_cell::Cell::Float(
                v.as_f64()
                    .ok_or_else(|| anyhow!("{} not float", json_key))?,
            ),
            zcat::ColumnType::Text => zexec_cell::Cell::Text(
                v.as_str()
                    .ok_or_else(|| anyhow!("{} not string", json_key))?
                    .to_string(),
            ),
            zcat::ColumnType::Vector => {
                let arr = v
                    .as_array()
                    .ok_or_else(|| anyhow!("{} not array", json_key))?;
                let mut vecf = Vec::with_capacity(arr.len());
                for e in arr {
                    vecf.push(
                        e.as_f64()
                            .ok_or_else(|| anyhow!("vector element not number"))?
                            as f32,
                    );
                }
                zexec_cell::Cell::Vector(vecf)
            }
        };
        cells[col_idx] = cell;
    }
    // Persist row
    let row_id = zs::next_seq(
        store,
        &zs::encode_key(&[b"rows", &org_id.to_be_bytes(), proj.table.as_bytes()]),
    )?;
    let key = zs::encode_key(&[
        b"rows",
        &org_id.to_be_bytes(),
        proj.table.as_bytes(),
        &row_id.to_be_bytes(),
    ]);
    let value = bincode::serialize(&cells)?;

    let mut tx = zs::WriteTransaction::new();
    tx.set(zs::COL_ROWS, key, value);
    tx.commit(store)?;
    Ok(())
}

pub fn list_projections(store: &zs::Store, org_id: u64) -> Result<Vec<CollectionInfo>> {
    let tx = zs::ReadTransaction::new();
    let org_bytes = org_id.to_be_bytes();
    let prefix = zs::encode_key(&[b"collections:proj", &org_bytes]);
    let items = tx.scan_prefix(store, zs::COL_CATALOG, &prefix)?;

    let mut out = Vec::new();
    for (key_bytes, value_bytes) in items {
        let parts = decode_key_parts(&key_bytes)?;
        let name_part = parts
            .get(2)
            .ok_or_else(|| anyhow!("invalid projection key"))?;
        let name = String::from_utf8(name_part.clone()).map_err(|_| anyhow!("invalid utf8"))?;
        let proj: Projection = bincode::deserialize(&value_bytes)?;
        out.push(CollectionInfo {
            name,
            projection: proj,
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

fn decode_key_parts(key: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut parts = Vec::new();
    let mut idx = 0usize;
    while idx < key.len() {
        if idx + 4 > key.len() {
            return Err(anyhow!("malformed key"));
        }
        let len = u32::from_be_bytes([key[idx], key[idx + 1], key[idx + 2], key[idx + 3]]) as usize;
        idx += 4;
        if idx + len > key.len() {
            return Err(anyhow!("malformed key"));
        }
        parts.push(key[idx..idx + len].to_vec());
        idx += len;
    }
    Ok(parts)
}

// Small shim to share the Cell type without introducing a hard dep cycle
mod zexec_cell {
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub enum Cell {
        Int(i64),
        Float(f64),
        Text(String),
        Vector(Vec<f32>),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use zcore_catalog::{ColumnDef, ColumnType, TableDef};

    // Test utilities
    fn create_test_store() -> (TempDir, zs::Store) {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let store = zs::Store::open(temp_dir.path()).expect("Failed to create store");
        (temp_dir, store)
    }

    fn create_test_catalog_with_table<'a>(store: &'a zs::Store, org_id: u64, table_name: &str) -> zcat::Catalog<'a> {
        let catalog = zcat::Catalog::new(store);

        // Create a test table with various column types
        let table_def = TableDef {
            name: table_name.to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    ty: ColumnType::Int,
                    nullable: false,
                    primary_key: false,
                },
                ColumnDef {
                    name: "title".to_string(),
                    ty: ColumnType::Text,
                    nullable: true,
                    primary_key: false,
                },
                ColumnDef {
                    name: "score".to_string(),
                    ty: ColumnType::Float,
                    nullable: true,
                    primary_key: false,
                },
                ColumnDef {
                    name: "embedding".to_string(),
                    ty: ColumnType::Vector,
                    nullable: true,
                    primary_key: false,
                },
            ],
        };

        catalog.create_table(org_id, &table_def).expect("Failed to create test table");
        catalog
    }

    fn create_sample_projection(table_name: &str) -> Projection {
        let mut columns = IndexMap::new();
        columns.insert("id".to_string(), "doc_id".to_string());
        columns.insert("title".to_string(), "document_title".to_string());
        columns.insert("score".to_string(), "relevance_score".to_string());
        columns.insert("embedding".to_string(), "vector_data".to_string());

        Projection {
            table: table_name.to_string(),
            columns,
        }
    }

    fn create_sample_document() -> JsonValue {
        serde_json::json!({
            "doc_id": 42,
            "document_title": "Test Document",
            "relevance_score": 0.95,
            "vector_data": [0.1, 0.2, 0.3, 0.4]
        })
    }

    #[test]
    fn test_projection_crud_operations() {
        let (_temp_dir, store) = create_test_store();
        let org_id = 1;
        let collection_name = "test_collection";
        let projection = create_sample_projection("test_table");

        // Test creating a projection
        put_projection(&store, org_id, collection_name, &projection)
            .expect("Failed to put projection");

        // Test retrieving the projection
        let retrieved = get_projection(&store, org_id, collection_name)
            .expect("Failed to get projection")
            .expect("Projection should exist");

        assert_eq!(retrieved.table, projection.table);
        assert_eq!(retrieved.columns, projection.columns);

        // Test retrieving non-existent projection
        let non_existent = get_projection(&store, org_id, "non_existent")
            .expect("Failed to query non-existent projection");
        assert!(non_existent.is_none());
    }

    #[test]
    fn test_projection_org_isolation() {
        let (_temp_dir, store) = create_test_store();
        let org1_id = 1;
        let org2_id = 2;
        let collection_name = "shared_name";
        let projection1 = create_sample_projection("table1");
        let projection2 = create_sample_projection("table2");

        // Create projections for different orgs with same collection name
        put_projection(&store, org1_id, collection_name, &projection1)
            .expect("Failed to put projection for org1");
        put_projection(&store, org2_id, collection_name, &projection2)
            .expect("Failed to put projection for org2");

        // Verify org isolation
        let retrieved1 = get_projection(&store, org1_id, collection_name)
            .expect("Failed to get projection for org1")
            .expect("Projection for org1 should exist");
        let retrieved2 = get_projection(&store, org2_id, collection_name)
            .expect("Failed to get projection for org2")
            .expect("Projection for org2 should exist");

        assert_eq!(retrieved1.table, "table1");
        assert_eq!(retrieved2.table, "table2");
    }

    #[test]
    fn test_list_projections() {
        let (_temp_dir, store) = create_test_store();
        let org_id = 1;

        // Create multiple projections
        let projections = vec![
            ("collection_a", create_sample_projection("table_a")),
            ("collection_b", create_sample_projection("table_b")),
            ("collection_c", create_sample_projection("table_c")),
        ];

        for (name, proj) in &projections {
            put_projection(&store, org_id, name, proj)
                .expect("Failed to put projection");
        }

        // List projections
        let listed = list_projections(&store, org_id)
            .expect("Failed to list projections");

        assert_eq!(listed.len(), 3);

        // Check that they're sorted by name
        let names: Vec<_> = listed.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["collection_a", "collection_b", "collection_c"]);

        // Verify content
        for (i, (expected_name, expected_proj)) in projections.iter().enumerate() {
            assert_eq!(listed[i].name, *expected_name);
            assert_eq!(listed[i].projection.table, expected_proj.table);
        }
    }

    #[test]
    fn test_list_projections_empty() {
        let (_temp_dir, store) = create_test_store();
        let org_id = 1;

        let listed = list_projections(&store, org_id)
            .expect("Failed to list projections");
        assert!(listed.is_empty());
    }

    #[test]
    fn test_apply_and_insert_basic_types() {
        let (_temp_dir, store) = create_test_store();
        let org_id = 1;
        let table_name = "test_table";
        let collection_name = "test_collection";

        let catalog = create_test_catalog_with_table(&store, org_id, table_name);
        let projection = create_sample_projection(table_name);

        // Create projection
        put_projection(&store, org_id, collection_name, &projection)
            .expect("Failed to put projection");

        // Create and insert document
        let document = create_sample_document();
        apply_and_insert(&store, &catalog, org_id, collection_name, &document)
            .expect("Failed to apply and insert document");

        // Verify the row was inserted by checking the sequence counter
        let seq_key = zs::encode_key(&[b"rows", &org_id.to_be_bytes(), table_name.as_bytes()]);
        let tx = zs::ReadTransaction::new();
        let seq = tx.get(&store, zs::COL_SEQ, &seq_key)
            .expect("Failed to get sequence")
            .and_then(|bytes| bincode::deserialize::<u64>(&bytes).ok())
            .unwrap_or(0);
        assert_eq!(seq, 1); // Should have incremented to 1
    }

    #[test]
    fn test_apply_and_insert_type_validation() {
        let (_temp_dir, store) = create_test_store();
        let org_id = 1;
        let table_name = "test_table";
        let collection_name = "test_collection";

        let catalog = create_test_catalog_with_table(&store, org_id, table_name);
        let projection = create_sample_projection(table_name);

        put_projection(&store, org_id, collection_name, &projection)
            .expect("Failed to put projection");

        // Test with invalid integer type
        let invalid_doc = serde_json::json!({
            "doc_id": "not_an_integer",
            "document_title": "Test Document",
            "relevance_score": 0.95,
            "vector_data": [0.1, 0.2, 0.3, 0.4]
        });

        let result = apply_and_insert(&store, &catalog, org_id, collection_name, &invalid_doc);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not int"));
    }

    #[test]
    fn test_apply_and_insert_missing_field() {
        let (_temp_dir, store) = create_test_store();
        let org_id = 1;
        let table_name = "test_table";
        let collection_name = "test_collection";

        let catalog = create_test_catalog_with_table(&store, org_id, table_name);
        let projection = create_sample_projection(table_name);

        put_projection(&store, org_id, collection_name, &projection)
            .expect("Failed to put projection");

        // Document missing required field
        let incomplete_doc = serde_json::json!({
            "doc_id": 42,
            "document_title": "Test Document",
            // Missing relevance_score and vector_data
        });

        let result = apply_and_insert(&store, &catalog, org_id, collection_name, &incomplete_doc);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing key"));
    }

    #[test]
    fn test_apply_and_insert_collection_not_found() {
        let (_temp_dir, store) = create_test_store();
        let org_id = 1;
        let table_name = "test_table";

        let catalog = create_test_catalog_with_table(&store, org_id, table_name);
        let document = create_sample_document();

        let result = apply_and_insert(&store, &catalog, org_id, "non_existent_collection", &document);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("collection not found"));
    }

    #[test]
    fn test_vector_field_conversion() {
        let (_temp_dir, store) = create_test_store();
        let org_id = 1;
        let table_name = "vector_table";
        let collection_name = "vector_collection";

        let catalog = create_test_catalog_with_table(&store, org_id, table_name);
        let projection = create_sample_projection(table_name);

        put_projection(&store, org_id, collection_name, &projection)
            .expect("Failed to put projection");

        // Document with vector field
        let vector_doc = serde_json::json!({
            "doc_id": 1,
            "document_title": "Vector Document",
            "relevance_score": 0.8,
            "vector_data": [1.0, 2.5, -0.5, std::f32::consts::PI]
        });

        apply_and_insert(&store, &catalog, org_id, collection_name, &vector_doc)
            .expect("Failed to insert vector document");

        // Verify insertion succeeded
        let seq_key = zs::encode_key(&[b"rows", &org_id.to_be_bytes(), table_name.as_bytes()]);
        let tx = zs::ReadTransaction::new();
        let seq = tx.get(&store, zs::COL_SEQ, &seq_key)
            .expect("Failed to get sequence")
            .and_then(|bytes| bincode::deserialize::<u64>(&bytes).ok())
            .unwrap_or(0);
        assert_eq!(seq, 1);
    }

    #[test]
    fn test_vector_field_invalid_type() {
        let (_temp_dir, store) = create_test_store();
        let org_id = 1;
        let table_name = "vector_table";
        let collection_name = "vector_collection";

        let catalog = create_test_catalog_with_table(&store, org_id, table_name);
        let projection = create_sample_projection(table_name);

        put_projection(&store, org_id, collection_name, &projection)
            .expect("Failed to put projection");

        // Document with invalid vector field (not an array)
        let invalid_vector_doc = serde_json::json!({
            "doc_id": 1,
            "document_title": "Invalid Vector Document",
            "relevance_score": 0.8,
            "vector_data": "not_an_array"
        });

        let result = apply_and_insert(&store, &catalog, org_id, collection_name, &invalid_vector_doc);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not array"));
    }

    #[test]
    fn test_multiple_documents_insertion() {
        let (_temp_dir, store) = create_test_store();
        let org_id = 1;
        let table_name = "multi_doc_table";
        let collection_name = "multi_doc_collection";

        let catalog = create_test_catalog_with_table(&store, org_id, table_name);
        let projection = create_sample_projection(table_name);

        put_projection(&store, org_id, collection_name, &projection)
            .expect("Failed to put projection");

        // Insert multiple documents
        for i in 1..=5 {
            let doc = serde_json::json!({
                "doc_id": i,
                "document_title": format!("Document {}", i),
                "relevance_score": i as f64 * 0.1,
                "vector_data": [i as f32, (i * 2) as f32, (i * 3) as f32]
            });

            apply_and_insert(&store, &catalog, org_id, collection_name, &doc)
                .unwrap_or_else(|_| panic!("Failed to insert document {}", i));
        }

        // Verify all documents were inserted
        let seq_key = zs::encode_key(&[b"rows", &org_id.to_be_bytes(), table_name.as_bytes()]);
        let tx = zs::ReadTransaction::new();
        let seq = tx.get(&store, zs::COL_SEQ, &seq_key)
            .expect("Failed to get sequence")
            .and_then(|bytes| bincode::deserialize::<u64>(&bytes).ok())
            .unwrap_or(0);
        assert_eq!(seq, 5);
    }

    #[test]
    fn test_key_encoding_decoding() {
        let org_id = 12345;
        let collection_name = "test_collection";

        // Test projection key encoding
        let key = key_proj(org_id, collection_name);
        assert!(!key.is_empty());

        // Test that different org_ids produce different keys
        let key2 = key_proj(67890, collection_name);
        assert_ne!(key, key2);

        // Test that different collection names produce different keys
        let key3 = key_proj(org_id, "different_collection");
        assert_ne!(key, key3);
    }

    #[test]
    fn test_decode_key_parts() {
        // Test valid key decoding
        let test_key = zs::encode_key(&[b"collections", b"proj", b"12345", b"test_collection"]);
        let parts = decode_key_parts(&test_key).expect("Failed to decode valid key");

        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], b"collections");
        assert_eq!(parts[1], b"proj");
        assert_eq!(parts[2], b"12345");
        assert_eq!(parts[3], b"test_collection");
    }

    #[test]
    fn test_decode_key_parts_malformed() {
        // Test malformed key (too short)
        let malformed_key = vec![0x00, 0x00];
        let result = decode_key_parts(&malformed_key);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("malformed key"));

        // Test key with invalid length
        let invalid_length_key = vec![0xFF, 0xFF, 0xFF, 0xFF]; // Claims huge length
        let result = decode_key_parts(&invalid_length_key);
        assert!(result.is_err());
    }

    #[test]
    fn test_projection_column_mapping_validation() {
        let (_temp_dir, store) = create_test_store();
        let org_id = 1;
        let table_name = "validation_table";
        let collection_name = "validation_collection";

        let catalog = create_test_catalog_with_table(&store, org_id, table_name);

        // Create projection with missing column mapping
        let mut incomplete_columns = IndexMap::new();
        incomplete_columns.insert("id".to_string(), "doc_id".to_string());
        // Missing mappings for other required columns

        let incomplete_projection = Projection {
            table: table_name.to_string(),
            columns: incomplete_columns,
        };

        put_projection(&store, org_id, collection_name, &incomplete_projection)
            .expect("Failed to put incomplete projection");

        let document = create_sample_document();
        let result = apply_and_insert(&store, &catalog, org_id, collection_name, &document);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing mapping for col"));
    }

    #[test]
    fn test_concurrent_operations() {
        use std::sync::Arc;
        use std::thread;

        let (_temp_dir, store) = create_test_store();
        let store = Arc::new(store);
        let org_id = 1;

        // Test concurrent projection creation
        let handles: Vec<_> = (0..5)
            .map(|i| {
                let store_clone = Arc::clone(&store);
                thread::spawn(move || {
                    let collection_name = format!("concurrent_collection_{}", i);
                    let projection = create_sample_projection(&format!("table_{}", i));
                    put_projection(&store_clone, org_id, &collection_name, &projection)
                })
            })
            .collect();

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap().expect("Concurrent projection creation failed");
        }

        // Verify all projections were created
        let projections = list_projections(&store, org_id)
            .expect("Failed to list projections after concurrent creation");
        assert_eq!(projections.len(), 5);
    }
}
