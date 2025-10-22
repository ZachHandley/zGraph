mod common;

use axum::http::StatusCode;
use common::{create_test_table, insert_test_data, TestClient};
use serde_json::json;

#[tokio::test]
async fn test_knn_search_exact() {
    let client = TestClient::new().await;

    // Create table with vector column
    create_test_table(&client, "vector_test", "id INT, embedding VECTOR").await;
    insert_test_data(&client, "vector_test", "(1, ARRAY[1.0, 0.0, 0.0]), (2, ARRAY[0.0, 1.0, 0.0]), (3, ARRAY[0.0, 0.0, 1.0])").await;

    let knn_request = json!({
        "metric": "l2",
        "query": [1.0, 0.0, 0.0],
        "vectors": [
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 0.0, 1.0],
            [0.5, 0.5, 0.0]
        ],
        "top_k": 2
    });

    let response = client.post("/v1/knn", knn_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let knn_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(knn_response["distances"].is_array());
    assert!(knn_response["indices"].is_array());

    let distances = knn_response["distances"].as_array().unwrap();
    let indices = knn_response["indices"].as_array().unwrap();

    assert_eq!(distances.len(), 2);
    assert_eq!(indices.len(), 2);

    // First result should be closest (smallest distance)
    assert!(distances[0].as_f64().unwrap() < distances[1].as_f64().unwrap());
}

#[tokio::test]
async fn test_knn_search_cosine() {
    let client = TestClient::new().await;

    let knn_request = json!({
        "metric": "cosine",
        "query": [1.0, 1.0, 0.0],
        "vectors": [
            [1.0, 1.0, 0.0],
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 1.0],
            [-1.0, -1.0, 0.0]
        ],
        "top_k": 3
    });

    let response = client.post("/v1/knn", knn_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let knn_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let distances = knn_response["distances"].as_array().unwrap();
    let indices = knn_response["indices"].as_array().unwrap();

    assert_eq!(distances.len(), 3);
    assert_eq!(indices.len(), 3);

    // With cosine similarity, higher values are more similar
    assert!(distances[0].as_f64().unwrap() >= distances[1].as_f64().unwrap());
}

#[tokio::test]
async fn test_knn_search_inner_product() {
    let client = TestClient::new().await;

    let knn_request = json!({
        "metric": "ip",
        "query": [2.0, 1.0, 0.0],
        "vectors": [
            [1.0, 1.0, 0.0],
            [2.0, 0.0, 0.0],
            [0.5, 0.5, 0.0],
            [0.0, 0.0, 1.0]
        ],
        "top_k": 2
    });

    let response = client.post("/v1/knn", knn_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let knn_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let distances = knn_response["distances"].as_array().unwrap();
    let indices = knn_response["indices"].as_array().unwrap();

    assert_eq!(distances.len(), 2);
    assert_eq!(indices.len(), 2);

    // With inner product, higher values are more similar
    assert!(distances[0].as_f64().unwrap() >= distances[1].as_f64().unwrap());
}

#[tokio::test]
async fn test_knn_search_invalid_metric() {
    let client = TestClient::new().await;

    let knn_request = json!({
        "metric": "invalid_metric",
        "query": [1.0, 0.0],
        "vectors": [[1.0, 0.0], [0.0, 1.0]],
        "top_k": 1
    });

    let response = client.post("/v1/knn", knn_request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_knn_search_dimension_mismatch() {
    let client = TestClient::new().await;

    let knn_request = json!({
        "metric": "l2",
        "query": [1.0, 0.0, 0.0], // 3D
        "vectors": [[1.0, 0.0], [0.0, 1.0]], // 2D
        "top_k": 1
    });

    let response = client.post("/v1/knn", knn_request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_knn_search_top_k_larger_than_dataset() {
    let client = TestClient::new().await;

    let knn_request = json!({
        "metric": "l2",
        "query": [1.0, 0.0],
        "vectors": [[1.0, 0.0], [0.0, 1.0], [0.5, 0.5]],
        "top_k": 10 // Larger than dataset size
    });

    let response = client.post("/v1/knn", knn_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let knn_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let distances = knn_response["distances"].as_array().unwrap();
    let indices = knn_response["indices"].as_array().unwrap();

    // Should return all vectors in dataset
    assert_eq!(distances.len(), 3);
    assert_eq!(indices.len(), 3);
}

#[tokio::test]
async fn test_knn_search_empty_dataset() {
    let client = TestClient::new().await;

    let knn_request = json!({
        "metric": "l2",
        "query": [1.0, 0.0],
        "vectors": [],
        "top_k": 1
    });

    let response = client.post("/v1/knn", knn_request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_knn_search_single_vector() {
    let client = TestClient::new().await;

    let knn_request = json!({
        "metric": "l2",
        "query": [1.0, 0.0],
        "vectors": [[1.0, 0.0]],
        "top_k": 1
    });

    let response = client.post("/v1/knn", knn_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let knn_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let distances = knn_response["distances"].as_array().unwrap();
    let indices = knn_response["indices"].as_array().unwrap();

    assert_eq!(distances.len(), 1);
    assert_eq!(indices.len(), 1);
    assert_eq!(distances[0], 0.0); // Distance to self should be 0
}

#[tokio::test]
async fn test_hnsw_build_index() {
    let client = TestClient::new().await;

    // Create table with vector column
    create_test_table(&client, "hnsw_test", "id INT, embedding VECTOR, category TEXT").await;
    insert_test_data(&client, "hnsw_test", "(1, ARRAY[1.0, 0.0, 0.0], 'A'), (2, ARRAY[0.0, 1.0, 0.0], 'B'), (3, ARRAY[0.0, 0.0, 1.0], 'A')").await;

    let build_request = json!({
        "table": "hnsw_test",
        "column": "embedding",
        "metric": "l2",
        "m": 16,
        "ef_construction": 100
    });

    let response = client.post("/v1/indexes/hnsw/build", build_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let build_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(build_response["index_id"].is_string());
    assert_eq!(build_response["table"], "hnsw_test");
    assert_eq!(build_response["column"], "embedding");
    assert!(build_response["num_vectors"].as_u64().unwrap() >= 3);
}

#[tokio::test]
async fn test_hnsw_list_indexes() {
    let client = TestClient::new().await;

    // Build an index first
    create_test_table(&client, "list_test", "id INT, embedding VECTOR").await;
    insert_test_data(&client, "list_test", "(1, ARRAY[1.0, 0.0])").await;

    let build_request = json!({
        "table": "list_test",
        "column": "embedding",
        "metric": "l2"
    });

    let build_response = client.post("/v1/indexes/hnsw/build", build_request).await;
    assert_eq!(build_response.status(), StatusCode::OK);

    // List indexes
    let response = client.get("/v1/indexes/hnsw").await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let list_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(list_response["indexes"].is_array());
    assert!(list_response["indexes"].as_array().unwrap().len() >= 1);

    // Find our index
    let indexes = list_response["indexes"].as_array().unwrap();
    let our_index = indexes.iter()
        .find(|idx| idx["table"] == "list_test" && idx["column"] == "embedding")
        .unwrap();

    assert_eq!(our_index["metric"], "l2");
}

#[tokio::test]
async fn test_hnsw_search() {
    let client = TestClient::new().await;

    // Create table and insert data
    create_test_table(&client, "search_test", "id INT, embedding VECTOR, name TEXT").await;
    insert_test_data(&client, "search_test",
        "(1, ARRAY[1.0, 0.0, 0.0], 'A'), (2, ARRAY[0.0, 1.0, 0.0], 'B'), (3, ARRAY[0.0, 0.0, 1.0], 'C'), (4, ARRAY[0.5, 0.5, 0.0], 'D')"
    ).await;

    // Build index
    let build_request = json!({
        "table": "search_test",
        "column": "embedding",
        "metric": "l2",
        "m": 16,
        "ef_construction": 100
    });

    let build_response = client.post("/v1/indexes/hnsw/build", build_request).await;
    assert_eq!(build_response.status(), StatusCode::OK);

    let build_body = axum::body::to_bytes(build_response.into_body(), usize::MAX).await.unwrap();
    let build_response_json: serde_json::Value = serde_json::from_slice(&build_body).unwrap();
    let index_id = build_response_json["index_id"].as_str().unwrap();

    // Search using the index
    let search_request = json!({
        "table": "search_test",
        "column": "embedding",
        "query": [1.0, 0.0, 0.0],
        "k": 2,
        "ef_search": 50
    });

    let response = client.post("/v1/indexes/hnsw/search", search_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let search_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(search_response["results"].is_array());
    let results = search_response["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);

    // Check that results have expected structure
    for result in results {
        assert!(result["id"].is_number());
        assert!(result["distance"].is_number());
        assert!(result["name"].is_string());
    }

    // Results should be ordered by distance (ascending for L2)
    if results.len() >= 2 {
        assert!(results[0]["distance"].as_f64().unwrap() <= results[1]["distance"].as_f64().unwrap());
    }
}

#[tokio::test]
async fn test_hnsw_search_with_filter() {
    let client = TestClient::new().await;

    // Create table with filterable column
    create_test_table(&client, "filter_test", "id INT, embedding VECTOR, category TEXT, active BOOLEAN").await;
    insert_test_data(&client, "filter_test",
        "(1, ARRAY[1.0, 0.0, 0.0], 'A', true), (2, ARRAY[0.9, 0.1, 0.0], 'A', false), (3, ARRAY[0.0, 1.0, 0.0], 'B', true)"
    ).await;

    // Build index
    let build_request = json!({
        "table": "filter_test",
        "column": "embedding",
        "metric": "l2"
    });

    let build_response = client.post("/v1/indexes/hnsw/build", build_request).await;
    assert_eq!(build_response.status(), StatusCode::OK);

    // Search with filter
    let search_request = json!({
        "table": "filter_test",
        "column": "embedding",
        "query": [1.0, 0.0, 0.0],
        "k": 5,
        "filter": {
            "category": "A",
            "active": true
        }
    });

    let response = client.post("/v1/indexes/hnsw/search", search_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let search_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let results = search_response["results"].as_array().unwrap();

    // Should only return results matching the filter
    for result in results {
        assert_eq!(result["category"], "A");
        assert_eq!(result["active"], true);
    }
}

#[tokio::test]
async fn test_hnsw_error_handling() {
    let client = TestClient::new().await;

    // Test build on non-existent table
    let build_request = json!({
        "table": "nonexistent_table",
        "column": "embedding",
        "metric": "l2"
    });

    let response = client.post("/v1/indexes/hnsw/build", build_request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Test build on non-existent column
    create_test_table(&client, "column_test", "id INT, name TEXT").await;

    let invalid_build_request = json!({
        "table": "column_test",
        "column": "nonexistent_column",
        "metric": "l2"
    });

    let response = client.post("/v1/indexes/hnsw/build", invalid_build_request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Test search on non-existent index
    let search_request = json!({
        "table": "nonexistent_table",
        "column": "embedding",
        "query": [1.0, 0.0],
        "k": 1
    });

    let response = client.post("/v1/indexes/hnsw/search", search_request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_vector_search_unauthorized() {
    let client = TestClient::new().await.with_token("".to_string());

    let knn_request = json!({
        "metric": "l2",
        "query": [1.0, 0.0],
        "vectors": [[1.0, 0.0]],
        "top_k": 1
    });

    let response = client.post("/v1/knn", knn_request).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let build_request = json!({
        "table": "test_table",
        "column": "embedding",
        "metric": "l2"
    });

    let response = client.post("/v1/indexes/hnsw/build", build_request).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = client.get("/v1/indexes/hnsw").await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_large_scale_vector_search() {
    let client = TestClient::new().await;

    // Create table with vectors
    create_test_table(&client, "large_test", "id INT, embedding VECTOR").await;

    // Generate and insert 100 vectors
    let mut values = Vec::new();
    for i in 0..100 {
        let vector = format!("ARRAY[{}, {}, {}]", i as f64 / 100.0, (i * 2) as f64 / 100.0, (i * 3) as f64 / 100.0);
        values.push(format!("({}, {})", i + 1, vector));
    }
    let all_values = values.join(", ");
    insert_test_data(&client, "large_test", &all_values).await;

    // Build HNSW index
    let build_request = json!({
        "table": "large_test",
        "column": "embedding",
        "metric": "l2",
        "m": 16,
        "ef_construction": 100
    });

    let build_response = client.post("/v1/indexes/hnsw/build", build_request).await;
    assert_eq!(build_response.status(), StatusCode::OK);

    let build_body = axum::body::to_bytes(build_response.into_body(), usize::MAX).await.unwrap();
    let build_response_json: serde_json::Value = serde_json::from_slice(&build_body).unwrap();
    assert_eq!(build_response_json["num_vectors"], 100);

    // Perform search
    let search_request = json!({
        "table": "large_test",
        "column": "embedding",
        "query": [0.5, 1.0, 1.5],
        "k": 10,
        "ef_search": 50
    });

    let response = client.post("/v1/indexes/hnsw/search", search_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let search_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let results = search_response["results"].as_array().unwrap();
    assert_eq!(results.len(), 10);

    // Results should be ordered by distance
    for i in 1..results.len() {
        assert!(results[i-1]["distance"].as_f64().unwrap() <= results[i]["distance"].as_f64().unwrap());
    }
}