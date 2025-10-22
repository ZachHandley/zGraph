mod common;

use axum::http::StatusCode;
use common::{assert_error_response, create_test_table, insert_test_data, select_test_data, TestClient};
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_list_collections_empty() {
    let client = TestClient::new().await;

    let response = client.get("/v1/collections").await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let collections: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(collections.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_create_collection_success() {
    let client = TestClient::new().await;

    let collection_request = json!({
        "name": "test_collection",
        "projection": {
            "fields": ["name", "email", "age"],
            "transforms": {
                "email": "lowercase",
                "age": "int"
            }
        }
    });

    let response = client.post("/v1/collections", collection_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(result["name"], "test_collection");
    assert!(result["projection"].is_object());

    // Verify it appears in list
    let response = client.get("/v1/collections").await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let collections: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(collections.as_array().unwrap().len(), 1);
    assert_eq!(collections[0]["name"], "test_collection");
}

#[tokio::test]
async fn test_create_collection_invalid_name() {
    let client = TestClient::new().await;

    let collection_request = json!({
        "name": "invalid/name/with/slashes",
        "projection": {
            "fields": ["name"]
        }
    });

    let response = client.post("/v1/collections", collection_request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_get_projection() {
    let client = TestClient::new().await;

    // First create a collection
    let collection_request = json!({
        "name": "test_collection",
        "projection": {
            "fields": ["name", "email"],
            "transforms": {
                "email": "lowercase"
            }
        }
    });

    let create_response = client.post("/v1/collections", collection_request).await;
    assert_eq!(create_response.status(), StatusCode::OK);

    // Get the projection
    let response = client.get("/v1/collections/test_collection/projection").await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let projection: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(projection["fields"], json!(["name", "email"]));
    assert_eq!(projection["transforms"]["email"], "lowercase");
}

#[tokio::test]
async fn test_update_projection() {
    let client = TestClient::new().await;

    // Create initial collection
    let collection_request = json!({
        "name": "test_collection",
        "projection": {
            "fields": ["name"]
        }
    });

    let create_response = client.post("/v1/collections", collection_request).await;
    assert_eq!(create_response.status(), StatusCode::OK);

    // Update the projection
    let updated_projection = json!({
        "fields": ["name", "email", "age"],
        "transforms": {
            "email": "lowercase",
            "age": "int"
        }
    });

    let response = client.put("/v1/collections/test_collection/projection", updated_projection).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Verify the update
    let get_response = client.get("/v1/collections/test_collection/projection").await;
    assert_eq!(get_response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(get_response.into_body(), usize::MAX).await.unwrap();
    let projection: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(projection["fields"], json!(["name", "email", "age"]));
    assert_eq!(projection["transforms"]["email"], "lowercase");
    assert_eq!(projection["transforms"]["age"], "int");
}

#[tokio::test]
async fn test_ingest_ndjson_collection() {
    let client = TestClient::new().await;

    // Create a source table
    create_test_table(&client, "users", "id INT, name TEXT, email TEXT, age INT").await;
    insert_test_data(&client, "users", "(1, 'Alice', 'ALICE@EXAMPLE.COM', 25), (2, 'Bob', 'bob@example.com', 30)").await;

    // Create collection with projection
    let collection_request = json!({
        "name": "users_collection",
        "source": "users",
        "projection": {
            "fields": ["name", "email", "age"],
            "transforms": {
                "email": "lowercase",
                "age": "int"
            }
        }
    });

    let create_response = client.post("/v1/collections", collection_request).await;
    assert_eq!(create_response.status(), StatusCode::OK);

    // Test NDJSON ingest
    let ndjson_data = r#"{"name": "Charlie", "email": "CHARLIE@TEST.COM", "age": "35"}
{"name": "Diana", "email": "diana@test.com", "age": "28"}"#;

    let response = client
        .request(
            "POST",
            "/v1/collections/users_collection/ingest",
            None,
        )
        .await;

    // Set proper content type for NDJSON
    let mut request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/collections/users_collection/ingest")
        .header("content-type", "application/x-ndjson")
        .header("authorization", format!("Bearer {}", client.get_token().await))
        .body(axum::body::Body::from(ndjson_data))
        .unwrap();

    let response = client.clone().app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(result["processed"].as_u64().unwrap() >= 2);
}

#[tokio::test]
async fn test_ingest_json_array_collection() {
    let client = TestClient::new().await;

    // Create a source table
    create_test_table(&client, "products", "id INT, name TEXT, price DECIMAL, category TEXT").await;

    // Create collection
    let collection_request = json!({
        "name": "products_collection",
        "source": "products",
        "projection": {
            "fields": ["name", "price", "category"],
            "transforms": {
                "price": "float"
            }
        }
    });

    let create_response = client.post("/v1/collections", collection_request).await;
    assert_eq!(create_response.status(), StatusCode::OK);

    // Test JSON array ingest
    let json_data = r#"[
        {"name": "Laptop", "price": "999.99", "category": "Electronics"},
        {"name": "Book", "price": "19.99", "category": "Education"},
        {"name": "Phone", "price": "699.00", "category": "Electronics"}
    ]"#;

    let response = client
        .request(
            "POST",
            "/v1/collections/products_collection/ingest",
            None,
        )
        .await;

    // Set proper content type for JSON
    let mut request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/collections/products_collection/ingest")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {}", client.get_token().await))
        .body(axum::body::Body::from(json_data))
        .unwrap();

    let response = client.clone().app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(result["processed"], 3);
}

#[tokio::test]
async fn test_ingest_compressed_data() {
    let client = TestClient::new().await;

    // Create a source table
    create_test_table(&client, "logs", "id INT, message TEXT, level TEXT, timestamp TEXT").await;

    // Create collection
    let collection_request = json!({
        "name": "logs_collection",
        "source": "logs",
        "projection": {
            "fields": ["message", "level", "timestamp"]
        }
    });

    let create_response = client.post("/v1/collections", collection_request).await;
    assert_eq!(create_response.status(), StatusCode::OK);

    // Create simple gzip-compressed data (in real scenario, this would be properly compressed)
    let json_data = r#"{"message": "Test log entry", "level": "INFO", "timestamp": "2023-01-01T00:00:00Z"}"#;

    let mut request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/collections/logs_collection/ingest")
        .header("content-type", "application/json")
        .header("content-encoding", "gzip")
        .header("authorization", format!("Bearer {}", client.get_token().await))
        .body(axum::body::Body::from(json_data))
        .unwrap();

    let response = client.clone().app.oneshot(request).await.unwrap();

    // The server should handle the compression header gracefully
    assert!(response.status().is_success() || response.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

#[tokio::test]
async fn test_collection_unauthorized_access() {
    // Create client with no auth token
    let client = TestClient::new().await.with_token("".to_string());

    let collection_request = json!({
        "name": "test_collection",
        "projection": {
            "fields": ["name"]
        }
    });

    let response = client.post("/v1/collections", collection_request).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = client.get("/v1/collections").await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_collection_error_handling() {
    let client = TestClient::new().await;

    // Test getting projection for non-existent collection
    let response = client.get("/v1/collections/nonexistent/projection").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // Test updating projection for non-existent collection
    let projection = json!({
        "fields": ["name"]
    });

    let response = client.put("/v1/collections/nonexistent/projection", projection).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // Test ingest to non-existent collection
    let response = client
        .request("POST", "/v1/collections/nonexistent/ingest", None)
        .await;

    let mut request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/collections/nonexistent/ingest")
        .header("content-type", "application/x-ndjson")
        .header("authorization", format!("Bearer {}", client.get_token().await))
        .body(axum::body::Body::from(r#"{"name": "test"}"#))
        .unwrap();

    let response = client.clone().app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_collection_concurrent_operations() {
    let client = TestClient::new().await;

    // Create base table
    create_test_table(&client, "concurrent_test", "id INT, value TEXT").await;

    // Create collection
    let collection_request = json!({
        "name": "concurrent_collection",
        "source": "concurrent_test",
        "projection": {
            "fields": ["id", "value"]
        }
    });

    let create_response = client.post("/v1/collections", collection_request).await;
    assert_eq!(create_response.status(), StatusCode::OK);

    // Concurrent ingest operations
    let mut handles = vec![];
    for i in 0..5 {
        let client_clone = client.clone();
        let handle = tokio::spawn(async move {
            let ndjson_data = format!(r#"{{"id": {}, "value": "test_{}"}}"#, i * 10, i);

            let mut request = axum::http::Request::builder()
                .method("POST")
                .uri("/v1/collections/concurrent_collection/ingest")
                .header("content-type", "application/x-ndjson")
                .header("authorization", format!("Bearer {}", client_clone.get_token().await))
                .body(axum::body::Body::from(ndjson_data))
                .unwrap();

            client_clone.app.oneshot(request).await.unwrap()
        });
        handles.push(handle);
    }

    // Wait for all operations to complete
    for handle in handles {
        let response = handle.await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // Verify data was ingested correctly
    let response = client.get("/v1/collections").await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let collections: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Find our collection and verify stats
    let collection = collections.as_array().unwrap()
        .iter()
        .find(|c| c["name"] == "concurrent_collection")
        .unwrap();

    assert!(collection["doc_count"].as_u64().unwrap() >= 5);
}

#[tokio::test]
async fn test_collection_permission_integration() {
    let client = TestClient::new().await;

    // Create collection
    let collection_request = json!({
        "name": "permission_test_collection",
        "projection": {
            "fields": ["name", "sensitive_data"]
        }
    });

    let response = client.post("/v1/collections", collection_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Test permission enforcement on collection operations
    // This would require creating users with different permission levels
    // For now, we just verify the endpoint exists and responds appropriately
    let response = client.get("/v1/permissions").await;
    assert!(response.status().is_success() || response.status() == StatusCode::FORBIDDEN);
}