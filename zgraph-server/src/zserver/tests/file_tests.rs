mod common;

use axum::http::StatusCode;
use common::TestClient;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::path::Path;

#[tokio::test]
async fn test_upload_file_success() {
    let client = TestClient::new().await;

    let test_content = b"Hello, World! This is a test file for upload.";
    let expected_sha = format!("{:x}", Sha256::digest(test_content));

    let mut request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("authorization", format!("Bearer {}", client.get_token().await))
        .header("content-type", "application/octet-stream")
        .body(axum::body::Body::from(test_content.as_ref()))
        .unwrap();

    let response = client.app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let upload_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(upload_response["sha"], expected_sha);
    assert_eq!(upload_response["size"], test_content.len());
    assert!(upload_response["uploaded_at"].is_string());
}

#[tokio::test]
async fn test_upload_json_file() {
    let client = TestClient::new().await;

    let json_content = r#"{"name": "test", "value": 42, "array": [1, 2, 3]}"#;
    let expected_sha = format!("{:x}", Sha256::digest(json_content.as_bytes()));

    let mut request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("authorization", format!("Bearer {}", client.get_token().await))
        .header("content-type", "application/json")
        .body(axum::body::Body::from(json_content))
        .unwrap();

    let response = client.app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let upload_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(upload_response["sha"], expected_sha);
    assert_eq!(upload_response["size"], json_content.len());
}

#[tokio::test]
async fn test_download_file_success() {
    let client = TestClient::new().await;

    // First upload a file
    let test_content = b"File content for download test";
    let expected_sha = format!("{:x}", Sha256::digest(test_content));

    let mut upload_request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("authorization", format!("Bearer {}", client.get_token().await))
        .header("content-type", "application/octet-stream")
        .body(axum::body::Body::from(test_content.as_ref()))
        .unwrap();

    let upload_response = client.app.clone().oneshot(upload_request).await.unwrap();
    assert_eq!(upload_response.status(), StatusCode::OK);

    // Now download the file
    let download_response = client.get(&format!("/v1/files/{}", expected_sha)).await;
    assert_eq!(download_response.status(), StatusCode::OK);

    let downloaded_content = axum::body::to_bytes(download_response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(downloaded_content.as_ref(), test_content);

    // Verify content type header
    assert_eq!(
        download_response.headers().get("content-type").unwrap(),
        "application/octet-stream"
    );
}

#[tokio::test]
async fn test_download_nonexistent_file() {
    let client = TestClient::new().await;

    let nonexistent_sha = "0000000000000000000000000000000000000000000000000000000000000000";
    let response = client.get(&format!("/v1/files/{}", nonexistent_sha)).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_file_deduplication() {
    let client = TestClient::new().await;

    let test_content = b"Duplicate content test";
    let expected_sha = format!("{:x}", Sha256::digest(test_content));

    // Upload the same content twice
    for _ in 0..2 {
        let mut request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/files")
            .header("authorization", format!("Bearer {}", client.get_token().await))
            .header("content-type", "application/octet-stream")
            .body(axum::body::Body::from(test_content.as_ref()))
            .unwrap();

        let response = client.app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let upload_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(upload_response["sha"], expected_sha);
        assert_eq!(upload_response["size"], test_content.len());
    }

    // Verify the file can still be downloaded
    let download_response = client.get(&format!("/v1/files/{}", expected_sha)).await;
    assert_eq!(download_response.status(), StatusCode::OK);

    let downloaded_content = axum::body::to_bytes(download_response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(downloaded_content.as_ref(), test_content);
}

#[tokio::test]
async fn test_upload_large_file() {
    let client = TestClient::new().await;

    // Create a larger test file (1MB)
    let large_content = vec![0u8; 1024 * 1024];
    let expected_sha = format!("{:x}", Sha256::digest(&large_content));

    let mut request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("authorization", format!("Bearer {}", client.get_token().await))
        .header("content-type", "application/octet-stream")
        .body(axum::body::Body::from(large_content))
        .unwrap();

    let response = client.app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let upload_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(upload_response["sha"], expected_sha);
    assert_eq!(upload_response["size"], 1024 * 1024);

    // Verify download works
    let download_response = client.get(&format!("/v1/files/{}", expected_sha)).await;
    assert_eq!(download_response.status(), StatusCode::OK);

    let downloaded_content = axum::body::to_bytes(download_response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(downloaded_content.len(), 1024 * 1024);
}

#[tokio::test]
async fn test_upload_empty_file() {
    let client = TestClient::new().await;

    let empty_content = b"";
    let expected_sha = format!("{:x}", Sha256::digest(empty_content));

    let mut request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("authorization", format!("Bearer {}", client.get_token().await))
        .header("content-type", "application/octet-stream")
        .body(axum::body::Body::from(empty_content.as_ref()))
        .unwrap();

    let response = client.app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let upload_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(upload_response["sha"], expected_sha);
    assert_eq!(upload_response["size"], 0);

    // Verify download of empty file
    let download_response = client.get(&format!("/v1/files/{}", expected_sha)).await;
    assert_eq!(download_response.status(), StatusCode::OK);

    let downloaded_content = axum::body::to_bytes(download_response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(downloaded_content.len(), 0);
}

#[tokio::test]
async fn test_file_unauthorized_access() {
    let client = TestClient::new().await.with_token("".to_string());

    // Test upload without auth
    let test_content = b"Unauthorized upload test";

    let mut request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("content-type", "application/octet-stream")
        .body(axum::body::Body::from(test_content.as_ref()))
        .unwrap();

    let response = client.app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // Test download without auth
    let any_sha = "0000000000000000000000000000000000000000000000000000000000000000";
    let response = client.get(&format!("/v1/files/{}", any_sha)).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_file_error_handling() {
    let client = TestClient::new().await;

    // Test invalid SHA format
    let invalid_sha = "invalid-sha-format";
    let response = client.get(&format!("/v1/files/{}", invalid_sha)).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Test download with valid but non-existent SHA
    let valid_nonexistent_sha = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let response = client.get(&format!("/v1/files/{}", valid_nonexistent_sha)).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_concurrent_file_uploads() {
    let client = TestClient::new().await;

    let mut handles = vec![];
    let mut file_shas = Vec::new();

    // Upload multiple files concurrently
    for i in 0..5 {
        let client_clone = client.clone();
        let handle = tokio::spawn(async move {
            let content = format!("Concurrent upload content {}", i).into_bytes();
            let expected_sha = format!("{:x}", Sha256::digest(&content));

            let mut request = axum::http::Request::builder()
                .method("POST")
                .uri("/v1/files")
                .header("authorization", format!("Bearer {}", client_clone.get_token().await))
                .header("content-type", "application/octet-stream")
                .body(axum::body::Body::from(content))
                .unwrap();

            let response = client_clone.app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);

            (expected_sha, response)
        });
        handles.push(handle);
    }

    // Wait for all uploads to complete and collect SHAs
    for handle in handles {
        let (sha, response) = handle.await.unwrap();
        file_shas.push(sha);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let upload_response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(upload_response["sha"], sha);
    }

    // Verify all files can be downloaded
    for sha in file_shas {
        let download_response = client.get(&format!("/v1/files/{}", sha)).await;
        assert_eq!(download_response.status(), StatusCode::OK);

        let _downloaded_content = axum::body::to_bytes(download_response.into_body(), usize::MAX).await.unwrap();
    }
}

#[tokio::test]
async fn test_file_content_type_handling() {
    let client = TestClient::new().await;

    // Test different content types
    let test_cases = vec![
        ("text/plain", "Plain text content"),
        ("application/json", r#"{"type": "json", "valid": true}"#),
        ("application/xml", r#"<?xml version="1.0"?><root><item>test</item></root>"#),
        ("text/csv", "name,age,city\nJohn,25,NYC\nJane,30,LA"),
    ];

    for (content_type, content) in test_cases {
        let expected_sha = format!("{:x}", Sha256::digest(content.as_bytes()));

        let mut request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/files")
            .header("authorization", format!("Bearer {}", client.get_token().await))
            .header("content-type", content_type)
            .body(axum::body::Body::from(content))
            .unwrap();

        let response = client.app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let upload_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(upload_response["sha"], expected_sha);

        // Verify download preserves content
        let download_response = client.get(&format!("/v1/files/{}", expected_sha)).await;
        assert_eq!(download_response.status(), StatusCode::OK);

        let downloaded_content = axum::body::to_bytes(download_response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            downloaded_content.headers().get("content-type").unwrap(),
            content_type
        );
    }
}

#[tokio::test]
async fn test_file_with_special_characters() {
    let client = TestClient::new().await;

    // Test content with special Unicode characters
    let special_content = "Special characters: áéíóú 中文 🎉 emoji\nTabs\tand\nnewlines\r\n";
    let expected_sha = format!("{:x}", Sha256::digest(special_content.as_bytes()));

    let mut request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("authorization", format!("Bearer {}", client.get_token().await))
        .header("content-type", "text/plain; charset=utf-8")
        .body(axum::body::Body::from(special_content))
        .unwrap();

    let response = client.app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let upload_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(upload_response["sha"], expected_sha);

    // Verify download preserves special characters
    let download_response = client.get(&format!("/v1/files/{}", expected_sha)).await;
    assert_eq!(download_response.status(), StatusCode::OK);

    let downloaded_content = axum::body::to_bytes(download_response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        std::str::from_utf8(&downloaded_content).unwrap(),
        special_content
    );
}