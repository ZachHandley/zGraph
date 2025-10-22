mod common;

use axum::http::StatusCode;
use common::{TestClient};
use serde_json::json;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

#[tokio::test]
async fn test_concurrent_sql_queries() {
    let client = TestClient::new().await;

    // Create test table
    let create_sql = json!({
        "sql": "CREATE TABLE concurrent_test (id INT, name TEXT, value DECIMAL)"
    });

    let response = client.post("/v1/sql", create_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Insert test data
    let insert_sql = json!({
        "sql": "INSERT INTO concurrent_test VALUES (1, 'test1', 10.5), (2, 'test2', 20.3), (3, 'test3', 15.7)"
    });

    let response = client.post("/v1/sql", insert_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Concurrent query execution
    let mut join_set = JoinSet::new();
    let num_concurrent = 50;

    let start_time = Instant::now();

    for i in 0..num_concurrent {
        let client_clone = client.clone();
        let query_num = i;
        join_set.spawn(async move {
            let sql = format!("SELECT * FROM concurrent_test WHERE id = {}", (query_num % 3) + 1);
            let request = json!({ "sql": sql });

            let response = client_clone.post("/v1/sql", request).await;
            assert_eq!(response.status(), StatusCode::OK);

            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let result: serde_json::Value = serde_json::from_slice(&body).unwrap();

            (query_num, result)
        });
    }

    let mut successful_queries = 0;
    let mut total_rows = 0;

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok((query_num, query_result)) => {
                successful_queries += 1;
                let rows = query_result["rows"].as_array().unwrap().len();
                total_rows += rows;
            }
            Err(e) => {
                panic!("Query failed: {}", e);
            }
        }
    }

    let duration = start_time.elapsed();
    println!("Concurrent SQL test: {} queries in {:?}", successful_queries, duration);
    println!("Average query time: {:?}", duration / successful_queries as u32);
    println!("Total rows returned: {}", total_rows);

    assert_eq!(successful_queries, num_concurrent);
    assert!(duration < Duration::from_secs(10)); // Should complete within 10 seconds
}

#[tokio::test]
async fn test_concurrent_file_uploads() {
    let client = TestClient::new().await;

    let mut join_set = JoinSet::new();
    let num_uploads = 20;
    let file_size = 1024 * 1024; // 1MB files

    let start_time = Instant::now();

    for i in 0..num_uploads {
        let client_clone = client.clone();
        join_set.spawn(async move {
            // Generate unique content for each file
            let content = vec![(i % 256) as u8; file_size];
            let expected_sha = format!("{:x}", sha2::Sha256::digest(&content));

            let mut request = axum::http::Request::builder()
                .method("POST")
                .uri("/v1/files")
                .header("authorization", format!("Bearer {}", client_clone.get_token().await))
                .header("content-type", "application/octet-stream")
                .body(axum::body::Body::from(content))
                .unwrap();

            let start = Instant::now();
            let response = client_clone.app.clone().oneshot(request).await.unwrap();
            let upload_duration = start.elapsed();

            assert_eq!(response.status(), StatusCode::OK);

            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let upload_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

            assert_eq!(upload_response["sha"], expected_sha);
            assert_eq!(upload_response["size"], file_size);

            (i, upload_duration)
        });
    }

    let mut successful_uploads = 0;
    let mut total_upload_time = Duration::new(0, 0);

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok((upload_num, upload_duration)) => {
                successful_uploads += 1;
                total_upload_time += upload_duration;
            }
            Err(e) => {
                panic!("Upload failed: {}", e);
            }
        }
    }

    let total_duration = start_time.elapsed();
    println!("Concurrent file upload test: {} uploads in {:?}", successful_uploads, total_duration);
    println!("Average upload time: {:?}", total_upload_time / successful_uploads as u32);
    println!("Total throughput: {:.2} MB/s", (successful_uploads as f64 * file_size as f64) / total_duration.as_secs_f64() / 1_048_576.0);

    assert_eq!(successful_uploads, num_uploads);
    assert!(total_duration < Duration::from_secs(30)); // Should complete within 30 seconds
}

#[tokio::test]
async fn test_concurrent_job_submissions() {
    let client = TestClient::new().await;

    let mut join_set = JoinSet::new();
    let num_jobs = 30;

    let start_time = Instant::now();

    for i in 0..num_jobs {
        let client_clone = client.clone();
        join_set.spawn(async move {
            let job_request = json!({
                "name": format!("load_test_job_{}", i),
                "command": ["sh", "-c", format!("echo 'Job {} completed' && sleep 0.1", i)],
                "timeout_secs": 10
            });

            let start = Instant::now();
            let response = client_clone.post("/v1/jobs", job_request).await;
            let submission_duration = start.elapsed();

            assert_eq!(response.status(), StatusCode::OK);

            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let job_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

            let job_id = job_response["id"].as_str().unwrap().to_string();

            // Wait for job to complete
            let mut attempts = 0;
            let max_attempts = 20; // 10 seconds max wait

            while attempts < max_attempts {
                let status_response = client_clone.get(&format!("/v1/jobs/{}", job_id)).await;
                if status_response.status() == StatusCode::OK {
                    let status_body = axum::body::to_bytes(status_response.into_body(), usize::MAX).await.unwrap();
                    let job_status: serde_json::Value = serde_json::from_slice(&status_body).unwrap();

                    let status = job_status["state"].as_str().unwrap_or("");
                    if status == "completed" || status == "failed" {
                        break;
                    }
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
                attempts += 1;
            }

            (i, submission_duration, attempts)
        });
    }

    let mut successful_jobs = 0;
    let mut total_submission_time = Duration::new(0, 0);
    let mut total_completion_time = Duration::new(0, 0);

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok((job_num, submission_duration, completion_attempts)) => {
                successful_jobs += 1;
                total_submission_time += submission_duration;
                total_completion_time += Duration::from_millis(completion_attempts * 500);
            }
            Err(e) => {
                panic!("Job failed: {}", e);
            }
        }
    }

    let total_duration = start_time.elapsed();
    println!("Concurrent job test: {} jobs in {:?}", successful_jobs, total_duration);
    println!("Average submission time: {:?}", total_submission_time / successful_jobs as u32);
    println!("Average completion time: {:?}", total_completion_time / successful_jobs as u32);

    assert_eq!(successful_jobs, num_jobs);
    assert!(total_duration < Duration::from_secs(30)); // Should complete within 30 seconds
}

#[tokio::test]
async fn test_mixed_workload_simulation() {
    let client = TestClient::new().await;

    // Setup test table
    let create_sql = json!({
        "sql": "CREATE TABLE mixed_workload_test (id INT, data TEXT, timestamp BIGINT)"
    });

    let response = client.post("/v1/sql", create_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    let mut join_set = JoinSet::new();
    let num_operations = 100;
    let start_time = Instant::now();

    // Mix of different operations
    for i in 0..num_operations {
        let client_clone = client.clone();
        let operation_type = i % 4; // 4 types of operations

        join_set.spawn(async move {
            match operation_type {
                0 => {
                    // SQL INSERT
                    let sql = json!({
                        "sql": format!("INSERT INTO mixed_workload_test VALUES ({}, 'data_{}', {})", i, i, chrono::Utc::now().timestamp())
                    });

                    let response = client_clone.post("/v1/sql", sql).await;
                    assert_eq!(response.status(), StatusCode::OK);
                    "sql_insert"
                }
                1 => {
                    // SQL SELECT
                    let sql = json!({
                        "sql": "SELECT COUNT(*) as count FROM mixed_workload_test"
                    });

                    let response = client_clone.post("/v1/sql", sql).await;
                    assert_eq!(response.status(), StatusCode::OK);
                    "sql_select"
                }
                2 => {
                    // File upload
                    let content = format!("Test file content {}", i).into_bytes();
                    let mut request = axum::http::Request::builder()
                        .method("POST")
                        .uri("/v1/files")
                        .header("authorization", format!("Bearer {}", client_clone.get_token().await))
                        .header("content-type", "text/plain")
                        .body(axum::body::Body::from(content))
                        .unwrap();

                    let response = client_clone.app.clone().oneshot(request).await.unwrap();
                    assert_eq!(response.status(), StatusCode::OK);
                    "file_upload"
                }
                3 => {
                    // Job submission
                    let job_request = json!({
                        "name": format!("mixed_job_{}", i),
                        "command": ["echo", format!("Operation {}", i)],
                        "timeout_secs": 5
                    });

                    let response = client_clone.post("/v1/jobs", job_request).await;
                    assert_eq!(response.status(), StatusCode::OK);

                    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
                    let job_response: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    let job_id = job_response["id"].as_str().unwrap().to_string();

                    // Quick wait for completion
                    for _ in 0..10 {
                        let status_response = client_clone.get(&format!("/v1/jobs/{}", job_id)).await;
                        if status_response.status() == StatusCode::OK {
                            let status_body = axum::body::to_bytes(status_response.into_body(), usize::MAX).await.unwrap();
                            let job_status: serde_json::Value = serde_json::from_slice(&status_body).unwrap();

                            if job_status["state"].as_str().unwrap_or("") == "completed" {
                                break;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    "job_submission"
                }
                _ => unreachable!()
            }
        });
    }

    let mut successful_operations = 0;
    let mut operation_counts = std::collections::HashMap::new();

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(operation_type) => {
                successful_operations += 1;
                *operation_counts.entry(operation_type).or_insert(0) += 1;
            }
            Err(e) => {
                panic!("Operation failed: {}", e);
            }
        }
    }

    let total_duration = start_time.elapsed();
    println!("Mixed workload test: {} operations in {:?}", successful_operations, total_duration);
    println!("Operation distribution: {:?}", operation_counts);
    println!("Average operation time: {:?}", total_duration / successful_operations as u32);

    assert_eq!(successful_operations, num_operations);
    assert!(total_duration < Duration::from_secs(60)); // Should complete within 60 seconds

    // Verify we have a good mix of operations
    assert!(operation_counts.len() >= 4);
    for count in operation_counts.values() {
        assert!(*count >= 10); // Each operation type should have at least 10 instances
    }
}

#[tokio::test]
async fn test_memory_usage_under_load() {
    let client = TestClient::new().await;

    // Create test table
    let create_sql = json!({
        "sql": "CREATE TABLE memory_test (id INT, data TEXT, vector VECTOR)"
    });

    let response = client.post("/v1/sql", create_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    let start_time = Instant::now();
    let num_operations = 500;

    // Perform a sequence of memory-intensive operations
    for i in 0..num_operations {
        // Insert larger data
        let large_data = "x".repeat(1000); // 1KB strings
        let vector_data = format!("ARRAY[{}, {}, {}, {}, {}]",
            i as f64 / 100.0,
            (i * 2) as f64 / 100.0,
            (i * 3) as f64 / 100.0,
            (i * 4) as f64 / 100.0,
            (i * 5) as f64 / 100.0);

        let insert_sql = json!({
            "sql": format!("INSERT INTO memory_test VALUES ({}, '{}', {})", i, large_data, vector_data)
        });

        let response = client.post("/v1/sql", insert_sql).await;
        assert_eq!(response.status(), StatusCode::OK);

        // Periodically query to force memory usage
        if i % 50 == 0 {
            let query_sql = json!({
                "sql": "SELECT COUNT(*) as count, AVG(LENGTH(data)) as avg_size FROM memory_test"
            });

            let response = client.post("/v1/sql", query_sql).await;
            assert_eq!(response.status(), StatusCode::OK);
        }

        // Give a small break to allow for cleanup
        if i % 100 == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    let duration = start_time.elapsed();
    println!("Memory usage test: {} operations in {:?}", num_operations, duration);
    println!("Average operation time: {:?}", duration / num_operations as u32);

    // Final verification
    let count_sql = json!({
        "sql": "SELECT COUNT(*) as total FROM memory_test"
    });

    let response = client.post("/v1/sql", count_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(result["rows"][0]["total"], num_operations);
    assert!(duration < Duration::from_secs(60)); // Should complete within 60 seconds
}

#[tokio::test]
async fn test_connection_pooling_simulation() {
    let client = TestClient::new().await;

    // Simulate many rapid connections
    let mut join_set = JoinSet::new();
    let num_connections = 200;
    let start_time = Instant::now();

    for i in 0..num_connections {
        let client_clone = client.clone();
        join_set.spawn(async move {
            // Simple health check to simulate connection
            let start = Instant::now();
            let response = client_clone.get("/healthz").await;
            let response_time = start.elapsed();

            assert!(response.status().is_success());

            // Also perform a simple SQL operation
            let sql = json!({ "sql": "SELECT 1 as test" });
            let sql_response = client_clone.post("/v1/sql", sql).await;
            assert_eq!(sql_response.status(), StatusCode::OK);

            (i, response_time)
        });
    }

    let mut successful_connections = 0;
    let mut total_response_time = Duration::new(0, 0);

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok((conn_num, response_time)) => {
                successful_connections += 1;
                total_response_time += response_time;
            }
            Err(e) => {
                panic!("Connection failed: {}", e);
            }
        }
    }

    let total_duration = start_time.elapsed();
    println!("Connection pooling test: {} connections in {:?}", successful_connections, total_duration);
    println!("Average response time: {:?}", total_response_time / successful_connections as u32);
    println!("Connections per second: {:.2}", successful_connections as f64 / total_duration.as_secs_f64());

    assert_eq!(successful_connections, num_connections);
    assert!(total_duration < Duration::from_secs(30)); // Should complete within 30 seconds
    assert!(total_response_time / successful_connections as u32 < Duration::from_millis(100)); // Average response under 100ms
}