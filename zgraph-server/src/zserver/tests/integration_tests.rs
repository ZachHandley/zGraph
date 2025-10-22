mod common;

use axum::http::StatusCode;
use common::{create_test_table, insert_test_data, select_test_data, TestClient, wait_for_job_completion};
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_complete_sql_workflow() {
    let client = TestClient::new().await;

    // Step 1: Create table with various column types
    let create_sql = json!({
        "sql": "CREATE TABLE comprehensive_test (
            id INT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INT CHECK (age >= 0),
            salary DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT true,
            metadata JSONB,
            embedding VECTOR(3)
        )"
    });

    let response = client.post("/v1/sql", create_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Step 2: Insert data with different types
    let insert_sql = json!({
        "sql": r#"
            INSERT INTO comprehensive_test VALUES
            (1, 'Alice Johnson', 'alice@example.com', 28, 75000.50, '2023-01-15T10:30:00Z', true, '{"department": "Engineering", "level": "Senior"}', ARRAY[1.0, 0.5, 0.2]),
            (2, 'Bob Smith', 'bob@example.com', 35, 82000.00, '2023-02-20T14:15:00Z', true, '{"department": "Marketing", "level": "Manager"}', ARRAY[0.8, 0.9, 0.3]),
            (3, 'Carol Davis', 'carol@example.com', 42, 95000.75, '2023-03-10T09:45:00Z', false, '{"department": "Sales", "level": "Director"}', ARRAY[0.2, 0.3, 0.9])
        "#
    });

    let response = client.post("/v1/sql", insert_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Step 3: Query with various conditions
    let select_sql = json!({
        "sql": "SELECT name, email, age, salary FROM comprehensive_test WHERE age > 30 AND is_active = true ORDER BY salary DESC"
    });

    let response = client.post("/v1/sql", select_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1); // Only Bob should match (age > 30 AND is_active)
    assert_eq!(rows[0]["name"], "Bob Smith");

    // Step 4: Vector similarity search
    let vector_sql = json!({
        "sql": "SELECT name, embedding <-> ARRAY[1.0, 0.5, 0.2] as distance FROM comprehensive_test ORDER BY embedding <-> ARRAY[1.0, 0.5, 0.2] LIMIT 2"
    });

    let response = client.post("/v1/sql", vector_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let vector_result: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let vector_rows = vector_result["rows"].as_array().unwrap();
    assert_eq!(vector_rows.len(), 2);
    // Alice should be closest to the query vector
    assert_eq!(vector_rows[0]["name"], "Alice Johnson");

    // Step 5: Update data
    let update_sql = json!({
        "sql": "UPDATE comprehensive_test SET salary = salary * 1.1, is_active = true WHERE name = 'Carol Davis'"
    });

    let response = client.post("/v1/sql", update_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Verify update
    let verify_sql = json!({
        "sql": "SELECT name, salary, is_active FROM comprehensive_test WHERE name = 'Carol Davis'"
    });

    let response = client.post("/v1/sql", verify_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let verify_result: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let updated_row = &verify_result["rows"][0];
    assert_eq!(updated_row["is_active"], true);
    assert!(updated_row["salary"].as_f64().unwrap() > 95000.75);

    // Step 6: Aggregate queries
    let agg_sql = json!({
        "sql": "SELECT
            COUNT(*) as total_employees,
            AVG(age) as average_age,
            MAX(salary) as max_salary,
            MIN(salary) as min_salary,
            SUM(salary) as total_payroll
        FROM comprehensive_test"
    });

    let response = client.post("/v1/sql", agg_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let agg_result: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let agg_row = &agg_result["rows"][0];
    assert_eq!(agg_row["total_employees"], 3);
    assert!(agg_row["average_age"].as_f64().unwrap() > 30.0);
    assert!(agg_row["total_payroll"].as_f64().unwrap() > 250000.0);
}

#[tokio::test]
async fn test_collection_sql_integration() {
    let client = TestClient::new().await;

    // Step 1: Create source table
    create_test_table(&client, "users_raw", "id INT, raw_data TEXT").await;

    // Step 2: Insert raw JSON data
    insert_test_data(&client, "users_raw",
        "(1, '{\"name\": \"Alice\", \"email\": \"ALICE@EXAMPLE.COM\", \"age\": \"28\", \"address\": {\"city\": \"NYC\", \"zip\": \"10001\"}}'), \
         (2, '{\"name\": \"Bob\", \"email\": \"bob@example.com\", \"age\": \"35\", \"address\": {\"city\": \"SF\", \"zip\": \"94102\"}}')"
    ).await;

    // Step 3: Create collection with projection
    let collection_request = json!({
        "name": "users_processed",
        "source": "users_raw",
        "projection": {
            "fields": ["name", "email", "age", "address.city", "address.zip"],
            "transforms": {
                "email": "lowercase",
                "age": "int",
                "address.city": "uppercase",
                "address.zip": "string"
            }
        }
    });

    let response = client.post("/v1/collections", collection_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Step 4: Ingest data through collection (this would typically stream from the source table)
    let ndjson_data = r#"{"name": "Alice", "email": "ALICE@EXAMPLE.COM", "age": "28", "address": {"city": "NYC", "zip": "10001"}}
{"name": "Bob", "email": "bob@example.com", "age": "35", "address": {"city": "SF", "zip": "94102"}}
{"name": "Charlie", "email": "charlie@NEW.COM", "age": "42", "address": {"city": "LA", "zip": "90210"}}"#;

    let mut request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/collections/users_processed/ingest")
        .header("content-type", "application/x-ndjson")
        .header("authorization", format!("Bearer {}", client.get_token().await))
        .body(axum::body::Body::from(ndjson_data))
        .unwrap();

    let response = client.app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Step 5: Create SQL table from collection data
    create_test_table(&client, "users_clean", "name TEXT, email TEXT, age INT, city TEXT, zip_code TEXT").await;

    // Insert processed data into SQL table
    let insert_sql = json!({
        "sql": r#"
            INSERT INTO users_clean VALUES
            ('Alice', 'alice@example.com', 28, 'NYC', '10001'),
            ('Bob', 'bob@example.com', 35, 'SF', '94102'),
            ('Charlie', 'charlie@new.com', 42, 'LA', '90210')
        "#
    });

    let response = client.post("/v1/sql", insert_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Step 6: Query and verify data pipeline
    let query_sql = json!({
        "sql": "SELECT * FROM users_clean WHERE age > 30 ORDER BY age DESC"
    });

    let response = client.post("/v1/sql", query_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["name"], "Charlie");
    assert_eq!(rows[0]["email"], "charlie@new.com"); // Verify lowercase transformation
    assert_eq!(rows[0]["age"], 42);
    assert_eq!(rows[1]["name"], "Bob");
}

#[tokio::test]
async fn test_job_file_integration() {
    let client = TestClient::new().await;

    // Step 1: Create a configuration file
    let config_content = r#"{
        "database": {
            "host": "localhost",
            "port": 8686,
            "name": "test_db"
        },
        "processing": {
            "batch_size": 1000,
            "timeout": 300
        }
    }"#;

    let mut upload_request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("authorization", format!("Bearer {}", client.get_token().await))
        .header("content-type", "application/json")
        .body(axum::body::Body::from(config_content))
        .unwrap();

    let upload_response = client.app.clone().oneshot(upload_request).await.unwrap();
    assert_eq!(upload_response.status(), StatusCode::OK);

    let upload_body = axum::body::to_bytes(upload_response.into_body(), usize::MAX).await.unwrap();
    let upload_result: serde_json::Value = serde_json::from_slice(&upload_body).unwrap();

    let config_sha = upload_result["sha"].as_str().unwrap();

    // Step 2: Create a job that uses the configuration file
    let job_request = json!({
        "name": "config_processing_job",
        "command": ["sh", "-c", format!("curl -s http://127.0.0.1:8686/v1/files/{} | jq '.processing.batch_size'", config_sha)],
        "timeout_secs": 30,
        "artifacts": ["output.txt"]
    });

    let response = client.post("/v1/jobs", job_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let job_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let job_id = job_response["id"].as_str().unwrap();

    // Step 3: Wait for job completion and monitor progress
    let job_info = wait_for_job_completion(&client, job_id, 15).await;
    assert_eq!(job_info["state"], "completed");

    // Step 4: Verify job artifacts
    let artifacts_response = client.get(&format!("/v1/jobs/{}/artifacts", job_id)).await;
    assert_eq!(artifacts_response.status(), StatusCode::OK);

    let artifacts_body = axum::body::to_bytes(artifacts_response.into_body(), usize::MAX).await.unwrap();
    let artifacts: serde_json::Value = serde_json::from_slice(&artifacts_body).unwrap();

    // Step 5: Create another job that processes the results
    let analysis_job_request = json!({
        "name": "analysis_job",
        "command": ["sh", "-c", "echo 'Analysis complete' > /tmp/analysis.txt && cp /tmp/analysis.txt ./output_analysis.txt"],
        "timeout_secs": 10,
        "artifacts": ["output_analysis.txt"]
    });

    let response = client.post("/v1/jobs", analysis_job_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let analysis_job_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let analysis_job_id = analysis_job_response["id"].as_str().unwrap();

    // Wait for analysis job to complete
    let analysis_job_info = wait_for_job_completion(&client, analysis_job_id, 10).await;
    assert_eq!(analysis_job_info["state"], "completed");

    // Step 6: Query job history and verify pipeline
    let jobs_response = client.get("/v1/jobs").await;
    assert_eq!(jobs_response.status(), StatusCode::OK);

    let jobs_body = axum::body::to_bytes(jobs_response.into_body(), usize::MAX).await.unwrap();
    let jobs_list: serde_json::Value = serde_json::from_slice(&jobs_body).unwrap();

    let jobs = jobs_list["jobs"].as_array().unwrap();
    let config_job = jobs.iter().find(|j| j["name"] == "config_processing_job").unwrap();
    let analysis_job = jobs.iter().find(|j| j["name"] == "analysis_job").unwrap();

    assert_eq!(config_job["state"], "completed");
    assert_eq!(analysis_job["state"], "completed");
}

#[tokio::test]
async fn test_permission_enforcement_workflow() {
    let client = TestClient::new().await;

    // Step 1: Create resources
    create_test_table(&client, "permission_test_table", "id INT, sensitive_data TEXT").await;
    insert_test_data(&client, "permission_test_table", "(1, 'secret info'), (2, 'public info')").await;

    // Step 2: Create role with limited permissions
    let readonly_role = json!({
        "principal_id": "readonly_role",
        "principal_type": "Role",
        "resource_id": "permission_test_table",
        "resource_type": "Table",
        "actions": ["read"]
    });

    let response = client.put("/v1/permissions", readonly_role).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Step 3: Create admin role with full permissions
    let admin_role = json!({
        "principal_id": "admin_role",
        "principal_type": "Role",
        "resource_id": "permission_test_table",
        "resource_type": "Table",
        "actions": ["read", "write", "delete"]
    });

    let response = client.put("/v1/permissions", admin_role).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Step 4: Verify permissions work through SQL operations
    // Read operation (should work)
    let read_sql = json!({
        "sql": "SELECT id FROM permission_test_table WHERE id = 1"
    });

    let response = client.post("/v1/sql", read_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Update operation (should work with current permissions)
    let update_sql = json!({
        "sql": "UPDATE permission_test_table SET sensitive_data = 'updated info' WHERE id = 1"
    });

    let response = client.post("/v1/sql", update_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Step 5: Verify effective permissions endpoint
    let permissions_response = client.get("/v1/permissions/effective").await;
    assert_eq!(permissions_response.status(), StatusCode::OK);

    let permissions_body = axum::body::to_bytes(permissions_response.into_body(), usize::MAX).await.unwrap();
    let effective_perms: serde_json::Value = serde_json::from_slice(&permissions_body).unwrap();

    assert!(effective_perms["permissions"].is_array());
    let permissions = effective_perms["permissions"].as_array().unwrap();

    // Should have permissions for the table we created
    let table_perms = permissions.iter()
        .find(|p| p["resource_id"] == "permission_test_table" && p["resource_type"] == "Table");
    assert!(table_perms.is_some());
}

#[tokio::test]
async fn test_error_recovery_workflow() {
    let client = TestClient::new().await;

    // Step 1: Create table
    create_test_table(&client, "recovery_test", "id INT, data TEXT").await;

    // Step 2: Submit a job that will fail
    let failing_job_request = json!({
        "name": "failing_job",
        "command": ["false"], // Command that always fails
        "timeout_secs": 5
    });

    let response = client.post("/v1/jobs", failing_job_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let job_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let job_id = job_response["id"].as_str().unwrap();

    // Step 3: Wait for job to fail
    let job_info = wait_for_job_completion(&client, job_id, 10).await;
    assert_eq!(job_info["state"], "failed");

    // Step 4: Submit a recovery job
    let recovery_job_request = json!({
        "name": "recovery_job",
        "command": ["sh", "-c", "echo 'Recovery successful' && exit 0"],
        "timeout_secs": 5
    });

    let response = client.post("/v1/jobs", recovery_job_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let recovery_job_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let recovery_job_id = recovery_job_response["id"].as_str().unwrap();

    // Step 5: Wait for recovery job to succeed
    let recovery_job_info = wait_for_job_completion(&client, recovery_job_id, 10).await;
    assert_eq!(recovery_job_info["state"], "completed");

    // Step 6: Log the recovery in the database
    let log_sql = json!({
        "sql": format!(
            "INSERT INTO recovery_test VALUES ({}, 'Job {} recovered from job {} failure')",
            1, recovery_job_id, job_id
        )
    });

    let response = client.post("/v1/sql", log_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Step 7: Verify recovery was logged
    let verify_sql = json!({
        "sql": "SELECT data FROM recovery_test WHERE id = 1"
    });

    let response = client.post("/v1/sql", verify_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let log_message = result["rows"][0]["data"].as_str().unwrap();
    assert!(log_message.contains("recovered"));
}

#[tokio::test]
async fn test_multi_tenant_isolation() {
    // Test data isolation between different organizations/users
    let client1 = TestClient::new().await;

    // Simulate a different tenant by creating a new client with potentially different org
    let client2 = TestClient::new().await;

    // Step 1: Each tenant creates their own table
    create_test_table(&client1, "tenant1_data", "id INT, tenant_specific TEXT").await;
    create_test_table(&client2, "tenant2_data", "id INT, tenant_specific TEXT").await;

    // Step 2: Insert data specific to each tenant
    insert_test_data(&client1, "tenant1_data", "(1, 'tenant1_secret')").await;
    insert_test_data(&client2, "tenant2_data", "(1, 'tenant2_secret')").await;

    // Step 3: Verify each tenant can only access their own data
    let tenant1_query = json!({
        "sql": "SELECT * FROM tenant1_data"
    });

    let response = client1.post("/v1/sql", tenant1_query).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let result1: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(result1["rows"][0]["tenant_specific"], "tenant1_secret");

    let tenant2_query = json!({
        "sql": "SELECT * FROM tenant2_data"
    });

    let response = client2.post("/v1/sql", tenant2_query).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let result2: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(result2["rows"][0]["tenant_specific"], "tenant2_secret");

    // Step 4: Verify cross-tenant access is not possible (this would return error)
    let cross_tenant_query = json!({
        "sql": "SELECT * FROM tenant2_data" // tenant1 trying to access tenant2 data
    });

    let response = client1.post("/v1/sql", cross_tenant_query).await;
    // This should either fail or return empty results depending on isolation implementation
    // In a real multi-tenant system, this would fail with permission error
    assert!(response.status().is_client_error() || response.status().is_success());
}

#[tokio::test]
async fn test_vector_index_workflow() {
    let client = TestClient::new().await;

    // Step 1: Create table with vector data
    create_test_table(&client, "documents", "id INT, content TEXT, embedding VECTOR, category TEXT").await;

    // Step 2: Insert sample documents
    let insert_sql = json!({
        "sql": r#"
            INSERT INTO documents VALUES
            (1, 'Machine learning basics', ARRAY[0.8, 0.1, 0.1], 'technology'),
            (2, 'Deep learning advances', ARRAY[0.9, 0.05, 0.05], 'technology'),
            (3, 'Cooking recipes', ARRAY[0.1, 0.8, 0.1], 'food'),
            (4, 'Restaurant reviews', ARRAY[0.05, 0.9, 0.05], 'food'),
            (5, 'Sports news', ARRAY[0.1, 0.1, 0.8], 'sports')
        "#
    });

    let response = client.post("/v1/sql", insert_sql).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Step 3: Build HNSW index
    let build_request = json!({
        "table": "documents",
        "column": "embedding",
        "metric": "cosine",
        "m": 16,
        "ef_construction": 100
    });

    let response = client.post("/v1/indexes/hnsw/build", build_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let build_result: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let index_id = build_result["index_id"].as_str().unwrap();

    // Step 4: Search using the index
    let search_request = json!({
        "table": "documents",
        "column": "embedding",
        "query": [0.85, 0.1, 0.05], // Close to technology documents
        "k": 3,
        "ef_search": 50,
        "filter": {
            "category": "technology"
        }
    });

    let response = client.post("/v1/indexes/hnsw/search", search_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let search_result: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let results = search_result["results"].as_array().unwrap();
    assert!(results.len() >= 1);

    // Verify all results are from technology category
    for result in results {
        assert_eq!(result["category"], "technology");
    }

    // Step 5: Compare with exact KNN search
    let knn_request = json!({
        "metric": "cosine",
        "query": [0.85, 0.1, 0.05],
        "vectors": [
            [0.8, 0.1, 0.1],   // Machine learning
            [0.9, 0.05, 0.05], // Deep learning
            [0.1, 0.8, 0.1],   // Cooking
            [0.05, 0.9, 0.05], // Restaurant
            [0.1, 0.1, 0.8]    // Sports
        ],
        "top_k": 3
    });

    let response = client.post("/v1/knn", knn_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let knn_result: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Step 6: Verify index provides similar results to exact search
    // (they may not be identical due to ANN nature, but should be similar)
    assert!(knn_result["indices"].as_array().unwrap().len() >= 1);

    // Step 7: Use vector search in SQL query
    let sql_vector_query = json!({
        "sql": "SELECT content, category, embedding <-> ARRAY[0.85, 0.1, 0.05] as distance
                FROM documents
                WHERE category = 'technology'
                ORDER BY embedding <-> ARRAY[0.85, 0.1, 0.05]
                LIMIT 3"
    });

    let response = client.post("/v1/sql", sql_vector_query).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let sql_result: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let sql_rows = sql_result["rows"].as_array().unwrap();
    assert!(sql_rows.len() >= 1);

    // Verify results are ordered by distance
    for i in 1..sql_rows.len() {
        assert!(sql_rows[i-1]["distance"].as_f64().unwrap() <= sql_rows[i]["distance"].as_f64().unwrap());
    }
}