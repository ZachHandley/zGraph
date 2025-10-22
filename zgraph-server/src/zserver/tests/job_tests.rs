mod common;

use axum::http::StatusCode;
use common::{wait_for_job_completion, TestClient};
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_submit_simple_job() {
    let client = TestClient::new().await;

    let job_request = json!({
        "name": "test_job",
        "command": ["echo", "Hello, World!"],
        "timeout_secs": 30
    });

    let response = client.post("/v1/jobs", job_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let job_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(job_response["id"].is_string());
    assert_eq!(job_response["name"], "test_job");
    assert_eq!(job_response["state"], "pending");
    assert!(job_response["created_at"].is_string());

    let job_id = job_response["id"].as_str().unwrap();

    // Wait for job to complete
    let job_info = wait_for_job_completion(&client, job_id, 10).await;
    assert_eq!(job_info["state"], "completed");
    assert_eq!(job_info["exit_code"], 0);
}

#[tokio::test]
async fn test_submit_job_with_environment() {
    let client = TestClient::new().await;

    let job_request = json!({
        "name": "env_test_job",
        "command": ["sh", "-c", "echo $TEST_VAR && echo $ANOTHER_VAR"],
        "environment": {
            "TEST_VAR": "test_value",
            "ANOTHER_VAR": "another_value"
        },
        "timeout_secs": 30
    });

    let response = client.post("/v1/jobs", job_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let job_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let job_id = job_response["id"].as_str().unwrap();

    // Wait for job to complete
    let job_info = wait_for_job_completion(&client, job_id, 10).await;
    assert_eq!(job_info["state"], "completed");

    // Check logs to verify environment variables were set
    let logs_response = client.get(&format!("/v1/jobs/{}/logs", job_id)).await;
    assert_eq!(logs_response.status(), StatusCode::OK);

    let logs_body = axum::body::to_bytes(logs_response.into_body(), usize::MAX).await.unwrap();
    let logs: serde_json::Value = serde_json::from_slice(&logs_body).unwrap();

    let log_content = logs["logs"].as_array().unwrap()
        .iter()
        .map(|log| log["message"].as_str().unwrap())
        .collect::<Vec<_>>()
        .join("");

    assert!(log_content.contains("test_value"));
    assert!(log_content.contains("another_value"));
}

#[tokio::test]
async fn test_submit_job_with_artifacts() {
    let client = TestClient::new().await;

    let job_request = json!({
        "name": "artifact_test_job",
        "command": ["sh", "-c", "echo 'artifact content' > test_artifact.txt && mkdir -p subdir && echo 'nested content' > subdir/nested.txt"],
        "timeout_secs": 30,
        "artifacts": ["test_artifact.txt", "subdir/"]
    });

    let response = client.post("/v1/jobs", job_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let job_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let job_id = job_response["id"].as_str().unwrap();

    // Wait for job to complete
    let job_info = wait_for_job_completion(&client, job_id, 10).await;
    assert_eq!(job_info["state"], "completed");

    // List artifacts
    let artifacts_response = client.get(&format!("/v1/jobs/{}/artifacts", job_id)).await;
    assert_eq!(artifacts_response.status(), StatusCode::OK);

    let artifacts_body = axum::body::to_bytes(artifacts_response.into_body(), usize::MAX).await.unwrap();
    let artifacts: serde_json::Value = serde_json::from_slice(&artifacts_body).unwrap();

    let artifact_names: Vec<String> = artifacts["artifacts"].as_array().unwrap()
        .iter()
        .map(|a| a["name"].as_str().unwrap().to_string())
        .collect();

    assert!(artifact_names.contains(&"test_artifact.txt".to_string()));
    assert!(artifact_names.iter().any(|name| name.contains("nested.txt")));

    // Download and verify artifact content
    if artifact_names.contains(&"test_artifact.txt".to_string()) {
        let download_response = client.get(&format!("/v1/jobs/{}/artifacts/test_artifact.txt", job_id)).await;
        assert_eq!(download_response.status(), StatusCode::OK);

        let content = axum::body::to_bytes(download_response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(std::str::from_utf8(&content).unwrap(), "artifact content\n");
    }
}

#[tokio::test]
async fn test_submit_long_running_job() {
    let client = TestClient::new().await;

    let job_request = json!({
        "name": "sleep_job",
        "command": ["sleep", "2"],
        "timeout_secs": 10
    });

    let response = client.post("/v1/jobs", job_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let job_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let job_id = job_response["id"].as_str().unwrap();

    // Check job status while running
    sleep(Duration::from_secs(1)).await;

    let status_response = client.get(&format!("/v1/jobs/{}", job_id)).await;
    assert_eq!(status_response.status(), StatusCode::OK);

    let status_body = axum::body::to_bytes(status_response.into_body(), usize::MAX).await.unwrap();
    let job_status: serde_json::Value = serde_json::from_slice(&status_body).unwrap();

    assert_eq!(job_status["state"], "running");

    // Wait for completion
    let job_info = wait_for_job_completion(&client, job_id, 10).await;
    assert_eq!(job_info["state"], "completed");
}

#[tokio::test]
async fn test_cancel_running_job() {
    let client = TestClient::new().await;

    let job_request = json!({
        "name": "cancelable_job",
        "command": ["sleep", "10"],
        "timeout_secs": 30
    });

    let response = client.post("/v1/jobs", job_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let job_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let job_id = job_response["id"].as_str().unwrap();

    // Wait a bit for job to start
    sleep(Duration::from_millis(500)).await;

    // Cancel the job
    let cancel_response = client.delete(&format!("/v1/jobs/{}", job_id)).await;
    assert_eq!(cancel_response.status(), StatusCode::OK);

    let cancel_body = axum::body::to_bytes(cancel_response.into_body(), usize::MAX).await.unwrap();
    let cancel_result: serde_json::Value = serde_json::from_slice(&cancel_body).unwrap();

    assert!(cancel_result["cancelled"].as_bool().unwrap());

    // Verify job is cancelled
    let status_response = client.get(&format!("/v1/jobs/{}", job_id)).await;
    assert_eq!(status_response.status(), StatusCode::OK);

    let status_body = axum::body::to_bytes(status_response.into_body(), usize::MAX).await.unwrap();
    let job_status: serde_json::Value = serde_json::from_slice(&status_body).unwrap();

    assert_eq!(job_status["state"], "cancelled");
}

#[tokio::test]
async fn test_job_timeout() {
    let client = TestClient::new().await;

    let job_request = json!({
        "name": "timeout_job",
        "command": ["sleep", "10"],
        "timeout_secs": 2
    });

    let response = client.post("/v1/jobs", job_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let job_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let job_id = job_response["id"].as_str().unwrap();

    // Wait for job to timeout
    let job_info = wait_for_job_completion(&client, job_id, 15).await;
    assert_eq!(job_info["state"], "failed");
    assert!(job_info["error"].as_str().unwrap().contains("timeout"));
}

#[tokio::test]
async fn test_failing_job() {
    let client = TestClient::new().await;

    let job_request = json!({
        "name": "failing_job",
        "command": ["false"], // Command that always fails
        "timeout_secs": 5
    });

    let response = client.post("/v1/jobs", job_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let job_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let job_id = job_response["id"].as_str().unwrap();

    // Wait for job to fail
    let job_info = wait_for_job_completion(&client, job_id, 10).await;
    assert_eq!(job_info["state"], "failed");
    assert_eq!(job_info["exit_code"], Some(json!(1)));
}

#[tokio::test]
async fn test_list_jobs() {
    let client = TestClient::new().await;

    // Submit multiple jobs
    let mut job_ids = Vec::new();
    for i in 0..3 {
        let job_request = json!({
            "name": format!("batch_job_{}", i),
            "command": ["echo", format!("Job {}", i)],
            "timeout_secs": 5
        });

        let response = client.post("/v1/jobs", job_request).await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let job_response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        job_ids.push(job_response["id"].as_str().unwrap().to_string());
    }

    // Wait for all jobs to complete
    for job_id in &job_ids {
        wait_for_job_completion(&client, job_id, 10).await;
    }

    // List all jobs
    let list_response = client.get("/v1/jobs").await;
    assert_eq!(list_response.status(), StatusCode::OK);

    let list_body = axum::body::to_bytes(list_response.into_body(), usize::MAX).await.unwrap();
    let jobs_list: serde_json::Value = serde_json::from_slice(&list_body).unwrap();

    let jobs = jobs_list["jobs"].as_array().unwrap();
    assert!(jobs.len() >= 3);

    // Verify our jobs are in the list
    let our_jobs: Vec<&serde_json::Value> = jobs
        .iter()
        .filter(|job| {
            let name = job["name"].as_str().unwrap_or("");
            name.starts_with("batch_job_")
        })
        .collect();

    assert_eq!(our_jobs.len(), 3);
}

#[tokio::test]
async fn test_job_logs_pagination() {
    let client = TestClient::new().await;

    // Submit a job that produces many log lines
    let job_request = json!({
        "name": "logging_job",
        "command": ["sh", "-c", "for i in $(seq 1 50); do echo \"Log line $i with some content\"; done"],
        "timeout_secs": 10
    });

    let response = client.post("/v1/jobs", job_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let job_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let job_id = job_response["id"].as_str().unwrap();

    // Wait for job to complete
    wait_for_job_completion(&client, job_id, 15).await;

    // Get logs with pagination
    let logs_response = client.get(&format!("/v1/jobs/{}/logs?limit=10&offset=0", job_id)).await;
    assert_eq!(logs_response.status(), StatusCode::OK);

    let logs_body = axum::body::to_bytes(logs_response.into_body(), usize::MAX).await.unwrap();
    let logs: serde_json::Value = serde_json::from_slice(&logs_body).unwrap();

    assert_eq!(logs["logs"].as_array().unwrap().len(), 10);
    assert!(logs["has_more"].as_bool().unwrap());

    // Get next page
    let logs_response2 = client.get(&format!("/v1/jobs/{}/logs?limit=10&offset=10", job_id)).await;
    assert_eq!(logs_response2.status(), StatusCode::OK);

    let logs_body2 = axum::body::to_bytes(logs_response2.into_body(), usize::MAX).await.unwrap();
    let logs2: serde_json::Value = serde_json::from_slice(&logs_body2).unwrap();

    assert_eq!(logs2["logs"].as_array().unwrap().len(), 10);
}

#[tokio::test]
async fn test_job_concurrent_execution() {
    let client = TestClient::new().await;

    // Submit multiple jobs concurrently
    let mut handles = vec![];
    for i in 0..5 {
        let client_clone = client.clone();
        let handle = tokio::spawn(async move {
            let job_request = json!({
                "name": format!("concurrent_job_{}", i),
                "command": ["sh", "-c", format!("echo 'Job {} completed' && sleep 1", i)],
                "timeout_secs": 10
            });

            let response = client_clone.post("/v1/jobs", job_request).await;
            assert_eq!(response.status(), StatusCode::OK);

            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let job_response: serde_json::Value = serde_json::from_slice(&body).unwrap();
            job_response["id"].as_str().unwrap().to_string()
        });
        handles.push(handle);
    }

    // Collect all job IDs
    let mut job_ids = Vec::new();
    for handle in handles {
        job_ids.push(handle.await.unwrap());
    }

    // Wait for all jobs to complete
    for job_id in job_ids {
        let job_info = wait_for_job_completion(&client, &job_id, 15).await;
        assert_eq!(job_info["state"], "completed");
    }
}

#[tokio::test]
async fn test_job_unauthorized_access() {
    // Client with no auth token
    let client = TestClient::new().await.with_token("".to_string());

    let job_request = json!({
        "name": "unauthorized_job",
        "command": ["echo", "test"],
        "timeout_secs": 5
    });

    let response = client.post("/v1/jobs", job_request).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = client.get("/v1/jobs").await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_job_error_handling() {
    let client = TestClient::new().await;

    // Test invalid job request (missing required fields)
    let invalid_job = json!({
        "name": "invalid_job"
        // Missing command field
    });

    let response = client.post("/v1/jobs", invalid_job).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Test getting non-existent job
    let response = client.get("/v1/jobs/nonexistent-job-id").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // Test cancelling non-existent job
    let response = client.delete("/v1/jobs/nonexistent-job-id").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // Test getting logs for non-existent job
    let response = client.get("/v1/jobs/nonexistent-job-id/logs").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}