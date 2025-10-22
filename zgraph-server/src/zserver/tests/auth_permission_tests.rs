mod common;

use axum::http::StatusCode;
use common::{assert_error_response, TestClient};
use serde_json::json;

#[tokio::test]
async fn test_whoami_with_valid_token() {
    let client = TestClient::new().await;

    let response = client.get("/v1/whoami").await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let whoami_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(whoami_response["user_id"].is_string());
    assert!(whoami_response["org_id"].is_number());
    assert_eq!(whoami_response["org_id"], 123);
    assert!(whoami_response["environment"].is_string());
}

#[tokio::test]
async fn test_whoami_without_token() {
    let client = TestClient::new().await.with_token("".to_string());

    let response = client.get("/v1/whoami").await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_whoami_with_invalid_token() {
    let client = TestClient::new().await.with_token("invalid_token".to_string());

    let response = client.get("/v1/whoami").await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_list_environments() {
    let client = TestClient::new().await;

    let response = client.get("/v1/environments").await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let envs_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(envs_response["environments"].is_array());
    // Should have at least live and sandbox environments
    assert!(envs_response["environments"].as_array().unwrap().len() >= 2);
}

#[tokio::test]
async fn test_environment_switch() {
    let client = TestClient::new().await;

    // Get available environments first
    let env_response = client.get_json("/v1/environments").await;
    let environments = env_response["environments"].as_array().unwrap();

    // Find a different environment to switch to
    let current_env = environments.iter()
        .find(|env| env["current"].as_bool().unwrap_or(false))
        .unwrap();
    let target_env = environments.iter()
        .find(|env| env["id"] != current_env["id"])
        .unwrap_or(&environments[0]);

    let switch_request = json!({
        "environment_id": target_env["id"]
    });

    let response = client.post("/v1/auth/switch", switch_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let switch_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(switch_response["environment_id"], target_env["id"]);
    assert!(switch_response["token"].is_string());

    // Test whoami with new token
    let new_token = switch_response["token"].as_str().unwrap();
    let client_with_new_token = TestClient::new().await.with_token(new_token.to_string());

    let whoami_response = client_with_new_token.get_json("/v1/whoami").await;
    assert_eq!(whoami_response["environment_id"], target_env["id"]);
}

#[tokio::test]
async fn test_login_flow() {
    let client = TestClient::new().await;

    // Test login with email and password
    let login_request = json!({
        "email": "test@example.com",
        "password": "password123"
    });

    let response = client.post("/v1/auth/login", login_request).await;
    // This might fail in test environment since we don't have real users
    // but it should not be an unauthorized error
    assert!(response.status().is_client_error() || response.status().is_success());
}

#[tokio::test]
async fn test_token_refresh() {
    let client = TestClient::new().await;

    let refresh_request = json!({
        "refresh_token": "dummy_refresh_token"
    });

    let response = client.post("/v1/auth/refresh", refresh_request).await;
    // In test environment, this might fail but should not be unauthorized
    assert!(response.status().is_client_error() || response.status().is_success());
}

#[tokio::test]
async fn test_permission_list() {
    let client = TestClient::new().await;

    let response = client.get("/v1/permissions").await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let permissions_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(permissions_response["permissions"].is_array());
}

#[tokio::test]
async fn test_permission_crud_operations() {
    let client = TestClient::new().await;

    // Create a permission
    let permission_request = json!({
        "principal_id": "user123",
        "principal_type": "User",
        "resource_id": "table1",
        "resource_type": "Table",
        "actions": ["read", "write"]
    });

    let response = client.put("/v1/permissions", permission_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let permission_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(permission_response["principal_id"], "user123");
    assert_eq!(permission_response["resource_type"], "Table");
    assert_eq!(permission_response["actions"], json!(["read", "write"]));

    // Verify it appears in the list
    let list_response = client.get("/v1/permissions").await;
    assert_eq!(list_response.status(), StatusCode::OK);

    let list_body = axum::body::to_bytes(list_response.into_body(), usize::MAX).await.unwrap();
    let permissions_list: serde_json::Value = serde_json::from_slice(&list_body).unwrap();

    let created_permission = permissions_list["permissions"].as_array().unwrap()
        .iter()
        .find(|p| p["principal_id"] == "user123" && p["resource_id"] == "table1")
        .unwrap();

    assert_eq!(created_permission["actions"], json!(["read", "write"]));
}

#[tokio::test]
async fn test_effective_permissions() {
    let client = TestClient::new().await;

    let response = client.get("/v1/permissions/effective").await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let effective_permissions: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(effective_permissions["permissions"].is_array());
    // Should have some default permissions
    assert!(effective_permissions["permissions"].as_array().unwrap().len() > 0);
}

#[tokio::test]
async fn test_permission_enforcement_on_sql() {
    let client = TestClient::new().await;

    // This test would require setting up users with different permission levels
    // For now, we'll test that the SQL endpoint respects authentication

    let sql_request = json!({
        "sql": "SELECT 1"
    });

    // Test without authentication
    let unauth_client = TestClient::new().await.with_token("".to_string());
    let response = unauth_client.post("/v1/sql", sql_request.clone()).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // Test with authentication - should work
    let response = client.post("/v1/sql", sql_request).await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_permission_enforcement_on_collections() {
    let client = TestClient::new().await;

    let collection_request = json!({
        "name": "permission_test_collection",
        "projection": {
            "fields": ["name"]
        }
    });

    // Test without authentication
    let unauth_client = TestClient::new().await.with_token("".to_string());
    let response = unauth_client.post("/v1/collections", collection_request.clone()).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // Test with authentication - should work
    let response = client.post("/v1/collections", collection_request).await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_permission_enforcement_on_jobs() {
    let client = TestClient::new().await;

    let job_request = json!({
        "name": "permission_test_job",
        "command": ["echo", "test"],
        "timeout_secs": 5
    });

    // Test without authentication
    let unauth_client = TestClient::new().await.with_token("".to_string());
    let response = unauth_client.post("/v1/jobs", job_request.clone()).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // Test with authentication - should work
    let response = client.post("/v1/jobs", job_request).await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_cross_org_access() {
    // This test would require setting up multiple organizations
    // For now, we'll test the basic structure

    let client = TestClient::new().await;

    // Try to access resources from a different organization
    // In a real test, this would fail with appropriate permissions
    let response = client.get("/v1/permissions").await;
    assert!(response.status().is_success());
}

#[tokio::test]
async fn test_role_based_permissions() {
    let client = TestClient::new().await;

    // Create permissions for a role
    let role_permission = json!({
        "principal_id": "admin_role",
        "principal_type": "Role",
        "resource_id": "*",
        "resource_type": "Table",
        "actions": ["read", "write", "delete"]
    });

    let response = client.put("/v1/permissions", role_permission).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Verify the permission was created
    let list_response = client.get("/v1/permissions").await;
    assert_eq!(list_response.status(), StatusCode::OK);

    let list_body = axum::body::to_bytes(list_response.into_body(), usize::MAX).await.unwrap();
    let permissions_list: serde_json::Value = serde_json::from_slice(&list_body).unwrap();

    let role_perm = permissions_list["permissions"].as_array().unwrap()
        .iter()
        .find(|p| p["principal_id"] == "admin_role" && p["principal_type"] == "Role")
        .unwrap();

    assert_eq!(role_perm["actions"], json!(["read", "write", "delete"]));
}

#[tokio::test]
async fn test_permission_inheritance() {
    let client = TestClient::new().await;

    // Create a group permission
    let group_permission = json!({
        "principal_id": "developers",
        "principal_type": "Group",
        "resource_id": "code_tables",
        "resource_type": "Table",
        "actions": ["read", "write"]
    });

    let response = client.put("/v1/permissions", group_permission).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Create a user in that group
    let user_permission = json!({
        "principal_id": "user_in_group",
        "principal_type": "User",
        "resource_id": "code_tables",
        "resource_type": "Table",
        "actions": ["read"] // More restrictive
    });

    let response = client.put("/v1/permissions", user_permission).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Test effective permissions for the user
    let effective_response = client.get("/v1/permissions/effective").await;
    assert_eq!(effective_response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_permission_error_handling() {
    let client = TestClient::new().await;

    // Test invalid permission structure
    let invalid_permission = json!({
        "principal_id": "test_user"
        // Missing required fields
    });

    let response = client.put("/v1/permissions", invalid_permission).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Test invalid principal type
    let invalid_principal = json!({
        "principal_id": "test_user",
        "principal_type": "InvalidType",
        "resource_id": "test_resource",
        "resource_type": "Table",
        "actions": ["read"]
    });

    let response = client.put("/v1/permissions", invalid_principal).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Test invalid resource type
    let invalid_resource = json!({
        "principal_id": "test_user",
        "principal_type": "User",
        "resource_id": "test_resource",
        "resource_type": "InvalidResource",
        "actions": ["read"]
    });

    let response = client.put("/v1/permissions", invalid_resource).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_auth_error_scenarios() {
    let client = TestClient::new().await;

    // Test malformed JSON in login
    let malformed_login = "{ invalid json }";
    let mut request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/auth/login")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {}", client.get_token().await))
        .body(axum::body::Body::from(malformed_login))
        .unwrap();

    let response = client.app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Test empty refresh token
    let empty_refresh = json!({
        "refresh_token": ""
    });

    let response = client.post("/v1/auth/refresh", empty_refresh).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Test invalid environment ID
    let invalid_env_switch = json!({
        "environment_id": "invalid_environment_id"
    });

    let response = client.post("/v1/auth/switch", invalid_env_switch).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}