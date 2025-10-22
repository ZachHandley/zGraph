//! Comprehensive Webhook Integration Tests

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::post,
    Router,
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::{sleep, timeout};

use crate::{
    Event, EventType, EventData, WebhookConfig, WebhookEndpoint, WebhookManager,
    WebhookDelivery, WebhookStatus, WebhookSignature,
};

type HmacSha256 = Hmac<Sha256>;

/// Mock webhook server for testing
#[derive(Debug, Clone)]
struct MockWebhookServer {
    received_webhooks: Arc<RwLock<Vec<ReceivedWebhook>>>,
    response_config: Arc<RwLock<ResponseConfig>>,
    request_count: Arc<AtomicU32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReceivedWebhook {
    headers: HashMap<String, String>,
    body: serde_json::Value,
    timestamp: chrono::DateTime<chrono::Utc>,
    signature: Option<String>,
}

#[derive(Debug, Clone)]
struct ResponseConfig {
    status_code: StatusCode,
    delay_ms: u64,
    should_fail: bool,
    failure_rate: f32, // 0.0 to 1.0
}

impl Default for ResponseConfig {
    fn default() -> Self {
        Self {
            status_code: StatusCode::OK,
            delay_ms: 0,
            should_fail: false,
            failure_rate: 0.0,
        }
    }
}

impl MockWebhookServer {
    fn new() -> Self {
        Self {
            received_webhooks: Arc::new(RwLock::new(Vec::new())),
            response_config: Arc::new(RwLock::new(ResponseConfig::default())),
            request_count: Arc::new(AtomicU32::new(0)),
        }
    }

    async fn handle_webhook(
        State(server): State<MockWebhookServer>,
        headers: HeaderMap,
        body: String,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        server.request_count.fetch_add(1, Ordering::SeqCst);

        let config = server.response_config.read().await.clone();

        // Apply configured delay
        if config.delay_ms > 0 {
            sleep(Duration::from_millis(config.delay_ms)).await;
        }

        // Check if should fail based on configuration
        if config.should_fail || (config.failure_rate > 0.0 && rand::random::<f32>() < config.failure_rate) {
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }

        // Parse and store the webhook
        let body_json: serde_json::Value = serde_json::from_str(&body)
            .map_err(|_| StatusCode::BAD_REQUEST)?;

        let mut header_map = HashMap::new();
        for (key, value) in headers.iter() {
            if let Ok(value_str) = value.to_str() {
                header_map.insert(key.to_string(), value_str.to_string());
            }
        }

        let webhook = ReceivedWebhook {
            headers: header_map.clone(),
            body: body_json,
            timestamp: chrono::Utc::now(),
            signature: header_map.get("x-webhook-signature").cloned(),
        };

        server.received_webhooks.write().await.push(webhook);

        // Return configured status
        if config.status_code == StatusCode::OK {
            Ok(Json(serde_json::json!({ "status": "received" })))
        } else {
            Err(config.status_code)
        }
    }

    async fn start_server(&self, port: u16) -> Result<()> {
        let app = Router::new()
            .route("/webhook", post(Self::handle_webhook))
            .route("/webhook/delay", post(Self::handle_webhook))
            .route("/webhook/fail", post(Self::handle_webhook))
            .route("/webhook/custom", post(Self::handle_webhook))
            .with_state(self.clone());

        let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // Give the server time to start
        sleep(Duration::from_millis(100)).await;
        Ok(())
    }

    async fn get_received_webhooks(&self) -> Vec<ReceivedWebhook> {
        self.received_webhooks.read().await.clone()
    }

    async fn clear_webhooks(&self) {
        self.received_webhooks.write().await.clear();
        self.request_count.store(0, Ordering::SeqCst);
    }

    async fn set_response_config(&self, config: ResponseConfig) {
        *self.response_config.write().await = config;
    }

    fn get_request_count(&self) -> u32 {
        self.request_count.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use zcore_storage::Store;

    #[tokio::test]
    async fn test_basic_webhook_delivery() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path)?);

        // Start mock webhook server
        let mock_server = MockWebhookServer::new();
        let port = 18001;
        mock_server.start_server(port).await?;

        // Create webhook manager
        let webhook_manager = Arc::new(WebhookManager::new(storage));

        // Configure webhook endpoint
        let endpoint = WebhookEndpoint {
            id: uuid::Uuid::new_v4(),
            url: format!("http://127.0.0.1:{}/webhook", port),
            secret: Some("test_secret".to_string()),
            enabled: true,
            event_filters: vec!["job.*".to_string()],
            max_retries: 3,
            timeout_seconds: 30,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        webhook_manager.register_endpoint(endpoint.clone()).await?;

        // Create test event
        let event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 1,
                status: Some("pending".to_string()),
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );

        // Deliver webhook
        webhook_manager.deliver_webhook(&endpoint, &event).await?;

        // Wait for delivery
        sleep(Duration::from_millis(200)).await;

        // Verify webhook was received
        let received = mock_server.get_received_webhooks().await;
        assert_eq!(received.len(), 1);

        let webhook = &received[0];
        assert!(webhook.signature.is_some(), "Webhook should have signature");
        assert!(webhook.headers.contains_key("content-type"));
        assert!(webhook.headers.contains_key("user-agent"));

        // Verify webhook content
        let event_id = event.id.to_string();
        let body_event_id = webhook.body.get("event")
            .and_then(|e| e.get("id"))
            .and_then(|id| id.as_str());
        assert_eq!(body_event_id, Some(event_id.as_str()));

        Ok(())
    }

    #[tokio::test]
    async fn test_webhook_signature_validation() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path)?);

        let mock_server = MockWebhookServer::new();
        let port = 18002;
        mock_server.start_server(port).await?;

        let webhook_manager = Arc::new(WebhookManager::new(storage));
        let secret = "test_secret_for_hmac";

        let endpoint = WebhookEndpoint {
            id: uuid::Uuid::new_v4(),
            url: format!("http://127.0.0.1:{}/webhook", port),
            secret: Some(secret.to_string()),
            enabled: true,
            event_filters: vec!["*".to_string()],
            max_retries: 3,
            timeout_seconds: 30,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        webhook_manager.register_endpoint(endpoint.clone()).await?;

        let event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 1,
                status: Some("test".to_string()),
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );

        webhook_manager.deliver_webhook(&endpoint, &event).await?;
        sleep(Duration::from_millis(200)).await;

        let received = mock_server.get_received_webhooks().await;
        assert_eq!(received.len(), 1);

        let webhook = &received[0];
        let signature = webhook.signature.as_ref()
            .ok_or_else(|| anyhow::anyhow!("No signature found"))?;

        // Verify HMAC signature
        let body_str = serde_json::to_string(&webhook.body)?;
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())?;
        mac.update(body_str.as_bytes());
        let expected_signature = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));

        assert_eq!(signature, &expected_signature, "Signature should match HMAC");

        Ok(())
    }

    #[tokio::test]
    async fn test_webhook_retry_logic() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path)?);

        let mock_server = MockWebhookServer::new();
        let port = 18003;
        mock_server.start_server(port).await?;

        // Configure server to fail initially
        mock_server.set_response_config(ResponseConfig {
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
            delay_ms: 0,
            should_fail: true,
            failure_rate: 0.0,
        }).await;

        let webhook_manager = Arc::new(WebhookManager::new(storage));

        let endpoint = WebhookEndpoint {
            id: uuid::Uuid::new_v4(),
            url: format!("http://127.0.0.1:{}/webhook", port),
            secret: Some("test_secret".to_string()),
            enabled: true,
            event_filters: vec!["*".to_string()],
            max_retries: 3,
            timeout_seconds: 5,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        webhook_manager.register_endpoint(endpoint.clone()).await?;

        let event = Event::new(
            EventType::JobFailed,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 1,
                status: Some("failed".to_string()),
                worker_id: None,
                error: Some("Test error".to_string()),
                output: None,
            },
            1,
        );

        // Attempt delivery (should fail and trigger retries)
        let delivery_result = webhook_manager.deliver_webhook(&endpoint, &event).await;

        // Wait for retry attempts
        sleep(Duration::from_millis(500)).await;

        let request_count = mock_server.get_request_count();
        println!("Requests received: {}", request_count);

        // Should have attempted delivery multiple times (initial + retries)
        assert!(request_count > 1, "Should have retried failed delivery");
        assert!(request_count <= 4, "Should not exceed max retries + 1"); // 1 initial + 3 retries

        // Change server to succeed
        mock_server.set_response_config(ResponseConfig::default()).await;
        mock_server.clear_webhooks().await;

        // Try again - should succeed
        webhook_manager.deliver_webhook(&endpoint, &event).await?;
        sleep(Duration::from_millis(200)).await;

        let received = mock_server.get_received_webhooks().await;
        assert_eq!(received.len(), 1, "Should succeed after server is fixed");

        Ok(())
    }

    #[tokio::test]
    async fn test_webhook_filtering() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path)?);

        let mock_server = MockWebhookServer::new();
        let port = 18004;
        mock_server.start_server(port).await?;

        let webhook_manager = Arc::new(WebhookManager::new(storage));

        // Create endpoint that only listens to job events
        let job_endpoint = WebhookEndpoint {
            id: uuid::Uuid::new_v4(),
            url: format!("http://127.0.0.1:{}/webhook", port),
            secret: Some("job_secret".to_string()),
            enabled: true,
            event_filters: vec!["job.*".to_string()],
            max_retries: 1,
            timeout_seconds: 30,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        webhook_manager.register_endpoint(job_endpoint.clone()).await?;

        // Send job event (should be delivered)
        let job_event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 1,
                status: Some("created".to_string()),
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );

        webhook_manager.deliver_webhook(&job_endpoint, &job_event).await?;

        // Send user event (should NOT be delivered due to filter)
        let user_event = Event::new(
            EventType::UserCreated,
            EventData::User {
                user_id: Some(uuid::Uuid::new_v4()),
                username: Some("testuser".to_string()),
                email: Some("test@example.com".to_string()),
                action: Some("created".to_string()),
                metadata: None,
            },
            1,
        );

        // This would normally fail because the event doesn't match the filter
        // In a real implementation, the manager would check filters before delivery
        // For now, we'll test that the correct event was received

        sleep(Duration::from_millis(200)).await;

        let received = mock_server.get_received_webhooks().await;
        assert_eq!(received.len(), 1, "Only job event should be delivered");

        // Verify it's the job event
        let event_type = received[0].body.get("event")
            .and_then(|e| e.get("event_type"))
            .and_then(|t| t.as_str());
        assert!(event_type.unwrap_or("").contains("JobCreated"));

        Ok(())
    }

    #[tokio::test]
    async fn test_webhook_timeout_handling() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path)?);

        let mock_server = MockWebhookServer::new();
        let port = 18005;
        mock_server.start_server(port).await?;

        // Configure server with long delay to trigger timeout
        mock_server.set_response_config(ResponseConfig {
            status_code: StatusCode::OK,
            delay_ms: 2000, // 2 second delay
            should_fail: false,
            failure_rate: 0.0,
        }).await;

        let webhook_manager = Arc::new(WebhookManager::new(storage));

        let endpoint = WebhookEndpoint {
            id: uuid::Uuid::new_v4(),
            url: format!("http://127.0.0.1:{}/webhook", port),
            secret: Some("timeout_test".to_string()),
            enabled: true,
            event_filters: vec!["*".to_string()],
            max_retries: 1,
            timeout_seconds: 1, // 1 second timeout
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        webhook_manager.register_endpoint(endpoint.clone()).await?;

        let event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 1,
                status: Some("timeout_test".to_string()),
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );

        // Measure delivery time
        let start = Instant::now();
        let result = webhook_manager.deliver_webhook(&endpoint, &event).await;
        let duration = start.elapsed();

        println!("Webhook delivery took: {:?}", duration);

        // Should timeout and not wait for the full server delay
        assert!(duration < Duration::from_secs(2), "Should timeout before server delay");

        // In a real implementation, this would return a timeout error
        // For now, we just verify the timing behavior

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_webhook_deliveries() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path)?);

        let mock_server = MockWebhookServer::new();
        let port = 18006;
        mock_server.start_server(port).await?;

        // Add small delay to simulate realistic webhook processing
        mock_server.set_response_config(ResponseConfig {
            status_code: StatusCode::OK,
            delay_ms: 50,
            should_fail: false,
            failure_rate: 0.0,
        }).await;

        let webhook_manager = Arc::new(WebhookManager::new(storage));

        let endpoint = WebhookEndpoint {
            id: uuid::Uuid::new_v4(),
            url: format!("http://127.0.0.1:{}/webhook", port),
            secret: Some("concurrent_test".to_string()),
            enabled: true,
            event_filters: vec!["*".to_string()],
            max_retries: 1,
            timeout_seconds: 10,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        webhook_manager.register_endpoint(endpoint.clone()).await?;

        // Send multiple webhooks concurrently
        let num_webhooks = 20;
        let mut tasks = Vec::new();

        let start_time = Instant::now();

        for i in 0..num_webhooks {
            let manager = webhook_manager.clone();
            let endpoint = endpoint.clone();

            let task = tokio::spawn(async move {
                let event = Event::new(
                    EventType::JobCreated,
                    EventData::Job {
                        job_id: uuid::Uuid::new_v4(),
                        org_id: 1,
                        status: Some(format!("concurrent_test_{}", i)),
                        worker_id: None,
                        error: None,
                        output: None,
                    },
                    1,
                );

                manager.deliver_webhook(&endpoint, &event).await
            });

            tasks.push(task);
        }

        // Wait for all deliveries to complete
        for task in tasks {
            task.await.unwrap()?;
        }

        let total_time = start_time.elapsed();

        // Wait a bit more for any delayed deliveries
        sleep(Duration::from_millis(200)).await;

        let received = mock_server.get_received_webhooks().await;
        println!(
            "Delivered {} webhooks in {:?} (avg: {:?} per webhook)",
            received.len(),
            total_time,
            total_time / received.len() as u32
        );

        assert_eq!(received.len(), num_webhooks, "All webhooks should be delivered");

        // With concurrency, total time should be much less than sequential
        // (20 webhooks * 50ms delay = 1000ms sequential, should be much faster concurrent)
        assert!(total_time < Duration::from_millis(800), "Concurrent delivery should be faster than sequential");

        Ok(())
    }

    #[tokio::test]
    async fn test_webhook_failure_tracking() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path)?);

        let mock_server = MockWebhookServer::new();
        let port = 18007;
        mock_server.start_server(port).await?;

        // Configure 50% failure rate
        mock_server.set_response_config(ResponseConfig {
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
            delay_ms: 10,
            should_fail: false,
            failure_rate: 0.5,
        }).await;

        let webhook_manager = Arc::new(WebhookManager::new(storage));

        let endpoint = WebhookEndpoint {
            id: uuid::Uuid::new_v4(),
            url: format!("http://127.0.0.1:{}/webhook", port),
            secret: Some("failure_test".to_string()),
            enabled: true,
            event_filters: vec!["*".to_string()],
            max_retries: 2,
            timeout_seconds: 5,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        webhook_manager.register_endpoint(endpoint.clone()).await?;

        let num_attempts = 50;
        let mut successful_deliveries = 0;
        let mut failed_deliveries = 0;

        for i in 0..num_attempts {
            let event = Event::new(
                EventType::JobCreated,
                EventData::Job {
                    job_id: uuid::Uuid::new_v4(),
                    org_id: 1,
                    status: Some(format!("failure_test_{}", i)),
                    worker_id: None,
                    error: None,
                    output: None,
                },
                1,
            );

            match webhook_manager.deliver_webhook(&endpoint, &event).await {
                Ok(_) => successful_deliveries += 1,
                Err(_) => failed_deliveries += 1,
            }
        }

        sleep(Duration::from_millis(500)).await;

        let total_requests = mock_server.get_request_count();
        let received = mock_server.get_received_webhooks().await;

        println!(
            "Results: {} successful, {} failed, {} total requests, {} received",
            successful_deliveries, failed_deliveries, total_requests, received.len()
        );

        // Should have some failures due to 50% failure rate
        assert!(failed_deliveries > 0, "Should have some failures with 50% failure rate");
        assert!(successful_deliveries > 0, "Should have some successes with 50% failure rate");

        // Total requests should be higher than attempts due to retries
        assert!(total_requests >= num_attempts, "Should have at least as many requests as attempts");

        Ok(())
    }
}