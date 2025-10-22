use crate::events::{Event, EventType};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use governor::{Quota, RateLimiter};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use ulid::Ulid;

type HmacSha256 = Hmac<Sha256>;
type WebhookRateLimiter = RateLimiter<String, governor::state::keyed::DefaultKeyedStateStore<String>, governor::clock::DefaultClock, governor::middleware::NoOpMiddleware>;

/// Webhook endpoint configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookEndpoint {
    /// Unique identifier for this endpoint
    pub id: String,
    /// URL to send webhooks to
    pub url: String,
    /// Secret for HMAC signature validation
    pub secret: String,
    /// Event types this endpoint subscribes to
    pub event_types: Vec<EventType>,
    /// Topic patterns to match (supports wildcards)
    pub topics: Vec<String>,
    /// Organization ID for multi-tenant isolation
    pub org_id: u64,
    /// Whether this endpoint is active
    pub active: bool,
    /// Maximum retries for failed deliveries
    pub max_retries: u32,
    /// Timeout for HTTP requests in seconds
    pub timeout_seconds: u64,
    /// Custom headers to include in webhook requests
    pub headers: HashMap<String, String>,
    /// Rate limiting configuration
    pub rate_limit: Option<RateLimitConfig>,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Updated timestamp
    pub updated_at: DateTime<Utc>,
}

impl WebhookEndpoint {
    pub fn new(
        url: String,
        secret: String,
        event_types: Vec<EventType>,
        org_id: u64,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Ulid::new().to_string(),
            url,
            secret,
            event_types,
            topics: vec!["**".to_string()], // Default to all topics
            org_id,
            active: true,
            max_retries: 3,
            timeout_seconds: 30,
            headers: HashMap::new(),
            rate_limit: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn with_topics(mut self, topics: Vec<String>) -> Self {
        self.topics = topics;
        self
    }

    pub fn with_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn with_timeout(mut self, timeout_seconds: u64) -> Self {
        self.timeout_seconds = timeout_seconds;
        self
    }

    pub fn with_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers = headers;
        self
    }

    pub fn with_rate_limit(mut self, rate_limit: RateLimitConfig) -> Self {
        self.rate_limit = Some(rate_limit);
        self
    }

    /// Check if this endpoint should receive the given event
    pub fn should_receive(&self, event: &Event) -> bool {
        if !self.active {
            return false;
        }

        // Check organization isolation
        if event.org_id != self.org_id {
            return false;
        }

        // Check event type subscription
        if !self.event_types.contains(&event.event_type) {
            return false;
        }

        // Check topic patterns
        let event_topic = event.to_topic();
        for pattern in &self.topics {
            if event.matches_topic(pattern) {
                return true;
            }
        }

        false
    }
}

/// Rate limiting configuration for webhooks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Maximum requests per period
    pub requests_per_period: u32,
    /// Period duration in seconds
    pub period_seconds: u64,
    /// Burst allowance
    pub burst: Option<u32>,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_period: 100,
            period_seconds: 60,
            burst: Some(10),
        }
    }
}

/// Webhook delivery status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WebhookStatus {
    Pending,
    Delivered,
    Failed,
    Retrying,
    Dropped,
}

/// Webhook delivery record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookDelivery {
    /// Unique delivery ID
    pub id: String,
    /// Webhook endpoint ID
    pub endpoint_id: String,
    /// Event ID that was delivered
    pub event_id: String,
    /// Delivery status
    pub status: WebhookStatus,
    /// Number of delivery attempts
    pub attempts: u32,
    /// HTTP status code from last attempt
    pub last_status_code: Option<u16>,
    /// Error message from last attempt
    pub last_error: Option<String>,
    /// Response body from last attempt (truncated)
    pub last_response: Option<String>,
    /// Delivery timestamp
    pub delivered_at: Option<DateTime<Utc>>,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Next retry timestamp
    pub next_retry_at: Option<DateTime<Utc>>,
}

impl WebhookDelivery {
    pub fn new(endpoint_id: String, event_id: String) -> Self {
        Self {
            id: Ulid::new().to_string(),
            endpoint_id,
            event_id,
            status: WebhookStatus::Pending,
            attempts: 0,
            last_status_code: None,
            last_error: None,
            last_response: None,
            delivered_at: None,
            created_at: Utc::now(),
            next_retry_at: None,
        }
    }

    pub fn record_attempt(&mut self, status_code: u16, response: Option<String>, error: Option<String>) {
        self.attempts += 1;
        self.last_status_code = Some(status_code);
        self.last_response = response.map(|r| r.chars().take(1000).collect()); // Truncate
        self.last_error = error;

        if status_code >= 200 && status_code < 300 {
            self.status = WebhookStatus::Delivered;
            self.delivered_at = Some(Utc::now());
        } else {
            self.status = WebhookStatus::Failed;
        }
    }

    pub fn mark_for_retry(&mut self, retry_delay: Duration) {
        self.status = WebhookStatus::Retrying;
        self.next_retry_at = Some(Utc::now() + chrono::Duration::from_std(retry_delay).unwrap());
    }

    pub fn mark_dropped(&mut self) {
        self.status = WebhookStatus::Dropped;
    }
}

/// Webhook signature for payload verification
#[derive(Debug, Clone)]
pub struct WebhookSignature {
    /// HMAC algorithm used
    pub algorithm: String,
    /// Computed signature
    pub signature: String,
    /// Timestamp of signature generation
    pub timestamp: DateTime<Utc>,
}

impl WebhookSignature {
    pub fn new(payload: &[u8], secret: &str) -> Result<Self> {
        let timestamp = Utc::now();
        let timestamp_str = timestamp.timestamp().to_string();

        // Create signed payload: timestamp.payload
        let signed_payload = format!("{}.{}", timestamp_str, String::from_utf8_lossy(payload));

        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .map_err(|e| anyhow!("Invalid secret key: {}", e))?;

        mac.update(signed_payload.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        Ok(Self {
            algorithm: "sha256".to_string(),
            signature,
            timestamp,
        })
    }

    pub fn verify(&self, payload: &[u8], secret: &str, tolerance_seconds: u64) -> Result<bool> {
        // Check timestamp tolerance
        let now = Utc::now();
        let age = (now - self.timestamp).num_seconds().abs() as u64;
        if age > tolerance_seconds {
            return Ok(false);
        }

        // Recreate signature
        let timestamp_str = self.timestamp.timestamp().to_string();
        let signed_payload = format!("{}.{}", timestamp_str, String::from_utf8_lossy(payload));

        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .map_err(|e| anyhow!("Invalid secret key: {}", e))?;

        mac.update(signed_payload.as_bytes());
        let expected = hex::encode(mac.finalize().into_bytes());

        Ok(self.signature == expected)
    }

    pub fn to_header(&self) -> String {
        format!("t={},v1={}", self.timestamp.timestamp(), self.signature)
    }
}

/// Webhook manager configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    /// Maximum concurrent deliveries
    pub max_concurrent: usize,
    /// Base retry delay in seconds
    pub base_retry_delay: u64,
    /// Maximum retry delay in seconds
    pub max_retry_delay: u64,
    /// Signature timestamp tolerance in seconds
    pub signature_tolerance: u64,
    /// Default timeout for HTTP requests
    pub default_timeout: u64,
    /// User agent for webhook requests
    pub user_agent: String,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 100,
            base_retry_delay: 2,
            max_retry_delay: 300,
            signature_tolerance: 300, // 5 minutes
            default_timeout: 30,
            user_agent: "ZRUSTDB-Webhooks/1.0".to_string(),
        }
    }
}

/// Webhook delivery statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookStats {
    /// Total deliveries attempted
    pub total_deliveries: u64,
    /// Successful deliveries
    pub successful_deliveries: u64,
    /// Failed deliveries
    pub failed_deliveries: u64,
    /// Dropped deliveries (after max retries)
    pub dropped_deliveries: u64,
    /// Current pending deliveries
    pub pending_deliveries: u64,
    /// Average delivery time in milliseconds
    pub avg_delivery_time_ms: f64,
}

/// Webhook manager for handling HTTP webhook deliveries
pub struct WebhookManager {
    config: WebhookConfig,
    endpoints: Arc<RwLock<HashMap<String, WebhookEndpoint>>>,
    deliveries: Arc<RwLock<HashMap<String, WebhookDelivery>>>,
    rate_limiters: Arc<RwLock<HashMap<String, WebhookRateLimiter>>>,
    http_client: Client,
    stats: Arc<RwLock<WebhookStats>>,
}

impl WebhookManager {
    pub async fn new(config: WebhookConfig) -> Result<Self> {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(config.default_timeout))
            .user_agent(&config.user_agent)
            .build()
            .map_err(|e| anyhow!("Failed to create HTTP client: {}", e))?;

        Ok(Self {
            config,
            endpoints: Arc::new(RwLock::new(HashMap::new())),
            deliveries: Arc::new(RwLock::new(HashMap::new())),
            rate_limiters: Arc::new(RwLock::new(HashMap::new())),
            http_client,
            stats: Arc::new(RwLock::new(WebhookStats {
                total_deliveries: 0,
                successful_deliveries: 0,
                failed_deliveries: 0,
                dropped_deliveries: 0,
                pending_deliveries: 0,
                avg_delivery_time_ms: 0.0,
            })),
        })
    }

    /// Register a webhook endpoint
    pub async fn register_endpoint(&self, endpoint: WebhookEndpoint) -> Result<()> {
        let endpoint_id = endpoint.id.clone();

        // Create rate limiter if configured
        if let Some(rate_limit) = &endpoint.rate_limit {
            let quota = Quota::with_period(Duration::from_secs(rate_limit.period_seconds))
                .unwrap()
                .allow_burst(NonZeroU32::new(rate_limit.burst.unwrap_or(rate_limit.requests_per_period)).unwrap());

            let limiter = RateLimiter::keyed(quota);
            self.rate_limiters.write().await.insert(endpoint_id.clone(), limiter);
        }

        self.endpoints.write().await.insert(endpoint_id.clone(), endpoint);

        info!(endpoint_id = %endpoint_id, "Webhook endpoint registered");
        Ok(())
    }

    /// Unregister a webhook endpoint
    pub async fn unregister_endpoint(&self, endpoint_id: &str) -> Result<()> {
        self.endpoints.write().await.remove(endpoint_id);
        self.rate_limiters.write().await.remove(endpoint_id);

        info!(endpoint_id = %endpoint_id, "Webhook endpoint unregistered");
        Ok(())
    }

    /// Get all registered endpoints
    pub async fn list_endpoints(&self) -> Vec<WebhookEndpoint> {
        self.endpoints.read().await.values().cloned().collect()
    }

    /// Get a specific endpoint
    pub async fn get_endpoint(&self, endpoint_id: &str) -> Option<WebhookEndpoint> {
        self.endpoints.read().await.get(endpoint_id).cloned()
    }

    /// Update an endpoint
    pub async fn update_endpoint(&self, endpoint: WebhookEndpoint) -> Result<()> {
        let endpoint_id = endpoint.id.clone();

        if self.endpoints.read().await.contains_key(&endpoint_id) {
            self.endpoints.write().await.insert(endpoint_id, endpoint);
            Ok(())
        } else {
            Err(anyhow!("Endpoint not found: {}", endpoint_id))
        }
    }

    /// Check if any endpoints subscribe to an event type
    pub fn has_subscribers(&self, event_type: &EventType) -> bool {
        if let Ok(endpoints) = self.endpoints.try_read() {
            endpoints.values().any(|ep| ep.event_types.contains(event_type))
        } else {
            false
        }
    }

    /// Deliver an event to all matching endpoints
    pub async fn deliver_event(&self, event: &Event) -> Result<()> {
        let endpoints = self.endpoints.read().await;
        let matching_endpoints: Vec<_> = endpoints
            .values()
            .filter(|ep| ep.should_receive(event))
            .cloned()
            .collect();
        drop(endpoints);

        if matching_endpoints.is_empty() {
            debug!(event_id = %event.id, "No webhook endpoints for event");
            return Ok(());
        }

        info!(
            event_id = %event.id,
            endpoints = matching_endpoints.len(),
            "Delivering event to webhook endpoints"
        );

        // Create delivery records
        let mut delivery_tasks = Vec::new();

        for endpoint in matching_endpoints {
            let delivery = WebhookDelivery::new(endpoint.id.clone(), event.id.to_string());
            let delivery_id = delivery.id.clone();

            self.deliveries.write().await.insert(delivery_id.clone(), delivery);

            // Spawn delivery task
            let task = self.deliver_to_endpoint(event.clone(), endpoint, delivery_id);
            delivery_tasks.push(task);
        }

        // Execute deliveries concurrently (up to max_concurrent)
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.max_concurrent));
        let futures: Vec<_> = delivery_tasks
            .into_iter()
            .map(|task| {
                let permit = semaphore.clone();
                async move {
                    let _permit = permit.acquire().await.unwrap();
                    task.await
                }
            })
            .collect();

        // Wait for all deliveries to complete
        futures_util::future::join_all(futures).await;

        Ok(())
    }

    /// Deliver event to a specific endpoint
    async fn deliver_to_endpoint(
        &self,
        event: Event,
        endpoint: WebhookEndpoint,
        delivery_id: String,
    ) -> Result<()> {
        let start_time = std::time::Instant::now();

        // Check rate limiting
        if let Some(rate_limiter) = self.rate_limiters.read().await.get(&endpoint.id) {
            if rate_limiter.check_key(&endpoint.id).is_err() {
                warn!(
                    endpoint_id = %endpoint.id,
                    "Webhook delivery rate limited"
                );

                if let Some(delivery) = self.deliveries.write().await.get_mut(&delivery_id) {
                    delivery.record_attempt(429, None, Some("Rate limited".to_string()));
                }
                return Ok(());
            }
        }

        // Prepare payload
        let payload = serde_json::to_vec(&event)?;

        // Generate signature
        let signature = WebhookSignature::new(&payload, &endpoint.secret)?;

        // Prepare headers
        let mut headers = endpoint.headers.clone();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        headers.insert("X-ZRUSTDB-Signature".to_string(), signature.to_header());
        headers.insert("X-ZRUSTDB-Event-Type".to_string(), event.event_type.to_string());
        headers.insert("X-ZRUSTDB-Event-ID".to_string(), event.id.to_string());
        headers.insert("X-ZRUSTDB-Delivery-ID".to_string(), delivery_id.clone());

        // Build request
        let mut request = self.http_client
            .post(&endpoint.url)
            .timeout(Duration::from_secs(endpoint.timeout_seconds))
            .body(payload);

        for (key, value) in headers {
            request = request.header(key, value);
        }

        // Execute request
        let result = request.send().await;
        let delivery_time = start_time.elapsed();

        // Update delivery record
        if let Some(delivery) = self.deliveries.write().await.get_mut(&delivery_id) {
            match result {
                Ok(response) => {
                    let status = response.status().as_u16();
                    let body = response.text().await.ok();

                    delivery.record_attempt(status, body, None);

                    if status >= 200 && status < 300 {
                        debug!(
                            endpoint_id = %endpoint.id,
                            delivery_id = %delivery_id,
                            status = status,
                            duration_ms = delivery_time.as_millis(),
                            "Webhook delivered successfully"
                        );
                    } else {
                        warn!(
                            endpoint_id = %endpoint.id,
                            delivery_id = %delivery_id,
                            status = status,
                            "Webhook delivery failed with HTTP error"
                        );

                        // Schedule retry if attempts are below max
                        if delivery.attempts < endpoint.max_retries {
                            let delay = self.calculate_retry_delay(delivery.attempts);
                            delivery.mark_for_retry(delay);
                        } else {
                            delivery.mark_dropped();
                        }
                    }
                }
                Err(e) => {
                    error!(
                        endpoint_id = %endpoint.id,
                        delivery_id = %delivery_id,
                        error = %e,
                        "Webhook delivery failed with network error"
                    );

                    delivery.record_attempt(0, None, Some(e.to_string()));

                    // Schedule retry if attempts are below max
                    if delivery.attempts < endpoint.max_retries {
                        let delay = self.calculate_retry_delay(delivery.attempts);
                        delivery.mark_for_retry(delay);
                    } else {
                        delivery.mark_dropped();
                    }
                }
            }
        }

        // Update statistics
        self.update_stats(delivery_time).await;

        Ok(())
    }

    /// Calculate exponential backoff retry delay
    fn calculate_retry_delay(&self, attempt: u32) -> Duration {
        let delay_seconds = std::cmp::min(
            self.config.base_retry_delay * 2_u64.pow(attempt),
            self.config.max_retry_delay,
        );
        Duration::from_secs(delay_seconds)
    }

    /// Update delivery statistics
    async fn update_stats(&self, delivery_time: std::time::Duration) {
        let mut stats = self.stats.write().await;
        stats.total_deliveries += 1;

        // Update rolling average delivery time
        let delivery_ms = delivery_time.as_millis() as f64;
        if stats.avg_delivery_time_ms == 0.0 {
            stats.avg_delivery_time_ms = delivery_ms;
        } else {
            stats.avg_delivery_time_ms = (stats.avg_delivery_time_ms * 0.9) + (delivery_ms * 0.1);
        }
    }

    /// Get delivery statistics
    pub async fn get_stats(&self) -> WebhookStats {
        let mut stats = self.stats.read().await.clone();

        // Count current deliveries by status
        let deliveries = self.deliveries.read().await;
        stats.successful_deliveries = deliveries.values().filter(|d| d.status == WebhookStatus::Delivered).count() as u64;
        stats.failed_deliveries = deliveries.values().filter(|d| d.status == WebhookStatus::Failed).count() as u64;
        stats.dropped_deliveries = deliveries.values().filter(|d| d.status == WebhookStatus::Dropped).count() as u64;
        stats.pending_deliveries = deliveries.values().filter(|d| matches!(d.status, WebhookStatus::Pending | WebhookStatus::Retrying)).count() as u64;

        stats
    }

    /// Get all deliveries
    pub async fn list_deliveries(&self) -> Vec<WebhookDelivery> {
        self.deliveries.read().await.values().cloned().collect()
    }

    /// Get deliveries for a specific endpoint
    pub async fn list_deliveries_for_endpoint(&self, endpoint_id: &str) -> Vec<WebhookDelivery> {
        self.deliveries
            .read()
            .await
            .values()
            .filter(|d| d.endpoint_id == endpoint_id)
            .cloned()
            .collect()
    }

    /// Cleanup old delivery records
    pub async fn cleanup_old_deliveries(&self, older_than: Duration) -> Result<usize> {
        let cutoff = Utc::now() - chrono::Duration::from_std(older_than)?;
        let mut deliveries = self.deliveries.write().await;

        let initial_count = deliveries.len();
        deliveries.retain(|_, delivery| {
            delivery.created_at > cutoff ||
            matches!(delivery.status, WebhookStatus::Pending | WebhookStatus::Retrying)
        });

        let removed = initial_count - deliveries.len();
        info!(removed = removed, "Cleaned up old webhook deliveries");

        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{Event, EventData, EventType};

    #[test]
    fn test_webhook_signature() {
        let payload = b"test payload";
        let secret = "test_secret";

        let sig = WebhookSignature::new(payload, secret).unwrap();
        assert_eq!(sig.algorithm, "sha256");
        assert!(!sig.signature.is_empty());

        // Verify with correct secret
        assert!(sig.verify(payload, secret, 300).unwrap());

        // Verify with wrong secret
        assert!(!sig.verify(payload, "wrong_secret", 300).unwrap());
    }

    #[test]
    fn test_webhook_endpoint_should_receive() {
        let endpoint = WebhookEndpoint::new(
            "https://example.com/webhook".to_string(),
            "secret".to_string(),
            vec![EventType::JobCreated],
            1,
        );

        let event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 1,
                status: None,
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );

        assert!(endpoint.should_receive(&event));

        // Wrong org
        let wrong_org_event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 2,
                status: None,
                worker_id: None,
                error: None,
                output: None,
            },
            2,
        );

        assert!(!endpoint.should_receive(&wrong_org_event));

        // Wrong event type
        let wrong_type_event = Event::new(
            EventType::UserCreated,
            EventData::User {
                user_id: Some(1),
                org_id: 1,
                email: Some("test@example.com".to_string()),
                action: "created".to_string(),
                ip_address: None,
            },
            1,
        );

        assert!(!endpoint.should_receive(&wrong_type_event));
    }

    #[tokio::test]
    async fn test_webhook_manager_creation() {
        let config = WebhookConfig::default();
        let manager = WebhookManager::new(config).await.unwrap();
        assert_eq!(manager.list_endpoints().await.len(), 0);
    }
}