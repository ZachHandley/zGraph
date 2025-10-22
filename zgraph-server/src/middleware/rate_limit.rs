//! Rate limiting middleware for zGraph web server
//!
//! Provides per-IP and per-user rate limiting to prevent abuse
//! and ensure fair resource allocation.

use axum::{
    extract::Request,
    http::{StatusCode, HeaderMap},
    middleware::Next,
    response::{IntoResponse, Json},
};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock as TokioRwLock;
use tracing::{debug, warn};

use crate::models::{ApiResponse, ErrorResponse};

/// Rate limiting configuration
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub requests_per_minute: u32,
    pub requests_per_hour: u32,
    pub burst_limit: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_minute: 60,
            requests_per_hour: 1000,
            burst_limit: 10,
        }
    }
}

/// Rate limiter state
#[derive(Debug)]
struct RateLimiter {
    /// Per-IP request tracking
    ip_requests: HashMap<IpAddr, RequestTracker>,

    /// Per-user request tracking
    user_requests: HashMap<String, RequestTracker>,

    config: RateLimitConfig,
}

/// Shared rate limiter with async access
#[derive(Debug, Clone)]
pub struct SharedRateLimiter {
    inner: Arc<TokioRwLock<RateLimiter>>,
}

#[derive(Debug)]
struct RequestTracker {
    count: u32,
    first_request: Instant,
}

impl RateLimiter {
    fn new(config: RateLimitConfig) -> Self {
        Self {
            ip_requests: HashMap::new(),
            user_requests: HashMap::new(),
            config,
        }
    }
}

impl SharedRateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            inner: Arc::new(TokioRwLock::new(RateLimiter::new(config))),
        }
    }

    pub async fn is_rate_limited(&self, ip: Option<IpAddr>, user_id: Option<&str>) -> bool {
        let now = Instant::now();
        let mut limiter = self.inner.write().await;

        // Check per-IP limit
        if let Some(ip) = ip {
            if let Some(tracker) = limiter.ip_requests.get_mut(&ip) {
                // Reset counter if minute has passed
                if now.duration_since(tracker.first_request) >= Duration::from_secs(60) {
                    tracker.count = 0;
                    tracker.first_request = now;
                }

                if tracker.count >= limiter.config.requests_per_minute {
                    return true;
                }
            }
        }

        // Check per-user limit
        if let Some(user) = user_id {
            if let Some(tracker) = limiter.user_requests.get_mut(user) {
                // Reset counter if hour has passed
                if now.duration_since(tracker.first_request) >= Duration::from_secs(3600) {
                    tracker.count = 0;
                    tracker.first_request = now;
                }

                if tracker.count >= limiter.config.requests_per_hour {
                    return true;
                }
            }
        }

        false
    }

    pub async fn record_request(&self, ip: Option<IpAddr>, user_id: Option<&str>) {
        let now = Instant::now();
        let mut limiter = self.inner.write().await;

        // Record per-IP request
        if let Some(ip_addr) = ip {
            let tracker = limiter.ip_requests.entry(ip_addr).or_insert_with(|| RequestTracker {
                count: 0,
                first_request: now,
            });
            tracker.count += 1;
        }

        // Record per-user request
        if let Some(user) = user_id {
            let tracker = limiter.user_requests.entry(user.to_string()).or_insert_with(|| RequestTracker {
                count: 0,
                first_request: now,
            });
            tracker.count += 1;
        }
    }
}

/// Rate limiting middleware
pub fn rate_limit_layer(
    config: RateLimitConfig,
) -> axum::middleware::FromFnLayer<impl Fn(Request, Next) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>> + Clone> {
    let limiter = SharedRateLimiter::new(config.clone());

    axum::middleware::from_fn(move |request: Request, next: Next| {
        let limiter = limiter.clone();
        let config = config.clone();

        Box::pin(async move {
            let headers = request.headers();
            let ip = headers
                .get("x-forwarded-for")
                .and_then(|ip| ip.to_str().ok())
                .and_then(|ip| ip.parse().ok())
                .or_else(|| headers.get("x-real-ip").and_then(|ip| ip.to_str().ok()).and_then(|ip| ip.parse().ok()));

            // Extract user ID from session (if available)
            let user_id = request.extensions()
                .get::<crate::auth::AuthSession>()
                .map(|session| session.user_id.as_str());

            // Check rate limits
            if limiter.is_rate_limited(ip, user_id).await {
                debug!(
                    "Rate limit exceeded for IP: {:?}, User: {:?}",
                    ip, user_id
                );

                let error_response = ErrorResponse {
                    code: "RATE_LIMIT_EXCEEDED".to_string(),
                    message: "Too many requests. Please try again later.".to_string(),
                    details: Some(serde_json::json!({
                        "limit_per_minute": config.requests_per_minute,
                        "limit_per_hour": config.requests_per_hour,
                    })),
                };

                let response = axum::Json(serde_json::json!(error_response));
                return axum::Response::builder()
                    .status(StatusCode::TOO_MANY_REQUESTS)
                    .body(response.into_response())
                    .unwrap()
                    .into_response();
            }

            // Record the request
            limiter.record_request(ip, user_id).await;

            next.run(request).await
        })
    })
}