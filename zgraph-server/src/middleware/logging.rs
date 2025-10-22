//! Structured logging middleware for zGraph web server
//!
//! Provides request/response logging with tracing integration
//! and configurable log levels for observability.

use axum::{
    extract::Request,
    middleware::Next,
    response::Response,
};
use std::time::Instant;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::observability::MetricsCollector;

/// Request logging middleware
pub async fn logging_layer(
    metrics: MetricsCollector,
) -> impl Fn(Request, Response) + Clone {
    move |mut request: Request, next: Next<Response>| {
        let start_time = Instant::now();
        let request_id = Uuid::new_v4().to_string();

        // Log request start
        info!(
            method = %request.method(),
            uri = %request.uri(),
            request_id = request_id,
            user_agent = ?request.headers().get("user-agent"),
            "Request started"
        );

        let response = next.run(request).await;

        // Calculate and log request duration
        let duration = start_time.elapsed();
        let path = request.uri().path();

        // Record metrics
        if let Some(endpoint) = path.strip_prefix("/v1/") {
            metrics.record_request(endpoint);
            metrics.record_request_duration(endpoint, duration);
        }

        // Log request completion
        let status = response.status();
        info!(
            method = %request.method(),
            uri = %request.uri(),
            request_id = request_id,
            status = %status,
            duration_ms = duration.as_millis(),
            "Request completed"
        );

        response
    }
}