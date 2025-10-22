//! Metrics collection middleware for zGraph web server
//!
//! Records request counts, durations, and errors for each endpoint
//! and integrates with the observability system.

use axum::{
    extract::Request,
    middleware::Next,
    response::Response,
};
use std::sync::Arc;
use tracing::debug;

use crate::observability::MetricsCollector;

/// Metrics collection middleware
pub fn metrics_layer(
    metrics: Arc<MetricsCollector>,
) -> impl Fn(Request, Response) + Clone {
    move |mut request: Request, next: Next<Response>| {
        let start_time = std::time::Instant::now();
        let response = next.run(request).await;

        // Record the request after it completes
        let duration = start_time.elapsed();
        if let Some(path) = request.uri().path().strip_prefix("/v1/") {
            metrics.record_request(path);
            metrics.record_request_duration(path, duration);
        }

        debug!(
            "Request completed in {:?}ms - Status: {}",
            duration.as_millis(),
            response.status()
        );

        response
    }
}