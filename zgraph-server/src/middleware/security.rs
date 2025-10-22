//! Security headers middleware for zGraph web server
//!
//! Adds security-focused HTTP headers to all responses
//! including CORS, CSP, and other security best practices.

use axum::{
    extract::Request,
    http::{HeaderMap, HeaderValue},
    middleware::Next,
    response::Response,
};
use tracing::debug;

/// Adds security headers to all HTTP responses
pub async fn security_headers<B>(
    _request: Request<B>,
    next: Next<B>,
) -> Response<B> {
    let mut response = next.run(_request).await;

    // Content Security Policy
    response.headers_mut().insert(
        "Content-Security-Policy",
        HeaderValue::from_static("default-src 'self'; script-src 'self'; object-src 'self'"),
    );

    // X-Content-Type-Options
    response.headers_mut().insert(
        "X-Content-Type-Options",
        HeaderValue::from_static("nosniff"),
    );

    // X-Frame-Options
    response.headers_mut().insert(
        "X-Frame-Options",
        HeaderValue::from_static("DENY"),
    );

    // Strict-Transport-Security
    response.headers_mut().insert(
        "Strict-Transport-Security",
        HeaderValue::from_static("max-age=31536000; includeSubDomains; preload"),
    );

    // X-XSS-Protection
    response.headers_mut().insert(
        "X-XSS-Protection",
        HeaderValue::from_static("1; mode=block"),
    );

    // Referrer-Policy
    response.headers_mut().insert(
        "Referrer-Policy",
        HeaderValue::from_static("strict-origin-when-cross-origin"),
    );

    // Permissions Policy (restricts what external sites can do)
    response.headers_mut().insert(
        "Permissions-Policy",
        HeaderValue::from_static("geolocation 'none'; microphone 'none'; payment 'none'"),
    );

    debug!("Added security headers to response");

    response
}