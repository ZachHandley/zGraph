//! Security middleware and utilities for zserver
//!
//! This module provides comprehensive security features including:
//! - Rate limiting
//! - Input validation and sanitization
//! - Security headers
//! - Request size limits
//! - Path traversal protection
//! - SQL injection protection

use axum::{
    async_trait,
    body::Body,
    extract::{ConnectInfo, Request, State},
    http::{header, HeaderValue, Method, StatusCode, Uri},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock};
use tower_http::{
    cors::CorsLayer,
    limit::RequestBodyLimitLayer,
    normalize_path::NormalizePathLayer,
    sensitive_headers::SetSensitiveHeadersLayer,
    trace::TraceLayer,
};
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use regex::Regex;
use validator::{Validate, ValidationError};

/// Rate limiting configuration
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum requests per window
    pub max_requests: u32,
    /// Time window in seconds
    pub window_seconds: u32,
    /// Whether to limit by IP address
    pub limit_by_ip: bool,
    /// Whether to limit by user ID
    pub limit_by_user: bool,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests: 100,
            window_seconds: 60,
            limit_by_ip: true,
            limit_by_user: true,
        }
    }
}

/// Rate limit tracker
#[derive(Debug, Clone)]
struct RateLimitTracker {
    requests: Vec<Instant>,
    window_start: Instant,
    window_size: Duration,
}

impl RateLimitTracker {
    fn new(window_size: Duration) -> Self {
        Self {
            requests: Vec::new(),
            window_start: Instant::now(),
            window_size,
        }
    }

    fn add_request(&mut self) -> bool {
        let now = Instant::now();

        // Reset window if expired
        if now - self.window_start > self.window_size {
            self.requests.clear();
            self.window_start = now;
        }

        // Check if limit exceeded
        if self.requests.len() >= 100 {
            return false;
        }

        self.requests.push(now);
        true
    }

    fn is_rate_limited(&self, max_requests: u32) -> bool {
        let now = Instant::now();

        // Remove expired requests
        let active_requests: Vec<_> = self.requests
            .iter()
            .filter(|&&req_time| now - req_time <= self.window_size)
            .cloned()
            .collect();

        active_requests.len() >= max_requests as usize
    }
}

/// Rate limiting state
#[derive(Debug)]
pub struct RateLimitState {
    ip_limits: Arc<RwLock<HashMap<String, RateLimitTracker>>>,
    user_limits: Arc<RwLock<HashMap<String, RateLimitTracker>>>,
    config: RateLimitConfig,
}

impl RateLimitState {
    pub fn new(config: RateLimitConfig) -> Self {
        let window_size = Duration::from_secs(config.window_seconds as u64);

        Self {
            ip_limits: Arc::new(RwLock::new(HashMap::new())),
            user_limits: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    pub async fn check_rate_limit(&self, ip: &str, user_id: Option<&str>) -> bool {
        let window_size = Duration::from_secs(self.config.window_seconds as u64);

        // Check IP-based limit
        if self.config.limit_by_ip {
            let mut ip_limits = self.ip_limits.write().await;
            let tracker = ip_limits.entry(ip.to_string()).or_insert_with(|| {
                RateLimitTracker::new(window_size)
            });

            if tracker.is_rate_limited(self.config.max_requests) {
                warn!(ip = %ip, "IP rate limit exceeded");
                return false;
            }
            tracker.add_request();
        }

        // Check user-based limit
        if let Some(user_id) = user_id {
            if self.config.limit_by_user {
                let mut user_limits = self.user_limits.write().await;
                let tracker = user_limits.entry(user_id.to_string()).or_insert_with(|| {
                    RateLimitTracker::new(window_size)
                });

                if tracker.is_rate_limited(self.config.max_requests) {
                    warn!(user_id = %user_id, "User rate limit exceeded");
                    return false;
                }
                tracker.add_request();
            }
        }

        true
    }
}

/// Rate limiting middleware
#[derive(Clone)]
pub struct RateLimitMiddleware {
    state: Arc<RateLimitState>,
}

impl RateLimitMiddleware {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            state: Arc::new(RateLimitState::new(config)),
        }
    }
}

// TODO: Reimplement as middleware function for Axum 0.7
// #[async_trait]
// impl<S> axum::middleware::Next<S> for RateLimitMiddleware
// where
//     S: Clone + Send + Sync + 'static,
// {
//     type Output = Response;
#[allow(dead_code)]
impl RateLimitMiddleware {
    async fn run(
        &self,
        req: Request,
        next: Next,
    ) -> Response {
        // Extract IP address
        let ip = req.extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|addr| addr.ip().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        // Extract user ID from headers (simplified)
        let user_id = req.headers()
            .get("authorization")
            .and_then(|h| h.to_str().ok())
            .map(|_| "authenticated_user"); // In real implementation, decode JWT

        // Check rate limit
        if !self.state.check_rate_limit(&ip, user_id).await {
            warn!(ip = %ip, user_id = ?user_id, "Rate limit exceeded");
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(serde_json::json!({
                    "error": "Rate limit exceeded",
                    "message": "Too many requests. Please try again later."
                }))
            ).into_response();
        }

        let response = next.run(req).await;
        response
    }
}

/// Security headers configuration
#[derive(Debug, Clone)]
pub struct SecurityHeadersConfig {
    /// Content Security Policy
    pub content_security_policy: Option<String>,
    /// Whether to enable HSTS
    pub enable_hsts: bool,
    /// HSTS max age in seconds
    pub hsts_max_age: u64,
    /// Whether to include subdomains in HSTS
    pub hsts_include_subdomains: bool,
    /// Whether to enable HSTS preload
    pub hsts_preload: bool,
    /// X-Frame-Options
    pub frame_options: String,
    /// X-Content-Type-Options
    pub content_type_options: String,
    /// Referrer Policy
    pub referrer_policy: String,
    /// Permissions Policy
    pub permissions_policy: Option<String>,
}

impl Default for SecurityHeadersConfig {
    fn default() -> Self {
        Self {
            content_security_policy: Some(
                "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self' ws: wss:; frame-ancestors 'none';".to_string()
            ),
            enable_hsts: true,
            hsts_max_age: 31536000, // 1 year
            hsts_include_subdomains: true,
            hsts_preload: false,
            frame_options: "DENY".to_string(),
            content_type_options: "nosniff".to_string(),
            referrer_policy: "strict-origin-when-cross-origin".to_string(),
            permissions_policy: Some(
                "camera=(), microphone=(), geolocation=(), payment=()".to_string()
            ),
        }
    }
}

/// Security headers middleware
#[derive(Clone)]
pub struct SecurityHeadersMiddleware {
    config: SecurityHeadersConfig,
}

impl SecurityHeadersMiddleware {
    pub fn new(config: SecurityHeadersConfig) -> Self {
        Self { config }
    }
}

// TODO: Reimplement as middleware function for Axum 0.7
#[allow(dead_code)]
impl SecurityHeadersMiddleware {
    async fn run(
        &self,
        req: Request,
        next: Next,
    ) -> Response {
        let mut response = next.run(req).await;

        let headers = response.headers_mut();

        // Content Security Policy
        if let Some(csp) = &self.config.content_security_policy {
            headers.insert(
                header::CONTENT_SECURITY_POLICY,
                HeaderValue::from_str(csp).unwrap_or_else(|_| HeaderValue::from_static("default-src 'self'")),
            );
        }

        // HSTS
        if self.config.enable_hsts {
            let hsts_value = format!(
                "max-age={}; includeSubDomains{}",
                self.config.hsts_max_age,
                if self.config.hsts_preload { "; preload" } else { "" }
            );
            headers.insert(
                "Strict-Transport-Security",
                HeaderValue::from_str(&hsts_value).unwrap_or_else(|_| HeaderValue::from_static("max-age=31536000")),
            );
        }

        // X-Frame-Options
        headers.insert(
            "X-Frame-Options",
            HeaderValue::from_str(&self.config.frame_options).unwrap_or_else(|_| HeaderValue::from_static("DENY")),
        );

        // X-Content-Type-Options
        headers.insert(
            "X-Content-Type-Options",
            HeaderValue::from_str(&self.config.content_type_options).unwrap_or_else(|_| HeaderValue::from_static("nosniff")),
        );

        // Referrer Policy
        headers.insert(
            "Referrer-Policy",
            HeaderValue::from_str(&self.config.referrer_policy)
                .unwrap_or_else(|_| HeaderValue::from_static("strict-origin-when-cross-origin")),
        );

        // Permissions Policy
        if let Some(policy) = &self.config.permissions_policy {
            headers.insert(
                "Permissions-Policy",
                HeaderValue::from_str(policy)
                    .unwrap_or_else(|_| HeaderValue::from_static("")),
            );
        }

        // X-XSS-Protection
        headers.insert("X-XSS-Protection", HeaderValue::from_static("1; mode=block"));

        // Remove server header for security
        headers.remove("server");

        response
    }
}

/// Input validation utilities
pub struct InputValidator;

impl InputValidator {
    /// Validate and sanitize a string input
    pub fn sanitize_string(input: &str, max_length: usize) -> Result<String, ValidationError> {
        if input.len() > max_length {
            return Err(ValidationError::new("string_too_long"));
        }

        // Remove potentially dangerous characters
        let sanitized = input
            .chars()
            .filter(|&c| !matches!(c, '\x00'..='\x08' | '\x0b'..='\x0c' | '\x0e'..='\x1f'))
            .collect::<String>();

        if sanitized.is_empty() && !input.is_empty() {
            return Err(ValidationError::new("invalid_characters"));
        }

        Ok(sanitized)
    }

    /// Validate a file path to prevent directory traversal
    pub fn validate_file_path(path: &str, base_dir: &str) -> Result<String, ValidationError> {
        use std::path::Path;

        // Normalize the path
        let path = Path::new(path);
        let normalized = path.canonicalize()
            .map_err(|_| ValidationError::new("invalid_path"))?;

        let base = Path::new(base_dir).canonicalize()
            .map_err(|_| ValidationError::new("invalid_base_path"))?;

        // Check if the path is within the base directory
        if !normalized.starts_with(base) {
            return Err(ValidationError::new("path_traversal_attempt"));
        }

        Ok(normalized.to_string_lossy().into_owned())
    }

    /// Validate SQL query to prevent injection
    pub fn validate_sql_query(query: &str) -> Result<(), ValidationError> {
        // Check for dangerous SQL patterns
        let dangerous_patterns = [
            (";", "semicolon"),
            ("--", "comment"),
            ("/*", "block_comment"),
            ("xp_", "xp_command"),
            ("sp_", "sp_command"),
            ("union", "union"),
            ("drop", "drop"),
            ("delete", "delete"),
            ("truncate", "truncate"),
            ("exec(", "execute"),
            ("execute(", "execute"),
        ];

        let query_lower = query.to_lowercase();

        for (pattern, desc) in &dangerous_patterns {
            if query_lower.contains(pattern) {
                warn!(pattern = %desc, query = %query, "Potential SQL injection detected");
                return Err(ValidationError::new("sql_injection_attempt"));
            }
        }

        Ok(())
    }

    /// Validate email format
    pub fn validate_email(email: &str) -> Result<(), ValidationError> {
        let email_regex = Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
            .map_err(|_| ValidationError::new("invalid_email_regex"))?;

        if !email_regex.is_match(email) {
            return Err(ValidationError::new("invalid_email_format"));
        }

        if email.len() > 254 {
            return Err(ValidationError::new("email_too_long"));
        }

        Ok(())
    }
}

/// Validation middleware for request bodies
#[derive(Clone)]
pub struct ValidationMiddleware;

// TODO: Reimplement as middleware function for Axum 0.7
#[allow(dead_code)]
impl ValidationMiddleware {
    async fn run(
        &self,
        req: Request,
        next: Next,
    ) -> Response {
        let uri = req.uri().clone();
        let method = req.method().clone();

        // Skip validation for certain endpoints
        if should_skip_validation(&uri, &method) {
            return next.run(req).await;
        }

        // For POST/PUT requests with JSON bodies, validate content type
        if (method == Method::POST || method == Method::PUT)
            && req.headers().get(header::CONTENT_TYPE)
                .and_then(|h| h.to_str().ok())
                .map(|ct| !ct.starts_with("application/json"))
                .unwrap_or(false) {

            warn!(uri = %uri, method = %method, "Invalid content type for JSON endpoint");
            return (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                Json(serde_json::json!({
                    "error": "invalid_content_type",
                    "message": "Content-Type must be application/json"
                }))
            ).into_response();
        }

        let response = next.run(req).await;
        response
    }
}

fn should_skip_validation(uri: &Uri, method: &Method) -> bool {
    let path = uri.path();

    // Skip validation for file uploads, static files, and web console
    path.starts_with("/console/")
        || path.starts_with("/v1/files/")
        || method == Method::GET
        || method == Method::OPTIONS
}

/// Apply rate limiting to requests
pub async fn apply_rate_limit(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    req: Request,
    next: Next,
) -> Response {
    // Check rate limit (simplified implementation)
    let ip = addr.ip().to_string();

    // Extract user ID from Authorization header if present
    let user_id = req.headers()
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|auth| {
            if auth.starts_with("Bearer ") {
                Some("authenticated_user".to_string())
            } else {
                None
            }
        });

    // For now, implement simple in-memory rate limiting
    // In production, you'd want to use Redis or similar
    static RATE_LIMITS: std::sync::OnceLock<tokio::sync::RwLock<HashMap<String, Vec<std::time::Instant>>>> =
        std::sync::OnceLock::new();

    let limits = RATE_LIMITS.get_or_init(||
        tokio::sync::RwLock::new(HashMap::new())
    );

    let key = format!("{}:{}", ip, user_id.unwrap_or_else(|| "anonymous".to_string()));
    let now = std::time::Instant::now();

    {
        let mut limits_guard = limits.write().await;
        let requests = limits_guard.entry(key.clone()).or_insert_with(Vec::new);

        // Remove old requests (older than 60 seconds)
        requests.retain(|&req_time| now.duration_since(req_time).as_secs() < 60);

        // Check if rate limit exceeded (100 requests per minute)
        if requests.len() >= 100 {
            warn!(key = %key, "Rate limit exceeded");
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(serde_json::json!({
                    "error": "Rate limit exceeded",
                    "message": "Too many requests. Please try again later."
                }))
            ).into_response();
        }

        requests.push(now);
    }

    next.run(req).await
}

/// Create security middleware stack
pub fn create_security_middleware() -> tower::ServiceBuilder<axum::routing::IntoMakeService<axum::Router>> {
    let cors = CorsLayer::new()
        .allow_origin(if cfg!(debug_assertions) {
            "http://localhost:3000".parse::<HeaderValue>().unwrap()
        } else {
            "https://your-domain.com".parse::<HeaderValue>().unwrap()
        })
        .allow_methods(vec![
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers(vec![
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            header::ACCEPT,
        ])
        .max_age(Duration::from_secs(86400));

    tower::ServiceBuilder::new()
        .layer(SetSensitiveHeadersLayer::new(std::iter::once(header::AUTHORIZATION)))
        .layer(TraceLayer::new_for_http())
        .layer(NormalizePathLayer::trim_trailing_slash())
        .layer(RequestBodyLimitLayer::new(10 * 1024 * 1024)) // 10MB limit
        .layer(cors)
        .layer(SecurityHeadersMiddleware::new(SecurityHeadersConfig::default()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_input_validator_sanitize_string() {
        let result = InputValidator::sanitize_string("hello\x00world", 100);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "helloworld");

        let result = InputValidator::sanitize_string("a".repeat(1001), 1000);
        assert!(result.is_err());
    }

    #[test]
    fn test_input_validator_validate_email() {
        assert!(InputValidator::validate_email("test@example.com").is_ok());
        assert!(InputValidator::validate_email("invalid-email").is_err());
        assert!(InputValidator::validate_email("a".repeat(255)).is_err());
    }

    #[test]
    fn test_input_validator_validate_sql_query() {
        assert!(InputValidator::validate_sql_query("SELECT * FROM users").is_ok());
        assert!(InputValidator::validate_sql_query("SELECT * FROM users; DROP TABLE users").is_err());
        assert!(InputValidator::validate_sql_query("SELECT * FROM users -- comment").is_err());
    }
}