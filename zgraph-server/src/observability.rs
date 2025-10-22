//! Observability and metrics collection for zGraph server
//!
//! Provides metrics collection, health checks, and structured logging
//! for production monitoring and alerting.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock as TokioRwLock;
use tracing::{debug, error, info, warn};

/// Metrics collector for tracking server performance
#[derive(Debug, Default, Clone)]
pub struct MetricsCollector {
    // Request counters
    total_requests: AtomicU64,
    sql_requests: AtomicU64,
    cypher_requests: AtomicU64,
    knn_requests: AtomicU64,

    // Error counters
    auth_errors: AtomicU64,
    sql_errors: AtomicU64,
    cypher_errors: AtomicU64,
    knn_errors: AtomicU64,

    // Performance metrics
    request_durations: Arc<TokioRwLock<HashMap<String, Vec<Duration>>>>,
    active_connections: AtomicU64,
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            sql_requests: AtomicU64::new(0),
            cypher_requests: AtomicU64::new(0),
            knn_requests: AtomicU64::new(0),
            auth_errors: AtomicU64::new(0),
            sql_errors: AtomicU64::new(0),
            cypher_errors: AtomicU64::new(0),
            knn_errors: AtomicU64::new(0),
            request_durations: Arc::new(TokioRwLock::new(HashMap::new())),
            active_connections: AtomicU64::new(0),
        }
    }
}

impl Clone for MetricsCollector {
    fn clone(&self) -> Self {
        Self {
            total_requests: AtomicU64::new(self.total_requests.load(Ordering::SeqCst)),
            sql_requests: AtomicU64::new(self.sql_requests.load(Ordering::SeqCst)),
            cypher_requests: AtomicU64::new(self.cypher_requests.load(Ordering::SeqCst)),
            knn_requests: AtomicU64::new(self.knn_requests.load(Ordering::SeqCst)),
            auth_errors: AtomicU64::new(self.auth_errors.load(Ordering::SeqCst)),
            sql_errors: AtomicU64::new(self.sql_errors.load(Ordering::SeqCst)),
            cypher_errors: AtomicU64::new(self.cypher_errors.load(Ordering::SeqCst)),
            knn_errors: AtomicU64::new(self.knn_errors.load(Ordering::SeqCst)),
            request_durations: Arc::clone(&self.request_durations),
            active_connections: AtomicU64::new(self.active_connections.load(Ordering::SeqCst)),
        }
    }
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_request(&self, endpoint: &str) {
        self.total_requests.fetch_add(1, Ordering::SeqCst);

        match endpoint {
            "/v1/sql" => { self.sql_requests.fetch_add(1, Ordering::SeqCst); }
            "/v1/cypher" => { self.cypher_requests.fetch_add(1, Ordering::SeqCst); }
            "/v1/knn" => { self.knn_requests.fetch_add(1, Ordering::SeqCst); }
            _ => {}
        }
    }

    pub fn record_auth_error(&self) {
        self.auth_errors.fetch_add(1, Ordering::SeqCst);
    }

    pub fn record_sql_error(&self) {
        self.sql_errors.fetch_add(1, Ordering::SeqCst);
    }

    pub fn record_cypher_error(&self) {
        self.cypher_errors.fetch_add(1, Ordering::SeqCst);
    }

    pub fn record_knn_error(&self) {
        self.knn_errors.fetch_add(1, Ordering::SeqCst);
    }

    pub async fn record_request_duration(&self, endpoint: &str, duration: Duration) {
        let mut durations = self.request_durations.write().await;
        durations.entry(endpoint.to_string())
            .or_insert_with(Vec::new)
            .push(duration);
    }

    pub fn increment_active_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::SeqCst);
    }

    pub fn decrement_active_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::SeqCst);
    }

    pub async fn get_metrics(&self) -> MetricsSnapshot {
        let total_requests = self.total_requests.load(Ordering::SeqCst);
        let sql_requests = self.sql_requests.load(Ordering::SeqCst);
        let cypher_requests = self.cypher_requests.load(Ordering::SeqCst);
        let knn_requests = self.knn_requests.load(Ordering::SeqCst);

        let auth_errors = self.auth_errors.load(Ordering::SeqCst);
        let sql_errors = self.sql_errors.load(Ordering::SeqCst);
        let cypher_errors = self.cypher_errors.load(Ordering::SeqCst);
        let knn_errors = self.knn_errors.load(Ordering::SeqCst);
        let active_connections = self.active_connections.load(Ordering::SeqCst);

        let durations = self.request_durations.read().await;
        let avg_sql_duration = durations
            .get("/v1/sql")
            .map(|d| d.iter().sum::<Duration>() / d.len() as f64);

        let avg_cypher_duration = durations
            .get("/v1/cypher")
            .map(|d| d.iter().sum::<Duration>() / d.len() as f64);

        let avg_knn_duration = durations
            .get("/v1/knn")
            .map(|d| d.iter().sum::<Duration>() / d.len() as f64);

        MetricsSnapshot {
            total_requests,
            sql_requests,
            cypher_requests,
            knn_requests,
            auth_errors,
            sql_errors,
            cypher_errors,
            knn_errors,
            active_connections,
            avg_sql_duration,
            avg_cypher_duration,
            avg_knn_duration,
        }
    }
}

/// Current metrics snapshot
#[derive(Debug, Serialize)]
pub struct MetricsSnapshot {
    pub total_requests: u64,
    pub sql_requests: u64,
    pub cypher_requests: u64,
    pub knn_requests: u64,
    pub auth_errors: u64,
    pub sql_errors: u64,
    pub cypher_errors: u64,
    pub knn_errors: u64,
    pub active_connections: u64,
    pub avg_sql_duration: Option<f64>,
    pub avg_cypher_duration: Option<f64>,
    pub avg_knn_duration: Option<f64>,
}

/// Health check status
#[derive(Debug, Serialize)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

impl Default for HealthStatus {
    fn default() -> Self {
        Self::Healthy
    }
}

/// Health check response
#[derive(Debug, Serialize)]
pub struct HealthCheck {
    pub status: HealthStatus,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub version: String,
    pub uptime_seconds: u64,
    pub details: HashMap<String, String>,
}

/// Database health check
pub async fn database_health(
    storage: Arc<zcore_storage::Store>,
    catalog: Arc<zcore_catalog::Catalog>,
) -> HealthCheck {
    let start_time = Instant::now();

    // Test basic storage operations
    let storage_healthy = match storage.ping().await {
        Ok(_) => {
            info!("Storage ping successful");
            true
        }
        Err(err) => {
            error!("Storage ping failed: {}", err);
            false
        }
    };

    // Test catalog operations
    let catalog_healthy = match catalog.ping().await {
        Ok(_) => {
            info!("Catalog ping successful");
            true
        }
        Err(err) => {
            error!("Catalog ping failed: {}", err);
            false
        }
    };

    let overall_status = match (storage_healthy, catalog_healthy) {
        (true, true) => HealthStatus::Healthy,
        (true, false) | (false, true) => HealthStatus::Degraded,
        (false, false) => HealthStatus::Unhealthy,
    };

    let uptime = start_time.elapsed().as_secs();

    let mut details = HashMap::new();
    if !storage_healthy {
        details.insert("storage".to_string(), "Storage operations failing".to_string());
    }
    if !catalog_healthy {
        details.insert("catalog".to_string(), "Catalog operations failing".to_string());
    }

    HealthCheck {
        status: overall_status,
        timestamp: chrono::Utc::now(),
        version: env!("CARGO_PKG_VERSION").unwrap_or_else(|_| "unknown".to_string()),
        uptime_seconds: uptime,
        details,
    }
}

/// Vector index health check
pub async fn vector_index_health(
    storage: Arc<zcore_storage::Store>,
    catalog: Arc<zcore_catalog::Catalog>,
    org_id: u64,
    table: &str,
) -> HealthCheck {
    let start_time = Instant::now();

    // Check if table exists and has vector index
    let table_healthy = match catalog.get_table_by_name(org_id, table).await {
        Some(table_def) => {
            table_def.columns.iter().any(|c| matches!(c.column_type, "Vector")).is_some()
        }
        None => false,
    };

    let overall_status = if table_healthy {
        HealthStatus::Healthy
    } else {
        HealthStatus::Degraded
    };

    let uptime = start_time.elapsed().as_secs();

    let mut details = HashMap::new();
    if !table_healthy {
        details.insert("vector_index".to_string(), "Vector index not available".to_string());
    }

    HealthCheck {
        status: overall_status,
        timestamp: chrono::Utc::now(),
        version: env!("CARGO_PKG_VERSION").unwrap_or_else(|_| "unknown".to_string()),
        uptime_seconds: uptime,
        details,
    }
}

#[derive(Debug, Serialize)]
pub struct ReadyResponse {
    pub ready: bool,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub version: String,
    pub message: String,
}

/// Server ready check
pub async fn ready_check(
    storage: Arc<zcore_storage::Store>,
    catalog: Arc<zcore_catalog::Catalog>,
) -> ReadyResponse {
    ReadyResponse {
        ready: true, // For now, always ready
        timestamp: chrono::Utc::now(),
        version: env!("CARGO_PKG_VERSION").unwrap_or_else(|_| "unknown".to_string()),
        message: "zGraph server is ready to accept queries".to_string(),
    }
}