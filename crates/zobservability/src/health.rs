//! Health check system for ZRUSTDB components

use anyhow::Result;
use axum::{extract::State, http::StatusCode, response::Json, routing::get, Router};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, sync::RwLock, time::timeout};

/// Health check status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HealthStatus {
    Healthy,
    Warning,
    Critical,
    Unknown,
}

/// Individual health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResult {
    pub name: String,
    pub status: HealthStatus,
    pub message: String,
    pub duration_ms: u64,
    pub last_checked: chrono::DateTime<chrono::Utc>,
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Overall system health response
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: HealthStatus,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub checks: Vec<HealthCheckResult>,
    pub summary: HealthSummary,
}

/// Health check summary
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthSummary {
    pub total_checks: usize,
    pub healthy_count: usize,
    pub warning_count: usize,
    pub critical_count: usize,
    pub unknown_count: usize,
    pub overall_duration_ms: u64,
}

/// Health check trait that components implement
#[async_trait::async_trait]
pub trait HealthCheck: Send + Sync {
    /// Name of this health check
    fn name(&self) -> &str;

    /// Perform the health check
    async fn check(&self) -> HealthCheckResult;

    /// Timeout for this health check
    fn timeout(&self) -> Duration {
        Duration::from_secs(30)
    }

    /// Whether this health check is critical for overall system health
    fn is_critical(&self) -> bool {
        true
    }
}

/// Health check manager
pub struct HealthManager {
    checks: RwLock<Vec<Box<dyn HealthCheck>>>,
    last_results: RwLock<Vec<HealthCheckResult>>,
    check_interval: Duration,
    running: RwLock<bool>,
}

impl HealthManager {
    pub fn new() -> Self {
        Self {
            checks: RwLock::new(Vec::new()),
            last_results: RwLock::new(Vec::new()),
            check_interval: Duration::from_secs(30),
            running: RwLock::new(false),
        }
    }

    /// Register a health check
    pub async fn register_check(&self, check: Box<dyn HealthCheck>) {
        let mut checks = self.checks.write().await;
        checks.push(check);
    }

    /// Start periodic health checking
    pub async fn start(self: Arc<Self>) -> Result<()> {
        {
            let mut running = self.running.write().await;
            if *running {
                return Ok(());
            }
            *running = true;
        }

        // Perform initial health check
        self.run_all_checks().await;

        // Start periodic checking
        let manager = Arc::clone(&self);
        let interval = self.check_interval;

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);

            loop {
                ticker.tick().await;

                let running = *manager.running.read().await;
                if !running {
                    break;
                }

                manager.run_all_checks().await;
            }
        });

        Ok(())
    }

    /// Stop health checking
    pub async fn shutdown(&self) -> Result<()> {
        let mut running = self.running.write().await;
        *running = false;
        Ok(())
    }

    /// Run all registered health checks
    pub async fn run_all_checks(&self) {
        let checks = self.checks.read().await;
        let mut results = Vec::new();

        let start_time = Instant::now();

        // Run checks concurrently
        let check_futures: Vec<_> = checks
            .iter()
            .map(|check| {
                let timeout_duration = check.timeout();
                async move {
                    match timeout(timeout_duration, check.check()).await {
                        Ok(result) => result,
                        Err(_) => HealthCheckResult {
                            name: check.name().to_string(),
                            status: HealthStatus::Critical,
                            message: "Health check timed out".to_string(),
                            duration_ms: timeout_duration.as_millis() as u64,
                            last_checked: chrono::Utc::now(),
                            metadata: HashMap::new(),
                        },
                    }
                }
            })
            .collect();

        let check_results = futures::future::join_all(check_futures).await;
        results.extend(check_results);

        // Update stored results
        let mut last_results = self.last_results.write().await;
        *last_results = results;

        let duration = start_time.elapsed();
        tracing::debug!("Health checks completed in {:?}", duration);
    }

    /// Get current health status
    pub async fn get_health(&self) -> HealthResponse {
        let results = self.last_results.read().await;
        let timestamp = chrono::Utc::now();

        let healthy_count = results.iter().filter(|r| r.status == HealthStatus::Healthy).count();
        let warning_count = results.iter().filter(|r| r.status == HealthStatus::Warning).count();
        let critical_count = results.iter().filter(|r| r.status == HealthStatus::Critical).count();
        let unknown_count = results.iter().filter(|r| r.status == HealthStatus::Unknown).count();

        let overall_status = if critical_count > 0 {
            HealthStatus::Critical
        } else if warning_count > 0 {
            HealthStatus::Warning
        } else if unknown_count > 0 {
            HealthStatus::Unknown
        } else {
            HealthStatus::Healthy
        };

        let total_duration = results.iter().map(|r| r.duration_ms).sum();

        HealthResponse {
            status: overall_status,
            timestamp,
            checks: results.clone(),
            summary: HealthSummary {
                total_checks: results.len(),
                healthy_count,
                warning_count,
                critical_count,
                unknown_count,
                overall_duration_ms: total_duration,
            },
        }
    }
}

/// Database connectivity health check
pub struct DatabaseHealthCheck {
    name: String,
    // In a real implementation, this would hold a database connection pool
}

impl DatabaseHealthCheck {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait::async_trait]
impl HealthCheck for DatabaseHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self) -> HealthCheckResult {
        let start = Instant::now();

        // Simulate database health check
        // In reality, this would:
        // 1. Test database connection
        // 2. Run a simple query
        // 3. Check replication lag
        // 4. Verify tablespace/disk space

        tokio::time::sleep(Duration::from_millis(50)).await;

        let duration = start.elapsed();
        let mut metadata = HashMap::new();
        metadata.insert("connection_pool_size".to_string(), serde_json::Value::from(10));
        metadata.insert("active_connections".to_string(), serde_json::Value::from(5));

        HealthCheckResult {
            name: self.name.clone(),
            status: HealthStatus::Healthy,
            message: "Database connection successful".to_string(),
            duration_ms: duration.as_millis() as u64,
            last_checked: chrono::Utc::now(),
            metadata,
        }
    }

    fn is_critical(&self) -> bool {
        true
    }
}

/// Vector index health check
pub struct VectorIndexHealthCheck {
    name: String,
}

impl VectorIndexHealthCheck {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait::async_trait]
impl HealthCheck for VectorIndexHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self) -> HealthCheckResult {
        let start = Instant::now();

        // Simulate vector index health check
        // In reality, this would:
        // 1. Check index integrity
        // 2. Verify index size vs expected
        // 3. Test search performance
        // 4. Check memory usage

        tokio::time::sleep(Duration::from_millis(25)).await;

        let duration = start.elapsed();
        let mut metadata = HashMap::new();
        metadata.insert("index_size".to_string(), serde_json::Value::from(10000));
        metadata.insert("memory_usage_mb".to_string(), serde_json::Value::from(256));

        HealthCheckResult {
            name: self.name.clone(),
            status: HealthStatus::Healthy,
            message: "Vector index operational".to_string(),
            duration_ms: duration.as_millis() as u64,
            last_checked: chrono::Utc::now(),
            metadata,
        }
    }

    fn is_critical(&self) -> bool {
        false
    }
}

/// WebSocket connection health check
pub struct WebSocketHealthCheck {
    name: String,
}

impl WebSocketHealthCheck {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait::async_trait]
impl HealthCheck for WebSocketHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self) -> HealthCheckResult {
        let start = Instant::now();

        // Simulate WebSocket health check
        tokio::time::sleep(Duration::from_millis(10)).await;

        let duration = start.elapsed();
        let mut metadata = HashMap::new();
        metadata.insert("active_connections".to_string(), serde_json::Value::from(15));
        metadata.insert("message_rate_per_sec".to_string(), serde_json::Value::from(120));

        HealthCheckResult {
            name: self.name.clone(),
            status: HealthStatus::Healthy,
            message: "WebSocket server operational".to_string(),
            duration_ms: duration.as_millis() as u64,
            last_checked: chrono::Utc::now(),
            metadata,
        }
    }

    fn is_critical(&self) -> bool {
        false
    }
}

/// System resource health check
pub struct SystemResourceHealthCheck {
    name: String,
    cpu_threshold: f32,
    memory_threshold: f32,
}

impl SystemResourceHealthCheck {
    pub fn new(name: String, cpu_threshold: f32, memory_threshold: f32) -> Self {
        Self {
            name,
            cpu_threshold,
            memory_threshold,
        }
    }
}

#[async_trait::async_trait]
impl HealthCheck for SystemResourceHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self) -> HealthCheckResult {
        let start = Instant::now();

        let mut system = sysinfo::System::new();
        system.refresh_cpu();
        system.refresh_memory();

        let cpu_usage = system.global_cpu_info().cpu_usage();
        let memory_usage = (system.used_memory() as f64 / system.total_memory() as f64) * 100.0;

        let status = if cpu_usage > self.cpu_threshold || memory_usage > self.memory_threshold as f64 {
            HealthStatus::Warning
        } else {
            HealthStatus::Healthy
        };

        let message = format!(
            "CPU: {:.1}%, Memory: {:.1}%",
            cpu_usage, memory_usage
        );

        let duration = start.elapsed();
        let mut metadata = HashMap::new();
        metadata.insert("cpu_usage_percent".to_string(), serde_json::Value::from(cpu_usage));
        metadata.insert("memory_usage_percent".to_string(), serde_json::Value::from(memory_usage));
        metadata.insert("total_memory_gb".to_string(), serde_json::Value::from(system.total_memory() / 1_000_000_000));

        HealthCheckResult {
            name: self.name.clone(),
            status,
            message,
            duration_ms: duration.as_millis() as u64,
            last_checked: chrono::Utc::now(),
            metadata,
        }
    }

    fn is_critical(&self) -> bool {
        true
    }
}

/// Start health check HTTP server
pub async fn start_health_server(
    health_manager: Arc<HealthManager>,
    addr: SocketAddr,
) -> Result<()> {
    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/health/live", get(liveness_handler))
        .route("/health/ready", get(readiness_handler))
        .with_state(health_manager);

    let listener = TcpListener::bind(addr).await?;
    tracing::info!("Health check server listening on {}", addr);

    axum::serve(listener, app).await?;
    Ok(())
}

/// Full health check endpoint
async fn health_handler(
    State(health_manager): State<Arc<HealthManager>>,
) -> Result<Json<HealthResponse>, StatusCode> {
    let health = health_manager.get_health().await;

    let status_code = match health.status {
        HealthStatus::Healthy => StatusCode::OK,
        HealthStatus::Warning => StatusCode::OK,
        HealthStatus::Critical => StatusCode::SERVICE_UNAVAILABLE,
        HealthStatus::Unknown => StatusCode::SERVICE_UNAVAILABLE,
    };

    Ok(Json(health))
}

/// Kubernetes liveness probe endpoint
async fn liveness_handler() -> StatusCode {
    // Simple liveness check - just return OK if the service is running
    StatusCode::OK
}

/// Kubernetes readiness probe endpoint
async fn readiness_handler(
    State(health_manager): State<Arc<HealthManager>>,
) -> StatusCode {
    let health = health_manager.get_health().await;

    match health.status {
        HealthStatus::Healthy => StatusCode::OK,
        HealthStatus::Warning => StatusCode::OK,
        HealthStatus::Critical => StatusCode::SERVICE_UNAVAILABLE,
        HealthStatus::Unknown => StatusCode::SERVICE_UNAVAILABLE,
    }
}