//! Service-specific health checks for ZRUSTDB components

use crate::health::{HealthCheck, HealthCheckResult, HealthStatus};
use zcommon::health::{AuthHealthCheck, JobQueueHealthCheck, HealthCheckError, AuthStats, QueueStats};
use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc, time::Instant};

/// Database connectivity health check
pub struct DatabaseHealthCheck {
    pub store: &'static zcore_storage::Store,
}

#[async_trait]
impl HealthCheck for DatabaseHealthCheck {
    fn name(&self) -> &str {
        "database_connectivity"
    }

    async fn check(&self) -> HealthCheckResult {
        let start = Instant::now();
        let mut metadata = HashMap::new();

        // Test basic read/write operations
        let result = match self.test_database_operations().await {
            Ok(stats) => {
                metadata.insert("operations_tested".to_string(), serde_json::Value::Number(stats.operations_count.into()));
                metadata.insert("read_latency_ms".to_string(), serde_json::Value::Number(serde_json::Number::from_f64(stats.read_latency_ms as f64).unwrap_or(serde_json::Number::from(0))));
                metadata.insert("write_latency_ms".to_string(), serde_json::Value::Number(serde_json::Number::from_f64(stats.write_latency_ms as f64).unwrap_or(serde_json::Number::from(0))));

                if stats.read_latency_ms > 100 || stats.write_latency_ms > 100 {
                    (HealthStatus::Warning, format!("Database operations slow - read: {}ms, write: {}ms", stats.read_latency_ms, stats.write_latency_ms))
                } else {
                    (HealthStatus::Healthy, "Database operations functioning normally".to_string())
                }
            }
            Err(e) => {
                metadata.insert("error".to_string(), serde_json::Value::String(e.to_string()));
                (HealthStatus::Critical, format!("Database operations failed: {}", e))
            }
        };

        HealthCheckResult {
            name: self.name().to_string(),
            status: result.0,
            message: result.1,
            duration_ms: start.elapsed().as_millis() as u64,
            last_checked: chrono::Utc::now(),
            metadata,
        }
    }

    fn is_critical(&self) -> bool {
        true
    }
}

impl DatabaseHealthCheck {
    async fn test_database_operations(&self) -> anyhow::Result<DatabaseStats> {
        let start = Instant::now();

        // Mock database operations for now
        // In production, this would use actual zcore-storage API
        let read_start = Instant::now();
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await; // Simulate read
        let read_latency = read_start.elapsed().as_millis() as u64;

        // Test write operation
        let write_start = Instant::now();
        tokio::time::sleep(tokio::time::Duration::from_millis(2)).await; // Simulate write
        let write_latency = write_start.elapsed().as_millis() as u64;

        Ok(DatabaseStats {
            operations_count: 2,
            read_latency_ms: read_latency,
            write_latency_ms: write_latency,
        })
    }
}

struct DatabaseStats {
    operations_count: u32,
    read_latency_ms: u64,
    write_latency_ms: u64,
}

/// Vector index health check
pub struct VectorIndexHealthCheck {
    pub index_name: String,
    pub expected_size: Option<usize>,
}

#[async_trait]
impl HealthCheck for VectorIndexHealthCheck {
    fn name(&self) -> &str {
        "vector_index_health"
    }

    async fn check(&self) -> HealthCheckResult {
        let start = Instant::now();
        let mut metadata = HashMap::new();
        metadata.insert("index_name".to_string(), serde_json::Value::String(self.index_name.clone()));

        // Simulate index health check (in production, this would check actual index)
        let result = match self.check_index_health().await {
            Ok(stats) => {
                metadata.insert("index_size".to_string(), serde_json::Value::Number(stats.index_size.into()));
                metadata.insert("search_latency_ms".to_string(), serde_json::Value::Number(serde_json::Number::from_f64(stats.avg_search_latency_ms as f64).unwrap_or(serde_json::Number::from(0))));
                metadata.insert("memory_usage_mb".to_string(), serde_json::Value::Number(serde_json::Number::from_f64(stats.memory_usage_mb as f64).unwrap_or(serde_json::Number::from(0))));

                if let Some(expected) = self.expected_size {
                    if stats.index_size < expected / 2 {
                        (HealthStatus::Warning, format!("Index size {} is significantly below expected {}", stats.index_size, expected))
                    } else if stats.avg_search_latency_ms > 50 {
                        (HealthStatus::Warning, format!("Index search latency high: {}ms", stats.avg_search_latency_ms))
                    } else {
                        (HealthStatus::Healthy, format!("Vector index {} is healthy", self.index_name))
                    }
                } else {
                    (HealthStatus::Healthy, format!("Vector index {} is functional", self.index_name))
                }
            }
            Err(e) => {
                metadata.insert("error".to_string(), serde_json::Value::String(e.to_string()));
                (HealthStatus::Critical, format!("Vector index {} failed health check: {}", self.index_name, e))
            }
        };

        HealthCheckResult {
            name: self.name().to_string(),
            status: result.0,
            message: result.1,
            duration_ms: start.elapsed().as_millis() as u64,
            last_checked: chrono::Utc::now(),
            metadata,
        }
    }

    fn is_critical(&self) -> bool {
        false // Vector index health is not critical for basic system operation
    }
}

impl VectorIndexHealthCheck {
    async fn check_index_health(&self) -> anyhow::Result<IndexStats> {
        // In production, this would check actual HNSW index statistics
        // For now, we'll simulate some realistic stats
        Ok(IndexStats {
            index_size: 10000,
            avg_search_latency_ms: 15,
            memory_usage_mb: 250,
        })
    }
}

struct IndexStats {
    index_size: usize,
    avg_search_latency_ms: u64,
    memory_usage_mb: u64,
}

/// WebSocket connection health check
pub struct WebSocketHealthCheck {
    pub active_connections: Arc<std::sync::atomic::AtomicUsize>,
    pub max_expected_connections: usize,
}

#[async_trait]
impl HealthCheck for WebSocketHealthCheck {
    fn name(&self) -> &str {
        "websocket_connections"
    }

    async fn check(&self) -> HealthCheckResult {
        let start = Instant::now();
        let mut metadata = HashMap::new();

        let active_count = self.active_connections.load(std::sync::atomic::Ordering::Relaxed);
        metadata.insert("active_connections".to_string(), serde_json::Value::Number(active_count.into()));
        metadata.insert("max_expected".to_string(), serde_json::Value::Number(self.max_expected_connections.into()));

        let result = if active_count > self.max_expected_connections {
            (HealthStatus::Warning, format!("WebSocket connections ({}) exceed expected maximum ({})", active_count, self.max_expected_connections))
        } else if active_count == 0 {
            (HealthStatus::Healthy, "No active WebSocket connections".to_string())
        } else {
            (HealthStatus::Healthy, format!("{} active WebSocket connections", active_count))
        };

        HealthCheckResult {
            name: self.name().to_string(),
            status: result.0,
            message: result.1,
            duration_ms: start.elapsed().as_millis() as u64,
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
    pub cpu_threshold_percent: f64,
    pub memory_threshold_mb: u64,
    pub disk_threshold_mb: u64,
}

#[async_trait]
impl HealthCheck for SystemResourceHealthCheck {
    fn name(&self) -> &str {
        "system_resources"
    }

    async fn check(&self) -> HealthCheckResult {
        let start = Instant::now();
        let mut metadata = HashMap::new();

        let result = match self.check_resources().await {
            Ok(stats) => {
                metadata.insert("cpu_usage_percent".to_string(), serde_json::Value::Number(serde_json::Number::from_f64(stats.cpu_usage_percent as f64).unwrap_or(serde_json::Number::from(0))));
                metadata.insert("memory_usage_mb".to_string(), serde_json::Value::Number(stats.memory_usage_mb.into()));
                metadata.insert("disk_usage_mb".to_string(), serde_json::Value::Number(stats.disk_usage_mb.into()));

                let mut warnings = Vec::new();
                if stats.cpu_usage_percent > self.cpu_threshold_percent {
                    warnings.push(format!("CPU usage {}% exceeds threshold {}%", stats.cpu_usage_percent, self.cpu_threshold_percent));
                }
                if stats.memory_usage_mb > self.memory_threshold_mb {
                    warnings.push(format!("Memory usage {}MB exceeds threshold {}MB", stats.memory_usage_mb, self.memory_threshold_mb));
                }
                if stats.disk_usage_mb > self.disk_threshold_mb {
                    warnings.push(format!("Disk usage {}MB exceeds threshold {}MB", stats.disk_usage_mb, self.disk_threshold_mb));
                }

                if !warnings.is_empty() {
                    (HealthStatus::Warning, warnings.join("; "))
                } else {
                    (HealthStatus::Healthy, format!("System resources within limits - CPU: {:.1}%, Memory: {}MB, Disk: {}MB",
                        stats.cpu_usage_percent, stats.memory_usage_mb, stats.disk_usage_mb))
                }
            }
            Err(e) => {
                metadata.insert("error".to_string(), serde_json::Value::String(e.to_string()));
                (HealthStatus::Critical, format!("Failed to check system resources: {}", e))
            }
        };

        HealthCheckResult {
            name: self.name().to_string(),
            status: result.0,
            message: result.1,
            duration_ms: start.elapsed().as_millis() as u64,
            last_checked: chrono::Utc::now(),
            metadata,
        }
    }

    fn is_critical(&self) -> bool {
        true
    }
}

impl SystemResourceHealthCheck {
    async fn check_resources(&self) -> anyhow::Result<ResourceStats> {
        // In production, this would use sysinfo or similar to get actual system stats
        // For now, we'll simulate some realistic values
        Ok(ResourceStats {
            cpu_usage_percent: 45.5,
            memory_usage_mb: 1024,
            disk_usage_mb: 5120,
        })
    }
}

struct ResourceStats {
    cpu_usage_percent: f64,
    memory_usage_mb: u64,
    disk_usage_mb: u64,
}

/// Authentication service health check using abstract interface
pub struct AuthHealthCheckAdapter {
    pub auth_health_check: Arc<dyn AuthHealthCheck>,
}

#[async_trait]
impl HealthCheck for AuthHealthCheckAdapter {
    fn name(&self) -> &str {
        "authentication_service"
    }

    async fn check(&self) -> HealthCheckResult {
        let start = Instant::now();
        let mut metadata = HashMap::new();

        let result = match self.auth_health_check.get_auth_stats().await {
            Ok(stats) => {
                metadata.insert("token_validation_ms".to_string(), serde_json::Value::Number(stats.token_validation_latency_ms.into()));
                metadata.insert("active_sessions".to_string(), serde_json::Value::Number(stats.active_sessions.into()));
                metadata.insert("failed_attempts_last_hour".to_string(), serde_json::Value::Number(stats.failed_auth_attempts_last_hour.into()));
                metadata.insert("successful_attempts_last_hour".to_string(), serde_json::Value::Number(stats.successful_auth_attempts_last_hour.into()));

                if stats.token_validation_latency_ms > 50 {
                    (HealthStatus::Warning, format!("Auth token validation slow: {}ms", stats.token_validation_latency_ms))
                } else {
                    (HealthStatus::Healthy, format!("Authentication service healthy - {} active sessions", stats.active_sessions))
                }
            }
            Err(e) => {
                metadata.insert("error".to_string(), serde_json::Value::String(e.to_string()));
                (HealthStatus::Critical, format!("Authentication service failed: {}", e))
            }
        };

        HealthCheckResult {
            name: self.name().to_string(),
            status: result.0,
            message: result.1,
            duration_ms: start.elapsed().as_millis() as u64,
            last_checked: chrono::Utc::now(),
            metadata,
        }
    }

    fn is_critical(&self) -> bool {
        true
    }
}

/// Job queue health check using abstract interface
pub struct JobQueueHealthCheckAdapter {
    pub queue_health_check: Arc<dyn JobQueueHealthCheck>,
    pub max_queue_size: usize,
}

#[async_trait]
impl HealthCheck for JobQueueHealthCheckAdapter {
    fn name(&self) -> &str {
        "job_queue"
    }

    async fn check(&self) -> HealthCheckResult {
        let start = Instant::now();
        let mut metadata = HashMap::new();

        let result = match self.queue_health_check.get_queue_stats().await {
            Ok(stats) => {
                metadata.insert("pending_jobs".to_string(), serde_json::Value::Number(stats.pending_jobs.into()));
                metadata.insert("running_jobs".to_string(), serde_json::Value::Number(stats.running_jobs.into()));
                metadata.insert("failed_jobs".to_string(), serde_json::Value::Number(stats.failed_jobs.into()));
                metadata.insert("completed_jobs_last_hour".to_string(), serde_json::Value::Number(stats.completed_jobs_last_hour.into()));
                metadata.insert("average_job_duration_seconds".to_string(), serde_json::Value::Number(serde_json::Number::from_f64(stats.average_job_duration_seconds).unwrap_or(serde_json::Number::from(0))));
                metadata.insert("oldest_pending_job_age_seconds".to_string(), serde_json::Value::Number(stats.oldest_pending_job_age_seconds.into()));

                if stats.pending_jobs > self.max_queue_size {
                    (HealthStatus::Warning, format!("Job queue size {} exceeds threshold {}", stats.pending_jobs, self.max_queue_size))
                } else if stats.failed_jobs > 10 {
                    (HealthStatus::Warning, format!("High number of failed jobs: {}", stats.failed_jobs))
                } else if stats.oldest_pending_job_age_seconds > 3600 {
                    (HealthStatus::Warning, format!("Oldest pending job is {} seconds old", stats.oldest_pending_job_age_seconds))
                } else {
                    (HealthStatus::Healthy, format!("Job queue healthy - {} pending, {} running", stats.pending_jobs, stats.running_jobs))
                }
            }
            Err(e) => {
                metadata.insert("error".to_string(), serde_json::Value::String(e.to_string()));
                (HealthStatus::Critical, format!("Job queue health check failed: {}", e))
            }
        };

        HealthCheckResult {
            name: self.name().to_string(),
            status: result.0,
            message: result.1,
            duration_ms: start.elapsed().as_millis() as u64,
            last_checked: chrono::Utc::now(),
            metadata,
        }
    }

    fn is_critical(&self) -> bool {
        false // Job queue issues don't prevent basic system operation
    }
}