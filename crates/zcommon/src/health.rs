//! Health check interfaces to break circular dependencies

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use chrono::{DateTime, Utc};

/// Health status levels
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Warning,
    Critical,
}

/// Health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResult {
    pub name: String,
    pub status: HealthStatus,
    pub message: String,
    pub duration_ms: u64,
    pub last_checked: DateTime<Utc>,
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Abstract health check interface
#[async_trait]
pub trait HealthCheck: Send + Sync {
    /// Name of the health check
    fn name(&self) -> &str;

    /// Perform the health check
    async fn check(&self) -> HealthCheckResult;

    /// Whether this check is critical for system operation
    fn is_critical(&self) -> bool;
}

/// Authentication service health check interface
#[async_trait]
pub trait AuthHealthCheck: Send + Sync {
    /// Get authentication service statistics
    async fn get_auth_stats(&self) -> Result<AuthStats, HealthCheckError>;
}

/// Job queue health check interface
#[async_trait]
pub trait JobQueueHealthCheck: Send + Sync {
    /// Get job queue statistics
    async fn get_queue_stats(&self) -> Result<QueueStats, HealthCheckError>;
}

/// Authentication statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthStats {
    pub token_validation_latency_ms: u64,
    pub active_sessions: u64,
    pub failed_auth_attempts_last_hour: u64,
    pub successful_auth_attempts_last_hour: u64,
}

/// Job queue statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStats {
    pub pending_jobs: usize,
    pub running_jobs: usize,
    pub failed_jobs: usize,
    pub completed_jobs_last_hour: usize,
    pub average_job_duration_seconds: f64,
    pub oldest_pending_job_age_seconds: u64,
}

/// Health check error
#[derive(Debug, thiserror::Error)]
pub enum HealthCheckError {
    #[error("Service unavailable: {0}")]
    ServiceUnavailable(String),

    #[error("Timeout after {0}ms")]
    Timeout(u64),

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

/// Null health check that always returns healthy (useful for testing)
pub struct NullHealthCheck {
    name: String,
}

impl NullHealthCheck {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait]
impl HealthCheck for NullHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self) -> HealthCheckResult {
        HealthCheckResult {
            name: self.name.clone(),
            status: HealthStatus::Healthy,
            message: "Null health check - always healthy".to_string(),
            duration_ms: 0,
            last_checked: Utc::now(),
            metadata: HashMap::new(),
        }
    }

    fn is_critical(&self) -> bool {
        false
    }
}

#[async_trait]
impl AuthHealthCheck for NullHealthCheck {
    async fn get_auth_stats(&self) -> Result<AuthStats, HealthCheckError> {
        Ok(AuthStats {
            token_validation_latency_ms: 0,
            active_sessions: 0,
            failed_auth_attempts_last_hour: 0,
            successful_auth_attempts_last_hour: 0,
        })
    }
}

#[async_trait]
impl JobQueueHealthCheck for NullHealthCheck {
    async fn get_queue_stats(&self) -> Result<QueueStats, HealthCheckError> {
        Ok(QueueStats {
            pending_jobs: 0,
            running_jobs: 0,
            failed_jobs: 0,
            completed_jobs_last_hour: 0,
            average_job_duration_seconds: 0.0,
            oldest_pending_job_age_seconds: 0,
        })
    }
}