//! Observability implementations for zauth

use crate::AuthRepository;
use anyhow::Result;
use zcommon::{
    health::{AuthHealthCheck, HealthCheckError, AuthStats},
    metrics::{AuthMetrics, MetricsCollector},
};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

/// Auth-specific health check implementation
pub struct AuthHealthCheckImpl {
    repo: AuthRepository<'static>,
    auth_stats: Arc<AuthStatsTracker>,
}

impl AuthHealthCheckImpl {
    pub fn new(repo: AuthRepository<'static>) -> Self {
        Self {
            repo,
            auth_stats: Arc::new(AuthStatsTracker::new()),
        }
    }
}

#[async_trait::async_trait]
impl AuthHealthCheck for AuthHealthCheckImpl {
    async fn get_auth_stats(&self) -> Result<AuthStats, HealthCheckError> {
        let _start = Instant::now();

        // Test basic repository operations
        let _result = self.repo.get_user_by_email(1, "test@example.com");

        // Test token validation performance
        let validation_start = Instant::now();
        tokio::time::sleep(Duration::from_millis(1)).await; // Simulate validation work
        let validation_latency = validation_start.elapsed().as_millis() as u64;

        // Get current stats and merge with our validation latency
        let mut stats = self.auth_stats.get_stats();
        stats.token_validation_latency_ms = validation_latency;

        Ok(stats)
    }
}

/// Auth-specific metrics implementation
#[derive(Clone)]
pub struct AuthMetricsImpl {
    stats: Arc<AuthStatsTracker>,
}

impl AuthMetricsImpl {
    pub fn new() -> Self {
        Self {
            stats: Arc::new(AuthStatsTracker::new()),
        }
    }

    pub fn stats(&self) -> Arc<AuthStatsTracker> {
        self.stats.clone()
    }
}

#[async_trait::async_trait]
impl AuthMetrics for AuthMetricsImpl {
    fn record_auth_attempt(&self, method: &str, success: bool) {
        self.stats.record_auth_attempt(method, success);
    }

    fn record_session_duration(&self, duration_seconds: f64) {
        self.stats.record_session_duration(duration_seconds);
    }

    fn record_token_validation(&self, latency_ms: u64) {
        self.stats.record_token_validation(latency_ms);
    }

    fn get_active_sessions(&self) -> u64 {
        self.stats.get_stats().active_sessions
    }
}

#[async_trait::async_trait]
impl MetricsCollector for AuthMetricsImpl {
    fn increment_counter(&self, _name: &str, _labels: &[(&str, &str)]) {
        // Simplified implementation - no actual tracing
    }

    fn set_gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {
        // Simplified implementation - no actual tracing
    }

    fn observe_histogram(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {
        // Simplified implementation - no actual tracing
    }

    fn start_timer(&self, _name: &str, _labels: &[(&str, &str)]) -> zcommon::metrics::MetricsTimer {
        zcommon::metrics::MetricsTimer::new(
            _name.to_string(),
            _labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            Arc::new((*self).clone()),
        )
    }

    fn name(&self) -> &str {
        "auth_metrics"
    }
}

/// Thread-safe statistics tracker
#[derive(Clone)]
pub struct AuthStatsTracker {
    inner: Arc<AuthStatsInner>,
}

struct AuthStatsInner {
    active_sessions: AtomicU64,
    failed_attempts_last_hour: AtomicU64,
    successful_attempts_last_hour: AtomicU64,
    total_token_validations: AtomicU64,
    total_token_validation_time_ms: AtomicU64,
}

impl AuthStatsTracker {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(AuthStatsInner {
                active_sessions: AtomicU64::new(0),
                failed_attempts_last_hour: AtomicU64::new(0),
                successful_attempts_last_hour: AtomicU64::new(0),
                total_token_validations: AtomicU64::new(0),
                total_token_validation_time_ms: AtomicU64::new(0),
            }),
        }
    }

    pub fn record_auth_attempt(&self, _method: &str, success: bool) {
        if success {
            self.inner.successful_attempts_last_hour.fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner.failed_attempts_last_hour.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_session_start(&self) {
        self.inner.active_sessions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_session_end(&self) {
        self.inner.active_sessions.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn record_session_duration(&self, _duration_seconds: f64) {
        // Could track average session duration here
    }

    pub fn record_token_validation(&self, latency_ms: u64) {
        self.inner.total_token_validations.fetch_add(1, Ordering::Relaxed);
        self.inner.total_token_validation_time_ms.fetch_add(latency_ms, Ordering::Relaxed);
    }

    pub fn get_stats(&self) -> AuthStats {
        let total_validations = self.inner.total_token_validations.load(Ordering::Relaxed);
        let avg_latency = if total_validations > 0 {
            self.inner.total_token_validation_time_ms.load(Ordering::Relaxed) / total_validations
        } else {
            0
        };

        AuthStats {
            token_validation_latency_ms: avg_latency,
            active_sessions: self.inner.active_sessions.load(Ordering::Relaxed),
            failed_auth_attempts_last_hour: self.inner.failed_attempts_last_hour.load(Ordering::Relaxed),
            successful_auth_attempts_last_hour: self.inner.successful_attempts_last_hour.load(Ordering::Relaxed),
        }
    }
}