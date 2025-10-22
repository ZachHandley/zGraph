//! Abstract metrics interfaces to break circular dependencies

use async_trait::async_trait;
use std::sync::Arc;
use tokio::time::Instant;

/// Abstract metrics collector interface
#[async_trait]
pub trait MetricsCollector: Send + Sync {
    /// Record a counter metric
    fn increment_counter(&self, name: &str, labels: &[(&str, &str)]);

    /// Record a gauge metric
    fn set_gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]);

    /// Record a histogram/timing metric
    fn observe_histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]);

    /// Start a timer for histogram observation
    fn start_timer(&self, name: &str, labels: &[(&str, &str)]) -> MetricsTimer;

    /// Get metrics collector name
    fn name(&self) -> &str;
}

/// Timer for histogram measurements
pub struct MetricsTimer {
    name: String,
    labels: Vec<(String, String)>,
    start: Instant,
    collector: Arc<dyn MetricsCollector>,
}

impl MetricsTimer {
    pub fn new(name: String, labels: Vec<(String, String)>, collector: Arc<dyn MetricsCollector>) -> Self {
        Self {
            name,
            labels,
            start: Instant::now(),
            collector,
        }
    }

    /// Stop the timer and record the observation
    pub fn stop(self) {
        let duration = self.start.elapsed().as_secs_f64();
        let label_refs: Vec<_> = self.labels.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        self.collector.observe_histogram(&self.name, duration, &label_refs);
    }
}

impl Drop for MetricsTimer {
    fn drop(&mut self) {
        if self.start.elapsed().as_secs_f64() > 0.0 {
            let duration = self.start.elapsed().as_secs_f64();
            let label_refs: Vec<_> = self.labels.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            self.collector.observe_histogram(&self.name, duration, &label_refs);
        }
    }
}

/// Authentication-specific metrics
#[async_trait]
pub trait AuthMetrics: Send + Sync {
    /// Record authentication attempt
    fn record_auth_attempt(&self, method: &str, success: bool);

    /// Record session duration
    fn record_session_duration(&self, duration_seconds: f64);

    /// Record token validation latency
    fn record_token_validation(&self, latency_ms: u64);

    /// Get active session count
    fn get_active_sessions(&self) -> u64;
}

/// Job queue-specific metrics
#[async_trait]
pub trait JobMetrics: Send + Sync {
    /// Record job submission
    fn record_job_submission(&self, job_type: &str, queue: &str);

    /// Record job completion
    fn record_job_completion(&self, job_type: &str, status: &str, duration_seconds: f64);

    /// Record job queue size
    fn record_queue_size(&self, queue: &str, priority: &str, size: u64);

    /// Record worker activity
    fn record_worker_activity(&self, worker_id: &str, action: &str);
}

/// Null metrics collector that does nothing (useful for testing)
pub struct NullMetricsCollector;

impl NullMetricsCollector {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

#[async_trait]
impl MetricsCollector for NullMetricsCollector {
    fn increment_counter(&self, _name: &str, _labels: &[(&str, &str)]) {}
    fn set_gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
    fn observe_histogram(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
    fn start_timer(&self, _name: &str, _labels: &[(&str, &str)]) -> MetricsTimer {
        MetricsTimer::new(
            _name.to_string(),
            _labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            Arc::new(Self),
        )
    }
    fn name(&self) -> &str {
        "null"
    }
}

#[async_trait]
impl AuthMetrics for NullMetricsCollector {
    fn record_auth_attempt(&self, _method: &str, _success: bool) {}
    fn record_session_duration(&self, _duration_seconds: f64) {}
    fn record_token_validation(&self, _latency_ms: u64) {}
    fn get_active_sessions(&self) -> u64 {
        0
    }
}

#[async_trait]
impl JobMetrics for NullMetricsCollector {
    fn record_job_submission(&self, _job_type: &str, _queue: &str) {}
    fn record_job_completion(&self, _job_type: &str, _status: &str, _duration_seconds: f64) {}
    fn record_queue_size(&self, _queue: &str, _priority: &str, _size: u64) {}
    fn record_worker_activity(&self, _worker_id: &str, _action: &str) {}
}