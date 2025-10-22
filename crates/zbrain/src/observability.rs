//! Observability implementations for zbrain

use crate::{JobQueue, JobState};
use anyhow::Result;
use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};
use zcommon::{
    health::{JobQueueHealthCheck, HealthCheckError, QueueStats},
    metrics::{JobMetrics, MetricsCollector},
};

/// Job queue health check implementation
pub struct JobQueueHealthCheckImpl {
    queue: Arc<JobQueue>,
}

impl JobQueueHealthCheckImpl {
    pub fn new(queue: Arc<JobQueue>) -> Self {
        Self { queue }
    }
}

#[async_trait::async_trait]
impl JobQueueHealthCheck for JobQueueHealthCheckImpl {
    async fn get_queue_stats(&self) -> Result<QueueStats, HealthCheckError> {
        let start = Instant::now();

        // Get actual job statistics from the queue
        let pending_jobs = self.queue.list(None, Some(&JobState::Pending)).await
            .map_err(|e| HealthCheckError::InternalError(e.to_string()))?
            .len();

        let running_jobs = self.queue.list(None, Some(&JobState::Running)).await
            .map_err(|e| HealthCheckError::InternalError(e.to_string()))?
            .len();

        let failed_jobs = self.queue.list(None, Some(&JobState::Failed)).await
            .map_err(|e| HealthCheckError::InternalError(e.to_string()))?
            .len();

        let completed_jobs = self.queue.list(None, Some(&JobState::Succeeded)).await
            .map_err(|e| HealthCheckError::InternalError(e.to_string()))?
            .len();

        // Calculate statistics
        let completed_last_hour = completed_jobs; // Simplified - in production would filter by time
        let avg_duration = if completed_jobs > 0 {
            30.0 // Simplified - would calculate from actual job durations
        } else {
            0.0
        };

        let oldest_pending_age = if pending_jobs > 0 {
            // Get oldest pending job age (simplified)
            300 // 5 minutes in seconds
        } else {
            0
        };

        Ok(QueueStats {
            pending_jobs,
            running_jobs,
            failed_jobs,
            completed_jobs_last_hour: completed_last_hour,
            average_job_duration_seconds: avg_duration,
            oldest_pending_job_age_seconds: oldest_pending_age,
        })
    }
}

/// Job metrics implementation
#[derive(Clone)]
pub struct JobMetricsImpl {
    stats: Arc<JobStatsTracker>,
}

impl JobMetricsImpl {
    pub fn new() -> Self {
        Self {
            stats: Arc::new(JobStatsTracker::new()),
        }
    }

    pub fn stats(&self) -> Arc<JobStatsTracker> {
        self.stats.clone()
    }
}

#[async_trait::async_trait]
impl JobMetrics for JobMetricsImpl {
    fn record_job_submission(&self, job_type: &str, queue: &str) {
        self.stats.record_job_submission(job_type, queue);
    }

    fn record_job_completion(&self, job_type: &str, status: &str, duration_seconds: f64) {
        self.stats.record_job_completion(job_type, status, duration_seconds);
    }

    fn record_queue_size(&self, queue: &str, priority: &str, size: u64) {
        self.stats.record_queue_size(queue, priority, size);
    }

    fn record_worker_activity(&self, _worker_id: &str, _action: &str) {
        // Simplified implementation - no actual tracing
    }
}

#[async_trait::async_trait]
impl MetricsCollector for JobMetricsImpl {
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
        "job_metrics"
    }
}

/// Thread-safe job statistics tracker
#[derive(Clone)]
pub struct JobStatsTracker {
    inner: Arc<JobStatsInner>,
}

struct JobStatsInner {
    total_submitted: AtomicU64,
    total_completed: AtomicU64,
    total_failed: AtomicU64,
    total_duration_ms: AtomicU64,
    queue_sizes: std::collections::HashMap<String, AtomicUsize>,
}

impl JobStatsTracker {
    pub fn new() -> Self {
        let mut queue_sizes = std::collections::HashMap::new();
        queue_sizes.insert("default".to_string(), AtomicUsize::new(0));
        queue_sizes.insert("high".to_string(), AtomicUsize::new(0));
        queue_sizes.insert("low".to_string(), AtomicUsize::new(0));

        Self {
            inner: Arc::new(JobStatsInner {
                total_submitted: AtomicU64::new(0),
                total_completed: AtomicU64::new(0),
                total_failed: AtomicU64::new(0),
                total_duration_ms: AtomicU64::new(0),
                queue_sizes,
            }),
        }
    }

    pub fn record_job_submission(&self, job_type: &str, queue: &str) {
        self.inner.total_submitted.fetch_add(1, Ordering::Relaxed);

        if let Some(size) = self.inner.queue_sizes.get(queue) {
            size.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_job_completion(&self, job_type: &str, status: &str, duration_seconds: f64) {
        self.inner.total_completed.fetch_add(1, Ordering::Relaxed);
        self.inner.total_duration_ms.fetch_add((duration_seconds * 1000.0) as u64, Ordering::Relaxed);

        if status == "failed" {
            self.inner.total_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_queue_size(&self, queue: &str, priority: &str, size: u64) {
        if let Some(queue_size) = self.inner.queue_sizes.get(queue) {
            queue_size.store(size as usize, Ordering::Relaxed);
        }
    }

    pub fn get_stats(&self) -> JobStats {
        let total_completed = self.inner.total_completed.load(Ordering::Relaxed);
        let avg_duration_ms = if total_completed > 0 {
            self.inner.total_duration_ms.load(Ordering::Relaxed) / total_completed
        } else {
            0
        };

        JobStats {
            total_submitted: self.inner.total_submitted.load(Ordering::Relaxed),
            total_completed,
            total_failed: self.inner.total_failed.load(Ordering::Relaxed),
            average_duration_ms: avg_duration_ms,
            queue_sizes: self.inner.queue_sizes.iter()
                .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed) as u64))
                .collect(),
        }
    }
}

/// Job statistics summary
#[derive(Debug, Clone)]
pub struct JobStats {
    pub total_submitted: u64,
    pub total_completed: u64,
    pub total_failed: u64,
    pub average_duration_ms: u64,
    pub queue_sizes: std::collections::HashMap<String, u64>,
}