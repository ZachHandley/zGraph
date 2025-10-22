//! Specialized metrics collectors for ZRUSTDB components

use anyhow::Result;
use std::{
    sync::Arc,
    time::Duration,
    collections::HashMap,
};
use tokio::time::interval;

use crate::metrics::ZdbMetrics;

/// Vector operations metrics collector
pub struct VectorMetricsCollector {
    metrics: Arc<ZdbMetrics>,
}

impl VectorMetricsCollector {
    pub fn new(metrics: Arc<ZdbMetrics>) -> Self {
        Self { metrics }
    }

    /// Record vector operation timing and metadata
    pub fn record_vector_operation(
        &self,
        operation: &str,
        metric_type: &str,
        duration: Duration,
        simd_enabled: bool,
        status: &str,
        dimensions: usize,
        batch_size: Option<usize>,
    ) {
        // Record operation count
        self.metrics
            .vector_operations_total
            .with_label_values(&[operation, metric_type, status])
            .inc();

        // Record operation duration
        let simd_label = if simd_enabled { "true" } else { "false" };
        self.metrics
            .vector_operation_duration
            .with_label_values(&[operation, metric_type, simd_label])
            .observe(duration.as_secs_f64());

        // Add span information if tracing is available
        tracing::info!(
            operation = operation,
            metric_type = metric_type,
            duration_ms = duration.as_millis(),
            simd_enabled = simd_enabled,
            dimensions = dimensions,
            batch_size = batch_size,
            "Vector operation completed"
        );
    }

    /// Record vector index statistics
    pub fn record_index_stats(
        &self,
        index_name: &str,
        index_type: &str,
        entry_count: usize,
        memory_usage_mb: f64,
    ) {
        self.metrics
            .vector_index_size
            .with_label_values(&[index_name, index_type])
            .set(entry_count as f64);

        tracing::debug!(
            index_name = index_name,
            index_type = index_type,
            entry_count = entry_count,
            memory_usage_mb = memory_usage_mb,
            "Vector index statistics updated"
        );
    }

    /// Record vector search recall accuracy
    pub fn record_recall_accuracy(
        &self,
        index_name: &str,
        k: usize,
        recall: f64,
    ) {
        self.metrics
            .vector_recall_accuracy
            .with_label_values(&[index_name, &k.to_string()])
            .observe(recall);
    }
}

/// SQL engine metrics collector
pub struct SqlMetricsCollector {
    metrics: Arc<ZdbMetrics>,
}

impl SqlMetricsCollector {
    pub fn new(metrics: Arc<ZdbMetrics>) -> Self {
        Self { metrics }
    }

    /// Record SQL query execution
    pub fn record_sql_query(
        &self,
        query_type: &str,
        duration: Duration,
        status: &str,
        has_vector_ops: bool,
        rows_processed: Option<usize>,
        plan_type: Option<&str>,
        has_index_scan: bool,
    ) {
        // Record query count and duration
        self.metrics
            .sql_queries_total
            .with_label_values(&[query_type, status])
            .inc();

        let vector_ops_label = if has_vector_ops { "true" } else { "false" };
        self.metrics
            .sql_query_duration
            .with_label_values(&[query_type, vector_ops_label])
            .observe(duration.as_secs_f64());

        // Record rows processed
        if let Some(rows) = rows_processed {
            self.metrics
                .sql_rows_processed
                .with_label_values(&[query_type])
                .inc_by(rows as f64);
        }

        // Record execution plan type
        if let Some(plan) = plan_type {
            let index_scan_label = if has_index_scan { "true" } else { "false" };
            self.metrics
                .sql_execution_plan_type
                .with_label_values(&[plan, index_scan_label])
                .inc();
        }

        tracing::info!(
            query_type = query_type,
            duration_ms = duration.as_millis(),
            status = status,
            has_vector_ops = has_vector_ops,
            rows_processed = rows_processed,
            plan_type = plan_type,
            "SQL query executed"
        );
    }
}

/// Storage metrics collector
pub struct StorageMetricsCollector {
    metrics: Arc<ZdbMetrics>,
}

impl StorageMetricsCollector {
    pub fn new(metrics: Arc<ZdbMetrics>) -> Self {
        Self { metrics }
    }

    /// Record storage operation
    pub fn record_storage_operation(
        &self,
        operation: &str,
        duration: Duration,
        status: &str,
        key_count: Option<usize>,
        data_size_bytes: Option<usize>,
    ) {
        self.metrics
            .storage_operations_total
            .with_label_values(&[operation, status])
            .inc();

        self.metrics
            .storage_operation_duration
            .with_label_values(&[operation])
            .observe(duration.as_secs_f64());

        tracing::debug!(
            operation = operation,
            duration_ms = duration.as_millis(),
            status = status,
            key_count = key_count,
            data_size_bytes = data_size_bytes,
            "Storage operation completed"
        );
    }

    /// Update storage size metrics
    pub fn update_storage_size(
        &self,
        storage_type: &str,
        organization: &str,
        size_bytes: usize,
    ) {
        self.metrics
            .storage_size_bytes
            .with_label_values(&[storage_type, organization])
            .set(size_bytes as f64);
    }

    /// Update cache hit ratio
    pub fn update_cache_hit_ratio(&self, cache_type: &str, hit_ratio: f64) {
        self.metrics
            .storage_cache_hit_ratio
            .with_label_values(&[cache_type])
            .set(hit_ratio);
    }
}

/// WebSocket/WebRTC metrics collector
pub struct RealtimeMetricsCollector {
    metrics: Arc<ZdbMetrics>,
}

impl RealtimeMetricsCollector {
    pub fn new(metrics: Arc<ZdbMetrics>) -> Self {
        Self { metrics }
    }

    /// Update WebSocket connection count
    pub fn update_websocket_connections(&self, count: i64) {
        self.metrics.websocket_connections_active.set(count as f64);
    }

    /// Record WebSocket message
    pub fn record_websocket_message(
        &self,
        direction: &str, // "inbound" or "outbound"
        message_type: &str,
    ) {
        self.metrics
            .websocket_messages_total
            .with_label_values(&[direction, message_type])
            .inc();
    }

    /// Update WebRTC session count
    pub fn update_webrtc_sessions(&self, count: i64) {
        self.metrics.webrtc_sessions_active.set(count as f64);
    }

    /// Record WebRTC bandwidth usage
    pub fn record_webrtc_bandwidth(
        &self,
        direction: &str, // "inbound" or "outbound"
        stream_type: &str, // "audio", "video", "data"
        bytes: usize,
    ) {
        self.metrics
            .webrtc_bandwidth_bytes
            .with_label_values(&[direction, stream_type])
            .inc_by(bytes as f64);
    }
}

/// Authentication metrics collector
pub struct AuthMetricsCollector {
    metrics: Arc<ZdbMetrics>,
}

impl AuthMetricsCollector {
    pub fn new(metrics: Arc<ZdbMetrics>) -> Self {
        Self { metrics }
    }

    /// Record authentication request
    pub fn record_auth_request(
        &self,
        method: &str, // "login", "refresh", "logout"
        status: &str, // "success", "failed"
        duration: Duration,
    ) {
        self.metrics
            .auth_requests_total
            .with_label_values(&[method, status])
            .inc();

        if method == "login" && status == "success" {
            self.metrics
                .auth_session_duration
                .observe(duration.as_secs_f64());
        }
    }

    /// Record token validation
    pub fn record_token_validation(
        &self,
        token_type: &str, // "access", "refresh"
        status: &str,    // "valid", "expired", "invalid"
    ) {
        self.metrics
            .auth_token_validations
            .with_label_values(&[token_type, status])
            .inc();
    }
}

/// Job orchestration metrics collector
pub struct JobMetricsCollector {
    metrics: Arc<ZdbMetrics>,
}

impl JobMetricsCollector {
    pub fn new(metrics: Arc<ZdbMetrics>) -> Self {
        Self { metrics }
    }

    /// Record job execution
    pub fn record_job_execution(
        &self,
        job_type: &str,
        worker_type: &str,
        duration: Duration,
        status: &str,
    ) {
        self.metrics
            .jobs_total
            .with_label_values(&[job_type, status])
            .inc();

        self.metrics
            .job_duration
            .with_label_values(&[job_type, worker_type])
            .observe(duration.as_secs_f64());
    }

    /// Update job queue size
    pub fn update_queue_size(
        &self,
        queue_name: &str,
        priority: &str,
        size: usize,
    ) {
        self.metrics
            .job_queue_size
            .with_label_values(&[queue_name, priority])
            .set(size as f64);
    }

    /// Update worker utilization
    pub fn update_worker_utilization(
        &self,
        worker_id: &str,
        worker_type: &str,
        utilization: f64,
    ) {
        self.metrics
            .worker_utilization
            .with_label_values(&[worker_id, worker_type])
            .set(utilization);
    }
}

/// HTTP request metrics collector
pub struct RequestMetricsCollector {
    metrics: Arc<ZdbMetrics>,
}

impl RequestMetricsCollector {
    pub fn new(metrics: Arc<ZdbMetrics>) -> Self {
        Self { metrics }
    }

    /// Record HTTP request
    pub fn record_request(
        &self,
        endpoint: &str,
        method: &str,
        status_code: u16,
        duration: Duration,
        is_error: bool,
        error_type: Option<&str>,
    ) {
        let status_code_str = status_code.to_string();

        // Record request duration
        self.metrics
            .request_duration
            .with_label_values(&[endpoint, method, &status_code_str])
            .observe(duration.as_secs_f64());

        // Record errors if applicable
        if is_error {
            let error_type = error_type.unwrap_or("unknown");
            self.metrics
                .request_errors_total
                .with_label_values(&[endpoint, error_type, &status_code_str])
                .inc();
        }
    }

    /// Update circuit breaker state
    pub fn update_circuit_breaker_state(
        &self,
        service: &str,
        endpoint: &str,
        state: u8, // 0=closed, 1=open, 2=half-open
    ) {
        self.metrics
            .circuit_breaker_state
            .with_label_values(&[service, endpoint])
            .set(state as f64);
    }
}

/// Comprehensive metrics collector that aggregates all component collectors
pub struct MetricsAggregator {
    pub vector: VectorMetricsCollector,
    pub sql: SqlMetricsCollector,
    pub storage: StorageMetricsCollector,
    pub realtime: RealtimeMetricsCollector,
    pub auth: AuthMetricsCollector,
    pub jobs: JobMetricsCollector,
    pub requests: RequestMetricsCollector,
    metrics: Arc<ZdbMetrics>,
}

impl MetricsAggregator {
    pub fn new(metrics: Arc<ZdbMetrics>) -> Self {
        Self {
            vector: VectorMetricsCollector::new(metrics.clone()),
            sql: SqlMetricsCollector::new(metrics.clone()),
            storage: StorageMetricsCollector::new(metrics.clone()),
            realtime: RealtimeMetricsCollector::new(metrics.clone()),
            auth: AuthMetricsCollector::new(metrics.clone()),
            jobs: JobMetricsCollector::new(metrics.clone()),
            requests: RequestMetricsCollector::new(metrics.clone()),
            metrics,
        }
    }

    /// Start periodic collection of system metrics
    pub async fn start_system_collection(&self) -> Result<()> {
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let mut system = sysinfo::System::new_all();
            let mut networks = sysinfo::Networks::new_with_refreshed_list();
            let mut interval = interval(Duration::from_secs(15));

            loop {
                interval.tick().await;

                system.refresh_all();
                networks.refresh();

                // Update CPU usage
                let cpu_usage = system.global_cpu_info().cpu_usage() as f64;
                metrics.cpu_usage_percent.set(cpu_usage);

                // Update memory usage
                let memory_used = system.used_memory() as f64;
                metrics.memory_usage_bytes.set(memory_used);

                // Update network I/O
                for (interface, data) in &networks {
                    metrics
                        .network_io_bytes
                        .with_label_values(&["rx", interface])
                        .inc_by(data.total_received() as f64);

                    metrics
                        .network_io_bytes
                        .with_label_values(&["tx", interface])
                        .inc_by(data.total_transmitted() as f64);
                }

                tracing::trace!(
                    cpu_usage = cpu_usage,
                    memory_used = memory_used,
                    "System metrics updated"
                );
            }
        });

        Ok(())
    }

    /// Export all current metrics for debugging
    pub fn export_current_metrics(&self) -> HashMap<String, f64> {
        let mut metrics = HashMap::new();

        // This would be expanded to collect current values from all metrics
        // For now, just return a placeholder
        metrics.insert("cpu_usage_percent".to_string(), 0.0);
        metrics.insert("memory_usage_bytes".to_string(), 0.0);

        metrics
    }
}