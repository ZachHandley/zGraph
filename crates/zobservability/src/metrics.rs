//! Metrics collection and exposition for ZRUSTDB

use anyhow::Result;
use axum::{extract::State, http::StatusCode, response::Response, routing::get, Router};
use prometheus::{Encoder, Registry, TextEncoder};
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;

/// Core metrics for ZRUSTDB components
pub struct ZdbMetrics {
    // Vector operation metrics
    pub vector_operations_total: prometheus::CounterVec,
    pub vector_operation_duration: prometheus::HistogramVec,
    pub vector_index_size: prometheus::GaugeVec,
    pub vector_recall_accuracy: prometheus::HistogramVec,

    // SQL engine metrics
    pub sql_queries_total: prometheus::CounterVec,
    pub sql_query_duration: prometheus::HistogramVec,
    pub sql_rows_processed: prometheus::CounterVec,
    pub sql_execution_plan_type: prometheus::CounterVec,

    // Storage metrics
    pub storage_operations_total: prometheus::CounterVec,
    pub storage_operation_duration: prometheus::HistogramVec,
    pub storage_size_bytes: prometheus::GaugeVec,
    pub storage_cache_hit_ratio: prometheus::GaugeVec,

    // WebSocket/WebRTC metrics
    pub websocket_connections_active: prometheus::Gauge,
    pub websocket_messages_total: prometheus::CounterVec,
    pub webrtc_sessions_active: prometheus::Gauge,
    pub webrtc_bandwidth_bytes: prometheus::CounterVec,

    // Authentication metrics
    pub auth_requests_total: prometheus::CounterVec,
    pub auth_token_validations: prometheus::CounterVec,
    pub auth_session_duration: prometheus::Histogram,

    // Job orchestration metrics
    pub jobs_total: prometheus::CounterVec,
    pub job_duration: prometheus::HistogramVec,
    pub job_queue_size: prometheus::GaugeVec,
    pub worker_utilization: prometheus::GaugeVec,

    // System resource metrics
    pub cpu_usage_percent: prometheus::Gauge,
    pub memory_usage_bytes: prometheus::Gauge,
    pub disk_io_operations: prometheus::CounterVec,
    pub network_io_bytes: prometheus::CounterVec,

    // Error and latency metrics
    pub request_errors_total: prometheus::CounterVec,
    pub request_duration: prometheus::HistogramVec,
    pub circuit_breaker_state: prometheus::GaugeVec,
}

impl ZdbMetrics {
    pub fn new() -> Result<Self> {
        let metrics = Self {
            // Vector operations
            vector_operations_total: prometheus::CounterVec::new(
                prometheus::Opts::new(
                    "zdb_vector_operations_total",
                    "Total number of vector operations performed"
                ),
                &["operation", "metric_type", "status"]
            )?,

            vector_operation_duration: prometheus::HistogramVec::new(
                prometheus::HistogramOpts::new(
                    "zdb_vector_operation_duration_seconds",
                    "Duration of vector operations in seconds"
                ).buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
                &["operation", "metric_type", "simd_enabled"]
            )?,

            vector_index_size: prometheus::GaugeVec::new(
                prometheus::Opts::new(
                    "zdb_vector_index_size_entries",
                    "Number of entries in vector indexes"
                ),
                &["index_name", "index_type"]
            )?,

            vector_recall_accuracy: prometheus::HistogramVec::new(
                prometheus::HistogramOpts::new(
                    "zdb_vector_recall_accuracy_ratio",
                    "Recall accuracy of approximate nearest neighbor searches"
                ).buckets(vec![0.80, 0.85, 0.90, 0.95, 0.98, 0.99, 1.0]),
                &["index_name", "k"]
            )?,

            // SQL engine
            sql_queries_total: prometheus::CounterVec::new(
                prometheus::Opts::new(
                    "zdb_sql_queries_total",
                    "Total number of SQL queries executed"
                ),
                &["query_type", "status"]
            )?,

            sql_query_duration: prometheus::HistogramVec::new(
                prometheus::HistogramOpts::new(
                    "zdb_sql_query_duration_seconds",
                    "Duration of SQL query execution"
                ).buckets(vec![0.001, 0.01, 0.1, 1.0, 10.0, 60.0]),
                &["query_type", "has_vector_ops"]
            )?,

            sql_rows_processed: prometheus::CounterVec::new(
                prometheus::Opts::new(
                    "zdb_sql_rows_processed_total",
                    "Total number of rows processed by SQL queries"
                ),
                &["operation"]
            )?,

            sql_execution_plan_type: prometheus::CounterVec::new(
                prometheus::Opts::new(
                    "zdb_sql_execution_plan_type_total",
                    "Count of SQL execution plan types used"
                ),
                &["plan_type", "has_index_scan"]
            )?,

            // Storage
            storage_operations_total: prometheus::CounterVec::new(
                prometheus::Opts::new(
                    "zdb_storage_operations_total",
                    "Total storage operations"
                ),
                &["operation", "status"]
            )?,

            storage_operation_duration: prometheus::HistogramVec::new(
                prometheus::HistogramOpts::new(
                    "zdb_storage_operation_duration_seconds",
                    "Duration of storage operations"
                ).buckets(vec![0.0001, 0.001, 0.01, 0.1, 1.0]),
                &["operation"]
            )?,

            storage_size_bytes: prometheus::GaugeVec::new(
                prometheus::Opts::new(
                    "zdb_storage_size_bytes",
                    "Storage size in bytes"
                ),
                &["storage_type", "organization"]
            )?,

            storage_cache_hit_ratio: prometheus::GaugeVec::new(
                prometheus::Opts::new(
                    "zdb_storage_cache_hit_ratio",
                    "Cache hit ratio for storage operations"
                ),
                &["cache_type"]
            )?,

            // WebSocket/WebRTC
            websocket_connections_active: prometheus::Gauge::new(
                "zdb_websocket_connections_active",
                "Number of active WebSocket connections"
            )?,

            websocket_messages_total: prometheus::CounterVec::new(
                prometheus::Opts::new(
                    "zdb_websocket_messages_total",
                    "Total WebSocket messages"
                ),
                &["direction", "message_type"]
            )?,

            webrtc_sessions_active: prometheus::Gauge::new(
                "zdb_webrtc_sessions_active",
                "Number of active WebRTC sessions"
            )?,

            webrtc_bandwidth_bytes: prometheus::CounterVec::new(
                prometheus::Opts::new(
                    "zdb_webrtc_bandwidth_bytes_total",
                    "WebRTC bandwidth usage in bytes"
                ),
                &["direction", "stream_type"]
            )?,

            // Authentication
            auth_requests_total: prometheus::CounterVec::new(
                prometheus::Opts::new(
                    "zdb_auth_requests_total",
                    "Total authentication requests"
                ),
                &["method", "status"]
            )?,

            auth_token_validations: prometheus::CounterVec::new(
                prometheus::Opts::new(
                    "zdb_auth_token_validations_total",
                    "Total token validation attempts"
                ),
                &["token_type", "status"]
            )?,

            auth_session_duration: prometheus::Histogram::with_opts(
                prometheus::HistogramOpts::new(
                    "zdb_auth_session_duration_seconds",
                    "Duration of authentication sessions"
                ).buckets(vec![60.0, 300.0, 1800.0, 3600.0, 7200.0, 86400.0])
            )?,

            // Job orchestration
            jobs_total: prometheus::CounterVec::new(
                prometheus::Opts::new(
                    "zdb_jobs_total",
                    "Total jobs processed"
                ),
                &["job_type", "status"]
            )?,

            job_duration: prometheus::HistogramVec::new(
                prometheus::HistogramOpts::new(
                    "zdb_job_duration_seconds",
                    "Duration of job execution"
                ).buckets(vec![1.0, 10.0, 60.0, 300.0, 1800.0, 3600.0]),
                &["job_type", "worker_type"]
            )?,

            job_queue_size: prometheus::GaugeVec::new(
                prometheus::Opts::new(
                    "zdb_job_queue_size",
                    "Number of jobs in queue"
                ),
                &["queue_name", "priority"]
            )?,

            worker_utilization: prometheus::GaugeVec::new(
                prometheus::Opts::new(
                    "zdb_worker_utilization_ratio",
                    "Worker utilization ratio"
                ),
                &["worker_id", "worker_type"]
            )?,

            // System resources
            cpu_usage_percent: prometheus::Gauge::new(
                "zdb_cpu_usage_percent",
                "CPU usage percentage"
            )?,

            memory_usage_bytes: prometheus::Gauge::new(
                "zdb_memory_usage_bytes",
                "Memory usage in bytes"
            )?,

            disk_io_operations: prometheus::CounterVec::new(
                prometheus::Opts::new(
                    "zdb_disk_io_operations_total",
                    "Total disk I/O operations"
                ),
                &["operation", "device"]
            )?,

            network_io_bytes: prometheus::CounterVec::new(
                prometheus::Opts::new(
                    "zdb_network_io_bytes_total",
                    "Network I/O in bytes"
                ),
                &["direction", "interface"]
            )?,

            // Errors and latency
            request_errors_total: prometheus::CounterVec::new(
                prometheus::Opts::new(
                    "zdb_request_errors_total",
                    "Total request errors"
                ),
                &["endpoint", "error_type", "status_code"]
            )?,

            request_duration: prometheus::HistogramVec::new(
                prometheus::HistogramOpts::new(
                    "zdb_request_duration_seconds",
                    "Request duration in seconds"
                ).buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]),
                &["endpoint", "method", "status_code"]
            )?,

            circuit_breaker_state: prometheus::GaugeVec::new(
                prometheus::Opts::new(
                    "zdb_circuit_breaker_state",
                    "Circuit breaker state (0=closed, 1=open, 2=half-open)"
                ),
                &["service", "endpoint"]
            )?,
        };

        Ok(metrics)
    }

    /// Register all metrics with a registry
    pub fn register(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.vector_operations_total.clone()))?;
        registry.register(Box::new(self.vector_operation_duration.clone()))?;
        registry.register(Box::new(self.vector_index_size.clone()))?;
        registry.register(Box::new(self.vector_recall_accuracy.clone()))?;

        registry.register(Box::new(self.sql_queries_total.clone()))?;
        registry.register(Box::new(self.sql_query_duration.clone()))?;
        registry.register(Box::new(self.sql_rows_processed.clone()))?;
        registry.register(Box::new(self.sql_execution_plan_type.clone()))?;

        registry.register(Box::new(self.storage_operations_total.clone()))?;
        registry.register(Box::new(self.storage_operation_duration.clone()))?;
        registry.register(Box::new(self.storage_size_bytes.clone()))?;
        registry.register(Box::new(self.storage_cache_hit_ratio.clone()))?;

        registry.register(Box::new(self.websocket_connections_active.clone()))?;
        registry.register(Box::new(self.websocket_messages_total.clone()))?;
        registry.register(Box::new(self.webrtc_sessions_active.clone()))?;
        registry.register(Box::new(self.webrtc_bandwidth_bytes.clone()))?;

        registry.register(Box::new(self.auth_requests_total.clone()))?;
        registry.register(Box::new(self.auth_token_validations.clone()))?;
        registry.register(Box::new(self.auth_session_duration.clone()))?;

        registry.register(Box::new(self.jobs_total.clone()))?;
        registry.register(Box::new(self.job_duration.clone()))?;
        registry.register(Box::new(self.job_queue_size.clone()))?;
        registry.register(Box::new(self.worker_utilization.clone()))?;

        registry.register(Box::new(self.cpu_usage_percent.clone()))?;
        registry.register(Box::new(self.memory_usage_bytes.clone()))?;
        registry.register(Box::new(self.disk_io_operations.clone()))?;
        registry.register(Box::new(self.network_io_bytes.clone()))?;

        registry.register(Box::new(self.request_errors_total.clone()))?;
        registry.register(Box::new(self.request_duration.clone()))?;
        registry.register(Box::new(self.circuit_breaker_state.clone()))?;

        Ok(())
    }
}

/// Metrics collector that periodically updates system metrics
pub struct MetricsCollector {
    metrics: Arc<ZdbMetrics>,
    system: sysinfo::System,
    disks: sysinfo::Disks,
    networks: sysinfo::Networks,
}

impl MetricsCollector {
    pub fn new(metrics: Arc<ZdbMetrics>) -> Self {
        Self {
            metrics,
            system: sysinfo::System::new_all(),
            disks: sysinfo::Disks::new_with_refreshed_list(),
            networks: sysinfo::Networks::new_with_refreshed_list(),
        }
    }

    /// Start periodic metrics collection
    pub async fn start(&mut self) -> Result<()> {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));

        loop {
            interval.tick().await;

            if let Err(e) = self.collect_system_metrics().await {
                tracing::warn!("Failed to collect system metrics: {}", e);
            }
        }
    }

    /// Collect system-level metrics
    async fn collect_system_metrics(&mut self) -> Result<()> {
        self.system.refresh_all();
        self.disks.refresh();
        self.networks.refresh();

        // CPU usage
        let cpu_usage = self.system.global_cpu_info().cpu_usage() as f64;
        self.metrics.cpu_usage_percent.set(cpu_usage);

        // Memory usage
        let memory_used = self.system.used_memory();
        self.metrics.memory_usage_bytes.set(memory_used as f64);

        // Disk I/O (simplified - would need more detailed implementation for production)
        for disk in &self.disks {
            let device = disk.name().to_string_lossy();
            // Note: sysinfo doesn't provide I/O counters directly
            // In production, you'd use procfs or similar for detailed disk stats
        }

        // Network I/O (simplified)
        for (interface, data) in &self.networks {
            self.metrics.network_io_bytes
                .with_label_values(&["rx", interface])
                .inc_by(data.total_received() as f64);

            self.metrics.network_io_bytes
                .with_label_values(&["tx", interface])
                .inc_by(data.total_transmitted() as f64);
        }

        Ok(())
    }
}

/// Start metrics HTTP server
pub async fn start_metrics_server(
    registry: Arc<Registry>,
    addr: SocketAddr,
) -> Result<()> {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(registry);

    let listener = TcpListener::bind(addr).await?;
    tracing::info!("Metrics server listening on {}", addr);

    axum::serve(listener, app).await?;
    Ok(())
}

/// Prometheus metrics endpoint handler
async fn metrics_handler(
    State(registry): State<Arc<Registry>>,
) -> Result<Response<String>, StatusCode> {
    let encoder = TextEncoder::new();
    let metric_families = registry.gather();

    match encoder.encode_to_string(&metric_families) {
        Ok(output) => Ok(Response::builder()
            .header("content-type", encoder.format_type())
            .body(output)
            .unwrap()),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}