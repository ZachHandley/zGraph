//! Performance monitoring and observability for the storage layer
//!
//! This module provides comprehensive monitoring, metrics collection, and observability
//! features for the storage layer to track performance, health, and usage patterns.

use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

use crate::{Store, ReadTransaction, WriteTransaction};

/// Performance metrics configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    /// Enable detailed performance metrics
    pub enable_metrics: bool,
    /// Metrics collection interval in seconds
    pub metrics_interval_secs: u64,
    /// Maximum number of metrics samples to keep
    pub max_samples: usize,
    /// Enable query performance tracking
    pub enable_query_tracking: bool,
    /// Enable storage health monitoring
    pub enable_health_monitoring: bool,
    /// Alert thresholds
    pub alert_thresholds: AlertThresholds,
}

/// Alert thresholds for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertThresholds {
    /// Read latency threshold in milliseconds
    pub read_latency_ms: u64,
    /// Write latency threshold in milliseconds
    pub write_latency_ms: u64,
    /// Error rate threshold (0.0 to 1.0)
    pub error_rate: f64,
    /// Memory usage threshold (0.0 to 1.0)
    pub memory_usage_ratio: f64,
    /// Disk usage threshold (0.0 to 1.0)
    pub disk_usage_ratio: f64,
    /// CPU usage threshold (0.0 to 1.0)
    pub cpu_usage_ratio: f64,
}

impl Default for AlertThresholds {
    fn default() -> Self {
        Self {
            read_latency_ms: 100,
            write_latency_ms: 200,
            error_rate: 0.05, // 5%
            memory_usage_ratio: 0.80, // 80%
            disk_usage_ratio: 0.85, // 85%
            cpu_usage_ratio: 0.75, // 75%
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            enable_metrics: true,
            metrics_interval_secs: 30,
            max_samples: 1000,
            enable_query_tracking: true,
            enable_health_monitoring: true,
            alert_thresholds: AlertThresholds::default(),
        }
    }
}

/// Operation type for metrics
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OperationType {
    Read,
    Write,
    Delete,
    Scan,
    Batch,
    Compact,
    Backup,
    Restore,
}

/// Query language type for tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QueryLanguage {
    Sql,
    Cypher,
    Sparql,
    GraphQL,
}

/// Performance metrics for a single operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationMetrics {
    pub operation_type: OperationType,
    pub query_language: Option<QueryLanguage>,
    pub start_time: Instant,
    pub end_time: Instant,
    pub duration: Duration,
    pub success: bool,
    pub error_message: Option<String>,
    pub data_size_bytes: u64,
    pub rows_affected: u64,
}

impl OperationMetrics {
    pub fn new(operation_type: OperationType) -> Self {
        let now = Instant::now();
        Self {
            operation_type,
            query_language: None,
            start_time: now,
            end_time: now,
            duration: Duration::ZERO,
            success: false,
            error_message: None,
            data_size_bytes: 0,
            rows_affected: 0,
        }
    }

    pub fn with_query_language(mut self, language: QueryLanguage) -> Self {
        self.query_language = Some(language);
        self
    }

    pub fn finish(mut self, success: bool, error_message: Option<String>) -> Self {
        self.end_time = Instant::now();
        self.duration = self.end_time.duration_since(self.start_time);
        self.success = success;
        self.error_message = error_message;
        self
    }

    pub fn with_data_size(mut self, size_bytes: u64) -> Self {
        self.data_size_bytes = size_bytes;
        self
    }

    pub fn with_rows_affected(mut self, rows: u64) -> Self {
        self.rows_affected = rows;
        self
    }
}

/// Aggregated performance statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceStats {
    pub operation_type: OperationType,
    pub total_operations: u64,
    pub successful_operations: u64,
    pub failed_operations: u64,
    pub avg_duration_ms: f64,
    pub min_duration_ms: f64,
    pub max_duration_ms: f64,
    pub p95_duration_ms: f64,
    pub p99_duration_ms: f64,
    pub total_data_bytes: u64,
    pub total_rows_affected: u64,
    pub error_rate: f64,
}

impl Default for PerformanceStats {
    fn default() -> Self {
        Self {
            operation_type: OperationType::Read,
            total_operations: 0,
            successful_operations: 0,
            failed_operations: 0,
            avg_duration_ms: 0.0,
            min_duration_ms: f64::MAX,
            max_duration_ms: 0.0,
            p95_duration_ms: 0.0,
            p99_duration_ms: 0.0,
            total_data_bytes: 0,
            total_rows_affected: 0,
            error_rate: 0.0,
        }
    }
}

/// System health indicators
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemHealth {
    pub status: HealthStatus,
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub disk_usage: f64,
    pub active_connections: u64,
    pub queue_depth: u64,
    pub storage_health: StorageHealth,
    pub last_check: Instant,
}

/// Health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Warning,
    Critical,
    Unknown,
}

/// Storage-specific health metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageHealth {
    pub total_collections: u64,
    pub total_keys: u64,
    pub storage_size_bytes: u64,
    pub compaction_status: CompactionStatus,
    pub backup_status: BackupStatus,
    pub index_health: IndexHealth,
}

/// Compaction status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionStatus {
    Idle,
    Running,
    Scheduled,
    Failed,
}

/// Backup status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupStatus {
    UpToDate,
    InProgress,
    Overdue,
    Failed,
}

/// Index health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexHealth {
    pub total_indexes: u64,
    pub healthy_indexes: u64,
    pub rebuilding_indexes: u64,
    pub corrupted_indexes: u64,
}

/// Alert information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub alert_id: String,
    pub severity: AlertSeverity,
    pub alert_type: AlertType,
    pub message: String,
    pub details: HashMap<String, String>,
    pub created_at: Instant,
    pub resolved_at: Option<Instant>,
}

/// Alert severity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

/// Alert type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertType {
    HighLatency,
    HighErrorRate,
    ResourceExhaustion,
    StorageIssue,
    IndexCorruption,
    BackupFailure,
    SecurityIssue,
}

/// Observability manager for storage layer
pub struct ObservabilityManager {
    config: ObservabilityConfig,
    /// Performance metrics history
    metrics_history: RwLock<HashMap<OperationType, Vec<OperationMetrics>>>,
    /// Aggregated statistics
    aggregated_stats: RwLock<HashMap<OperationType, PerformanceStats>>,
    /// Active alerts
    active_alerts: RwLock<Vec<Alert>>,
    /// System health
    system_health: RwLock<SystemHealth>,
    /// Query performance tracking
    query_performance: RwLock<HashMap<String, Vec<OperationMetrics>>>,
    /// Rate limiting for metrics collection
    metrics_semaphore: Arc<Semaphore>,
}

impl ObservabilityManager {
    pub fn new(config: ObservabilityConfig) -> Self {
        Self {
            config,
            metrics_history: RwLock::new(HashMap::new()),
            aggregated_stats: RwLock::new(HashMap::new()),
            active_alerts: RwLock::new(Vec::new()),
            system_health: RwLock::new(SystemHealth {
                status: HealthStatus::Unknown,
                cpu_usage: 0.0,
                memory_usage: 0.0,
                disk_usage: 0.0,
                active_connections: 0,
                queue_depth: 0,
                storage_health: StorageHealth {
                    total_collections: 0,
                    total_keys: 0,
                    storage_size_bytes: 0,
                    compaction_status: CompactionStatus::Idle,
                    backup_status: BackupStatus::UpToDate,
                    index_health: IndexHealth {
                        total_indexes: 0,
                        healthy_indexes: 0,
                        rebuilding_indexes: 0,
                        corrupted_indexes: 0,
                    },
                },
                last_check: Instant::now(),
            }),
            query_performance: RwLock::new(HashMap::new()),
            metrics_semaphore: Arc::new(Semaphore::new(100)), // Limit concurrent metrics collection
        }
    }

    /// Record an operation metric
    pub async fn record_operation(&self, metric: OperationMetrics) -> Result<()> {
        if !self.config.enable_metrics {
            return Ok(());
        }

        // Rate limit metrics collection
        let _permit = self.metrics_semaphore.acquire().await?;

        // Store in history
        {
            let mut history = self.metrics_history.write();
            let metrics = history.entry(metric.operation_type).or_insert_with(Vec::new);
            metrics.push(metric.clone());

            // Maintain maximum sample size
            if metrics.len() > self.config.max_samples {
                metrics.remove(0);
            }
        }

        // Update aggregated statistics
        self.update_aggregated_stats(&metric);

        // Check for alerts
        if self.config.enable_health_monitoring {
            self.check_alerts(&metric).await?;
        }

        // Log detailed information for debugging
        debug!("Recorded operation: {:?}, duration: {:?}ms, success: {}",
               metric.operation_type,
               metric.duration.as_millis(),
               metric.success);

        Ok(())
    }

    /// Get performance statistics for an operation type
    pub fn get_performance_stats(&self, operation_type: OperationType) -> Option<PerformanceStats> {
        let stats = self.aggregated_stats.read();
        stats.get(&operation_type).cloned()
    }

    /// Get all performance statistics
    pub fn get_all_performance_stats(&self) -> HashMap<OperationType, PerformanceStats> {
        self.aggregated_stats.read().clone()
    }

    /// Get recent metrics for an operation type
    pub fn get_recent_metrics(&self, operation_type: OperationType, limit: usize) -> Vec<OperationMetrics> {
        let history = self.metrics_history.read();
        if let Some(metrics) = history.get(&operation_type) {
            let start = if metrics.len() > limit { metrics.len() - limit } else { 0 };
            metrics[start..].to_vec()
        } else {
            Vec::new()
        }
    }

    /// Get system health status
    pub fn get_system_health(&self) -> SystemHealth {
        self.system_health.read().clone()
    }

    /// Get active alerts
    pub fn get_active_alerts(&self) -> Vec<Alert> {
        self.active_alerts.read().clone()
    }

    /// Get alerts by severity
    pub fn get_alerts_by_severity(&self, severity: AlertSeverity) -> Vec<Alert> {
        let alerts = self.active_alerts.read();
        alerts.iter()
            .filter(|alert| alert.severity == severity)
            .cloned()
            .collect()
    }

    /// Resolve an alert
    pub fn resolve_alert(&self, alert_id: &str) -> Result<bool> {
        let mut alerts = self.active_alerts.write();
        if let Some(alert) = alerts.iter_mut().find(|a| a.alert_id == alert_id) {
            alert.resolved_at = Some(Instant::now());
            info!("Resolved alert: {}", alert_id);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Update system health
    pub async fn update_system_health(&self, store: &Store) -> Result<SystemHealth> {
        if !self.config.enable_health_monitoring {
            return Ok(self.system_health.read().clone());
        }

        let health = self.collect_system_health(store).await?;

        // Update stored health
        {
            let mut stored_health = self.system_health.write();
            *stored_health = health.clone();
        }

        // Check for health-related alerts
        self.check_health_alerts(&health).await?;

        Ok(health)
    }

    /// Clear all metrics
    pub fn clear_metrics(&self) {
        self.metrics_history.write().clear();
        self.aggregated_stats.write().clear();
        self.query_performance.write().clear();
        info!("Cleared all performance metrics");
    }

    /// Export metrics in Prometheus format
    pub fn export_prometheus_metrics(&self) -> String {
        let mut output = String::new();

        // Performance metrics
        let stats = self.aggregated_stats.read();
        for (operation_type, stat) in stats.iter() {
            let op_name = format!("{:?}", operation_type).to_lowercase();

            output.push_str(&format!(
                "# HELP zcore_storage_{}_operations_total Total number of {} operations\n",
                op_name, op_name
            ));
            output.push_str(&format!(
                "# TYPE zcore_storage_{}_operations_total counter\n",
                op_name
            ));
            output.push_str(&format!(
                "zcore_storage_{}_operations_total {}\n\n",
                op_name, stat.total_operations
            ));

            output.push_str(&format!(
                "# HELP zcore_storage_{}_duration_ms Average duration of {} operations in milliseconds\n",
                op_name, op_name
            ));
            output.push_str(&format!(
                "# TYPE zcore_storage_{}_duration_ms gauge\n",
                op_name
            ));
            output.push_str(&format!(
                "zcore_storage_{}_duration_ms {:.2}\n\n",
                op_name, stat.avg_duration_ms
            ));

            output.push_str(&format!(
                "# HELP zcore_storage_{}_error_rate Error rate for {} operations\n",
                op_name, op_name
            ));
            output.push_str(&format!(
                "# TYPE zcore_storage_{}_error_rate gauge\n",
                op_name
            ));
            output.push_str(&format!(
                "zcore_storage_{}_error_rate {:.4}\n\n",
                op_name, stat.error_rate
            ));
        }

        // System health metrics
        let health = self.system_health.read();
        output.push_str("# HELP zcore_storage_cpu_usage CPU usage ratio\n");
        output.push_str("# TYPE zcore_storage_cpu_usage gauge\n");
        output.push_str(&format!("zcore_storage_cpu_usage {:.4}\n", health.cpu_usage));

        output.push_str("# HELP zcore_storage_memory_usage Memory usage ratio\n");
        output.push_str("# TYPE zcore_storage_memory_usage gauge\n");
        output.push_str(&format!("zcore_storage_memory_usage {:.4}\n", health.memory_usage));

        output.push_str("# HELP zcore_storage_disk_usage Disk usage ratio\n");
        output.push_str("# TYPE zcore_storage_disk_usage gauge\n");
        output.push_str(&format!("zcore_storage_disk_usage {:.4}\n", health.disk_usage));

        // Alert metrics
        let alerts = self.active_alerts.read();
        output.push_str("# HELP zcore_storage_active_alerts Number of active alerts\n");
        output.push_str("# TYPE zcore_storage_active_alerts gauge\n");
        output.push_str(&format!("zcore_storage_active_alerts {}\n", alerts.len()));

        for severity in [AlertSeverity::Info, AlertSeverity::Warning, AlertSeverity::Error, AlertSeverity::Critical] {
            let count = alerts.iter().filter(|a| a.severity == severity).count();
            let severity_name = format!("{:?}", severity).to_lowercase();
            output.push_str(&format!(
                "# HELP zcore_storage_alerts_{}_count Number of {} alerts\n",
                severity_name, severity_name
            ));
            output.push_str(&format!(
                "# TYPE zcore_storage_alerts_{}_count gauge\n",
                severity_name
            ));
            output.push_str(&format!(
                "zcore_storage_alerts_{}_count {}\n",
                severity_name, count
            ));
        }

        output
    }

    // Private methods

    fn update_aggregated_stats(&self, metric: &OperationMetrics) {
        let mut stats = self.aggregated_stats.write();
        let stat = stats.entry(metric.operation_type).or_insert_with(PerformanceStats::default);

        stat.total_operations += 1;
        if metric.success {
            stat.successful_operations += 1;
        } else {
            stat.failed_operations += 1;
        }

        let duration_ms = metric.duration.as_millis() as f64;
        stat.total_data_bytes += metric.data_size_bytes;
        stat.total_rows_affected += metric.rows_affected;

        // Update duration statistics
        if duration_ms < stat.min_duration_ms {
            stat.min_duration_ms = duration_ms;
        }
        if duration_ms > stat.max_duration_ms {
            stat.max_duration_ms = duration_ms;
        }

        // Calculate new average
        if stat.total_operations > 0 {
            stat.avg_duration_ms = ((stat.avg_duration_ms * (stat.total_operations - 1) as f64) + duration_ms) / stat.total_operations as f64;
        }

        // Calculate error rate
        stat.error_rate = stat.failed_operations as f64 / stat.total_operations as f64;

        // Update percentiles (simplified - in production, you'd want proper percentile calculation)
        self.update_percentiles(stat, duration_ms);
    }

    fn update_percentiles(&self, stat: &mut PerformanceStats, duration_ms: f64) {
        // This is a simplified percentile calculation
        // In production, you'd want to maintain a sorted list of durations
        // or use a proper percentile calculation algorithm

        // For now, just use approximations based on recent activity
        let op_type = stat.operation_type;
        let recent_metrics = self.get_recent_metrics(op_type, 100);

        if !recent_metrics.is_empty() {
            let mut durations: Vec<f64> = recent_metrics.iter()
                .map(|m| m.duration.as_millis() as f64)
                .collect();
            durations.sort_by(|a, b| a.partial_cmp(b).unwrap());

            let len = durations.len();
            if len > 0 {
                stat.p95_duration_ms = durations[(len as f64 * 0.95) as usize];
                stat.p99_duration_ms = durations[(len as f64 * 0.99) as usize];
            }
        }
    }

    async fn check_alerts(&self, metric: &OperationMetrics) -> Result<()> {
        let threshold = &self.config.alert_thresholds;
        let duration_ms = metric.duration.as_millis() as u64;

        // Check latency alerts
        match metric.operation_type {
            OperationType::Read | OperationType::Scan => {
                if duration_ms > threshold.read_latency_ms {
                    self.create_alert(
                        AlertSeverity::Warning,
                        AlertType::HighLatency,
                        format!("High read latency detected: {}ms", duration_ms),
                        vec![
                            ("operation_type".to_string(), format!("{:?}", metric.operation_type)),
                            ("duration_ms".to_string(), duration_ms.to_string()),
                            ("threshold_ms".to_string(), threshold.read_latency_ms.to_string()),
                        ],
                    ).await?;
                }
            }
            OperationType::Write | OperationType::Delete | OperationType::Batch => {
                if duration_ms > threshold.write_latency_ms {
                    self.create_alert(
                        AlertSeverity::Warning,
                        AlertType::HighLatency,
                        format!("High write latency detected: {}ms", duration_ms),
                        vec![
                            ("operation_type".to_string(), format!("{:?}", metric.operation_type)),
                            ("duration_ms".to_string(), duration_ms.to_string()),
                            ("threshold_ms".to_string(), threshold.write_latency_ms.to_string()),
                        ],
                    ).await?;
                }
            }
            _ => {}
        }

        // Check error rate alerts
        if !metric.success {
            self.create_alert(
                AlertSeverity::Error,
                AlertType::HighErrorRate,
                "Operation failed".to_string(),
                vec![
                    ("operation_type".to_string(), format!("{:?}", metric.operation_type)),
                    ("error_message".to_string(), metric.error_message.clone().unwrap_or_default()),
                ],
            ).await?;
        }

        Ok(())
    }

    async fn check_health_alerts(&self, health: &SystemHealth) -> Result<()> {
        let threshold = &self.config.alert_thresholds;

        // Check resource usage alerts
        if health.memory_usage > threshold.memory_usage_ratio {
            self.create_alert(
                AlertSeverity::Warning,
                AlertType::ResourceExhaustion,
                format!("High memory usage: {:.1}%", health.memory_usage * 100.0),
                vec![
                    ("memory_usage".to_string(), format!("{:.4}", health.memory_usage)),
                    ("threshold".to_string(), format!("{:.4}", threshold.memory_usage_ratio)),
                ],
            ).await?;
        }

        if health.disk_usage > threshold.disk_usage_ratio {
            self.create_alert(
                AlertSeverity::Critical,
                AlertType::ResourceExhaustion,
                format!("High disk usage: {:.1}%", health.disk_usage * 100.0),
                vec![
                    ("disk_usage".to_string(), format!("{:.4}", health.disk_usage)),
                    ("threshold".to_string(), format!("{:.4}", threshold.disk_usage_ratio)),
                ],
            ).await?;
        }

        if health.cpu_usage > threshold.cpu_usage_ratio {
            self.create_alert(
                AlertSeverity::Warning,
                AlertType::ResourceExhaustion,
                format!("High CPU usage: {:.1}%", health.cpu_usage * 100.0),
                vec![
                    ("cpu_usage".to_string(), format!("{:.4}", health.cpu_usage)),
                    ("threshold".to_string(), format!("{:.4}", threshold.cpu_usage_ratio)),
                ],
            ).await?;
        }

        // Check storage health alerts
        if health.storage_health.compaction_status == CompactionStatus::Failed {
            self.create_alert(
                AlertSeverity::Error,
                AlertType::StorageIssue,
                "Compaction failed".to_string(),
                vec![
                    ("compaction_status".to_string(), "failed".to_string()),
                ],
            ).await?;
        }

        if health.storage_health.backup_status == BackupStatus::Failed {
            self.create_alert(
                AlertSeverity::Critical,
                AlertType::BackupFailure,
                "Backup failed".to_string(),
                vec![
                    ("backup_status".to_string(), "failed".to_string()),
                ],
            ).await?;
        }

        if health.storage_health.index_health.corrupted_indexes > 0 {
            self.create_alert(
                AlertSeverity::Critical,
                AlertType::IndexCorruption,
                format!("{} corrupted indexes detected", health.storage_health.index_health.corrupted_indexes),
                vec![
                    ("corrupted_count".to_string(), health.storage_health.index_health.corrupted_indexes.to_string()),
                ],
            ).await?;
        }

        Ok(())
    }

    async fn create_alert(&self, severity: AlertSeverity, alert_type: AlertType,
                         message: String, details: Vec<(String, String)>) -> Result<()> {
        let alert = Alert {
            alert_id: uuid::Uuid::new_v4().to_string(),
            severity,
            alert_type,
            message: message.clone(),
            details: details.into_iter().collect(),
            created_at: Instant::now(),
            resolved_at: None,
        };

        // Store alert
        {
            let mut alerts = self.active_alerts.write();
            alerts.push(alert.clone());

            // Limit number of stored alerts
            if alerts.len() > 1000 {
                alerts.remove(0);
            }
        }

        // Log alert
        match severity {
            AlertSeverity::Info => info!("ALERT: {}", message),
            AlertSeverity::Warning => warn!("ALERT: {}", message),
            AlertSeverity::Error | AlertSeverity::Critical => error!("ALERT: {}", message),
        }

        Ok(())
    }

    async fn collect_system_health(&self, store: &Store) -> Result<SystemHealth> {
        // This is a simplified health collection
        // In production, you'd want to collect actual system metrics

        let cpu_usage = self.get_cpu_usage().await?;
        let memory_usage = self.get_memory_usage().await?;
        let disk_usage = self.get_disk_usage().await?;

        let storage_health = StorageHealth {
            total_collections: 42, // TODO: Get actual count
            total_keys: 1000000,   // TODO: Get actual count
            storage_size_bytes: self.get_storage_size(store)?,
            compaction_status: CompactionStatus::Idle, // TODO: Get actual status
            backup_status: BackupStatus::UpToDate,     // TODO: Get actual status
            index_health: IndexHealth {
                total_indexes: 10,      // TODO: Get actual count
                healthy_indexes: 10,    // TODO: Get actual count
                rebuilding_indexes: 0,  // TODO: Get actual count
                corrupted_indexes: 0,   // TODO: Get actual count
            },
        };

        let overall_status = if cpu_usage > self.config.alert_thresholds.cpu_usage_ratio ||
                             memory_usage > self.config.alert_thresholds.memory_usage_ratio ||
                             disk_usage > self.config.alert_thresholds.disk_usage_ratio {
            HealthStatus::Critical
        } else if cpu_usage > 0.7 || memory_usage > 0.7 || disk_usage > 0.7 {
            HealthStatus::Warning
        } else {
            HealthStatus::Healthy
        };

        Ok(SystemHealth {
            status: overall_status,
            cpu_usage,
            memory_usage,
            disk_usage,
            active_connections: 100, // TODO: Get actual count
            queue_depth: 10,         // TODO: Get actual count
            storage_health,
            last_check: Instant::now(),
        })
    }

    async fn get_cpu_usage(&self) -> Result<f64> {
        // TODO: Implement actual CPU usage collection
        // For now, return a placeholder value
        Ok(0.3)
    }

    async fn get_memory_usage(&self) -> Result<f64> {
        // TODO: Implement actual memory usage collection
        // For now, return a placeholder value
        Ok(0.4)
    }

    async fn get_disk_usage(&self) -> Result<f64> {
        // TODO: Implement actual disk usage collection
        // For now, return a placeholder value
        Ok(0.5)
    }

    fn get_storage_size(&self, _store: &Store) -> Result<u64> {
        // TODO: Implement actual storage size calculation
        // For now, return a placeholder value
        Ok(1024 * 1024 * 1024) // 1GB
    }
}

/// Performance monitoring wrapper for storage operations
pub struct MonitoredStore {
    store: Store,
    observability: Arc<ObservabilityManager>,
}

impl MonitoredStore {
    pub fn new(store: Store, observability: Arc<ObservabilityManager>) -> Self {
        Self {
            store,
            observability,
        }
    }

    pub async fn monitored_get(&self, collection: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut metric = OperationMetrics::new(OperationType::Read);

        let result = {
            let tx = ReadTransaction::new();
            tx.get(&self.store, collection, key)
        };

        match result {
            Ok(Some(value)) => {
                metric = metric.with_data_size(value.len() as u64);
                let final_metric = metric.finish(true, None);
                self.observability.record_operation(final_metric).await?;
                Ok(Some(value))
            }
            Ok(None) => {
                let final_metric = metric.finish(true, None);
                self.observability.record_operation(final_metric).await?;
                Ok(None)
            }
            Err(e) => {
                let final_metric = metric.finish(false, Some(e.to_string()));
                self.observability.record_operation(final_metric).await?;
                Err(e)
            }
        }
    }

    pub async fn monitored_set(&self, collection: &str, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut metric = OperationMetrics::new(OperationType::Write);
        metric = metric.with_data_size((key.len() + value.len()) as u64);

        let mut tx = WriteTransaction::new();
        tx.set(collection, key, value);

        let result = tx.commit(&self.store);

        match result {
            Ok(()) => {
                let final_metric = metric.finish(true, None);
                self.observability.record_operation(final_metric).await?;
                Ok(())
            }
            Err(e) => {
                let final_metric = metric.finish(false, Some(e.to_string()));
                self.observability.record_operation(final_metric).await?;
                Err(e)
            }
        }
    }

    pub async fn monitored_scan(&self, collection: &str, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut metric = OperationMetrics::new(OperationType::Scan);

        let result = {
            let tx = ReadTransaction::new();
            tx.scan_prefix(&self.store, collection, prefix)
        };

        match result {
            Ok(results) => {
                let total_size: u64 = results.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum();
                metric = metric.with_data_size(total_size).with_rows_affected(results.len() as u64);
                let final_metric = metric.finish(true, None);
                self.observability.record_operation(final_metric).await?;
                Ok(results)
            }
            Err(e) => {
                let final_metric = metric.finish(false, Some(e.to_string()));
                self.observability.record_operation(final_metric).await?;
                Err(e)
            }
        }
    }

    /// Get the underlying store
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Get the observability manager
    pub fn observability(&self) -> &Arc<ObservabilityManager> {
        &self.observability
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_observability_manager() {
        let config = ObservabilityConfig::default();
        let manager = ObservabilityManager::new(config);

        // Record some operations
        let metric1 = OperationMetrics::new(OperationType::Read)
            .finish(true, None);
        manager.record_operation(metric1).await.unwrap();

        let metric2 = OperationMetrics::new(OperationType::Write)
            .with_data_size(1024)
            .finish(true, None);
        manager.record_operation(metric2).await.unwrap();

        let metric3 = OperationMetrics::new(OperationType::Read)
            .finish(false, Some("Connection error".to_string()));
        manager.record_operation(metric3).await.unwrap();

        // Check statistics
        let read_stats = manager.get_performance_stats(OperationType::Read).unwrap();
        assert_eq!(read_stats.total_operations, 2);
        assert_eq!(read_stats.successful_operations, 1);
        assert_eq!(read_stats.failed_operations, 1);
        assert_eq!(read_stats.error_rate, 0.5);

        let write_stats = manager.get_performance_stats(OperationType::Write).unwrap();
        assert_eq!(write_stats.total_operations, 1);
        assert_eq!(write_stats.successful_operations, 1);
        assert_eq!(write_stats.total_data_bytes, 1024);

        // Get recent metrics
        let recent_reads = manager.get_recent_metrics(OperationType::Read, 10);
        assert_eq!(recent_reads.len(), 2);

        // Test system health
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("health_test")).unwrap();
        let health = manager.update_system_health(&store).await.unwrap();
        assert!(matches!(health.status, HealthStatus::Healthy | HealthStatus::Warning));
    }

    #[tokio::test]
    async fn test_alert_system() {
        let mut config = ObservabilityConfig::default();
        config.alert_thresholds.read_latency_ms = 10; // Low threshold for testing
        let manager = ObservabilityManager::new(config);

        // Record a slow operation to trigger alert
        let mut metric = OperationMetrics::new(OperationType::Read);
        metric.start_time = Instant::now() - Duration::from_millis(100); // Simulate slow operation
        let metric = metric.finish(true, None);
        manager.record_operation(metric).await.unwrap();

        // Check if alert was created
        let alerts = manager.get_active_alerts();
        assert!(!alerts.is_empty());

        let warning_alerts = manager.get_alerts_by_severity(AlertSeverity::Warning);
        assert!(!warning_alerts.is_empty());

        // Resolve alert
        let alert_id = &warning_alerts[0].alert_id;
        let resolved = manager.resolve_alert(alert_id).unwrap();
        assert!(resolved);
    }

    #[tokio::test]
    async fn test_prometheus_export() {
        let manager = ObservabilityManager::new(ObservabilityConfig::default());

        // Record some operations
        let metric = OperationMetrics::new(OperationType::Read)
            .finish(true, None);
        manager.record_operation(metric).await.unwrap();

        // Export metrics
        let prometheus_output = manager.export_prometheus_metrics();
        assert!(prometheus_output.contains("zcore_storage_read_operations_total"));
        assert!(prometheus_output.contains("zcore_storage_cpu_usage"));
        assert!(prometheus_output.contains("zcore_storage_active_alerts"));
    }

    #[tokio::test]
    async fn test_monitored_store() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("monitored_test")).unwrap();
        let observability = Arc::new(ObservabilityManager::new(ObservabilityConfig::default()));
        let monitored_store = MonitoredStore::new(store, observability);

        // Test monitored operations
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        // Set operation
        monitored_store.monitored_set("test_collection", key.clone(), value.clone()).await.unwrap();

        // Get operation
        let retrieved = monitored_store.monitored_get("test_collection", &key).await.unwrap();
        assert_eq!(retrieved, Some(value));

        // Scan operation
        let results = monitored_store.monitored_scan("test_collection", b"test").await.unwrap();
        assert!(!results.is_empty());

        // Check metrics were recorded
        let stats = monitored_store.observability().get_all_performance_stats();
        assert!(stats.contains_key(&OperationType::Read));
        assert!(stats.contains_key(&OperationType::Write));
        assert!(stats.contains_key(&OperationType::Scan));
    }
}