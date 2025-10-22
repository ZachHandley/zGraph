use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, info, warn};

use crate::events::{Event, EventType};
use crate::handlers::{EventHandler, HandlerResult};

/// Comprehensive event metrics collector
#[derive(Clone)]
pub struct EventMetricsCollector {
    /// Event processing metrics
    metrics: Arc<RwLock<EventMetrics>>,
    /// Event latency tracker
    latency_tracker: Arc<RwLock<LatencyTracker>>,
    /// Event rate limiter and analyzer
    rate_analyzer: Arc<RwLock<EventRateAnalyzer>>,
    /// Alert thresholds
    alert_config: Arc<RwLock<AlertConfig>>,
    /// Alert sender channel
    alert_sender: Option<mpsc::UnboundedSender<EventAlert>>,
}

/// Complete event system metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventMetrics {
    /// Total events processed
    pub total_events: u64,
    /// Events by type
    pub events_by_type: HashMap<String, EventTypeMetrics>,
    /// Events by organization
    pub events_by_org: HashMap<u64, OrgMetrics>,
    /// Event handler metrics
    pub handler_metrics: HashMap<String, HandlerMetrics>,
    /// System-wide performance metrics
    pub performance: PerformanceMetrics,
    /// Error and failure metrics
    pub error_metrics: ErrorMetrics,
    /// Real-time metrics (last 5 minutes)
    pub realtime_metrics: RealtimeMetrics,
}

/// Metrics for a specific event type
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventTypeMetrics {
    /// Number of events of this type
    pub count: u64,
    /// Average processing time in milliseconds
    pub avg_processing_time_ms: f64,
    /// Maximum processing time seen
    pub max_processing_time_ms: u64,
    /// Minimum processing time seen
    pub min_processing_time_ms: u64,
    /// Number of failed events
    pub failures: u64,
    /// Number of retried events
    pub retries: u64,
    /// Last event timestamp
    pub last_event: Option<chrono::DateTime<chrono::Utc>>,
    /// Event size metrics
    pub avg_event_size_bytes: f64,
}

/// Organization-specific metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OrgMetrics {
    /// Organization ID
    pub org_id: u64,
    /// Total events for this organization
    pub total_events: u64,
    /// Events by type for this organization
    pub events_by_type: HashMap<String, u64>,
    /// Average events per hour
    pub avg_events_per_hour: f64,
    /// Peak events per minute
    pub peak_events_per_minute: u64,
    /// Error rate for this organization
    pub error_rate: f64,
}

/// Handler-specific metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HandlerMetrics {
    /// Handler name/identifier
    pub handler_name: String,
    /// Total events processed by this handler
    pub events_processed: u64,
    /// Successful processing count
    pub successes: u64,
    /// Failed processing count
    pub failures: u64,
    /// Retry count
    pub retries: u64,
    /// Average processing time
    pub avg_processing_time_ms: f64,
    /// Handler error rate
    pub error_rate: f64,
    /// Events currently being processed
    pub in_flight: u64,
}

/// System-wide performance metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    /// Events processed per second
    pub events_per_second: f64,
    /// Average event processing latency
    pub avg_latency_ms: f64,
    /// 95th percentile latency
    pub p95_latency_ms: f64,
    /// 99th percentile latency
    pub p99_latency_ms: f64,
    /// Memory usage in bytes
    pub memory_usage_bytes: u64,
    /// Event queue depth
    pub queue_depth: u64,
    /// Handler thread pool utilization
    pub thread_utilization: f64,
}

/// Error and failure tracking
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ErrorMetrics {
    /// Total errors across all events
    pub total_errors: u64,
    /// Error rate (errors/total events)
    pub error_rate: f64,
    /// Errors by type
    pub errors_by_type: HashMap<String, u64>,
    /// Errors by handler
    pub errors_by_handler: HashMap<String, u64>,
    /// Critical errors that require attention
    pub critical_errors: u64,
    /// Recent error messages (last 100)
    pub recent_errors: Vec<ErrorEntry>,
}

/// Error entry for tracking recent errors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorEntry {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub event_type: String,
    pub handler_name: String,
    pub error_message: String,
    pub is_critical: bool,
}

/// Real-time metrics (rolling window)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RealtimeMetrics {
    /// Events in last 5 minutes
    pub events_last_5min: u64,
    /// Events in last hour
    pub events_last_hour: u64,
    /// Current events per minute
    pub current_events_per_minute: f64,
    /// Trend (increasing, decreasing, stable)
    pub trend: EventTrend,
    /// Top event types in last hour
    pub top_event_types: Vec<(String, u64)>,
    /// Recent latency samples
    pub recent_latencies: Vec<f64>,
}

/// Event trend analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventTrend {
    Increasing,
    Decreasing,
    Stable,
    Volatile,
}

impl Default for EventTrend {
    fn default() -> Self {
        EventTrend::Stable
    }
}

/// Event latency tracker
#[derive(Debug)]
pub struct LatencyTracker {
    /// Latency samples for different percentiles
    samples: Vec<f64>,
    /// Maximum samples to keep
    max_samples: usize,
    /// Sample index (circular buffer)
    sample_index: usize,
}

impl LatencyTracker {
    pub fn new(max_samples: usize) -> Self {
        Self {
            samples: Vec::with_capacity(max_samples),
            max_samples,
            sample_index: 0,
        }
    }

    pub fn add_sample(&mut self, latency_ms: f64) {
        if self.samples.len() < self.max_samples {
            self.samples.push(latency_ms);
        } else {
            self.samples[self.sample_index] = latency_ms;
            self.sample_index = (self.sample_index + 1) % self.max_samples;
        }
    }

    pub fn calculate_percentiles(&self) -> (f64, f64, f64) {
        if self.samples.is_empty() {
            return (0.0, 0.0, 0.0);
        }

        let mut sorted_samples = self.samples.clone();
        sorted_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let len = sorted_samples.len();
        let p50 = sorted_samples[len * 50 / 100];
        let p95 = sorted_samples[len * 95 / 100];
        let p99 = sorted_samples[len * 99 / 100];

        (p50, p95, p99)
    }

    pub fn average(&self) -> f64 {
        if self.samples.is_empty() {
            0.0
        } else {
            self.samples.iter().sum::<f64>() / self.samples.len() as f64
        }
    }
}

/// Event rate analyzer for detecting spikes and anomalies
#[derive(Debug)]
pub struct EventRateAnalyzer {
    /// Event counts in time windows
    time_windows: Vec<TimeWindow>,
    /// Current window index
    current_window: usize,
    /// Window duration in seconds
    window_duration: u64,
    /// Number of windows to track
    num_windows: usize,
}

#[derive(Debug, Clone)]
struct TimeWindow {
    start_time: Instant,
    event_count: u64,
    events_by_type: HashMap<String, u64>,
}

impl EventRateAnalyzer {
    pub fn new(window_duration_seconds: u64, num_windows: usize) -> Self {
        let mut time_windows = Vec::with_capacity(num_windows);
        let now = Instant::now();

        for i in 0..num_windows {
            time_windows.push(TimeWindow {
                start_time: now - Duration::from_secs(window_duration_seconds * (num_windows - i) as u64),
                event_count: 0,
                events_by_type: HashMap::new(),
            });
        }

        Self {
            time_windows,
            current_window: 0,
            window_duration: window_duration_seconds,
            num_windows,
        }
    }

    pub fn record_event(&mut self, event_type: &str) {
        let now = Instant::now();
        let current = &mut self.time_windows[self.current_window];

        // Check if we need to advance to next window
        if now.duration_since(current.start_time).as_secs() >= self.window_duration {
            self.advance_window();
            let current = &mut self.time_windows[self.current_window];
        }

        let current = &mut self.time_windows[self.current_window];
        current.event_count += 1;
        *current.events_by_type.entry(event_type.to_string()).or_insert(0) += 1;
    }

    fn advance_window(&mut self) {
        self.current_window = (self.current_window + 1) % self.num_windows;
        let now = Instant::now();

        // Reset the current window
        self.time_windows[self.current_window] = TimeWindow {
            start_time: now,
            event_count: 0,
            events_by_type: HashMap::new(),
        };
    }

    pub fn calculate_trend(&self) -> EventTrend {
        if self.time_windows.len() < 3 {
            return EventTrend::Stable;
        }

        let recent_windows = &self.time_windows[(self.num_windows - 3)..];
        let counts: Vec<u64> = recent_windows.iter().map(|w| w.event_count).collect();

        let first = counts[0] as f64;
        let last = counts[2] as f64;

        if last > first * 1.5 {
            EventTrend::Increasing
        } else if last < first * 0.5 {
            EventTrend::Decreasing
        } else {
            // Check for volatility
            let variance = self.calculate_variance(&counts);
            if variance > first * 0.3 {
                EventTrend::Volatile
            } else {
                EventTrend::Stable
            }
        }
    }

    fn calculate_variance(&self, counts: &[u64]) -> f64 {
        let mean = counts.iter().sum::<u64>() as f64 / counts.len() as f64;
        let variance = counts
            .iter()
            .map(|&x| (x as f64 - mean).powi(2))
            .sum::<f64>() / counts.len() as f64;
        variance
    }

    pub fn events_per_second(&self) -> f64 {
        let total_events: u64 = self.time_windows.iter().map(|w| w.event_count).sum();
        let total_duration = self.window_duration * (self.num_windows as u64);
        total_events as f64 / total_duration as f64
    }
}

/// Alert configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertConfig {
    /// Enable/disable alerting
    pub enabled: bool,
    /// Maximum events per second before alert
    pub max_events_per_second: f64,
    /// Maximum latency before alert (in milliseconds)
    pub max_latency_ms: f64,
    /// Maximum error rate before alert (0.0-1.0)
    pub max_error_rate: f64,
    /// Maximum handler failures before alert
    pub max_handler_failures: u64,
    /// Minimum time between similar alerts (in seconds)
    pub alert_cooldown_seconds: u64,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_events_per_second: 1000.0,
            max_latency_ms: 5000.0,
            max_error_rate: 0.1, // 10%
            max_handler_failures: 100,
            alert_cooldown_seconds: 300, // 5 minutes
        }
    }
}

/// Event alert
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventAlert {
    pub alert_id: String,
    pub alert_type: AlertType,
    pub severity: AlertSeverity,
    pub title: String,
    pub description: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Types of alerts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertType {
    HighLatency,
    HighErrorRate,
    EventSpike,
    HandlerFailure,
    SystemOverload,
    Custom(String),
}

/// Alert severity levels
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

impl EventMetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> (Self, Option<mpsc::UnboundedReceiver<EventAlert>>) {
        let (alert_sender, alert_receiver) = mpsc::unbounded_channel();

        let collector = Self {
            metrics: Arc::new(RwLock::new(EventMetrics::default())),
            latency_tracker: Arc::new(RwLock::new(LatencyTracker::new(1000))),
            rate_analyzer: Arc::new(RwLock::new(EventRateAnalyzer::new(60, 10))), // 60-second windows, 10 windows
            alert_config: Arc::new(RwLock::new(AlertConfig::default())),
            alert_sender: Some(alert_sender),
        };

        (collector, Some(alert_receiver))
    }

    /// Create a collector without alerting
    pub fn new_without_alerts() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(EventMetrics::default())),
            latency_tracker: Arc::new(RwLock::new(LatencyTracker::new(1000))),
            rate_analyzer: Arc::new(RwLock::new(EventRateAnalyzer::new(60, 10))),
            alert_config: Arc::new(RwLock::new(AlertConfig::default())),
            alert_sender: None,
        }
    }

    /// Record an event being processed
    pub async fn record_event_start(&self, event: &Event) -> EventProcessingHandle {
        let start_time = Instant::now();

        // Update basic metrics
        {
            let mut metrics = self.metrics.write().await;
            metrics.total_events += 1;

            // Update organization metrics
            let org_metrics = metrics.events_by_org.entry(event.org_id).or_default();
            org_metrics.org_id = event.org_id;
            org_metrics.total_events += 1;
            *org_metrics.events_by_type.entry(event.event_type.to_string()).or_insert(0) += 1;

            // Update event type metrics
            let event_type_metrics = metrics.events_by_type.entry(event.event_type.to_string()).or_default();
            event_type_metrics.count += 1;
            event_type_metrics.last_event = Some(event.timestamp);

            // Estimate event size
            if let Ok(serialized) = serde_json::to_vec(event) {
                let size = serialized.len() as f64;
                if event_type_metrics.avg_event_size_bytes == 0.0 {
                    event_type_metrics.avg_event_size_bytes = size;
                } else {
                    event_type_metrics.avg_event_size_bytes = (event_type_metrics.avg_event_size_bytes + size) / 2.0;
                }
            }
        }

        // Update rate analyzer
        self.rate_analyzer.write().await.record_event(&event.event_type.to_string());

        EventProcessingHandle {
            start_time,
            event_id: event.id.to_string(),
            event_type: event.event_type.clone(),
            org_id: event.org_id,
            metrics_collector: self.clone(),
        }
    }

    /// Record an event processing completion
    pub async fn record_event_complete(
        &self,
        handle: EventProcessingHandle,
        handler_name: &str,
        result: &HandlerResult,
    ) {
        let duration = handle.start_time.elapsed();
        let duration_ms = duration.as_millis() as f64;

        // Update latency tracker
        self.latency_tracker.write().await.add_sample(duration_ms);

        // Update metrics
        {
            let mut metrics = self.metrics.write().await;

            // Update event type metrics
            if let Some(event_metrics) = metrics.events_by_type.get_mut(&handle.event_type.to_string()) {
                // Update timing
                if event_metrics.avg_processing_time_ms == 0.0 {
                    event_metrics.avg_processing_time_ms = duration_ms;
                } else {
                    event_metrics.avg_processing_time_ms = (event_metrics.avg_processing_time_ms + duration_ms) / 2.0;
                }

                if duration_ms > event_metrics.max_processing_time_ms as f64 {
                    event_metrics.max_processing_time_ms = duration_ms as u64;
                }

                if event_metrics.min_processing_time_ms == 0 || duration_ms < event_metrics.min_processing_time_ms as f64 {
                    event_metrics.min_processing_time_ms = duration_ms as u64;
                }

                // Update result metrics
                match result {
                    HandlerResult::Success => {}
                    HandlerResult::Retry => event_metrics.retries += 1,
                    HandlerResult::Discard => event_metrics.failures += 1,
                    HandlerResult::DeadLetter(_) => event_metrics.failures += 1,
                }
            }

            // Update handler metrics
            {
                let handler_metrics = metrics.handler_metrics.entry(handler_name.to_string()).or_default();
                handler_metrics.handler_name = handler_name.to_string();
                handler_metrics.events_processed += 1;

                match result {
                    HandlerResult::Success => handler_metrics.successes += 1,
                    HandlerResult::Retry => handler_metrics.retries += 1,
                    HandlerResult::Discard | HandlerResult::DeadLetter(_) => {
                        handler_metrics.failures += 1;
                    }
                }

                // Update handler performance
                if handler_metrics.avg_processing_time_ms == 0.0 {
                    handler_metrics.avg_processing_time_ms = duration_ms;
                } else {
                    handler_metrics.avg_processing_time_ms = (handler_metrics.avg_processing_time_ms + duration_ms) / 2.0;
                }

                // Calculate error rate
                if handler_metrics.events_processed > 0 {
                    handler_metrics.error_rate = handler_metrics.failures as f64 / handler_metrics.events_processed as f64;
                }
            }

            // Update error metrics if needed
            if matches!(result, HandlerResult::Discard | HandlerResult::DeadLetter(_)) {
                metrics.error_metrics.total_errors += 1;
                *metrics.error_metrics.errors_by_type.entry(handle.event_type.to_string()).or_insert(0) += 1;
                *metrics.error_metrics.errors_by_handler.entry(handler_name.to_string()).or_insert(0) += 1;

                // Add to recent errors
                let error_entry = ErrorEntry {
                    timestamp: chrono::Utc::now(),
                    event_type: handle.event_type.to_string(),
                    handler_name: handler_name.to_string(),
                    error_message: format!("Handler failed for event {}", handle.event_id),
                    is_critical: false,
                };

                metrics.error_metrics.recent_errors.push(error_entry);
                if metrics.error_metrics.recent_errors.len() > 100 {
                    metrics.error_metrics.recent_errors.remove(0);
                }
            }

            // Update system error rate
            if metrics.total_events > 0 {
                metrics.error_metrics.error_rate = metrics.error_metrics.total_errors as f64 / metrics.total_events as f64;
            }
        }

        // Check for alerts
        self.check_alerts(&handle, handler_name, result, duration_ms).await;

        debug!(
            event_id = %handle.event_id,
            event_type = %handle.event_type,
            handler = %handler_name,
            duration_ms = duration_ms,
            result = ?result,
            "Event processing metrics recorded"
        );
    }

    /// Check if any alert conditions are met
    async fn check_alerts(
        &self,
        handle: &EventProcessingHandle,
        handler_name: &str,
        result: &HandlerResult,
        duration_ms: f64,
    ) {
        let alert_config = self.alert_config.read().await;
        if !alert_config.enabled {
            return;
        }

        let mut alerts = Vec::new();

        // Check latency alerts
        if duration_ms > alert_config.max_latency_ms {
            alerts.push(EventAlert {
                alert_id: format!("latency_{}", ulid::Ulid::new()),
                alert_type: AlertType::HighLatency,
                severity: if duration_ms > alert_config.max_latency_ms * 2.0 {
                    AlertSeverity::Error
                } else {
                    AlertSeverity::Warning
                },
                title: "High Event Processing Latency".to_string(),
                description: format!(
                    "Event {} took {}ms to process (threshold: {}ms)",
                    handle.event_id, duration_ms, alert_config.max_latency_ms
                ),
                timestamp: chrono::Utc::now(),
                metadata: {
                    let mut map = HashMap::new();
                    map.insert("event_id".to_string(), serde_json::Value::String(handle.event_id.clone()));
                    map.insert("event_type".to_string(), serde_json::Value::String(handle.event_type.to_string()));
                    map.insert("handler".to_string(), serde_json::Value::String(handler_name.to_string()));
                    map.insert("duration_ms".to_string(), serde_json::Value::Number(serde_json::Number::from_f64(duration_ms).unwrap()));
                    map
                },
            });
        }

        // Check handler failure alerts
        if matches!(result, HandlerResult::Retry) {
            let metrics = self.metrics.read().await;
            if let Some(handler_metrics) = metrics.handler_metrics.get(handler_name) {
                if handler_metrics.failures >= alert_config.max_handler_failures {
                    alerts.push(EventAlert {
                        alert_id: format!("handler_failure_{}", ulid::Ulid::new()),
                        alert_type: AlertType::HandlerFailure,
                        severity: AlertSeverity::Error,
                        title: "Handler Failure Threshold Exceeded".to_string(),
                        description: format!(
                            "Handler '{}' has failed {} times (threshold: {})",
                            handler_name, handler_metrics.failures, alert_config.max_handler_failures
                        ),
                        timestamp: chrono::Utc::now(),
                        metadata: {
                            let mut map = HashMap::new();
                            map.insert("handler".to_string(), serde_json::Value::String(handler_name.to_string()));
                            map.insert("failures".to_string(), serde_json::Value::Number(serde_json::Number::from(handler_metrics.failures)));
                            map
                        },
                    });
                }
            }
        }

        // Check event rate alerts
        let events_per_second = self.rate_analyzer.read().await.events_per_second();
        if events_per_second > alert_config.max_events_per_second {
            alerts.push(EventAlert {
                alert_id: format!("event_spike_{}", ulid::Ulid::new()),
                alert_type: AlertType::EventSpike,
                severity: if events_per_second > alert_config.max_events_per_second * 2.0 {
                    AlertSeverity::Critical
                } else {
                    AlertSeverity::Warning
                },
                title: "High Event Rate Detected".to_string(),
                description: format!(
                    "Processing {:.2} events per second (threshold: {:.2})",
                    events_per_second, alert_config.max_events_per_second
                ),
                timestamp: chrono::Utc::now(),
                metadata: {
                    let mut map = HashMap::new();
                    map.insert("events_per_second".to_string(), serde_json::Value::Number(serde_json::Number::from_f64(events_per_second).unwrap()));
                    map
                },
            });
        }

        drop(alert_config);

        // Send alerts
        if let Some(sender) = &self.alert_sender {
            for alert in alerts {
                if let Err(e) = sender.send(alert) {
                    warn!("Failed to send alert: {}", e);
                }
            }
        }
    }

    /// Get current metrics snapshot
    pub async fn get_metrics(&self) -> EventMetrics {
        let mut metrics = self.metrics.read().await.clone();

        // Update performance metrics
        let latency_tracker = self.latency_tracker.read().await;
        let (p50, p95, p99) = latency_tracker.calculate_percentiles();
        metrics.performance.avg_latency_ms = latency_tracker.average();
        metrics.performance.p95_latency_ms = p95;
        metrics.performance.p99_latency_ms = p99;

        // Update rate metrics
        let rate_analyzer = self.rate_analyzer.read().await;
        metrics.performance.events_per_second = rate_analyzer.events_per_second();
        metrics.realtime_metrics.trend = rate_analyzer.calculate_trend();

        // Update realtime metrics
        metrics.realtime_metrics.recent_latencies = latency_tracker.samples.clone();

        metrics
    }

    /// Update alert configuration
    pub async fn update_alert_config(&self, config: AlertConfig) {
        *self.alert_config.write().await = config;
    }

    /// Get alert configuration
    pub async fn get_alert_config(&self) -> AlertConfig {
        self.alert_config.read().await.clone()
    }

    /// Reset all metrics (useful for testing)
    pub async fn reset_metrics(&self) {
        *self.metrics.write().await = EventMetrics::default();
        *self.latency_tracker.write().await = LatencyTracker::new(1000);
        *self.rate_analyzer.write().await = EventRateAnalyzer::new(60, 10);
    }
}

/// Handle for tracking event processing
pub struct EventProcessingHandle {
    start_time: Instant,
    event_id: String,
    event_type: EventType,
    org_id: u64,
    metrics_collector: EventMetricsCollector,
}

impl EventProcessingHandle {
    /// Complete event processing with success
    pub async fn success(self, handler_name: &str) {
        let collector = self.metrics_collector.clone();
        collector.record_event_complete(self, handler_name, &HandlerResult::Success).await;
    }

    /// Complete event processing with retry
    pub async fn retry(self, handler_name: &str) {
        let collector = self.metrics_collector.clone();
        collector.record_event_complete(self, handler_name, &HandlerResult::Retry).await;
    }

    /// Complete event processing with failure
    pub async fn failed(self, handler_name: &str) {
        let collector = self.metrics_collector.clone();
        collector.record_event_complete(self, handler_name, &HandlerResult::Retry).await;
    }
}

/// Event handler wrapper that automatically collects metrics
pub struct MetricsWrappedHandler {
    inner: Arc<dyn EventHandler>,
    handler_name: String,
    metrics_collector: EventMetricsCollector,
}

impl MetricsWrappedHandler {
    pub fn new(
        handler: Arc<dyn EventHandler>,
        handler_name: String,
        metrics_collector: EventMetricsCollector,
    ) -> Self {
        Self {
            inner: handler,
            handler_name,
            metrics_collector,
        }
    }
}

#[async_trait::async_trait]
impl EventHandler for MetricsWrappedHandler {
    async fn handle(&self, event: &Event) -> HandlerResult {
        let handle = self.metrics_collector.record_event_start(event).await;
        let result = self.inner.handle(event).await;
        self.metrics_collector.record_event_complete(handle, &self.handler_name, &result).await;
        result
    }

    fn interested_in(&self, event_type: &EventType) -> bool {
        self.inner.interested_in(event_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{EventData, EventId};

    #[tokio::test]
    async fn test_metrics_collector_creation() {
        let (collector, _receiver) = EventMetricsCollector::new();
        let metrics = collector.get_metrics().await;
        assert_eq!(metrics.total_events, 0);
    }

    #[tokio::test]
    async fn test_event_processing_metrics() {
        let (collector, _receiver) = EventMetricsCollector::new();

        let event = crate::events::Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 1,
                status: Some("pending".to_string()),
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );

        let handle = collector.record_event_start(&event).await;
        handle.success("test_handler").await;

        let metrics = collector.get_metrics().await;
        assert_eq!(metrics.total_events, 1);
        assert!(metrics.events_by_type.contains_key("job.created"));
        assert!(metrics.handler_metrics.contains_key("test_handler"));
        assert!(metrics.events_by_org.contains_key(&1));
    }

    #[tokio::test]
    async fn test_latency_tracker() {
        let mut tracker = LatencyTracker::new(100);

        tracker.add_sample(10.0);
        tracker.add_sample(20.0);
        tracker.add_sample(30.0);

        let avg = tracker.average();
        assert_eq!(avg, 20.0);

        let (p50, p95, p99) = tracker.calculate_percentiles();
        assert!(p50 >= 10.0 && p50 <= 30.0);
    }

    #[tokio::test]
    async fn test_rate_analyzer() {
        let mut analyzer = EventRateAnalyzer::new(1, 3); // 1 second windows

        analyzer.record_event("test_event");
        analyzer.record_event("test_event");

        let eps = analyzer.events_per_second();
        assert!(eps > 0.0);
    }

    #[tokio::test]
    async fn test_alert_generation() {
        let (collector, mut receiver) = EventMetricsCollector::new();

        // Set low thresholds for testing
        let alert_config = AlertConfig {
            enabled: true,
            max_latency_ms: 1.0, // 1ms threshold
            ..Default::default()
        };
        collector.update_alert_config(alert_config).await;

        let event = crate::events::Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 1,
                status: Some("pending".to_string()),
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );

        let handle = collector.record_event_start(&event).await;

        // Simulate slow processing
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        handle.success("test_handler").await;

        // Should receive a high latency alert
        let alert = if let Some(ref mut receiver) = receiver {
            receiver.try_recv()
        } else {
            panic!("No alert receiver available")
        };
        assert!(alert.is_ok());

        if let Ok(alert) = alert {
            assert!(matches!(alert.alert_type, AlertType::HighLatency));
        }
    }
}