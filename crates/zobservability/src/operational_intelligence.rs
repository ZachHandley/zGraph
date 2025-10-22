//! Operational intelligence features for ZRUSTDB
//!
//! This module provides log aggregation, anomaly detection, and
//! predictive monitoring capabilities for production operations.

use anyhow::Result;
use chrono::{DateTime, Utc, Duration};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration as StdDuration,
};
use tokio::sync::RwLock;

/// Log entry for aggregation and analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: LogLevel,
    pub service: String,
    pub component: String,
    pub message: String,
    pub metadata: HashMap<String, serde_json::Value>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
}

/// Log severity levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
    Critical,
}

/// Aggregated log statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogStatistics {
    pub time_window: TimeWindow,
    pub total_logs: u64,
    pub level_distribution: HashMap<LogLevel, u64>,
    pub service_distribution: HashMap<String, u64>,
    pub component_distribution: HashMap<String, u64>,
    pub error_rate: f64,
    pub top_error_messages: Vec<(String, u64)>,
    pub patterns: Vec<LogPattern>,
}

/// Time window for aggregation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeWindow {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub duration_minutes: i64,
}

/// Detected log pattern
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogPattern {
    pub pattern: String,
    pub count: u64,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub services: Vec<String>,
    pub severity: LogLevel,
}

/// Anomaly detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Anomaly {
    pub id: String,
    pub anomaly_type: AnomalyType,
    pub detected_at: DateTime<Utc>,
    pub severity: AnomalySeverity,
    pub description: String,
    pub metrics: HashMap<String, f64>,
    pub affected_services: Vec<String>,
    pub recommendations: Vec<String>,
    pub confidence: f64,
}

/// Types of anomalies that can be detected
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AnomalyType {
    PerformanceDegradation,
    ErrorRateSpike,
    ResourceExhaustion,
    UnusualTrafficPattern,
    ServiceCommunicationFailure,
    DataInconsistency,
}

/// Anomaly severity levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum AnomalySeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// Metric data point for analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricDataPoint {
    pub timestamp: DateTime<Utc>,
    pub metric_name: String,
    pub value: f64,
    pub labels: HashMap<String, String>,
}

/// Statistical summary of metric values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSummary {
    pub metric_name: String,
    pub window: TimeWindow,
    pub count: u64,
    pub mean: f64,
    pub median: f64,
    pub std_dev: f64,
    pub min: f64,
    pub max: f64,
    pub percentile_95: f64,
    pub percentile_99: f64,
}

/// Log aggregation service
pub struct LogAggregationService {
    log_buffer: Arc<RwLock<VecDeque<LogEntry>>>,
    max_buffer_size: usize,
    aggregation_windows: Vec<StdDuration>,
}

impl LogAggregationService {
    pub fn new(max_buffer_size: usize) -> Self {
        Self {
            log_buffer: Arc::new(RwLock::new(VecDeque::new())),
            max_buffer_size,
            aggregation_windows: vec![
                StdDuration::from_secs(60),      // 1 minute
                StdDuration::from_secs(300),     // 5 minutes
                StdDuration::from_secs(1800),    // 30 minutes
                StdDuration::from_secs(3600),    // 1 hour
            ],
        }
    }

    /// Ingest a log entry
    pub async fn ingest_log(&self, log: LogEntry) -> Result<()> {
        let mut buffer = self.log_buffer.write().await;

        // Add to buffer
        buffer.push_back(log);

        // Maintain buffer size
        while buffer.len() > self.max_buffer_size {
            buffer.pop_front();
        }

        Ok(())
    }

    /// Aggregate logs for a specific time window
    pub async fn aggregate_logs(&self, window_duration: StdDuration) -> Result<LogStatistics> {
        let buffer = self.log_buffer.read().await;
        let now = Utc::now();
        let window_start = now - Duration::from_std(window_duration)?;

        let relevant_logs: Vec<_> = buffer
            .iter()
            .filter(|log| log.timestamp >= window_start)
            .collect();

        let mut level_distribution = HashMap::new();
        let mut service_distribution = HashMap::new();
        let mut component_distribution = HashMap::new();
        let mut error_messages = HashMap::new();
        let mut patterns = HashMap::new();

        let total_logs = relevant_logs.len() as u64;
        let mut error_count = 0;

        for log in &relevant_logs {
            // Level distribution
            *level_distribution.entry(log.level.clone()).or_insert(0) += 1;

            // Service distribution
            *service_distribution.entry(log.service.clone()).or_insert(0) += 1;

            // Component distribution
            *component_distribution.entry(log.component.clone()).or_insert(0) += 1;

            // Error tracking
            if matches!(log.level, LogLevel::Error | LogLevel::Critical) {
                error_count += 1;
                *error_messages.entry(log.message.clone()).or_insert(0) += 1;
            }

            // Pattern detection (simplified)
            let pattern = self.extract_pattern(&log.message);
            let pattern_entry = patterns.entry(pattern.clone()).or_insert(LogPattern {
                pattern,
                count: 0,
                first_seen: log.timestamp,
                last_seen: log.timestamp,
                services: Vec::new(),
                severity: log.level.clone(),
            });

            pattern_entry.count += 1;
            pattern_entry.last_seen = log.timestamp.max(pattern_entry.last_seen);
            if !pattern_entry.services.contains(&log.service) {
                pattern_entry.services.push(log.service.clone());
            }
        }

        let error_rate = if total_logs > 0 {
            error_count as f64 / total_logs as f64
        } else {
            0.0
        };

        let mut top_error_messages: Vec<_> = error_messages.into_iter().collect();
        top_error_messages.sort_by(|a, b| b.1.cmp(&a.1));
        top_error_messages.truncate(10);

        Ok(LogStatistics {
            time_window: TimeWindow {
                start: window_start,
                end: now,
                duration_minutes: window_duration.as_secs() as i64 / 60,
            },
            total_logs,
            level_distribution,
            service_distribution,
            component_distribution,
            error_rate,
            top_error_messages,
            patterns: patterns.into_values().collect(),
        })
    }

    /// Extract pattern from log message (simplified pattern recognition)
    fn extract_pattern(&self, message: &str) -> String {
        // Simple pattern extraction - replace numbers and specific values with placeholders
        let pattern = message
            .split_whitespace()
            .map(|word| {
                if word.chars().all(|c| c.is_numeric() || c == '.') {
                    "<NUMBER>"
                } else if word.starts_with('/') || word.contains("://") {
                    "<PATH>"
                } else if word.len() > 32 && word.chars().all(|c| c.is_alphanumeric()) {
                    "<ID>"
                } else {
                    word
                }
            })
            .collect::<Vec<_>>()
            .join(" ");

        pattern
    }
}

/// Anomaly detection engine
pub struct AnomalyDetectionEngine {
    metric_history: Arc<RwLock<HashMap<String, VecDeque<MetricDataPoint>>>>,
    max_history_size: usize,
    detection_rules: Vec<DetectionRule>,
}

/// Anomaly detection rule
#[derive(Debug, Clone)]
pub struct DetectionRule {
    pub name: String,
    pub metric_pattern: String,
    pub detector: AnomalyDetector,
    pub severity: AnomalySeverity,
    pub confidence_threshold: f64,
}

/// Anomaly detection algorithms
#[derive(Debug, Clone)]
pub enum AnomalyDetector {
    StatisticalOutlier { std_dev_threshold: f64 },
    PercentileThreshold { percentile: f64, threshold: f64 },
    RateOfChange { change_threshold: f64, time_window_seconds: u64 },
    SeasonalDecomposition { period_hours: u64, deviation_threshold: f64 },
}

impl AnomalyDetectionEngine {
    pub fn new(max_history_size: usize) -> Self {
        Self {
            metric_history: Arc::new(RwLock::new(HashMap::new())),
            max_history_size,
            detection_rules: Self::default_detection_rules(),
        }
    }

    /// Add metric data for analysis
    pub async fn add_metric_data(&self, data_point: MetricDataPoint) -> Result<()> {
        let mut history = self.metric_history.write().await;
        let metric_history = history
            .entry(data_point.metric_name.clone())
            .or_insert_with(VecDeque::new);

        metric_history.push_back(data_point);

        // Maintain history size
        while metric_history.len() > self.max_history_size {
            metric_history.pop_front();
        }

        Ok(())
    }

    /// Detect anomalies in current metrics
    pub async fn detect_anomalies(&self) -> Result<Vec<Anomaly>> {
        let mut anomalies = Vec::new();
        let history = self.metric_history.read().await;

        for rule in &self.detection_rules {
            for (metric_name, data_points) in history.iter() {
                if self.metric_matches_pattern(metric_name, &rule.metric_pattern) {
                    if let Some(anomaly) = self.apply_detection_rule(rule, metric_name, data_points).await? {
                        if anomaly.confidence >= rule.confidence_threshold {
                            anomalies.push(anomaly);
                        }
                    }
                }
            }
        }

        Ok(anomalies)
    }

    /// Apply a detection rule to metric data
    async fn apply_detection_rule(
        &self,
        rule: &DetectionRule,
        metric_name: &str,
        data_points: &VecDeque<MetricDataPoint>,
    ) -> Result<Option<Anomaly>> {
        if data_points.len() < 10 {
            return Ok(None); // Not enough data
        }

        let values: Vec<f64> = data_points.iter().map(|dp| dp.value).collect();
        let latest_value = values[values.len() - 1];

        let (is_anomaly, confidence, description) = match &rule.detector {
            AnomalyDetector::StatisticalOutlier { std_dev_threshold } => {
                self.detect_statistical_outlier(&values, latest_value, *std_dev_threshold)
            }
            AnomalyDetector::PercentileThreshold { percentile, threshold } => {
                self.detect_percentile_threshold(&values, latest_value, *percentile, *threshold)
            }
            AnomalyDetector::RateOfChange { change_threshold, time_window_seconds } => {
                self.detect_rate_of_change(data_points, *change_threshold, *time_window_seconds)
            }
            AnomalyDetector::SeasonalDecomposition { period_hours, deviation_threshold } => {
                self.detect_seasonal_anomaly(data_points, *period_hours, *deviation_threshold)
            }
        };

        if is_anomaly {
            let anomaly = Anomaly {
                id: format!("{}_{}", rule.name, Utc::now().timestamp()),
                anomaly_type: self.classify_anomaly_type(metric_name),
                detected_at: Utc::now(),
                severity: rule.severity.clone(),
                description,
                metrics: HashMap::from([(metric_name.to_string(), latest_value)]),
                affected_services: vec!["zrustdb".to_string()], // Would be derived from labels
                recommendations: self.generate_recommendations(metric_name, &rule.detector),
                confidence,
            };
            Ok(Some(anomaly))
        } else {
            Ok(None)
        }
    }

    /// Statistical outlier detection using standard deviation
    fn detect_statistical_outlier(&self, values: &[f64], latest_value: f64, threshold: f64) -> (bool, f64, String) {
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
        let std_dev = variance.sqrt();

        let z_score = (latest_value - mean).abs() / std_dev;
        let is_anomaly = z_score > threshold;
        let confidence = (z_score / threshold).min(1.0);

        let description = format!(
            "Statistical outlier detected: value {:.2} deviates {:.2} standard deviations from mean {:.2}",
            latest_value, z_score, mean
        );

        (is_anomaly, confidence, description)
    }

    /// Percentile threshold detection
    fn detect_percentile_threshold(&self, values: &[f64], latest_value: f64, percentile: f64, threshold: f64) -> (bool, f64, String) {
        let mut sorted_values = values.to_vec();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let percentile_index = ((percentile / 100.0) * (sorted_values.len() - 1) as f64) as usize;
        let percentile_value = sorted_values[percentile_index];

        let is_anomaly = latest_value > percentile_value * threshold;
        let confidence = if is_anomaly {
            ((latest_value / (percentile_value * threshold)) - 1.0).min(1.0)
        } else {
            0.0
        };

        let description = format!(
            "Percentile threshold exceeded: value {:.2} exceeds {}th percentile {:.2} by threshold {:.2}",
            latest_value, percentile, percentile_value, threshold
        );

        (is_anomaly, confidence, description)
    }

    /// Rate of change detection
    fn detect_rate_of_change(&self, data_points: &VecDeque<MetricDataPoint>, threshold: f64, window_seconds: u64) -> (bool, f64, String) {
        let now = Utc::now();
        let window_start = now - Duration::seconds(window_seconds as i64);

        let recent_points: Vec<_> = data_points
            .iter()
            .filter(|dp| dp.timestamp >= window_start)
            .collect();

        if recent_points.len() < 2 {
            return (false, 0.0, "Insufficient data for rate of change analysis".to_string());
        }

        let first_value = recent_points[0].value;
        let last_value = recent_points[recent_points.len() - 1].value;
        let rate_of_change = (last_value - first_value) / first_value;

        let is_anomaly = rate_of_change.abs() > threshold;
        let confidence = (rate_of_change.abs() / threshold).min(1.0);

        let description = format!(
            "High rate of change detected: {:.2}% change over {} seconds (threshold: {:.2}%)",
            rate_of_change * 100.0, window_seconds, threshold * 100.0
        );

        (is_anomaly, confidence, description)
    }

    /// Seasonal anomaly detection (simplified)
    fn detect_seasonal_anomaly(&self, data_points: &VecDeque<MetricDataPoint>, period_hours: u64, threshold: f64) -> (bool, f64, String) {
        // Simplified seasonal detection - would need more sophisticated analysis in production
        let period_duration = Duration::hours(period_hours as i64);
        let now = Utc::now();

        // Get values from the same time in previous periods
        let seasonal_values: Vec<f64> = data_points
            .iter()
            .filter(|dp| {
                let time_diff = now - dp.timestamp;
                let cycles = time_diff.num_hours() / period_hours as i64;
                let remainder_hours = time_diff.num_hours() % period_hours as i64;
                cycles > 0 && remainder_hours < 1
            })
            .map(|dp| dp.value)
            .collect();

        if seasonal_values.is_empty() {
            return (false, 0.0, "No seasonal data available".to_string());
        }

        let seasonal_mean = seasonal_values.iter().sum::<f64>() / seasonal_values.len() as f64;
        let latest_value = data_points.back().unwrap().value;
        let deviation = (latest_value - seasonal_mean).abs() / seasonal_mean;

        let is_anomaly = deviation > threshold;
        let confidence = (deviation / threshold).min(1.0);

        let description = format!(
            "Seasonal anomaly detected: current value {:.2} deviates {:.2}% from seasonal average {:.2}",
            latest_value, deviation * 100.0, seasonal_mean
        );

        (is_anomaly, confidence, description)
    }

    /// Check if metric name matches pattern
    fn metric_matches_pattern(&self, metric_name: &str, pattern: &str) -> bool {
        // Simple pattern matching - could be enhanced with regex
        pattern == "*" || metric_name.contains(pattern)
    }

    /// Classify anomaly type based on metric name
    fn classify_anomaly_type(&self, metric_name: &str) -> AnomalyType {
        if metric_name.contains("latency") || metric_name.contains("duration") {
            AnomalyType::PerformanceDegradation
        } else if metric_name.contains("error") || metric_name.contains("failed") {
            AnomalyType::ErrorRateSpike
        } else if metric_name.contains("cpu") || metric_name.contains("memory") {
            AnomalyType::ResourceExhaustion
        } else if metric_name.contains("request") || metric_name.contains("traffic") {
            AnomalyType::UnusualTrafficPattern
        } else {
            AnomalyType::DataInconsistency
        }
    }

    /// Generate recommendations based on anomaly type
    fn generate_recommendations(&self, metric_name: &str, detector: &AnomalyDetector) -> Vec<String> {
        let mut recommendations = Vec::new();

        match detector {
            AnomalyDetector::StatisticalOutlier { .. } => {
                recommendations.push("Investigate recent changes or deployments".to_string());
                recommendations.push("Check for unusual traffic patterns".to_string());
            }
            AnomalyDetector::RateOfChange { .. } => {
                recommendations.push("Monitor for cascading effects".to_string());
                recommendations.push("Consider scaling resources if needed".to_string());
            }
            _ => {
                recommendations.push("Review system logs for additional context".to_string());
            }
        }

        if metric_name.contains("cpu") {
            recommendations.push("Consider horizontal scaling".to_string());
        } else if metric_name.contains("memory") {
            recommendations.push("Check for memory leaks".to_string());
        } else if metric_name.contains("latency") {
            recommendations.push("Analyze request tracing data".to_string());
        }

        recommendations
    }

    /// Default detection rules for ZRUSTDB
    fn default_detection_rules() -> Vec<DetectionRule> {
        vec![
            DetectionRule {
                name: "high_cpu_usage".to_string(),
                metric_pattern: "cpu_usage".to_string(),
                detector: AnomalyDetector::StatisticalOutlier { std_dev_threshold: 2.0 },
                severity: AnomalySeverity::Medium,
                confidence_threshold: 0.7,
            },
            DetectionRule {
                name: "memory_leak".to_string(),
                metric_pattern: "memory_usage".to_string(),
                detector: AnomalyDetector::RateOfChange { change_threshold: 0.1, time_window_seconds: 300 },
                severity: AnomalySeverity::High,
                confidence_threshold: 0.8,
            },
            DetectionRule {
                name: "response_time_degradation".to_string(),
                metric_pattern: "duration".to_string(),
                detector: AnomalyDetector::PercentileThreshold { percentile: 95.0, threshold: 1.5 },
                severity: AnomalySeverity::Medium,
                confidence_threshold: 0.6,
            },
            DetectionRule {
                name: "error_rate_spike".to_string(),
                metric_pattern: "error".to_string(),
                detector: AnomalyDetector::StatisticalOutlier { std_dev_threshold: 3.0 },
                severity: AnomalySeverity::Critical,
                confidence_threshold: 0.9,
            },
        ]
    }
}

/// Operational intelligence manager
pub struct OperationalIntelligenceManager {
    log_aggregation: LogAggregationService,
    anomaly_detection: AnomalyDetectionEngine,
}

impl OperationalIntelligenceManager {
    pub fn new() -> Self {
        Self {
            log_aggregation: LogAggregationService::new(10000),
            anomaly_detection: AnomalyDetectionEngine::new(1000),
        }
    }

    /// Process log entry
    pub async fn process_log(&self, log: LogEntry) -> Result<()> {
        self.log_aggregation.ingest_log(log).await
    }

    /// Process metric data
    pub async fn process_metric(&self, metric: MetricDataPoint) -> Result<()> {
        self.anomaly_detection.add_metric_data(metric).await
    }

    /// Get operational insights
    pub async fn get_insights(&self, window_minutes: u64) -> Result<OperationalInsights> {
        let log_stats = self.log_aggregation
            .aggregate_logs(StdDuration::from_secs(window_minutes * 60))
            .await?;

        let anomalies = self.anomaly_detection.detect_anomalies().await?;

        Ok(OperationalInsights {
            timestamp: Utc::now(),
            window_minutes,
            log_statistics: log_stats,
            anomalies: anomalies.clone(),
            health_score: self.calculate_health_score(&anomalies),
            recommendations: self.generate_operational_recommendations(&anomalies),
        })
    }

    /// Calculate overall system health score
    fn calculate_health_score(&self, anomalies: &[Anomaly]) -> f64 {
        if anomalies.is_empty() {
            return 100.0;
        }

        let total_impact: f64 = anomalies
            .iter()
            .map(|a| match a.severity {
                AnomalySeverity::Low => 5.0,
                AnomalySeverity::Medium => 15.0,
                AnomalySeverity::High => 30.0,
                AnomalySeverity::Critical => 50.0,
            })
            .sum();

        (100.0 - total_impact).max(0.0)
    }

    /// Generate operational recommendations
    fn generate_operational_recommendations(&self, anomalies: &[Anomaly]) -> Vec<String> {
        let mut recommendations = Vec::new();

        if anomalies.iter().any(|a| matches!(a.severity, AnomalySeverity::Critical)) {
            recommendations.push("Critical anomalies detected - immediate attention required".to_string());
        }

        let performance_issues = anomalies.iter()
            .filter(|a| matches!(a.anomaly_type, AnomalyType::PerformanceDegradation))
            .count();

        if performance_issues > 2 {
            recommendations.push("Multiple performance issues detected - consider system-wide optimization".to_string());
        }

        if anomalies.iter().any(|a| matches!(a.anomaly_type, AnomalyType::ResourceExhaustion)) {
            recommendations.push("Resource exhaustion detected - consider scaling up infrastructure".to_string());
        }

        recommendations
    }
}

/// Complete operational insights report
#[derive(Debug, Serialize, Deserialize)]
pub struct OperationalInsights {
    pub timestamp: DateTime<Utc>,
    pub window_minutes: u64,
    pub log_statistics: LogStatistics,
    pub anomalies: Vec<Anomaly>,
    pub health_score: f64,
    pub recommendations: Vec<String>,
}