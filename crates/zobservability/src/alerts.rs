//! Alert rule definitions and alerting system for ZRUSTDB

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, RwLock};
use chrono::{DateTime, Utc};
use crate::health::{HealthStatus, HealthCheckResult};

/// Alert severity levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AlertSeverity {
    Critical,
    Warning,
    Info,
}

/// Alert rule definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    pub name: String,
    pub description: String,
    pub severity: AlertSeverity,
    pub query: String,
    pub threshold: f64,
    pub comparison: ComparisonOperator,
    pub duration: String, // e.g., "5m", "10s"
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
}

/// Comparison operators for alert thresholds
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComparisonOperator {
    GreaterThan,
    LessThan,
    Equal,
    NotEqual,
    GreaterThanOrEqual,
    LessThanOrEqual,
}

/// Active alert state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveAlert {
    pub rule_name: String,
    pub severity: AlertSeverity,
    pub message: String,
    pub triggered_at: DateTime<Utc>,
    pub last_evaluated: DateTime<Utc>,
    pub evaluation_count: u64,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub resolved: bool,
    pub resolved_at: Option<DateTime<Utc>>,
}

/// Alert event for notification system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEvent {
    pub alert: ActiveAlert,
    pub event_type: AlertEventType,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertEventType {
    Triggered,
    Resolved,
    Escalated,
}

/// Alert manager for ZRUSTDB with real-time notifications
pub struct AlertManager {
    rules: Vec<AlertRule>,
    active_alerts: Arc<RwLock<HashMap<String, ActiveAlert>>>,
    alert_sender: mpsc::UnboundedSender<AlertEvent>,
    last_evaluation: Arc<RwLock<HashMap<String, Instant>>>,
}

/// Alert evaluation result
#[derive(Debug)]
pub struct AlertEvaluationResult {
    pub rule_name: String,
    pub triggered: bool,
    pub value: f64,
    pub threshold: f64,
    pub message: String,
}

impl AlertManager {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<AlertEvent>) {
        let (alert_sender, alert_receiver) = mpsc::unbounded_channel();

        let manager = Self {
            rules: Self::default_alert_rules(),
            active_alerts: Arc::new(RwLock::new(HashMap::new())),
            alert_sender,
            last_evaluation: Arc::new(RwLock::new(HashMap::new())),
        };

        (manager, alert_receiver)
    }

    /// Add a new alert rule
    pub async fn add_rule(&mut self, rule: AlertRule) {
        self.rules.push(rule);
    }

    /// Remove an alert rule
    pub async fn remove_rule(&mut self, rule_name: &str) {
        self.rules.retain(|r| r.name != rule_name);
    }

    /// Evaluate all alert rules against current metrics
    pub async fn evaluate_alerts(&self, metrics_values: &HashMap<String, f64>) -> Result<Vec<AlertEvaluationResult>> {
        let mut results = Vec::new();
        let now = Utc::now();

        for rule in &self.rules {
            if let Some(current_value) = metrics_values.get(&rule.query) {
                let triggered = self.evaluate_threshold(*current_value, rule.threshold, &rule.comparison);

                let message = if triggered {
                    format!("{}: {} {} {:.2} (current: {:.2})",
                        rule.name,
                        rule.query,
                        self.comparison_to_string(&rule.comparison),
                        rule.threshold,
                        current_value)
                } else {
                    format!("{}: Normal - {:.2}", rule.name, current_value)
                };

                // Handle alert state changes
                if triggered {
                    self.handle_alert_triggered(rule, *current_value, &message, now).await?;
                } else {
                    self.handle_alert_resolved(&rule.name, now).await?;
                }

                results.push(AlertEvaluationResult {
                    rule_name: rule.name.clone(),
                    triggered,
                    value: *current_value,
                    threshold: rule.threshold,
                    message,
                });
            }
        }

        Ok(results)
    }

    /// Evaluate health check results for alerting
    pub async fn evaluate_health_alerts(&self, health_results: &[HealthCheckResult]) -> Result<Vec<AlertEvaluationResult>> {
        let mut results = Vec::new();
        let now = Utc::now();

        for health_result in health_results {
            let rule_name = format!("health_{}", health_result.name);
            let triggered = matches!(health_result.status, HealthStatus::Critical | HealthStatus::Warning);

            if triggered {
                let severity = match health_result.status {
                    HealthStatus::Critical => AlertSeverity::Critical,
                    HealthStatus::Warning => AlertSeverity::Warning,
                    _ => AlertSeverity::Info,
                };

                let alert_rule = AlertRule {
                    name: rule_name.clone(),
                    description: format!("Health check alert for {}", health_result.name),
                    severity,
                    query: format!("health_check_{}", health_result.name),
                    threshold: 1.0,
                    comparison: ComparisonOperator::GreaterThanOrEqual,
                    duration: "0s".to_string(),
                    labels: HashMap::from([
                        ("check_name".to_string(), health_result.name.clone()),
                        ("status".to_string(), format!("{:?}", health_result.status)),
                    ]),
                    annotations: HashMap::from([
                        ("message".to_string(), health_result.message.clone()),
                        ("duration_ms".to_string(), health_result.duration_ms.to_string()),
                    ]),
                };

                self.handle_alert_triggered(&alert_rule, 1.0, &health_result.message, now).await?;
            } else {
                self.handle_alert_resolved(&rule_name, now).await?;
            }

            results.push(AlertEvaluationResult {
                rule_name,
                triggered,
                value: if triggered { 1.0 } else { 0.0 },
                threshold: 1.0,
                message: health_result.message.clone(),
            });
        }

        Ok(results)
    }

    /// Handle alert being triggered
    async fn handle_alert_triggered(
        &self,
        rule: &AlertRule,
        current_value: f64,
        message: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        let mut active_alerts = self.active_alerts.write().await;

        match active_alerts.get_mut(&rule.name) {
            Some(existing_alert) => {
                // Update existing alert
                existing_alert.last_evaluated = timestamp;
                existing_alert.evaluation_count += 1;
                existing_alert.message = message.to_string();
            }
            None => {
                // Create new alert
                let alert = ActiveAlert {
                    rule_name: rule.name.clone(),
                    severity: rule.severity.clone(),
                    message: message.to_string(),
                    triggered_at: timestamp,
                    last_evaluated: timestamp,
                    evaluation_count: 1,
                    labels: rule.labels.clone(),
                    annotations: rule.annotations.clone(),
                    resolved: false,
                    resolved_at: None,
                };

                // Send alert notification
                let alert_event = AlertEvent {
                    alert: alert.clone(),
                    event_type: AlertEventType::Triggered,
                    timestamp,
                };

                if let Err(e) = self.alert_sender.send(alert_event) {
                    tracing::error!("Failed to send alert notification: {}", e);
                }

                active_alerts.insert(rule.name.clone(), alert);
                tracing::warn!("Alert triggered: {} - {}", rule.name, message);
            }
        }

        Ok(())
    }

    /// Handle alert being resolved
    async fn handle_alert_resolved(&self, rule_name: &str, timestamp: DateTime<Utc>) -> Result<()> {
        let mut active_alerts = self.active_alerts.write().await;

        if let Some(mut alert) = active_alerts.remove(rule_name) {
            alert.resolved = true;
            alert.resolved_at = Some(timestamp);

            // Send resolution notification
            let alert_event = AlertEvent {
                alert,
                event_type: AlertEventType::Resolved,
                timestamp,
            };

            if let Err(e) = self.alert_sender.send(alert_event) {
                tracing::error!("Failed to send alert resolution notification: {}", e);
            }

            tracing::info!("Alert resolved: {}", rule_name);
        }

        Ok(())
    }

    /// Get all active alerts
    pub async fn get_active_alerts(&self) -> Vec<ActiveAlert> {
        let active_alerts = self.active_alerts.read().await;
        active_alerts.values().cloned().collect()
    }

    /// Get alert history (for dashboard/reporting)
    pub async fn get_alert_history(&self, hours: u32) -> Vec<ActiveAlert> {
        // In production, this would query from persistent storage
        // For now, just return current active alerts
        self.get_active_alerts().await
    }

    /// Evaluate threshold condition
    fn evaluate_threshold(&self, value: f64, threshold: f64, comparison: &ComparisonOperator) -> bool {
        match comparison {
            ComparisonOperator::GreaterThan => value > threshold,
            ComparisonOperator::LessThan => value < threshold,
            ComparisonOperator::Equal => (value - threshold).abs() < f64::EPSILON,
            ComparisonOperator::NotEqual => (value - threshold).abs() >= f64::EPSILON,
            ComparisonOperator::GreaterThanOrEqual => value >= threshold,
            ComparisonOperator::LessThanOrEqual => value <= threshold,
        }
    }

    /// Convert comparison operator to string
    fn comparison_to_string(&self, comparison: &ComparisonOperator) -> &'static str {
        match comparison {
            ComparisonOperator::GreaterThan => ">",
            ComparisonOperator::LessThan => "<",
            ComparisonOperator::Equal => "==",
            ComparisonOperator::NotEqual => "!=",
            ComparisonOperator::GreaterThanOrEqual => ">=",
            ComparisonOperator::LessThanOrEqual => "<=",
        }
    }

    /// Start periodic alert evaluation
    pub async fn start_periodic_evaluation(
        self: Arc<Self>,
        metrics_provider: Arc<dyn MetricsProvider>,
        health_provider: Arc<dyn HealthProvider>,
        interval: Duration,
    ) -> Result<()> {
        let mut ticker = tokio::time::interval(interval);

        loop {
            ticker.tick().await;

            // Evaluate metrics-based alerts
            if let Ok(metrics) = metrics_provider.get_current_metrics().await {
                if let Err(e) = self.evaluate_alerts(&metrics).await {
                    tracing::error!("Failed to evaluate metrics alerts: {}", e);
                }
            }

            // Evaluate health-based alerts
            if let Ok(health_results) = health_provider.get_current_health().await {
                if let Err(e) = self.evaluate_health_alerts(&health_results).await {
                    tracing::error!("Failed to evaluate health alerts: {}", e);
                }
            }
        }
    }

    /// Default alert rules for ZRUSTDB production monitoring
    fn default_alert_rules() -> Vec<AlertRule> {
        vec![
            // CPU usage alert
            AlertRule {
                name: "high_cpu_usage".to_string(),
                description: "High CPU usage detected".to_string(),
                severity: AlertSeverity::Warning,
                query: "zdb_cpu_usage_percent".to_string(),
                threshold: 80.0,
                comparison: ComparisonOperator::GreaterThan,
                duration: "5m".to_string(),
                labels: HashMap::from([("service".to_string(), "zrustdb".to_string())]),
                annotations: HashMap::from([("summary".to_string(), "CPU usage is above 80%".to_string())]),
            },
            // Memory usage alert
            AlertRule {
                name: "high_memory_usage".to_string(),
                description: "High memory usage detected".to_string(),
                severity: AlertSeverity::Warning,
                query: "zdb_memory_usage_bytes".to_string(),
                threshold: 8_000_000_000.0, // 8GB
                comparison: ComparisonOperator::GreaterThan,
                duration: "3m".to_string(),
                labels: HashMap::from([("service".to_string(), "zrustdb".to_string())]),
                annotations: HashMap::from([("summary".to_string(), "Memory usage is above 8GB".to_string())]),
            },
            // Vector operation latency alert
            AlertRule {
                name: "high_vector_latency".to_string(),
                description: "High vector operation latency".to_string(),
                severity: AlertSeverity::Warning,
                query: "zdb_vector_operation_duration_seconds".to_string(),
                threshold: 0.1, // 100ms
                comparison: ComparisonOperator::GreaterThan,
                duration: "2m".to_string(),
                labels: HashMap::from([("operation".to_string(), "vector".to_string())]),
                annotations: HashMap::from([("summary".to_string(), "Vector operations taking longer than 100ms".to_string())]),
            },
            // Authentication failure rate alert
            AlertRule {
                name: "high_auth_failure_rate".to_string(),
                description: "High authentication failure rate".to_string(),
                severity: AlertSeverity::Critical,
                query: "zdb_auth_requests_total".to_string(),
                threshold: 0.2, // 20% failure rate
                comparison: ComparisonOperator::GreaterThan,
                duration: "1m".to_string(),
                labels: HashMap::from([("service".to_string(), "auth".to_string())]),
                annotations: HashMap::from([("summary".to_string(), "More than 20% of auth requests are failing".to_string())]),
            },
            // Job queue backlog alert
            AlertRule {
                name: "job_queue_backlog".to_string(),
                description: "Job queue has high backlog".to_string(),
                severity: AlertSeverity::Warning,
                query: "zdb_job_queue_size".to_string(),
                threshold: 100.0,
                comparison: ComparisonOperator::GreaterThan,
                duration: "5m".to_string(),
                labels: HashMap::from([("service".to_string(), "job_queue".to_string())]),
                annotations: HashMap::from([("summary".to_string(), "Job queue has more than 100 pending jobs".to_string())]),
            },
        ]
    }

    /// Export alert rules in Prometheus format
    pub fn export_prometheus_rules(&self) -> Result<String> {
        let mut output = String::from("groups:\n");
        output.push_str("  - name: zrustdb_alerts\n");
        output.push_str("    rules:\n");

        for rule in &self.rules {
            output.push_str(&format!("      - alert: {}\n", rule.name));
            output.push_str(&format!("        expr: {}\n", rule.query));
            output.push_str(&format!("        for: {}\n", rule.duration));

            if !rule.labels.is_empty() {
                output.push_str("        labels:\n");
                for (key, value) in &rule.labels {
                    output.push_str(&format!("          {}: {}\n", key, value));
                }
            }

            if !rule.annotations.is_empty() {
                output.push_str("        annotations:\n");
                for (key, value) in &rule.annotations {
                    output.push_str(&format!("          {}: {}\n", key, value));
                }
            }

            output.push('\n');
        }

        Ok(output)
    }

    /// Export alert rules in Grafana format
    pub fn export_grafana_rules(&self) -> Result<serde_json::Value> {
        let mut rules = Vec::new();

        for rule in &self.rules {
            let grafana_rule = serde_json::json!({
                "uid": format!("zdb_{}", rule.name.to_lowercase()),
                "title": rule.name,
                "condition": "A",
                "data": [{
                    "refId": "A",
                    "queryType": "",
                    "model": {
                        "expr": rule.query,
                        "interval": "",
                        "refId": "A"
                    }
                }],
                "dashboardUid": "zrustdb-main",
                "panelId": 1,
                "noDataState": "NoData",
                "execErrState": "Alerting",
                "for": rule.duration,
                "annotations": rule.annotations,
                "labels": rule.labels
            });

            rules.push(grafana_rule);
        }

        Ok(serde_json::json!({
            "groups": [{
                "name": "zrustdb_alerts",
                "orgId": 1,
                "folder": "zrustdb",
                "rules": rules
            }]
        }))
    }

    /// Get rules by severity
    pub fn get_rules_by_severity(&self, severity: AlertSeverity) -> Vec<&AlertRule> {
        self.rules
            .iter()
            .filter(|rule| rule.severity == severity)
            .collect()
    }
}

/// Provider trait for metrics data
#[async_trait::async_trait]
pub trait MetricsProvider: Send + Sync {
    async fn get_current_metrics(&self) -> Result<HashMap<String, f64>>;
}

/// Provider trait for health check data
#[async_trait::async_trait]
pub trait HealthProvider: Send + Sync {
    async fn get_current_health(&self) -> Result<Vec<HealthCheckResult>>;
}

/// Notification handler for alerts
#[async_trait::async_trait]
pub trait AlertNotificationHandler: Send + Sync {
    async fn handle_alert(&self, alert_event: &AlertEvent) -> Result<()>;
}

/// Real-time alert notification service
pub struct AlertNotificationService {
    handlers: Vec<Box<dyn AlertNotificationHandler>>,
}

impl AlertNotificationService {
    pub fn new() -> Self {
        Self {
            handlers: Vec::new(),
        }
    }

    /// Add a notification handler
    pub fn add_handler(&mut self, handler: Box<dyn AlertNotificationHandler>) {
        self.handlers.push(handler);
    }

    /// Start processing alert events
    pub async fn start_processing(
        &self,
        mut alert_receiver: mpsc::UnboundedReceiver<AlertEvent>,
    ) -> Result<()> {
        while let Some(alert_event) = alert_receiver.recv().await {
            for handler in &self.handlers {
                if let Err(e) = handler.handle_alert(&alert_event).await {
                    tracing::error!("Alert notification handler failed: {}", e);
                }
            }
        }
        Ok(())
    }
}

/// Example: Email notification handler (could integrate with znotifications)
pub struct EmailNotificationHandler {
    pub recipient: String,
}

#[async_trait::async_trait]
impl AlertNotificationHandler for EmailNotificationHandler {
    async fn handle_alert(&self, alert_event: &AlertEvent) -> Result<()> {
        let subject = match alert_event.event_type {
            AlertEventType::Triggered => format!("🚨 ALERT: {}", alert_event.alert.rule_name),
            AlertEventType::Resolved => format!("✅ RESOLVED: {}", alert_event.alert.rule_name),
            AlertEventType::Escalated => format!("🆘 ESCALATED: {}", alert_event.alert.rule_name),
        };

        let body = format!(
            "Alert: {}\nSeverity: {:?}\nMessage: {}\nTimestamp: {}\nLabels: {:?}",
            alert_event.alert.rule_name,
            alert_event.alert.severity,
            alert_event.alert.message,
            alert_event.timestamp,
            alert_event.alert.labels
        );

        tracing::info!("Email notification to {}: {} - {}", self.recipient, subject, body);

        // In production, this would integrate with znotifications or email service
        // For now, just log the notification
        Ok(())
    }
}

/// Webhook notification handler
pub struct WebhookNotificationHandler {
    pub webhook_url: String,
    pub client: reqwest::Client,
}

#[async_trait::async_trait]
impl AlertNotificationHandler for WebhookNotificationHandler {
    async fn handle_alert(&self, alert_event: &AlertEvent) -> Result<()> {
        let payload = serde_json::to_value(alert_event)?;

        let response = self.client
            .post(&self.webhook_url)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Webhook request failed with status: {}", response.status()));
        }

        tracing::info!("Webhook notification sent for alert: {}", alert_event.alert.rule_name);
        Ok(())
    }
}

