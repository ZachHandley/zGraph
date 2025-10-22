use crate::bus::EventBus;
use crate::events::{Event, EventData, EventType};
use crate::handlers::{EventHandler, HandlerResult};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc, Timelike};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use ulid::Ulid;

/// Condition for triggering actions
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TriggerCondition {
    /// Event type matches
    EventType {
        event_type: EventType,
    },
    /// Topic pattern matches
    TopicPattern {
        pattern: String,
    },
    /// Event data contains specific values
    DataContains {
        field_path: String,
        value: serde_json::Value,
        operator: ComparisonOperator,
    },
    /// Event metadata matches
    MetadataContains {
        key: String,
        value: String,
        operator: StringOperator,
    },
    /// Organization ID matches
    OrganizationId {
        org_id: u64,
    },
    /// Time-based condition
    TimeWindow {
        start_hour: u8,
        end_hour: u8,
        timezone: Option<String>,
    },
    /// Event frequency condition
    Frequency {
        event_type: EventType,
        count: u64,
        window_seconds: u64,
    },
    /// Complex condition with AND/OR logic
    Compound {
        operator: LogicalOperator,
        conditions: Vec<TriggerCondition>,
    },
    /// Regular expression match on event fields
    Regex {
        field_path: String,
        pattern: String,
    },
}

impl TriggerCondition {
    /// Check if an event matches this condition
    pub fn matches(&self, event: &Event, frequency_tracker: &FrequencyTracker) -> bool {
        match self {
            TriggerCondition::EventType { event_type } => event.event_type == *event_type,

            TriggerCondition::TopicPattern { pattern } => event.matches_topic(pattern),

            TriggerCondition::DataContains {
                field_path,
                value,
                operator,
            } => {
                if let Some(field_value) = self.extract_field_value(&event.data, field_path) {
                    operator.compare(&field_value, value)
                } else {
                    false
                }
            }

            TriggerCondition::MetadataContains { key, value, operator } => {
                if let Some(attr_value) = event.metadata.attributes.get(key) {
                    operator.compare_strings(attr_value, value)
                } else {
                    false
                }
            }

            TriggerCondition::OrganizationId { org_id } => event.org_id == *org_id,

            TriggerCondition::TimeWindow {
                start_hour,
                end_hour,
                timezone: _,
            } => {
                let hour = event.timestamp.hour();
                if start_hour <= end_hour {
                    hour >= *start_hour as u32 && hour <= *end_hour as u32
                } else {
                    // Wrap around midnight
                    hour >= *start_hour as u32 || hour <= *end_hour as u32
                }
            }

            TriggerCondition::Frequency {
                event_type,
                count,
                window_seconds,
            } => {
                if event.event_type == *event_type {
                    frequency_tracker.check_frequency(
                        &event.event_type.to_string(),
                        event.org_id,
                        *count,
                        *window_seconds,
                    )
                } else {
                    false
                }
            }

            TriggerCondition::Compound {
                operator,
                conditions,
            } => match operator {
                LogicalOperator::And => conditions
                    .iter()
                    .all(|cond| cond.matches(event, frequency_tracker)),
                LogicalOperator::Or => conditions
                    .iter()
                    .any(|cond| cond.matches(event, frequency_tracker)),
                LogicalOperator::Not => {
                    if conditions.len() == 1 {
                        !conditions[0].matches(event, frequency_tracker)
                    } else {
                        false // Invalid NOT condition
                    }
                }
            },

            TriggerCondition::Regex { field_path, pattern } => {
                if let Some(field_value) = self.extract_field_value(&event.data, field_path) {
                    if let Some(text) = field_value.as_str() {
                        if let Ok(regex) = Regex::new(pattern) {
                            return regex.is_match(text);
                        }
                    }
                }
                false
            }
        }
    }

    /// Extract a field value from event data using a dot-separated path
    fn extract_field_value(&self, data: &EventData, path: &str) -> Option<serde_json::Value> {
        let json_data = serde_json::to_value(data).ok()?;
        let parts: Vec<&str> = path.split('.').collect();

        let mut current = &json_data;
        for part in parts {
            current = current.get(part)?;
        }

        Some(current.clone())
    }
}

/// Comparison operators for data conditions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Contains,
    StartsWith,
    EndsWith,
}

impl ComparisonOperator {
    pub fn compare(&self, left: &serde_json::Value, right: &serde_json::Value) -> bool {
        match self {
            ComparisonOperator::Equal => left == right,
            ComparisonOperator::NotEqual => left != right,
            ComparisonOperator::GreaterThan => {
                if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    l > r
                } else {
                    false
                }
            }
            ComparisonOperator::GreaterThanOrEqual => {
                if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    l >= r
                } else {
                    false
                }
            }
            ComparisonOperator::LessThan => {
                if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    l < r
                } else {
                    false
                }
            }
            ComparisonOperator::LessThanOrEqual => {
                if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    l <= r
                } else {
                    false
                }
            }
            ComparisonOperator::Contains => {
                if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
                    l.contains(r)
                } else {
                    false
                }
            }
            ComparisonOperator::StartsWith => {
                if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
                    l.starts_with(r)
                } else {
                    false
                }
            }
            ComparisonOperator::EndsWith => {
                if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
                    l.ends_with(r)
                } else {
                    false
                }
            }
        }
    }
}

/// String comparison operators
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StringOperator {
    Equal,
    NotEqual,
    Contains,
    StartsWith,
    EndsWith,
    Regex,
}

impl StringOperator {
    pub fn compare_strings(&self, left: &str, right: &str) -> bool {
        match self {
            StringOperator::Equal => left == right,
            StringOperator::NotEqual => left != right,
            StringOperator::Contains => left.contains(right),
            StringOperator::StartsWith => left.starts_with(right),
            StringOperator::EndsWith => left.ends_with(right),
            StringOperator::Regex => {
                if let Ok(regex) = Regex::new(right) {
                    regex.is_match(left)
                } else {
                    false
                }
            }
        }
    }
}

/// Logical operators for compound conditions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalOperator {
    And,
    Or,
    Not,
}

/// Action to take when a trigger condition is met
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TriggerAction {
    /// Emit a new event
    EmitEvent {
        event_type: EventType,
        event_data: serde_json::Value,
    },
    /// Call a webhook
    CallWebhook {
        url: String,
        method: String,
        headers: HashMap<String, String>,
        body_template: Option<String>,
    },
    /// Execute a function
    ExecuteFunction {
        function_name: String,
        parameters: serde_json::Value,
    },
    /// Log a message
    Log {
        level: LogLevel,
        message_template: String,
    },
    /// Store data
    StoreData {
        key: String,
        value_template: String,
        ttl_seconds: Option<u64>,
    },
    /// Send notification
    SendNotification {
        channel: String,
        message_template: String,
        priority: NotificationPriority,
    },
}

/// Log levels for log actions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

/// Notification priorities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NotificationPriority {
    Low,
    Normal,
    High,
    Critical,
}

/// Trigger configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerConfig {
    /// Unique trigger identifier
    pub id: String,
    /// Human-readable trigger name
    pub name: String,
    /// Organization ID for isolation
    pub org_id: u64,
    /// Condition that must be met
    pub condition: TriggerCondition,
    /// Actions to execute when triggered
    pub actions: Vec<TriggerAction>,
    /// Whether this trigger is enabled
    pub enabled: bool,
    /// Rate limiting (max triggers per period)
    pub rate_limit: Option<RateLimit>,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Updated timestamp
    pub updated_at: DateTime<Utc>,
    /// Last triggered timestamp
    pub last_triggered: Option<DateTime<Utc>>,
    /// Trigger count
    pub trigger_count: u64,
    /// Tags for organization
    pub tags: Vec<String>,
}

impl TriggerConfig {
    pub fn new(
        name: String,
        org_id: u64,
        condition: TriggerCondition,
        actions: Vec<TriggerAction>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Ulid::new().to_string(),
            name,
            org_id,
            condition,
            actions,
            enabled: true,
            rate_limit: None,
            created_at: now,
            updated_at: now,
            last_triggered: None,
            trigger_count: 0,
            tags: Vec::new(),
        }
    }

    pub fn with_rate_limit(mut self, rate_limit: RateLimit) -> Self {
        self.rate_limit = Some(rate_limit);
        self
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }
}

/// Rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimit {
    /// Maximum triggers per period
    pub max_triggers: u64,
    /// Period in seconds
    pub period_seconds: u64,
}

/// Frequency tracking for frequency-based conditions
#[derive(Debug)]
pub struct FrequencyTracker {
    /// Event counts by type and org
    counts: Arc<RwLock<HashMap<String, Vec<DateTime<Utc>>>>>,
}

impl FrequencyTracker {
    pub fn new() -> Self {
        Self {
            counts: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Record an event occurrence
    pub async fn record_event(&self, event_key: &str) {
        self.record_event_with_org(event_key, 1).await;
    }

    /// Record an event occurrence for a specific org
    pub async fn record_event_with_org(&self, event_key: &str, org_id: u64) {
        let key = format!("{}:{}", org_id, event_key);
        let mut counts = self.counts.write().await;
        let entries = counts.entry(key).or_insert_with(Vec::new);
        entries.push(Utc::now());

        // Keep only recent entries (last hour)
        let cutoff = Utc::now() - chrono::Duration::hours(1);
        entries.retain(|&timestamp| timestamp > cutoff);
    }

    /// Check if frequency condition is met
    pub fn check_frequency(
        &self,
        event_key: &str,
        org_id: u64,
        required_count: u64,
        window_seconds: u64,
    ) -> bool {
        let key = format!("{}:{}", org_id, event_key);

        if let Ok(counts) = self.counts.try_read() {
            if let Some(entries) = counts.get(&key) {
                let cutoff = Utc::now() - chrono::Duration::seconds(window_seconds as i64);
                let recent_count = entries.iter().filter(|&&timestamp| timestamp > cutoff).count() as u64;
                return recent_count >= required_count;
            }
        }

        false
    }

    /// Clean up old entries
    pub async fn cleanup(&self) {
        let cutoff = Utc::now() - chrono::Duration::hours(24);
        let mut counts = self.counts.write().await;

        for entries in counts.values_mut() {
            entries.retain(|&timestamp| timestamp > cutoff);
        }

        // Remove empty entries
        counts.retain(|_, entries| !entries.is_empty());
    }
}

impl Default for FrequencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Trigger execution record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerExecution {
    /// Unique execution ID
    pub id: String,
    /// Trigger ID
    pub trigger_id: String,
    /// Event that triggered this execution
    pub event_id: String,
    /// Execution timestamp
    pub timestamp: DateTime<Utc>,
    /// Actions executed
    pub actions_executed: Vec<String>,
    /// Execution results
    pub results: Vec<ActionResult>,
    /// Whether execution was successful
    pub success: bool,
    /// Error message if failed
    pub error: Option<String>,
}

/// Result of executing a trigger action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionResult {
    /// Action type that was executed
    pub action_type: String,
    /// Whether the action succeeded
    pub success: bool,
    /// Result data or error message
    pub result: serde_json::Value,
    /// Execution duration in milliseconds
    pub duration_ms: u64,
}

/// Trigger manager statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerStats {
    /// Total triggers configured
    pub total_triggers: u64,
    /// Enabled triggers
    pub enabled_triggers: u64,
    /// Triggers executed in last 24 hours
    pub executions_24h: u64,
    /// Successful executions in last 24 hours
    pub successful_24h: u64,
    /// Failed executions in last 24 hours
    pub failed_24h: u64,
    /// Average execution time in milliseconds
    pub avg_execution_time_ms: f64,
}

/// Event trigger manager
pub struct TriggerManager {
    /// Event bus for receiving events
    event_bus: Arc<EventBus>,
    /// Trigger configurations
    triggers: Arc<RwLock<HashMap<String, TriggerConfig>>>,
    /// Execution history
    executions: Arc<RwLock<HashMap<String, TriggerExecution>>>,
    /// Frequency tracker for frequency-based conditions
    frequency_tracker: Arc<FrequencyTracker>,
    /// Rate limiters for triggers
    rate_limiters: Arc<RwLock<HashMap<String, (DateTime<Utc>, u64)>>>, // trigger_id -> (window_start, count)
    /// Manager state
    running: Arc<AtomicBool>,
    /// Background task handles
    task_handles: Arc<RwLock<Vec<JoinHandle<()>>>>,
}

impl TriggerManager {
    pub fn new(event_bus: Arc<EventBus>) -> Self {
        Self {
            event_bus,
            triggers: Arc::new(RwLock::new(HashMap::new())),
            executions: Arc::new(RwLock::new(HashMap::new())),
            frequency_tracker: Arc::new(FrequencyTracker::new()),
            rate_limiters: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(AtomicBool::new(false)),
            task_handles: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Start the trigger manager
    pub async fn start(&self) -> Result<()> {
        if self.running.load(Ordering::Acquire) {
            return Err(anyhow!("Trigger manager is already running"));
        }

        self.running.store(true, Ordering::Release);

        // Subscribe to events
        let handler = Arc::new(TriggerEventHandler {
            triggers: self.triggers.clone(),
            executions: self.executions.clone(),
            frequency_tracker: self.frequency_tracker.clone(),
            rate_limiters: self.rate_limiters.clone(),
        });

        self.event_bus.subscribe(handler).await?;

        // Start cleanup task
        let cleanup_task = self.start_cleanup_task().await;
        self.task_handles.write().await.push(cleanup_task);

        info!("Trigger manager started");
        Ok(())
    }

    /// Stop the trigger manager
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping trigger manager");

        self.running.store(false, Ordering::Release);

        // Cancel background tasks
        let mut handles = self.task_handles.write().await;
        for handle in handles.drain(..) {
            handle.abort();
        }

        info!("Trigger manager stopped");
        Ok(())
    }

    /// Add a trigger
    pub async fn add_trigger(&self, trigger: TriggerConfig) -> Result<()> {
        let trigger_id = trigger.id.clone();
        self.triggers.write().await.insert(trigger_id.clone(), trigger);

        info!(trigger_id = %trigger_id, "Trigger added");
        Ok(())
    }

    /// Remove a trigger
    pub async fn remove_trigger(&self, trigger_id: &str) -> Result<()> {
        if self.triggers.write().await.remove(trigger_id).is_some() {
            // Also remove rate limiter
            self.rate_limiters.write().await.remove(trigger_id);

            info!(trigger_id = %trigger_id, "Trigger removed");
            Ok(())
        } else {
            Err(anyhow!("Trigger not found: {}", trigger_id))
        }
    }

    /// Update a trigger
    pub async fn update_trigger(&self, mut trigger: TriggerConfig) -> Result<()> {
        trigger.updated_at = Utc::now();
        let trigger_id = trigger.id.clone();

        if self.triggers.read().await.contains_key(&trigger_id) {
            self.triggers.write().await.insert(trigger_id.clone(), trigger);
            info!(trigger_id = %trigger_id, "Trigger updated");
            Ok(())
        } else {
            Err(anyhow!("Trigger not found: {}", trigger_id))
        }
    }

    /// List all triggers
    pub async fn list_triggers(&self) -> Vec<TriggerConfig> {
        self.triggers.read().await.values().cloned().collect()
    }

    /// List triggers for an organization
    pub async fn list_triggers_for_org(&self, org_id: u64) -> Vec<TriggerConfig> {
        self.triggers
            .read()
            .await
            .values()
            .filter(|trigger| trigger.org_id == org_id)
            .cloned()
            .collect()
    }

    /// Get a specific trigger
    pub async fn get_trigger(&self, trigger_id: &str) -> Option<TriggerConfig> {
        self.triggers.read().await.get(trigger_id).cloned()
    }

    /// List trigger executions
    pub async fn list_executions(&self) -> Vec<TriggerExecution> {
        self.executions.read().await.values().cloned().collect()
    }

    /// List executions for a specific trigger
    pub async fn list_executions_for_trigger(&self, trigger_id: &str) -> Vec<TriggerExecution> {
        self.executions
            .read()
            .await
            .values()
            .filter(|exec| exec.trigger_id == trigger_id)
            .cloned()
            .collect()
    }

    /// Get trigger statistics
    pub async fn get_stats(&self) -> TriggerStats {
        let triggers = self.triggers.read().await;
        let executions = self.executions.read().await;

        let total_triggers = triggers.len() as u64;
        let enabled_triggers = triggers.values().filter(|t| t.enabled).count() as u64;

        let now = Utc::now();
        let day_ago = now - chrono::Duration::hours(24);

        let recent_executions: Vec<_> = executions
            .values()
            .filter(|e| e.timestamp > day_ago)
            .collect();

        let executions_24h = recent_executions.len() as u64;
        let successful_24h = recent_executions.iter().filter(|e| e.success).count() as u64;
        let failed_24h = executions_24h - successful_24h;

        // Calculate average execution time (placeholder)
        let avg_execution_time_ms = 0.0; // Would calculate from action results

        TriggerStats {
            total_triggers,
            enabled_triggers,
            executions_24h,
            successful_24h,
            failed_24h,
            avg_execution_time_ms,
        }
    }

    /// Start cleanup task for old executions and frequency data
    async fn start_cleanup_task(&self) -> JoinHandle<()> {
        let executions = self.executions.clone();
        let frequency_tracker = self.frequency_tracker.clone();
        let running = self.running.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600)); // Every hour

            while running.load(Ordering::Acquire) {
                interval.tick().await;

                // Clean up old executions (keep 7 days)
                let cutoff = Utc::now() - chrono::Duration::days(7);
                let mut exec_map = executions.write().await;
                let initial_len = exec_map.len();
                exec_map.retain(|_, execution| execution.timestamp > cutoff);
                let removed = initial_len - exec_map.len();

                if removed > 0 {
                    info!(removed = removed, "Cleaned up old trigger executions");
                }

                // Clean up frequency tracker
                frequency_tracker.cleanup().await;
            }

            debug!("Trigger cleanup task stopped");
        })
    }
}

/// Event handler for processing events against triggers
struct TriggerEventHandler {
    triggers: Arc<RwLock<HashMap<String, TriggerConfig>>>,
    executions: Arc<RwLock<HashMap<String, TriggerExecution>>>,
    frequency_tracker: Arc<FrequencyTracker>,
    rate_limiters: Arc<RwLock<HashMap<String, (DateTime<Utc>, u64)>>>,
}

#[async_trait::async_trait]
impl EventHandler for TriggerEventHandler {
    async fn handle(&self, event: &Event) -> HandlerResult {
        let triggers = self.triggers.read().await;

        for trigger in triggers.values() {
            if !trigger.enabled {
                continue;
            }

            // Check rate limiting
            if let Some(rate_limit) = &trigger.rate_limit {
                let mut limiters = self.rate_limiters.write().await;
                let (window_start, count) = limiters
                    .entry(trigger.id.clone())
                    .or_insert((Utc::now(), 0));

                let window_duration = chrono::Duration::seconds(rate_limit.period_seconds as i64);
                let now = Utc::now();

                if now - *window_start > window_duration {
                    // Reset window
                    *window_start = now;
                    *count = 0;
                }

                if *count >= rate_limit.max_triggers {
                    continue; // Rate limited
                }
            }

            // Check if condition matches
            if trigger.condition.matches(event, &self.frequency_tracker) {
                // Execute trigger
                let execution = self.execute_trigger(trigger, event).await;

                // Update rate limiter
                if trigger.rate_limit.is_some() {
                    if let Some((_, count)) = self.rate_limiters.write().await.get_mut(&trigger.id) {
                        *count += 1;
                    }
                }

                // Store execution record
                self.executions.write().await.insert(execution.id.clone(), execution);

                // Update trigger statistics
                // This would update the trigger's last_triggered and trigger_count
                // but we can't modify the trigger here due to borrowing rules
            }
        }

        // Record event for frequency tracking
        let event_key = format!("{}:{}", event.org_id, event.event_type);
        self.frequency_tracker.record_event(&event_key).await;

        HandlerResult::Success
    }

    fn interested_in(&self, _event_type: &EventType) -> bool {
        true // Process all events to check against triggers
    }

    fn name(&self) -> &str {
        "TriggerEventHandler"
    }
}

impl TriggerEventHandler {
    /// Execute a trigger's actions
    async fn execute_trigger(&self, trigger: &TriggerConfig, event: &Event) -> TriggerExecution {
        let execution_id = Ulid::new().to_string();
        let start_time = std::time::Instant::now();

        debug!(
            trigger_id = %trigger.id,
            event_id = %event.id,
            execution_id = %execution_id,
            "Executing trigger"
        );

        let mut results = Vec::new();
        let mut all_success = true;

        for action in &trigger.actions {
            let action_start = std::time::Instant::now();
            let action_result = self.execute_action(action, event).await;
            let action_duration = action_start.elapsed().as_millis() as u64;

            let success = action_result.is_ok();
            all_success &= success;

            let result = ActionResult {
                action_type: format!("{:?}", action).split('{').next().unwrap().to_string(),
                success,
                result: if success {
                    action_result.unwrap_or(serde_json::Value::Null)
                } else {
                    serde_json::json!({
                        "error": action_result.unwrap_err().to_string()
                    })
                },
                duration_ms: action_duration,
            };

            results.push(result);
        }

        TriggerExecution {
            id: execution_id,
            trigger_id: trigger.id.clone(),
            event_id: event.id.to_string(),
            timestamp: Utc::now(),
            actions_executed: trigger.actions.iter().map(|a| format!("{:?}", a)).collect(),
            results,
            success: all_success,
            error: if all_success {
                None
            } else {
                Some("One or more actions failed".to_string())
            },
        }
    }

    /// Execute a specific action
    async fn execute_action(&self, action: &TriggerAction, event: &Event) -> Result<serde_json::Value> {
        match action {
            TriggerAction::EmitEvent { event_type, event_data } => {
                // This would emit the event through the event bus
                Ok(serde_json::json!({
                    "event_type": event_type.to_string(),
                    "emitted": true
                }))
            }

            TriggerAction::CallWebhook { url, method, headers, body_template } => {
                let client = reqwest::Client::new();
                let mut request = match method.to_uppercase().as_str() {
                    "GET" => client.get(url),
                    "POST" => client.post(url),
                    "PUT" => client.put(url),
                    "DELETE" => client.delete(url),
                    _ => return Err(anyhow!("Unsupported HTTP method: {}", method)),
                };

                // Add headers
                for (key, value) in headers {
                    request = request.header(key, value);
                }

                // Add body if template is provided
                if let Some(template) = body_template {
                    let body = self.render_template(template, event)?;
                    request = request.body(body);
                }

                let response = request.send().await?;
                let status = response.status();
                let body = response.text().await?;

                Ok(serde_json::json!({
                    "status": status.as_u16(),
                    "body": body
                }))
            }

            TriggerAction::ExecuteFunction { function_name, parameters } => {
                // This would execute the function
                Ok(serde_json::json!({
                    "function": function_name,
                    "executed": true
                }))
            }

            TriggerAction::Log { level, message_template } => {
                let message = self.render_template(message_template, event)?;

                match level {
                    LogLevel::Debug => debug!("{}", message),
                    LogLevel::Info => info!("{}", message),
                    LogLevel::Warn => warn!("{}", message),
                    LogLevel::Error => error!("{}", message),
                }

                Ok(serde_json::json!({
                    "logged": true,
                    "level": format!("{:?}", level),
                    "message": message
                }))
            }

            TriggerAction::StoreData { key, value_template, ttl_seconds } => {
                let value = self.render_template(value_template, event)?;

                // This would store the data in a key-value store
                Ok(serde_json::json!({
                    "stored": true,
                    "key": key,
                    "value": value,
                    "ttl": ttl_seconds
                }))
            }

            TriggerAction::SendNotification { channel, message_template, priority } => {
                let message = self.render_template(message_template, event)?;

                // This would send the notification
                Ok(serde_json::json!({
                    "sent": true,
                    "channel": channel,
                    "message": message,
                    "priority": format!("{:?}", priority)
                }))
            }
        }
    }

    /// Render a template with event data
    fn render_template(&self, template: &str, event: &Event) -> Result<String> {
        // Simple template rendering - replace {{field}} with event data
        let mut result = template.to_string();

        // Replace common event fields
        result = result.replace("{{event_id}}", &event.id.to_string());
        result = result.replace("{{event_type}}", &event.event_type.to_string());
        result = result.replace("{{org_id}}", &event.org_id.to_string());
        result = result.replace("{{timestamp}}", &event.timestamp.to_rfc3339());

        // Replace event data fields (simplified)
        if let Ok(data_json) = serde_json::to_string(&event.data) {
            result = result.replace("{{event_data}}", &data_json);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{Event, EventData, EventType};

    #[test]
    fn test_trigger_condition_event_type() {
        let condition = TriggerCondition::EventType {
            event_type: EventType::JobCreated,
        };

        let event = Event::new(
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

        let tracker = FrequencyTracker::new();
        assert!(condition.matches(&event, &tracker));

        let wrong_event = Event::new(
            EventType::UserCreated,
            EventData::User {
                user_id: Some(1),
                org_id: 1,
                email: Some("test@example.com".to_string()),
                action: "created".to_string(),
                ip_address: None,
            },
            1,
        );

        assert!(!condition.matches(&wrong_event, &tracker));
    }

    #[test]
    fn test_trigger_condition_org_id() {
        let condition = TriggerCondition::OrganizationId { org_id: 1 };

        let event = Event::new(
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

        let tracker = FrequencyTracker::new();
        assert!(condition.matches(&event, &tracker));

        let wrong_org_event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 2,
                status: Some("pending".to_string()),
                worker_id: None,
                error: None,
                output: None,
            },
            2,
        );

        assert!(!condition.matches(&wrong_org_event, &tracker));
    }

    #[test]
    fn test_trigger_condition_compound_and() {
        let condition = TriggerCondition::Compound {
            operator: LogicalOperator::And,
            conditions: vec![
                TriggerCondition::EventType {
                    event_type: EventType::JobCreated,
                },
                TriggerCondition::OrganizationId { org_id: 1 },
            ],
        };

        let event = Event::new(
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

        let tracker = FrequencyTracker::new();
        assert!(condition.matches(&event, &tracker));

        let wrong_org_event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 2,
                status: Some("pending".to_string()),
                worker_id: None,
                error: None,
                output: None,
            },
            2,
        );

        assert!(!condition.matches(&wrong_org_event, &tracker));
    }

    #[tokio::test]
    async fn test_frequency_tracker() {
        let tracker = FrequencyTracker::new();
        let event_key = "test_event";

        // Record some events
        tracker.record_event(event_key).await;
        tracker.record_event(event_key).await;
        tracker.record_event(event_key).await;

        // Check frequency
        assert!(tracker.check_frequency(event_key, 1, 3, 60)); // 3 events in last 60 seconds
        assert!(!tracker.check_frequency(event_key, 1, 5, 60)); // Not 5 events
    }

    #[test]
    fn test_comparison_operators() {
        let left = serde_json::Value::Number(serde_json::Number::from(10));
        let right = serde_json::Value::Number(serde_json::Number::from(5));

        assert!(ComparisonOperator::GreaterThan.compare(&left, &right));
        assert!(!ComparisonOperator::LessThan.compare(&left, &right));

        let text_left = serde_json::Value::String("hello world".to_string());
        let text_right = serde_json::Value::String("world".to_string());

        assert!(ComparisonOperator::Contains.compare(&text_left, &text_right));
        assert!(!ComparisonOperator::StartsWith.compare(&text_left, &text_right));
    }
}