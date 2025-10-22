use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, info, warn, error};
use uuid::Uuid;
use zevents::{Event, EventData, EventType};

/// Real-time transaction notification system for WebSocket and WebRTC
#[derive(Clone)]
pub struct TransactionNotificationManager {
    /// Active WebSocket connections by connection ID
    websocket_connections: Arc<RwLock<HashMap<String, WebSocketConnection>>>,
    /// Active WebRTC sessions by session ID
    webrtc_sessions: Arc<RwLock<HashMap<String, WebRTCTransactionSession>>>,
    /// Event subscriptions by connection/session
    subscriptions: Arc<RwLock<HashMap<String, TransactionSubscription>>>,
    /// Event broadcast channel
    event_sender: Arc<mpsc::UnboundedSender<TransactionNotificationEvent>>,
    /// Metrics collector
    metrics: Arc<RwLock<NotificationMetrics>>,
}

/// WebSocket connection information
#[derive(Debug, Clone)]
pub struct WebSocketConnection {
    pub connection_id: String,
    pub user_id: Option<u64>,
    pub org_id: u64,
    pub connected_at: chrono::DateTime<chrono::Utc>,
    pub last_activity: chrono::DateTime<chrono::Utc>,
    /// Channel for sending messages to this connection
    pub sender: mpsc::UnboundedSender<String>,
    /// Connection metadata
    pub metadata: HashMap<String, String>,
}

/// WebRTC session information for transaction notifications
/// Note: This is a simplified version - the full session is in session.rs
#[derive(Debug, Clone)]
pub struct WebRTCTransactionSession {
    pub session_id: String,
    pub user_id: Option<u64>,
    pub org_id: u64,
    pub room_id: Option<String>,
    pub connected_at: chrono::DateTime<chrono::Utc>,
    pub last_activity: chrono::DateTime<chrono::Utc>,
    /// Data channels for sending messages
    pub data_channels: HashMap<String, mpsc::UnboundedSender<Vec<u8>>>,
    /// Session metadata
    pub metadata: HashMap<String, String>,
}

/// Transaction subscription configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionSubscription {
    pub subscription_id: String,
    pub connection_id: String,
    pub org_id: u64,
    pub user_id: Option<u64>,
    /// Event types to subscribe to
    pub event_types: HashSet<EventType>,
    /// Table patterns to watch (supports wildcards)
    pub table_patterns: Vec<String>,
    /// Transaction event types to include
    pub transaction_types: HashSet<TransactionEventType>,
    /// Filters for events
    pub filters: NotificationFilters,
    /// Delivery preferences
    pub delivery_config: DeliveryConfig,
    /// When this subscription was created
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Types of transaction events to notify about
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum TransactionEventType {
    /// Transaction lifecycle events
    TransactionBegin,
    TransactionCommit,
    TransactionRollback,
    TransactionAbort,
    /// Data change events within transactions
    DataInsert,
    DataUpdate,
    DataDelete,
    /// Schema change events within transactions
    SchemaCreate,
    SchemaAlter,
    SchemaDrop,
    /// Transaction performance events
    SlowTransaction,
    LongRunningTransaction,
    DeadlockDetected,
}

/// Filters for transaction notifications
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationFilters {
    /// Minimum transaction duration to notify (in milliseconds)
    pub min_duration_ms: Option<u64>,
    /// Minimum rows affected to notify
    pub min_rows_affected: Option<u64>,
    /// Specific columns to watch for changes
    pub watched_columns: Vec<String>,
    /// User IDs to filter notifications for
    pub user_ids: Vec<u64>,
    /// Connection IDs to filter notifications for
    pub connection_ids: Vec<String>,
    /// Custom filters based on event data
    pub custom_filters: Vec<CustomFilter>,
}

/// Custom filter for notifications
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomFilter {
    pub field_path: String,
    pub operator: FilterOperator,
    pub value: serde_json::Value,
}

/// Filter operators for custom filters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterOperator {
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    Contains,
    StartsWith,
    In,
    NotIn,
}

/// Delivery configuration for notifications
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryConfig {
    /// Preferred delivery method
    pub delivery_method: DeliveryMethod,
    /// Whether to guarantee delivery
    pub guaranteed_delivery: bool,
    /// Maximum retry attempts for failed deliveries
    pub max_retries: u32,
    /// Whether to batch notifications
    pub batch_notifications: bool,
    /// Batch size for batched notifications
    pub batch_size: u32,
    /// Batch timeout in milliseconds
    pub batch_timeout_ms: u64,
    /// Whether to compress large notifications
    pub compression_enabled: bool,
}

/// Delivery methods for notifications
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeliveryMethod {
    /// Send via WebSocket
    WebSocket,
    /// Send via WebRTC data channel
    WebRTC { channel_name: String },
    /// Send via both WebSocket and WebRTC
    Both { webrtc_channel: String },
}

/// Transaction notification event
#[derive(Debug, Clone, Serialize)]
pub struct TransactionNotificationEvent {
    pub event_id: String,
    pub org_id: u64,
    pub transaction_id: Option<String>,
    pub event_type: TransactionEventType,
    pub table_name: Option<String>,
    pub operation: Option<String>,
    pub user_id: Option<u64>,
    pub connection_id: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub duration_ms: Option<u64>,
    pub rows_affected: Option<u64>,
    pub data_changes: Option<DataChanges>,
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Data changes within a transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataChanges {
    pub old_values: Option<HashMap<String, serde_json::Value>>,
    pub new_values: Option<HashMap<String, serde_json::Value>>,
    pub affected_columns: Vec<String>,
}

/// Notification delivery metrics
#[derive(Debug, Clone, Default)]
pub struct NotificationMetrics {
    pub total_notifications_sent: u64,
    pub websocket_notifications: u64,
    pub webrtc_notifications: u64,
    pub failed_deliveries: u64,
    pub retries_attempted: u64,
    pub average_delivery_time_ms: f64,
    pub active_subscriptions: u64,
    pub active_connections: u64,
}

impl Default for NotificationFilters {
    fn default() -> Self {
        Self {
            min_duration_ms: None,
            min_rows_affected: None,
            watched_columns: Vec::new(),
            user_ids: Vec::new(),
            connection_ids: Vec::new(),
            custom_filters: Vec::new(),
        }
    }
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            delivery_method: DeliveryMethod::WebSocket,
            guaranteed_delivery: false,
            max_retries: 3,
            batch_notifications: false,
            batch_size: 10,
            batch_timeout_ms: 1000,
            compression_enabled: true,
        }
    }
}

impl TransactionNotificationManager {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<TransactionNotificationEvent>) {
        let (event_sender, event_receiver) = mpsc::unbounded_channel();

        let manager = Self {
            websocket_connections: Arc::new(RwLock::new(HashMap::new())),
            webrtc_sessions: Arc::new(RwLock::new(HashMap::new())),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            event_sender: Arc::new(event_sender),
            metrics: Arc::new(RwLock::new(NotificationMetrics::default())),
        };

        (manager, event_receiver)
    }

    /// Register a WebSocket connection
    pub async fn register_websocket_connection(
        &self,
        connection_id: String,
        user_id: Option<u64>,
        org_id: u64,
        sender: mpsc::UnboundedSender<String>,
        metadata: HashMap<String, String>,
    ) -> Result<()> {
        let connection = WebSocketConnection {
            connection_id: connection_id.clone(),
            user_id,
            org_id,
            connected_at: chrono::Utc::now(),
            last_activity: chrono::Utc::now(),
            sender,
            metadata,
        };

        self.websocket_connections.write().await.insert(connection_id.clone(), connection);

        // Update metrics
        {
            let mut metrics = self.metrics.write().await;
            metrics.active_connections += 1;
        }

        info!(
            connection_id = %connection_id,
            org_id = org_id,
            user_id = ?user_id,
            "WebSocket connection registered for transaction notifications"
        );

        Ok(())
    }

    /// Register a WebRTC session
    pub async fn register_webrtc_session(
        &self,
        session_id: String,
        user_id: Option<u64>,
        org_id: u64,
        room_id: Option<String>,
        data_channels: HashMap<String, mpsc::UnboundedSender<Vec<u8>>>,
        metadata: HashMap<String, String>,
    ) -> Result<()> {
        let room_id_clone = room_id.clone();
        let session = WebRTCTransactionSession {
            session_id: session_id.clone(),
            user_id,
            org_id,
            room_id,
            connected_at: chrono::Utc::now(),
            last_activity: chrono::Utc::now(),
            data_channels,
            metadata,
        };

        self.webrtc_sessions.write().await.insert(session_id.clone(), session);

        // Update metrics
        {
            let mut metrics = self.metrics.write().await;
            metrics.active_connections += 1;
        }

        info!(
            session_id = %session_id,
            org_id = org_id,
            user_id = ?user_id,
            room_id = ?room_id_clone,
            "WebRTC session registered for transaction notifications"
        );

        Ok(())
    }

    /// Create a transaction event subscription
    pub async fn create_subscription(
        &self,
        connection_id: String,
        org_id: u64,
        user_id: Option<u64>,
        event_types: HashSet<EventType>,
        table_patterns: Vec<String>,
        transaction_types: HashSet<TransactionEventType>,
        filters: NotificationFilters,
        delivery_config: DeliveryConfig,
    ) -> Result<String> {
        let subscription_id = Uuid::new_v4().to_string();

        let subscription = TransactionSubscription {
            subscription_id: subscription_id.clone(),
            connection_id: connection_id.clone(),
            org_id,
            user_id,
            event_types,
            table_patterns,
            transaction_types,
            filters,
            delivery_config,
            created_at: chrono::Utc::now(),
        };

        self.subscriptions.write().await.insert(subscription_id.clone(), subscription);

        // Update metrics
        {
            let mut metrics = self.metrics.write().await;
            metrics.active_subscriptions += 1;
        }

        info!(
            subscription_id = %subscription_id,
            connection_id = %connection_id,
            org_id = org_id,
            "Transaction subscription created"
        );

        Ok(subscription_id)
    }

    /// Remove a subscription
    pub async fn remove_subscription(&self, subscription_id: &str) -> Result<()> {
        if self.subscriptions.write().await.remove(subscription_id).is_some() {
            // Update metrics
            {
                let mut metrics = self.metrics.write().await;
                metrics.active_subscriptions = metrics.active_subscriptions.saturating_sub(1);
            }

            info!(
                subscription_id = %subscription_id,
                "Transaction subscription removed"
            );
        }

        Ok(())
    }

    /// Process an event and send notifications to matching subscriptions
    pub async fn process_event(&self, event: &Event) -> Result<()> {
        // Convert event to transaction notification event
        let notification_event = self.convert_to_notification_event(event).await?;

        // Find matching subscriptions
        let matching_subscriptions = self.find_matching_subscriptions(&notification_event).await;

        // Send notifications
        for subscription in matching_subscriptions {
            if let Err(e) = self.send_notification(&subscription, &notification_event).await {
                warn!(
                    subscription_id = %subscription.subscription_id,
                    event_id = %notification_event.event_id,
                    error = %e,
                    "Failed to send transaction notification"
                );

                // Update metrics
                {
                    let mut metrics = self.metrics.write().await;
                    metrics.failed_deliveries += 1;
                }
            } else {
                // Update metrics
                {
                    let mut metrics = self.metrics.write().await;
                    metrics.total_notifications_sent += 1;
                }
            }
        }

        Ok(())
    }

    /// Convert zevents Event to TransactionNotificationEvent
    async fn convert_to_notification_event(&self, event: &Event) -> Result<TransactionNotificationEvent> {
        let (event_type, table_name, operation, duration_ms, rows_affected, data_changes) = match &event.data {
            EventData::Transaction { transaction_id: _, operations_count, duration_ms, error, .. } => {
                let event_type = match event.event_type {
                    EventType::TransactionBegun => TransactionEventType::TransactionBegin,
                    EventType::TransactionCommitted => TransactionEventType::TransactionCommit,
                    EventType::TransactionRolledBack => TransactionEventType::TransactionRollback,
                    EventType::TransactionAborted => TransactionEventType::TransactionAbort,
                    _ => return Err(anyhow::anyhow!("Unexpected transaction event type")),
                };

                (event_type, None, None, *duration_ms, operations_count.map(|c| c as u64), None)
            }
            EventData::DatabaseChange { table_name, operation, rows_affected, old_values, new_values, .. } => {
                let event_type = match operation.as_str() {
                    "INSERT" => TransactionEventType::DataInsert,
                    "UPDATE" => TransactionEventType::DataUpdate,
                    "DELETE" => TransactionEventType::DataDelete,
                    "CREATE" | "CREATE_TABLE" => TransactionEventType::SchemaCreate,
                    "ALTER" | "ALTER_TABLE" => TransactionEventType::SchemaAlter,
                    "DROP" | "DROP_TABLE" => TransactionEventType::SchemaDrop,
                    _ => return Err(anyhow::anyhow!("Unknown database operation: {}", operation)),
                };

                let data_changes = if old_values.is_some() || new_values.is_some() {
                    Some(DataChanges {
                        old_values: old_values.clone(),
                        new_values: new_values.clone(),
                        affected_columns: self.extract_affected_columns(old_values, new_values),
                    })
                } else {
                    None
                };

                (event_type, Some(table_name.clone()), Some(operation.clone()), None, Some(*rows_affected), data_changes)
            }
            _ => return Err(anyhow::anyhow!("Event type not supported for transaction notifications")),
        };

        Ok(TransactionNotificationEvent {
            event_id: event.id.to_string(),
            org_id: event.org_id,
            transaction_id: self.extract_transaction_id(event),
            event_type,
            table_name,
            operation,
            user_id: self.extract_user_id(event),
            connection_id: self.extract_connection_id(event),
            timestamp: event.timestamp,
            duration_ms,
            rows_affected,
            data_changes,
            metadata: HashMap::new(),
        })
    }

    /// Extract affected columns from old and new values
    fn extract_affected_columns(
        &self,
        old_values: &Option<HashMap<String, serde_json::Value>>,
        new_values: &Option<HashMap<String, serde_json::Value>>,
    ) -> Vec<String> {
        let mut columns = HashSet::new();

        if let Some(old) = old_values {
            columns.extend(old.keys().cloned());
        }

        if let Some(new) = new_values {
            columns.extend(new.keys().cloned());
        }

        columns.into_iter().collect()
    }

    /// Extract transaction ID from event
    fn extract_transaction_id(&self, event: &Event) -> Option<String> {
        match &event.data {
            EventData::Transaction { transaction_id, .. } => Some(transaction_id.clone()),
            EventData::DatabaseChange { transaction_id, .. } => transaction_id.clone(),
            _ => None,
        }
    }

    /// Extract user ID from event
    fn extract_user_id(&self, event: &Event) -> Option<u64> {
        match &event.data {
            EventData::User { user_id, .. } => *user_id,
            EventData::DatabaseChange { user_id, .. } => *user_id,
            _ => None,
        }
    }

    /// Extract connection ID from event
    fn extract_connection_id(&self, event: &Event) -> Option<String> {
        match &event.data {
            EventData::Connection { connection_id, .. } => Some(connection_id.clone()),
            EventData::DatabaseChange { connection_id, .. } => connection_id.clone(),
            _ => None,
        }
    }

    /// Find subscriptions that match the notification event
    async fn find_matching_subscriptions(&self, event: &TransactionNotificationEvent) -> Vec<TransactionSubscription> {
        let subscriptions = self.subscriptions.read().await;
        let mut matching = Vec::new();

        for subscription in subscriptions.values() {
            if self.subscription_matches_event(subscription, event).await {
                matching.push(subscription.clone());
            }
        }

        matching
    }

    /// Check if a subscription matches an event
    async fn subscription_matches_event(&self, subscription: &TransactionSubscription, event: &TransactionNotificationEvent) -> bool {
        // Check organization isolation
        if subscription.org_id != event.org_id {
            return false;
        }

        // Check transaction event types
        if !subscription.transaction_types.is_empty() && !subscription.transaction_types.contains(&event.event_type) {
            return false;
        }

        // Check table patterns
        if !subscription.table_patterns.is_empty() {
            if let Some(table_name) = &event.table_name {
                let matches = subscription.table_patterns.iter().any(|pattern| {
                    self.matches_pattern(table_name, pattern)
                });
                if !matches {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Apply filters
        self.apply_notification_filters(&subscription.filters, event).await
    }

    /// Check if a table name matches a pattern
    fn matches_pattern(&self, table_name: &str, pattern: &str) -> bool {
        if pattern == "*" {
            return true;
        }

        if pattern.contains('*') {
            let regex_pattern = pattern.replace('*', ".*");
            if let Ok(regex) = regex::Regex::new(&format!("^{}$", regex_pattern)) {
                return regex.is_match(table_name);
            }
        }

        table_name == pattern
    }

    /// Apply notification filters
    async fn apply_notification_filters(&self, filters: &NotificationFilters, event: &TransactionNotificationEvent) -> bool {
        // Check duration filter
        if let Some(min_duration) = filters.min_duration_ms {
            if event.duration_ms.map_or(true, |d| d < min_duration) {
                return false;
            }
        }

        // Check rows affected filter
        if let Some(min_rows) = filters.min_rows_affected {
            if event.rows_affected.map_or(true, |r| r < min_rows) {
                return false;
            }
        }

        // Check watched columns
        if !filters.watched_columns.is_empty() {
            if let Some(data_changes) = &event.data_changes {
                let has_watched_column = filters.watched_columns.iter().any(|col| {
                    data_changes.affected_columns.contains(col)
                });
                if !has_watched_column {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Check user ID filter
        if !filters.user_ids.is_empty() {
            if let Some(user_id) = event.user_id {
                if !filters.user_ids.contains(&user_id) {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Check connection ID filter
        if !filters.connection_ids.is_empty() {
            if let Some(connection_id) = &event.connection_id {
                if !filters.connection_ids.contains(connection_id) {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Apply custom filters
        for custom_filter in &filters.custom_filters {
            if !self.apply_custom_filter(custom_filter, event).await {
                return false;
            }
        }

        true
    }

    /// Apply custom filter to event
    async fn apply_custom_filter(&self, filter: &CustomFilter, event: &TransactionNotificationEvent) -> bool {
        // Extract value from event using field path
        let actual_value = self.extract_field_value(&filter.field_path, event);

        if let Some(value) = actual_value {
            self.apply_filter_operator(&filter.operator, &value, &filter.value)
        } else {
            false
        }
    }

    /// Extract field value from event using JSONPath-like syntax
    fn extract_field_value(&self, field_path: &str, event: &TransactionNotificationEvent) -> Option<serde_json::Value> {
        let event_json = serde_json::to_value(event).ok()?;

        let parts: Vec<&str> = field_path.split('.').collect();
        let mut current = &event_json;

        for part in parts {
            match current {
                serde_json::Value::Object(obj) => {
                    current = obj.get(part)?;
                }
                _ => return None,
            }
        }

        Some(current.clone())
    }

    /// Apply filter operator
    fn apply_filter_operator(&self, operator: &FilterOperator, actual: &serde_json::Value, expected: &serde_json::Value) -> bool {
        match operator {
            FilterOperator::Equal => actual == expected,
            FilterOperator::NotEqual => actual != expected,
            FilterOperator::GreaterThan => {
                if let (Some(a), Some(e)) = (actual.as_f64(), expected.as_f64()) {
                    a > e
                } else {
                    false
                }
            }
            FilterOperator::LessThan => {
                if let (Some(a), Some(e)) = (actual.as_f64(), expected.as_f64()) {
                    a < e
                } else {
                    false
                }
            }
            FilterOperator::Contains => {
                if let (Some(a), Some(e)) = (actual.as_str(), expected.as_str()) {
                    a.contains(e)
                } else {
                    false
                }
            }
            FilterOperator::StartsWith => {
                if let (Some(a), Some(e)) = (actual.as_str(), expected.as_str()) {
                    a.starts_with(e)
                } else {
                    false
                }
            }
            FilterOperator::In => {
                if let Some(expected_array) = expected.as_array() {
                    expected_array.contains(actual)
                } else {
                    false
                }
            }
            FilterOperator::NotIn => {
                if let Some(expected_array) = expected.as_array() {
                    !expected_array.contains(actual)
                } else {
                    true
                }
            }
        }
    }

    /// Send notification to a subscription
    async fn send_notification(&self, subscription: &TransactionSubscription, event: &TransactionNotificationEvent) -> Result<()> {
        let start_time = std::time::Instant::now();

        match &subscription.delivery_config.delivery_method {
            DeliveryMethod::WebSocket => {
                self.send_websocket_notification(&subscription.connection_id, event).await?;

                // Update metrics
                {
                    let mut metrics = self.metrics.write().await;
                    metrics.websocket_notifications += 1;
                }
            }
            DeliveryMethod::WebRTC { channel_name } => {
                self.send_webrtc_notification(&subscription.connection_id, channel_name, event).await?;

                // Update metrics
                {
                    let mut metrics = self.metrics.write().await;
                    metrics.webrtc_notifications += 1;
                }
            }
            DeliveryMethod::Both { webrtc_channel } => {
                // Try WebSocket first
                if let Err(e) = self.send_websocket_notification(&subscription.connection_id, event).await {
                    debug!("WebSocket delivery failed, trying WebRTC: {}", e);
                } else {
                    // Update metrics for WebSocket
                    let mut metrics = self.metrics.write().await;
                    metrics.websocket_notifications += 1;
                }

                // Also send via WebRTC
                if let Err(e) = self.send_webrtc_notification(&subscription.connection_id, webrtc_channel, event).await {
                    debug!("WebRTC delivery failed: {}", e);
                } else {
                    // Update metrics for WebRTC
                    let mut metrics = self.metrics.write().await;
                    metrics.webrtc_notifications += 1;
                }
            }
        }

        // Update delivery time metrics
        let delivery_time = start_time.elapsed().as_millis() as f64;
        {
            let mut metrics = self.metrics.write().await;
            metrics.average_delivery_time_ms = (metrics.average_delivery_time_ms + delivery_time) / 2.0;
        }

        debug!(
            subscription_id = %subscription.subscription_id,
            event_id = %event.event_id,
            delivery_time_ms = delivery_time,
            "Transaction notification delivered"
        );

        Ok(())
    }

    /// Send notification via WebSocket
    async fn send_websocket_notification(&self, connection_id: &str, event: &TransactionNotificationEvent) -> Result<()> {
        let connections = self.websocket_connections.read().await;

        if let Some(connection) = connections.get(connection_id) {
            let message = serde_json::to_string(event)?;
            connection.sender.send(message)?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("WebSocket connection not found: {}", connection_id))
        }
    }

    /// Send notification via WebRTC data channel
    async fn send_webrtc_notification(&self, session_id: &str, channel_name: &str, event: &TransactionNotificationEvent) -> Result<()> {
        let sessions = self.webrtc_sessions.read().await;

        if let Some(session) = sessions.get(session_id) {
            if let Some(channel_sender) = session.data_channels.get(channel_name) {
                let message = serde_json::to_vec(event)?;
                channel_sender.send(message)?;
                Ok(())
            } else {
                Err(anyhow::anyhow!("WebRTC data channel '{}' not found in session {}", channel_name, session_id))
            }
        } else {
            Err(anyhow::anyhow!("WebRTC session not found: {}", session_id))
        }
    }

    /// Remove a connection (WebSocket or WebRTC)
    pub async fn remove_connection(&self, connection_id: &str) -> Result<()> {
        // Remove from WebSocket connections
        if self.websocket_connections.write().await.remove(connection_id).is_some() {
            info!(connection_id = %connection_id, "WebSocket connection removed");
        }

        // Remove from WebRTC sessions
        if self.webrtc_sessions.write().await.remove(connection_id).is_some() {
            info!(session_id = %connection_id, "WebRTC session removed");
        }

        // Remove related subscriptions
        let mut subscriptions = self.subscriptions.write().await;
        let to_remove: Vec<String> = subscriptions
            .iter()
            .filter(|(_, sub)| sub.connection_id == connection_id)
            .map(|(id, _)| id.clone())
            .collect();

        for subscription_id in to_remove {
            subscriptions.remove(&subscription_id);
        }

        // Update metrics
        {
            let mut metrics = self.metrics.write().await;
            metrics.active_connections = metrics.active_connections.saturating_sub(1);
        }

        Ok(())
    }

    /// Get notification metrics
    pub async fn get_metrics(&self) -> NotificationMetrics {
        self.metrics.read().await.clone()
    }

    /// Get active subscriptions for an organization
    pub async fn get_subscriptions_for_org(&self, org_id: u64) -> Vec<TransactionSubscription> {
        self.subscriptions
            .read()
            .await
            .values()
            .filter(|sub| sub.org_id == org_id)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zevents::EventId;

    #[tokio::test]
    async fn test_notification_manager_creation() {
        let (manager, mut receiver) = TransactionNotificationManager::new();

        // Test that we can create the manager
        assert_eq!(manager.get_metrics().await.active_connections, 0);
        assert_eq!(manager.get_metrics().await.active_subscriptions, 0);
    }

    #[tokio::test]
    async fn test_pattern_matching() {
        let (manager, _) = TransactionNotificationManager::new();

        assert!(manager.matches_pattern("users", "users"));
        assert!(manager.matches_pattern("users", "*"));
        assert!(manager.matches_pattern("user_profiles", "user*"));
        assert!(!manager.matches_pattern("products", "user*"));
    }

    #[tokio::test]
    async fn test_subscription_creation() {
        let (manager, _) = TransactionNotificationManager::new();

        let subscription_id = manager
            .create_subscription(
                "conn_123".to_string(),
                1,
                Some(42),
                HashSet::from([EventType::TransactionCommitted]),
                vec!["users".to_string()],
                HashSet::from([TransactionEventType::TransactionCommit]),
                NotificationFilters::default(),
                DeliveryConfig::default(),
            )
            .await
            .unwrap();

        assert!(!subscription_id.is_empty());
        assert_eq!(manager.get_metrics().await.active_subscriptions, 1);
    }
}