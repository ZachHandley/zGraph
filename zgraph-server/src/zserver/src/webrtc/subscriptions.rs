use super::WebRTCError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use zauth::AuthSession;
use zbrain;
use zpermissions::{Crud, PermissionService, Principal, ResourceKind};

/// Data subscription system for real-time streams
#[derive(Debug, Clone)]
pub struct DataSubscription {
    pub id: String,
    pub user_id: String,
    pub org_id: u64,
    pub project_id: Option<String>,
    pub subscription_type: SubscriptionType,
    pub filters: SubscriptionFilters,
    pub permissions: SubscriptionPermissions,
    pub api_key_id: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_activity: chrono::DateTime<chrono::Utc>,
    pub metadata: SubscriptionMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubscriptionType {
    /// Real-time data streams (SQL results, collection changes)
    DataStream {
        table_name: Option<String>,
        collection_name: Option<String>,
        query_pattern: Option<String>,
    },
    /// Function execution events and logs
    FunctionEvents {
        function_name: Option<String>,
        event_types: Vec<FunctionEventType>,
    },
    /// System metrics and monitoring
    SystemMetrics {
        metric_types: Vec<MetricType>,
        interval_seconds: u32,
    },
    /// User activity and session events
    UserActivity {
        user_ids: Vec<String>,
        activity_types: Vec<ActivityType>,
    },
    /// Custom topic-based subscriptions
    TopicStream {
        topic_pattern: String,
        message_types: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FunctionEventType {
    Execution,
    Completion,
    Error,
    Log,
    StateChange,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MetricType {
    ConnectionCount,
    MemoryUsage,
    CpuUsage,
    DatabaseStats,
    NetworkStats,
    CustomMetric(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ActivityType {
    Login,
    Logout,
    DataAccess,
    FunctionCall,
    FileUpload,
    PermissionChange,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionFilters {
    /// Include only events matching these patterns
    pub include_patterns: Vec<String>,
    /// Exclude events matching these patterns
    pub exclude_patterns: Vec<String>,
    /// Time-based filtering
    pub time_window: Option<TimeWindow>,
    /// Rate limiting
    pub rate_limit: Option<RateLimit>,
    /// Data filtering based on content
    pub content_filters: Vec<ContentFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeWindow {
    pub start: Option<chrono::DateTime<chrono::Utc>>,
    pub end: Option<chrono::DateTime<chrono::Utc>>,
    pub duration_minutes: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimit {
    pub max_events_per_second: u32,
    pub burst_size: u32,
    pub window_seconds: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentFilter {
    pub field_path: String,
    pub operator: FilterOperator,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterOperator {
    Equals,
    NotEquals,
    Contains,
    StartsWith,
    EndsWith,
    GreaterThan,
    LessThan,
    MatchesRegex,
    In,
    NotIn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionPermissions {
    /// Required resource permissions for this subscription
    pub required_resources: Vec<RequiredResource>,
    /// Additional permission checks
    pub custom_checks: Vec<String>,
    /// Maximum data rate allowed
    pub max_bandwidth_mbps: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequiredResource {
    pub resource_kind: String, // Maps to ResourceKind
    pub crud_operations: Vec<String>, // Maps to Crud operations
    pub resource_pattern: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionMetadata {
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub priority: SubscriptionPriority,
    pub quality_of_service: QualityOfService,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubscriptionPriority {
    Low,
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityOfService {
    pub guaranteed_delivery: bool,
    pub ordered_delivery: bool,
    pub duplicate_detection: bool,
    pub compression_enabled: bool,
}

impl Default for SubscriptionFilters {
    fn default() -> Self {
        Self {
            include_patterns: Vec::new(),
            exclude_patterns: Vec::new(),
            time_window: None,
            rate_limit: Some(RateLimit {
                max_events_per_second: 100,
                burst_size: 1000,
                window_seconds: 60,
            }),
            content_filters: Vec::new(),
        }
    }
}

impl Default for SubscriptionPermissions {
    fn default() -> Self {
        Self {
            required_resources: Vec::new(),
            custom_checks: Vec::new(),
            max_bandwidth_mbps: Some(10.0),
        }
    }
}

impl Default for SubscriptionMetadata {
    fn default() -> Self {
        Self {
            display_name: None,
            description: None,
            tags: Vec::new(),
            priority: SubscriptionPriority::Normal,
            quality_of_service: QualityOfService {
                guaranteed_delivery: false,
                ordered_delivery: false,
                duplicate_detection: true,
                compression_enabled: true,
            },
        }
    }
}

/// Subscription manager for handling real-time data streams
pub struct SubscriptionManager {
    subscriptions: Arc<RwLock<HashMap<String, DataSubscription>>>,
    user_subscriptions: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    api_key_subscriptions: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    permission_service: Arc<PermissionService<'static>>,
}

impl SubscriptionManager {
    pub fn new(permission_service: Arc<PermissionService<'static>>) -> Self {
        Self {
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            user_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            api_key_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            permission_service,
        }
    }

    /// Create a new subscription with permission validation
    pub async fn create_subscription(
        &self,
        auth: &AuthSession,
        request: CreateSubscriptionRequest,
    ) -> Result<DataSubscription, WebRTCError> {
        // Validate permissions for the subscription
        self.validate_subscription_permissions(auth, &request).await?;

        // Check subscription limits
        self.check_subscription_limits(auth, &request).await?;

        let subscription = DataSubscription {
            id: uuid::Uuid::new_v4().to_string(),
            user_id: auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone()),
            org_id: auth.org_id,
            project_id: request.project_id,
            subscription_type: request.subscription_type,
            filters: request.filters.unwrap_or_default(),
            permissions: request.permissions.unwrap_or_default(),
            api_key_id: request.api_key_id,
            created_at: chrono::Utc::now(),
            last_activity: chrono::Utc::now(),
            metadata: request.metadata.unwrap_or_default(),
        };

        // Store the subscription
        self.subscriptions.write().await.insert(subscription.id.clone(), subscription.clone());

        // Update user index
        self.user_subscriptions
            .write()
            .await
            .entry(auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone()))
            .or_insert_with(HashSet::new)
            .insert(subscription.id.clone());

        // Update API key index if applicable
        if let Some(api_key_id) = &subscription.api_key_id {
            self.api_key_subscriptions
                .write()
                .await
                .entry(api_key_id.clone())
                .or_insert_with(HashSet::new)
                .insert(subscription.id.clone());
        }

        tracing::info!(
            subscription_id = %subscription.id,
            user_id = ?auth.user_id,
            org_id = auth.org_id,
            subscription_type = ?subscription.subscription_type,
            "Created new data subscription"
        );

        Ok(subscription)
    }

    /// Remove a subscription
    pub async fn remove_subscription(
        &self,
        auth: &AuthSession,
        subscription_id: &str,
    ) -> Result<(), WebRTCError> {
        let mut subscriptions = self.subscriptions.write().await;

        if let Some(subscription) = subscriptions.get(subscription_id) {
            // Check ownership or admin permissions
            let auth_user_id = auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone());
            if subscription.user_id != auth_user_id && subscription.org_id != auth.org_id {
                return Err(WebRTCError::PermissionDenied);
            }

            // Remove from indices
            if let Some(user_subs) = self.user_subscriptions.write().await.get_mut(&subscription.user_id) {
                user_subs.remove(subscription_id);
            }

            if let Some(api_key_id) = &subscription.api_key_id {
                if let Some(api_subs) = self.api_key_subscriptions.write().await.get_mut(api_key_id) {
                    api_subs.remove(subscription_id);
                }
            }

            subscriptions.remove(subscription_id);

            tracing::info!(
                subscription_id = %subscription_id,
                user_id = ?auth.user_id,
                "Removed data subscription"
            );

            Ok(())
        } else {
            Err(WebRTCError::SessionNotFound)
        }
    }

    /// Get subscriptions for a user
    pub async fn get_user_subscriptions(&self, user_id: &str) -> Vec<DataSubscription> {
        let user_subs = self.user_subscriptions.read().await;
        let subscriptions = self.subscriptions.read().await;

        if let Some(sub_ids) = user_subs.get(user_id) {
            sub_ids
                .iter()
                .filter_map(|id| subscriptions.get(id).cloned())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get subscriptions by API key
    pub async fn get_api_key_subscriptions(&self, api_key_id: &str) -> Vec<DataSubscription> {
        let api_key_subs = self.api_key_subscriptions.read().await;
        let subscriptions = self.subscriptions.read().await;

        if let Some(sub_ids) = api_key_subs.get(api_key_id) {
            sub_ids
                .iter()
                .filter_map(|id| subscriptions.get(id).cloned())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Filter and route data to matching subscriptions
    pub async fn route_data_to_subscriptions(
        &self,
        data: &SubscriptionData,
    ) -> Vec<(String, SubscriptionData)> {
        let subscriptions = self.subscriptions.read().await;
        let mut results = Vec::new();

        for subscription in subscriptions.values() {
            if self.matches_subscription(subscription, data).await {
                if let Some(filtered_data) = self.apply_filters(subscription, data).await {
                    results.push((subscription.id.clone(), filtered_data));
                }
            }
        }

        results
    }

    /// Check if data matches a subscription
    async fn matches_subscription(&self, subscription: &DataSubscription, data: &SubscriptionData) -> bool {
        // Check project isolation
        if subscription.project_id != data.project_id {
            return false;
        }

        // Check organization isolation
        if subscription.org_id != data.org_id {
            return false;
        }

        // Check subscription type
        match (&subscription.subscription_type, &data.data_type) {
            (SubscriptionType::DataStream { table_name, collection_name, .. },
             SubscriptionDataType::DataStream { table, collection, .. }) => {
                if let Some(expected_table) = table_name {
                    if table.as_ref() != Some(expected_table) {
                        return false;
                    }
                }
                if let Some(expected_collection) = collection_name {
                    if collection.as_ref() != Some(expected_collection) {
                        return false;
                    }
                }
            }
            (SubscriptionType::FunctionEvents { function_name, event_types },
             SubscriptionDataType::FunctionEvent { function, event_type, .. }) => {
                if let Some(expected_function) = function_name {
                    if function != expected_function {
                        return false;
                    }
                }
                if !event_types.contains(event_type) {
                    return false;
                }
            }
            (SubscriptionType::SystemMetrics { metric_types, .. },
             SubscriptionDataType::SystemMetric { metric_type, .. }) => {
                if !metric_types.contains(metric_type) {
                    return false;
                }
            }
            _ => return false,
        }

        true
    }

    /// Apply filters and return filtered data
    async fn apply_filters(&self, subscription: &DataSubscription, data: &SubscriptionData) -> Option<SubscriptionData> {
        let filters = &subscription.filters;

        // Apply content filters
        for filter in &filters.content_filters {
            if !self.apply_content_filter(filter, data) {
                return None;
            }
        }

        // Apply time window filter
        if let Some(time_window) = &filters.time_window {
            if !self.is_within_time_window(time_window, data.timestamp) {
                return None;
            }
        }

        // Apply include/exclude patterns
        if !filters.include_patterns.is_empty() {
            let matches = filters.include_patterns.iter().any(|pattern| {
                data.topic.contains(pattern) || data.payload.to_string().contains(pattern)
            });
            if !matches {
                return None;
            }
        }

        for exclude_pattern in &filters.exclude_patterns {
            if data.topic.contains(exclude_pattern) || data.payload.to_string().contains(exclude_pattern) {
                return None;
            }
        }

        Some(data.clone())
    }

    fn apply_content_filter(&self, filter: &ContentFilter, data: &SubscriptionData) -> bool {
        // Extract field value using JSON path
        let field_value = self.extract_field_value(&filter.field_path, &data.payload);

        match (&filter.operator, field_value) {
            (FilterOperator::Equals, Some(value)) => value == filter.value,
            (FilterOperator::NotEquals, Some(value)) => value != filter.value,
            (FilterOperator::Contains, Some(serde_json::Value::String(s))) => {
                if let serde_json::Value::String(needle) = &filter.value {
                    s.contains(needle)
                } else {
                    false
                }
            }
            // Add more filter operators as needed
            _ => true, // Default to include if filter can't be applied
        }
    }

    fn extract_field_value(&self, path: &str, payload: &serde_json::Value) -> Option<serde_json::Value> {
        // Simple JSONPath-like extraction
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = payload;

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

    fn is_within_time_window(&self, window: &TimeWindow, timestamp: chrono::DateTime<chrono::Utc>) -> bool {
        if let Some(start) = window.start {
            if timestamp < start {
                return false;
            }
        }

        if let Some(end) = window.end {
            if timestamp > end {
                return false;
            }
        }

        if let Some(duration_minutes) = window.duration_minutes {
            let now = chrono::Utc::now();
            let window_start = now - chrono::Duration::minutes(duration_minutes);
            if timestamp < window_start {
                return false;
            }
        }

        true
    }

    async fn validate_subscription_permissions(
        &self,
        auth: &AuthSession,
        request: &CreateSubscriptionRequest,
    ) -> Result<(), WebRTCError> {
        // Validate required resources
        for required_resource in &request.permissions.as_ref().unwrap_or(&SubscriptionPermissions::default()).required_resources {
            // Convert string back to ResourceKind (this would need proper mapping)
            let resource_kind = match required_resource.resource_kind.as_str() {
                "Jobs" => ResourceKind::Jobs,
                "Tables" => ResourceKind::Tables,
                "Collections" => ResourceKind::Collections,
                "Files" => ResourceKind::Files,
                "Functions" => ResourceKind::Jobs, // Use Jobs as fallback for Functions
                "Permissions" => ResourceKind::Permissions,
                _ => continue, // Skip unknown resource types
            };

            for crud_op in &required_resource.crud_operations {
                let crud = match crud_op.as_str() {
                    "CREATE" => Crud::CREATE,
                    "READ" => Crud::READ,
                    "UPDATE" => Crud::UPDATE,
                    "DELETE" => Crud::DELETE,
                    _ => continue,
                };

                let principals = vec![Principal::User(auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone()))];

                if let Err(_) = self.permission_service.ensure(
                    auth.org_id,
                    &principals,
                    resource_kind,
                    crud,
                ) {
                    return Err(WebRTCError::PermissionDenied);
                }
            }
        }

        Ok(())
    }

    async fn check_subscription_limits(
        &self,
        auth: &AuthSession,
        _request: &CreateSubscriptionRequest,
    ) -> Result<(), WebRTCError> {
        let user_subs = self.user_subscriptions.read().await;
        let auth_user_id = auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone());
        if let Some(sub_ids) = user_subs.get(&auth_user_id) {
            if sub_ids.len() >= 10 { // Configurable limit
                return Err(WebRTCError::SubscriptionLimitExceeded);
            }
        }
        Ok(())
    }

    /// Set up standard data channels for event streaming
    pub async fn setup_standard_channels(
        session: &Arc<super::WebRTCSession>,
    ) -> Result<(), WebRTCError> {
        // Create standard channels for different event types
        let channels = ["logs", "events", "metrics"];

        for channel_name in &channels {
            let raw_data_channel = session.peer_connection
                .create_data_channel(channel_name, None)
                .await
                .map_err(|e| WebRTCError::InternalError(format!("Failed to create '{}' data channel: {}", channel_name, e)))?;

            let data_channel = raw_data_channel;

            // Set up event handlers for the channel
            let session_id = session.id.clone();
            let channel_name_owned = channel_name.to_string();

            data_channel.on_open(Box::new(move || {
                let session_id = session_id.clone();
                let channel_name = channel_name_owned.clone();
                Box::pin(async move {
                    tracing::info!(
                        session_id = %session_id,
                        channel = %channel_name,
                        "Event streaming data channel opened"
                    );
                })
            }));

            let session_id = session.id.clone();
            let channel_name_owned = channel_name.to_string();
            data_channel.on_close(Box::new(move || {
                let session_id = session_id.clone();
                let channel_name = channel_name_owned.clone();
                Box::pin(async move {
                    tracing::info!(
                        session_id = %session_id,
                        channel = %channel_name,
                        "Event streaming data channel closed"
                    );
                })
            }));

            let session_id = session.id.clone();
            let channel_name_owned = channel_name.to_string();
            data_channel.on_message(Box::new(move |msg| {
                let session_id = session_id.clone();
                let channel_name = channel_name_owned.clone();
                Box::pin(async move {
                    tracing::debug!(
                        session_id = %session_id,
                        channel = %channel_name,
                        data_len = msg.data.len(),
                        is_string = msg.is_string,
                        "Received message on event streaming data channel"
                    );
                })
            }));

            // Add channel to session
            session.add_data_channel(channel_name, data_channel).await;
        }

        tracing::info!(
            session_id = %session.id,
            "Set up standard event streaming data channels"
        );

        Ok(())
    }

    /// Start streaming events from job queue to WebRTC data channels
    pub async fn start_streaming(
        &self,
        session: Arc<super::WebRTCSession>,
        job_queue: Arc<zbrain::JobQueue>,
    ) -> Result<tokio::task::JoinHandle<()>, WebRTCError> {
        let session_id = session.id.clone();
        let org_id = session.org_id;

        // Set up standard channels first
        Self::setup_standard_channels(&session).await?;

        // Create broadcast receiver from job queue
        let mut rx = job_queue.subscribe();

        tracing::info!(
            session_id = %session_id,
            org_id = org_id,
            "Starting event streaming for WebRTC session"
        );

        let handle = tokio::spawn(async move {
            while let Ok(event) = rx.recv().await {
                // Filter events by organization ID
                if let Some(event_org_id) = Self::extract_org_id_from_event(&event) {
                    if event_org_id != org_id {
                        continue;
                    }
                }

                // Route event to appropriate data channel
                if let Err(e) = Self::route_event_to_channels(&session, &event).await {
                    tracing::warn!(
                        session_id = %session_id,
                        org_id = org_id,
                        error = %e,
                        "Failed to route event to data channel"
                    );
                }
            }

            tracing::info!(
                session_id = %session_id,
                org_id = org_id,
                "Event streaming ended for WebRTC session"
            );
        });

        Ok(handle)
    }

    /// Extract organization ID from a BusEvent topic or data
    fn extract_org_id_from_event(event: &zbrain::BusEvent) -> Option<u64> {
        // Try to extract org_id from the event data
        if let Some(org_id) = event.data.get("org_id") {
            if let Some(org_id_str) = org_id.as_str() {
                return org_id_str.parse().ok();
            }
            if let Some(org_id_num) = org_id.as_u64() {
                return Some(org_id_num);
            }
        }

        // For job events, we need to get org_id from the job data
        // This is a limitation - we might need to enhance BusEvent to include org_id
        // For now, we'll allow all events and rely on permission filtering later
        None
    }

    /// Route a BusEvent to the appropriate WebRTC data channels
    async fn route_event_to_channels(
        session: &Arc<super::WebRTCSession>,
        event: &zbrain::BusEvent,
    ) -> Result<(), String> {
        // Determine event type and route to appropriate channels
        if event.topic.starts_with("jobs/") {
            Self::route_job_event_to_channels(session, event).await
        } else {
            // Handle other event types as they are added
            tracing::debug!("Unhandled event topic: {}", event.topic);
            Ok(())
        }
    }

    /// Route job-specific events to data channels
    async fn route_job_event_to_channels(
        session: &Arc<super::WebRTCSession>,
        event: &zbrain::BusEvent,
    ) -> Result<(), String> {
        let event_data = &event.data;

        // Determine the event type from the data
        if let Some(event_type) = event_data.get("event").and_then(|e| e.as_str()) {
            match event_type {
                "log" => {
                    // Route log events to logs channel
                    Self::send_to_channel(session, "logs", event).await
                }
                "state" => {
                    // Route state changes to events channel
                    Self::send_to_channel(session, "events", event).await
                }
                "metrics" => {
                    // Route metrics to metrics channel
                    Self::send_to_channel(session, "metrics", event).await
                }
                _ => {
                    // Route unknown job events to general events channel
                    Self::send_to_channel(session, "events", event).await
                }
            }
        } else {
            // If no event type, route to events channel as fallback
            Self::send_to_channel(session, "events", event).await
        }
    }

    /// Send event data to a specific data channel
    async fn send_to_channel(
        session: &Arc<super::WebRTCSession>,
        channel_name: &str,
        event: &zbrain::BusEvent,
    ) -> Result<(), String> {
        // Create subscription data from BusEvent
        let subscription_data = SubscriptionData {
            id: uuid::Uuid::new_v4().to_string(),
            org_id: session.org_id,
            project_id: session.project_id.clone(),
            topic: event.topic.clone(),
            data_type: Self::map_event_to_subscription_type(event),
            payload: event.data.clone(),
            timestamp: event.ts,
            metadata: std::collections::HashMap::new(),
        };

        // Check if session has subscriptions that match this data
        let subscriptions = session.get_subscriptions().await;
        let should_send = subscriptions.is_empty() || // Send if no specific subscriptions
            subscriptions.iter().any(|sub| {
                // Check if subscription pattern matches the topic
                subscription_data.topic.contains(sub) ||
                subscription_data.topic.starts_with(sub)
            });

        if should_send {
            // Serialize the event for transmission
            let message = match serde_json::to_string(&subscription_data) {
                Ok(msg) => msg,
                Err(e) => {
                    return Err(format!("Failed to serialize event: {}", e));
                }
            };

            // Send to the specific data channel
            if let Err(e) = session.send_data_channel_message(channel_name, &message).await {
                return Err(format!("Failed to send to data channel '{}': {}", channel_name, e));
            }

            tracing::debug!(
                session_id = %session.id,
                channel = channel_name,
                topic = %event.topic,
                "Sent event to WebRTC data channel"
            );
        }

        Ok(())
    }

    /// Map a BusEvent to a SubscriptionDataType
    fn map_event_to_subscription_type(event: &zbrain::BusEvent) -> SubscriptionDataType {
        if event.topic.starts_with("jobs/") {
            if let Some(event_type) = event.data.get("event").and_then(|e| e.as_str()) {
                match event_type {
                    "log" => {
                        SubscriptionDataType::FunctionEvent {
                            function: event.topic.clone(),
                            event_type: FunctionEventType::Log,
                        }
                    }
                    "state" => {
                        let status = event.data.get("status")
                            .and_then(|s| s.as_str())
                            .unwrap_or("unknown");

                        match status {
                            "running" => SubscriptionDataType::FunctionEvent {
                                function: event.topic.clone(),
                                event_type: FunctionEventType::Execution,
                            },
                            "succeeded" => SubscriptionDataType::FunctionEvent {
                                function: event.topic.clone(),
                                event_type: FunctionEventType::Completion,
                            },
                            "failed" | "canceled" => SubscriptionDataType::FunctionEvent {
                                function: event.topic.clone(),
                                event_type: FunctionEventType::Error,
                            },
                            _ => SubscriptionDataType::FunctionEvent {
                                function: event.topic.clone(),
                                event_type: FunctionEventType::StateChange,
                            },
                        }
                    }
                    _ => {
                        SubscriptionDataType::TopicMessage {
                            message_type: event_type.to_string(),
                        }
                    }
                }
            } else {
                SubscriptionDataType::TopicMessage {
                    message_type: "job_event".to_string(),
                }
            }
        } else {
            SubscriptionDataType::TopicMessage {
                message_type: "generic".to_string(),
            }
        }
    }
}

/// Data that flows through the subscription system
#[derive(Debug, Clone, Serialize)]
pub struct SubscriptionData {
    pub id: String,
    pub org_id: u64,
    pub project_id: Option<String>,
    pub topic: String,
    pub data_type: SubscriptionDataType,
    pub payload: serde_json::Value,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize)]
pub enum SubscriptionDataType {
    DataStream {
        table: Option<String>,
        collection: Option<String>,
        operation: String,
    },
    FunctionEvent {
        function: String,
        event_type: FunctionEventType,
    },
    SystemMetric {
        metric_type: MetricType,
    },
    UserActivity {
        user_id: String,
        activity_type: ActivityType,
    },
    TopicMessage {
        message_type: String,
    },
}

/// Request to create a new subscription
#[derive(Debug, Deserialize)]
pub struct CreateSubscriptionRequest {
    pub project_id: Option<String>,
    pub subscription_type: SubscriptionType,
    pub filters: Option<SubscriptionFilters>,
    pub permissions: Option<SubscriptionPermissions>,
    pub api_key_id: Option<String>,
    pub metadata: Option<SubscriptionMetadata>,
}

/// Request to update a subscription
#[derive(Debug, Deserialize)]
pub struct UpdateSubscriptionRequest {
    pub filters: Option<SubscriptionFilters>,
    pub metadata: Option<SubscriptionMetadata>,
}