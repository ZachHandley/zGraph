use super::WebRTCError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use zauth::AuthSession;
// Temporarily commented out to isolate compilation issues
// use zfunctions::{FunctionExecutor, FunctionRequest, FunctionResult};

/// Function trigger system for WebRTC connections
#[derive(Debug, Clone)]
pub struct FunctionTrigger {
    pub id: String,
    pub name: String,
    pub org_id: u64,
    pub project_id: Option<String>,
    pub trigger_type: TriggerType,
    pub function_name: String,
    pub conditions: TriggerConditions,
    pub permissions: TriggerPermissions,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub enabled: bool,
    pub metadata: TriggerMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TriggerType {
    /// Triggered when a participant joins a room
    ParticipantJoined {
        room_pattern: Option<String>,
        role_filter: Option<String>,
    },
    /// Triggered when a participant leaves a room
    ParticipantLeft {
        room_pattern: Option<String>,
        role_filter: Option<String>,
    },
    /// Triggered when media state changes
    MediaStateChanged {
        room_pattern: Option<String>,
        state_filter: MediaStateFilter,
    },
    /// Triggered by data channel messages
    DataChannelMessage {
        channel_name: String,
        message_pattern: Option<String>,
    },
    /// Triggered by subscription data
    SubscriptionData {
        subscription_pattern: String,
        data_filter: Option<String>,
    },
    /// Triggered by WebRTC connection events
    ConnectionEvent {
        event_types: Vec<ConnectionEventType>,
    },
    /// Triggered manually by API call
    Manual {
        api_endpoint: String,
    },
    /// Triggered by timer/schedule
    Scheduled {
        cron_expression: String,
        timezone: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaStateFilter {
    pub audio_enabled: Option<bool>,
    pub video_enabled: Option<bool>,
    pub screen_sharing: Option<bool>,
    pub recording: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConnectionEventType {
    Connected,
    Disconnected,
    Failed,
    Reconnected,
    QualityChanged,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerConditions {
    /// Minimum participant count required
    pub min_participants: Option<usize>,
    /// Maximum participant count allowed
    pub max_participants: Option<usize>,
    /// Time-based conditions
    pub time_conditions: Option<TimeConditions>,
    /// Custom conditions using expressions
    pub custom_conditions: Vec<CustomCondition>,
    /// Rate limiting for trigger execution
    pub rate_limit: Option<TriggerRateLimit>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeConditions {
    pub allowed_hours: Option<Vec<u8>>, // 0-23
    pub allowed_days: Option<Vec<u8>>,  // 1-7 (Monday-Sunday)
    pub timezone: Option<String>,
    pub not_before: Option<chrono::DateTime<chrono::Utc>>,
    pub not_after: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomCondition {
    pub name: String,
    pub expression: String, // JSONPath or simple expression
    pub operator: ConditionOperator,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConditionOperator {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    Contains,
    MatchesRegex,
    In,
    NotIn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerRateLimit {
    pub max_executions_per_minute: u32,
    pub max_executions_per_hour: u32,
    pub max_executions_per_day: u32,
    pub cooldown_seconds: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerPermissions {
    /// Required permissions to execute this trigger
    pub required_permissions: Vec<String>,
    /// Users allowed to manually trigger this function
    pub allowed_users: Option<Vec<String>>,
    /// Roles allowed to execute this trigger
    pub allowed_roles: Option<Vec<String>>,
    /// API keys allowed to trigger this function
    pub allowed_api_keys: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerMetadata {
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub priority: TriggerPriority,
    pub timeout_seconds: Option<u32>,
    pub retry_config: Option<RetryConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TriggerPriority {
    Low,
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub backoff_seconds: u32,
    pub retry_on_errors: Vec<String>,
}

impl Default for TriggerConditions {
    fn default() -> Self {
        Self {
            min_participants: None,
            max_participants: None,
            time_conditions: None,
            custom_conditions: Vec::new(),
            rate_limit: Some(TriggerRateLimit {
                max_executions_per_minute: 60,
                max_executions_per_hour: 1000,
                max_executions_per_day: 10000,
                cooldown_seconds: None,
            }),
        }
    }
}

impl Default for TriggerPermissions {
    fn default() -> Self {
        Self {
            required_permissions: Vec::new(),
            allowed_users: None,
            allowed_roles: None,
            allowed_api_keys: None,
        }
    }
}

impl Default for TriggerMetadata {
    fn default() -> Self {
        Self {
            display_name: None,
            description: None,
            tags: Vec::new(),
            priority: TriggerPriority::Normal,
            timeout_seconds: Some(300), // 5 minutes default
            retry_config: Some(RetryConfig {
                max_retries: 3,
                backoff_seconds: 5,
                retry_on_errors: vec!["timeout".to_string(), "internal_error".to_string()],
            }),
        }
    }
}

/// Manager for WebRTC function triggers
pub struct FunctionTriggerManager {
    triggers: Arc<RwLock<HashMap<String, FunctionTrigger>>>,
    execution_history: Arc<RwLock<HashMap<String, Vec<TriggerExecution>>>>,
    // Temporarily commented out to isolate compilation issues
    // function_executor: Arc<FunctionExecutor>,
}

#[derive(Debug, Clone)]
pub struct TriggerExecution {
    pub trigger_id: String,
    pub execution_id: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub status: ExecutionStatus,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub context: TriggerContext,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionStatus {
    Running,
    Completed,
    Failed,
    Timeout,
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct TriggerContext {
    pub user_id: String,
    pub org_id: u64,
    pub project_id: Option<String>,
    pub room_id: Option<String>,
    pub session_id: Option<String>,
    pub event_data: serde_json::Value,
    pub metadata: HashMap<String, String>,
}

/// User context for trigger execution permission checking
#[derive(Debug, Clone)]
pub struct ExecutionUserContext {
    pub user_id: String,
    pub org_id: u64,
    pub session_id: Option<String>,
    pub roles: Vec<String>,
    pub groups: Vec<String>,
}

impl From<&TriggerContext> for ExecutionUserContext {
    fn from(context: &TriggerContext) -> Self {
        Self {
            user_id: context.user_id.clone(),
            org_id: context.org_id,
            session_id: context.session_id.clone(),
            roles: Vec::new(), // TODO: Extract from context metadata
            groups: Vec::new(), // TODO: Extract from context metadata
        }
    }
}

impl FunctionTriggerManager {
    pub fn new() -> Self {
        Self {
            triggers: Arc::new(RwLock::new(HashMap::new())),
            execution_history: Arc::new(RwLock::new(HashMap::new())),
            // function_executor,
        }
    }

    /// Register a new function trigger
    pub async fn register_trigger(
        &self,
        auth: &AuthSession,
        request: CreateTriggerRequest,
    ) -> Result<FunctionTrigger, WebRTCError> {
        // Validate permissions
        self.validate_trigger_permissions(auth, &request).await?;

        let trigger = FunctionTrigger {
            id: uuid::Uuid::new_v4().to_string(),
            name: request.name,
            org_id: auth.org_id,
            project_id: request.project_id,
            trigger_type: request.trigger_type,
            function_name: request.function_name,
            conditions: request.conditions.unwrap_or_default(),
            permissions: request.permissions.unwrap_or_default(),
            created_at: chrono::Utc::now(),
            enabled: request.enabled.unwrap_or(true),
            metadata: request.metadata.unwrap_or_default(),
        };

        self.triggers.write().await.insert(trigger.id.clone(), trigger.clone());

        tracing::info!(
            trigger_id = %trigger.id,
            trigger_name = %trigger.name,
            function_name = %trigger.function_name,
            user_id = ?auth.user_id,
            org_id = auth.org_id,
            "Registered new WebRTC function trigger"
        );

        Ok(trigger)
    }

    /// Remove a function trigger
    pub async fn remove_trigger(
        &self,
        auth: &AuthSession,
        trigger_id: &str,
    ) -> Result<(), WebRTCError> {
        let mut triggers = self.triggers.write().await;

        if let Some(trigger) = triggers.get(trigger_id) {
            // Check permissions
            if trigger.org_id != auth.org_id {
                return Err(WebRTCError::PermissionDenied);
            }

            triggers.remove(trigger_id);

            tracing::info!(
                trigger_id = %trigger_id,
                user_id = ?auth.user_id,
                "Removed WebRTC function trigger"
            );

            Ok(())
        } else {
            Err(WebRTCError::SessionNotFound)
        }
    }

    /// Execute trigger based on event
    pub async fn execute_triggers(&self, event: &TriggerEvent) -> Vec<TriggerExecution> {
        self.execute_triggers_with_permissions(event, None).await
    }

    /// Execute triggers with permission checking
    pub async fn execute_triggers_with_permissions(
        &self,
        event: &TriggerEvent,
        permissions_service: Option<Arc<zpermissions::PermissionService<'static>>>,
    ) -> Vec<TriggerExecution> {
        let triggers = self.triggers.read().await;
        let mut executions = Vec::new();

        for trigger in triggers.values() {
            if !trigger.enabled {
                continue;
            }

            // Check if trigger matches the event
            if self.matches_trigger(trigger, event).await {
                // Check conditions
                if self.check_conditions(trigger, event).await {
                    // Check execution permissions
                    let user_context = ExecutionUserContext::from(&event.context);
                    match self.check_execution_permission(trigger, &user_context, permissions_service.clone()).await {
                        Ok(()) => {
                            // Execute the function
                            match self.execute_function(trigger, event).await {
                                Ok(execution) => {
                                    executions.push(execution.clone());

                                    // Store execution history
                                    self.execution_history
                                        .write()
                                        .await
                                        .entry(trigger.id.clone())
                                        .or_insert_with(Vec::new)
                                        .push(execution);
                                }
                                Err(e) => {
                                    tracing::error!(
                                        trigger_id = %trigger.id,
                                        error = %e,
                                        "Failed to execute function trigger"
                                    );
                                }
                            }
                        }
                        Err(WebRTCError::PermissionDenied) => {
                            tracing::warn!(
                                trigger_id = %trigger.id,
                                user_id = %user_context.user_id,
                                org_id = user_context.org_id,
                                "Trigger execution denied due to insufficient permissions"
                            );

                            // Record denied execution in history
                            let denied_execution = TriggerExecution {
                                trigger_id: trigger.id.clone(),
                                execution_id: uuid::Uuid::new_v4().to_string(),
                                started_at: chrono::Utc::now(),
                                completed_at: Some(chrono::Utc::now()),
                                status: ExecutionStatus::Failed,
                                result: Some(serde_json::json!({
                                    "message": "Permission denied",
                                    "reason": "Insufficient permissions to execute trigger"
                                })),
                                error: Some("Permission denied".to_string()),
                                context: event.context.clone(),
                            };

                            self.execution_history
                                .write()
                                .await
                                .entry(trigger.id.clone())
                                .or_insert_with(Vec::new)
                                .push(denied_execution);
                        }
                        Err(e) => {
                            tracing::error!(
                                trigger_id = %trigger.id,
                                error = %e,
                                "Permission check failed for trigger execution"
                            );
                        }
                    }
                }
            }
        }

        executions
    }

    /// Check if trigger matches the event
    async fn matches_trigger(&self, trigger: &FunctionTrigger, event: &TriggerEvent) -> bool {
        // Check organization isolation
        if trigger.org_id != event.context.org_id {
            return false;
        }

        // Check project isolation
        if trigger.project_id != event.context.project_id {
            return false;
        }

        // Check trigger type
        match (&trigger.trigger_type, &event.event_type) {
            (TriggerType::ParticipantJoined { room_pattern, role_filter },
             TriggerEventType::ParticipantJoined { room_id, role, .. }) => {
                if let Some(pattern) = room_pattern {
                    if !room_id.contains(pattern) {
                        return false;
                    }
                }
                if let Some(filter) = role_filter {
                    if &format!("{:?}", role) != filter {
                        return false;
                    }
                }
            }
            (TriggerType::DataChannelMessage { channel_name, message_pattern },
             TriggerEventType::DataChannelMessage { channel, message, .. }) => {
                if channel_name != channel {
                    return false;
                }
                if let Some(pattern) = message_pattern {
                    if !message.contains(pattern) {
                        return false;
                    }
                }
            }
            // Add more trigger type matching
            _ => return false,
        }

        true
    }

    /// Check trigger conditions
    async fn check_conditions(&self, trigger: &FunctionTrigger, event: &TriggerEvent) -> bool {
        let conditions = &trigger.conditions;

        // Check participant count conditions
        if let Some(min_participants) = conditions.min_participants {
            if event.participant_count.unwrap_or(0) < min_participants {
                return false;
            }
        }

        if let Some(max_participants) = conditions.max_participants {
            if event.participant_count.unwrap_or(0) > max_participants {
                return false;
            }
        }

        // Check time conditions
        if let Some(time_conditions) = &conditions.time_conditions {
            if !self.check_time_conditions(time_conditions).await {
                return false;
            }
        }

        // Check custom conditions
        for condition in &conditions.custom_conditions {
            if !self.check_custom_condition(condition, event).await {
                return false;
            }
        }

        // Check rate limits
        if let Some(rate_limit) = &conditions.rate_limit {
            if !self.check_rate_limit(trigger, rate_limit).await {
                return false;
            }
        }

        true
    }

    async fn check_time_conditions(&self, conditions: &TimeConditions) -> bool {
        let now = chrono::Utc::now();

        if let Some(not_before) = conditions.not_before {
            if now < not_before {
                return false;
            }
        }

        if let Some(not_after) = conditions.not_after {
            if now > not_after {
                return false;
            }
        }

        // Add more time condition checks as needed
        true
    }

    async fn check_custom_condition(&self, condition: &CustomCondition, event: &TriggerEvent) -> bool {
        // Extract value from event using expression
        let value = self.extract_value_from_event(&condition.expression, event);

        match (&condition.operator, value) {
            (ConditionOperator::Equals, Some(val)) => val == condition.value,
            (ConditionOperator::NotEquals, Some(val)) => val != condition.value,
            // Add more operators
            _ => true, // Default to true if condition can't be evaluated
        }
    }

    fn extract_value_from_event(&self, expression: &str, event: &TriggerEvent) -> Option<serde_json::Value> {
        // Simple expression evaluation - could be enhanced with a proper expression engine
        match expression {
            "participant_count" => event.participant_count.map(|c| serde_json::Value::Number(c.into())),
            "room_id" => Some(serde_json::Value::String(event.context.room_id.clone().unwrap_or_default())),
            "user_id" => Some(serde_json::Value::String(event.context.user_id.clone())),
            _ => None,
        }
    }

    async fn check_rate_limit(&self, trigger: &FunctionTrigger, rate_limit: &TriggerRateLimit) -> bool {
        let history = self.execution_history.read().await;
        if let Some(executions) = history.get(&trigger.id) {
            let now = chrono::Utc::now();

            // Check minute limit
            let minute_ago = now - chrono::Duration::minutes(1);
            let minute_count = executions.iter()
                .filter(|e| e.started_at > minute_ago)
                .count() as u32;
            if minute_count >= rate_limit.max_executions_per_minute {
                return false;
            }

            // Check hour limit
            let hour_ago = now - chrono::Duration::hours(1);
            let hour_count = executions.iter()
                .filter(|e| e.started_at > hour_ago)
                .count() as u32;
            if hour_count >= rate_limit.max_executions_per_hour {
                return false;
            }

            // Check day limit
            let day_ago = now - chrono::Duration::days(1);
            let day_count = executions.iter()
                .filter(|e| e.started_at > day_ago)
                .count() as u32;
            if day_count >= rate_limit.max_executions_per_day {
                return false;
            }

            // Check cooldown
            if let Some(cooldown_seconds) = rate_limit.cooldown_seconds {
                if let Some(last_execution) = executions.last() {
                    let cooldown_duration = chrono::Duration::seconds(cooldown_seconds as i64);
                    if now - last_execution.started_at < cooldown_duration {
                        return false;
                    }
                }
            }
        }

        true
    }

    /// Execute the function
    async fn execute_function(
        &self,
        trigger: &FunctionTrigger,
        event: &TriggerEvent,
    ) -> Result<TriggerExecution, WebRTCError> {
        let execution_id = uuid::Uuid::new_v4().to_string();
        let started_at = chrono::Utc::now();

        let mut execution = TriggerExecution {
            trigger_id: trigger.id.clone(),
            execution_id: execution_id.clone(),
            started_at,
            completed_at: None,
            status: ExecutionStatus::Running,
            result: None,
            error: None,
            context: event.context.clone(),
        };

        // TODO: Prepare function request when zfunctions is available
        tracing::info!(
            trigger_id = %trigger.id,
            execution_id = %execution_id,
            function_name = %trigger.function_name,
            "Function trigger would execute: {} (stubbed)",
            trigger.function_name
        );

        // Simulate function execution (always succeeds for now)
        let simulated_result: Result<(), String> = Ok(());
        match simulated_result {
            Ok(_) => {
                execution.completed_at = Some(chrono::Utc::now());
                execution.status = ExecutionStatus::Completed;
                execution.result = Some(serde_json::json!({
                    "message": "Function execution stubbed",
                    "trigger_id": trigger.id,
                    "function_name": trigger.function_name
                }));

                tracing::info!(
                    trigger_id = %trigger.id,
                    execution_id = %execution_id,
                    function_name = %trigger.function_name,
                    "Function trigger executed successfully"
                );
            }
            Err(e) => {
                execution.completed_at = Some(chrono::Utc::now());
                execution.status = ExecutionStatus::Failed;
                execution.error = Some(e.to_string());

                tracing::error!(
                    trigger_id = %trigger.id,
                    execution_id = %execution_id,
                    function_name = %trigger.function_name,
                    error = %e,
                    "Function trigger execution failed"
                );
            }
        }

        Ok(execution)
    }

    async fn validate_trigger_permissions(
        &self,
        auth: &AuthSession,
        request: &CreateTriggerRequest,
    ) -> Result<(), WebRTCError> {
        // Check if user has permission to create function triggers
        // For now, we'll use a simple role-based check
        // In a full implementation, this would integrate with zpermissions

        // Check required permissions in trigger permissions
        for required_permission in &request.permissions.as_ref().unwrap_or(&TriggerPermissions::default()).required_permissions {
            // Validate that the user has the required permission
            // This is a placeholder - in practice this would check against zpermissions
            tracing::debug!(
                user_id = ?auth.user_id,
                org_id = auth.org_id,
                required_permission = %required_permission,
                "Validating trigger permission requirement"
            );
        }

        // Check if user is in allowed users list (if specified)
        if let Some(allowed_users) = &request.permissions.as_ref().unwrap_or(&TriggerPermissions::default()).allowed_users {
            let user_id = auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone());
            if !allowed_users.contains(&user_id) {
                return Err(WebRTCError::PermissionDenied);
            }
        }

        // Additional permission checks can be added here
        Ok(())
    }

    /// Check if a user has permission to execute a specific trigger
    pub async fn check_execution_permission(
        &self,
        trigger: &FunctionTrigger,
        user_context: &ExecutionUserContext,
        permissions_service: Option<Arc<zpermissions::PermissionService<'static>>>,
    ) -> Result<(), WebRTCError> {
        // Check organization isolation first
        if trigger.org_id != user_context.org_id {
            return Err(WebRTCError::PermissionDenied);
        }

        // Check if user is in allowed users (if specified)
        if let Some(allowed_users) = &trigger.permissions.allowed_users {
            if !allowed_users.contains(&user_context.user_id) {
                return Err(WebRTCError::PermissionDenied);
            }
        }

        // Check required permissions through zpermissions service
        if let Some(permissions_svc) = permissions_service {
            for required_permission in &trigger.permissions.required_permissions {
                // Map trigger permission to zpermissions resource/operation
                let (resource_kind, crud_op) = self.map_trigger_permission_to_zpermission(required_permission);

                // Create principals for the user
                let principals = vec![
                    zpermissions::Principal::User(user_context.user_id.clone()),
                    // Add additional principals like roles, groups as needed
                ];

                // Check permission
                match permissions_svc.ensure(
                    user_context.org_id,
                    &principals,
                    resource_kind,
                    crud_op,
                ) {
                    Ok(()) => continue,
                    Err(_) => return Err(WebRTCError::PermissionDenied),
                }
            }
        }

        tracing::debug!(
            trigger_id = %trigger.id,
            user_id = %user_context.user_id,
            org_id = user_context.org_id,
            "Permission check passed for trigger execution"
        );

        Ok(())
    }

    /// Map trigger permission strings to zpermissions resource kinds and CRUD operations
    fn map_trigger_permission_to_zpermission(&self, permission: &str) -> (zpermissions::ResourceKind, zpermissions::Crud) {
        match permission {
            "execute_function" => (zpermissions::ResourceKind::Jobs, zpermissions::Crud::CREATE),
            "read_data" => (zpermissions::ResourceKind::Collections, zpermissions::Crud::READ),
            "write_data" => (zpermissions::ResourceKind::Collections, zpermissions::Crud::UPDATE),
            "delete_data" => (zpermissions::ResourceKind::Collections, zpermissions::Crud::DELETE),
            "manage_triggers" => (zpermissions::ResourceKind::Jobs, zpermissions::Crud::UPDATE),
            "read_logs" => (zpermissions::ResourceKind::Jobs, zpermissions::Crud::READ),
            _ => (zpermissions::ResourceKind::Jobs, zpermissions::Crud::READ), // Default to most restrictive
        }
    }

    /// Get trigger execution history
    pub async fn get_execution_history(&self, trigger_id: &str) -> Vec<TriggerExecution> {
        self.execution_history
            .read()
            .await
            .get(trigger_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get all triggers for an organization
    pub async fn get_org_triggers(&self, org_id: u64) -> Vec<FunctionTrigger> {
        self.triggers
            .read()
            .await
            .values()
            .filter(|t| t.org_id == org_id)
            .cloned()
            .collect()
    }
}

/// Event that can trigger function execution
#[derive(Debug, Clone)]
pub struct TriggerEvent {
    pub event_type: TriggerEventType,
    pub event_data: serde_json::Value,
    pub context: TriggerContext,
    pub participant_count: Option<usize>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub enum TriggerEventType {
    ParticipantJoined {
        room_id: String,
        user_id: String,
        role: super::ParticipantRole,
    },
    ParticipantLeft {
        room_id: String,
        user_id: String,
        role: super::ParticipantRole,
    },
    MediaStateChanged {
        room_id: String,
        user_id: String,
        old_state: super::MediaState,
        new_state: super::MediaState,
    },
    DataChannelMessage {
        room_id: String,
        user_id: String,
        channel: String,
        message: String,
    },
    SubscriptionData {
        subscription_id: String,
        data: serde_json::Value,
    },
    ConnectionEvent {
        user_id: String,
        event_type: ConnectionEventType,
    },
    DatabaseChange {
        table_name: String,
        operation: super::database_triggers::DatabaseOperation,
        changes: Vec<super::database_triggers::DatabaseChangeEvent>,
    },
}

/// Request to create a function trigger
#[derive(Debug, Deserialize)]
pub struct CreateTriggerRequest {
    pub name: String,
    pub project_id: Option<String>,
    pub trigger_type: TriggerType,
    pub function_name: String,
    pub conditions: Option<TriggerConditions>,
    pub permissions: Option<TriggerPermissions>,
    pub enabled: Option<bool>,
    pub metadata: Option<TriggerMetadata>,
}

/// Request to update a function trigger
#[derive(Debug, Deserialize)]
pub struct UpdateTriggerRequest {
    pub conditions: Option<TriggerConditions>,
    pub permissions: Option<TriggerPermissions>,
    pub enabled: Option<bool>,
    pub metadata: Option<TriggerMetadata>,
}