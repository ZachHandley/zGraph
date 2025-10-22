use super::{TriggerEvent, TriggerEventType, TriggerContext, FunctionTriggerManager, WebRTCError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Conditional event handler system with rule engine
#[derive(Debug, Clone)]
pub struct ConditionalHandler {
    pub id: String,
    pub name: String,
    pub org_id: u64,
    pub project_id: Option<String>,
    pub rule_set: RuleSet,
    pub actions: Vec<HandlerAction>,
    pub enabled: bool,
    pub priority: HandlerPriority,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub metadata: ConditionalHandlerMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleSet {
    /// Primary rule that must be satisfied
    pub primary_rule: Rule,
    /// Additional rules (AND logic with primary)
    pub additional_rules: Vec<Rule>,
    /// Alternative rule sets (OR logic with primary + additional)
    pub alternative_rule_sets: Vec<RuleGroup>,
    /// Time-based constraints
    pub time_constraints: Option<TimeConstraints>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleGroup {
    pub rules: Vec<Rule>,
    pub logic: GroupLogic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GroupLogic {
    And,
    Or,
    Not,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    /// Field to evaluate (JSONPath-style selector)
    pub field: String,
    /// Comparison operator
    pub operator: RuleOperator,
    /// Value to compare against
    pub value: RuleValue,
    /// Optional weight for scoring rules
    pub weight: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuleOperator {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    Contains,
    NotContains,
    StartsWith,
    EndsWith,
    Matches, // Regex
    In,      // Value in array
    NotIn,   // Value not in array
    Exists,  // Field exists
    NotExists, // Field doesn't exist
    Between, // Numeric range
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuleValue {
    String(String),
    Number(f64),
    Boolean(bool),
    Array(Vec<String>),
    Null,
    Range { min: f64, max: f64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeConstraints {
    pub allowed_hours: Option<Vec<u8>>, // 0-23
    pub allowed_days: Option<Vec<u8>>,  // 1-7 (Monday-Sunday)
    pub allowed_months: Option<Vec<u8>>, // 1-12
    pub timezone: Option<String>,
    pub cooldown_seconds: Option<u32>,
    pub max_executions_per_hour: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HandlerAction {
    ExecuteFunction {
        function_name: String,
        parameters: HashMap<String, serde_json::Value>,
    },
    SendNotification {
        notification_type: String,
        recipients: Vec<String>,
        template: Option<String>,
    },
    TriggerWebhook {
        url: String,
        method: String,
        headers: HashMap<String, String>,
        payload: serde_json::Value,
    },
    ModifyData {
        target: String,
        modifications: HashMap<String, serde_json::Value>,
    },
    StopProcessing, // Prevent further handlers from running
    DelayExecution {
        delay_seconds: u32,
        action: Box<HandlerAction>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HandlerPriority {
    Critical,
    High,
    Normal,
    Low,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionalHandlerMetadata {
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub author: Option<String>,
    pub version: String,
    pub timeout_seconds: u32,
}

impl Default for ConditionalHandlerMetadata {
    fn default() -> Self {
        Self {
            description: None,
            tags: Vec::new(),
            author: None,
            version: "1.0.0".to_string(),
            timeout_seconds: 30,
        }
    }
}

/// Result of rule evaluation
#[derive(Debug, Clone)]
pub struct RuleEvaluationResult {
    pub passed: bool,
    pub score: f32,
    pub matched_rules: Vec<String>,
    pub failed_rules: Vec<String>,
    pub evaluation_details: HashMap<String, serde_json::Value>,
}

/// Manager for conditional event handlers
pub struct ConditionalHandlerManager {
    handlers: Arc<RwLock<HashMap<String, ConditionalHandler>>>,
    function_trigger_manager: Arc<FunctionTriggerManager>,
    execution_history: Arc<RwLock<HashMap<String, Vec<HandlerExecution>>>>,
    rule_engine: Arc<RuleEngine>,
}

#[derive(Debug, Clone)]
pub struct HandlerExecution {
    pub handler_id: String,
    pub execution_id: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub status: ExecutionStatus,
    pub rule_result: RuleEvaluationResult,
    pub actions_executed: Vec<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionStatus {
    Running,
    Completed,
    Failed,
    Timeout,
    Skipped,
}

/// Rule engine for evaluating conditions
pub struct RuleEngine {
    // Future: Could integrate with a more sophisticated rule engine like Rego
}

impl RuleEngine {
    pub fn new() -> Self {
        Self {}
    }

    /// Evaluate a rule set against an event
    pub async fn evaluate_rule_set(
        &self,
        rule_set: &RuleSet,
        event: &TriggerEvent,
    ) -> RuleEvaluationResult {
        // Check time constraints first
        if let Some(time_constraints) = &rule_set.time_constraints {
            if !self.check_time_constraints(time_constraints).await {
                return RuleEvaluationResult {
                    passed: false,
                    score: 0.0,
                    matched_rules: Vec::new(),
                    failed_rules: vec!["time_constraint".to_string()],
                    evaluation_details: HashMap::new(),
                };
            }
        }

        // Evaluate primary rule and additional rules (AND logic)
        let mut primary_passed = self.evaluate_rule(&rule_set.primary_rule, event).await;
        let mut total_score = if primary_passed.passed { primary_passed.score } else { 0.0 };
        let mut matched_rules = if primary_passed.passed { vec![rule_set.primary_rule.field.clone()] } else { Vec::new() };
        let mut failed_rules = if !primary_passed.passed { vec![rule_set.primary_rule.field.clone()] } else { Vec::new() };
        let mut evaluation_details = HashMap::new();

        evaluation_details.insert("primary_rule".to_string(), serde_json::json!({
            "passed": primary_passed.passed,
            "score": primary_passed.score,
            "field": rule_set.primary_rule.field,
            "operator": rule_set.primary_rule.operator,
        }));

        // Evaluate additional rules (must all pass if primary passed)
        let additional_all_passed = if primary_passed.passed {
            let mut all_passed = true;
            for rule in &rule_set.additional_rules {
                let rule_result = self.evaluate_rule(rule, event).await;
                if rule_result.passed {
                    matched_rules.push(rule.field.clone());
                    total_score += rule_result.score;
                } else {
                    failed_rules.push(rule.field.clone());
                    all_passed = false;
                }

                evaluation_details.insert(
                    format!("additional_rule_{}", rule.field),
                    serde_json::json!({
                        "passed": rule_result.passed,
                        "score": rule_result.score,
                        "field": rule.field,
                        "operator": rule.operator,
                    })
                );
            }
            all_passed
        } else {
            false
        };

        let primary_and_additional_passed = primary_passed.passed && additional_all_passed;

        // Evaluate alternative rule sets (OR logic)
        let mut alternative_passed = false;
        for (idx, rule_group) in rule_set.alternative_rule_sets.iter().enumerate() {
            let group_result = self.evaluate_rule_group(rule_group, event).await;
            if group_result.passed {
                alternative_passed = true;
                matched_rules.extend(group_result.matched_rules);
                total_score += group_result.score;
            } else {
                failed_rules.extend(group_result.failed_rules);
            }

            evaluation_details.insert(
                format!("alternative_group_{}", idx),
                serde_json::json!({
                    "passed": group_result.passed,
                    "score": group_result.score,
                    "logic": rule_group.logic,
                })
            );
        }

        let final_passed = primary_and_additional_passed || alternative_passed;

        RuleEvaluationResult {
            passed: final_passed,
            score: total_score,
            matched_rules,
            failed_rules,
            evaluation_details,
        }
    }

    async fn evaluate_rule(&self, rule: &Rule, event: &TriggerEvent) -> RuleEvaluationResult {
        let extracted_value = self.extract_field_value(&rule.field, event);
        let passed = self.apply_operator(&rule.operator, &extracted_value, &rule.value);
        let score = if passed { rule.weight.unwrap_or(1.0) } else { 0.0 };

        RuleEvaluationResult {
            passed,
            score,
            matched_rules: if passed { vec![rule.field.clone()] } else { Vec::new() },
            failed_rules: if !passed { vec![rule.field.clone()] } else { Vec::new() },
            evaluation_details: {
                let mut details = HashMap::new();
                details.insert("extracted_value".to_string(), extracted_value.unwrap_or(serde_json::Value::Null));
                details.insert("expected_value".to_string(), self.rule_value_to_json(&rule.value));
                details.insert("operator".to_string(), serde_json::json!(format!("{:?}", rule.operator)));
                details
            },
        }
    }

    async fn evaluate_rule_group(&self, group: &RuleGroup, event: &TriggerEvent) -> RuleEvaluationResult {
        let mut results = Vec::new();
        for rule in &group.rules {
            results.push(self.evaluate_rule(rule, event).await);
        }

        match group.logic {
            GroupLogic::And => {
                let all_passed = results.iter().all(|r| r.passed);
                let total_score: f32 = results.iter().map(|r| r.score).sum();
                let matched_rules: Vec<String> = results.iter().flat_map(|r| r.matched_rules.clone()).collect();
                let failed_rules: Vec<String> = results.iter().flat_map(|r| r.failed_rules.clone()).collect();

                RuleEvaluationResult {
                    passed: all_passed,
                    score: if all_passed { total_score } else { 0.0 },
                    matched_rules: if all_passed { matched_rules } else { Vec::new() },
                    failed_rules: if !all_passed { failed_rules } else { Vec::new() },
                    evaluation_details: HashMap::new(),
                }
            }
            GroupLogic::Or => {
                let any_passed = results.iter().any(|r| r.passed);
                let max_score = results.iter().map(|r| r.score).fold(0.0, f32::max);
                let matched_rules: Vec<String> = results.iter()
                    .filter(|r| r.passed)
                    .flat_map(|r| r.matched_rules.clone())
                    .collect();
                let failed_rules: Vec<String> = results.iter()
                    .filter(|r| !r.passed)
                    .flat_map(|r| r.failed_rules.clone())
                    .collect();

                RuleEvaluationResult {
                    passed: any_passed,
                    score: if any_passed { max_score } else { 0.0 },
                    matched_rules,
                    failed_rules: if any_passed { Vec::new() } else { failed_rules },
                    evaluation_details: HashMap::new(),
                }
            }
            GroupLogic::Not => {
                let any_passed = results.iter().any(|r| r.passed);
                RuleEvaluationResult {
                    passed: !any_passed,
                    score: if !any_passed { 1.0 } else { 0.0 },
                    matched_rules: if !any_passed { vec!["not_group".to_string()] } else { Vec::new() },
                    failed_rules: if any_passed { vec!["not_group".to_string()] } else { Vec::new() },
                    evaluation_details: HashMap::new(),
                }
            }
        }
    }

    fn extract_field_value(&self, field: &str, event: &TriggerEvent) -> Option<serde_json::Value> {
        // Simple field extraction - could be enhanced with JSONPath
        match field {
            "event_type" => Some(serde_json::json!(format!("{:?}", std::mem::discriminant(&event.event_type)))),
            "timestamp" => Some(serde_json::json!(event.timestamp)),
            "participant_count" => event.participant_count.map(|c| serde_json::json!(c)),
            "context.user_id" => Some(serde_json::json!(event.context.user_id)),
            "context.org_id" => Some(serde_json::json!(event.context.org_id)),
            "context.room_id" => event.context.room_id.as_ref().map(|r| serde_json::json!(r)),
            "event_data" => Some(event.event_data.clone()),
            _ => {
                // Try to extract from event_data using simple dot notation
                if field.starts_with("event_data.") {
                    let key = &field[11..]; // Remove "event_data." prefix
                    event.event_data.get(key).cloned()
                } else {
                    None
                }
            }
        }
    }

    fn apply_operator(
        &self,
        operator: &RuleOperator,
        actual: &Option<serde_json::Value>,
        expected: &RuleValue,
    ) -> bool {
        match actual {
            Some(actual_val) => {
                match operator {
                    RuleOperator::Equals => {
                        let expected_val = self.rule_value_to_json(expected);
                        actual_val == &expected_val
                    }
                    RuleOperator::NotEquals => {
                        let expected_val = self.rule_value_to_json(expected);
                        actual_val != &expected_val
                    }
                    RuleOperator::GreaterThan => {
                        if let (Some(actual_num), RuleValue::Number(expected_num)) = (actual_val.as_f64(), expected) {
                            actual_num > *expected_num
                        } else {
                            false
                        }
                    }
                    RuleOperator::LessThan => {
                        if let (Some(actual_num), RuleValue::Number(expected_num)) = (actual_val.as_f64(), expected) {
                            actual_num < *expected_num
                        } else {
                            false
                        }
                    }
                    RuleOperator::Contains => {
                        if let (Some(actual_str), RuleValue::String(expected_str)) = (actual_val.as_str(), expected) {
                            actual_str.contains(expected_str)
                        } else {
                            false
                        }
                    }
                    RuleOperator::In => {
                        if let RuleValue::Array(expected_array) = expected {
                            if let Some(actual_str) = actual_val.as_str() {
                                expected_array.contains(&actual_str.to_string())
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    }
                    RuleOperator::Exists => true, // Field exists if we got a value
                    RuleOperator::NotExists => false, // Field exists
                    RuleOperator::IsNull => actual_val.is_null(),
                    RuleOperator::IsNotNull => !actual_val.is_null(),
                    RuleOperator::Between => {
                        if let (Some(actual_num), RuleValue::Range { min, max }) = (actual_val.as_f64(), expected) {
                            actual_num >= *min && actual_num <= *max
                        } else {
                            false
                        }
                    }
                    // TODO: Implement remaining operators
                    _ => false,
                }
            }
            None => {
                match operator {
                    RuleOperator::NotExists => true,
                    RuleOperator::Exists => false,
                    _ => false,
                }
            }
        }
    }

    fn rule_value_to_json(&self, value: &RuleValue) -> serde_json::Value {
        match value {
            RuleValue::String(s) => serde_json::json!(s),
            RuleValue::Number(n) => serde_json::json!(n),
            RuleValue::Boolean(b) => serde_json::json!(b),
            RuleValue::Array(a) => serde_json::json!(a),
            RuleValue::Null => serde_json::Value::Null,
            RuleValue::Range { min, max } => serde_json::json!({ "min": min, "max": max }),
        }
    }

    async fn check_time_constraints(&self, _constraints: &TimeConstraints) -> bool {
        // TODO: Implement time constraint checking
        true
    }
}

impl ConditionalHandlerManager {
    pub fn new(function_trigger_manager: Arc<FunctionTriggerManager>) -> Self {
        Self {
            handlers: Arc::new(RwLock::new(HashMap::new())),
            function_trigger_manager,
            execution_history: Arc::new(RwLock::new(HashMap::new())),
            rule_engine: Arc::new(RuleEngine::new()),
        }
    }

    /// Register a new conditional handler
    pub async fn register_handler(&self, handler: ConditionalHandler) -> Result<(), WebRTCError> {
        let mut handlers = self.handlers.write().await;
        handlers.insert(handler.id.clone(), handler.clone());

        tracing::info!(
            handler_id = %handler.id,
            handler_name = %handler.name,
            priority = ?handler.priority,
            "Registered conditional handler"
        );

        Ok(())
    }

    /// Remove a conditional handler
    pub async fn remove_handler(&self, handler_id: &str) -> Result<(), WebRTCError> {
        let mut handlers = self.handlers.write().await;
        if handlers.remove(handler_id).is_some() {
            self.execution_history.write().await.remove(handler_id);

            tracing::info!(
                handler_id = %handler_id,
                "Removed conditional handler"
            );
            Ok(())
        } else {
            Err(WebRTCError::SessionNotFound)
        }
    }

    /// Process an event against all conditional handlers
    pub async fn process_event(&self, event: &TriggerEvent) -> Vec<HandlerExecution> {
        let handlers = self.handlers.read().await;
        let mut executions = Vec::new();

        // Sort handlers by priority
        let mut sorted_handlers: Vec<_> = handlers.values().collect();
        sorted_handlers.sort_by(|a, b| {
            match (&a.priority, &b.priority) {
                (HandlerPriority::Critical, _) => std::cmp::Ordering::Less,
                (_, HandlerPriority::Critical) => std::cmp::Ordering::Greater,
                (HandlerPriority::High, HandlerPriority::Low) => std::cmp::Ordering::Less,
                (HandlerPriority::Low, HandlerPriority::High) => std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Equal,
            }
        });

        for handler in sorted_handlers {
            if !handler.enabled {
                continue;
            }

            // Check organization isolation
            if handler.org_id != event.context.org_id {
                continue;
            }

            // Check project isolation
            if handler.project_id != event.context.project_id {
                continue;
            }

            // Evaluate rule set
            let rule_result = self.rule_engine.evaluate_rule_set(&handler.rule_set, event).await;

            if rule_result.passed {
                match self.execute_handler_actions(handler, event, rule_result.clone()).await {
                    Ok(execution) => {
                        executions.push(execution.clone());

                        // Store execution history
                        self.execution_history
                            .write()
                            .await
                            .entry(handler.id.clone())
                            .or_insert_with(Vec::new)
                            .push(execution.clone());

                        // Check if handler requested to stop processing
                        if execution.actions_executed.iter().any(|action| action == "StopProcessing") {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            handler_id = %handler.id,
                            error = %e,
                            "Failed to execute conditional handler"
                        );
                    }
                }
            } else {
                tracing::debug!(
                    handler_id = %handler.id,
                    rule_score = rule_result.score,
                    failed_rules = ?rule_result.failed_rules,
                    "Conditional handler rule evaluation failed"
                );
            }
        }

        executions
    }

    async fn execute_handler_actions(
        &self,
        handler: &ConditionalHandler,
        event: &TriggerEvent,
        rule_result: RuleEvaluationResult,
    ) -> Result<HandlerExecution, WebRTCError> {
        let execution_id = uuid::Uuid::new_v4().to_string();
        let started_at = chrono::Utc::now();

        let mut execution = HandlerExecution {
            handler_id: handler.id.clone(),
            execution_id: execution_id.clone(),
            started_at,
            completed_at: None,
            status: ExecutionStatus::Running,
            rule_result,
            actions_executed: Vec::new(),
            error: None,
        };

        for action in &handler.actions {
            match self.execute_action(action, event, handler).await {
                Ok(action_name) => {
                    execution.actions_executed.push(action_name);
                }
                Err(e) => {
                    execution.error = Some(e.to_string());
                    execution.status = ExecutionStatus::Failed;
                    break;
                }
            }
        }

        if execution.error.is_none() {
            execution.status = ExecutionStatus::Completed;
        }
        execution.completed_at = Some(chrono::Utc::now());

        tracing::info!(
            handler_id = %handler.id,
            execution_id = %execution_id,
            status = ?execution.status,
            actions_count = execution.actions_executed.len(),
            "Executed conditional handler"
        );

        Ok(execution)
    }

    async fn execute_action(
        &self,
        action: &HandlerAction,
        event: &TriggerEvent,
        handler: &ConditionalHandler,
    ) -> Result<String, WebRTCError> {
        match action {
            HandlerAction::ExecuteFunction { function_name, parameters } => {
                // Convert to trigger event and execute through function trigger manager
                let mut trigger_event = event.clone();

                // Add action parameters to event data
                if let serde_json::Value::Object(ref mut map) = &mut trigger_event.event_data {
                    map.insert("function_parameters".to_string(), serde_json::json!(parameters));
                    map.insert("triggered_by_handler".to_string(), serde_json::json!(handler.id));
                }

                self.function_trigger_manager.execute_triggers(&trigger_event).await;
                Ok(format!("ExecuteFunction({})", function_name))
            }
            HandlerAction::SendNotification { notification_type, recipients, template } => {
                tracing::info!(
                    handler_id = %handler.id,
                    notification_type = %notification_type,
                    recipient_count = recipients.len(),
                    "Sending notification (stubbed)"
                );
                Ok(format!("SendNotification({})", notification_type))
            }
            HandlerAction::TriggerWebhook { url, method, headers, payload } => {
                tracing::info!(
                    handler_id = %handler.id,
                    webhook_url = %url,
                    method = %method,
                    "Triggering webhook (stubbed)"
                );
                Ok(format!("TriggerWebhook({})", url))
            }
            HandlerAction::ModifyData { target, modifications } => {
                tracing::info!(
                    handler_id = %handler.id,
                    target = %target,
                    modification_count = modifications.len(),
                    "Modifying data (stubbed)"
                );
                Ok(format!("ModifyData({})", target))
            }
            HandlerAction::StopProcessing => {
                tracing::info!(
                    handler_id = %handler.id,
                    "Stopping event processing"
                );
                Ok("StopProcessing".to_string())
            }
            HandlerAction::DelayExecution { delay_seconds, action } => {
                tracing::info!(
                    handler_id = %handler.id,
                    delay_seconds = %delay_seconds,
                    "Delaying action execution (stubbed)"
                );
                Ok(format!("DelayExecution({}s)", delay_seconds))
            }
        }
    }

    /// Get all conditional handlers for an organization
    pub async fn get_org_handlers(&self, org_id: u64) -> Vec<ConditionalHandler> {
        self.handlers
            .read()
            .await
            .values()
            .filter(|h| h.org_id == org_id)
            .cloned()
            .collect()
    }

    /// Get execution history for a handler
    pub async fn get_execution_history(&self, handler_id: &str) -> Vec<HandlerExecution> {
        self.execution_history
            .read()
            .await
            .get(handler_id)
            .cloned()
            .unwrap_or_default()
    }
}

/// Request to create a conditional handler
#[derive(Debug, Deserialize)]
pub struct CreateConditionalHandlerRequest {
    pub name: String,
    pub project_id: Option<String>,
    pub rule_set: RuleSet,
    pub actions: Vec<HandlerAction>,
    pub enabled: Option<bool>,
    pub priority: Option<HandlerPriority>,
    pub metadata: Option<ConditionalHandlerMetadata>,
}

/// Request to update a conditional handler
#[derive(Debug, Deserialize)]
pub struct UpdateConditionalHandlerRequest {
    pub rule_set: Option<RuleSet>,
    pub actions: Option<Vec<HandlerAction>>,
    pub enabled: Option<bool>,
    pub priority: Option<HandlerPriority>,
    pub metadata: Option<ConditionalHandlerMetadata>,
}