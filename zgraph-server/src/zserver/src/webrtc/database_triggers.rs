use super::{TriggerEvent, TriggerEventType, TriggerContext, FunctionTriggerManager, WebRTCError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Database trigger system for executing functions on database changes
#[derive(Debug, Clone)]
pub struct DatabaseTrigger {
    pub id: String,
    pub name: String,
    pub org_id: u64,
    pub project_id: Option<String>,
    pub trigger_config: DatabaseTriggerConfig,
    pub function_name: String,
    pub enabled: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub metadata: DatabaseTriggerMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseTriggerConfig {
    /// Tables to monitor for changes
    pub tables: Vec<String>,
    /// Types of operations to trigger on
    pub operations: Vec<DatabaseOperation>,
    /// Optional conditions for triggering
    pub conditions: Option<DatabaseTriggerConditions>,
    /// Whether to include old/new row data in the event
    pub include_row_data: bool,
    /// Batch settings for high-frequency changes
    pub batch_settings: Option<BatchSettings>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DatabaseOperation {
    Insert,
    Update,
    Delete,
    Truncate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseTriggerConditions {
    /// Specific columns that must change (for UPDATE operations)
    pub changed_columns: Option<Vec<String>>,
    /// SQL WHERE clause for filtering rows
    pub where_clause: Option<String>,
    /// Row count thresholds
    pub min_affected_rows: Option<u32>,
    pub max_affected_rows: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSettings {
    /// Maximum time to wait before executing trigger (milliseconds)
    pub max_wait_ms: u32,
    /// Maximum number of changes to batch together
    pub max_batch_size: u32,
    /// Whether to deduplicate changes to the same row
    pub deduplicate_rows: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseTriggerMetadata {
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub priority: TriggerPriority,
    pub timeout_seconds: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TriggerPriority {
    Low,
    Normal,
    High,
    Critical,
}

impl Default for DatabaseTriggerMetadata {
    fn default() -> Self {
        Self {
            description: None,
            tags: Vec::new(),
            priority: TriggerPriority::Normal,
            timeout_seconds: Some(300),
        }
    }
}

/// Database change event
#[derive(Debug, Clone)]
pub struct DatabaseChangeEvent {
    pub table_name: String,
    pub operation: DatabaseOperation,
    pub affected_rows: u32,
    pub old_row: Option<serde_json::Value>,
    pub new_row: Option<serde_json::Value>,
    pub changed_columns: Vec<String>,
    pub transaction_id: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub user_context: Option<DatabaseUserContext>,
}

#[derive(Debug, Clone)]
pub struct DatabaseUserContext {
    pub user_id: Option<String>,
    pub org_id: u64,
    pub session_id: Option<String>,
    pub connection_info: HashMap<String, String>,
}

/// Manager for database triggers
pub struct DatabaseTriggerManager {
    triggers: Arc<RwLock<HashMap<String, DatabaseTrigger>>>,
    function_trigger_manager: Arc<FunctionTriggerManager>,
    pending_batches: Arc<RwLock<HashMap<String, PendingBatch>>>,
}

#[derive(Debug)]
struct PendingBatch {
    trigger_id: String,
    changes: Vec<DatabaseChangeEvent>,
    batch_start: chrono::DateTime<chrono::Utc>,
    settings: BatchSettings,
}

impl DatabaseTriggerManager {
    pub fn new(function_trigger_manager: Arc<FunctionTriggerManager>) -> Self {
        Self {
            triggers: Arc::new(RwLock::new(HashMap::new())),
            function_trigger_manager,
            pending_batches: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new database trigger
    pub async fn register_trigger(&self, trigger: DatabaseTrigger) -> Result<(), WebRTCError> {
        let mut triggers = self.triggers.write().await;
        triggers.insert(trigger.id.clone(), trigger.clone());

        tracing::info!(
            trigger_id = %trigger.id,
            trigger_name = %trigger.name,
            tables = ?trigger.trigger_config.tables,
            operations = ?trigger.trigger_config.operations,
            "Registered database trigger"
        );

        Ok(())
    }

    /// Remove a database trigger
    pub async fn remove_trigger(&self, trigger_id: &str) -> Result<(), WebRTCError> {
        let mut triggers = self.triggers.write().await;
        if triggers.remove(trigger_id).is_some() {
            // Clean up any pending batches
            self.pending_batches.write().await.remove(trigger_id);

            tracing::info!(
                trigger_id = %trigger_id,
                "Removed database trigger"
            );
            Ok(())
        } else {
            Err(WebRTCError::SessionNotFound)
        }
    }

    /// Process a database change event
    pub async fn on_database_change(
        &self,
        change_event: DatabaseChangeEvent,
    ) -> Result<(), WebRTCError> {
        let triggers = self.triggers.read().await;

        for trigger in triggers.values() {
            if !trigger.enabled {
                continue;
            }

            if self.matches_database_trigger(trigger, &change_event).await {
                if let Some(batch_settings) = &trigger.trigger_config.batch_settings {
                    // Handle batching
                    self.handle_batched_trigger(trigger, &change_event, batch_settings.clone()).await?;
                } else {
                    // Execute immediately
                    self.execute_database_trigger(trigger, vec![change_event.clone()]).await?;
                }
            }
        }

        Ok(())
    }

    async fn matches_database_trigger(
        &self,
        trigger: &DatabaseTrigger,
        change_event: &DatabaseChangeEvent,
    ) -> bool {
        let config = &trigger.trigger_config;

        // Check table match
        if !config.tables.is_empty() && !config.tables.contains(&change_event.table_name) {
            return false;
        }

        // Check operation match
        if !config.operations.contains(&change_event.operation) {
            return false;
        }

        // Check conditions
        if let Some(conditions) = &config.conditions {
            if let Some(min_rows) = conditions.min_affected_rows {
                if change_event.affected_rows < min_rows {
                    return false;
                }
            }

            if let Some(max_rows) = conditions.max_affected_rows {
                if change_event.affected_rows > max_rows {
                    return false;
                }
            }

            if let Some(changed_columns) = &conditions.changed_columns {
                if !changed_columns.iter().any(|col| change_event.changed_columns.contains(col)) {
                    return false;
                }
            }

            // TODO: Implement WHERE clause evaluation
            if conditions.where_clause.is_some() {
                // For now, skip WHERE clause evaluation
                tracing::debug!("WHERE clause evaluation not yet implemented, allowing trigger");
            }
        }

        // Check organization isolation
        if let Some(user_context) = &change_event.user_context {
            if user_context.org_id != trigger.org_id {
                return false;
            }
        }

        true
    }

    async fn handle_batched_trigger(
        &self,
        trigger: &DatabaseTrigger,
        change_event: &DatabaseChangeEvent,
        batch_settings: BatchSettings,
    ) -> Result<(), WebRTCError> {
        let mut pending_batches = self.pending_batches.write().await;
        let trigger_id = trigger.id.clone();

        if let Some(batch) = pending_batches.get_mut(&trigger_id) {
            // Add to existing batch
            if batch_settings.deduplicate_rows {
                // Remove previous changes to the same row if deduplication is enabled
                batch.changes.retain(|existing| {
                    existing.table_name != change_event.table_name ||
                    existing.new_row != change_event.new_row
                });
            }

            batch.changes.push(change_event.clone());

            // Check if batch should be executed
            let should_execute = batch.changes.len() >= batch_settings.max_batch_size as usize ||
                chrono::Utc::now().signed_duration_since(batch.batch_start).num_milliseconds() >= batch_settings.max_wait_ms as i64;

            if should_execute {
                let changes = batch.changes.clone();
                pending_batches.remove(&trigger_id);
                drop(pending_batches);

                self.execute_database_trigger(trigger, changes).await?;
            }
        } else {
            // Create new batch
            let new_batch = PendingBatch {
                trigger_id: trigger_id.clone(),
                changes: vec![change_event.clone()],
                batch_start: chrono::Utc::now(),
                settings: batch_settings.clone(),
            };

            pending_batches.insert(trigger_id.clone(), new_batch);
            drop(pending_batches);

            // Set up timer to flush batch if needed
            let trigger_manager = Arc::downgrade(&self.function_trigger_manager);
            let pending_batches_weak = Arc::downgrade(&self.pending_batches);
            let trigger_clone = trigger.clone();

            tokio::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(batch_settings.max_wait_ms as u64)).await;

                if let (Some(_), Some(pending_batches)) = (trigger_manager.upgrade(), pending_batches_weak.upgrade()) {
                    if let Some(batch) = pending_batches.write().await.remove(&trigger_id) {
                        if let Err(e) = Self::execute_database_trigger_static(&trigger_clone, batch.changes, trigger_manager.upgrade().unwrap()).await {
                            tracing::error!(
                                trigger_id = %trigger_id,
                                error = %e,
                                "Failed to execute batched database trigger"
                            );
                        }
                    }
                }
            });
        }

        Ok(())
    }

    async fn execute_database_trigger(
        &self,
        trigger: &DatabaseTrigger,
        changes: Vec<DatabaseChangeEvent>,
    ) -> Result<(), WebRTCError> {
        Self::execute_database_trigger_static(trigger, changes, self.function_trigger_manager.clone()).await
    }

    async fn execute_database_trigger_static(
        trigger: &DatabaseTrigger,
        changes: Vec<DatabaseChangeEvent>,
        function_trigger_manager: Arc<FunctionTriggerManager>,
    ) -> Result<(), WebRTCError> {
        // Convert database change to trigger event
        let first_change = &changes[0];
        let user_context = first_change.user_context.as_ref();

        let trigger_event = TriggerEvent {
            event_type: TriggerEventType::DatabaseChange {
                table_name: first_change.table_name.clone(),
                operation: first_change.operation.clone(),
                changes: changes.clone(),
            },
            event_data: serde_json::json!({
                "table_name": first_change.table_name,
                "operation": first_change.operation,
                "change_count": changes.len(),
                "batched": changes.len() > 1,
                "changes": changes.iter().map(|c| serde_json::json!({
                    "affected_rows": c.affected_rows,
                    "changed_columns": c.changed_columns,
                    "old_row": c.old_row,
                    "new_row": c.new_row,
                    "timestamp": c.timestamp,
                })).collect::<Vec<_>>(),
            }),
            context: TriggerContext {
                user_id: user_context.and_then(|ctx| ctx.user_id.clone()).unwrap_or_default(),
                org_id: user_context.map(|ctx| ctx.org_id).unwrap_or(trigger.org_id),
                project_id: trigger.project_id.clone(),
                room_id: None,
                session_id: user_context.and_then(|ctx| ctx.session_id.clone()),
                event_data: serde_json::json!({
                    "trigger_type": "database_change",
                    "batch_size": changes.len(),
                    "transaction_id": first_change.transaction_id,
                }),
                metadata: {
                    let mut meta = HashMap::new();
                    meta.insert("event_source".to_string(), "database_trigger".to_string());
                    meta.insert("table_name".to_string(), first_change.table_name.clone());
                    meta.insert("operation".to_string(), format!("{:?}", first_change.operation));
                    if changes.len() > 1 {
                        meta.insert("batched".to_string(), "true".to_string());
                        meta.insert("batch_size".to_string(), changes.len().to_string());
                    }
                    meta
                },
            },
            participant_count: None,
            timestamp: chrono::Utc::now(),
        };

        // Execute the trigger
        function_trigger_manager.execute_triggers(&trigger_event).await;

        tracing::info!(
            trigger_id = %trigger.id,
            function_name = %trigger.function_name,
            table_name = %first_change.table_name,
            change_count = changes.len(),
            "Executed database trigger"
        );

        Ok(())
    }

    /// Get all database triggers for an organization
    pub async fn get_org_triggers(&self, org_id: u64) -> Vec<DatabaseTrigger> {
        self.triggers
            .read()
            .await
            .values()
            .filter(|t| t.org_id == org_id)
            .cloned()
            .collect()
    }

    /// Flush all pending batches (useful for shutdown)
    pub async fn flush_all_batches(&self) -> Result<(), WebRTCError> {
        let mut pending_batches = self.pending_batches.write().await;
        let triggers = self.triggers.read().await;

        for (trigger_id, batch) in pending_batches.drain() {
            if let Some(trigger) = triggers.get(&trigger_id) {
                self.execute_database_trigger(trigger, batch.changes).await?;
            }
        }

        Ok(())
    }
}

/// Request to create a database trigger
#[derive(Debug, Deserialize)]
pub struct CreateDatabaseTriggerRequest {
    pub name: String,
    pub project_id: Option<String>,
    pub trigger_config: DatabaseTriggerConfig,
    pub function_name: String,
    pub enabled: Option<bool>,
    pub metadata: Option<DatabaseTriggerMetadata>,
}

// Add to TriggerEventType enum (we need to extend the existing enum)
impl super::TriggerEventType {
    pub fn is_database_change(&self) -> bool {
        matches!(self, super::TriggerEventType::DatabaseChange { .. })
    }
}

// We need to add this variant to the existing TriggerEventType enum in functions.rs
#[derive(Debug, Clone)]
pub enum DatabaseTriggerEventType {
    DatabaseChange {
        table_name: String,
        operation: DatabaseOperation,
        changes: Vec<DatabaseChangeEvent>,
    },
}