//! Change Data Capture (CDC) for database operations
//!
//! This module provides comprehensive change tracking for all database operations,
//! capturing before/after values, transaction context, and user information for
//! audit trails and real-time data streaming.

use std::collections::HashMap;
use std::sync::Arc;
use chrono::Utc;
use serde_json::Value as JsonValue;
use zevents::{Event, EventData, EventType, EventMetadata};
use anyhow::Result;

/// Context information for database operations
#[derive(Debug, Clone)]
pub struct OperationContext {
    pub org_id: u64,
    pub user_id: Option<u64>,
    pub connection_id: Option<String>,
    pub transaction_id: Option<String>,
}

/// Change data capture for a single row operation
#[derive(Debug, Clone)]
pub struct RowChange {
    pub table_name: String,
    pub operation: RowOperation,
    pub old_values: Option<HashMap<String, JsonValue>>,
    pub new_values: Option<HashMap<String, JsonValue>>,
    pub where_clause: Option<String>,
    pub context: OperationContext,
}

/// Type of row operation
#[derive(Debug, Clone)]
pub enum RowOperation {
    Insert,
    Update,
    Delete,
}

impl RowOperation {
    fn as_str(&self) -> &'static str {
        match self {
            RowOperation::Insert => "INSERT",
            RowOperation::Update => "UPDATE",
            RowOperation::Delete => "DELETE",
        }
    }
}

/// Schema change tracking
#[derive(Debug, Clone)]
pub struct SchemaChange {
    pub table_name: String,
    pub operation: SchemaOperation,
    pub old_schema: Option<JsonValue>,
    pub new_schema: Option<JsonValue>,
    pub ddl_statement: String,
    pub context: OperationContext,
}

/// Type of schema operation
#[derive(Debug, Clone)]
pub enum SchemaOperation {
    CreateTable,
    AlterTable,
    DropTable,
    CreateIndex,
    DropIndex,
}

impl SchemaOperation {
    fn as_str(&self) -> &'static str {
        match self {
            SchemaOperation::CreateTable => "CREATE_TABLE",
            SchemaOperation::AlterTable => "ALTER_TABLE",
            SchemaOperation::DropTable => "DROP_TABLE",
            SchemaOperation::CreateIndex => "CREATE_INDEX",
            SchemaOperation::DropIndex => "DROP_INDEX",
        }
    }
}

/// Batch of related changes for efficient event publishing
#[derive(Debug, Clone)]
pub struct ChangeBatch {
    pub changes: Vec<ChangeEvent>,
    pub transaction_id: Option<String>,
    pub batch_id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Unified change event
#[derive(Debug, Clone)]
pub enum ChangeEvent {
    Row(RowChange),
    Schema(SchemaChange),
}

/// Event filtering configuration
#[derive(Debug, Clone)]
pub struct EventFilter {
    pub tables: Option<Vec<String>>,
    pub operations: Option<Vec<String>>,
    pub users: Option<Vec<u64>>,
    pub organizations: Option<Vec<u64>>,
}

impl EventFilter {
    pub fn new() -> Self {
        Self {
            tables: None,
            operations: None,
            users: None,
            organizations: None,
        }
    }

    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.tables = Some(tables);
        self
    }

    pub fn with_operations(mut self, operations: Vec<String>) -> Self {
        self.operations = Some(operations);
        self
    }

    pub fn with_users(mut self, users: Vec<u64>) -> Self {
        self.users = Some(users);
        self
    }

    pub fn with_organizations(mut self, organizations: Vec<u64>) -> Self {
        self.organizations = Some(organizations);
        self
    }

    pub fn matches_row_change(&self, change: &RowChange) -> bool {
        // Check table filter
        if let Some(ref tables) = self.tables {
            if !tables.contains(&change.table_name) {
                return false;
            }
        }

        // Check operation filter
        if let Some(ref operations) = self.operations {
            if !operations.contains(&change.operation.as_str().to_string()) {
                return false;
            }
        }

        // Check user filter
        if let Some(ref users) = self.users {
            if let Some(user_id) = change.context.user_id {
                if !users.contains(&user_id) {
                    return false;
                }
            } else {
                return false; // No user ID but filter requires specific users
            }
        }

        // Check organization filter
        if let Some(ref orgs) = self.organizations {
            if !orgs.contains(&change.context.org_id) {
                return false;
            }
        }

        true
    }

    pub fn matches_schema_change(&self, change: &SchemaChange) -> bool {
        // Check table filter
        if let Some(ref tables) = self.tables {
            if !tables.contains(&change.table_name) {
                return false;
            }
        }

        // Check operation filter (schema operations)
        if let Some(ref operations) = self.operations {
            if !operations.contains(&change.operation.as_str().to_string()) {
                return false;
            }
        }

        // Check user filter
        if let Some(ref users) = self.users {
            if let Some(user_id) = change.context.user_id {
                if !users.contains(&user_id) {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Check organization filter
        if let Some(ref orgs) = self.organizations {
            if !orgs.contains(&change.context.org_id) {
                return false;
            }
        }

        true
    }
}

/// Change data capture manager
pub struct ChangeDataCapture {
    event_publisher: Option<Arc<dyn EventPublisher>>,
    batching_config: BatchingConfig,
    current_batch: parking_lot::Mutex<Option<ChangeBatch>>,
}

impl std::fmt::Debug for ChangeDataCapture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeDataCapture")
            .field("event_publisher", &self.event_publisher.as_ref().map(|_| "<EventPublisher>"))
            .field("batching_config", &self.batching_config)
            .field("current_batch", &"<Mutex>")
            .finish()
    }
}

impl Clone for ChangeDataCapture {
    fn clone(&self) -> Self {
        Self {
            event_publisher: self.event_publisher.clone(),
            batching_config: self.batching_config.clone(),
            current_batch: parking_lot::Mutex::new(None), // Cannot clone mutex content, so create empty
        }
    }
}

/// Batching configuration for change events
#[derive(Debug, Clone)]
pub struct BatchingConfig {
    pub max_batch_size: usize,
    pub max_batch_age_ms: u64,
    pub transaction_scoped: bool,
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            max_batch_age_ms: 1000,
            transaction_scoped: true,
        }
    }
}

/// Trait for publishing change events
#[async_trait::async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish_event(&self, event: Event) -> Result<()>;
    async fn publish_batch(&self, events: Vec<Event>) -> Result<()>;
}

impl ChangeDataCapture {
    pub fn new(
        event_publisher: Option<Arc<dyn EventPublisher>>,
        batching_config: BatchingConfig,
    ) -> Self {
        Self {
            event_publisher,
            batching_config,
            current_batch: parking_lot::Mutex::new(None),
        }
    }

    /// Capture a row change event
    pub async fn capture_row_change(&self, change: RowChange) -> Result<()> {
        let event_type = match change.operation {
            RowOperation::Insert => EventType::RowInserted,
            RowOperation::Update => EventType::RowUpdated,
            RowOperation::Delete => EventType::RowDeleted,
        };

        let event_data = EventData::DatabaseChange {
            org_id: change.context.org_id,
            table_name: change.table_name.clone(),
            operation: change.operation.as_str().to_string(),
            transaction_id: change.context.transaction_id.clone(),
            user_id: change.context.user_id,
            connection_id: change.context.connection_id.clone(),
            old_values: change.old_values.clone(),
            new_values: change.new_values.clone(),
            where_clause: change.where_clause.clone(),
            rows_affected: 1,
            schema_changes: None,
            timestamp_ms: Utc::now().timestamp_millis(),
        };

        let event = Event::new(event_type, event_data, change.context.org_id)
            .with_source("zexec-engine".to_string())
            .with_metadata(EventMetadata {
                source: "zexec-engine".to_string(),
                correlation_id: change.context.transaction_id.clone(),
                causation_id: None,
                version: "1.0".to_string(),
                attributes: {
                    let mut attrs = HashMap::new();
                    attrs.insert("table".to_string(), change.table_name.clone());
                    attrs.insert("operation".to_string(), change.operation.as_str().to_string());
                    if let Some(ref conn_id) = change.context.connection_id {
                        attrs.insert("connection_id".to_string(), conn_id.clone());
                    }
                    attrs
                },
            });

        let has_transaction_id = change.context.transaction_id.is_some();
        let is_transaction_scoped = self.batching_config.transaction_scoped && has_transaction_id;

        if is_transaction_scoped {
            self.add_to_batch(ChangeEvent::Row(change)).await?;
        } else {
            self.publish_immediately(event).await?;
        }

        Ok(())
    }

    /// Capture a schema change event
    pub async fn capture_schema_change(&self, change: SchemaChange) -> Result<()> {
        let event_data = EventData::DatabaseChange {
            org_id: change.context.org_id,
            table_name: change.table_name.clone(),
            operation: change.operation.as_str().to_string(),
            transaction_id: change.context.transaction_id.clone(),
            user_id: change.context.user_id,
            connection_id: change.context.connection_id.clone(),
            old_values: None,
            new_values: None,
            where_clause: None,
            rows_affected: 0,
            schema_changes: Some(serde_json::json!({
                "ddl_statement": change.ddl_statement,
                "old_schema": change.old_schema,
                "new_schema": change.new_schema
            })),
            timestamp_ms: Utc::now().timestamp_millis(),
        };

        let event = Event::new(EventType::SchemaChanged, event_data, change.context.org_id)
            .with_source("zexec-engine".to_string())
            .with_metadata(EventMetadata {
                source: "zexec-engine".to_string(),
                correlation_id: change.context.transaction_id.clone(),
                causation_id: None,
                version: "1.0".to_string(),
                attributes: {
                    let mut attrs = HashMap::new();
                    attrs.insert("table".to_string(), change.table_name.clone());
                    attrs.insert("operation".to_string(), change.operation.as_str().to_string());
                    attrs.insert("ddl_statement".to_string(), change.ddl_statement);
                    attrs
                },
            });

        self.publish_immediately(event).await?;
        Ok(())
    }

    /// Flush any pending batched changes
    pub async fn flush_batch(&self) -> Result<()> {
        let batch = {
            let mut current = self.current_batch.lock();
            current.take()
        };

        if let Some(batch) = batch {
            self.publish_batch(batch).await?;
        }

        Ok(())
    }

    async fn add_to_batch(&self, change: ChangeEvent) -> Result<()> {
        let mut current = self.current_batch.lock();

        if current.is_none() {
            *current = Some(ChangeBatch {
                changes: Vec::new(),
                transaction_id: self.extract_transaction_id(&change),
                batch_id: uuid::Uuid::new_v4().to_string(),
                created_at: Utc::now(),
            });
        }

        if let Some(ref mut batch) = *current {
            batch.changes.push(change);

            // Check if batch should be flushed
            if batch.changes.len() >= self.batching_config.max_batch_size {
                let completed_batch = current.take().unwrap();
                drop(current); // Release lock before async operation
                self.publish_batch(completed_batch).await?;
            }
        }

        Ok(())
    }

    fn extract_transaction_id(&self, change: &ChangeEvent) -> Option<String> {
        match change {
            ChangeEvent::Row(row_change) => row_change.context.transaction_id.clone(),
            ChangeEvent::Schema(schema_change) => schema_change.context.transaction_id.clone(),
        }
    }

    async fn publish_immediately(&self, event: Event) -> Result<()> {
        if let Some(ref publisher) = self.event_publisher {
            publisher.publish_event(event).await?;
        }
        Ok(())
    }

    async fn publish_batch(&self, batch: ChangeBatch) -> Result<()> {
        if let Some(ref publisher) = self.event_publisher {
            let events: Vec<Event> = batch.changes.into_iter().map(|change| {
                match change {
                    ChangeEvent::Row(row_change) => {
                        let event_type = match row_change.operation {
                            RowOperation::Insert => EventType::RowInserted,
                            RowOperation::Update => EventType::RowUpdated,
                            RowOperation::Delete => EventType::RowDeleted,
                        };

                        Event::new(
                            event_type,
                            EventData::DatabaseChange {
                                org_id: row_change.context.org_id,
                                table_name: row_change.table_name.clone(),
                                operation: row_change.operation.as_str().to_string(),
                                transaction_id: row_change.context.transaction_id.clone(),
                                user_id: row_change.context.user_id,
                                connection_id: row_change.context.connection_id.clone(),
                                old_values: row_change.old_values,
                                new_values: row_change.new_values,
                                where_clause: row_change.where_clause,
                                rows_affected: 1,
                                schema_changes: None,
                                timestamp_ms: Utc::now().timestamp_millis(),
                            },
                            row_change.context.org_id,
                        )
                    }
                    ChangeEvent::Schema(schema_change) => {
                        Event::new(
                            EventType::SchemaChanged,
                            EventData::DatabaseChange {
                                org_id: schema_change.context.org_id,
                                table_name: schema_change.table_name.clone(),
                                operation: schema_change.operation.as_str().to_string(),
                                transaction_id: schema_change.context.transaction_id.clone(),
                                user_id: schema_change.context.user_id,
                                connection_id: schema_change.context.connection_id.clone(),
                                old_values: None,
                                new_values: None,
                                where_clause: None,
                                rows_affected: 0,
                                schema_changes: Some(serde_json::json!({
                                    "ddl_statement": schema_change.ddl_statement,
                                    "old_schema": schema_change.old_schema,
                                    "new_schema": schema_change.new_schema
                                })),
                                timestamp_ms: Utc::now().timestamp_millis(),
                            },
                            schema_change.context.org_id,
                        )
                    }
                }
            }).collect();

            publisher.publish_batch(events).await?;
        }
        Ok(())
    }
}

/// Helper to extract values from Cell enum for change tracking
pub fn cell_to_json_value(cell: &crate::Cell) -> JsonValue {
    match cell {
        crate::Cell::Int(i) => JsonValue::Number((*i).into()),
        crate::Cell::Float(f) => {
            JsonValue::Number(serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0)))
        }
        crate::Cell::Text(s) => JsonValue::String(s.clone()),
        crate::Cell::Vector(v) => JsonValue::Array(
            v.iter().map(|f| JsonValue::Number(
                serde_json::Number::from_f64(*f as f64).unwrap_or(serde_json::Number::from(0))
            )).collect()
        ),
    }
}

/// Convert row data to JSON for change tracking
pub fn row_to_json_values(columns: &[crate::Column], row: &[crate::Cell]) -> HashMap<String, JsonValue> {
    let mut values = HashMap::new();

    for (i, cell) in row.iter().enumerate() {
        if let Some(column) = columns.get(i) {
            values.insert(column.name.clone(), cell_to_json_value(cell));
        }
    }

    values
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Debug, Default)]
    struct TestEventPublisher {
        events: Arc<Mutex<Vec<Event>>>,
    }

    #[async_trait::async_trait]
    impl EventPublisher for TestEventPublisher {
        async fn publish_event(&self, event: Event) -> Result<()> {
            self.events.lock().await.push(event);
            Ok(())
        }

        async fn publish_batch(&self, events: Vec<Event>) -> Result<()> {
            self.events.lock().await.extend(events);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_row_change_capture() {
        let publisher = Arc::new(TestEventPublisher::default());
        let cdc = ChangeDataCapture::new(Some(publisher.clone()), BatchingConfig::default());

        let context = OperationContext {
            org_id: 1,
            user_id: Some(100),
            connection_id: Some("conn123".to_string()),
            transaction_id: Some("txn456".to_string()),
        };

        let mut old_values = HashMap::new();
        old_values.insert("name".to_string(), JsonValue::String("John".to_string()));

        let mut new_values = HashMap::new();
        new_values.insert("name".to_string(), JsonValue::String("Jane".to_string()));

        let change = RowChange {
            table_name: "users".to_string(),
            operation: RowOperation::Update,
            old_values: Some(old_values),
            new_values: Some(new_values),
            where_clause: Some("id = 1".to_string()),
            context,
        };

        cdc.capture_row_change(change).await.unwrap();
        cdc.flush_batch().await.unwrap();

        let events = publisher.events.lock().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, EventType::RowUpdated);
    }

    #[tokio::test]
    async fn test_event_filtering() {
        let filter = EventFilter::new()
            .with_tables(vec!["users".to_string()])
            .with_operations(vec!["UPDATE".to_string()]);

        let context = OperationContext {
            org_id: 1,
            user_id: Some(100),
            connection_id: None,
            transaction_id: None,
        };

        let update_change = RowChange {
            table_name: "users".to_string(),
            operation: RowOperation::Update,
            old_values: None,
            new_values: None,
            where_clause: None,
            context: context.clone(),
        };

        let insert_change = RowChange {
            table_name: "users".to_string(),
            operation: RowOperation::Insert,
            old_values: None,
            new_values: None,
            where_clause: None,
            context,
        };

        assert!(filter.matches_row_change(&update_change));
        assert!(!filter.matches_row_change(&insert_change));
    }
}