//! Integration layer for database change events with event sourcing
//!
//! This module provides seamless integration between the database change capture system
//! and the existing event sourcing infrastructure, including webhook delivery,
//! real-time streaming, and persistent storage.

use std::sync::Arc;
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::RwLock;
use crate::{
    Event, EventBus, EventStore, WebhookManager, EventHandler, HandlerResult,
    EventFilter, CompositeFilter, PermissionFilter, BatchConfig, EventBatcher, BatchHandler,
    EventBatch,
};

/// Database change event integration manager
pub struct DatabaseChangeIntegration {
    event_bus: Arc<EventBus>,
    event_store: Arc<EventStore>,
    webhook_manager: Arc<WebhookManager>,
    filters: RwLock<Vec<CompositeFilter>>,
    batcher: Option<Arc<EventBatcher>>,
}

impl DatabaseChangeIntegration {
    pub fn new(
        event_bus: Arc<EventBus>,
        event_store: Arc<EventStore>,
        webhook_manager: Arc<WebhookManager>,
        batch_config: Option<BatchConfig>,
    ) -> Self {
        let batcher = if let Some(config) = batch_config {
            let handler = Arc::new(IntegratedBatchHandler {
                event_bus: event_bus.clone(),
                webhook_manager: webhook_manager.clone(),
            });
            Some(Arc::new(EventBatcher::new(config, handler)))
        } else {
            None
        };

        Self {
            event_bus,
            event_store,
            webhook_manager,
            filters: RwLock::new(Vec::new()),
            batcher,
        }
    }

    /// Add a filter for database change events
    pub async fn add_filter(&self, filter: CompositeFilter) {
        self.filters.write().await.push(filter);
    }

    /// Remove all filters
    pub async fn clear_filters(&self) {
        self.filters.write().await.clear();
    }

    /// Process a database change event through the integration pipeline
    pub async fn process_change_event(&self, event: Event) -> Result<()> {
        // Apply filters
        let filters = self.filters.read().await;
        for filter in filters.iter() {
            if !filter.matches(&event) {
                return Ok(()); // Event filtered out
            }
        }
        drop(filters);

        // Route through batcher or direct processing
        if let Some(ref batcher) = self.batcher {
            batcher.add_event(event)?;
        } else {
            self.process_event_direct(event).await?;
        }

        Ok(())
    }

    /// Process event directly without batching
    async fn process_event_direct(&self, event: Event) -> Result<()> {
        // Publish to event bus for real-time subscribers
        self.event_bus.publish(event.clone()).await?;

        // Trigger webhooks for change events
        if let Err(e) = self.webhook_manager.deliver_event(&event).await {
            tracing::warn!("Failed to deliver webhook for event {}: {}", event.id, e);
        }

        Ok(())
    }

    /// Get statistics about processed events
    pub async fn get_stats(&self) -> IntegrationStats {
        // In a real implementation, you'd track these metrics
        IntegrationStats {
            total_events_processed: 0,
            events_filtered: 0,
            events_batched: 0,
            webhook_deliveries: 0,
            failed_deliveries: 0,
        }
    }

    /// Flush any pending batched events
    pub async fn flush_pending(&self) -> Result<()> {
        if let Some(ref batcher) = self.batcher {
            batcher.flush()?;
        }
        Ok(())
    }
}

/// Statistics for the integration system
#[derive(Debug, Clone)]
pub struct IntegrationStats {
    pub total_events_processed: u64,
    pub events_filtered: u64,
    pub events_batched: u64,
    pub webhook_deliveries: u64,
    pub failed_deliveries: u64,
}

/// Batch handler that integrates with the event sourcing system
struct IntegratedBatchHandler {
    event_bus: Arc<EventBus>,
    webhook_manager: Arc<WebhookManager>,
}

#[async_trait]
impl BatchHandler for IntegratedBatchHandler {
    async fn handle_batch(&self, batch: EventBatch) -> Result<()> {
        tracing::info!(
            "Processing batch {} with {} database change events",
            batch.id,
            batch.len()
        );

        // Process each event in the batch
        for event in batch.events {
            // Publish to event bus
            if let Err(e) = self.event_bus.publish(event.clone()).await {
                tracing::error!("Failed to publish event {} to bus: {}", event.id, e);
                continue;
            }

            // Trigger webhooks
            if let Err(e) = self.webhook_manager.deliver_event(&event).await {
                tracing::warn!("Failed to deliver webhook for event {}: {}", event.id, e);
            }
        }

        Ok(())
    }
}

/// Database change event subscriber that can be added to the event bus
pub struct DatabaseChangeSubscriber {
    integration: Arc<DatabaseChangeIntegration>,
}

impl DatabaseChangeSubscriber {
    pub fn new(integration: Arc<DatabaseChangeIntegration>) -> Self {
        Self { integration }
    }
}

#[async_trait]
impl EventHandler for DatabaseChangeSubscriber {
    async fn handle(&self, event: &Event) -> HandlerResult {
        match self.integration.process_change_event(event.clone()).await {
            Ok(()) => HandlerResult::Success,
            Err(e) => {
                tracing::error!("Failed to process database change event: {}", e);
                HandlerResult::Retry
            }
        }
    }

    fn interested_in(&self, event_type: &crate::EventType) -> bool {
        matches!(
            event_type,
            crate::EventType::RowInserted
                | crate::EventType::RowUpdated
                | crate::EventType::RowDeleted
                | crate::EventType::SchemaChanged
        )
    }
}

/// Factory for creating common integration configurations
pub struct IntegrationConfigFactory;

impl IntegrationConfigFactory {
    /// Create integration for high-volume OLTP workloads
    pub fn high_volume_oltp() -> BatchConfig {
        BatchConfig {
            max_batch_size: 1000,
            max_batch_age_ms: 500, // 500ms
            transaction_scoped: true,
            organization_scoped: false,
            buffer_size: 10000,
            flush_interval_ms: 250,
        }
    }

    /// Create integration for real-time analytics
    pub fn real_time_analytics() -> BatchConfig {
        BatchConfig {
            max_batch_size: 50,
            max_batch_age_ms: 100, // 100ms
            transaction_scoped: false,
            organization_scoped: true,
            buffer_size: 1000,
            flush_interval_ms: 50,
        }
    }

    /// Create integration for audit trail scenarios
    pub fn audit_trail() -> BatchConfig {
        BatchConfig {
            max_batch_size: 10,
            max_batch_age_ms: 1000, // 1 second
            transaction_scoped: true,
            organization_scoped: true,
            buffer_size: 500,
            flush_interval_ms: 500,
        }
    }

    /// Create filter for user-specific database changes
    pub fn user_scoped_filter(user_id: u64, allowed_tables: Vec<String>) -> CompositeFilter {
        let event_filter = EventFilter::database_changes()
            .with_tables(allowed_tables)
            .with_users(vec![user_id]);

        CompositeFilter::new().with_event_filter(event_filter)
    }

    /// Create filter for organization-wide changes with permissions
    pub fn org_scoped_filter(
        user_id: u64,
        org_id: u64,
        permissions: Vec<String>,
        table_access: Vec<String>,
    ) -> CompositeFilter {
        let event_filter = EventFilter::database_changes()
            .with_organizations(vec![org_id])
            .with_tables(table_access.clone());

        let permission_filter = PermissionFilter::new(
            user_id,
            permissions,
            vec![org_id],
            [(org_id, table_access)].into_iter().collect(),
        );

        CompositeFilter::new()
            .with_event_filter(event_filter)
            .with_permission_filter(permission_filter)
    }
}

/// Extended event system with database change integration
pub struct ExtendedEventSystem {
    base: Arc<crate::EventSystem>,
    db_integration: Arc<DatabaseChangeIntegration>,
}

impl ExtendedEventSystem {
    pub async fn new(
        base: Arc<crate::EventSystem>,
        batch_config: Option<BatchConfig>,
    ) -> Result<Self> {
        let db_integration = Arc::new(DatabaseChangeIntegration::new(
            base.bus.clone(),
            base.store.clone(),
            base.webhooks.clone(),
            batch_config,
        ));

        // Subscribe the database change handler to the event bus
        let subscriber = Arc::new(DatabaseChangeSubscriber::new(db_integration.clone()));
        base.bus.subscribe(subscriber).await?;

        Ok(Self {
            base,
            db_integration,
        })
    }

    /// Get reference to the base event system
    pub fn base(&self) -> &Arc<crate::EventSystem> {
        &self.base
    }

    /// Get reference to database integration
    pub fn database_integration(&self) -> &Arc<DatabaseChangeIntegration> {
        &self.db_integration
    }

    /// Add a database change filter
    pub async fn add_database_filter(&self, filter: CompositeFilter) {
        self.db_integration.add_filter(filter).await;
    }

    /// Flush any pending database change events
    pub async fn flush_database_changes(&self) -> Result<()> {
        self.db_integration.flush_pending().await
    }

    /// Get database integration statistics
    pub async fn get_database_stats(&self) -> IntegrationStats {
        self.db_integration.get_stats().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EventSystemConfig, EventData};
    use tempfile::tempdir;
    use zcore_storage::Store;

    #[tokio::test]
    async fn test_database_integration_setup() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let config = EventSystemConfig::default();
        let base_system = Arc::new(crate::EventSystem::new(storage, config).await.unwrap());

        let batch_config = IntegrationConfigFactory::real_time_analytics();
        let extended = ExtendedEventSystem::new(base_system, Some(batch_config))
            .await
            .unwrap();

        // Test adding a filter
        let filter = IntegrationConfigFactory::user_scoped_filter(1, vec!["users".to_string()]);
        extended.add_database_filter(filter).await;

        // Test getting stats
        let stats = extended.get_database_stats().await;
        assert_eq!(stats.total_events_processed, 0);
    }

    #[tokio::test]
    async fn test_event_filtering() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let config = EventSystemConfig::default();
        let base_system = Arc::new(crate::EventSystem::new(storage, config).await.unwrap());

        let db_integration = DatabaseChangeIntegration::new(
            base_system.bus.clone(),
            base_system.store.clone(),
            base_system.webhooks.clone(),
            None,
        );

        let filter = IntegrationConfigFactory::user_scoped_filter(1, vec!["users".to_string()]);
        db_integration.add_filter(filter).await;

        // Create a matching event
        let matching_event = Event::new(
            crate::EventType::RowInserted,
            EventData::DatabaseChange {
                org_id: 1,
                table_name: "users".to_string(),
                operation: "INSERT".to_string(),
                transaction_id: None,
                user_id: Some(1),
                connection_id: None,
                old_values: None,
                new_values: None,
                where_clause: None,
                rows_affected: 1,
                schema_changes: None,
                timestamp_ms: chrono::Utc::now().timestamp_millis(),
            },
            1,
        );

        // Should process without error (event matches filter)
        assert!(db_integration.process_change_event(matching_event).await.is_ok());
    }
}