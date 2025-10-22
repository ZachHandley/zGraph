pub mod batching;
pub mod bus;
pub mod consumer;
pub mod events;
pub mod exactly_once_bus;
pub mod filtering;
pub mod handlers;
pub mod hooks;
pub mod integration;
pub mod metrics;
pub mod persistent_queue;
pub mod scheduler;
pub mod sourcing;
pub mod transaction;
pub mod triggers;
pub mod unified_streaming;

#[cfg(test)]
pub mod exactly_once_integration_tests;

#[cfg(test)]
pub mod enhanced_bus_tests;

#[cfg(test)]
pub mod enhanced_sourcing_tests;

#[cfg(test)]
pub mod webhook_integration_tests;

#[cfg(test)]
pub mod enhanced_trigger_tests;

#[cfg(test)]
pub mod persistence_tests;

pub use batching::{BatchConfig, EventBatch, EventBatcher, BatchHandler};
pub use bus::{EventBus, EventBusConfig};
pub use consumer::{PersistentConsumer, ConsumerConfig, ConsumerGroup, ConsumerStats};
pub use events::{Event, EventData, EventId, EventMetadata, EventType};
pub use exactly_once_bus::{ExactlyOnceEventBus, ExactlyOnceEventBusConfig, ExactlyOnceEventBusBuilder, ExactlyOnceEventBusStats};
pub use persistent_queue::{PersistentMessageQueue, PersistentQueueConfig, PersistentMessage, PersistentQueueStats};
pub use filtering::{EventFilter, PermissionFilter, CompositeFilter};
pub use handlers::{EventHandler, HandlerError, HandlerResult};
pub use integration::{
    DatabaseChangeIntegration, ExtendedEventSystem, IntegrationConfigFactory, IntegrationStats,
};
pub use metrics::{EventMetricsCollector, EventMetrics, EventAlert, AlertConfig, MetricsWrappedHandler};
pub use hooks::{
    WebhookConfig, WebhookDelivery, WebhookEndpoint, WebhookManager, WebhookSignature,
    WebhookStatus,
};
pub use scheduler::{CronScheduler, ScheduledTask, TaskConfig};
pub use sourcing::{EventStore, EventStream, Snapshot};
pub use transaction::{TransactionEventManager, TransactionData, TransactionState, TransactionOperation, EventPublisher};
pub use triggers::{TriggerCondition, TriggerConfig, TriggerManager};
pub use unified_streaming::{UnifiedEventStreamer, UnifiedStream, UnifiedEvent, StreamConfig, SystemIntegration};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use zcore_storage::Store;

/// Main event system coordinator
#[derive(Clone)]
pub struct EventSystem {
    pub bus: Arc<EventBus>,
    pub store: Arc<EventStore>,
    pub webhooks: Arc<WebhookManager>,
    pub scheduler: Arc<CronScheduler>,
    pub triggers: Arc<TriggerManager>,
    pub transactions: Arc<TransactionEventManager>,
    pub metrics: Arc<EventMetricsCollector>,
    pub unified_streamer: Arc<UnifiedEventStreamer>,
    storage: Arc<Store>,
}

impl EventSystem {
    pub async fn new(storage: Arc<Store>, config: EventSystemConfig) -> Result<Self> {
        let bus_config = EventBusConfig {
            buffer_size: config.bus_buffer_size,
            dead_letter_enabled: config.dead_letter_enabled,
        };

        let bus = Arc::new(EventBus::new(bus_config)?);
        let store = Arc::new(EventStore::new(storage.clone())?);
        let webhooks = Arc::new(WebhookManager::new(config.webhook).await?);
        let scheduler = Arc::new(CronScheduler::new());
        let triggers = Arc::new(TriggerManager::new(bus.clone()));

        // Create transaction event publisher that publishes to the bus
        let transaction_publisher = Arc::new(BusEventPublisher { bus: bus.clone() });
        let transactions = Arc::new(TransactionEventManager::with_publisher(transaction_publisher));

        // Create metrics collector
        let (metrics_collector, _alert_receiver) = metrics::EventMetricsCollector::new();
        let metrics = Arc::new(metrics_collector);

        // Create unified event streamer
        let unified_streamer = Arc::new(unified_streaming::UnifiedEventStreamer::new(metrics.clone()));

        // Connect components
        let event_system = Self {
            bus: bus.clone(),
            store: store.clone(),
            webhooks: webhooks.clone(),
            scheduler: scheduler.clone(),
            triggers: triggers.clone(),
            transactions: transactions.clone(),
            metrics: metrics.clone(),
            unified_streamer: unified_streamer.clone(),
            storage,
        };

        // Set up event sourcing
        bus.subscribe_persistent(Arc::new(EventSourceSubscriber {
            store: store.clone(),
        }))
        .await?;

        // Set up webhook delivery
        bus.subscribe(Arc::new(WebhookSubscriber {
            manager: webhooks.clone(),
        }))
        .await?;

        Ok(event_system)
    }

    pub async fn start(&self) -> Result<()> {
        self.scheduler.start().await?;
        self.triggers.start().await?;
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        self.scheduler.stop().await?;
        self.triggers.stop().await?;
        Ok(())
    }

    pub async fn publish(&self, event: Event) -> Result<()> {
        self.bus.publish(event).await
    }

    pub async fn replay_events(&self, from: Option<EventId>, to: Option<EventId>) -> Result<()> {
        let events = self.store.get_events(from, to).await?;
        for event in events {
            self.bus.publish(event).await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSystemConfig {
    pub bus_buffer_size: usize,
    pub dead_letter_enabled: bool,
    pub webhook: WebhookConfig,
}

impl Default for EventSystemConfig {
    fn default() -> Self {
        Self {
            bus_buffer_size: 10000,
            dead_letter_enabled: true,
            webhook: WebhookConfig::default(),
        }
    }
}

// Event sourcing subscriber
struct EventSourceSubscriber {
    store: Arc<EventStore>,
}

#[async_trait::async_trait]
impl crate::handlers::EventHandler for EventSourceSubscriber {
    async fn handle(&self, event: &Event) -> HandlerResult {
        match self.store.append_event(event).await {
            Ok(_) => HandlerResult::Success,
            Err(e) => {
                tracing::error!("Failed to store event: {}", e);
                HandlerResult::Retry
            }
        }
    }

    fn interested_in(&self, _event_type: &EventType) -> bool {
        true // Store all events
    }
}

// Webhook delivery subscriber
struct WebhookSubscriber {
    manager: Arc<WebhookManager>,
}

#[async_trait::async_trait]
impl crate::handlers::EventHandler for WebhookSubscriber {
    async fn handle(&self, event: &Event) -> HandlerResult {
        match self.manager.deliver_event(event).await {
            Ok(_) => HandlerResult::Success,
            Err(e) => {
                tracing::warn!("Webhook delivery failed: {}", e);
                HandlerResult::Retry
            }
        }
    }

    fn interested_in(&self, event_type: &EventType) -> bool {
        // Check if any webhooks are configured for this event type
        self.manager.has_subscribers(event_type)
    }
}

/// Event publisher implementation that publishes to the event bus
struct BusEventPublisher {
    bus: Arc<EventBus>,
}

#[async_trait::async_trait]
impl transaction::EventPublisher for BusEventPublisher {
    async fn publish(&self, event: Event) -> Result<()> {
        self.bus.publish(event).await
    }

    async fn publish_batch(&self, events: Vec<Event>) -> Result<()> {
        for event in events {
            self.bus.publish(event).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_event_system_creation() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let config = EventSystemConfig::default();
        let system = EventSystem::new(storage, config).await.unwrap();

        assert!(!system.bus.is_closed());
    }
}