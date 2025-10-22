use crate::events::Event;
use crate::handlers::{EventHandler, HandlerResult};
use crate::persistent_queue::{PersistentMessageQueue, PersistentQueueConfig, PersistentQueueStats};
use crate::consumer::{PersistentConsumer, ConsumerConfig, ConsumerGroup, ConsumerStats};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use zcore_storage::Store;

/// Configuration for the exactly-once event bus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExactlyOnceEventBusConfig {
    /// Persistent queue configuration
    pub queue_config: PersistentQueueConfig,
    /// Default consumer configuration template
    pub default_consumer_config: ConsumerConfig,
    /// Enable automatic consumer group management
    pub auto_manage_consumers: bool,
    /// Maximum number of consumer groups
    pub max_consumer_groups: usize,
}

impl Default for ExactlyOnceEventBusConfig {
    fn default() -> Self {
        Self {
            queue_config: PersistentQueueConfig::default(),
            default_consumer_config: ConsumerConfig::default(),
            auto_manage_consumers: true,
            max_consumer_groups: 100,
        }
    }
}

/// Subscription information for exactly-once delivery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExactlyOnceSubscription {
    /// Unique subscription ID
    pub id: String,
    /// Consumer ID assigned to this subscription
    pub consumer_id: String,
    /// Topic patterns this subscription is interested in
    pub topics: Vec<String>,
    /// Whether this is a persistent subscription (survives bus restarts)
    pub persistent: bool,
    /// Consumer configuration
    pub config: ConsumerConfig,
    /// Subscription creation timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Statistics combining queue and consumer metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExactlyOnceEventBusStats {
    /// Persistent queue statistics
    pub queue_stats: PersistentQueueStats,
    /// Consumer group statistics
    pub consumer_groups: HashMap<String, Vec<ConsumerStats>>,
    /// Total active subscriptions
    pub active_subscriptions: usize,
    /// Total persistent subscriptions
    pub persistent_subscriptions: usize,
    /// Bus uptime
    pub uptime_secs: u64,
}

/// High-performance event bus with exactly-once delivery guarantees
pub struct ExactlyOnceEventBus {
    config: ExactlyOnceEventBusConfig,
    storage: Arc<Store>,
    queue: Arc<PersistentMessageQueue>,
    consumer_groups: Arc<RwLock<HashMap<String, Arc<ConsumerGroup>>>>,
    subscriptions: Arc<RwLock<HashMap<String, ExactlyOnceSubscription>>>,
    persistent_subscriptions: Arc<RwLock<HashMap<String, ExactlyOnceSubscription>>>,
    start_time: chrono::DateTime<chrono::Utc>,
    shutdown: Arc<tokio::sync::broadcast::Sender<()>>,
}

impl ExactlyOnceEventBus {
    /// Create a new exactly-once event bus
    pub async fn new(storage: Arc<Store>, config: ExactlyOnceEventBusConfig) -> Result<Self> {
        let queue = Arc::new(
            PersistentMessageQueue::new(storage.clone(), config.queue_config.clone()).await?
        );

        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);

        let bus = Self {
            config,
            storage,
            queue,
            consumer_groups: Arc::new(RwLock::new(HashMap::new())),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            persistent_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            start_time: chrono::Utc::now(),
            shutdown: Arc::new(shutdown_tx),
        };

        info!("Exactly-once event bus initialized");
        Ok(bus)
    }

    /// Start the event bus
    pub async fn start(&self) -> Result<()> {
        info!("Starting exactly-once event bus");

        // Restore persistent subscriptions
        self.restore_persistent_subscriptions().await?;

        info!("Exactly-once event bus started");
        Ok(())
    }

    /// Publish an event with exactly-once delivery guarantee
    pub async fn publish(&self, event: Event) -> Result<String> {
        let topic = event.to_topic();
        let event_id = event.id.to_string();

        debug!(
            event_id = %event_id,
            event_type = %event.event_type,
            topic = %topic,
            "Publishing event with exactly-once delivery"
        );

        let message_id = self.queue.enqueue(event, topic).await?;

        debug!(
            event_id = %event_id,
            message_id = %message_id,
            "Event published to persistent queue"
        );

        Ok(message_id)
    }

    /// Subscribe to events with exactly-once processing
    pub async fn subscribe(&self, handler: Arc<dyn EventHandler>) -> Result<String> {
        self.subscribe_with_config(
            handler,
            vec!["**".to_string()],
            false,
            self.config.default_consumer_config.clone()
        ).await
    }

    /// Subscribe to specific topics with exactly-once processing
    pub async fn subscribe_topics(
        &self,
        handler: Arc<dyn EventHandler>,
        topics: Vec<String>,
    ) -> Result<String> {
        self.subscribe_with_config(
            handler,
            topics,
            false,
            self.config.default_consumer_config.clone()
        ).await
    }

    /// Subscribe as a persistent subscriber with exactly-once processing
    pub async fn subscribe_persistent(&self, handler: Arc<dyn EventHandler>) -> Result<String> {
        self.subscribe_with_config(
            handler,
            vec!["**".to_string()],
            true,
            self.config.default_consumer_config.clone()
        ).await
    }

    /// Subscribe with full configuration
    pub async fn subscribe_with_config(
        &self,
        handler: Arc<dyn EventHandler>,
        topics: Vec<String>,
        persistent: bool,
        mut consumer_config: ConsumerConfig,
    ) -> Result<String> {
        let subscription_id = ulid::Ulid::new().to_string();
        let consumer_id = format!("consumer_{}", subscription_id);

        // Update consumer config
        consumer_config.consumer_id = consumer_id.clone();
        consumer_config.topics = topics.clone();
        consumer_config.persistent = persistent;
        consumer_config.auto_start = false; // We'll start manually

        let subscription = ExactlyOnceSubscription {
            id: subscription_id.clone(),
            consumer_id: consumer_id.clone(),
            topics: topics.clone(),
            persistent,
            config: consumer_config.clone(),
            created_at: chrono::Utc::now(),
        };

        // Create consumer group for this subscription if needed
        let group_id = if persistent {
            format!("persistent_{}", subscription_id)
        } else {
            format!("transient_{}", subscription_id)
        };

        let group = {
            let mut groups = self.consumer_groups.write().await;
            if let Some(existing_group) = groups.get(&group_id) {
                existing_group.clone()
            } else {
                let new_group = Arc::new(ConsumerGroup::new(group_id.clone(), self.queue.clone()));
                groups.insert(group_id.clone(), new_group.clone());
                new_group
            }
        };

        // Add consumer to group
        group.add_consumer(consumer_config, handler).await?;

        // Start the consumer
        if let Some(consumers) = self.consumer_groups.read().await.get(&group_id) {
            consumers.start_all().await?;
        }

        // Store subscription
        if persistent {
            self.persistent_subscriptions
                .write()
                .await
                .insert(subscription_id.clone(), subscription.clone());

            // Persist to storage
            self.persist_subscription(&subscription).await?;
        } else {
            self.subscriptions
                .write()
                .await
                .insert(subscription_id.clone(), subscription);
        }

        info!(
            subscription_id = %subscription_id,
            consumer_id = %consumer_id,
            topics = ?topics,
            persistent = persistent,
            "Created exactly-once subscription"
        );

        Ok(subscription_id)
    }

    /// Unsubscribe from events
    pub async fn unsubscribe(&self, subscription_id: &str) -> Result<()> {
        let subscription = {
            // Try regular subscriptions first
            if let Some(sub) = self.subscriptions.write().await.remove(subscription_id) {
                Some(sub)
            } else if let Some(sub) = self.persistent_subscriptions.write().await.remove(subscription_id) {
                // Remove from persistent storage
                self.remove_persisted_subscription(subscription_id).await?;
                Some(sub)
            } else {
                None
            }
        };

        if let Some(subscription) = subscription {
            // Find and remove consumer from appropriate group
            let group_id = if subscription.persistent {
                format!("persistent_{}", subscription_id)
            } else {
                format!("transient_{}", subscription_id)
            };

            if let Some(group) = self.consumer_groups.read().await.get(&group_id) {
                group.remove_consumer(&subscription.consumer_id).await?;

                // Remove empty groups
                if group.consumer_count().await == 0 {
                    self.consumer_groups.write().await.remove(&group_id);
                }
            }

            info!(
                subscription_id = %subscription_id,
                consumer_id = %subscription.consumer_id,
                "Unsubscribed from exactly-once delivery"
            );

            Ok(())
        } else {
            Err(anyhow!("Subscription not found: {}", subscription_id))
        }
    }

    /// Get current bus statistics
    pub async fn stats(&self) -> ExactlyOnceEventBusStats {
        let queue_stats = self.queue.stats().await;

        let mut consumer_groups = HashMap::new();
        let groups = self.consumer_groups.read().await;
        for (group_id, group) in groups.iter() {
            let group_stats = group.group_stats().await;
            consumer_groups.insert(group_id.clone(), group_stats);
        }

        let active_subscriptions = self.subscriptions.read().await.len();
        let persistent_subscriptions = self.persistent_subscriptions.read().await.len();

        let uptime_secs = (chrono::Utc::now() - self.start_time).num_seconds() as u64;

        ExactlyOnceEventBusStats {
            queue_stats,
            consumer_groups,
            active_subscriptions,
            persistent_subscriptions,
            uptime_secs,
        }
    }

    /// Get messages in the dead letter queue
    pub async fn dead_letter_messages(&self) -> Result<Vec<String>> {
        // This would need to be implemented in PersistentMessageQueue
        // For now, return empty list
        Ok(Vec::new())
    }

    /// Retry messages from dead letter queue
    pub async fn retry_dead_letter_messages(&self, message_ids: Vec<String>) -> Result<usize> {
        // This would need to be implemented in PersistentMessageQueue
        // For now, return 0
        Ok(0)
    }

    /// Shutdown the event bus gracefully
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down exactly-once event bus");

        // Signal shutdown
        let _ = self.shutdown.send(());

        // Stop all consumer groups
        let groups = self.consumer_groups.read().await;
        let mut shutdown_tasks = Vec::new();

        for (group_id, group) in groups.iter() {
            let group_clone = group.clone();
            let group_id_clone = group_id.clone();
            shutdown_tasks.push(tokio::spawn(async move {
                if let Err(e) = group_clone.stop_all().await {
                    error!(
                        group_id = %group_id_clone,
                        error = %e,
                        "Failed to stop consumer group"
                    );
                }
            }));
        }

        // Wait for all groups to stop
        for task in shutdown_tasks {
            let _ = task.await;
        }

        // Shutdown the queue
        self.queue.shutdown().await?;

        info!("Exactly-once event bus shutdown complete");
        Ok(())
    }

    /// Check if the bus is healthy (all consumers are running)
    pub async fn health_check(&self) -> Result<bool> {
        let stats = self.stats().await;

        // Check if queue is responsive
        if stats.queue_stats.total_messages == 0 && stats.queue_stats.queued_messages == 0 {
            // Try a simple operation - just call stats to check responsiveness
            let _ = self.queue.stats().await;
        }

        // Check if consumers are running
        for (_, group_stats) in stats.consumer_groups.iter() {
            for consumer_stat in group_stats {
                if !consumer_stat.is_running {
                    warn!(
                        consumer_id = %consumer_stat.consumer_id,
                        "Consumer is not running"
                    );
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    /// Internal method to persist subscription to storage
    async fn persist_subscription(&self, subscription: &ExactlyOnceSubscription) -> Result<()> {
        let mut write_txn = self.storage.begin_write()?;
        {
            let mut subs_table = write_txn.open_table(&self.storage, "persistent_subscriptions")?;
            let key = subscription.id.as_bytes();
            let data = serde_json::to_vec(subscription)?;
            subs_table.insert(key, &data)?;
        }
        write_txn.commit(&self.storage)?;
        Ok(())
    }

    /// Internal method to remove persisted subscription
    async fn remove_persisted_subscription(&self, subscription_id: &str) -> Result<()> {
        let mut write_txn = self.storage.begin_write()?;
        {
            let mut subs_table = write_txn.open_table(&self.storage, "persistent_subscriptions")?;
            subs_table.remove(subscription_id.as_bytes())?;
        }
        write_txn.commit(&self.storage)?;
        Ok(())
    }

    /// Internal method to restore persistent subscriptions on startup
    async fn restore_persistent_subscriptions(&self) -> Result<()> {
        let read_txn = self.storage.begin_read()?;

        // Try to open the table, create if it doesn't exist
        let subs_table = match read_txn.open_table(&self.storage, "persistent_subscriptions") {
            Ok(table) => table,
            Err(_) => {
                // Table doesn't exist yet, create it
                drop(read_txn);
                let mut write_txn = self.storage.begin_write()?;
                {
                    let _ = write_txn.open_table(&self.storage, "persistent_subscriptions")?;
                }
                write_txn.commit(&self.storage)?;
                return Ok(());
            }
        };

        let mut restored_count = 0;

        for result in subs_table.iter()? {
            let (_, value) = result?;
            if let Ok(subscription) = serde_json::from_slice::<ExactlyOnceSubscription>(&value) {
                // Store in memory
                self.persistent_subscriptions
                    .write()
                    .await
                    .insert(subscription.id.clone(), subscription.clone());

                // TODO: Restore the actual consumer and handler
                // This would require serializing handlers or using a registry
                // For now, just track the subscription metadata

                restored_count += 1;

                debug!(
                    subscription_id = %subscription.id,
                    consumer_id = %subscription.consumer_id,
                    "Restored persistent subscription"
                );
            }
        }

        if restored_count > 0 {
            info!(
                count = restored_count,
                "Restored persistent subscriptions"
            );
        }

        Ok(())
    }
}

/// Builder for configuring exactly-once event bus
pub struct ExactlyOnceEventBusBuilder {
    config: ExactlyOnceEventBusConfig,
}

impl ExactlyOnceEventBusBuilder {
    pub fn new() -> Self {
        Self {
            config: ExactlyOnceEventBusConfig::default(),
        }
    }

    pub fn with_queue_config(mut self, queue_config: PersistentQueueConfig) -> Self {
        self.config.queue_config = queue_config;
        self
    }

    pub fn with_consumer_config(mut self, consumer_config: ConsumerConfig) -> Self {
        self.config.default_consumer_config = consumer_config;
        self
    }

    pub fn with_auto_manage_consumers(mut self, enabled: bool) -> Self {
        self.config.auto_manage_consumers = enabled;
        self
    }

    pub fn with_max_consumer_groups(mut self, max: usize) -> Self {
        self.config.max_consumer_groups = max;
        self
    }

    pub async fn build(self, storage: Arc<Store>) -> Result<ExactlyOnceEventBus> {
        ExactlyOnceEventBus::new(storage, self.config).await
    }
}

impl Default for ExactlyOnceEventBusBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{Event, EventType, EventData};
    use crate::handlers::EventHandler;
    use tempfile::tempdir;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct TestHandler {
        processed_count: AtomicU32,
        consumer_id: String,
    }

    impl TestHandler {
        fn new(consumer_id: String) -> Self {
            Self {
                processed_count: AtomicU32::new(0),
                consumer_id,
            }
        }

        fn get_count(&self) -> u32 {
            self.processed_count.load(Ordering::Acquire)
        }
    }

    #[async_trait::async_trait]
    impl EventHandler for TestHandler {
        async fn handle(&self, event: &Event) -> HandlerResult {
            self.processed_count.fetch_add(1, Ordering::SeqCst);
            println!("Handler {} processed event {}", self.consumer_id, event.id);
            HandlerResult::Success
        }

        fn interested_in(&self, _event_type: &EventType) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn test_exactly_once_bus_basic_flow() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let bus = ExactlyOnceEventBusBuilder::new()
            .build(storage)
            .await
            .unwrap();

        bus.start().await.unwrap();

        let handler = Arc::new(TestHandler::new("test_handler".to_string()));
        let subscription_id = bus.subscribe(handler.clone()).await.unwrap();

        // Publish an event
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

        let message_id = bus.publish(event).await.unwrap();
        assert!(!message_id.is_empty());

        // Wait for processing
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // Verify processing
        assert_eq!(handler.get_count(), 1);

        // Check stats
        let stats = bus.stats().await;
        assert_eq!(stats.queue_stats.acknowledged_messages, 1);
        assert_eq!(stats.active_subscriptions, 1);

        // Clean up
        bus.unsubscribe(&subscription_id).await.unwrap();
        bus.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_exactly_once_delivery_guarantee() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let bus = ExactlyOnceEventBusBuilder::new()
            .build(storage)
            .await
            .unwrap();

        bus.start().await.unwrap();

        // Create multiple handlers for the same topic
        let handler1 = Arc::new(TestHandler::new("handler1".to_string()));
        let handler2 = Arc::new(TestHandler::new("handler2".to_string()));

        let sub1 = bus.subscribe_topics(handler1.clone(), vec!["jobs/**".to_string()]).await.unwrap();
        let sub2 = bus.subscribe_topics(handler2.clone(), vec!["jobs/**".to_string()]).await.unwrap();

        // Publish multiple events
        for i in 0..10 {
            let event = Event::new(
                EventType::JobCreated,
                EventData::Job {
                    job_id: uuid::Uuid::new_v4(),
                    org_id: 1,
                    status: Some(format!("test_{}", i)),
                    worker_id: None,
                    error: None,
                    output: None,
                },
                1,
            );

            bus.publish(event).await.unwrap();
        }

        // Wait for processing
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // With load balancing, each event should be delivered to exactly ONE consumer
        // Total processed should be 10 (each event processed exactly once)
        let total_processed = handler1.get_count() + handler2.get_count();
        assert_eq!(total_processed, 10, "Each event should be processed exactly once across all consumers");

        let stats = bus.stats().await;
        assert_eq!(stats.queue_stats.acknowledged_messages, 10);

        bus.unsubscribe(&sub1).await.unwrap();
        bus.unsubscribe(&sub2).await.unwrap();
        bus.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_health_check() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let bus = ExactlyOnceEventBusBuilder::new()
            .build(storage)
            .await
            .unwrap();

        bus.start().await.unwrap();

        // Should be healthy initially
        assert!(bus.health_check().await.unwrap());

        let handler = Arc::new(TestHandler::new("health_test".to_string()));
        let _subscription_id = bus.subscribe(handler).await.unwrap();

        // Should still be healthy with consumers
        assert!(bus.health_check().await.unwrap());

        bus.shutdown().await.unwrap();
    }
}