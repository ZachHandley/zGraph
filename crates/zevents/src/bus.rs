use crate::events::Event;
use crate::handlers::{EventHandler, HandlerResult};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// Configuration for the event bus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBusConfig {
    /// Buffer size for the event channel
    pub buffer_size: usize,
    /// Enable dead letter queue for failed events
    pub dead_letter_enabled: bool,
}

impl Default for EventBusConfig {
    fn default() -> Self {
        Self {
            buffer_size: 10000,
            dead_letter_enabled: true,
        }
    }
}

/// Statistics for event bus monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBusStats {
    /// Total events published
    pub events_published: u64,
    /// Total events delivered successfully
    pub events_delivered: u64,
    /// Total events failed
    pub events_failed: u64,
    /// Total events retried
    pub events_retried: u64,
    /// Total dead letter events
    pub dead_letter_count: u64,
    /// Number of active subscribers
    pub active_subscribers: usize,
    /// Number of persistent subscribers
    pub persistent_subscribers: usize,
}

/// Atomic statistics for thread-safe updates
struct AtomicEventBusStats {
    events_published: AtomicU64,
    events_delivered: AtomicU64,
    events_failed: AtomicU64,
    events_retried: AtomicU64,
    dead_letter_count: AtomicU64,
}

impl AtomicEventBusStats {
    fn new() -> Self {
        Self {
            events_published: AtomicU64::new(0),
            events_delivered: AtomicU64::new(0),
            events_failed: AtomicU64::new(0),
            events_retried: AtomicU64::new(0),
            dead_letter_count: AtomicU64::new(0),
        }
    }

    fn to_stats(&self, active_subscribers: usize, persistent_subscribers: usize) -> EventBusStats {
        EventBusStats {
            events_published: self.events_published.load(Ordering::Relaxed),
            events_delivered: self.events_delivered.load(Ordering::Relaxed),
            events_failed: self.events_failed.load(Ordering::Relaxed),
            events_retried: self.events_retried.load(Ordering::Relaxed),
            dead_letter_count: self.dead_letter_count.load(Ordering::Relaxed),
            active_subscribers,
            persistent_subscribers,
        }
    }
}

/// Subscription information
#[derive(Clone)]
pub struct Subscription {
    /// Unique subscription ID
    pub id: String,
    /// Topic patterns this subscription is interested in
    pub topics: Vec<String>,
    /// Handler for processing events
    pub handler: Arc<dyn EventHandler>,
    /// Whether this is a persistent subscription (survives bus restarts)
    pub persistent: bool,
    /// Retry configuration
    pub max_retries: u32,
    /// Whether this subscription is active
    pub active: bool,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("id", &self.id)
            .field("topics", &self.topics)
            .field("handler", &"<EventHandler>")
            .field("persistent", &self.persistent)
            .field("max_retries", &self.max_retries)
            .field("active", &self.active)
            .finish()
    }
}

/// Event delivery tracking
#[derive(Debug, Clone)]
struct DeliveryTrack {
    /// Event ID being delivered
    event_id: String,
    /// Subscription ID
    subscription_id: String,
    /// Retry count
    retry_count: u32,
    /// Last delivery attempt timestamp
    last_attempt: chrono::DateTime<chrono::Utc>,
}

/// High-performance event bus with topic-based routing
pub struct EventBus {
    /// Configuration
    config: EventBusConfig,
    /// Event publisher
    publisher: broadcast::Sender<Arc<Event>>,
    /// Subscriber registry
    subscribers: Arc<RwLock<HashMap<String, Subscription>>>,
    /// Persistent subscriber registry (survives restarts)
    persistent_subscribers: Arc<RwLock<HashMap<String, Subscription>>>,
    /// Bus statistics
    atomic_stats: Arc<AtomicEventBusStats>,
    /// Dead letter queue
    dead_letter: Arc<RwLock<Vec<(Event, String)>>>, // Event + reason
    /// Shutdown signal
    shutdown: Arc<AtomicBool>,
    /// Background task handles
    task_handles: Arc<RwLock<Vec<JoinHandle<()>>>>,
    /// Event sequence counter
    sequence: AtomicU64,
    /// Delivery tracking for retries
    delivery_tracking: Arc<RwLock<HashMap<String, DeliveryTrack>>>,
}

impl EventBus {
    pub fn new(config: EventBusConfig) -> Result<Self> {
        let (publisher, _) = broadcast::channel(config.buffer_size);

        let atomic_stats = Arc::new(AtomicEventBusStats::new());

        let bus = Self {
            config,
            publisher,
            subscribers: Arc::new(RwLock::new(HashMap::new())),
            persistent_subscribers: Arc::new(RwLock::new(HashMap::new())),
            atomic_stats,
            dead_letter: Arc::new(RwLock::new(Vec::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
            task_handles: Arc::new(RwLock::new(Vec::new())),
            sequence: AtomicU64::new(1),
            delivery_tracking: Arc::new(RwLock::new(HashMap::new())),
        };

        Ok(bus)
    }

    /// Start the event bus with background processing
    pub async fn start(&self) -> Result<()> {
        if self.is_closed() {
            return Err(anyhow!("Event bus is already shut down"));
        }

        // Start delivery processor
        let delivery_task = self.start_delivery_processor().await;
        self.task_handles.write().await.push(delivery_task);

        // Start retry processor if dead letter is enabled
        if self.config.dead_letter_enabled {
            let retry_task = self.start_retry_processor().await;
            self.task_handles.write().await.push(retry_task);
        }

        info!("Event bus started with {} buffer size", self.config.buffer_size);
        Ok(())
    }

    /// Publish an event to the bus
    pub async fn publish(&self, event: Event) -> Result<()> {
        if self.is_closed() {
            return Err(anyhow!("Event bus is shut down"));
        }

        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
        debug!(
            event_id = %event.id,
            event_type = %event.event_type,
            sequence = seq,
            "Publishing event"
        );

        let event_arc = Arc::new(event);

        match self.publisher.send(event_arc.clone()) {
            Ok(receiver_count) => {
                debug!(
                    event_id = %event_arc.id,
                    receivers = receiver_count,
                    "Event published to {} receivers",
                    receiver_count
                );

                // Update stats
                self.atomic_stats.events_published.fetch_add(1, Ordering::Relaxed);

                Ok(())
            }
            Err(_) => {
                warn!(event_id = %event_arc.id, "No active receivers for event");
                Ok(()) // Not an error to have no receivers
            }
        }
    }

    /// Subscribe to events with a handler
    pub async fn subscribe(&self, handler: Arc<dyn EventHandler>) -> Result<String> {
        self.subscribe_with_config(handler, vec!["**".to_string()], false, 3)
            .await
    }

    /// Subscribe to specific topics with a handler
    pub async fn subscribe_topics(
        &self,
        handler: Arc<dyn EventHandler>,
        topics: Vec<String>,
    ) -> Result<String> {
        self.subscribe_with_config(handler, topics, false, 3).await
    }

    /// Subscribe as a persistent subscriber (survives bus restarts)
    pub async fn subscribe_persistent(&self, handler: Arc<dyn EventHandler>) -> Result<String> {
        self.subscribe_with_config(handler, vec!["**".to_string()], true, 3)
            .await
    }

    /// Subscribe with full configuration
    pub async fn subscribe_with_config(
        &self,
        handler: Arc<dyn EventHandler>,
        topics: Vec<String>,
        persistent: bool,
        max_retries: u32,
    ) -> Result<String> {
        if self.is_closed() {
            return Err(anyhow!("Event bus is shut down"));
        }

        let subscription_id = uuid::Uuid::new_v4().to_string();

        let subscription = Subscription {
            id: subscription_id.clone(),
            topics,
            handler,
            persistent,
            max_retries,
            active: true,
        };

        if persistent {
            self.persistent_subscribers
                .write()
                .await
                .insert(subscription_id.clone(), subscription);
        } else {
            self.subscribers
                .write()
                .await
                .insert(subscription_id.clone(), subscription);
        }

        info!(
            subscription_id = %subscription_id,
            persistent = persistent,
            "New subscription created"
        );

        Ok(subscription_id)
    }

    /// Unsubscribe a handler
    pub async fn unsubscribe(&self, subscription_id: &str) -> Result<()> {
        let mut removed = false;

        // Try regular subscribers first
        if self.subscribers.write().await.remove(subscription_id).is_some() {
            removed = true;
        }

        // Try persistent subscribers
        if self
            .persistent_subscribers
            .write()
            .await
            .remove(subscription_id)
            .is_some()
        {
            removed = true;
        }

        if removed {
            info!(subscription_id = %subscription_id, "Subscription removed");
            Ok(())
        } else {
            Err(anyhow!("Subscription not found: {}", subscription_id))
        }
    }

    /// Get current bus statistics
    pub fn stats(&self) -> EventBusStats {
        let subscribers_count = self.subscribers.try_read().map(|s| s.len()).unwrap_or(0);
        let persistent_count = self
            .persistent_subscribers
            .try_read()
            .map(|s| s.len())
            .unwrap_or(0);

        self.atomic_stats.to_stats(subscribers_count, persistent_count)
    }

    /// Get dead letter events
    pub async fn dead_letter_events(&self) -> Vec<(Event, String)> {
        self.dead_letter.read().await.clone()
    }

    /// Clear dead letter queue
    pub async fn clear_dead_letter(&self) -> Result<()> {
        self.dead_letter.write().await.clear();
        info!("Dead letter queue cleared");
        Ok(())
    }

    /// Check if the bus is closed
    pub fn is_closed(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Shutdown the event bus
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down event bus");

        // Signal shutdown
        self.shutdown.store(true, Ordering::Release);

        // Cancel all background tasks
        let mut handles = self.task_handles.write().await;
        for handle in handles.drain(..) {
            handle.abort();
        }

        // Clear subscribers
        self.subscribers.write().await.clear();

        info!("Event bus shut down complete");
        Ok(())
    }

    /// Start the delivery processor background task
    async fn start_delivery_processor(&self) -> JoinHandle<()> {
        let mut receiver = self.publisher.subscribe();
        let subscribers = self.subscribers.clone();
        let persistent_subscribers = self.persistent_subscribers.clone();
        let dead_letter = self.dead_letter.clone();
        let delivery_tracking = self.delivery_tracking.clone();
        let shutdown = self.shutdown.clone();
        let stats = self.atomic_stats.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            while !shutdown.load(Ordering::Acquire) {
                match receiver.recv().await {
                    Ok(event) => {
                        // Deliver to regular subscribers
                        let subs = subscribers.read().await;
                        for (sub_id, subscription) in subs.iter() {
                            if !subscription.active {
                                continue;
                            }

                            if Self::should_deliver(&event, subscription) {
                                Self::deliver_event_to_subscriber(
                                    &event,
                                    subscription,
                                    &delivery_tracking,
                                    &dead_letter,
                                    &stats,
                                    &config,
                                )
                                .await;
                            }
                        }
                        drop(subs);

                        // Deliver to persistent subscribers
                        let persistent_subs = persistent_subscribers.read().await;
                        for (sub_id, subscription) in persistent_subs.iter() {
                            if !subscription.active {
                                continue;
                            }

                            if Self::should_deliver(&event, subscription) {
                                Self::deliver_event_to_subscriber(
                                    &event,
                                    subscription,
                                    &delivery_tracking,
                                    &dead_letter,
                                    &stats,
                                    &config,
                                )
                                .await;
                            }
                        }
                        drop(persistent_subs);
                    }
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        warn!("Event delivery lagged, missed {} events", missed);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        debug!("Event bus channel closed");
                        break;
                    }
                }
            }

            debug!("Event delivery processor stopped");
        })
    }

    /// Start the retry processor background task
    async fn start_retry_processor(&self) -> JoinHandle<()> {
        let delivery_tracking = self.delivery_tracking.clone();
        let dead_letter = self.dead_letter.clone();
        let shutdown = self.shutdown.clone();
        let stats = self.atomic_stats.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

            while !shutdown.load(Ordering::Acquire) {
                interval.tick().await;

                let mut tracking = delivery_tracking.write().await;
                let mut to_retry = Vec::new();
                let mut to_dead_letter = Vec::new();

                let now = chrono::Utc::now();

                // Find events that need retry or dead lettering
                for (event_id, track) in tracking.iter() {
                    let elapsed = now.signed_duration_since(track.last_attempt);
                    if elapsed.num_seconds() > 60 {
                        // Retry after 60 seconds
                        if track.retry_count < 3 {
                            // Max 3 retries
                            to_retry.push(event_id.clone());
                        } else {
                            to_dead_letter.push(event_id.clone());
                        }
                    }
                }

                // Process retries and dead letters
                for event_id in to_retry {
                    if let Some(track) = tracking.get_mut(&event_id) {
                        track.retry_count += 1;
                        track.last_attempt = now;

                        stats.events_retried.fetch_add(1, Ordering::Relaxed);

                        debug!(
                            event_id = %event_id,
                            retry_count = track.retry_count,
                            "Retrying event delivery"
                        );
                    }
                }

                for event_id in to_dead_letter {
                    tracking.remove(&event_id);

                    stats.dead_letter_count.fetch_add(1, Ordering::Relaxed);

                    warn!(
                        event_id = %event_id,
                        "Event moved to dead letter queue after max retries"
                    );
                }
            }

            debug!("Retry processor stopped");
        })
    }

    /// Check if an event should be delivered to a subscription
    fn should_deliver(event: &Event, subscription: &Subscription) -> bool {
        // Check if handler is interested in this event type
        if !subscription.handler.interested_in(&event.event_type) {
            return false;
        }

        // Check topic matching
        let event_topic = event.to_topic();
        for pattern in &subscription.topics {
            if event.matches_topic(pattern) {
                return true;
            }
        }

        false
    }

    /// Deliver an event to a specific subscriber
    async fn deliver_event_to_subscriber(
        event: &Arc<Event>,
        subscription: &Subscription,
        delivery_tracking: &Arc<RwLock<HashMap<String, DeliveryTrack>>>,
        dead_letter: &Arc<RwLock<Vec<(Event, String)>>>,
        stats: &Arc<AtomicEventBusStats>,
        config: &EventBusConfig,
    ) {
        let result = subscription.handler.handle(event).await;

        match result {
            HandlerResult::Success => {
                debug!(
                    event_id = %event.id,
                    subscription_id = %subscription.id,
                    "Event delivered successfully"
                );

                stats.events_delivered.fetch_add(1, Ordering::Relaxed);

                // Remove from tracking
                delivery_tracking
                    .write()
                    .await
                    .remove(&event.id.to_string());
            }
            HandlerResult::Retry => {
                warn!(
                    event_id = %event.id,
                    subscription_id = %subscription.id,
                    "Event delivery failed, will retry"
                );

                stats.events_failed.fetch_add(1, Ordering::Relaxed);

                // Add to retry tracking
                let track = DeliveryTrack {
                    event_id: event.id.to_string(),
                    subscription_id: subscription.id.clone(),
                    retry_count: 0,
                    last_attempt: chrono::Utc::now(),
                };

                delivery_tracking
                    .write()
                    .await
                    .insert(event.id.to_string(), track);
            }
            HandlerResult::Discard => {
                debug!(
                    event_id = %event.id,
                    subscription_id = %subscription.id,
                    "Event discarded by handler"
                );

                // Remove from tracking
                delivery_tracking
                    .write()
                    .await
                    .remove(&event.id.to_string());
            }
            HandlerResult::DeadLetter(reason) => {
                error!(
                    event_id = %event.id,
                    subscription_id = %subscription.id,
                    reason = %reason,
                    "Event moved to dead letter queue"
                );

                stats.dead_letter_count.fetch_add(1, Ordering::Relaxed);

                if config.dead_letter_enabled {
                    dead_letter
                        .write()
                        .await
                        .push(((**event).clone(), reason));
                }

                // Remove from tracking
                delivery_tracking
                    .write()
                    .await
                    .remove(&event.id.to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::EventHandler;
    use crate::events::{Event, EventData, EventType};
    use std::sync::atomic::AtomicU32;

    struct TestHandler {
        received_count: AtomicU32,
        should_fail: AtomicBool,
    }

    impl TestHandler {
        fn new() -> Self {
            Self {
                received_count: AtomicU32::new(0),
                should_fail: AtomicBool::new(false),
            }
        }

        fn get_count(&self) -> u32 {
            self.received_count.load(Ordering::Acquire)
        }

        fn set_should_fail(&self, fail: bool) {
            self.should_fail.store(fail, Ordering::Release);
        }
    }

    #[async_trait::async_trait]
    impl EventHandler for TestHandler {
        async fn handle(&self, event: &Event) -> HandlerResult {
            self.received_count.fetch_add(1, Ordering::SeqCst);

            if self.should_fail.load(Ordering::Acquire) {
                HandlerResult::Retry
            } else {
                HandlerResult::Success
            }
        }

        fn interested_in(&self, _event_type: &EventType) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn test_event_bus_creation() {
        let config = EventBusConfig::default();
        let bus = EventBus::new(config).unwrap();
        assert!(!bus.is_closed());
    }

    #[tokio::test]
    async fn test_publish_and_subscribe() {
        let config = EventBusConfig::default();
        let bus = EventBus::new(config).unwrap();
        bus.start().await.unwrap();

        let handler = Arc::new(TestHandler::new());
        let _subscription_id = bus.subscribe(handler.clone()).await.unwrap();

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

        bus.publish(event).await.unwrap();

        // Give some time for async delivery
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        assert_eq!(handler.get_count(), 1);

        bus.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let config = EventBusConfig::default();
        let bus = EventBus::new(config).unwrap();
        bus.start().await.unwrap();

        let handler = Arc::new(TestHandler::new());
        let subscription_id = bus.subscribe(handler.clone()).await.unwrap();

        // Unsubscribe
        bus.unsubscribe(&subscription_id).await.unwrap();

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

        bus.publish(event).await.unwrap();

        // Give some time for potential delivery
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        assert_eq!(handler.get_count(), 0);

        bus.shutdown().await.unwrap();
    }
}