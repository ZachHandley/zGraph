use crate::events::Event;
use crate::handlers::{EventHandler, HandlerResult};
use crate::persistent_queue::{PersistentMessageQueue, PersistentMessage};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, Semaphore};
use tokio::time::{timeout, sleep};
use tracing::{debug, error, info, warn};

/// Configuration for a persistent message consumer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerConfig {
    /// Unique consumer identifier
    pub consumer_id: String,
    /// Topics this consumer is interested in (supports wildcards)
    pub topics: Vec<String>,
    /// Maximum number of messages to process concurrently
    pub max_concurrency: usize,
    /// Timeout for message processing
    pub processing_timeout_secs: u64,
    /// Interval between polling for new messages
    pub poll_interval_ms: u64,
    /// Whether to auto-start consuming on creation
    pub auto_start: bool,
    /// Whether consumer survives queue restarts
    pub persistent: bool,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            consumer_id: format!("consumer_{}", ulid::Ulid::new()),
            topics: vec!["**".to_string()], // Subscribe to all topics by default
            max_concurrency: 10,
            processing_timeout_secs: 30,
            poll_interval_ms: 100,
            auto_start: true,
            persistent: false,
        }
    }
}

/// Statistics for monitoring consumer performance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerStats {
    pub consumer_id: String,
    pub is_running: bool,
    pub messages_processed: u64,
    pub messages_acked: u64,
    pub messages_nacked: u64,
    pub processing_errors: u64,
    pub current_in_flight: usize,
    pub max_concurrency: usize,
    pub last_message_processed: Option<chrono::DateTime<chrono::Utc>>,
    pub average_processing_time_ms: f64,
}

/// State for tracking message processing
#[derive(Debug)]
struct ProcessingState {
    message_id: String,
    start_time: std::time::Instant,
}

/// High-performance persistent message consumer with exactly-once processing
pub struct PersistentConsumer {
    config: ConsumerConfig,
    queue: Arc<PersistentMessageQueue>,
    handler: Arc<dyn EventHandler>,
    running: Arc<RwLock<bool>>,
    stats: Arc<RwLock<ConsumerStats>>,
    semaphore: Arc<Semaphore>,
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    processing_times: Arc<RwLock<Vec<f64>>>, // Rolling window for average calculation
}

impl PersistentConsumer {
    pub fn new(
        config: ConsumerConfig,
        queue: Arc<PersistentMessageQueue>,
        handler: Arc<dyn EventHandler>,
    ) -> Self {
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);

        let consumer = Self {
            semaphore: Arc::new(Semaphore::new(config.max_concurrency)),
            config: config.clone(),
            queue,
            handler,
            running: Arc::new(RwLock::new(false)),
            stats: Arc::new(RwLock::new(ConsumerStats {
                consumer_id: config.consumer_id.clone(),
                is_running: false,
                messages_processed: 0,
                messages_acked: 0,
                messages_nacked: 0,
                processing_errors: 0,
                current_in_flight: 0,
                max_concurrency: config.max_concurrency,
                last_message_processed: None,
                average_processing_time_ms: 0.0,
            })),
            shutdown_tx,
            processing_times: Arc::new(RwLock::new(Vec::new())),
        };

        if config.auto_start {
            let consumer_clone = consumer.clone();
            tokio::spawn(async move {
                if let Err(e) = consumer_clone.start().await {
                    error!(
                        consumer_id = %config.consumer_id,
                        error = %e,
                        "Failed to auto-start consumer"
                    );
                }
            });
        }

        consumer
    }

    /// Start the consumer processing loop
    pub async fn start(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            return Err(anyhow!("Consumer {} is already running", self.config.consumer_id));
        }

        *running = true;
        self.update_stats(|stats| stats.is_running = true).await;

        info!(
            consumer_id = %self.config.consumer_id,
            max_concurrency = self.config.max_concurrency,
            topics = ?self.config.topics,
            "Starting persistent consumer"
        );

        // Start the main processing loop
        let consumer_clone = self.clone();
        tokio::spawn(async move {
            consumer_clone.processing_loop().await;
        });

        Ok(())
    }

    /// Stop the consumer gracefully
    pub async fn stop(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if !*running {
            return Ok(());
        }

        info!(
            consumer_id = %self.config.consumer_id,
            "Stopping consumer"
        );

        *running = false;
        self.update_stats(|stats| stats.is_running = false).await;

        // Send shutdown signal
        let _ = self.shutdown_tx.send(());

        // Wait for all in-flight messages to complete
        let max_wait = Duration::from_secs(30);
        let start = std::time::Instant::now();

        while start.elapsed() < max_wait {
            let in_flight = self.stats.read().await.current_in_flight;
            if in_flight == 0 {
                break;
            }

            debug!(
                consumer_id = %self.config.consumer_id,
                in_flight = in_flight,
                "Waiting for in-flight messages to complete"
            );

            sleep(Duration::from_millis(100)).await;
        }

        info!(
            consumer_id = %self.config.consumer_id,
            "Consumer stopped"
        );

        Ok(())
    }

    /// Get current consumer statistics
    pub async fn stats(&self) -> ConsumerStats {
        self.stats.read().await.clone()
    }

    /// Check if consumer is currently running
    pub async fn is_running(&self) -> bool {
        *self.running.read().await
    }

    /// Main processing loop
    async fn processing_loop(&self) {
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let poll_interval = Duration::from_millis(self.config.poll_interval_ms);

        while *self.running.read().await {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    debug!(
                        consumer_id = %self.config.consumer_id,
                        "Received shutdown signal"
                    );
                    break;
                }

                _ = self.process_next_message() => {
                    // Continue processing
                }

                _ = sleep(poll_interval) => {
                    // Polling interval reached, continue loop
                }
            }
        }

        debug!(
            consumer_id = %self.config.consumer_id,
            "Processing loop stopped"
        );
    }

    /// Process the next available message
    async fn process_next_message(&self) {
        // Check if we can process more messages
        if let Ok(_permit) = self.semaphore.clone().try_acquire_owned() {
            match self.queue.dequeue(self.config.consumer_id.clone()).await {
                Ok(Some(message)) => {
                    self.update_stats(|stats| {
                        stats.current_in_flight += 1;
                        stats.messages_processed += 1;
                    }).await;

                    // Check if message topic matches our interests
                    if !self.is_interested_in_message(&message) {
                        // NACK the message since we're not interested
                        if let Err(e) = self.queue.nack(
                            &message.id,
                            &self.config.consumer_id,
                            "Consumer not interested in topic".to_string()
                        ).await {
                            error!(
                                consumer_id = %self.config.consumer_id,
                                message_id = %message.id,
                                error = %e,
                                "Failed to NACK uninteresting message"
                            );
                        }

                        self.update_stats(|stats| {
                            stats.current_in_flight = stats.current_in_flight.saturating_sub(1);
                            stats.messages_nacked += 1;
                        }).await;

                        return;
                    }

                    // Spawn task to process the message
                    let consumer_clone = self.clone();
                    tokio::spawn(async move {
                        consumer_clone.process_message(message).await;
                        drop(_permit); // Release semaphore permit
                    });
                }
                Ok(None) => {
                    // No messages available, wait for notification
                    if let Err(e) = timeout(Duration::from_secs(1), self.queue.wait_for_messages()).await {
                        // Timeout is fine, just continue polling
                        debug!(
                            consumer_id = %self.config.consumer_id,
                            "Timeout waiting for messages"
                        );
                    }
                }
                Err(e) => {
                    error!(
                        consumer_id = %self.config.consumer_id,
                        error = %e,
                        "Failed to dequeue message"
                    );
                    sleep(Duration::from_millis(1000)).await; // Back off on error
                }
            }
        } else {
            // At max concurrency, wait a bit
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// Process a single message with timeout and error handling
    async fn process_message(&self, message: PersistentMessage) {
        let start_time = std::time::Instant::now();
        let message_id = message.id.clone();

        debug!(
            consumer_id = %self.config.consumer_id,
            message_id = %message_id,
            topic = %message.topic,
            "Processing message"
        );

        let processing_timeout = Duration::from_secs(self.config.processing_timeout_secs);

        let result = timeout(processing_timeout, self.handler.handle(&message.event)).await;

        let processing_duration = start_time.elapsed();
        self.record_processing_time(processing_duration.as_secs_f64() * 1000.0).await;

        match result {
            Ok(handler_result) => {
                match handler_result {
                    HandlerResult::Success => {
                        // ACK the message
                        match self.queue.ack(&message_id, &self.config.consumer_id).await {
                            Ok(()) => {
                                debug!(
                                    consumer_id = %self.config.consumer_id,
                                    message_id = %message_id,
                                    duration_ms = processing_duration.as_millis(),
                                    "Message processed successfully"
                                );

                                self.update_stats(|stats| {
                                    stats.messages_acked += 1;
                                    stats.last_message_processed = Some(chrono::Utc::now());
                                }).await;
                            }
                            Err(e) => {
                                error!(
                                    consumer_id = %self.config.consumer_id,
                                    message_id = %message_id,
                                    error = %e,
                                    "Failed to ACK message"
                                );
                                self.update_stats(|stats| stats.processing_errors += 1).await;
                            }
                        }
                    }
                    HandlerResult::Retry => {
                        // NACK the message for retry
                        match self.queue.nack(&message_id, &self.config.consumer_id, "Handler requested retry".to_string()).await {
                            Ok(()) => {
                                debug!(
                                    consumer_id = %self.config.consumer_id,
                                    message_id = %message_id,
                                    "Message NACKed for retry"
                                );

                                self.update_stats(|stats| stats.messages_nacked += 1).await;
                            }
                            Err(e) => {
                                error!(
                                    consumer_id = %self.config.consumer_id,
                                    message_id = %message_id,
                                    error = %e,
                                    "Failed to NACK message"
                                );
                                self.update_stats(|stats| stats.processing_errors += 1).await;
                            }
                        }
                    }
                    HandlerResult::Discard => {
                        // ACK the message but don't process further
                        match self.queue.ack(&message_id, &self.config.consumer_id).await {
                            Ok(()) => {
                                debug!(
                                    consumer_id = %self.config.consumer_id,
                                    message_id = %message_id,
                                    "Message discarded"
                                );

                                self.update_stats(|stats| {
                                    stats.messages_acked += 1;
                                    stats.last_message_processed = Some(chrono::Utc::now());
                                }).await;
                            }
                            Err(e) => {
                                error!(
                                    consumer_id = %self.config.consumer_id,
                                    message_id = %message_id,
                                    error = %e,
                                    "Failed to ACK discarded message"
                                );
                                self.update_stats(|stats| stats.processing_errors += 1).await;
                            }
                        }
                    }
                    HandlerResult::DeadLetter(reason) => {
                        // NACK with dead letter reason
                        match self.queue.nack(&message_id, &self.config.consumer_id, reason).await {
                            Ok(()) => {
                                warn!(
                                    consumer_id = %self.config.consumer_id,
                                    message_id = %message_id,
                                    "Message dead lettered"
                                );

                                self.update_stats(|stats| stats.messages_nacked += 1).await;
                            }
                            Err(e) => {
                                error!(
                                    consumer_id = %self.config.consumer_id,
                                    message_id = %message_id,
                                    error = %e,
                                    "Failed to NACK message for dead letter"
                                );
                                self.update_stats(|stats| stats.processing_errors += 1).await;
                            }
                        }
                    }
                }
            }
            Err(_timeout) => {
                // Processing timed out
                error!(
                    consumer_id = %self.config.consumer_id,
                    message_id = %message_id,
                    timeout_secs = self.config.processing_timeout_secs,
                    "Message processing timed out"
                );

                match self.queue.nack(&message_id, &self.config.consumer_id, "Processing timeout".to_string()).await {
                    Ok(()) => {
                        self.update_stats(|stats| {
                            stats.messages_nacked += 1;
                            stats.processing_errors += 1;
                        }).await;
                    }
                    Err(e) => {
                        error!(
                            consumer_id = %self.config.consumer_id,
                            message_id = %message_id,
                            error = %e,
                            "Failed to NACK timed out message"
                        );
                        self.update_stats(|stats| stats.processing_errors += 1).await;
                    }
                }
            }
        }

        // Update in-flight count
        self.update_stats(|stats| {
            stats.current_in_flight = stats.current_in_flight.saturating_sub(1);
        }).await;
    }

    /// Check if the consumer is interested in this message based on topic patterns
    fn is_interested_in_message(&self, message: &PersistentMessage) -> bool {
        // First check if handler is interested in the event type
        if !self.handler.interested_in(&message.event.event_type) {
            return false;
        }

        // Check topic patterns
        for pattern in &self.config.topics {
            if message.event.matches_topic(pattern) {
                return true;
            }
        }

        false
    }

    /// Record processing time for performance tracking
    async fn record_processing_time(&self, time_ms: f64) {
        let mut times = self.processing_times.write().await;
        times.push(time_ms);

        // Keep only last 100 measurements for rolling average
        if times.len() > 100 {
            times.remove(0);
        }

        // Update average in stats
        let average = times.iter().sum::<f64>() / times.len() as f64;
        self.update_stats(|stats| stats.average_processing_time_ms = average).await;
    }

    /// Update consumer statistics
    async fn update_stats<F>(&self, update_fn: F)
    where
        F: FnOnce(&mut ConsumerStats),
    {
        let mut stats = self.stats.write().await;
        update_fn(&mut *stats);
    }
}

impl Clone for PersistentConsumer {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            queue: self.queue.clone(),
            handler: self.handler.clone(),
            running: self.running.clone(),
            stats: self.stats.clone(),
            semaphore: self.semaphore.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            processing_times: self.processing_times.clone(),
        }
    }
}

/// Consumer group for managing multiple consumers with load balancing
pub struct ConsumerGroup {
    group_id: String,
    consumers: Arc<RwLock<Vec<Arc<PersistentConsumer>>>>,
    queue: Arc<PersistentMessageQueue>,
}

impl ConsumerGroup {
    pub fn new(group_id: String, queue: Arc<PersistentMessageQueue>) -> Self {
        Self {
            group_id,
            consumers: Arc::new(RwLock::new(Vec::new())),
            queue,
        }
    }

    /// Add a consumer to the group
    pub async fn add_consumer(
        &self,
        config: ConsumerConfig,
        handler: Arc<dyn EventHandler>,
    ) -> Result<String> {
        let consumer_id = config.consumer_id.clone();
        let consumer = Arc::new(PersistentConsumer::new(config, self.queue.clone(), handler));

        self.consumers.write().await.push(consumer);

        info!(
            group_id = %self.group_id,
            consumer_id = %consumer_id,
            "Consumer added to group"
        );

        Ok(consumer_id)
    }

    /// Remove a consumer from the group
    pub async fn remove_consumer(&self, consumer_id: &str) -> Result<()> {
        let mut consumers = self.consumers.write().await;
        let initial_len = consumers.len();

        // Stop the consumer first
        for consumer in consumers.iter() {
            if consumer.config.consumer_id == consumer_id {
                consumer.stop().await?;
                break;
            }
        }

        // Remove from list
        consumers.retain(|c| c.config.consumer_id != consumer_id);

        if consumers.len() < initial_len {
            info!(
                group_id = %self.group_id,
                consumer_id = %consumer_id,
                "Consumer removed from group"
            );
            Ok(())
        } else {
            Err(anyhow!("Consumer {} not found in group {}", consumer_id, self.group_id))
        }
    }

    /// Start all consumers in the group that aren't already running
    pub async fn start_all(&self) -> Result<()> {
        let consumers = self.consumers.read().await;
        let mut start_tasks = Vec::new();
        let mut started_count = 0;

        for consumer in consumers.iter() {
            // Check if consumer is already running
            let running = consumer.running.read().await;
            if !*running {
                drop(running); // Release the lock before spawning
                let consumer_clone = consumer.clone();
                start_tasks.push(tokio::spawn(async move {
                    consumer_clone.start().await
                }));
                started_count += 1;
            }
        }

        // Wait for all new consumers to start
        for task in start_tasks {
            task.await??;
        }

        info!(
            group_id = %self.group_id,
            consumer_count = consumers.len(),
            started_count = started_count,
            "Consumers in group started (skipped already running ones)"
        );

        Ok(())
    }

    /// Stop all consumers in the group
    pub async fn stop_all(&self) -> Result<()> {
        let consumers = self.consumers.read().await;
        let mut stop_tasks = Vec::new();

        for consumer in consumers.iter() {
            let consumer_clone = consumer.clone();
            stop_tasks.push(tokio::spawn(async move {
                consumer_clone.stop().await
            }));
        }

        // Wait for all consumers to stop
        for task in stop_tasks {
            task.await??;
        }

        info!(
            group_id = %self.group_id,
            consumer_count = consumers.len(),
            "All consumers in group stopped"
        );

        Ok(())
    }

    /// Get statistics for all consumers in the group
    pub async fn group_stats(&self) -> Vec<ConsumerStats> {
        let consumers = self.consumers.read().await;
        let mut stats_tasks = Vec::new();

        for consumer in consumers.iter() {
            let consumer_clone = consumer.clone();
            stats_tasks.push(tokio::spawn(async move {
                consumer_clone.stats().await
            }));
        }

        let mut all_stats = Vec::new();
        for task in stats_tasks {
            if let Ok(stats) = task.await {
                all_stats.push(stats);
            }
        }

        all_stats
    }

    /// Get the number of consumers in the group
    pub async fn consumer_count(&self) -> usize {
        self.consumers.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{Event, EventType, EventData};
    use crate::handlers::EventHandler;
    use zcore_storage::Store;
    use tempfile::tempdir;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct TestHandler {
        processed_count: AtomicU32,
        should_fail: std::sync::atomic::AtomicBool,
    }

    impl TestHandler {
        fn new() -> Self {
            Self {
                processed_count: AtomicU32::new(0),
                should_fail: std::sync::atomic::AtomicBool::new(false),
            }
        }

        fn get_count(&self) -> u32 {
            self.processed_count.load(Ordering::Acquire)
        }

        fn set_should_fail(&self, fail: bool) {
            self.should_fail.store(fail, Ordering::Release);
        }
    }

    #[async_trait::async_trait]
    impl EventHandler for TestHandler {
        async fn handle(&self, _event: &Event) -> HandlerResult {
            self.processed_count.fetch_add(1, Ordering::SeqCst);

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
    async fn test_consumer_processing() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let queue_config = crate::persistent_queue::PersistentQueueConfig::default();
        let queue = Arc::new(PersistentMessageQueue::new(storage, queue_config).await.unwrap());

        let handler = Arc::new(TestHandler::new());
        let consumer_config = ConsumerConfig {
            consumer_id: "test_consumer".to_string(),
            auto_start: false,
            ..Default::default()
        };

        let consumer = PersistentConsumer::new(consumer_config, queue.clone(), handler.clone());

        // Enqueue a message
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

        queue.enqueue(event, "test-topic".to_string()).await.unwrap();

        // Start consumer and wait for processing
        consumer.start().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        assert_eq!(handler.get_count(), 1);

        let stats = consumer.stats().await;
        assert_eq!(stats.messages_acked, 1);

        consumer.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_consumer_group() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let queue_config = crate::persistent_queue::PersistentQueueConfig::default();
        let queue = Arc::new(PersistentMessageQueue::new(storage, queue_config).await.unwrap());

        let group = ConsumerGroup::new("test_group".to_string(), queue.clone());

        // Add multiple consumers
        let handler1 = Arc::new(TestHandler::new());
        let handler2 = Arc::new(TestHandler::new());

        let config1 = ConsumerConfig {
            consumer_id: "consumer1".to_string(),
            auto_start: false,
            ..Default::default()
        };

        let config2 = ConsumerConfig {
            consumer_id: "consumer2".to_string(),
            auto_start: false,
            ..Default::default()
        };

        group.add_consumer(config1, handler1.clone()).await.unwrap();
        group.add_consumer(config2, handler2.clone()).await.unwrap();

        // Enqueue multiple messages
        for i in 0..10 {
            let event = Event::new(
                EventType::JobCreated,
                EventData::Job {
                    job_id: uuid::Uuid::new_v4(),
                    org_id: 1,
                    status: Some(format!("message_{}", i)),
                    worker_id: None,
                    error: None,
                    output: None,
                },
                1,
            );
            queue.enqueue(event, "test-topic".to_string()).await.unwrap();
        }

        // Start all consumers
        group.start_all().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // Check that messages were distributed across consumers
        let total_processed = handler1.get_count() + handler2.get_count();
        assert_eq!(total_processed, 10);

        group.stop_all().await.unwrap();
    }
}