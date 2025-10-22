use crate::events::{Event, EventType, EventData};
use crate::handlers::{EventHandler, HandlerResult};
use crate::persistent_queue::{PersistentMessageQueue, PersistentQueueConfig};
use crate::consumer::{PersistentConsumer, ConsumerConfig};
use crate::exactly_once_bus::{ExactlyOnceEventBus, ExactlyOnceEventBusBuilder};
use anyhow::Result;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::{RwLock};
use uuid::Uuid;
use zcore_storage::Store;

/// Test handler that tracks processing with configurable behavior
pub struct TestExactlyOnceHandler {
    pub id: String,
    pub processed_events: Arc<RwLock<Vec<String>>>,
    pub processing_count: Arc<AtomicU32>,
    pub should_fail_count: Arc<AtomicU32>,
    pub should_fail_until: Arc<AtomicU32>,
    pub processing_delay_ms: Arc<AtomicU64>,
}

impl TestExactlyOnceHandler {
    pub fn new(id: String) -> Self {
        Self {
            id,
            processed_events: Arc::new(RwLock::new(Vec::new())),
            processing_count: Arc::new(AtomicU32::new(0)),
            should_fail_count: Arc::new(AtomicU32::new(0)),
            should_fail_until: Arc::new(AtomicU32::new(0)),
            processing_delay_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn get_processed_count(&self) -> usize {
        self.processed_events.read().await.len()
    }

    pub async fn get_processed_events(&self) -> Vec<String> {
        self.processed_events.read().await.clone()
    }

    pub fn set_should_fail_until(&self, count: u32) {
        self.should_fail_until.store(count, Ordering::SeqCst);
    }

    pub fn set_processing_delay(&self, delay_ms: u64) {
        self.processing_delay_ms.store(delay_ms, Ordering::SeqCst);
    }

    pub fn get_processing_count(&self) -> u32 {
        self.processing_count.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl EventHandler for TestExactlyOnceHandler {
    async fn handle(&self, event: &Event) -> HandlerResult {
        let processing_count = self.processing_count.fetch_add(1, Ordering::SeqCst) + 1;

        // Add processing delay if configured
        let delay = self.processing_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }

        // Check if we should fail this processing attempt
        let should_fail_until = self.should_fail_until.load(Ordering::SeqCst);
        if processing_count <= should_fail_until {
            tracing::debug!(
                handler_id = %self.id,
                event_id = %event.id,
                processing_count = processing_count,
                should_fail_until = should_fail_until,
                "Handler configured to fail this attempt"
            );
            return HandlerResult::Retry;
        }

        // Successfully process the event
        self.processed_events.write().await.push(event.id.to_string());

        tracing::debug!(
            handler_id = %self.id,
            event_id = %event.id,
            processing_count = processing_count,
            "Event processed successfully"
        );

        HandlerResult::Success
    }

    fn interested_in(&self, _event_type: &EventType) -> bool {
        true
    }
}

/// Test that events are delivered exactly once even with retries
#[tokio::test]
async fn test_exactly_once_delivery_with_retries() -> Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test.redb");
    let storage = Arc::new(Store::open(&db_path)?);

    let config = PersistentQueueConfig {
        ack_timeout_secs: 2,
        max_delivery_attempts: 3,
        cleanup_interval_secs: 1,
        ..Default::default()
    };

    let bus = ExactlyOnceEventBusBuilder::new()
        .with_queue_config(config)
        .build(storage)
        .await?;

    bus.start().await?;

    let handler = Arc::new(TestExactlyOnceHandler::new("test_handler".to_string()));

    // Configure handler to fail first 2 attempts, succeed on 3rd
    handler.set_should_fail_until(2);

    let subscription_id = bus.subscribe(handler.clone()).await?;

    // Publish test events
    let mut event_ids = Vec::new();
    for i in 0..5 {
        let event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: Uuid::new_v4(),
                org_id: 1,
                status: Some(format!("test_job_{}", i)),
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );
        event_ids.push(event.id.to_string());
        bus.publish(event).await?;
    }

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Verify exactly-once delivery
    let processed_count = handler.get_processed_count().await;
    let processing_attempts = handler.get_processing_count();
    let processed_events = handler.get_processed_events().await;

    assert_eq!(processed_count, 5, "Should process exactly 5 events");
    assert_eq!(processing_attempts, 15, "Should have 15 total processing attempts (3 per event)");

    // Check that each event was processed exactly once
    for event_id in event_ids {
        let count = processed_events.iter().filter(|id| **id == event_id).count();
        assert_eq!(count, 1, "Event {} should be processed exactly once", event_id);
    }

    bus.unsubscribe(&subscription_id).await?;
    bus.shutdown().await?;
    Ok(())
}

/// Test that messages are not lost during consumer restarts
#[tokio::test]
async fn test_crash_recovery_with_offset_tracking() -> Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test.redb");
    let storage = Arc::new(Store::open(&db_path)?);

    let config = PersistentQueueConfig {
        ack_timeout_secs: 1,
        max_delivery_attempts: 5,
        cleanup_interval_secs: 1,
        ..Default::default()
    };

    let bus = ExactlyOnceEventBusBuilder::new()
        .with_queue_config(config.clone())
        .build(storage.clone())
        .await?;

    bus.start().await?;

    // Publish events before any consumers
    let mut event_ids = Vec::new();
    for i in 0..10 {
        let event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: Uuid::new_v4(),
                org_id: 1,
                status: Some(format!("crash_test_{}", i)),
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );
        event_ids.push(event.id.to_string());
        bus.publish(event).await?;
    }

    let handler1 = Arc::new(TestExactlyOnceHandler::new("handler1".to_string()));
    let subscription_id1 = bus.subscribe(handler1.clone()).await?;

    // Let it process some events
    tokio::time::sleep(Duration::from_secs(2)).await;

    let processed_before_restart = handler1.get_processed_count().await;

    // Simulate crash by unsubscribing
    bus.unsubscribe(&subscription_id1).await?;

    // Create new bus instance (simulating restart)
    let bus2 = ExactlyOnceEventBusBuilder::new()
        .with_queue_config(config)
        .build(storage)
        .await?;

    bus2.start().await?;

    let handler2 = Arc::new(TestExactlyOnceHandler::new("handler2".to_string()));
    let _subscription_id2 = bus2.subscribe(handler2.clone()).await?;

    // Wait for recovery processing
    tokio::time::sleep(Duration::from_secs(3)).await;

    let processed_after_restart = handler2.get_processed_count().await;
    let total_processed = processed_before_restart + processed_after_restart;

    assert_eq!(total_processed, 10, "All events should be processed exactly once across restarts");

    bus.shutdown().await?;
    bus2.shutdown().await?;
    Ok(())
}

/// Test deduplication prevents duplicate processing
#[tokio::test]
async fn test_message_deduplication() -> Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test.redb");
    let storage = Arc::new(Store::open(&db_path)?);

    let config = PersistentQueueConfig {
        enable_deduplication: true,
        deduplication_window_hours: 1,
        ..Default::default()
    };

    let bus = ExactlyOnceEventBusBuilder::new()
        .with_queue_config(config)
        .build(storage)
        .await?;

    bus.start().await?;

    let handler = Arc::new(TestExactlyOnceHandler::new("dedup_handler".to_string()));
    let _subscription_id = bus.subscribe(handler.clone()).await?;

    // Create an event
    let event = Event::new(
        EventType::JobCreated,
        EventData::Job {
            job_id: Uuid::new_v4(),
            org_id: 1,
            status: Some("duplicate_test".to_string()),
            worker_id: None,
            error: None,
            output: None,
        },
        1,
    );

    // Publish the same event multiple times (same event ID)
    for _ in 0..5 {
        let duplicate_event = Event {
            id: event.id, // Same ID
            ..event.clone()
        };
        bus.publish(duplicate_event).await?;
    }

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(2)).await;

    let processed_count = handler.get_processed_count().await;
    let stats = bus.stats().await;

    assert_eq!(processed_count, 1, "Should process the event exactly once despite duplicates");
    assert_eq!(stats.queue_stats.duplicate_messages, 4, "Should detect 4 duplicate messages");

    bus.shutdown().await?;
    Ok(())
}

/// Test that ACK timeout causes redelivery
#[tokio::test]
async fn test_ack_timeout_redelivery() -> Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test.redb");
    let storage = Arc::new(Store::open(&db_path)?);

    let config = PersistentQueueConfig {
        ack_timeout_secs: 1, // Very short timeout
        max_delivery_attempts: 2,
        cleanup_interval_secs: 1,
        ..Default::default()
    };

    let queue = Arc::new(PersistentMessageQueue::new(storage, config).await?);

    // Create a slow handler that doesn't ACK in time
    let handler = Arc::new(TestExactlyOnceHandler::new("slow_handler".to_string()));
    handler.set_processing_delay(2000); // 2 second delay, longer than ACK timeout

    let consumer_config = ConsumerConfig {
        consumer_id: "slow_consumer".to_string(),
        processing_timeout_secs: 5,
        poll_interval_ms: 100,
        auto_start: false,
        ..Default::default()
    };

    let consumer = PersistentConsumer::new(consumer_config, queue.clone(), handler.clone());
    consumer.start().await?;

    // Publish an event
    let event = Event::new(
        EventType::JobCreated,
        EventData::Job {
            job_id: Uuid::new_v4(),
            org_id: 1,
            status: Some("timeout_test".to_string()),
            worker_id: None,
            error: None,
            output: None,
        },
        1,
    );

    queue.enqueue(event, "test-topic".to_string()).await?;

    // Wait for initial processing and timeout
    tokio::time::sleep(Duration::from_secs(6)).await;

    let processing_attempts = handler.get_processing_count();
    let processed_count = handler.get_processed_count().await;

    // Should have multiple processing attempts due to timeout
    assert!(processing_attempts >= 2, "Should have multiple processing attempts due to ACK timeout");
    assert_eq!(processed_count, 1, "Should eventually process the event exactly once");

    consumer.stop().await?;
    queue.shutdown().await?;
    Ok(())
}

/// Test dead letter queue functionality
#[tokio::test]
async fn test_dead_letter_queue() -> Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test.redb");
    let storage = Arc::new(Store::open(&db_path)?);

    let config = PersistentQueueConfig {
        max_delivery_attempts: 2,
        ack_timeout_secs: 1,
        cleanup_interval_secs: 1,
        ..Default::default()
    };

    let bus = ExactlyOnceEventBusBuilder::new()
        .with_queue_config(config)
        .build(storage)
        .await?;

    bus.start().await?;

    // Handler that always fails
    let handler = Arc::new(TestExactlyOnceHandler::new("failing_handler".to_string()));
    handler.set_should_fail_until(10); // Fail more than max attempts

    let _subscription_id = bus.subscribe(handler.clone()).await?;

    // Publish events
    for i in 0..3 {
        let event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: Uuid::new_v4(),
                org_id: 1,
                status: Some(format!("failing_job_{}", i)),
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );
        bus.publish(event).await?;
    }

    // Wait for processing attempts and dead lettering
    tokio::time::sleep(Duration::from_secs(5)).await;

    let stats = bus.stats().await;
    let processed_count = handler.get_processed_count().await;

    assert_eq!(processed_count, 0, "Should not successfully process any events");
    assert!(stats.queue_stats.dead_letter_messages > 0, "Should have messages in dead letter queue");

    bus.shutdown().await?;
    Ok(())
}

/// Test multiple consumers processing different events
#[tokio::test]
async fn test_multiple_consumers_load_distribution() -> Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test.redb");
    let storage = Arc::new(Store::open(&db_path)?);

    let bus = ExactlyOnceEventBusBuilder::new()
        .build(storage)
        .await?;

    bus.start().await?;

    // Create multiple handlers
    let handler1 = Arc::new(TestExactlyOnceHandler::new("handler1".to_string()));
    let handler2 = Arc::new(TestExactlyOnceHandler::new("handler2".to_string()));
    let handler3 = Arc::new(TestExactlyOnceHandler::new("handler3".to_string()));

    let _sub1 = bus.subscribe_topics(handler1.clone(), vec!["jobs/**".to_string()]).await?;
    let _sub2 = bus.subscribe_topics(handler2.clone(), vec!["jobs/**".to_string()]).await?;
    let _sub3 = bus.subscribe_topics(handler3.clone(), vec!["jobs/**".to_string()]).await?;

    // Publish many events
    for i in 0..30 {
        let event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: Uuid::new_v4(),
                org_id: 1,
                status: Some(format!("load_test_job_{}", i)),
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );
        bus.publish(event).await?;
    }

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(5)).await;

    let count1 = handler1.get_processed_count().await;
    let count2 = handler2.get_processed_count().await;
    let count3 = handler3.get_processed_count().await;
    let total_processed = count1 + count2 + count3;

    assert_eq!(total_processed, 30, "Each event should be processed exactly once with load balancing (30 events total)");

    // With load balancing, events should be distributed among handlers
    // Each handler should process some events, but total should equal number of published events
    assert!(count1 + count2 + count3 == 30, "Total processed should equal published events");
    println!("Handler distribution: h1={}, h2={}, h3={}", count1, count2, count3);

    bus.shutdown().await?;
    Ok(())
}

/// Test bus health checking functionality
#[tokio::test]
async fn test_bus_health_monitoring() -> Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test.redb");
    let storage = Arc::new(Store::open(&db_path)?);

    let bus = ExactlyOnceEventBusBuilder::new()
        .build(storage)
        .await?;

    bus.start().await?;

    // Initially should be healthy
    assert!(bus.health_check().await?, "Bus should be healthy initially");

    let handler = Arc::new(TestExactlyOnceHandler::new("health_test_handler".to_string()));
    let _subscription_id = bus.subscribe(handler.clone()).await?;

    // Should still be healthy with active consumers
    assert!(bus.health_check().await?, "Bus should be healthy with active consumers");

    // Test stats collection
    let stats = bus.stats().await;
    assert_eq!(stats.active_subscriptions, 1, "Should have 1 active subscription");
    assert_eq!(stats.persistent_subscriptions, 0, "Should have 0 persistent subscriptions");

    bus.shutdown().await?;
    Ok(())
}

/// Performance test to ensure exactly-once delivery doesn't significantly impact throughput
#[tokio::test]
async fn test_exactly_once_performance() -> Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test.redb");
    let storage = Arc::new(Store::open(&db_path)?);

    let config = PersistentQueueConfig {
        memory_cache_size: 5000,
        cleanup_interval_secs: 30,
        ..Default::default()
    };

    let bus = ExactlyOnceEventBusBuilder::new()
        .with_queue_config(config)
        .build(storage)
        .await?;

    bus.start().await?;

    let handler = Arc::new(TestExactlyOnceHandler::new("perf_handler".to_string()));
    let _subscription_id = bus.subscribe(handler.clone()).await?;

    let start_time = std::time::Instant::now();
    let num_events = 1000;

    // Publish many events quickly
    for i in 0..num_events {
        let event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: Uuid::new_v4(),
                org_id: 1,
                status: Some(format!("perf_job_{}", i)),
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );
        bus.publish(event).await?;
    }

    let publish_duration = start_time.elapsed();

    // Wait for all processing to complete
    let process_start = std::time::Instant::now();
    loop {
        let processed_count = handler.get_processed_count().await;
        if processed_count >= num_events {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let total_duration = process_start.elapsed();

    let processed_count = handler.get_processed_count().await;
    assert_eq!(processed_count, num_events, "Should process all events exactly once");

    let publish_throughput = num_events as f64 / publish_duration.as_secs_f64();
    let process_throughput = num_events as f64 / total_duration.as_secs_f64();

    println!("Performance Results:");
    println!("  Events: {}", num_events);
    println!("  Publish throughput: {:.2} events/sec", publish_throughput);
    println!("  Process throughput: {:.2} events/sec", process_throughput);
    println!("  Total publish time: {:?}", publish_duration);
    println!("  Total process time: {:?}", total_duration);

    // Basic performance assertions - these should be quite fast
    assert!(publish_throughput > 1000.0, "Publish throughput should be > 1000 events/sec");
    assert!(process_throughput > 500.0, "Process throughput should be > 500 events/sec");

    bus.shutdown().await?;
    Ok(())
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    /// Helper function to create a test event
    pub fn create_test_event(id: u32, org_id: u64) -> Event {
        Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: Uuid::new_v4(),
                org_id,
                status: Some(format!("test_event_{}", id)),
                worker_id: None,
                error: None,
                output: None,
            },
            org_id,
        )
    }

    /// Helper to wait for condition with timeout
    pub async fn wait_for_condition<F, Fut>(mut condition: F, timeout: Duration) -> bool
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if condition().await {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }
}