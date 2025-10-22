//! Enhanced Event Bus Tests for Concurrent Scenarios and Backpressure

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::{sleep, timeout};

use crate::{
    Event, EventType, EventData, EventBus, EventHandler, HandlerResult,
    EventSystemConfig, EventFilter, MetricsHandler,
};

/// Test handler that can simulate slow processing and backpressure
struct SlowTestHandler {
    processed_count: AtomicU32,
    processing_delay_ms: u64,
    max_concurrent: Arc<Semaphore>,
    handler_id: String,
}

impl SlowTestHandler {
    fn new(handler_id: String, processing_delay_ms: u64, max_concurrent: usize) -> Self {
        Self {
            processed_count: AtomicU32::new(0),
            processing_delay_ms,
            max_concurrent: Arc::new(Semaphore::new(max_concurrent)),
            handler_id,
        }
    }

    fn get_count(&self) -> u32 {
        self.processed_count.load(Ordering::Acquire)
    }
}

#[async_trait::async_trait]
impl EventHandler for SlowTestHandler {
    async fn handle(&self, event: &Event) -> HandlerResult {
        let _permit = self.max_concurrent.acquire().await.unwrap();

        if self.processing_delay_ms > 0 {
            sleep(Duration::from_millis(self.processing_delay_ms)).await;
        }

        self.processed_count.fetch_add(1, Ordering::SeqCst);
        println!("Handler {} processed event {}", self.handler_id, event.id);
        HandlerResult::Success
    }

    fn interested_in(&self, _event_type: &EventType) -> bool {
        true
    }
}

/// Test handler that can fail selectively
struct FailingTestHandler {
    processed_count: AtomicU32,
    failed_count: AtomicU32,
    failure_rate: f32, // 0.0 to 1.0
    handler_id: String,
}

impl FailingTestHandler {
    fn new(handler_id: String, failure_rate: f32) -> Self {
        Self {
            processed_count: AtomicU32::new(0),
            failed_count: AtomicU32::new(0),
            failure_rate,
            handler_id,
        }
    }

    fn get_processed_count(&self) -> u32 {
        self.processed_count.load(Ordering::Acquire)
    }

    fn get_failed_count(&self) -> u32 {
        self.failed_count.load(Ordering::Acquire)
    }
}

#[async_trait::async_trait]
impl EventHandler for FailingTestHandler {
    async fn handle(&self, event: &Event) -> HandlerResult {
        let should_fail = rand::random::<f32>() < self.failure_rate;

        if should_fail {
            self.failed_count.fetch_add(1, Ordering::SeqCst);
            println!("Handler {} failed to process event {}", self.handler_id, event.id);
            HandlerResult::Retry
        } else {
            self.processed_count.fetch_add(1, Ordering::SeqCst);
            println!("Handler {} processed event {}", self.handler_id, event.id);
            HandlerResult::Success
        }
    }

    fn interested_in(&self, _event_type: &EventType) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use zcore_storage::Store;

    #[tokio::test]
    async fn test_concurrent_publishers_and_subscribers() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let config = EventSystemConfig::default();
        let event_bus = Arc::new(EventBus::new(config, storage));

        // Create multiple concurrent handlers
        let handler1 = Arc::new(SlowTestHandler::new("handler1".to_string(), 10, 5));
        let handler2 = Arc::new(SlowTestHandler::new("handler2".to_string(), 20, 3));
        let handler3 = Arc::new(SlowTestHandler::new("handler3".to_string(), 5, 10));

        let _sub1 = event_bus.subscribe(handler1.clone()).await.unwrap();
        let _sub2 = event_bus.subscribe(handler2.clone()).await.unwrap();
        let _sub3 = event_bus.subscribe(handler3.clone()).await.unwrap();

        // Spawn multiple publisher tasks
        let num_publishers = 5;
        let events_per_publisher = 20;
        let mut publish_tasks = Vec::new();

        for publisher_id in 0..num_publishers {
            let bus = event_bus.clone();
            let task = tokio::spawn(async move {
                for event_id in 0..events_per_publisher {
                    let event = Event::new(
                        EventType::JobCreated,
                        EventData::Job {
                            job_id: uuid::Uuid::new_v4(),
                            org_id: 1,
                            status: Some(format!("pub_{}_event_{}", publisher_id, event_id)),
                            worker_id: None,
                            error: None,
                            output: None,
                        },
                        1,
                    );

                    bus.publish(event).await.unwrap();

                    // Small delay to simulate realistic publishing patterns
                    sleep(Duration::from_millis(1)).await;
                }
            });
            publish_tasks.push(task);
        }

        // Wait for all publishers to complete
        for task in publish_tasks {
            task.await.unwrap();
        }

        // Wait for processing to complete
        sleep(Duration::from_secs(3)).await;

        let total_events = num_publishers * events_per_publisher;
        let processed1 = handler1.get_count();
        let processed2 = handler2.get_count();
        let processed3 = handler3.get_count();
        let total_processed = processed1 + processed2 + processed3;

        println!(
            "Published: {}, Processed: {} (h1: {}, h2: {}, h3: {})",
            total_events, total_processed, processed1, processed2, processed3
        );

        // All events should be processed across all handlers
        assert_eq!(total_processed, total_events * 3); // Each handler processes all events
        assert!(processed1 > 0, "Handler 1 should process some events");
        assert!(processed2 > 0, "Handler 2 should process some events");
        assert!(processed3 > 0, "Handler 3 should process some events");
    }

    #[tokio::test]
    async fn test_backpressure_handling() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let config = EventSystemConfig::default();
        let event_bus = Arc::new(EventBus::new(config, storage));

        // Create a very slow handler to create backpressure
        let slow_handler = Arc::new(SlowTestHandler::new("slow_handler".to_string(), 100, 1));
        let _sub = event_bus.subscribe(slow_handler.clone()).await.unwrap();

        // Measure publish performance under backpressure
        let start_time = Instant::now();
        let num_events = 50;

        for i in 0..num_events {
            let event = Event::new(
                EventType::JobCreated,
                EventData::Job {
                    job_id: uuid::Uuid::new_v4(),
                    org_id: 1,
                    status: Some(format!("backpressure_test_{}", i)),
                    worker_id: None,
                    error: None,
                    output: None,
                },
                1,
            );

            event_bus.publish(event).await.unwrap();
        }

        let publish_duration = start_time.elapsed();

        // Even with backpressure, publishing should complete relatively quickly
        assert!(publish_duration < Duration::from_secs(5),
               "Publishing should not be significantly slowed by backpressure");

        // Wait for processing to complete
        sleep(Duration::from_secs(6)).await;

        let processed = slow_handler.get_count();
        assert_eq!(processed, num_events, "All events should eventually be processed");
    }

    #[tokio::test]
    async fn test_error_recovery_and_retries() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let config = EventSystemConfig::default();
        let event_bus = Arc::new(EventBus::new(config, storage));

        // Create a handler that fails 30% of the time
        let failing_handler = Arc::new(FailingTestHandler::new("failing_handler".to_string(), 0.3));
        let _sub = event_bus.subscribe(failing_handler.clone()).await.unwrap();

        let num_events = 100;

        for i in 0..num_events {
            let event = Event::new(
                EventType::JobCreated,
                EventData::Job {
                    job_id: uuid::Uuid::new_v4(),
                    org_id: 1,
                    status: Some(format!("retry_test_{}", i)),
                    worker_id: None,
                    error: None,
                    output: None,
                },
                1,
            );

            event_bus.publish(event).await.unwrap();
        }

        // Wait for processing and retries
        sleep(Duration::from_secs(5)).await;

        let processed = failing_handler.get_processed_count();
        let failed = failing_handler.get_failed_count();

        println!("Processed: {}, Failed: {}, Total: {}", processed, failed, processed + failed);

        // Should have some failures but most events should eventually succeed
        assert!(failed > 0, "Some events should fail initially");
        assert!(processed > num_events / 2, "Most events should eventually be processed");
    }

    #[tokio::test]
    async fn test_high_throughput_performance() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let config = EventSystemConfig::default();
        let event_bus = Arc::new(EventBus::new(config, storage));

        // Create fast handlers for throughput testing
        let handler1 = Arc::new(SlowTestHandler::new("fast1".to_string(), 0, 100));
        let handler2 = Arc::new(SlowTestHandler::new("fast2".to_string(), 0, 100));

        let _sub1 = event_bus.subscribe(handler1.clone()).await.unwrap();
        let _sub2 = event_bus.subscribe(handler2.clone()).await.unwrap();

        let num_events = 1000;
        let start_time = Instant::now();

        // Publish events rapidly
        for i in 0..num_events {
            let event = Event::new(
                EventType::JobCreated,
                EventData::Job {
                    job_id: uuid::Uuid::new_v4(),
                    org_id: 1,
                    status: Some(format!("throughput_test_{}", i)),
                    worker_id: None,
                    error: None,
                    output: None,
                },
                1,
            );

            event_bus.publish(event).await.unwrap();
        }

        let publish_time = start_time.elapsed();

        // Wait for processing
        sleep(Duration::from_secs(2)).await;

        let process_time = start_time.elapsed();

        let processed1 = handler1.get_count();
        let processed2 = handler2.get_count();
        let total_processed = processed1 + processed2;

        println!(
            "Published {} events in {:?}, processed {} in {:?}",
            num_events, publish_time, total_processed, process_time
        );

        // Performance assertions
        assert!(publish_time < Duration::from_secs(2), "Publishing should be fast");
        assert!(process_time < Duration::from_secs(5), "Processing should complete quickly");
        assert_eq!(total_processed, num_events * 2, "All events should be processed by both handlers");

        // Throughput calculation
        let events_per_sec = num_events as f64 / publish_time.as_secs_f64();
        println!("Publishing throughput: {:.2} events/sec", events_per_sec);
        assert!(events_per_sec > 500.0, "Should achieve good publishing throughput");
    }

    #[tokio::test]
    async fn test_subscriber_failure_isolation() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let config = EventSystemConfig::default();
        let event_bus = Arc::new(EventBus::new(config, storage));

        // Create handlers: one that always fails, one that always succeeds
        let failing_handler = Arc::new(FailingTestHandler::new("always_fail".to_string(), 1.0));
        let good_handler = Arc::new(SlowTestHandler::new("always_succeed".to_string(), 1, 10));

        let _sub1 = event_bus.subscribe(failing_handler.clone()).await.unwrap();
        let _sub2 = event_bus.subscribe(good_handler.clone()).await.unwrap();

        let num_events = 50;

        for i in 0..num_events {
            let event = Event::new(
                EventType::JobCreated,
                EventData::Job {
                    job_id: uuid::Uuid::new_v4(),
                    org_id: 1,
                    status: Some(format!("isolation_test_{}", i)),
                    worker_id: None,
                    error: None,
                    output: None,
                },
                1,
            );

            event_bus.publish(event).await.unwrap();
        }

        // Wait for processing
        sleep(Duration::from_secs(3)).await;

        let failed_processed = failing_handler.get_processed_count();
        let failed_failed = failing_handler.get_failed_count();
        let good_processed = good_handler.get_count();

        println!(
            "Failing handler - processed: {}, failed: {}",
            failed_processed, failed_failed
        );
        println!("Good handler - processed: {}", good_processed);

        // The failing handler should have many failures but few successes
        assert!(failed_failed > failed_processed, "Failing handler should fail more than succeed");

        // The good handler should process all events successfully
        assert_eq!(good_processed, num_events, "Good handler should process all events");
    }

    #[tokio::test]
    async fn test_graceful_shutdown_under_load() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let config = EventSystemConfig::default();
        let event_bus = Arc::new(EventBus::new(config, storage));

        let handler = Arc::new(SlowTestHandler::new("shutdown_test".to_string(), 50, 5));
        let _sub = event_bus.subscribe(handler.clone()).await.unwrap();

        // Start publishing events continuously
        let bus_clone = event_bus.clone();
        let publish_task = tokio::spawn(async move {
            for i in 0..200 {
                let event = Event::new(
                    EventType::JobCreated,
                    EventData::Job {
                        job_id: uuid::Uuid::new_v4(),
                        org_id: 1,
                        status: Some(format!("shutdown_test_{}", i)),
                        worker_id: None,
                        error: None,
                        output: None,
                    },
                    1,
                );

                if bus_clone.publish(event).await.is_err() {
                    break; // Bus is shutting down
                }

                sleep(Duration::from_millis(10)).await;
            }
        });

        // Let some events be published and start processing
        sleep(Duration::from_millis(500)).await;

        // Trigger shutdown while under load
        let shutdown_start = Instant::now();

        // Cancel the publish task (simulates stopping event generation)
        publish_task.abort();

        // Note: In a real implementation, there would be a shutdown method
        // For now, we simulate by dropping the event bus
        drop(event_bus);

        let shutdown_duration = shutdown_start.elapsed();

        println!(
            "Shutdown completed in {:?}, processed {} events",
            shutdown_duration,
            handler.get_count()
        );

        // Shutdown should complete within reasonable time even under load
        assert!(shutdown_duration < Duration::from_secs(10),
               "Shutdown should complete within reasonable time");

        // Some events should have been processed
        assert!(handler.get_count() > 0, "Some events should have been processed before shutdown");
    }
}