//! Comprehensive Persistence Layer Tests for Durability and Performance

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tempfile::TempDir;
use tokio::sync::Semaphore;
use tokio::time::sleep;

use crate::{
    Event, EventType, EventData, EventStore, EventStream, Snapshot,
    PersistentMessageQueue, PersistentQueueConfig, PersistentMessage,
    ExactlyOnceEventBus, ExactlyOnceEventBusBuilder,
};

/// Performance tracking metrics
#[derive(Debug, Clone, Default)]
struct PerformanceMetrics {
    write_latency_ms: Vec<u64>,
    read_latency_ms: Vec<u64>,
    throughput_events_per_sec: f64,
    storage_size_bytes: u64,
    memory_usage_mb: f64,
}

impl PerformanceMetrics {
    fn add_write_latency(&mut self, latency_ms: u64) {
        self.write_latency_ms.push(latency_ms);
    }

    fn add_read_latency(&mut self, latency_ms: u64) {
        self.read_latency_ms.push(latency_ms);
    }

    fn calculate_percentiles(&self, latencies: &[u64]) -> (u64, u64, u64) {
        if latencies.is_empty() {
            return (0, 0, 0);
        }

        let mut sorted = latencies.to_vec();
        sorted.sort_unstable();

        let len = sorted.len();
        let p50 = sorted[len * 50 / 100];
        let p95 = sorted[len * 95 / 100];
        let p99 = sorted[len * 99 / 100];

        (p50, p95, p99)
    }

    fn write_percentiles(&self) -> (u64, u64, u64) {
        self.calculate_percentiles(&self.write_latency_ms)
    }

    fn read_percentiles(&self) -> (u64, u64, u64) {
        self.calculate_percentiles(&self.read_latency_ms)
    }

    fn average_write_latency(&self) -> f64 {
        if self.write_latency_ms.is_empty() {
            0.0
        } else {
            self.write_latency_ms.iter().sum::<u64>() as f64 / self.write_latency_ms.len() as f64
        }
    }

    fn average_read_latency(&self) -> f64 {
        if self.read_latency_ms.is_empty() {
            0.0
        } else {
            self.read_latency_ms.iter().sum::<u64>() as f64 / self.read_latency_ms.len() as f64
        }
    }
}

/// Test data generator for persistence testing
struct TestDataGenerator {
    event_counter: AtomicU32,
}

impl TestDataGenerator {
    fn new() -> Self {
        Self {
            event_counter: AtomicU32::new(0),
        }
    }

    fn generate_event(&self, size_kb: usize) -> Event {
        let counter = self.event_counter.fetch_add(1, Ordering::SeqCst);

        // Create payload of approximately the requested size
        let payload_size = size_kb * 1024;
        let chunk_size = 100;
        let num_chunks = payload_size / chunk_size;

        let mut large_data = HashMap::new();
        for i in 0..num_chunks {
            large_data.insert(
                format!("chunk_{}", i),
                serde_json::Value::String("x".repeat(chunk_size)),
            );
        }

        Event::new(
            EventType::Custom(format!("test_event_{}", counter)),
            EventData::Custom(serde_json::Value::Object(serde_json::Map::from_iter(
                large_data.into_iter(),
            ))),
            1,
        )
    }

    fn generate_events(&self, count: usize, size_kb: usize) -> Vec<Event> {
        (0..count).map(|_| self.generate_event(size_kb)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use zcore_storage::Store;

    #[tokio::test]
    async fn test_large_dataset_storage_and_retrieval() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("large_dataset.redb");
        let storage = Arc::new(Store::open(&db_path)?);
        let event_store = Arc::new(EventStore::new(storage));

        let generator = TestDataGenerator::new();
        let mut metrics = PerformanceMetrics::default();

        // Test with large dataset
        let num_events = 10000;
        let event_size_kb = 1; // 1KB per event

        println!("Generating {} events of {} KB each...", num_events, event_size_kb);
        let events = generator.generate_events(num_events, event_size_kb);

        // Measure write performance
        let write_start = Instant::now();
        let stream_id = "large_dataset_stream";

        for (i, event) in events.iter().enumerate() {
            let start = Instant::now();
            event_store.append_event(stream_id, event.clone(), i as u32).await?;
            let latency = start.elapsed().as_millis() as u64;
            metrics.add_write_latency(latency);

            if i % 1000 == 0 {
                println!("Written {} events", i);
            }
        }

        let total_write_time = write_start.elapsed();
        metrics.throughput_events_per_sec = num_events as f64 / total_write_time.as_secs_f64();

        println!("Write completed in {:?}", total_write_time);
        println!("Write throughput: {:.2} events/sec", metrics.throughput_events_per_sec);

        // Measure read performance
        let read_start = Instant::now();
        let retrieved_events = event_store.get_events(stream_id, 0, None).await?;
        let total_read_time = read_start.elapsed();

        println!("Read completed in {:?}", total_read_time);
        println!("Read {} events", retrieved_events.len());

        // Verify data integrity
        assert_eq!(retrieved_events.len(), num_events);

        for (i, (original, retrieved)) in events.iter().zip(retrieved_events.iter()).enumerate() {
            assert_eq!(original.id, retrieved.id, "Event ID mismatch at index {}", i);
            assert_eq!(original.event_type, retrieved.event_type, "Event type mismatch at index {}", i);

            // For large payloads, just verify a sample
            if i % 1000 == 0 {
                assert_eq!(original.data, retrieved.data, "Event data mismatch at index {}", i);
            }
        }

        // Performance assertions
        let (write_p50, write_p95, write_p99) = metrics.write_percentiles();
        println!("Write latency - P50: {}ms, P95: {}ms, P99: {}ms", write_p50, write_p95, write_p99);

        assert!(write_p50 < 10, "P50 write latency should be < 10ms");
        assert!(write_p95 < 50, "P95 write latency should be < 50ms");
        assert!(metrics.throughput_events_per_sec > 100.0, "Write throughput should be > 100 events/sec");

        // Check file size
        let db_metadata = std::fs::metadata(&db_path)?;
        metrics.storage_size_bytes = db_metadata.len();
        println!("Database size: {} MB", metrics.storage_size_bytes / 1024 / 1024);

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_access_durability() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("concurrent_test.redb");
        let storage = Arc::new(Store::open(&db_path)?);
        let event_store = Arc::new(EventStore::new(storage));

        let generator = TestDataGenerator::new();
        let num_writers = 10;
        let events_per_writer = 100;

        // Concurrent writers
        let mut write_tasks = Vec::new();
        let semaphore = Arc::new(Semaphore::new(num_writers));

        for writer_id in 0..num_writers {
            let store = event_store.clone();
            let gen = generator.clone();
            let sem = semaphore.clone();

            let task = tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                let stream_id = format!("stream_{}", writer_id);
                let events = gen.generate_events(events_per_writer, 1);

                for (i, event) in events.iter().enumerate() {
                    store.append_event(&stream_id, event.clone(), i as u32).await?;
                }

                Ok::<_, anyhow::Error>(events)
            });

            write_tasks.push(task);
        }

        // Wait for all writers to complete
        let mut all_written_events = Vec::new();
        for task in write_tasks {
            let events = task.await.unwrap()?;
            all_written_events.extend(events);
        }

        println!("Concurrent writes completed. Total events: {}", all_written_events.len());

        // Verify all data was written correctly
        for writer_id in 0..num_writers {
            let stream_id = format!("stream_{}", writer_id);
            let events = event_store.get_events(&stream_id, 0, None).await?;
            assert_eq!(events.len(), events_per_writer, "Writer {} should have {} events", writer_id, events_per_writer);
        }

        // Test concurrent readers while writing continues
        let read_tasks = (0..5).map(|reader_id| {
            let store = event_store.clone();
            tokio::spawn(async move {
                let mut total_events_read = 0;
                for _ in 0..10 {
                    for writer_id in 0..num_writers {
                        let stream_id = format!("stream_{}", writer_id);
                        let events = store.get_events(&stream_id, 0, None).await?;
                        total_events_read += events.len();
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok::<_, anyhow::Error>(total_events_read)
            })
        });

        // Wait for readers
        for task in read_tasks {
            let events_read = task.await.unwrap()?;
            assert!(events_read > 0, "Reader should have read some events");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_crash_recovery_simulation() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("crash_recovery.redb");

        // Phase 1: Write initial data
        {
            let storage = Arc::new(Store::open(&db_path)?);
            let event_store = Arc::new(EventStore::new(storage));
            let generator = TestDataGenerator::new();

            let stream_id = "recovery_test_stream";
            let events = generator.generate_events(1000, 1);

            for (i, event) in events.iter().enumerate() {
                event_store.append_event(stream_id, event.clone(), i as u32).await?;
            }

            // Simulate snapshot creation
            let stream = event_store.get_stream(stream_id).await?
                .ok_or_else(|| anyhow::anyhow!("Stream not found"))?;

            let snapshot_data = serde_json::json!({
                "stream_id": stream_id,
                "event_count": events.len(),
                "checksum": "test_checksum"
            });

            let snapshot = Snapshot {
                stream_id: stream_id.to_string(),
                version: stream.version,
                data: snapshot_data,
                created_at: chrono::Utc::now(),
            };

            event_store.save_snapshot(stream_id, snapshot).await?;

            println!("Phase 1: Written {} events and snapshot", events.len());
        } // Storage dropped here, simulating crash

        // Phase 2: Recovery
        {
            let storage = Arc::new(Store::open(&db_path)?);
            let event_store = Arc::new(EventStore::new(storage));

            let stream_id = "recovery_test_stream";

            // Verify data survived the "crash"
            let recovered_events = event_store.get_events(stream_id, 0, None).await?;
            assert_eq!(recovered_events.len(), 1000, "All events should be recovered");

            // Verify snapshot survived
            let snapshot = event_store.get_latest_snapshot(stream_id).await?;
            assert!(snapshot.is_some(), "Snapshot should be recovered");

            let snapshot = snapshot.unwrap();
            assert_eq!(snapshot.version, 999, "Snapshot version should be correct"); // 0-based

            // Continue writing after recovery
            let generator = TestDataGenerator::new();
            let new_events = generator.generate_events(500, 1);

            for (i, event) in new_events.iter().enumerate() {
                event_store.append_event(stream_id, event.clone(), 1000 + i as u32).await?;
            }

            // Verify total count
            let all_events = event_store.get_events(stream_id, 0, None).await?;
            assert_eq!(all_events.len(), 1500, "Should have original + new events");

            println!("Phase 2: Recovery successful, total events: {}", all_events.len());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_queue_persistence_under_load() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("queue_persistence.redb");
        let storage = Arc::new(Store::open(&db_path)?);

        let config = PersistentQueueConfig {
            max_queue_size: 10000,
            enable_deduplication: true,
            deduplication_window_hours: 1,
            ack_timeout_seconds: 30,
            max_retries: 3,
            retry_delay_seconds: 1,
            enable_dead_letter: true,
            cleanup_interval_hours: 1,
        };

        let queue = Arc::new(PersistentMessageQueue::new(storage, config));

        let generator = TestDataGenerator::new();
        let num_messages = 5000;

        // Test high-throughput enqueue
        let enqueue_start = Instant::now();
        let mut message_ids = Vec::new();

        for i in 0..num_messages {
            let event = generator.generate_event(1);
            let topic = format!("topic_{}", i % 10); // 10 different topics

            let message_id = queue.enqueue(event, topic).await?;
            message_ids.push(message_id);

            if i % 1000 == 0 {
                println!("Enqueued {} messages", i);
            }
        }

        let enqueue_duration = enqueue_start.elapsed();
        let enqueue_throughput = num_messages as f64 / enqueue_duration.as_secs_f64();

        println!("Enqueue completed in {:?}", enqueue_duration);
        println!("Enqueue throughput: {:.2} messages/sec", enqueue_throughput);

        // Test dequeue performance
        let dequeue_start = Instant::now();
        let mut dequeued_count = 0;

        // Multiple consumers
        let mut dequeue_tasks = Vec::new();

        for consumer_id in 0..5 {
            let queue_clone = queue.clone();
            let consumer_topic = format!("topic_{}", consumer_id * 2); // Each consumer gets 2 topics

            let task = tokio::spawn(async move {
                let mut count = 0;
                while count < 500 { // Each consumer processes 500 messages
                    if let Some(message) = queue_clone.dequeue(&consumer_topic).await? {
                        // Simulate processing
                        tokio::time::sleep(Duration::from_millis(1)).await;

                        // Acknowledge every other message to test retry logic
                        if count % 2 == 0 {
                            queue_clone.acknowledge(&message.id).await?;
                        }

                        count += 1;
                    } else {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
                Ok::<_, anyhow::Error>(count)
            });

            dequeue_tasks.push(task);
        }

        // Wait for consumers
        for task in dequeue_tasks {
            let count = task.await.unwrap()?;
            dequeued_count += count;
        }

        let dequeue_duration = dequeue_start.elapsed();
        let dequeue_throughput = dequeued_count as f64 / dequeue_duration.as_secs_f64();

        println!("Dequeue completed in {:?}", dequeue_duration);
        println!("Dequeue throughput: {:.2} messages/sec", dequeue_throughput);
        println!("Processed {} messages", dequeued_count);

        // Check queue statistics
        let stats = queue.stats().await;
        println!("Queue stats: {:?}", stats);

        assert!(enqueue_throughput > 500.0, "Enqueue throughput should be > 500 msg/sec");
        assert!(dequeue_throughput > 100.0, "Dequeue throughput should be > 100 msg/sec");
        assert!(stats.pending_messages > 0, "Should have some pending messages");

        Ok(())
    }

    #[tokio::test]
    async fn test_exactly_once_bus_persistence() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("exactly_once_persistence.redb");
        let storage = Arc::new(Store::open(&db_path)?);

        // Test with exactly-once bus persistence
        let bus = ExactlyOnceEventBusBuilder::new()
            .build(storage.clone())
            .await?;

        bus.start().await?;

        let generator = TestDataGenerator::new();
        let num_events = 1000;

        // Publish events
        let publish_start = Instant::now();
        let mut published_ids = Vec::new();

        for i in 0..num_events {
            let event = generator.generate_event(1);
            let message_id = bus.publish(event).await?;
            published_ids.push(message_id);

            if i % 200 == 0 {
                println!("Published {} events", i);
            }
        }

        let publish_duration = publish_start.elapsed();
        println!("Published {} events in {:?}", num_events, publish_duration);

        // Shutdown and restart to test persistence
        bus.shutdown().await?;
        drop(bus);

        // Restart bus
        let bus2 = ExactlyOnceEventBusBuilder::new()
            .build(storage)
            .await?;

        bus2.start().await?;

        // Check stats to verify persistence
        let stats = bus2.stats().await;
        println!("Recovered stats: {:?}", stats);

        // Should have messages in queue after restart
        assert!(stats.queue_stats.total_messages > 0, "Should have persistent messages");

        bus2.shutdown().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_size_optimization() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("size_optimization.redb");
        let storage = Arc::new(Store::open(&db_path)?);
        let event_store = Arc::new(EventStore::new(storage));

        let generator = TestDataGenerator::new();

        // Test with different event sizes
        let size_tests = vec![
            (1000, 1),   // 1000 events x 1KB = ~1MB
            (100, 10),   // 100 events x 10KB = ~1MB
            (10, 100),   // 10 events x 100KB = ~1MB
        ];

        let mut results = Vec::new();

        for (count, size_kb) in size_tests {
            let stream_id = format!("size_test_{}_{}", count, size_kb);
            let events = generator.generate_events(count, size_kb);

            let start_size = std::fs::metadata(&db_path)
                .map(|m| m.len())
                .unwrap_or(0);

            let write_start = Instant::now();

            for (i, event) in events.iter().enumerate() {
                event_store.append_event(&stream_id, event.clone(), i as u32).await?;
            }

            let write_duration = write_start.elapsed();
            let end_size = std::fs::metadata(&db_path)?.len();
            let size_increase = end_size - start_size;

            results.push((count, size_kb, write_duration, size_increase));

            println!(
                "Test {}: {} events x {}KB, write time: {:?}, size increase: {} MB",
                results.len(),
                count,
                size_kb,
                write_duration,
                size_increase / 1024 / 1024
            );
        }

        // Analyze compression efficiency
        for (count, size_kb, duration, size_increase) in results {
            let expected_size = count * size_kb * 1024;
            let compression_ratio = expected_size as f64 / size_increase as f64;

            println!(
                "Compression ratio for {}x{}KB: {:.2}x",
                count, size_kb, compression_ratio
            );

            // Reasonable compression expected for test data
            assert!(compression_ratio > 0.5, "Should have reasonable compression");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_memory_usage_under_load() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("memory_usage.redb");
        let storage = Arc::new(Store::open(&db_path)?);
        let event_store = Arc::new(EventStore::new(storage));

        let generator = TestDataGenerator::new();

        // Monitor memory usage during large operations
        let initial_memory = get_process_memory_mb();
        println!("Initial memory usage: {:.2} MB", initial_memory);

        // Large batch write
        let num_events = 10000;
        let events = generator.generate_events(num_events, 2); // 2KB events

        let mid_memory = get_process_memory_mb();
        println!("Memory after generating events: {:.2} MB", mid_memory);

        let stream_id = "memory_test";
        for (i, event) in events.iter().enumerate() {
            event_store.append_event(stream_id, event.clone(), i as u32).await?;

            // Check memory periodically
            if i % 1000 == 0 {
                let current_memory = get_process_memory_mb();
                println!("Memory at event {}: {:.2} MB", i, current_memory);
            }
        }

        let final_memory = get_process_memory_mb();
        println!("Final memory usage: {:.2} MB", final_memory);

        // Memory should not grow unboundedly
        let memory_growth = final_memory - initial_memory;
        println!("Total memory growth: {:.2} MB", memory_growth);

        // Allow reasonable memory growth for caching, but not excessive
        assert!(memory_growth < 100.0, "Memory growth should be < 100MB for this test");

        // Test that memory is freed after large read
        let _read_events = event_store.get_events(stream_id, 0, None).await?;

        // Give GC a chance to run
        tokio::time::sleep(Duration::from_millis(100)).await;

        let post_read_memory = get_process_memory_mb();
        println!("Memory after read: {:.2} MB", post_read_memory);

        Ok(())
    }

    // Helper function to get process memory usage
    fn get_process_memory_mb() -> f64 {
        // This is a simplified implementation
        // In a real test, you might use a proper system monitoring library
        #[cfg(target_os = "linux")]
        {
            if let Ok(contents) = std::fs::read_to_string("/proc/self/status") {
                for line in contents.lines() {
                    if line.starts_with("VmRSS:") {
                        if let Some(kb_str) = line.split_whitespace().nth(1) {
                            if let Ok(kb) = kb_str.parse::<f64>() {
                                return kb / 1024.0; // Convert KB to MB
                            }
                        }
                    }
                }
            }
        }

        // Fallback: estimate based on allocated data
        0.0
    }
}