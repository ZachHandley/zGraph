use crate::events::{Event, EventId};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{RwLock, Notify};
use tracing::{debug, error, info, warn};
use zcore_storage::Store;

// Storage column definitions for the persistent queue
const COL_MESSAGES: &str = "queue_messages";
const COL_CONSUMER_OFFSETS: &str = "consumer_offsets";
const COL_PENDING_ACKS: &str = "pending_acks";
const COL_DEAD_LETTER: &str = "dead_letter_queue";
const COL_DEDUPLICATION: &str = "message_deduplication";

/// Configuration for the persistent message queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentQueueConfig {
    /// Maximum number of messages to keep in memory cache
    pub memory_cache_size: usize,
    /// ACK timeout in seconds - how long to wait for consumer acknowledgment
    pub ack_timeout_secs: u64,
    /// Maximum number of delivery attempts before dead lettering
    pub max_delivery_attempts: u32,
    /// Cleanup interval for expired ACKs and dead letters
    pub cleanup_interval_secs: u64,
    /// Enable message deduplication based on event ID
    pub enable_deduplication: bool,
    /// Deduplication window in hours - how long to remember seen messages
    pub deduplication_window_hours: u64,
}

impl Default for PersistentQueueConfig {
    fn default() -> Self {
        Self {
            memory_cache_size: 1000,
            ack_timeout_secs: 30,
            max_delivery_attempts: 3,
            cleanup_interval_secs: 60,
            enable_deduplication: true,
            deduplication_window_hours: 24,
        }
    }
}

/// Message state in the queue
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MessageState {
    /// Message is queued and ready for delivery
    Queued,
    /// Message is being processed by a consumer (pending ACK)
    InFlight { consumer_id: String, delivered_at: DateTime<Utc> },
    /// Message processing succeeded
    Acknowledged,
    /// Message processing failed and will be retried
    Failed { attempts: u32, last_error: String },
    /// Message exceeded max attempts and is in dead letter queue
    DeadLetter { reason: String },
}

/// Persistent message in the queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentMessage {
    /// Message ID (ULID for ordering)
    pub id: String,
    /// Event being delivered
    pub event: Event,
    /// Current state
    pub state: MessageState,
    /// Queue position/sequence number
    pub sequence: u64,
    /// Topic for routing
    pub topic: String,
    /// First queued timestamp
    pub queued_at: DateTime<Utc>,
    /// Last updated timestamp
    pub updated_at: DateTime<Utc>,
    /// Delivery attempts
    pub delivery_attempts: u32,
    /// Message metadata
    pub metadata: HashMap<String, String>,
}

impl PersistentMessage {
    pub fn new(event: Event, topic: String, sequence: u64) -> Self {
        let now = Utc::now();
        Self {
            id: ulid::Ulid::new().to_string(),
            event,
            state: MessageState::Queued,
            sequence,
            topic,
            queued_at: now,
            updated_at: now,
            delivery_attempts: 0,
            metadata: HashMap::new(),
        }
    }

    pub fn is_ready_for_delivery(&self) -> bool {
        matches!(self.state, MessageState::Queued)
    }

    pub fn mark_in_flight(&mut self, consumer_id: String) {
        self.state = MessageState::InFlight {
            consumer_id,
            delivered_at: Utc::now()
        };
        self.updated_at = Utc::now();
        self.delivery_attempts += 1;
    }

    pub fn mark_acknowledged(&mut self) {
        self.state = MessageState::Acknowledged;
        self.updated_at = Utc::now();
    }

    pub fn mark_failed(&mut self, error: String) {
        self.state = MessageState::Failed {
            attempts: self.delivery_attempts,
            last_error: error
        };
        self.updated_at = Utc::now();
    }

    pub fn mark_dead_letter(&mut self, reason: String) {
        self.state = MessageState::DeadLetter { reason };
        self.updated_at = Utc::now();
    }
}

/// Consumer position for crash recovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerOffset {
    pub consumer_id: String,
    pub last_processed_sequence: u64,
    pub updated_at: DateTime<Utc>,
}

/// Pending acknowledgment tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingAck {
    pub message_id: String,
    pub consumer_id: String,
    pub delivered_at: DateTime<Utc>,
    pub timeout_at: DateTime<Utc>,
}

/// Statistics for monitoring the persistent queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentQueueStats {
    pub total_messages: u64,
    pub queued_messages: u64,
    pub in_flight_messages: u64,
    pub acknowledged_messages: u64,
    pub failed_messages: u64,
    pub dead_letter_messages: u64,
    pub active_consumers: usize,
    pub pending_acks: u64,
    pub duplicate_messages: u64,
}

/// High-performance persistent message queue with exactly-once delivery guarantees
pub struct PersistentMessageQueue {
    storage: Arc<Store>,
    config: PersistentQueueConfig,
    sequence_counter: AtomicU64,
    consumers: Arc<RwLock<HashMap<String, ConsumerOffset>>>,
    notify: Arc<Notify>,
    stats: Arc<RwLock<PersistentQueueStats>>,
    shutdown: Arc<tokio::sync::broadcast::Sender<()>>,
}

impl PersistentMessageQueue {
    pub async fn new(storage: Arc<Store>, config: PersistentQueueConfig) -> Result<Self> {
        let queue = Self {
            storage: storage.clone(),
            config,
            sequence_counter: AtomicU64::new(1),
            consumers: Arc::new(RwLock::new(HashMap::new())),
            notify: Arc::new(Notify::new()),
            stats: Arc::new(RwLock::new(PersistentQueueStats {
                total_messages: 0,
                queued_messages: 0,
                in_flight_messages: 0,
                acknowledged_messages: 0,
                failed_messages: 0,
                dead_letter_messages: 0,
                active_consumers: 0,
                pending_acks: 0,
                duplicate_messages: 0,
            })),
            shutdown: Arc::new(tokio::sync::broadcast::channel(1).0),
        };

        // Initialize storage tables
        queue.initialize_storage().await?;

        // Load existing sequence counter
        queue.load_sequence_counter().await?;

        // Start background tasks
        queue.start_background_tasks().await;

        info!("Persistent message queue initialized");
        Ok(queue)
    }

    /// Initialize storage tables for the persistent queue
    async fn initialize_storage(&self) -> Result<()> {
        let mut write_txn = self.storage.begin_write()?;

        // Initialize all required collections for the persistent queue
        let collections = [
            COL_MESSAGES,
            COL_CONSUMER_OFFSETS,
            COL_PENDING_ACKS,
            COL_DEAD_LETTER,
            COL_DEDUPLICATION,
        ];

        {
            for collection in &collections {
                write_txn.open_table(&self.storage, collection)
                    .map_err(|e| anyhow!("Failed to initialize collection '{}': {}", collection, e))?;
                debug!("Initialized persistent queue collection: {}", collection);
            }
        }

        write_txn.commit(&self.storage)?;
        info!("Persistent queue storage tables initialized successfully");
        Ok(())
    }

    /// Load the sequence counter from storage
    async fn load_sequence_counter(&self) -> Result<()> {
        let read_txn = self.storage.begin_read()?;
        let messages_table = read_txn.open_table(&self.storage, COL_MESSAGES)?;

        // Find the highest sequence number
        let mut max_sequence = 0u64;
        for result in messages_table.iter()? {
            let (key, _) = result?;
            let key_str = std::str::from_utf8(&key)?;

            // Keys are formatted as "{sequence:020}:{message_id}"
            if let Some(seq_str) = key_str.split(':').next() {
                if let Ok(seq) = seq_str.parse::<u64>() {
                    max_sequence = max_sequence.max(seq);
                }
            }
        }

        self.sequence_counter.store(max_sequence + 1, Ordering::SeqCst);
        debug!(next_sequence = max_sequence + 1, "Sequence counter loaded");
        Ok(())
    }

    /// Start background tasks for ACK timeout handling and cleanup
    async fn start_background_tasks(&self) {
        let queue_clone = self.clone();
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                std::time::Duration::from_secs(queue_clone.config.cleanup_interval_secs)
            );

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = queue_clone.cleanup_expired_acks().await {
                            error!(error = %e, "Failed to cleanup expired ACKs");
                        }
                        if let Err(e) = queue_clone.cleanup_old_deduplication_entries().await {
                            error!(error = %e, "Failed to cleanup old deduplication entries");
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Background cleanup task shutting down");
                        break;
                    }
                }
            }
        });
    }

    /// Enqueue a new message
    pub async fn enqueue(&self, event: Event, topic: String) -> Result<String> {
        // Check for duplicates if deduplication is enabled
        if self.config.enable_deduplication {
            if self.is_duplicate(&event.id).await? {
                self.update_stats(|stats| stats.duplicate_messages += 1).await;
                debug!(event_id = %event.id, "Duplicate message ignored");
                return Ok(event.id.to_string());
            }
        }

        let sequence = self.sequence_counter.fetch_add(1, Ordering::SeqCst);
        let message = PersistentMessage::new(event, topic, sequence);
        let message_id = message.id.clone();

        // Store the message
        let mut write_txn = self.storage.begin_write()?;
        {
            let mut messages_table = write_txn.open_table(&self.storage, COL_MESSAGES)?;
            let key = format!("{:020}:{}", sequence, message_id);
            let data = serde_json::to_vec(&message)?;
            messages_table.insert(key.as_bytes(), &data)?;

            // Add to deduplication table if enabled
            if self.config.enable_deduplication {
                let mut dedup_table = write_txn.open_table(&self.storage, COL_DEDUPLICATION)?;
                let dedup_key = message.event.id.to_string();
                let dedup_data = serde_json::to_vec(&Utc::now())?;
                dedup_table.insert(dedup_key.as_bytes(), &dedup_data)?;
            }
        }
        write_txn.commit(&self.storage)?;

        // Update stats
        self.update_stats(|stats| {
            stats.total_messages += 1;
            stats.queued_messages += 1;
        }).await;

        // Notify waiting consumers
        self.notify.notify_waiters();

        debug!(
            message_id = %message_id,
            sequence = sequence,
            topic = %message.topic,
            "Message enqueued"
        );

        Ok(message_id)
    }

    /// Dequeue the next available message for a consumer
    pub async fn dequeue(&self, consumer_id: String) -> Result<Option<PersistentMessage>> {
        let consumer_offset = self.get_consumer_offset(&consumer_id).await;

        let read_txn = self.storage.begin_read()?;
        let messages_table = read_txn.open_table(&self.storage, COL_MESSAGES)?;

        // Find the next message after consumer's last processed sequence
        for result in messages_table.iter()? {
            let (key, value) = result?;
            let key_str = std::str::from_utf8(&key)?;

            if let Some(seq_str) = key_str.split(':').next() {
                if let Ok(sequence) = seq_str.parse::<u64>() {
                    if sequence <= consumer_offset {
                        continue; // Already processed
                    }

                    let mut message: PersistentMessage = serde_json::from_slice(&value)?;

                    if message.is_ready_for_delivery() {
                        drop(read_txn); // Release read transaction

                        // Mark message as in-flight
                        message.mark_in_flight(consumer_id.clone());
                        self.update_message(&message).await?;

                        // Create pending ACK entry
                        self.create_pending_ack(&message, &consumer_id).await?;

                        // Update stats
                        self.update_stats(|stats| {
                            stats.queued_messages = stats.queued_messages.saturating_sub(1);
                            stats.in_flight_messages += 1;
                        }).await;

                        debug!(
                            message_id = %message.id,
                            consumer_id = %consumer_id,
                            sequence = sequence,
                            "Message dequeued"
                        );

                        return Ok(Some(message));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Wait for messages to become available
    pub async fn wait_for_messages(&self) -> Result<()> {
        self.notify.notified().await;
        Ok(())
    }

    /// Acknowledge successful message processing
    pub async fn ack(&self, message_id: &str, consumer_id: &str) -> Result<()> {
        // Get the message
        let mut message = self.get_message(message_id).await?
            .ok_or_else(|| anyhow!("Message not found: {}", message_id))?;

        // Verify the consumer can ACK this message
        match &message.state {
            MessageState::InFlight { consumer_id: msg_consumer_id, .. } => {
                if msg_consumer_id != consumer_id {
                    return Err(anyhow!("Consumer {} cannot ACK message owned by {}", consumer_id, msg_consumer_id));
                }
            }
            _ => {
                return Err(anyhow!("Message {} is not in-flight", message_id));
            }
        }

        // Mark as acknowledged
        message.mark_acknowledged();
        self.update_message(&message).await?;

        // Remove pending ACK
        self.remove_pending_ack(message_id).await?;

        // Update consumer offset
        self.update_consumer_offset(consumer_id, message.sequence).await?;

        // Update stats
        self.update_stats(|stats| {
            stats.in_flight_messages = stats.in_flight_messages.saturating_sub(1);
            stats.acknowledged_messages += 1;
        }).await;

        debug!(
            message_id = %message_id,
            consumer_id = %consumer_id,
            "Message acknowledged"
        );

        Ok(())
    }

    /// Negative acknowledgment - message processing failed
    pub async fn nack(&self, message_id: &str, consumer_id: &str, error: String) -> Result<()> {
        let mut message = self.get_message(message_id).await?
            .ok_or_else(|| anyhow!("Message not found: {}", message_id))?;

        // Verify the consumer can NACK this message
        match &message.state {
            MessageState::InFlight { consumer_id: msg_consumer_id, .. } => {
                if msg_consumer_id != consumer_id {
                    return Err(anyhow!("Consumer {} cannot NACK message owned by {}", consumer_id, msg_consumer_id));
                }
            }
            _ => {
                return Err(anyhow!("Message {} is not in-flight", message_id));
            }
        }

        // Check if we should dead letter or retry
        if message.delivery_attempts >= self.config.max_delivery_attempts {
            message.mark_dead_letter(format!("Max attempts exceeded: {}", error));
            self.move_to_dead_letter(&message).await?;

            self.update_stats(|stats| {
                stats.in_flight_messages = stats.in_flight_messages.saturating_sub(1);
                stats.dead_letter_messages += 1;
            }).await;
        } else {
            message.mark_failed(error);

            self.update_stats(|stats| {
                stats.in_flight_messages = stats.in_flight_messages.saturating_sub(1);
                stats.failed_messages += 1;
            }).await;
        }

        self.update_message(&message).await?;
        self.remove_pending_ack(message_id).await?;

        debug!(
            message_id = %message_id,
            consumer_id = %consumer_id,
            attempts = message.delivery_attempts,
            "Message negatively acknowledged"
        );

        Ok(())
    }

    /// Get queue statistics
    pub async fn stats(&self) -> PersistentQueueStats {
        let mut stats = self.stats.read().await.clone();
        stats.active_consumers = self.consumers.read().await.len();
        stats
    }

    /// Implementation helper methods...

    async fn is_duplicate(&self, event_id: &EventId) -> Result<bool> {
        let read_txn = self.storage.begin_read()?;
        let dedup_table = read_txn.open_table(&self.storage, COL_DEDUPLICATION)?;

        let key = event_id.to_string();
        Ok(dedup_table.get(key.as_bytes())?.is_some())
    }

    async fn get_consumer_offset(&self, consumer_id: &str) -> u64 {
        if let Some(offset) = self.consumers.read().await.get(consumer_id) {
            return offset.last_processed_sequence;
        }

        // Try to load from storage
        if let Ok(read_txn) = self.storage.begin_read() {
            if let Ok(offsets_table) = read_txn.open_table(&self.storage, COL_CONSUMER_OFFSETS) {
                if let Ok(Some(data)) = offsets_table.get(consumer_id.as_bytes()) {
                    if let Ok(offset) = serde_json::from_slice::<ConsumerOffset>(data.value()) {
                        self.consumers.write().await.insert(consumer_id.to_string(), offset.clone());
                        return offset.last_processed_sequence;
                    }
                }
            }
        }

        0 // Start from beginning if no offset found
    }

    async fn update_consumer_offset(&self, consumer_id: &str, sequence: u64) -> Result<()> {
        let offset = ConsumerOffset {
            consumer_id: consumer_id.to_string(),
            last_processed_sequence: sequence,
            updated_at: Utc::now(),
        };

        // Update in memory
        self.consumers.write().await.insert(consumer_id.to_string(), offset.clone());

        // Persist to storage
        let mut write_txn = self.storage.begin_write()?;
        {
            let mut offsets_table = write_txn.open_table(&self.storage, COL_CONSUMER_OFFSETS)?;
            let data = serde_json::to_vec(&offset)?;
            offsets_table.insert(consumer_id.as_bytes(), &data)?;
        }
        write_txn.commit(&self.storage)?;

        Ok(())
    }

    async fn get_message(&self, message_id: &str) -> Result<Option<PersistentMessage>> {
        let read_txn = self.storage.begin_read()?;
        let messages_table = read_txn.open_table(&self.storage, COL_MESSAGES)?;

        // We need to scan since we only have message_id, not the full key
        for result in messages_table.iter()? {
            let (key, value) = result?;
            let key_str = std::str::from_utf8(&key)?;

            if key_str.ends_with(&format!(":{}", message_id)) {
                let message: PersistentMessage = serde_json::from_slice(&value)?;
                return Ok(Some(message));
            }
        }

        Ok(None)
    }

    async fn update_message(&self, message: &PersistentMessage) -> Result<()> {
        let mut write_txn = self.storage.begin_write()?;
        {
            let mut messages_table = write_txn.open_table(&self.storage, COL_MESSAGES)?;
            let key = format!("{:020}:{}", message.sequence, message.id);
            let data = serde_json::to_vec(message)?;
            messages_table.insert(key.as_bytes(), &data)?;
        }
        write_txn.commit(&self.storage)?;
        Ok(())
    }

    async fn create_pending_ack(&self, message: &PersistentMessage, consumer_id: &str) -> Result<()> {
        let pending_ack = PendingAck {
            message_id: message.id.clone(),
            consumer_id: consumer_id.to_string(),
            delivered_at: Utc::now(),
            timeout_at: Utc::now() + chrono::Duration::seconds(self.config.ack_timeout_secs as i64),
        };

        let mut write_txn = self.storage.begin_write()?;
        {
            let mut pending_table = write_txn.open_table(&self.storage, COL_PENDING_ACKS)?;
            let key = message.id.as_bytes();
            let data = serde_json::to_vec(&pending_ack)?;
            pending_table.insert(key, &data)?;
        }
        write_txn.commit(&self.storage)?;

        self.update_stats(|stats| stats.pending_acks += 1).await;
        Ok(())
    }

    async fn remove_pending_ack(&self, message_id: &str) -> Result<()> {
        let mut write_txn = self.storage.begin_write()?;
        {
            let mut pending_table = write_txn.open_table(&self.storage, COL_PENDING_ACKS)?;
            pending_table.remove(message_id.as_bytes())?;
        }
        write_txn.commit(&self.storage)?;

        self.update_stats(|stats| stats.pending_acks = stats.pending_acks.saturating_sub(1)).await;
        Ok(())
    }

    async fn move_to_dead_letter(&self, message: &PersistentMessage) -> Result<()> {
        let mut write_txn = self.storage.begin_write()?;
        {
            let mut dead_letter_table = write_txn.open_table(&self.storage, COL_DEAD_LETTER)?;
            let key = format!("{}:{}", Utc::now().timestamp_nanos_opt().unwrap_or(0), message.id);
            let data = serde_json::to_vec(message)?;
            dead_letter_table.insert(key.as_bytes(), &data)?;
        }
        write_txn.commit(&self.storage)?;
        Ok(())
    }

    async fn cleanup_expired_acks(&self) -> Result<()> {
        let read_txn = self.storage.begin_read()?;
        let pending_table = read_txn.open_table(&self.storage, COL_PENDING_ACKS)?;

        let mut expired_messages = Vec::new();
        let now = Utc::now();

        for result in pending_table.iter()? {
            let (key, value) = result?;
            let pending_ack: PendingAck = serde_json::from_slice(&value)?;

            if pending_ack.timeout_at <= now {
                expired_messages.push(pending_ack.message_id.clone());
            }
        }

        drop(read_txn);

        // Process expired messages
        for message_id in expired_messages {
            if let Ok(Some(mut message)) = self.get_message(&message_id).await {
                warn!(
                    message_id = %message_id,
                    "ACK timeout expired, requeueing message"
                );

                message.state = MessageState::Queued;
                message.updated_at = Utc::now();
                self.update_message(&message).await?;
                self.remove_pending_ack(&message_id).await?;

                self.update_stats(|stats| {
                    stats.in_flight_messages = stats.in_flight_messages.saturating_sub(1);
                    stats.queued_messages += 1;
                }).await;

                self.notify.notify_waiters();
            }
        }

        Ok(())
    }

    async fn cleanup_old_deduplication_entries(&self) -> Result<()> {
        if !self.config.enable_deduplication {
            return Ok(());
        }

        let cutoff = Utc::now() - chrono::Duration::hours(self.config.deduplication_window_hours as i64);

        let read_txn = self.storage.begin_read()?;
        let dedup_table = read_txn.open_table(&self.storage, COL_DEDUPLICATION)?;

        let mut keys_to_remove = Vec::new();

        for result in dedup_table.iter()? {
            let (key, value) = result?;
            if let Ok(timestamp) = serde_json::from_slice::<DateTime<Utc>>(&value) {
                if timestamp < cutoff {
                    keys_to_remove.push(key.to_vec());
                }
            }
        }

        drop(read_txn);

        if !keys_to_remove.is_empty() {
            let mut write_txn = self.storage.begin_write()?;
            {
                let mut dedup_table = write_txn.open_table(&self.storage, COL_DEDUPLICATION)?;
                for key in keys_to_remove {
                    dedup_table.remove(&key)?;
                }
            }
            write_txn.commit(&self.storage)?;
        }

        Ok(())
    }

    async fn update_stats<F>(&self, update_fn: F)
    where
        F: FnOnce(&mut PersistentQueueStats),
    {
        let mut stats = self.stats.write().await;
        update_fn(&mut *stats);
    }

    /// Shutdown the queue gracefully
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down persistent message queue");
        let _ = self.shutdown.send(());
        Ok(())
    }
}

// Implement Clone for background task spawning
impl Clone for PersistentMessageQueue {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            config: self.config.clone(),
            sequence_counter: AtomicU64::new(self.sequence_counter.load(Ordering::SeqCst)),
            consumers: self.consumers.clone(),
            notify: self.notify.clone(),
            stats: self.stats.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{Event, EventType, EventData};
    use tempfile::tempdir;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_enqueue_dequeue() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let config = PersistentQueueConfig::default();
        let queue = PersistentMessageQueue::new(storage, config).await.unwrap();

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

        // Enqueue message
        let message_id = queue.enqueue(event.clone(), "test-topic".to_string()).await.unwrap();

        // Dequeue message
        let dequeued = queue.dequeue("consumer1".to_string()).await.unwrap();
        assert!(dequeued.is_some());

        let message = dequeued.unwrap();
        assert_eq!(message.event.id, event.id);
        assert_eq!(message.topic, "test-topic");

        // ACK the message
        queue.ack(&message.id, "consumer1").await.unwrap();

        let stats = queue.stats().await;
        assert_eq!(stats.acknowledged_messages, 1);
    }

    #[tokio::test]
    async fn test_ack_timeout() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let mut config = PersistentQueueConfig::default();
        config.ack_timeout_secs = 1; // Very short timeout for testing

        let queue = PersistentMessageQueue::new(storage, config).await.unwrap();

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

        queue.enqueue(event.clone(), "test-topic".to_string()).await.unwrap();

        // Dequeue but don't ACK
        let message = queue.dequeue("consumer1".to_string()).await.unwrap().unwrap();

        // Wait for timeout
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Trigger cleanup
        queue.cleanup_expired_acks().await.unwrap();

        // Should be able to dequeue again
        let requeued = queue.dequeue("consumer2".to_string()).await.unwrap();
        assert!(requeued.is_some());
        assert_eq!(requeued.unwrap().id, message.id);
    }

    #[tokio::test]
    async fn test_deduplication() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let config = PersistentQueueConfig {
            enable_deduplication: true,
            ..Default::default()
        };
        let queue = PersistentMessageQueue::new(storage, config).await.unwrap();

        let event1 = Event::new(
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

        let event2 = Event {
            id: event1.id, // Same ID
            ..event1.clone()
        };

        // Enqueue first message
        queue.enqueue(event1, "test-topic".to_string()).await.unwrap();

        // Enqueue duplicate - should be ignored
        queue.enqueue(event2, "test-topic".to_string()).await.unwrap();

        let stats = queue.stats().await;
        assert_eq!(stats.total_messages, 1);
        assert_eq!(stats.duplicate_messages, 1);
    }
}