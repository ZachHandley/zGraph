use crate::events::{Event, EventId, EventType};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info};
use zcore_storage::{Store, COL_EVENTS, COL_SNAPSHOTS, COL_EVENT_INDEX};

/// Event stream configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventStreamConfig {
    /// Maximum number of events to return in a single query
    pub max_events_per_query: usize,
    /// Enable compression for stored events
    pub compression_enabled: bool,
    /// Snapshot interval (create snapshot every N events)
    pub snapshot_interval: Option<u64>,
}

impl Default for EventStreamConfig {
    fn default() -> Self {
        Self {
            max_events_per_query: 1000,
            compression_enabled: false,
            snapshot_interval: Some(1000), // Snapshot every 1000 events
        }
    }
}

/// Event stream for querying and filtering events
#[derive(Debug, Clone)]
pub struct EventStream {
    /// Stream identifier
    pub stream_id: String,
    /// Organization ID for isolation
    pub org_id: u64,
    /// Event type filters
    pub event_types: Option<Vec<EventType>>,
    /// Topic filters (supports wildcards)
    pub topics: Option<Vec<String>>,
    /// Start time filter
    pub start_time: Option<DateTime<Utc>>,
    /// End time filter
    pub end_time: Option<DateTime<Utc>>,
    /// Maximum events to return
    pub limit: Option<usize>,
}

impl EventStream {
    pub fn new(stream_id: String, org_id: u64) -> Self {
        Self {
            stream_id,
            org_id,
            event_types: None,
            topics: None,
            start_time: None,
            end_time: None,
            limit: None,
        }
    }

    pub fn with_event_types(mut self, event_types: Vec<EventType>) -> Self {
        self.event_types = Some(event_types);
        self
    }

    pub fn with_topics(mut self, topics: Vec<String>) -> Self {
        self.topics = Some(topics);
        self
    }

    pub fn with_time_range(mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        self.start_time = Some(start);
        self.end_time = Some(end);
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Check if an event matches this stream's filters
    pub fn matches(&self, event: &Event) -> bool {
        // Check organization
        if event.org_id != self.org_id {
            return false;
        }

        // Check event types
        if let Some(ref types) = self.event_types {
            if !types.contains(&event.event_type) {
                return false;
            }
        }

        // Check topics
        if let Some(ref topics) = self.topics {
            let event_topic = event.to_topic();
            let matches_any = topics.iter().any(|pattern| event.matches_topic(pattern));
            if !matches_any {
                return false;
            }
        }

        // Check time range
        if let Some(start) = self.start_time {
            if event.timestamp < start {
                return false;
            }
        }

        if let Some(end) = self.end_time {
            if event.timestamp > end {
                return false;
            }
        }

        true
    }
}

/// Snapshot of aggregate state at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Unique snapshot ID
    pub id: String,
    /// Aggregate ID this snapshot belongs to
    pub aggregate_id: String,
    /// Organization ID
    pub org_id: u64,
    /// Snapshot data
    pub data: serde_json::Value,
    /// Event sequence number at snapshot time
    pub sequence_number: u64,
    /// Snapshot timestamp
    pub timestamp: DateTime<Utc>,
    /// Snapshot version for schema evolution
    pub version: String,
}

impl Snapshot {
    pub fn new(
        aggregate_id: String,
        org_id: u64,
        data: serde_json::Value,
        sequence_number: u64,
    ) -> Self {
        Self {
            id: ulid::Ulid::new().to_string(),
            aggregate_id,
            org_id,
            data,
            sequence_number,
            timestamp: Utc::now(),
            version: "1.0".to_string(),
        }
    }
}

/// Event index entry for efficient querying
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EventIndexEntry {
    /// Event ID
    event_id: String,
    /// Event type
    event_type: String,
    /// Organization ID
    org_id: u64,
    /// Event timestamp
    timestamp: DateTime<Utc>,
    /// Event topic
    topic: String,
    /// Sequence number
    sequence: u64,
}

/// Event sourcing statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventStoreStats {
    /// Total events stored
    pub total_events: u64,
    /// Total snapshots stored
    pub total_snapshots: u64,
    /// Events by type count
    pub events_by_type: std::collections::HashMap<String, u64>,
    /// Events by organization count
    pub events_by_org: std::collections::HashMap<u64, u64>,
    /// Storage size in bytes
    pub storage_size_bytes: u64,
    /// Oldest event timestamp
    pub oldest_event: Option<DateTime<Utc>>,
    /// Newest event timestamp
    pub newest_event: Option<DateTime<Utc>>,
}

/// Event store for persistence and querying of events
pub struct EventStore {
    storage: Arc<Store>,
    config: EventStreamConfig,
    sequence_counter: std::sync::atomic::AtomicU64,
}

impl EventStore {
    pub fn new(storage: Arc<Store>) -> Result<Self> {
        let store = Self {
            storage,
            config: EventStreamConfig::default(),
            sequence_counter: std::sync::atomic::AtomicU64::new(1),
        };

        // Initialize tables
        store.initialize_tables()?;

        Ok(store)
    }

    pub fn with_config(mut self, config: EventStreamConfig) -> Self {
        self.config = config;
        self
    }

    /// Initialize database collections
    fn initialize_tables(&self) -> Result<()> {
        let mut write_txn = self.storage.begin_write()?;

        {
            let _ = write_txn.open_table(&self.storage, COL_EVENTS)?;
            let _ = write_txn.open_table(&self.storage, COL_SNAPSHOTS)?;
            let _ = write_txn.open_table(&self.storage, COL_EVENT_INDEX)?;
        }

        write_txn.commit(&self.storage)?;
        info!("Event store tables initialized");
        Ok(())
    }

    /// Append an event to the store
    pub async fn append_event(&self, event: &Event) -> Result<u64> {
        let sequence = self
            .sequence_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let mut write_txn = self.storage.begin_write()?;

        {
            // Store the event
            let mut events_table = write_txn.open_table(&self.storage, COL_EVENTS)?;
            let event_key = format!("{}:{:020}", event.org_id, sequence);

            let event_data = if self.config.compression_enabled {
                self.compress_event_data(event)?
            } else {
                serde_json::to_vec(event)?
            };

            events_table.insert(event_key.as_bytes(), event_data.as_slice())?;

            // Create index entry
            let index_entry = EventIndexEntry {
                event_id: event.id.to_string(),
                event_type: event.event_type.to_string(),
                org_id: event.org_id,
                timestamp: event.timestamp,
                topic: event.to_topic(),
                sequence,
            };

            let mut index_table = write_txn.open_table(&self.storage, COL_EVENT_INDEX)?;
            let index_key = format!("{}:{}:{:020}", event.org_id, event.event_type, sequence);
            let index_data = serde_json::to_vec(&index_entry)?;
            index_table.insert(index_key.as_bytes(), index_data.as_slice())?;
        }

        write_txn.commit(&self.storage)?;

        debug!(
            event_id = %event.id,
            sequence = sequence,
            "Event appended to store"
        );

        // Create snapshot if configured and interval reached
        if let Some(interval) = self.config.snapshot_interval {
            if sequence % interval == 0 {
                self.create_automatic_snapshot(event.org_id, sequence).await?;
            }
        }

        Ok(sequence)
    }

    /// Get events by ID range
    pub async fn get_events(
        &self,
        from: Option<EventId>,
        to: Option<EventId>,
    ) -> Result<Vec<Event>> {
        let read_txn = self.storage.begin_read()?;
        let events_table = read_txn.open_table(&self.storage, COL_EVENTS)?;

        let mut events = Vec::new();
        let limit = self.config.max_events_per_query;

        // Iterate through events (this is a simplified implementation)
        // In practice, you'd want more efficient range queries
        for result in events_table.iter()? {
            if events.len() >= limit {
                break;
            }

            let (key, value) = result?;
            let event_data = &value;

            let event: Event = if self.config.compression_enabled {
                self.decompress_event_data(event_data)?
            } else {
                serde_json::from_slice(event_data)?
            };

            // Apply ID filters
            if let Some(ref from_id) = from {
                if event.id.as_ulid() < from_id.as_ulid() {
                    continue;
                }
            }

            if let Some(ref to_id) = to {
                if event.id.as_ulid() > to_id.as_ulid() {
                    continue;
                }
            }

            events.push(event);
        }

        debug!(count = events.len(), "Retrieved events from store");
        Ok(events)
    }

    /// Get events matching a stream definition
    pub async fn get_events_for_stream(&self, stream: &EventStream) -> Result<Vec<Event>> {
        let read_txn = self.storage.begin_read()?;
        let events_table = read_txn.open_table(&self.storage, COL_EVENTS)?;

        let mut events = Vec::new();
        let limit = stream.limit.unwrap_or(self.config.max_events_per_query);

        // Use index for efficient filtering when possible
        if let Some(ref event_types) = stream.event_types {
            // Query by event type index
            let index_table = read_txn.open_table(&self.storage, COL_EVENT_INDEX)?;

            for event_type in event_types {
                let prefix = format!("{}:{}:", stream.org_id, event_type);

                let end_key = {
                    let mut end = prefix.as_bytes().to_vec();
                    end.push(0xFF);
                    end
                };
                for (key, _) in index_table.range(prefix.as_bytes()..&end_key)? {
                    if events.len() >= limit {
                        break;
                    }

                    let key_str = std::str::from_utf8(&key).map_err(|e| anyhow!("Invalid UTF-8 in key: {}", e))?;

                    if !key_str.starts_with(&prefix) {
                        break;
                    }

                    // Extract sequence and get event
                    if let Some(sequence_str) = key_str.split(':').last() {
                        if let Ok(sequence) = sequence_str.parse::<u64>() {
                            let event_key = format!("{}:{:020}", stream.org_id, sequence);
                            if let Some(event_data) = events_table.get(event_key.as_bytes())? {
                                let event: Event = if self.config.compression_enabled {
                                    self.decompress_event_data(event_data.value())?
                                } else {
                                    serde_json::from_slice(event_data.value())?
                                };

                                if stream.matches(&event) {
                                    events.push(event);
                                }
                            }
                        }
                    }
                }
            }
        } else {
            // Full scan with filters
            let prefix = format!("{}:", stream.org_id);

            let end_key = {
                let mut end = prefix.as_bytes().to_vec();
                end.push(0xFF);
                end
            };
            for (key, value) in events_table.range(prefix.as_bytes()..&end_key)? {
                if events.len() >= limit {
                    break;
                }

                let key_str = std::str::from_utf8(&key).map_err(|e| anyhow!("Invalid UTF-8 in key: {}", e))?;

                if !key_str.starts_with(&prefix) {
                    break;
                }

                let event: Event = if self.config.compression_enabled {
                    self.decompress_event_data(&value)?
                } else {
                    serde_json::from_slice(&value)?
                };

                if stream.matches(&event) {
                    events.push(event);
                }
            }
        }

        debug!(
            stream_id = %stream.stream_id,
            count = events.len(),
            "Retrieved events for stream"
        );

        Ok(events)
    }

    /// Store a snapshot
    pub async fn store_snapshot(&self, snapshot: &Snapshot) -> Result<()> {
        let mut write_txn = self.storage.begin_write()?;

        {
            let mut snapshots_table = write_txn.open_table(&self.storage, COL_SNAPSHOTS)?;
            let snapshot_key = format!("{}:{}:{:020}",
                snapshot.org_id,
                snapshot.aggregate_id,
                snapshot.sequence_number
            );

            let snapshot_data = serde_json::to_vec(snapshot)?;
            snapshots_table.insert(snapshot_key.as_bytes(), snapshot_data.as_slice())?;
        }

        write_txn.commit(&self.storage)?;

        debug!(
            snapshot_id = %snapshot.id,
            aggregate_id = %snapshot.aggregate_id,
            sequence = snapshot.sequence_number,
            "Snapshot stored"
        );

        Ok(())
    }

    /// Get the latest snapshot for an aggregate
    pub async fn get_latest_snapshot(
        &self,
        org_id: u64,
        aggregate_id: &str,
    ) -> Result<Option<Snapshot>> {
        let read_txn = self.storage.begin_read()?;
        let snapshots_table = read_txn.open_table(&self.storage, COL_SNAPSHOTS)?;

        let prefix = format!("{}:{}:", org_id, aggregate_id);
        let mut latest_snapshot = None;

        // Find the latest snapshot by iterating in reverse
        let end_key = {
            let mut end = prefix.as_bytes().to_vec();
            end.push(0xFF);
            end
        };
        for (key, value) in snapshots_table.range(prefix.as_bytes()..&end_key)? {
            let key_str = std::str::from_utf8(&key).map_err(|e| anyhow!("Invalid UTF-8 in key: {}", e))?;

            if !key_str.starts_with(&prefix) {
                break;
            }

            let snapshot: Snapshot = serde_json::from_slice(&value)?;
            latest_snapshot = Some(snapshot);
        }

        debug!(
            org_id = org_id,
            aggregate_id = %aggregate_id,
            found = latest_snapshot.is_some(),
            "Retrieved latest snapshot"
        );

        Ok(latest_snapshot)
    }

    /// Get store statistics
    pub async fn get_stats(&self) -> Result<EventStoreStats> {
        let read_txn = self.storage.begin_read()?;

        let events_table = read_txn.open_table(&self.storage, COL_EVENTS)?;
        let snapshots_table = read_txn.open_table(&self.storage, COL_SNAPSHOTS)?;

        let mut stats = EventStoreStats {
            total_events: 0,
            total_snapshots: 0,
            events_by_type: std::collections::HashMap::new(),
            events_by_org: std::collections::HashMap::new(),
            storage_size_bytes: 0,
            oldest_event: None,
            newest_event: None,
        };

        // Count events and collect statistics
        for result in events_table.iter()? {
            let (_, value) = result?;
            stats.total_events += 1;
            stats.storage_size_bytes += value.len() as u64;

            let event: Event = if self.config.compression_enabled {
                self.decompress_event_data(&value)?
            } else {
                bincode::deserialize(&value)?
            };

            // Update event type counts
            *stats.events_by_type.entry(event.event_type.to_string()).or_insert(0) += 1;

            // Update org counts
            *stats.events_by_org.entry(event.org_id).or_insert(0) += 1;

            // Update timestamp range
            if stats.oldest_event.is_none() || Some(event.timestamp) < stats.oldest_event {
                stats.oldest_event = Some(event.timestamp);
            }
            if stats.newest_event.is_none() || Some(event.timestamp) > stats.newest_event {
                stats.newest_event = Some(event.timestamp);
            }
        }

        // Count snapshots
        for _ in snapshots_table.iter()? {
            stats.total_snapshots += 1;
        }

        debug!(
            total_events = stats.total_events,
            total_snapshots = stats.total_snapshots,
            "Retrieved event store statistics"
        );

        Ok(stats)
    }

    /// Compact old events (remove events before a certain date, keeping snapshots)
    pub async fn compact_events(&self, before: DateTime<Utc>) -> Result<u64> {
        // First, collect keys to remove
        let read_txn = self.storage.begin_read()?;
        let events_table = read_txn.open_table(&self.storage, COL_EVENTS)?;

        let mut keys_to_remove = Vec::new();
        let mut index_keys_to_remove = Vec::new();

        // Find events to remove
        for result in events_table.iter()? {
            let (key, value) = result?;
            let event: Event = if self.config.compression_enabled {
                self.decompress_event_data(&value)?
            } else {
                bincode::deserialize(&value)?
            };

            if event.timestamp < before {
                keys_to_remove.push(std::str::from_utf8(&key).map_err(|e| anyhow!("Invalid UTF-8 in key: {}", e))?.to_string());

                // Also remove from index
                let index_key = format!("{}:{}",
                    event.org_id,
                    event.event_type,
                );
                index_keys_to_remove.push(index_key);
            }
        }

        drop(read_txn);

        // Now perform the deletions
        let mut write_txn = self.storage.begin_write()?;
        let events_removed = keys_to_remove.len() as u64;

        {
            let mut events_table = write_txn.open_table(&self.storage, COL_EVENTS)?;

            // Remove events
            for key in keys_to_remove {
                events_table.remove(key.as_bytes())?;
            }
        }

        {
            let mut index_table = write_txn.open_table(&self.storage, COL_EVENT_INDEX)?;

            // Remove index entries
            for key in index_keys_to_remove {
                index_table.remove(key.as_bytes())?;
            }
        }

        write_txn.commit(&self.storage)?;

        info!(
            events_removed = events_removed,
            before = %before,
            "Compacted old events"
        );

        Ok(events_removed)
    }

    /// Create an automatic snapshot based on current state
    async fn create_automatic_snapshot(&self, org_id: u64, sequence: u64) -> Result<()> {
        // This is a placeholder - in practice, you'd aggregate events to create meaningful snapshots
        let snapshot = Snapshot::new(
            format!("auto_{}", org_id),
            org_id,
            serde_json::json!({"sequence": sequence, "timestamp": Utc::now()}),
            sequence,
        );

        self.store_snapshot(&snapshot).await?;
        debug!(org_id = org_id, sequence = sequence, "Automatic snapshot created");
        Ok(())
    }

    /// Compress event data for storage
    fn compress_event_data(&self, event: &Event) -> Result<Vec<u8>> {
        // Placeholder for compression - could use zstd, gzip, etc.
        serde_json::to_vec(event).map_err(Into::into)
    }

    /// Decompress event data from storage
    fn decompress_event_data(&self, data: &[u8]) -> Result<Event> {
        // Placeholder for decompression
        serde_json::from_slice(data).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{Event, EventData, EventType};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_event_store_creation() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let event_store = EventStore::new(storage).unwrap();
        let stats = event_store.get_stats().await.unwrap();
        assert_eq!(stats.total_events, 0);
    }

    #[tokio::test]
    async fn test_append_and_retrieve_event() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let event_store = EventStore::new(storage).unwrap();

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

        let sequence = event_store.append_event(&event).await.unwrap();
        assert_eq!(sequence, 1);

        let events = event_store.get_events(None, None).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, event.id);
    }

    #[tokio::test]
    async fn test_event_stream_filtering() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let event_store = EventStore::new(storage).unwrap();

        // Add events of different types
        let job_event = Event::new(
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

        let user_event = Event::new(
            EventType::UserCreated,
            EventData::User {
                user_id: Some(1),
                org_id: 1,
                email: Some("test@example.com".to_string()),
                action: "created".to_string(),
                ip_address: None,
            },
            1,
        );

        event_store.append_event(&job_event).await.unwrap();
        event_store.append_event(&user_event).await.unwrap();

        // Create stream filtering only job events
        let stream = EventStream::new("test_stream".to_string(), 1)
            .with_event_types(vec![EventType::JobCreated]);

        let filtered_events = event_store.get_events_for_stream(&stream).await.unwrap();
        assert_eq!(filtered_events.len(), 1);
        assert_eq!(filtered_events[0].event_type, EventType::JobCreated);
    }

    #[tokio::test]
    async fn test_snapshot_storage() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let event_store = EventStore::new(storage).unwrap();

        let snapshot = Snapshot::new(
            "test_aggregate".to_string(),
            1,
            serde_json::json!({"state": "test"}),
            100,
        );

        event_store.store_snapshot(&snapshot).await.unwrap();

        let retrieved = event_store
            .get_latest_snapshot(1, "test_aggregate")
            .await
            .unwrap();

        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.aggregate_id, "test_aggregate");
        assert_eq!(retrieved.sequence_number, 100);
    }
}