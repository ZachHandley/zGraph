//! Event batching system for efficient processing
//!
//! This module provides configurable event batching to reduce overhead and improve
//! performance when processing large volumes of database changes. It supports
//! time-based, size-based, and transaction-scoped batching strategies.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use tokio::time::interval;
use serde::{Deserialize, Serialize};
use anyhow::Result;
use crate::{Event, EventId};

/// Configuration for event batching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Maximum number of events per batch
    pub max_batch_size: usize,
    /// Maximum age of a batch before forcing flush (milliseconds)
    pub max_batch_age_ms: u64,
    /// Whether to batch events by transaction scope
    pub transaction_scoped: bool,
    /// Whether to batch events by organization
    pub organization_scoped: bool,
    /// Buffer size for incoming events
    pub buffer_size: usize,
    /// Flush interval for periodic batching
    pub flush_interval_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            max_batch_age_ms: 5000, // 5 seconds
            transaction_scoped: true,
            organization_scoped: false,
            buffer_size: 1000,
            flush_interval_ms: 1000, // 1 second
        }
    }
}

/// A batch of events with metadata
#[derive(Debug, Clone)]
pub struct EventBatch {
    pub id: String,
    pub events: Vec<Event>,
    pub created_at: Instant,
    pub transaction_id: Option<String>,
    pub organization_id: Option<u64>,
    pub metadata: HashMap<String, String>,
}

impl EventBatch {
    pub fn new(transaction_id: Option<String>, organization_id: Option<u64>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            events: Vec::new(),
            created_at: Instant::now(),
            transaction_id,
            organization_id,
            metadata: HashMap::new(),
        }
    }

    pub fn add_event(&mut self, event: Event) {
        self.events.push(event);
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    pub fn should_flush(&self, config: &BatchConfig) -> bool {
        self.len() >= config.max_batch_size
            || self.age().as_millis() as u64 >= config.max_batch_age_ms
    }
}

/// Trait for handling batched events
#[async_trait::async_trait]
pub trait BatchHandler: Send + Sync {
    async fn handle_batch(&self, batch: EventBatch) -> Result<()>;
}

/// Event batcher that collects and flushes events according to configuration
pub struct EventBatcher {
    config: BatchConfig,
    handler: Arc<dyn BatchHandler>,
    sender: mpsc::UnboundedSender<BatcherMessage>,
    _handle: tokio::task::JoinHandle<()>,
}

impl std::fmt::Debug for EventBatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventBatcher")
            .field("config", &self.config)
            .field("handler", &"<BatchHandler>")
            .finish()
    }
}

#[derive(Debug)]
enum BatcherMessage {
    AddEvent(Event),
    Flush,
    Stop,
}

/// Internal state for the batcher
struct BatcherState {
    config: BatchConfig,
    handler: Arc<dyn BatchHandler>,
    receiver: mpsc::UnboundedReceiver<BatcherMessage>,
    // Transaction-scoped batches
    transaction_batches: HashMap<String, EventBatch>,
    // Organization-scoped batches
    organization_batches: HashMap<u64, EventBatch>,
    // Global batch for non-scoped events
    global_batch: Option<EventBatch>,
}

impl EventBatcher {
    pub fn new(config: BatchConfig, handler: Arc<dyn BatchHandler>) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();

        let state = BatcherState {
            config: config.clone(),
            handler: handler.clone(),
            receiver,
            transaction_batches: HashMap::new(),
            organization_batches: HashMap::new(),
            global_batch: None,
        };

        let handle = tokio::spawn(Self::run_batcher(state));

        Self {
            config,
            handler,
            sender,
            _handle: handle,
        }
    }

    /// Add an event to be batched
    pub fn add_event(&self, event: Event) -> Result<()> {
        self.sender.send(BatcherMessage::AddEvent(event))
            .map_err(|_| anyhow::anyhow!("Batcher is shut down"))?;
        Ok(())
    }

    /// Force flush all pending batches
    pub fn flush(&self) -> Result<()> {
        self.sender.send(BatcherMessage::Flush)
            .map_err(|_| anyhow::anyhow!("Batcher is shut down"))?;
        Ok(())
    }

    /// Stop the batcher
    pub fn stop(&self) -> Result<()> {
        self.sender.send(BatcherMessage::Stop)
            .map_err(|_| anyhow::anyhow!("Batcher is shut down"))?;
        Ok(())
    }

    async fn run_batcher(mut state: BatcherState) {
        let mut flush_interval = interval(Duration::from_millis(state.config.flush_interval_ms));

        loop {
            tokio::select! {
                message = state.receiver.recv() => {
                    match message {
                        Some(BatcherMessage::AddEvent(event)) => {
                            if let Err(e) = state.handle_add_event(event).await {
                                tracing::error!("Failed to add event to batch: {}", e);
                            }
                        }
                        Some(BatcherMessage::Flush) => {
                            if let Err(e) = state.flush_all_batches().await {
                                tracing::error!("Failed to flush batches: {}", e);
                            }
                        }
                        Some(BatcherMessage::Stop) => {
                            if let Err(e) = state.flush_all_batches().await {
                                tracing::error!("Failed to flush batches during shutdown: {}", e);
                            }
                            break;
                        }
                        None => break, // Channel closed
                    }
                }
                _ = flush_interval.tick() => {
                    if let Err(e) = state.flush_ready_batches().await {
                        tracing::error!("Failed to flush ready batches: {}", e);
                    }
                }
            }
        }
    }
}

impl BatcherState {
    async fn handle_add_event(&mut self, event: Event) -> Result<()> {
        let transaction_id = self.extract_transaction_id(&event);
        let organization_id = Some(event.org_id);
        let event_org_id = event.org_id;

        // Clone transaction_id for later use
        let transaction_id_for_flush = transaction_id.clone();

        // Determine which batch to use
        let batch = if self.config.transaction_scoped && transaction_id.is_some() {
            let tx_id = transaction_id.as_ref().unwrap().clone();
            self.transaction_batches
                .entry(tx_id.clone())
                .or_insert_with(|| EventBatch::new(Some(tx_id), organization_id))
        } else if self.config.organization_scoped {
            self.organization_batches
                .entry(event_org_id)
                .or_insert_with(|| EventBatch::new(transaction_id, Some(event_org_id)))
        } else {
            if self.global_batch.is_none() {
                self.global_batch = Some(EventBatch::new(transaction_id, organization_id));
            }
            self.global_batch.as_mut().unwrap()
        };

        batch.add_event(event);

        // Check if batch should be flushed immediately
        if batch.should_flush(&self.config) {
            let batch_to_flush = if self.config.transaction_scoped && transaction_id_for_flush.is_some() {
                let tx_id = transaction_id_for_flush.as_ref().unwrap();
                self.transaction_batches.remove(tx_id).unwrap()
            } else if self.config.organization_scoped {
                self.organization_batches.remove(&event_org_id).unwrap()
            } else {
                self.global_batch.take().unwrap()
            };

            self.handler.handle_batch(batch_to_flush).await?;
        }

        Ok(())
    }

    async fn flush_ready_batches(&mut self) -> Result<()> {
        let mut to_flush = Vec::new();

        // Check transaction batches
        let mut expired_tx_batches = Vec::new();
        for (tx_id, batch) in &self.transaction_batches {
            if batch.age().as_millis() as u64 >= self.config.max_batch_age_ms {
                expired_tx_batches.push(tx_id.clone());
            }
        }
        for tx_id in expired_tx_batches {
            if let Some(batch) = self.transaction_batches.remove(&tx_id) {
                to_flush.push(batch);
            }
        }

        // Check organization batches
        let mut expired_org_batches = Vec::new();
        for (org_id, batch) in &self.organization_batches {
            if batch.age().as_millis() as u64 >= self.config.max_batch_age_ms {
                expired_org_batches.push(*org_id);
            }
        }
        for org_id in expired_org_batches {
            if let Some(batch) = self.organization_batches.remove(&org_id) {
                to_flush.push(batch);
            }
        }

        // Check global batch
        if let Some(ref batch) = self.global_batch {
            if batch.age().as_millis() as u64 >= self.config.max_batch_age_ms {
                if let Some(batch) = self.global_batch.take() {
                    to_flush.push(batch);
                }
            }
        }

        // Flush all ready batches
        for batch in to_flush {
            if !batch.is_empty() {
                self.handler.handle_batch(batch).await?;
            }
        }

        Ok(())
    }

    async fn flush_all_batches(&mut self) -> Result<()> {
        let mut all_batches = Vec::new();

        // Collect all transaction batches
        for (_, batch) in self.transaction_batches.drain() {
            if !batch.is_empty() {
                all_batches.push(batch);
            }
        }

        // Collect all organization batches
        for (_, batch) in self.organization_batches.drain() {
            if !batch.is_empty() {
                all_batches.push(batch);
            }
        }

        // Collect global batch
        if let Some(batch) = self.global_batch.take() {
            if !batch.is_empty() {
                all_batches.push(batch);
            }
        }

        // Flush all batches
        for batch in all_batches {
            self.handler.handle_batch(batch).await?;
        }

        Ok(())
    }

    fn extract_transaction_id(&self, event: &Event) -> Option<String> {
        // Try to extract transaction ID from event metadata
        if let Some(correlation_id) = &event.metadata.correlation_id {
            return Some(correlation_id.clone());
        }

        // Try to extract from event data
        match &event.data {
            crate::EventData::DatabaseChange { transaction_id, .. } => transaction_id.clone(),
            crate::EventData::Transaction { transaction_id, .. } => Some(transaction_id.clone()),
            _ => None,
        }
    }
}

/// Simple batch handler that logs batches (for testing/debugging)
pub struct LoggingBatchHandler;

#[async_trait::async_trait]
impl BatchHandler for LoggingBatchHandler {
    async fn handle_batch(&self, batch: EventBatch) -> Result<()> {
        tracing::info!(
            "Processing batch {} with {} events (age: {:?}ms)",
            batch.id,
            batch.len(),
            batch.age().as_millis()
        );

        for event in &batch.events {
            tracing::debug!("Event: {:?} -> {:?}", event.event_type, event.id);
        }

        Ok(())
    }
}

/// Batch handler that forwards to the event bus
pub struct EventBusBatchHandler {
    bus: Arc<crate::EventBus>,
}

impl EventBusBatchHandler {
    pub fn new(bus: Arc<crate::EventBus>) -> Self {
        Self { bus }
    }
}

#[async_trait::async_trait]
impl BatchHandler for EventBusBatchHandler {
    async fn handle_batch(&self, batch: EventBatch) -> Result<()> {
        for event in batch.events {
            self.bus.publish(event).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Event, EventType, EventData, EventMetadata};
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio::time::sleep;

    #[derive(Debug, Default)]
    struct TestBatchHandler {
        batches: Arc<StdMutex<Vec<EventBatch>>>,
    }

    #[async_trait::async_trait]
    impl BatchHandler for TestBatchHandler {
        async fn handle_batch(&self, batch: EventBatch) -> Result<()> {
            self.batches.lock().unwrap().push(batch);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_size_based_batching() {
        let handler = Arc::new(TestBatchHandler::default());
        let config = BatchConfig {
            max_batch_size: 3,
            max_batch_age_ms: 10000, // Long timeout
            ..Default::default()
        };

        let batcher = EventBatcher::new(config, handler.clone());

        // Add events to trigger size-based flush
        for i in 0..5 {
            let event = Event::new(
                EventType::RowInserted,
                EventData::Custom(serde_json::json!({"test": i})),
                1,
            );
            batcher.add_event(event).unwrap();
        }

        // Wait a bit for processing
        sleep(Duration::from_millis(100)).await;
        batcher.flush().unwrap();
        sleep(Duration::from_millis(100)).await;

        let batches = handler.batches.lock().unwrap();
        assert!(batches.len() >= 1);

        // First batch should have 3 events (max_batch_size)
        assert_eq!(batches[0].len(), 3);
    }

    #[tokio::test]
    async fn test_time_based_batching() {
        let handler = Arc::new(TestBatchHandler::default());
        let config = BatchConfig {
            max_batch_size: 100, // Large size
            max_batch_age_ms: 100, // Short timeout
            flush_interval_ms: 50,
            ..Default::default()
        };

        let batcher = EventBatcher::new(config, handler.clone());

        // Add a single event
        let event = Event::new(
            EventType::RowInserted,
            EventData::Custom(serde_json::json!({"test": "time"})),
            1,
        );
        batcher.add_event(event).unwrap();

        // Wait for time-based flush
        sleep(Duration::from_millis(200)).await;

        let batches = handler.batches.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
    }
}