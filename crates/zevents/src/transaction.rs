use crate::events::{Event, EventData, EventId, EventMetadata, EventType};
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Transaction lifecycle states
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    /// Transaction has been initiated
    Begun,
    /// Transaction is actively executing operations
    Active,
    /// Transaction is being prepared for commit
    Preparing,
    /// Transaction has been successfully committed
    Committed,
    /// Transaction has been rolled back
    RolledBack,
    /// Transaction was aborted due to error
    Aborted,
}

/// Detailed transaction operation information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionOperation {
    /// Operation type (INSERT, UPDATE, DELETE, SELECT, etc.)
    pub operation_type: String,
    /// Affected table name
    pub table_name: Option<String>,
    /// Number of rows affected
    pub rows_affected: Option<u64>,
    /// Operation duration in milliseconds
    pub duration_ms: Option<u64>,
    /// Operation-specific metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Extended transaction data for detailed tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionData {
    /// Unique transaction identifier
    pub transaction_id: String,
    /// Connection ID that initiated the transaction
    pub connection_id: String,
    /// Organization ID for multi-tenancy
    pub org_id: u64,
    /// Current transaction state
    pub state: TransactionState,
    /// When the transaction was started
    pub started_at: DateTime<Utc>,
    /// When the transaction was last modified
    pub updated_at: DateTime<Utc>,
    /// Transaction duration in milliseconds
    pub duration_ms: Option<u64>,
    /// Number of operations in this transaction
    pub operations_count: u64,
    /// List of operations performed in this transaction
    pub operations: Vec<TransactionOperation>,
    /// Transaction isolation level
    pub isolation_level: Option<String>,
    /// Error information if transaction failed
    pub error: Option<String>,
    /// Additional transaction metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Transaction event manager for comprehensive lifecycle tracking
pub struct TransactionEventManager {
    /// Active transactions tracking
    active_transactions: Arc<RwLock<HashMap<String, TransactionData>>>,
    /// Event publisher for broadcasting transaction events
    event_publisher: Option<Arc<dyn EventPublisher>>,
    /// Transaction metrics collector
    metrics: Arc<RwLock<TransactionMetrics>>,
}

/// Trait for event publishing abstraction
#[async_trait::async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(&self, event: Event) -> Result<()>;
    async fn publish_batch(&self, events: Vec<Event>) -> Result<()>;
}

/// Transaction performance and health metrics
#[derive(Debug, Clone, Default)]
pub struct TransactionMetrics {
    /// Total transactions started
    pub total_transactions: u64,
    /// Total transactions committed
    pub committed_transactions: u64,
    /// Total transactions rolled back
    pub rolled_back_transactions: u64,
    /// Total transactions aborted
    pub aborted_transactions: u64,
    /// Average transaction duration in milliseconds
    pub avg_duration_ms: f64,
    /// Maximum transaction duration seen
    pub max_duration_ms: u64,
    /// Minimum transaction duration seen
    pub min_duration_ms: u64,
    /// Transactions by organization
    pub transactions_by_org: HashMap<u64, u64>,
    /// Active transaction count
    pub active_count: u64,
}

impl TransactionEventManager {
    /// Create a new transaction event manager
    pub fn new() -> Self {
        Self {
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            event_publisher: None,
            metrics: Arc::new(RwLock::new(TransactionMetrics::default())),
        }
    }

    /// Create a new transaction event manager with event publisher
    pub fn with_publisher(publisher: Arc<dyn EventPublisher>) -> Self {
        Self {
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            event_publisher: Some(publisher),
            metrics: Arc::new(RwLock::new(TransactionMetrics::default())),
        }
    }

    /// Begin a new transaction and emit event
    pub async fn begin_transaction(
        &self,
        transaction_id: String,
        connection_id: String,
        org_id: u64,
        isolation_level: Option<String>,
    ) -> Result<()> {
        let now = Utc::now();

        let transaction_data = TransactionData {
            transaction_id: transaction_id.clone(),
            connection_id: connection_id.clone(),
            org_id,
            state: TransactionState::Begun,
            started_at: now,
            updated_at: now,
            duration_ms: None,
            operations_count: 0,
            operations: Vec::new(),
            isolation_level: isolation_level.clone(),
            error: None,
            metadata: HashMap::new(),
        };

        // Store transaction data
        self.active_transactions
            .write()
            .await
            .insert(transaction_id.clone(), transaction_data.clone());

        // Update metrics
        {
            let mut metrics = self.metrics.write().await;
            metrics.total_transactions += 1;
            metrics.active_count += 1;
            *metrics.transactions_by_org.entry(org_id).or_insert(0) += 1;
        }

        // Create and publish event
        let event = Event::new(
            EventType::TransactionBegun,
            EventData::Transaction {
                transaction_id: transaction_id.clone(),
                connection_id: connection_id.clone(),
                org_id,
                operations_count: Some(0),
                duration_ms: None,
                error: None,
            },
            org_id,
        )
        .with_source("zexec-engine".to_string())
        .with_metadata(EventMetadata {
            source: "zexec-engine".to_string(),
            correlation_id: Some(transaction_id.clone()),
            causation_id: None,
            version: "1.0".to_string(),
            attributes: {
                let mut attrs = HashMap::new();
                if let Some(ref level) = isolation_level {
                    attrs.insert("isolation_level".to_string(), level.clone());
                }
                attrs.insert("state".to_string(), "begun".to_string());
                attrs
            },
        });

        if let Some(publisher) = &self.event_publisher {
            publisher.publish(event).await?;
        }

        info!(
            transaction_id = %transaction_id,
            org_id = org_id,
            connection_id = %connection_id,
            "Transaction begun"
        );

        Ok(())
    }

    /// Add an operation to an active transaction
    pub async fn add_operation(
        &self,
        transaction_id: &str,
        operation: TransactionOperation,
    ) -> Result<()> {
        let mut transactions = self.active_transactions.write().await;

        if let Some(transaction) = transactions.get_mut(transaction_id) {
            transaction.operations.push(operation.clone());
            transaction.operations_count += 1;
            transaction.updated_at = Utc::now();

            // Update transaction state to active if it was just begun
            if transaction.state == TransactionState::Begun {
                transaction.state = TransactionState::Active;
            }

            debug!(
                transaction_id = %transaction_id,
                operation_type = %operation.operation_type,
                table_name = ?operation.table_name,
                rows_affected = ?operation.rows_affected,
                "Added operation to transaction"
            );
        } else {
            warn!(
                transaction_id = %transaction_id,
                "Attempted to add operation to non-existent transaction"
            );
        }

        Ok(())
    }

    /// Commit a transaction and emit event
    pub async fn commit_transaction(&self, transaction_id: &str) -> Result<()> {
        let mut transactions = self.active_transactions.write().await;

        if let Some(mut transaction) = transactions.remove(transaction_id) {
            let now = Utc::now();
            let duration_ms = now
                .signed_duration_since(transaction.started_at)
                .num_milliseconds() as u64;

            transaction.state = TransactionState::Committed;
            transaction.updated_at = now;
            transaction.duration_ms = Some(duration_ms);

            // Update metrics
            {
                let mut metrics = self.metrics.write().await;
                metrics.committed_transactions += 1;
                metrics.active_count = metrics.active_count.saturating_sub(1);

                // Update duration statistics
                if metrics.max_duration_ms < duration_ms {
                    metrics.max_duration_ms = duration_ms;
                }
                if metrics.min_duration_ms == 0 || metrics.min_duration_ms > duration_ms {
                    metrics.min_duration_ms = duration_ms;
                }

                // Recalculate average duration
                let total_completed = metrics.committed_transactions + metrics.rolled_back_transactions + metrics.aborted_transactions;
                if total_completed > 0 {
                    metrics.avg_duration_ms = (metrics.avg_duration_ms * (total_completed - 1) as f64 + duration_ms as f64) / total_completed as f64;
                }
            }

            // Create and publish commit event
            let event = Event::new(
                EventType::TransactionCommitted,
                EventData::Transaction {
                    transaction_id: transaction.transaction_id.clone(),
                    connection_id: transaction.connection_id.clone(),
                    org_id: transaction.org_id,
                    operations_count: Some(transaction.operations_count),
                    duration_ms: Some(duration_ms),
                    error: None,
                },
                transaction.org_id,
            )
            .with_source("zexec-engine".to_string())
            .with_metadata(EventMetadata {
                source: "zexec-engine".to_string(),
                correlation_id: Some(transaction.transaction_id.clone()),
                causation_id: None,
                version: "1.0".to_string(),
                attributes: {
                    let mut attrs = HashMap::new();
                    attrs.insert("state".to_string(), "committed".to_string());
                    attrs.insert("operations_count".to_string(), transaction.operations_count.to_string());
                    attrs.insert("duration_ms".to_string(), duration_ms.to_string());
                    attrs
                },
            });

            if let Some(publisher) = &self.event_publisher {
                publisher.publish(event).await?;
            }

            info!(
                transaction_id = %transaction_id,
                org_id = transaction.org_id,
                operations_count = transaction.operations_count,
                duration_ms = duration_ms,
                "Transaction committed"
            );
        } else {
            warn!(
                transaction_id = %transaction_id,
                "Attempted to commit non-existent transaction"
            );
        }

        Ok(())
    }

    /// Rollback a transaction and emit event
    pub async fn rollback_transaction(&self, transaction_id: &str, error: Option<String>) -> Result<()> {
        let mut transactions = self.active_transactions.write().await;

        if let Some(mut transaction) = transactions.remove(transaction_id) {
            let now = Utc::now();
            let duration_ms = now
                .signed_duration_since(transaction.started_at)
                .num_milliseconds() as u64;

            transaction.state = TransactionState::RolledBack;
            transaction.updated_at = now;
            transaction.duration_ms = Some(duration_ms);
            transaction.error = error.clone();

            // Update metrics
            {
                let mut metrics = self.metrics.write().await;
                metrics.rolled_back_transactions += 1;
                metrics.active_count = metrics.active_count.saturating_sub(1);
            }

            // Create and publish rollback event
            let event = Event::new(
                EventType::TransactionRolledBack,
                EventData::Transaction {
                    transaction_id: transaction.transaction_id.clone(),
                    connection_id: transaction.connection_id.clone(),
                    org_id: transaction.org_id,
                    operations_count: Some(transaction.operations_count),
                    duration_ms: Some(duration_ms),
                    error: error.clone(),
                },
                transaction.org_id,
            )
            .with_source("zexec-engine".to_string())
            .with_metadata(EventMetadata {
                source: "zexec-engine".to_string(),
                correlation_id: Some(transaction.transaction_id.clone()),
                causation_id: None,
                version: "1.0".to_string(),
                attributes: {
                    let mut attrs = HashMap::new();
                    attrs.insert("state".to_string(), "rolled_back".to_string());
                    attrs.insert("operations_count".to_string(), transaction.operations_count.to_string());
                    attrs.insert("duration_ms".to_string(), duration_ms.to_string());
                    if let Some(err) = &error {
                        attrs.insert("error".to_string(), err.clone());
                    }
                    attrs
                },
            });

            if let Some(publisher) = &self.event_publisher {
                publisher.publish(event).await?;
            }

            info!(
                transaction_id = %transaction_id,
                org_id = transaction.org_id,
                operations_count = transaction.operations_count,
                duration_ms = duration_ms,
                error = ?error,
                "Transaction rolled back"
            );
        } else {
            warn!(
                transaction_id = %transaction_id,
                "Attempted to rollback non-existent transaction"
            );
        }

        Ok(())
    }

    /// Abort a transaction due to system error
    pub async fn abort_transaction(&self, transaction_id: &str, error: String) -> Result<()> {
        let mut transactions = self.active_transactions.write().await;

        if let Some(mut transaction) = transactions.remove(transaction_id) {
            let now = Utc::now();
            let duration_ms = now
                .signed_duration_since(transaction.started_at)
                .num_milliseconds() as u64;

            transaction.state = TransactionState::Aborted;
            transaction.updated_at = now;
            transaction.duration_ms = Some(duration_ms);
            transaction.error = Some(error.clone());

            // Update metrics
            {
                let mut metrics = self.metrics.write().await;
                metrics.aborted_transactions += 1;
                metrics.active_count = metrics.active_count.saturating_sub(1);
            }

            // Create and publish abort event
            let event = Event::new(
                EventType::TransactionAborted,
                EventData::Transaction {
                    transaction_id: transaction.transaction_id.clone(),
                    connection_id: transaction.connection_id.clone(),
                    org_id: transaction.org_id,
                    operations_count: Some(transaction.operations_count),
                    duration_ms: Some(duration_ms),
                    error: Some(error.clone()),
                },
                transaction.org_id,
            )
            .with_source("zexec-engine".to_string())
            .with_metadata(EventMetadata {
                source: "zexec-engine".to_string(),
                correlation_id: Some(transaction.transaction_id.clone()),
                causation_id: None,
                version: "1.0".to_string(),
                attributes: {
                    let mut attrs = HashMap::new();
                    attrs.insert("state".to_string(), "aborted".to_string());
                    attrs.insert("operations_count".to_string(), transaction.operations_count.to_string());
                    attrs.insert("duration_ms".to_string(), duration_ms.to_string());
                    attrs.insert("error".to_string(), error.clone());
                    attrs
                },
            });

            if let Some(publisher) = &self.event_publisher {
                publisher.publish(event).await?;
            }

            warn!(
                transaction_id = %transaction_id,
                org_id = transaction.org_id,
                operations_count = transaction.operations_count,
                duration_ms = duration_ms,
                error = %error,
                "Transaction aborted"
            );
        } else {
            warn!(
                transaction_id = %transaction_id,
                "Attempted to abort non-existent transaction"
            );
        }

        Ok(())
    }

    /// Get transaction data by ID
    pub async fn get_transaction(&self, transaction_id: &str) -> Option<TransactionData> {
        self.active_transactions.read().await.get(transaction_id).cloned()
    }

    /// Get all active transactions for an organization
    pub async fn get_active_transactions(&self, org_id: u64) -> Vec<TransactionData> {
        self.active_transactions
            .read()
            .await
            .values()
            .filter(|tx| tx.org_id == org_id)
            .cloned()
            .collect()
    }

    /// Get transaction metrics
    pub async fn get_metrics(&self) -> TransactionMetrics {
        self.metrics.read().await.clone()
    }

    /// Clean up old transactions (should be called periodically)
    pub async fn cleanup_stale_transactions(&self, timeout_minutes: i64) -> Result<u64> {
        let mut transactions = self.active_transactions.write().await;
        let cutoff = Utc::now() - chrono::Duration::minutes(timeout_minutes);

        let mut cleaned = 0u64;
        let stale_transactions: Vec<String> = transactions
            .iter()
            .filter(|(_, tx)| tx.updated_at < cutoff)
            .map(|(id, _)| id.clone())
            .collect();

        for transaction_id in stale_transactions {
            if let Some(transaction) = transactions.remove(&transaction_id) {
                cleaned += 1;

                // Emit abort event for stale transaction
                if let Some(publisher) = &self.event_publisher {
                    let event = Event::new(
                        EventType::TransactionAborted,
                        EventData::Transaction {
                            transaction_id: transaction.transaction_id.clone(),
                            connection_id: transaction.connection_id.clone(),
                            org_id: transaction.org_id,
                            operations_count: Some(transaction.operations_count),
                            duration_ms: transaction.duration_ms,
                            error: Some("Transaction timeout".to_string()),
                        },
                        transaction.org_id,
                    )
                    .with_source("zevents-cleanup".to_string());

                    if let Err(e) = publisher.publish(event).await {
                        warn!("Failed to publish cleanup event: {}", e);
                    }
                }

                warn!(
                    transaction_id = %transaction_id,
                    org_id = transaction.org_id,
                    "Cleaned up stale transaction"
                );
            }
        }

        // Update metrics
        if cleaned > 0 {
            let mut metrics = self.metrics.write().await;
            metrics.aborted_transactions += cleaned;
            metrics.active_count = metrics.active_count.saturating_sub(cleaned);
        }

        Ok(cleaned)
    }

    /// Create transaction change events for cache invalidation
    pub async fn create_change_events(
        &self,
        transaction_id: &str,
        affected_tables: Vec<String>,
        org_id: u64,
    ) -> Vec<Event> {
        let mut events = Vec::new();

        for table_name in affected_tables {
            let event = Event::new(
                EventType::SchemaChanged,
                EventData::Database {
                    org_id,
                    table_name: Some(table_name.clone()),
                    index_name: None,
                    operation: "transaction_commit".to_string(),
                    rows_affected: None,
                    duration_ms: None,
                },
                org_id,
            )
            .with_source("zevents-transaction".to_string())
            .with_correlation_id(transaction_id.to_string())
            .with_metadata(EventMetadata {
                source: "zevents-transaction".to_string(),
                correlation_id: Some(transaction_id.to_string()),
                causation_id: None,
                version: "1.0".to_string(),
                attributes: {
                    let mut attrs = HashMap::new();
                    attrs.insert("event_purpose".to_string(), "cache_invalidation".to_string());
                    attrs.insert("table_name".to_string(), table_name.clone());
                    attrs
                },
            });

            events.push(event);
        }

        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    struct MockPublisher {
        published: Arc<RwLock<Vec<Event>>>,
    }

    impl MockPublisher {
        fn new() -> Self {
            Self {
                published: Arc::new(RwLock::new(Vec::new())),
            }
        }

        async fn get_published(&self) -> Vec<Event> {
            self.published.read().await.clone()
        }
    }

    #[async_trait::async_trait]
    impl EventPublisher for MockPublisher {
        async fn publish(&self, event: Event) -> Result<()> {
            self.published.write().await.push(event);
            Ok(())
        }

        async fn publish_batch(&self, events: Vec<Event>) -> Result<()> {
            self.published.write().await.extend(events);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_transaction_lifecycle() {
        let publisher = Arc::new(MockPublisher::new());
        let manager = TransactionEventManager::with_publisher(publisher.clone() as Arc<dyn EventPublisher>);

        let transaction_id = "tx_123".to_string();
        let connection_id = "conn_456".to_string();
        let org_id = 1;

        // Begin transaction
        manager.begin_transaction(transaction_id.clone(), connection_id.clone(), org_id, None).await.unwrap();

        // Add operation
        let operation = TransactionOperation {
            operation_type: "INSERT".to_string(),
            table_name: Some("users".to_string()),
            rows_affected: Some(1),
            duration_ms: Some(10),
            metadata: HashMap::new(),
        };
        manager.add_operation(&transaction_id, operation).await.unwrap();

        // Commit transaction
        manager.commit_transaction(&transaction_id).await.unwrap();

        // Check published events
        let events = publisher.get_published().await;
        assert_eq!(events.len(), 2); // Begin and commit events
        assert_eq!(events[0].event_type, EventType::TransactionBegun);
        assert_eq!(events[1].event_type, EventType::TransactionCommitted);

        // Check metrics
        let metrics = manager.get_metrics().await;
        assert_eq!(metrics.total_transactions, 1);
        assert_eq!(metrics.committed_transactions, 1);
        assert_eq!(metrics.active_count, 0);
    }

    #[tokio::test]
    async fn test_transaction_rollback() {
        let publisher = Arc::new(MockPublisher::new());
        let manager = TransactionEventManager::with_publisher(publisher.clone() as Arc<dyn EventPublisher>);

        let transaction_id = "tx_789".to_string();
        let connection_id = "conn_101".to_string();
        let org_id = 2;
        let error_msg = "Constraint violation".to_string();

        // Begin transaction
        manager.begin_transaction(transaction_id.clone(), connection_id, org_id, None).await.unwrap();

        // Rollback transaction
        manager.rollback_transaction(&transaction_id, Some(error_msg.clone())).await.unwrap();

        // Check published events
        let events = publisher.get_published().await;
        assert_eq!(events.len(), 2); // Begin and rollback events
        assert_eq!(events[0].event_type, EventType::TransactionBegun);
        assert_eq!(events[1].event_type, EventType::TransactionRolledBack);

        // Check metrics
        let metrics = manager.get_metrics().await;
        assert_eq!(metrics.total_transactions, 1);
        assert_eq!(metrics.rolled_back_transactions, 1);
        assert_eq!(metrics.active_count, 0);
    }
}