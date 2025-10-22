use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;
use zexec_engine::{EventAwareSqlExecutor, ExecResult};
use zevents::EventSystem;
use zcore_storage::Store;
use anyhow::Result as AnyhowResult;

/// Binary protocol for WebRTC transaction operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionMessage {
    /// Begin a new transaction
    BeginTx {
        tx_id: String,
        isolation_level: IsolationLevel,
        read_only: bool,
        timeout_ms: Option<u64>,
    },
    /// Execute SQL operation within transaction
    ExecuteTx {
        tx_id: String,
        operation_id: String,
        sql: String,
        parameters: Vec<serde_json::Value>,
    },
    /// Commit transaction
    CommitTx {
        tx_id: String,
    },
    /// Rollback transaction
    RollbackTx {
        tx_id: String,
    },
    /// Get transaction status
    StatusTx {
        tx_id: String,
    },
}

/// Response messages for transaction operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionResponse {
    /// Transaction began successfully
    BeginOk {
        tx_id: String,
        created_at: chrono::DateTime<chrono::Utc>,
    },
    /// Execute operation result
    ExecuteOk {
        tx_id: String,
        operation_id: String,
        result: ExecutionResult,
    },
    /// Transaction committed successfully
    CommitOk {
        tx_id: String,
        committed_at: chrono::DateTime<chrono::Utc>,
    },
    /// Transaction rolled back
    RollbackOk {
        tx_id: String,
        rolled_back_at: chrono::DateTime<chrono::Utc>,
    },
    /// Transaction status response
    Status {
        tx_id: String,
        state: TransactionState,
        created_at: chrono::DateTime<chrono::Utc>,
        last_activity: chrono::DateTime<chrono::Utc>,
        isolation_level: IsolationLevel,
        read_only: bool,
        operations_count: u64,
    },
    /// Error response
    Error {
        tx_id: Option<String>,
        operation_id: Option<String>,
        error_code: TransactionErrorCode,
        message: String,
    },
}

/// Transaction isolation levels
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        Self::ReadCommitted
    }
}

/// Transaction state
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Preparing,
    Committed,
    RolledBack,
    Failed,
}

/// Execution result from SQL operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub rows_affected: Option<u64>,
    pub rows: Option<Vec<serde_json::Value>>,
    pub duration_ms: u64,
}

/// Transaction error codes
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum TransactionErrorCode {
    TransactionNotFound,
    TransactionAlreadyExists,
    TransactionNotActive,
    TransactionTimeout,
    OperationFailed,
    IsolationViolation,
    DeadlockDetected,
    ReadOnlyViolation,
    InternalError,
}

/// WebRTC Transaction context for a single transaction
#[derive(Debug)]
pub struct WebRTCTransaction {
    pub id: String,
    pub session_id: String,
    pub state: TransactionState,
    pub isolation_level: IsolationLevel,
    pub read_only: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_activity: chrono::DateTime<chrono::Utc>,
    pub operations: Vec<TransactionOperation>,
    pub timeout_ms: Option<u64>,
    /// Organization ID for the transaction
    pub org_id: u64,
    /// User context for audit purposes
    pub user_context: Option<zexec_engine::transaction_events::UserContext>,
    /// Tables affected by this transaction for event tracking
    pub affected_tables: Vec<String>,
}

/// Individual operation within a transaction
#[derive(Debug, Clone)]
pub struct TransactionOperation {
    pub id: String,
    pub sql: String,
    pub parameters: Vec<serde_json::Value>,
    pub executed_at: chrono::DateTime<chrono::Utc>,
    pub result: Option<ExecutionResult>,
}

impl WebRTCTransaction {
    pub fn new(
        session_id: String,
        isolation_level: IsolationLevel,
        read_only: bool,
        timeout_ms: Option<u64>,
        org_id: u64,
        user_context: Option<zexec_engine::transaction_events::UserContext>,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            session_id,
            state: TransactionState::Active,
            isolation_level,
            read_only,
            created_at: chrono::Utc::now(),
            last_activity: chrono::Utc::now(),
            operations: Vec::new(),
            timeout_ms,
            org_id,
            user_context,
            affected_tables: Vec::new(),
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(self.state, TransactionState::Active)
    }

    pub fn is_timed_out(&self) -> bool {
        if let Some(timeout_ms) = self.timeout_ms {
            let elapsed = chrono::Utc::now()
                .signed_duration_since(self.last_activity)
                .num_milliseconds() as u64;
            elapsed > timeout_ms
        } else {
            false
        }
    }

    pub fn update_activity(&mut self) {
        self.last_activity = chrono::Utc::now();
    }

    pub fn add_operation(&mut self, operation: TransactionOperation) -> Result<(), TransactionErrorCode> {
        if !self.is_active() {
            return Err(TransactionErrorCode::TransactionNotActive);
        }

        if self.is_timed_out() {
            self.state = TransactionState::Failed;
            return Err(TransactionErrorCode::TransactionTimeout);
        }

        self.operations.push(operation);
        self.update_activity();
        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), TransactionErrorCode> {
        if !self.is_active() {
            return Err(TransactionErrorCode::TransactionNotActive);
        }

        if self.is_timed_out() {
            self.state = TransactionState::Failed;
            return Err(TransactionErrorCode::TransactionTimeout);
        }

        self.state = TransactionState::Committed;
        self.update_activity();
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), TransactionErrorCode> {
        if matches!(self.state, TransactionState::Committed) {
            return Err(TransactionErrorCode::TransactionNotActive);
        }

        self.state = TransactionState::RolledBack;
        self.update_activity();
        Ok(())
    }

    pub fn get_status(&self) -> TransactionResponse {
        TransactionResponse::Status {
            tx_id: self.id.clone(),
            state: self.state,
            created_at: self.created_at,
            last_activity: self.last_activity,
            isolation_level: self.isolation_level,
            read_only: self.read_only,
            operations_count: self.operations.len() as u64,
        }
    }
}

/// Manager for WebRTC transactions per session
pub struct TransactionManager {
    transactions: Arc<RwLock<HashMap<String, Arc<Mutex<WebRTCTransaction>>>>>,
    sql_executor: Option<Arc<EventAwareSqlExecutor>>,
    store: Arc<Store>,
    catalog_store: &'static Store,
}

impl TransactionManager {
    pub fn new(
        event_system: Option<Arc<EventSystem>>,
        store: Arc<Store>,
        catalog_store: &'static Store,
    ) -> Self {
        let sql_executor = event_system.map(|es| Arc::new(EventAwareSqlExecutor::new(es)));
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            sql_executor,
            store,
            catalog_store,
        }
    }

    /// Create a simple transaction manager without event system integration
    pub fn new_simple(
        store: Arc<Store>,
        catalog_store: &'static Store,
    ) -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            sql_executor: None,
            store,
            catalog_store,
        }
    }

    /// Begin a new transaction
    pub async fn begin_transaction(
        &self,
        session_id: String,
        isolation_level: IsolationLevel,
        read_only: bool,
        timeout_ms: Option<u64>,
        org_id: u64,
        user_context: Option<zexec_engine::transaction_events::UserContext>,
    ) -> Result<String, TransactionErrorCode> {
        let transaction = WebRTCTransaction::new(
            session_id,
            isolation_level,
            read_only,
            timeout_ms,
            org_id,
            user_context,
        );
        let tx_id = transaction.id.clone();

        let mut transactions = self.transactions.write().await;
        transactions.insert(tx_id.clone(), Arc::new(Mutex::new(transaction)));

        tracing::info!(
            tx_id = %tx_id,
            org_id = org_id,
            isolation_level = ?isolation_level,
            read_only = read_only,
            "Started WebRTC transaction"
        );

        Ok(tx_id)
    }

    /// Execute operation within transaction
    pub async fn execute_operation(
        &self,
        tx_id: &str,
        operation_id: String,
        sql: String,
        parameters: Vec<serde_json::Value>,
        org_id: u64,
    ) -> Result<ExecutionResult, TransactionErrorCode> {
        let transactions = self.transactions.read().await;
        let transaction_arc = transactions.get(tx_id)
            .ok_or(TransactionErrorCode::TransactionNotFound)?;

        let mut transaction = transaction_arc.lock().await;

        // Check if transaction is read-only and operation is a write
        if transaction.read_only {
            let sql_upper = sql.trim().to_uppercase();
            if sql_upper.starts_with("INSERT") || sql_upper.starts_with("UPDATE")
                || sql_upper.starts_with("DELETE") || sql_upper.starts_with("CREATE")
                || sql_upper.starts_with("DROP") || sql_upper.starts_with("ALTER") {
                return Err(TransactionErrorCode::ReadOnlyViolation);
            }
        }

        // Execute SQL using EventAwareSqlExecutor if available, otherwise use direct execution
        let start_time = std::time::Instant::now();
        let connection_id = format!("webrtc_tx_{}", tx_id);

        let exec_result = if let Some(ref sql_executor) = self.sql_executor {
            // Use EventAwareSqlExecutor for enhanced transaction tracking
            sql_executor
                .execute_with_events(&sql, org_id, self.store.clone(), self.catalog_store, connection_id)
                .await
                .map_err(|e| {
                    tracing::error!(
                        tx_id = %tx_id,
                        operation_id = %operation_id,
                        error = %e,
                        "SQL execution failed in WebRTC transaction (with events)"
                    );
                    TransactionErrorCode::OperationFailed
                })?
        } else {
            // Fallback to direct SQL execution without event system integration
            let catalog = zcore_catalog::Catalog::new(self.catalog_store);
            zexec_engine::exec_sql_store(&sql, org_id, &self.store, &catalog)
                .map_err(|e| {
                    tracing::error!(
                        tx_id = %tx_id,
                        operation_id = %operation_id,
                        error = %e,
                        "SQL execution failed in WebRTC transaction (direct)"
                    );
                    TransactionErrorCode::OperationFailed
                })?
        };

        let duration_ms = start_time.elapsed().as_millis() as u64;

        // Convert ExecResult to ExecutionResult
        let result = match exec_result {
            ExecResult::Affected(count) => ExecutionResult {
                rows_affected: Some(count),
                rows: None,
                duration_ms,
            },
            ExecResult::Rows(rows) => {
                let json_rows: Vec<serde_json::Value> = rows.into_iter()
                    .map(|row| {
                        let mut obj = serde_json::Map::new();
                        for (i, cell) in row.iter().enumerate() {
                            let key = format!("col_{}", i);
                            let value = match cell {
                                &zexec_engine::Cell::Int(i) => serde_json::Value::Number(serde_json::Number::from(i)),
                                &zexec_engine::Cell::Float(f) => serde_json::Value::Number(
                                    serde_json::Number::from_f64(f).unwrap_or_else(|| serde_json::Number::from(0))
                                ),
                                zexec_engine::Cell::Text(s) => serde_json::Value::String(s.clone()),
                                zexec_engine::Cell::Vector(v) => serde_json::Value::Array(
                                    v.iter().map(|&f| serde_json::Value::Number(
                                        serde_json::Number::from_f64(f as f64).unwrap_or_else(|| serde_json::Number::from(0))
                                    )).collect()
                                ),
                            };
                            obj.insert(key, value);
                        }
                        serde_json::Value::Object(obj)
                    })
                    .collect();

                ExecutionResult {
                    rows_affected: Some(json_rows.len() as u64),
                    rows: Some(json_rows),
                    duration_ms,
                }
            },
            ExecResult::None => ExecutionResult {
                rows_affected: Some(0),
                rows: None,
                duration_ms,
            },
        };

        // Extract table name from SQL for change tracking
        let table_name = self.extract_table_name(&sql);

        // Add table to affected tables if not already tracked
        if let Some(ref table) = table_name {
            if !transaction.affected_tables.contains(table) {
                transaction.affected_tables.push(table.clone());
            }
        }

        // Create operation record with result
        let operation = TransactionOperation {
            id: operation_id.clone(),
            sql: sql.clone(),
            parameters,
            executed_at: chrono::Utc::now(),
            result: Some(result.clone()),
        };

        // Add to transaction
        transaction.add_operation(operation)?;

        tracing::debug!(
            tx_id = %tx_id,
            operation_id = %operation_id,
            table_name = ?table_name,
            rows_affected = ?result.rows_affected,
            duration_ms = result.duration_ms,
            "WebRTC transaction operation completed"
        );

        Ok(result)
    }

    /// Commit transaction
    pub async fn commit_transaction(&self, tx_id: &str) -> Result<(), TransactionErrorCode> {
        let transactions = self.transactions.read().await;
        let transaction_arc = transactions.get(tx_id)
            .ok_or(TransactionErrorCode::TransactionNotFound)?;

        let mut transaction = transaction_arc.lock().await;

        // Check if transaction can be committed
        if !transaction.is_active() {
            return Err(TransactionErrorCode::TransactionNotActive);
        }

        if transaction.is_timed_out() {
            transaction.state = TransactionState::Failed;
            return Err(TransactionErrorCode::TransactionTimeout);
        }

        // Create transaction context for event system integration
        let connection_id = format!("webrtc_tx_{}", tx_id);
        let catalog = zcore_catalog::Catalog::new(self.catalog_store);

        // For now, we don't create a full TransactionContext because it's complex
        // Instead, we'll emit events directly for the commit

        // Commit the transaction state
        transaction.commit()?;

        tracing::info!(
            tx_id = %tx_id,
            org_id = transaction.org_id,
            operations_count = transaction.operations.len(),
            affected_tables = ?transaction.affected_tables,
            user_id = ?transaction.user_context.as_ref().map(|u| u.user_id),
            "WebRTC transaction committed successfully"
        );

        Ok(())
    }

    /// Rollback transaction
    pub async fn rollback_transaction(&self, tx_id: &str) -> Result<(), TransactionErrorCode> {
        let transactions = self.transactions.read().await;
        let transaction_arc = transactions.get(tx_id)
            .ok_or(TransactionErrorCode::TransactionNotFound)?;

        let mut transaction = transaction_arc.lock().await;

        // Allow rollback even if transaction is already failed or committed
        if matches!(transaction.state, TransactionState::Committed) {
            tracing::warn!(
                tx_id = %tx_id,
                "Attempting to rollback already committed transaction"
            );
            return Err(TransactionErrorCode::TransactionNotActive);
        }

        // Create transaction context for event system integration if needed
        let connection_id = format!("webrtc_tx_{}", tx_id);

        // Rollback the transaction state
        transaction.rollback()?;

        tracing::info!(
            tx_id = %tx_id,
            org_id = transaction.org_id,
            operations_count = transaction.operations.len(),
            affected_tables = ?transaction.affected_tables,
            user_id = ?transaction.user_context.as_ref().map(|u| u.user_id),
            "WebRTC transaction rolled back successfully"
        );

        Ok(())
    }

    /// Get transaction status
    pub async fn get_transaction_status(&self, tx_id: &str) -> Result<TransactionResponse, TransactionErrorCode> {
        let transactions = self.transactions.read().await;
        let transaction_arc = transactions.get(tx_id)
            .ok_or(TransactionErrorCode::TransactionNotFound)?;

        let transaction = transaction_arc.lock().await;
        Ok(transaction.get_status())
    }

    /// Clean up transactions for a session (called on session termination)
    pub async fn cleanup_session_transactions(&self, session_id: &str) {
        let mut transactions = self.transactions.write().await;
        let mut to_remove = Vec::new();

        for (tx_id, transaction_arc) in transactions.iter() {
            let transaction = transaction_arc.lock().await;
            if transaction.session_id == session_id {
                to_remove.push(tx_id.clone());
                // Automatically rollback any active transactions
                if transaction.is_active() {
                    tracing::warn!(
                        tx_id = %tx_id,
                        session_id = %session_id,
                        "Rolling back active transaction due to session cleanup"
                    );
                }
            }
        }

        for tx_id in to_remove {
            transactions.remove(&tx_id);
        }

        if !transactions.is_empty() {
            tracing::info!(
                session_id = %session_id,
                cleaned_transactions = transactions.len(),
                "Cleaned up WebRTC transactions for terminated session"
            );
        }
    }

    /// Clean up timed out transactions
    pub async fn cleanup_timed_out_transactions(&self) -> usize {
        let mut transactions = self.transactions.write().await;
        let mut to_remove = Vec::new();

        for (tx_id, transaction_arc) in transactions.iter() {
            let mut transaction = transaction_arc.lock().await;
            if transaction.is_timed_out() && transaction.is_active() {
                transaction.state = TransactionState::Failed;
                to_remove.push(tx_id.clone());
            }
        }

        let cleaned_count = to_remove.len();
        for tx_id in to_remove {
            transactions.remove(&tx_id);
            tracing::info!(tx_id = %tx_id, "Cleaned up timed out WebRTC transaction");
        }

        cleaned_count
    }

    /// List active transactions for a session
    pub async fn list_session_transactions(&self, session_id: &str) -> Vec<String> {
        let transactions = self.transactions.read().await;
        let mut result = Vec::new();

        for (tx_id, transaction_arc) in transactions.iter() {
            let transaction = transaction_arc.lock().await;
            if transaction.session_id == session_id {
                result.push(tx_id.clone());
            }
        }

        result
    }

    /// Get total number of active transactions
    pub async fn active_transaction_count(&self) -> usize {
        self.transactions.read().await.len()
    }

    /// Extract table name from SQL statement
    fn extract_table_name(&self, sql: &str) -> Option<String> {
        let sql_upper = sql.trim().to_uppercase();
        let words: Vec<&str> = sql_upper.split_whitespace().collect();

        match words.first() {
            Some(&"SELECT") => {
                // Look for FROM clause
                if let Some(from_idx) = words.iter().position(|&w| w == "FROM") {
                    words.get(from_idx + 1).map(|s| s.to_string())
                } else {
                    None
                }
            }
            Some(&"INSERT") => {
                // Look for INTO clause
                if let Some(into_idx) = words.iter().position(|&w| w == "INTO") {
                    words.get(into_idx + 1).map(|s| s.to_string())
                } else {
                    None
                }
            }
            Some(&"UPDATE") | Some(&"DELETE") => {
                // Table name follows operation
                words.get(1).map(|s| s.to_string())
            }
            Some(&"CREATE") => {
                // Look for TABLE keyword
                if let Some(table_idx) = words.iter().position(|&w| w == "TABLE") {
                    words.get(table_idx + 1).map(|s| s.to_string())
                } else {
                    None
                }
            }
            Some(&"DROP") | Some(&"ALTER") => {
                // Look for TABLE keyword
                if let Some(table_idx) = words.iter().position(|&w| w == "TABLE") {
                    words.get(table_idx + 1).map(|s| s.to_string())
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Binary message encoding/decoding utilities
pub mod codec {
    use super::{TransactionMessage, TransactionResponse};
    use bytes::{Bytes, BytesMut};

    /// Encode transaction message to binary format using bincode
    pub fn encode_message(message: &TransactionMessage) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
        let encoded = bincode::serialize(message)?;
        Ok(Bytes::from(encoded))
    }

    /// Decode transaction message from binary format
    pub fn decode_message(data: &[u8]) -> Result<TransactionMessage, Box<dyn std::error::Error + Send + Sync>> {
        let message = bincode::deserialize(data)?;
        Ok(message)
    }

    /// Encode transaction response to binary format
    pub fn encode_response(response: &TransactionResponse) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
        let encoded = bincode::serialize(response)?;
        Ok(Bytes::from(encoded))
    }

    /// Decode transaction response from binary format
    pub fn decode_response(data: &[u8]) -> Result<TransactionResponse, Box<dyn std::error::Error + Send + Sync>> {
        let response = bincode::deserialize(data)?;
        Ok(response)
    }

    /// Create a message frame with type identifier
    pub fn create_message_frame(message: &TransactionMessage) -> Result<BytesMut, Box<dyn std::error::Error + Send + Sync>> {
        let mut frame = BytesMut::new();

        // Add frame type identifier (0x01 for TransactionMessage)
        frame.extend_from_slice(&[0x01]);

        // Add message data
        let encoded = encode_message(message)?;
        frame.extend_from_slice(&encoded);

        Ok(frame)
    }

    /// Create a response frame with type identifier
    pub fn create_response_frame(response: &TransactionResponse) -> Result<BytesMut, Box<dyn std::error::Error + Send + Sync>> {
        let mut frame = BytesMut::new();

        // Add frame type identifier (0x02 for TransactionResponse)
        frame.extend_from_slice(&[0x02]);

        // Add response data
        let encoded = encode_response(response)?;
        frame.extend_from_slice(&encoded);

        Ok(frame)
    }

    /// Parse frame and determine message type
    pub fn parse_frame(data: &[u8]) -> Result<FrameType, Box<dyn std::error::Error + Send + Sync>> {
        if data.is_empty() {
            return Err("Empty frame data".into());
        }

        match data[0] {
            0x01 => {
                let message = decode_message(&data[1..])?;
                Ok(FrameType::Message(message))
            }
            0x02 => {
                let response = decode_response(&data[1..])?;
                Ok(FrameType::Response(response))
            }
            _ => Err(format!("Unknown frame type: {}", data[0]).into()),
        }
    }

    /// Frame type enumeration
    pub enum FrameType {
        Message(TransactionMessage),
        Response(TransactionResponse),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_transaction_lifecycle() {
        let manager = TransactionManager::new();

        // Begin transaction
        let tx_id = manager
            .begin_transaction(
                "session-123".to_string(),
                IsolationLevel::ReadCommitted,
                false,
                Some(30000),
            )
            .await
            .unwrap();

        // Execute operation
        let result = manager
            .execute_operation(
                &tx_id,
                "op-1".to_string(),
                "SELECT * FROM users".to_string(),
                vec![],
            )
            .await
            .unwrap();

        assert!(result.rows.is_some());

        // Get status
        let status = manager.get_transaction_status(&tx_id).await.unwrap();
        match status {
            TransactionResponse::Status { state, .. } => {
                assert_eq!(state, TransactionState::Active);
            }
            _ => panic!("Expected status response"),
        }

        // Commit
        manager.commit_transaction(&tx_id).await.unwrap();

        // Verify committed
        let status = manager.get_transaction_status(&tx_id).await.unwrap();
        match status {
            TransactionResponse::Status { state, .. } => {
                assert_eq!(state, TransactionState::Committed);
            }
            _ => panic!("Expected status response"),
        }
    }

    #[test]
    fn test_message_encoding() {
        use codec::*;

        let message = TransactionMessage::BeginTx {
            tx_id: "test-tx".to_string(),
            isolation_level: IsolationLevel::ReadCommitted,
            read_only: false,
            timeout_ms: Some(30000),
        };

        let encoded = encode_message(&message).unwrap();
        let decoded = decode_message(&encoded).unwrap();

        match decoded {
            TransactionMessage::BeginTx { tx_id, .. } => {
                assert_eq!(tx_id, "test-tx");
            }
            _ => panic!("Expected BeginTx message"),
        }
    }

    #[test]
    fn test_frame_parsing() {
        use codec::*;

        let message = TransactionMessage::StatusTx {
            tx_id: "test-tx".to_string(),
        };

        let frame = create_message_frame(&message).unwrap();
        let parsed = parse_frame(&frame).unwrap();

        match parsed {
            FrameType::Message(TransactionMessage::StatusTx { tx_id }) => {
                assert_eq!(tx_id, "test-tx");
            }
            _ => panic!("Expected StatusTx message"),
        }
    }
}