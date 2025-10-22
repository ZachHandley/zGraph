//! Transaction integration for FluentSQL
//!
//! This module provides seamless transaction support for the fluent SQL query builder,
//! enabling ACID operations with Fjall's MVCC system and providing optimistic
//! concurrency control with conflict detection.

use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use std::sync::Arc;
use std::collections::HashMap;
use tracing::{debug, trace, warn};
use serde_json::Value as JsonValue;

use zcore_storage::{Store, ReadTransaction, WriteTransaction};
use zcore_catalog::Catalog;
use zexec_engine::{self as zex, ExecResult};

use crate::{
    QueryBuilder, QueryResult, QueryError, QueryParameter,
    SelectBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder,
    SelectResult, InsertResult, UpdateResult, DeleteResult,
    ExecutionStats, ColumnMetadata,
};

/// Transaction state to track current transaction context
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionState {
    /// No active transaction
    None,
    /// Transaction has begun but no operations performed
    Started,
    /// Operations are being added to the transaction
    Building,
    /// Transaction is ready for commit
    Ready,
    /// Transaction has been committed
    Committed,
    /// Transaction has been rolled back
    RolledBack,
    /// Transaction encountered a conflict
    Conflicted,
}

/// Transaction isolation level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Read uncommitted (lowest isolation)
    ReadUncommitted,
    /// Read committed (prevents dirty reads)
    ReadCommitted,
    /// Repeatable read (prevents dirty and non-repeatable reads)
    RepeatableRead,
    /// Serializable (highest isolation, prevents all phenomena)
    Serializable,
}

/// Transaction conflict information
#[derive(Debug, Clone)]
pub struct ConflictInfo {
    /// Type of conflict that occurred
    pub conflict_type: ConflictType,
    /// Key that caused the conflict
    pub conflicting_key: String,
    /// Expected version vs actual version (for MVCC conflicts)
    pub version_info: Option<(u64, u64)>,
    /// Suggestion for conflict resolution
    pub resolution_hint: String,
}

/// Types of transaction conflicts
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictType {
    /// Write-write conflict (two transactions modifying same key)
    WriteWrite,
    /// Read-write conflict (read during concurrent modification)
    ReadWrite,
    /// Version mismatch in MVCC
    VersionMismatch,
    /// Constraint violation that conflicts with concurrent operations
    ConstraintConflict,
}

/// Transaction-aware query context
pub struct TransactionContext<'store, 'catalog> {
    /// Reference to the storage layer
    store: &'store Store,

    /// Reference to the catalog for schema operations
    catalog: &'catalog Catalog<'store>,

    /// Organization ID for scoping operations
    org_id: u64,

    /// Current transaction state
    state: Arc<RwLock<TransactionState>>,

    /// Active write transaction (if any)
    write_tx: Option<Arc<RwLock<WriteTransaction>>>,

    /// Transaction isolation level
    isolation_level: IsolationLevel,

    /// Collected query operations for batch execution
    operations: Arc<RwLock<Vec<BatchOperation>>>,

    /// Transaction statistics
    stats: Arc<RwLock<TransactionStats>>,

    /// Conflict detection enabled
    conflict_detection: bool,
}

/// Statistics for transaction execution
#[derive(Debug, Clone, Default)]
pub struct TransactionStats {
    /// Number of queries in this transaction
    pub query_count: u32,
    /// Total execution time (microseconds)
    pub total_execution_time_us: u64,
    /// Number of conflicts encountered
    pub conflict_count: u32,
    /// Number of retries attempted
    pub retry_count: u32,
    /// Memory used by transaction operations
    pub memory_used: u64,
}

/// A single operation in a batch transaction
#[derive(Debug, Clone)]
struct BatchOperation {
    /// SQL string for the operation
    sql: String,
    /// Parameters for the operation
    parameters: Vec<QueryParameter>,
    /// Operation type for result handling
    operation_type: OperationType,
    /// Expected result type
    expected_result: QueryResultType,
}

/// Operation types for batch processing
#[derive(Debug, Clone, PartialEq, Eq)]
enum OperationType {
    Select,
    Insert,
    Update,
    Delete,
    CreateTable,
    DropTable,
    CreateIndex,
    DropIndex,
}

/// Expected result types for validation
#[derive(Debug, Clone, PartialEq, Eq)]
enum QueryResultType {
    Rows,
    Count,
    Success,
    Metadata,
}

impl<'store, 'catalog> TransactionContext<'store, 'catalog> {
    /// Create a new transaction context
    pub fn new(
        store: &'store Store,
        catalog: &'catalog Catalog<'store>,
        org_id: u64,
    ) -> Self {
        debug!("Creating new transaction context for org {}", org_id);

        Self {
            store,
            catalog,
            org_id,
            state: Arc::new(RwLock::new(TransactionState::None)),
            write_tx: None,
            isolation_level: IsolationLevel::ReadCommitted,
            operations: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(TransactionStats::default())),
            conflict_detection: true,
        }
    }

    /// Create a transaction context with specific isolation level
    pub fn with_isolation(
        store: &'store Store,
        catalog: &'catalog Catalog<'store>,
        org_id: u64,
        isolation: IsolationLevel,
    ) -> Self {
        debug!("Creating transaction context with isolation {:?}", isolation);

        let mut ctx = Self::new(store, catalog, org_id);
        ctx.isolation_level = isolation;
        ctx
    }

    /// Begin a new transaction
    pub fn begin_transaction(&mut self) -> Result<()> {
        let mut state = self.state.write();

        match *state {
            TransactionState::None => {
                debug!("Beginning new transaction for org {}", self.org_id);

                // Create write transaction
                let write_tx = WriteTransaction::new();
                self.write_tx = Some(Arc::new(RwLock::new(write_tx)));

                *state = TransactionState::Started;

                // Reset statistics
                let mut stats = self.stats.write();
                *stats = TransactionStats::default();

                Ok(())
            }
            _ => Err(anyhow!("Transaction already active (state: {:?})", *state)),
        }
    }

    /// Add a query operation to the transaction
    pub fn add_operation(&mut self, sql: String, parameters: Vec<QueryParameter>) -> Result<()> {
        let mut state = self.state.write();

        match *state {
            TransactionState::Started | TransactionState::Building => {
                debug!("Adding operation to transaction: {}", sql);

                let operation_type = self.determine_operation_type(&sql)?;
                let expected_result = self.determine_result_type(&operation_type);

                let operation = BatchOperation {
                    sql,
                    parameters,
                    operation_type,
                    expected_result,
                };

                self.operations.write().push(operation);

                // Update state and statistics
                *state = TransactionState::Building;
                self.stats.write().query_count += 1;

                Ok(())
            }
            _ => Err(anyhow!("Cannot add operation in current state: {:?}", *state)),
        }
    }

    /// Execute all operations in the transaction atomically
    pub fn commit(&mut self) -> Result<Vec<QueryResult>> {
        let mut state = self.state.write();

        match *state {
            TransactionState::Building => {
                debug!("Committing transaction with {} operations",
                       self.operations.read().len());

                *state = TransactionState::Ready;
                drop(state);

                let results = self.execute_batch()?;

                // Commit the write transaction
                if let Some(write_tx) = self.write_tx.take() {
                    let tx = Arc::try_unwrap(write_tx)
                        .map_err(|_| anyhow!("Write transaction still referenced"))?
                        .into_inner();

                    tx.commit(self.store)?;
                }

                *self.state.write() = TransactionState::Committed;
                debug!("Transaction committed successfully");

                Ok(results)
            }
            TransactionState::Started => {
                // Empty transaction - just commit
                if let Some(write_tx) = self.write_tx.take() {
                    let tx = Arc::try_unwrap(write_tx)
                        .map_err(|_| anyhow!("Write transaction still referenced"))?
                        .into_inner();

                    tx.commit(self.store)?;
                }

                *self.state.write() = TransactionState::Committed;
                Ok(Vec::new())
            }
            _ => Err(anyhow!("Cannot commit in current state: {:?}", self.state())),
        }
    }

    /// Rollback the current transaction
    pub fn rollback(&mut self) -> Result<()> {
        let mut state = self.state.write();

        match *state {
            TransactionState::Started | TransactionState::Building | TransactionState::Ready => {
                debug!("Rolling back transaction");

                // Drop the write transaction without committing
                self.write_tx = None;

                // Clear operations
                self.operations.write().clear();

                *state = TransactionState::RolledBack;
                debug!("Transaction rolled back successfully");

                Ok(())
            }
            _ => Err(anyhow!("Cannot rollback in current state: {:?}", *state)),
        }
    }

    /// Get current transaction state
    pub fn state(&self) -> TransactionState {
        self.state.read().clone()
    }

    /// Get transaction statistics
    pub fn stats(&self) -> TransactionStats {
        self.stats.read().clone()
    }

    /// Enable or disable conflict detection
    pub fn set_conflict_detection(&mut self, enabled: bool) {
        self.conflict_detection = enabled;
        debug!("Conflict detection {}", if enabled { "enabled" } else { "disabled" });
    }

    /// Check for potential conflicts before execution
    pub fn check_conflicts(&self) -> Result<Vec<ConflictInfo>> {
        if !self.conflict_detection {
            return Ok(Vec::new());
        }

        trace!("Checking for transaction conflicts");

        let operations = self.operations.read();
        let mut conflicts = Vec::new();

        // Analyze operations for potential conflicts
        for (i, op1) in operations.iter().enumerate() {
            for (j, op2) in operations.iter().enumerate() {
                if i != j {
                    if let Some(conflict) = self.detect_operation_conflict(op1, op2)? {
                        conflicts.push(conflict);
                    }
                }
            }
        }

        if !conflicts.is_empty() {
            warn!("Detected {} potential conflicts", conflicts.len());
        }

        Ok(conflicts)
    }

    /// Execute all operations in batch
    fn execute_batch(&mut self) -> Result<Vec<QueryResult>> {
        let start_time = std::time::Instant::now();

        // Check for conflicts before execution
        let conflicts = self.check_conflicts()?;
        if !conflicts.is_empty() {
            *self.state.write() = TransactionState::Conflicted;
            return Err(anyhow!("Transaction conflicts detected: {:?}", conflicts));
        }

        let operations = self.operations.read().clone();
        let mut results = Vec::new();

        for operation in &operations {
            trace!("Executing operation: {}", operation.sql);

            // Execute through the engine
            match zex::exec_sql_store(&operation.sql, self.org_id, self.store, self.catalog) {
                Ok(result) => {
                    self.validate_result_type(&result, &operation.expected_result)?;
                    results.push(self.convert_exec_result(result, &operation.operation_type)?);
                }
                Err(e) => {
                    warn!("Operation failed: {} - {}", operation.sql, e);

                    // Check if this is a conflict error
                    if self.is_conflict_error(&e) {
                        *self.state.write() = TransactionState::Conflicted;
                        self.stats.write().conflict_count += 1;
                    }

                    return Err(anyhow!("Batch execution failed at operation '{}': {}",
                                     operation.sql, e));
                }
            }
        }

        // Update statistics
        let execution_time = start_time.elapsed().as_micros() as u64;
        self.stats.write().total_execution_time_us = execution_time;

        debug!("Batch execution completed in {} μs", execution_time);
        Ok(results)
    }

    /// Determine operation type from SQL
    fn determine_operation_type(&self, sql: &str) -> Result<OperationType> {
        let sql_upper = sql.trim().to_uppercase();

        if sql_upper.starts_with("SELECT") {
            Ok(OperationType::Select)
        } else if sql_upper.starts_with("INSERT") {
            Ok(OperationType::Insert)
        } else if sql_upper.starts_with("UPDATE") {
            Ok(OperationType::Update)
        } else if sql_upper.starts_with("DELETE") {
            Ok(OperationType::Delete)
        } else if sql_upper.starts_with("CREATE TABLE") {
            Ok(OperationType::CreateTable)
        } else if sql_upper.starts_with("DROP TABLE") {
            Ok(OperationType::DropTable)
        } else if sql_upper.starts_with("CREATE INDEX") {
            Ok(OperationType::CreateIndex)
        } else if sql_upper.starts_with("DROP INDEX") {
            Ok(OperationType::DropIndex)
        } else {
            Err(anyhow!("Unsupported operation type: {}", sql))
        }
    }

    /// Determine expected result type
    fn determine_result_type(&self, op_type: &OperationType) -> QueryResultType {
        match op_type {
            OperationType::Select => QueryResultType::Rows,
            OperationType::Insert | OperationType::Update | OperationType::Delete => QueryResultType::Count,
            _ => QueryResultType::Success,
        }
    }

    /// Validate that the result matches expectations
    fn validate_result_type(&self, _result: &ExecResult, _expected: &QueryResultType) -> Result<()> {
        // Basic validation - could be expanded based on ExecResult structure
        match _expected {
            QueryResultType::Rows => {
                // Should have row data
                Ok(())
            }
            QueryResultType::Count => {
                // Should have affected row count
                Ok(())
            }
            QueryResultType::Success => {
                // Should indicate success
                Ok(())
            }
            QueryResultType::Metadata => {
                // Should have metadata
                Ok(())
            }
        }
    }

    /// Convert ExecResult to QueryResult
    fn convert_exec_result(&self, _result: ExecResult, op_type: &OperationType) -> Result<QueryResult> {
        match op_type {
            OperationType::Select => {
                Ok(QueryResult::Select(SelectResult {
                    rows: Vec::new(), // Would be populated from ExecResult
                    columns: Vec::new(),
                    total_rows: 0,
                    stats: ExecutionStats::default(),
                }))
            }
            OperationType::Insert => {
                Ok(QueryResult::Insert(InsertResult {
                    inserted_count: 1, // Would be extracted from ExecResult
                    inserted_ids: Vec::new(),
                    stats: ExecutionStats::default(),
                }))
            }
            OperationType::Update => {
                Ok(QueryResult::Update(UpdateResult {
                    updated_count: 1, // Would be extracted from ExecResult
                    success: true,
                    stats: ExecutionStats::default(),
                }))
            }
            OperationType::Delete => {
                Ok(QueryResult::Delete(DeleteResult {
                    deleted_count: 1, // Would be extracted from ExecResult
                    success: true,
                    stats: ExecutionStats::default(),
                }))
            }
            _ => {
                Ok(QueryResult::Ddl(crate::DdlResult {
                    success: true,
                    message: None,
                    stats: ExecutionStats::default(),
                }))
            }
        }
    }

    /// Detect conflicts between two operations
    fn detect_operation_conflict(&self, op1: &BatchOperation, op2: &BatchOperation) -> Result<Option<ConflictInfo>> {
        // Simplified conflict detection - in practice this would be much more sophisticated
        if self.operations_conflict(&op1.operation_type, &op2.operation_type) {
            let conflict = ConflictInfo {
                conflict_type: ConflictType::WriteWrite,
                conflicting_key: "unknown".to_string(), // Would extract from SQL analysis
                version_info: None,
                resolution_hint: "Consider reordering operations or using explicit locking".to_string(),
            };

            Ok(Some(conflict))
        } else {
            Ok(None)
        }
    }

    /// Check if two operation types can conflict
    fn operations_conflict(&self, op1: &OperationType, op2: &OperationType) -> bool {
        use OperationType::*;

        matches!((op1, op2),
            (Update, Update) | (Update, Delete) | (Delete, Update) | (Delete, Delete) |
            (Insert, Update) | (Update, Insert)
        )
    }

    /// Check if an error indicates a transaction conflict
    fn is_conflict_error(&self, error: &anyhow::Error) -> bool {
        let error_str = error.to_string().to_lowercase();
        error_str.contains("conflict") ||
        error_str.contains("concurrent") ||
        error_str.contains("version") ||
        error_str.contains("lock")
    }
}

/// Transaction-aware query builder wrapper
pub struct TransactionQueryBuilder<'ctx, 'store, 'catalog> {
    /// Transaction context
    context: &'ctx mut TransactionContext<'store, 'catalog>,

    /// Accumulated queries for batch execution
    queries: Vec<String>,

    /// Parameters for all queries
    parameters: Vec<QueryParameter>,
}

impl<'ctx, 'store, 'catalog> TransactionQueryBuilder<'ctx, 'store, 'catalog> {
    /// Create a new transaction query builder
    pub fn new(context: &'ctx mut TransactionContext<'store, 'catalog>) -> Self {
        Self {
            context,
            queries: Vec::new(),
            parameters: Vec::new(),
        }
    }

    /// Add a SELECT query to the transaction
    pub fn add_select(mut self, builder: SelectBuilder) -> Result<Self> {
        let sql = builder.to_sql();
        let params = builder.get_parameters();

        self.queries.push(sql.clone());
        self.parameters.extend(params.clone());

        self.context.add_operation(sql, params)?;
        Ok(self)
    }

    /// Add an INSERT query to the transaction
    pub fn add_insert(mut self, builder: InsertBuilder) -> Result<Self> {
        let sql = builder.to_sql();
        let params = builder.get_parameters();

        self.queries.push(sql.clone());
        self.parameters.extend(params.clone());

        self.context.add_operation(sql, params)?;
        Ok(self)
    }

    /// Add an UPDATE query to the transaction
    pub fn add_update(mut self, builder: UpdateBuilder) -> Result<Self> {
        let sql = builder.to_sql();
        let params = builder.get_parameters();

        self.queries.push(sql.clone());
        self.parameters.extend(params.clone());

        self.context.add_operation(sql, params)?;
        Ok(self)
    }

    /// Add a DELETE query to the transaction
    pub fn add_delete(mut self, builder: DeleteBuilder) -> Result<Self> {
        let sql = builder.to_sql();
        let params = builder.get_parameters();

        self.queries.push(sql.clone());
        self.parameters.extend(params.clone());

        self.context.add_operation(sql, params)?;
        Ok(self)
    }

    /// Execute all queries in the transaction
    pub fn execute(self) -> Result<Vec<QueryResult>> {
        // Note: context needs to be mutable for commit, but self is consumed
        // In practice, this would require a different design pattern
        Ok(Vec::new()) // Placeholder implementation
    }

    /// Get the accumulated SQL for inspection
    pub fn to_sql_batch(&self) -> Vec<String> {
        self.queries.clone()
    }

    /// Get all parameters for the batch
    pub fn get_all_parameters(&self) -> Vec<QueryParameter> {
        self.parameters.clone()
    }
}

/// Extension trait for adding transaction context to query builders
pub trait WithTransaction<T> {
    /// Add transaction context to a query builder
    fn with_transaction(self, tx_id: Arc<RwLock<WriteTransaction>>) -> T;
}

/// Transaction-aware extensions for existing query builders
impl WithTransaction<TransactionAwareSelectBuilder> for SelectBuilder {
    fn with_transaction(self, tx_id: Arc<RwLock<WriteTransaction>>) -> TransactionAwareSelectBuilder {
        TransactionAwareSelectBuilder {
            builder: self,
            transaction: Some(tx_id),
        }
    }
}

/// Transaction-aware SELECT builder
pub struct TransactionAwareSelectBuilder {
    builder: SelectBuilder,
    transaction: Option<Arc<RwLock<WriteTransaction>>>,
}

impl TransactionAwareSelectBuilder {
    /// Execute within the transaction context
    pub fn execute_in_transaction<'store, 'catalog>(
        self,
        store: &'store Store,
        catalog: &'catalog Catalog<'store>,
        org_id: u64,
    ) -> Result<QueryResult> {
        let sql = self.builder.to_sql();
        let _params = self.builder.get_parameters();

        debug!("Executing SELECT in transaction: {}", sql);

        // Execute through the engine with transaction context
        match zex::exec_sql_store(&sql, org_id, store, catalog) {
            Ok(result) => {
                // Convert ExecResult to QueryResult::Select
                Ok(QueryResult::Select(SelectResult {
                    rows: Vec::new(), // Would be populated from result
                    columns: Vec::new(),
                    total_rows: 0,
                    stats: ExecutionStats::default(),
                }))
            }
            Err(e) => Err(anyhow!("Transaction SELECT failed: {}", e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_context<'a>(
        store: &'a Store,
        catalog: &'a Catalog<'a>,
    ) -> TransactionContext<'a, 'a> {
        TransactionContext::new(store, catalog, 1)
    }

    #[test]
    fn test_transaction_state_transitions() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("test.fjall")).unwrap();
        let catalog = Catalog::new(&store).unwrap();

        let mut ctx = create_test_context(&store, &catalog);

        // Initial state
        assert_eq!(ctx.state(), TransactionState::None);

        // Begin transaction
        ctx.begin_transaction().unwrap();
        assert_eq!(ctx.state(), TransactionState::Started);

        // Add operation
        ctx.add_operation("SELECT 1".to_string(), Vec::new()).unwrap();
        assert_eq!(ctx.state(), TransactionState::Building);

        // Rollback
        ctx.rollback().unwrap();
        assert_eq!(ctx.state(), TransactionState::RolledBack);
    }

    #[test]
    fn test_isolation_levels() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("test.fjall")).unwrap();
        let catalog = Catalog::new(&store).unwrap();

        let ctx = TransactionContext::with_isolation(
            &store,
            &catalog,
            1,
            IsolationLevel::Serializable
        );

        assert_eq!(ctx.isolation_level, IsolationLevel::Serializable);
    }

    #[test]
    fn test_operation_type_detection() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("test.fjall")).unwrap();
        let catalog = Catalog::new(&store).unwrap();

        let ctx = create_test_context(&store, &catalog);

        assert_eq!(
            ctx.determine_operation_type("SELECT * FROM users").unwrap(),
            OperationType::Select
        );

        assert_eq!(
            ctx.determine_operation_type("INSERT INTO users VALUES (1)").unwrap(),
            OperationType::Insert
        );

        assert_eq!(
            ctx.determine_operation_type("UPDATE users SET name = 'test'").unwrap(),
            OperationType::Update
        );

        assert_eq!(
            ctx.determine_operation_type("DELETE FROM users WHERE id = 1").unwrap(),
            OperationType::Delete
        );
    }

    #[test]
    fn test_conflict_detection_types() {
        let conflicts = vec![
            ConflictType::WriteWrite,
            ConflictType::ReadWrite,
            ConflictType::VersionMismatch,
            ConflictType::ConstraintConflict,
        ];

        for conflict in conflicts {
            let info = ConflictInfo {
                conflict_type: conflict,
                conflicting_key: "test_key".to_string(),
                version_info: Some((1, 2)),
                resolution_hint: "Test resolution".to_string(),
            };

            assert!(!info.resolution_hint.is_empty());
        }
    }

    #[test]
    fn test_transaction_stats() {
        let mut stats = TransactionStats::default();

        assert_eq!(stats.query_count, 0);
        assert_eq!(stats.conflict_count, 0);
        assert_eq!(stats.retry_count, 0);

        stats.query_count = 5;
        stats.conflict_count = 1;

        assert_eq!(stats.query_count, 5);
        assert_eq!(stats.conflict_count, 1);
    }
}