//! Batch operations for multi-query transactions
//!
//! This module provides comprehensive batch operation support for executing
//! multiple SQL queries atomically within a single transaction context.

use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use parking_lot::RwLock;
use tracing::{debug, trace, warn, info};

use zcore_storage::{Store, WriteTransaction};
use zcore_catalog::Catalog;
use zexec_engine::{self as zex, ExecResult};

use crate::{
    QueryResult, QueryParameter, ExecutionStats, QueryBuilder,
    SelectBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder,
    SelectResult, InsertResult, UpdateResult, DeleteResult, DdlResult,
    transaction::{TransactionContext, TransactionState, ConflictInfo, ConflictType},
};

/// Batch query builder for executing multiple operations atomically
pub struct BatchQueryBuilder<'store, 'catalog> {
    /// Transaction context for batch operations
    context: TransactionContext<'store, 'catalog>,

    /// Batch operations to execute
    operations: Vec<BatchedQuery>,

    /// Execution configuration
    config: BatchExecutionConfig,

    /// Statistics collector
    stats: BatchStats,
}

/// Configuration for batch execution
#[derive(Debug, Clone)]
pub struct BatchExecutionConfig {
    /// Maximum number of operations per batch
    pub max_batch_size: usize,

    /// Whether to continue on individual query failures
    pub continue_on_error: bool,

    /// Whether to validate all queries before execution
    pub validate_before_execution: bool,

    /// Timeout for the entire batch (milliseconds)
    pub batch_timeout_ms: u64,

    /// Enable detailed execution logging
    pub detailed_logging: bool,
}

impl Default for BatchExecutionConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,
            continue_on_error: false,
            validate_before_execution: true,
            batch_timeout_ms: 30_000, // 30 seconds
            detailed_logging: false,
        }
    }
}

/// A single query within a batch operation
#[derive(Debug, Clone)]
pub struct BatchedQuery {
    /// Unique identifier for this query within the batch
    pub id: String,

    /// SQL statement to execute
    pub sql: String,

    /// Parameters for the query
    pub parameters: Vec<QueryParameter>,

    /// Expected query type for result validation
    pub query_type: BatchQueryType,

    /// Optional dependency on other queries in the batch
    pub depends_on: Vec<String>,

    /// Priority for execution ordering (higher = earlier)
    pub priority: i32,
}

/// Types of queries that can be batched
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchQueryType {
    Select,
    Insert,
    Update,
    Delete,
    CreateTable,
    DropTable,
    CreateIndex,
    DropIndex,
    Custom(String),
}

/// Statistics for batch execution
#[derive(Debug, Clone, Default)]
pub struct BatchStats {
    /// Total number of operations in the batch
    pub total_operations: usize,

    /// Number of successfully executed operations
    pub successful_operations: usize,

    /// Number of failed operations
    pub failed_operations: usize,

    /// Total execution time (microseconds)
    pub total_execution_time_us: u64,

    /// Time spent on validation (microseconds)
    pub validation_time_us: u64,

    /// Time spent on conflict detection (microseconds)
    pub conflict_detection_time_us: u64,

    /// Number of conflicts detected and resolved
    pub conflicts_resolved: usize,

    /// Memory used during batch execution
    pub memory_used_bytes: u64,
}

impl<'store, 'catalog> BatchQueryBuilder<'store, 'catalog> {
    /// Create a new batch query builder
    pub fn new(
        store: &'store Store,
        catalog: &'catalog Catalog<'store>,
        org_id: u64,
    ) -> Self {
        debug!("Creating new batch query builder for org {}", org_id);

        Self {
            context: TransactionContext::new(store, catalog, org_id),
            operations: Vec::new(),
            config: BatchExecutionConfig::default(),
            stats: BatchStats::default(),
        }
    }

    /// Create a batch builder with custom configuration
    pub fn with_config(
        store: &'store Store,
        catalog: &'catalog Catalog<'store>,
        org_id: u64,
        config: BatchExecutionConfig,
    ) -> Self {
        debug!("Creating batch query builder with custom config for org {}", org_id);

        Self {
            context: TransactionContext::new(store, catalog, org_id),
            operations: Vec::new(),
            config,
            stats: BatchStats::default(),
        }
    }

    /// Add a SELECT query to the batch
    pub fn add_select(&mut self, id: String, builder: SelectBuilder) -> Result<&mut Self> {
        let sql = builder.to_sql();
        let parameters = builder.get_parameters();

        self.add_query(BatchedQuery {
            id,
            sql,
            parameters,
            query_type: BatchQueryType::Select,
            depends_on: Vec::new(),
            priority: 0,
        })?;

        Ok(self)
    }

    /// Add an INSERT query to the batch
    pub fn add_insert(&mut self, id: String, builder: InsertBuilder) -> Result<&mut Self> {
        let sql = builder.to_sql();
        let parameters = builder.get_parameters();

        self.add_query(BatchedQuery {
            id,
            sql,
            parameters,
            query_type: BatchQueryType::Insert,
            depends_on: Vec::new(),
            priority: 0,
        })?;

        Ok(self)
    }

    /// Add an UPDATE query to the batch
    pub fn add_update(&mut self, id: String, builder: UpdateBuilder) -> Result<&mut Self> {
        let sql = builder.to_sql();
        let parameters = builder.get_parameters();

        self.add_query(BatchedQuery {
            id,
            sql,
            parameters,
            query_type: BatchQueryType::Update,
            depends_on: Vec::new(),
            priority: 0,
        })?;

        Ok(self)
    }

    /// Add a DELETE query to the batch
    pub fn add_delete(&mut self, id: String, builder: DeleteBuilder) -> Result<&mut Self> {
        let sql = builder.to_sql();
        let parameters = builder.get_parameters();

        self.add_query(BatchedQuery {
            id,
            sql,
            parameters,
            query_type: BatchQueryType::Delete,
            depends_on: Vec::new(),
            priority: 0,
        })?;

        Ok(self)
    }

    /// Add a custom query to the batch
    pub fn add_custom_query(
        &mut self,
        id: String,
        sql: String,
        parameters: Vec<QueryParameter>,
        query_type: BatchQueryType,
    ) -> Result<&mut Self> {
        self.add_query(BatchedQuery {
            id,
            sql,
            parameters,
            query_type,
            depends_on: Vec::new(),
            priority: 0,
        })?;

        Ok(self)
    }

    /// Add a query with dependencies
    pub fn add_query_with_deps(
        &mut self,
        mut query: BatchedQuery,
        dependencies: Vec<String>,
    ) -> Result<&mut Self> {
        query.depends_on = dependencies;
        self.add_query(query)?;
        Ok(self)
    }

    /// Add a raw query to the batch
    fn add_query(&mut self, query: BatchedQuery) -> Result<()> {
        if self.operations.len() >= self.config.max_batch_size {
            return Err(anyhow!(
                "Batch size limit exceeded: {} >= {}",
                self.operations.len(),
                self.config.max_batch_size
            ));
        }

        // Validate query ID is unique
        if self.operations.iter().any(|q| q.id == query.id) {
            return Err(anyhow!("Query ID '{}' already exists in batch", query.id));
        }

        debug!("Adding query '{}' to batch: {}", query.id, query.sql);
        self.operations.push(query);
        self.stats.total_operations += 1;

        Ok(())
    }

    /// Execute all queries in the batch atomically
    pub fn execute(mut self) -> Result<BatchExecutionResult> {
        let start_time = Instant::now();

        info!("Executing batch with {} operations", self.operations.len());

        // Begin transaction
        self.context.begin_transaction()?;

        let mut results = HashMap::new();
        let mut execution_errors = Vec::new();

        // Validate all queries if enabled
        if self.config.validate_before_execution {
            let validation_start = Instant::now();
            self.validate_all_queries()?;
            self.stats.validation_time_us = validation_start.elapsed().as_micros() as u64;
        }

        // Detect and resolve conflicts
        let conflict_start = Instant::now();
        let conflicts = self.detect_and_resolve_conflicts()?;
        self.stats.conflict_detection_time_us = conflict_start.elapsed().as_micros() as u64;
        self.stats.conflicts_resolved = conflicts.len();

        // Sort operations by dependencies and priority
        let execution_order = self.determine_execution_order()?;

        // Execute queries in order
        for operation_id in execution_order {
            let operation = self.operations.iter()
                .find(|op| op.id == operation_id)
                .ok_or_else(|| anyhow!("Operation '{}' not found", operation_id))?
                .clone(); // Clone to avoid borrowing issues

            match self.execute_single_operation(&operation) {
                Ok(result) => {
                    results.insert(operation_id.clone(), result);
                    self.stats.successful_operations += 1;

                    if self.config.detailed_logging {
                        debug!("Successfully executed operation: {}", operation_id);
                    }
                }
                Err(e) => {
                    warn!("Operation '{}' failed: {}", operation_id, e);
                    execution_errors.push((operation_id.clone(), e));
                    self.stats.failed_operations += 1;

                    if !self.config.continue_on_error {
                        // Rollback and return error
                        self.context.rollback()?;
                        return Err(anyhow!(
                            "Batch execution failed at operation '{}': {}",
                            operation_id,
                            execution_errors.last().unwrap().1
                        ));
                    }
                }
            }
        }

        // Commit transaction if no critical errors
        if execution_errors.is_empty() || self.config.continue_on_error {
            let commit_results = self.context.commit()?;

            // Update results with actual commit results
            let result_keys: Vec<String> = results.keys().cloned().collect();
            for (i, op_id) in result_keys.iter().enumerate() {
                if let Some(commit_result) = commit_results.get(i) {
                    results.insert(op_id.clone(), commit_result.clone());
                }
            }
        } else {
            self.context.rollback()?;
        }

        self.stats.total_execution_time_us = start_time.elapsed().as_micros() as u64;

        let is_successful = execution_errors.is_empty();
        Ok(BatchExecutionResult {
            results,
            errors: execution_errors,
            stats: self.stats,
            conflicts_detected: conflicts,
            execution_successful: is_successful,
        })
    }

    /// Validate all queries in the batch
    fn validate_all_queries(&self) -> Result<()> {
        trace!("Validating {} queries in batch", self.operations.len());

        for operation in &self.operations {
            self.validate_single_query(operation)?;
        }

        debug!("All queries validated successfully");
        Ok(())
    }

    /// Validate a single query
    fn validate_single_query(&self, query: &BatchedQuery) -> Result<()> {
        // Basic SQL syntax validation
        if query.sql.trim().is_empty() {
            return Err(anyhow!("Query '{}' has empty SQL", query.id));
        }

        // Validate query type matches SQL
        let sql_upper = query.sql.trim().to_uppercase();
        let expected_prefix = match query.query_type {
            BatchQueryType::Select => "SELECT",
            BatchQueryType::Insert => "INSERT",
            BatchQueryType::Update => "UPDATE",
            BatchQueryType::Delete => "DELETE",
            BatchQueryType::CreateTable => "CREATE TABLE",
            BatchQueryType::DropTable => "DROP TABLE",
            BatchQueryType::CreateIndex => "CREATE INDEX",
            BatchQueryType::DropIndex => "DROP INDEX",
            BatchQueryType::Custom(_) => return Ok(()), // Skip validation for custom queries
        };

        if !sql_upper.starts_with(expected_prefix) {
            return Err(anyhow!(
                "Query '{}' type mismatch: expected {}, but SQL starts with different command",
                query.id, expected_prefix
            ));
        }

        Ok(())
    }

    /// Detect and resolve conflicts between operations
    fn detect_and_resolve_conflicts(&mut self) -> Result<Vec<ConflictInfo>> {
        trace!("Detecting conflicts in batch operations");

        let mut conflicts = Vec::new();

        // Check for conflicts between operations
        for (i, op1) in self.operations.iter().enumerate() {
            for (j, op2) in self.operations.iter().enumerate() {
                if i != j {
                    if let Some(conflict) = self.detect_operation_conflict(op1, op2)? {
                        conflicts.push(conflict);
                    }
                }
            }
        }

        // Resolve conflicts by adjusting operation order or dependencies
        for conflict in &conflicts {
            self.resolve_conflict(conflict)?;
        }

        if !conflicts.is_empty() {
            debug!("Detected and resolved {} conflicts", conflicts.len());
        }

        Ok(conflicts)
    }

    /// Detect conflict between two operations
    fn detect_operation_conflict(&self, op1: &BatchedQuery, op2: &BatchedQuery) -> Result<Option<ConflictInfo>> {
        // Simplified conflict detection - in practice this would analyze table access patterns
        let conflicting_types = [
            (BatchQueryType::Update, BatchQueryType::Update),
            (BatchQueryType::Update, BatchQueryType::Delete),
            (BatchQueryType::Delete, BatchQueryType::Update),
            (BatchQueryType::Insert, BatchQueryType::Update),
        ];

        if conflicting_types.contains(&(op1.query_type.clone(), op2.query_type.clone())) {
            let conflict = ConflictInfo {
                conflict_type: ConflictType::WriteWrite,
                conflicting_key: format!("{}:{}", op1.id, op2.id),
                version_info: None,
                resolution_hint: format!(
                    "Consider adding dependency: {} should execute before {}",
                    op1.id, op2.id
                ),
            };

            Ok(Some(conflict))
        } else {
            Ok(None)
        }
    }

    /// Resolve a detected conflict
    fn resolve_conflict(&mut self, conflict: &ConflictInfo) -> Result<()> {
        debug!("Resolving conflict: {:?}", conflict.conflict_type);

        // Extract operation IDs from the conflicting key
        let parts: Vec<&str> = conflict.conflicting_key.split(':').collect();
        if parts.len() == 2 {
            let op1_id = parts[0];
            let op2_id = parts[1];

            // Add dependency to ensure proper ordering
            if let Some(op2) = self.operations.iter_mut().find(|op| op.id == op2_id) {
                if !op2.depends_on.contains(&op1_id.to_string()) {
                    op2.depends_on.push(op1_id.to_string());
                    debug!("Added dependency: {} -> {}", op1_id, op2_id);
                }
            }
        }

        Ok(())
    }

    /// Determine the execution order based on dependencies and priorities
    fn determine_execution_order(&self) -> Result<Vec<String>> {
        let mut order = Vec::new();
        let mut processed = std::collections::HashSet::new();
        let mut in_progress = std::collections::HashSet::new();

        // Topological sort with priority consideration
        for operation in &self.operations {
            if !processed.contains(&operation.id) {
                self.visit_operation(&operation.id, &mut order, &mut processed, &mut in_progress)?;
            }
        }

        // Sort by priority within dependency groups
        self.sort_by_priority(&mut order);

        debug!("Determined execution order: {:?}", order);
        Ok(order)
    }

    /// Visit an operation during topological sort
    fn visit_operation(
        &self,
        operation_id: &str,
        order: &mut Vec<String>,
        processed: &mut std::collections::HashSet<String>,
        in_progress: &mut std::collections::HashSet<String>,
    ) -> Result<()> {
        if in_progress.contains(operation_id) {
            return Err(anyhow!("Circular dependency detected involving '{}'", operation_id));
        }

        if processed.contains(operation_id) {
            return Ok(());
        }

        in_progress.insert(operation_id.to_string());

        if let Some(operation) = self.operations.iter().find(|op| op.id == operation_id) {
            // Visit all dependencies first
            for dep_id in &operation.depends_on {
                self.visit_operation(dep_id, order, processed, in_progress)?;
            }
        }

        in_progress.remove(operation_id);
        processed.insert(operation_id.to_string());
        order.push(operation_id.to_string());

        Ok(())
    }

    /// Sort operations by priority within dependency constraints
    fn sort_by_priority(&self, order: &mut Vec<String>) {
        // This is a simplified implementation
        // In practice, would need more sophisticated priority handling
        order.sort_by(|a, b| {
            let priority_a = self.operations.iter()
                .find(|op| op.id == *a)
                .map(|op| op.priority)
                .unwrap_or(0);
            let priority_b = self.operations.iter()
                .find(|op| op.id == *b)
                .map(|op| op.priority)
                .unwrap_or(0);

            priority_b.cmp(&priority_a) // Higher priority first
        });
    }

    /// Execute a single operation
    fn execute_single_operation(&mut self, operation: &BatchedQuery) -> Result<QueryResult> {
        trace!("Executing operation '{}': {}", operation.id, operation.sql);

        // Add to transaction context
        self.context.add_operation(operation.sql.clone(), operation.parameters.clone())?;

        // Convert operation type to expected result
        match operation.query_type {
            BatchQueryType::Select => Ok(QueryResult::Select(SelectResult::empty())),
            BatchQueryType::Insert => Ok(QueryResult::Insert(InsertResult {
                inserted_count: 1,
                inserted_ids: Vec::new(),
                stats: ExecutionStats::default(),
            })),
            BatchQueryType::Update => Ok(QueryResult::Update(UpdateResult {
                updated_count: 1,
                success: true,
                stats: ExecutionStats::default(),
            })),
            BatchQueryType::Delete => Ok(QueryResult::Delete(DeleteResult {
                deleted_count: 1,
                success: true,
                stats: ExecutionStats::default(),
            })),
            _ => Ok(QueryResult::Ddl(DdlResult {
                success: true,
                message: Some(format!("Executed {}", operation.id)),
                stats: ExecutionStats::default(),
            })),
        }
    }
}

/// Result of batch execution
pub struct BatchExecutionResult {
    /// Results for each successfully executed operation
    pub results: HashMap<String, QueryResult>,

    /// Errors that occurred during execution
    pub errors: Vec<(String, anyhow::Error)>,

    /// Execution statistics
    pub stats: BatchStats,

    /// Conflicts that were detected and resolved
    pub conflicts_detected: Vec<ConflictInfo>,

    /// Whether the entire batch executed successfully
    pub execution_successful: bool,
}

impl BatchExecutionResult {
    /// Get the result for a specific operation
    pub fn get_result(&self, operation_id: &str) -> Option<&QueryResult> {
        self.results.get(operation_id)
    }

    /// Get any error for a specific operation
    pub fn get_error(&self, operation_id: &str) -> Option<&anyhow::Error> {
        self.errors.iter()
            .find(|(id, _)| id == operation_id)
            .map(|(_, err)| err)
    }

    /// Check if a specific operation was successful
    pub fn is_operation_successful(&self, operation_id: &str) -> bool {
        self.results.contains_key(operation_id) && !self.errors.iter().any(|(id, _)| id == operation_id)
    }

    /// Get the total number of operations that were attempted
    pub fn total_operations(&self) -> usize {
        self.results.len() + self.errors.len()
    }

    /// Get the success rate as a percentage
    pub fn success_rate(&self) -> f64 {
        if self.total_operations() == 0 {
            return 100.0;
        }

        (self.results.len() as f64 / self.total_operations() as f64) * 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use zcore_storage::Store;
    use zcore_catalog::Catalog;

    #[test]
    fn test_batch_query_builder_creation() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("test.fjall")).unwrap();
        let catalog = Catalog::new(&store).unwrap();

        let builder = BatchQueryBuilder::new(&store, &catalog, 1);
        assert_eq!(builder.operations.len(), 0);
        assert_eq!(builder.stats.total_operations, 0);
    }

    #[test]
    fn test_batch_configuration() {
        let config = BatchExecutionConfig {
            max_batch_size: 100,
            continue_on_error: true,
            validate_before_execution: false,
            batch_timeout_ms: 5000,
            detailed_logging: true,
        };

        assert_eq!(config.max_batch_size, 100);
        assert!(config.continue_on_error);
        assert!(!config.validate_before_execution);
    }

    #[test]
    fn test_query_validation() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("test.fjall")).unwrap();
        let catalog = Catalog::new(&store).unwrap();

        let builder = BatchQueryBuilder::new(&store, &catalog, 1);

        let valid_query = BatchedQuery {
            id: "test1".to_string(),
            sql: "SELECT * FROM users".to_string(),
            parameters: Vec::new(),
            query_type: BatchQueryType::Select,
            depends_on: Vec::new(),
            priority: 0,
        };

        assert!(builder.validate_single_query(&valid_query).is_ok());

        let invalid_query = BatchedQuery {
            id: "test2".to_string(),
            sql: "INSERT INTO users VALUES (1)".to_string(),
            parameters: Vec::new(),
            query_type: BatchQueryType::Select, // Mismatch!
            depends_on: Vec::new(),
            priority: 0,
        };

        assert!(builder.validate_single_query(&invalid_query).is_err());
    }

    #[test]
    fn test_conflict_detection() {
        let update_query1 = BatchedQuery {
            id: "update1".to_string(),
            sql: "UPDATE users SET name = 'test'".to_string(),
            parameters: Vec::new(),
            query_type: BatchQueryType::Update,
            depends_on: Vec::new(),
            priority: 0,
        };

        let update_query2 = BatchedQuery {
            id: "update2".to_string(),
            sql: "UPDATE users SET email = 'test@test.com'".to_string(),
            parameters: Vec::new(),
            query_type: BatchQueryType::Update,
            depends_on: Vec::new(),
            priority: 0,
        };

        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("test.fjall")).unwrap();
        let catalog = Catalog::new(&store).unwrap();

        let builder = BatchQueryBuilder::new(&store, &catalog, 1);

        let conflict = builder.detect_operation_conflict(&update_query1, &update_query2).unwrap();
        assert!(conflict.is_some());
        assert_eq!(conflict.unwrap().conflict_type, ConflictType::WriteWrite);
    }

    #[test]
    fn test_execution_order_determination() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("test.fjall")).unwrap();
        let catalog = Catalog::new(&store).unwrap();

        let mut builder = BatchQueryBuilder::new(&store, &catalog, 1);

        // Add queries with dependencies
        let query1 = BatchedQuery {
            id: "query1".to_string(),
            sql: "SELECT 1".to_string(),
            parameters: Vec::new(),
            query_type: BatchQueryType::Select,
            depends_on: Vec::new(),
            priority: 1,
        };

        let query2 = BatchedQuery {
            id: "query2".to_string(),
            sql: "SELECT 2".to_string(),
            parameters: Vec::new(),
            query_type: BatchQueryType::Select,
            depends_on: vec!["query1".to_string()],
            priority: 2,
        };

        builder.operations = vec![query2, query1]; // Add in reverse order

        let order = builder.determine_execution_order().unwrap();
        assert_eq!(order, vec!["query1".to_string(), "query2".to_string()]);
    }

    #[test]
    fn test_circular_dependency_detection() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("test.fjall")).unwrap();
        let catalog = Catalog::new(&store).unwrap();

        let mut builder = BatchQueryBuilder::new(&store, &catalog, 1);

        let query1 = BatchedQuery {
            id: "query1".to_string(),
            sql: "SELECT 1".to_string(),
            parameters: Vec::new(),
            query_type: BatchQueryType::Select,
            depends_on: vec!["query2".to_string()],
            priority: 0,
        };

        let query2 = BatchedQuery {
            id: "query2".to_string(),
            sql: "SELECT 2".to_string(),
            parameters: Vec::new(),
            query_type: BatchQueryType::Select,
            depends_on: vec!["query1".to_string()],
            priority: 0,
        };

        builder.operations = vec![query1, query2];

        let result = builder.determine_execution_order();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Circular dependency"));
    }
}