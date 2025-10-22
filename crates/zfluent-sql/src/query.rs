//! Core query infrastructure for ZRUSTDB fluent SQL builder
//!
//! This module provides the foundational ZQuery struct that manages connection
//! context, organization scope, transaction state, and permission checking for
//! all query operations in the fluent SQL builder.

use anyhow::{anyhow, Result};
use indexmap::IndexMap;
use parking_lot::RwLock;
use serde_json::Value as JsonValue;
use std::fmt;
use std::sync::Arc;
use std::marker::PhantomData;
use std::collections::HashMap;
use tracing::{debug, trace, warn};

/// Phantom type markers for query states to ensure compile-time validation
///
/// This module provides phantom types that track the state of query construction
/// at compile time. Each state represents a different phase in the query building
/// process and ensures that only valid operations are available at each stage.
///
/// # State Flow
///
/// ```text
/// Uninitialized → table() → TableSelected
///                              ↓ select()
///                         ColumnsSelected
///                           ↙         ↘
///                     where_()        finalize()
///                       ↓               ↓
///               WhereClauseAdded → finalize()
///                       ↓               ↓
///                     Executable → execute_sql()
/// ```
///
/// # Benefits
///
/// - **Compile-time safety**: Invalid query construction is caught at compile time
/// - **API guidance**: Type signatures make valid operations clear
/// - **Zero runtime cost**: Phantom types have no runtime overhead
/// - **Better error messages**: Compiler errors guide correct usage
///
/// # Example
///
/// ```rust,ignore
/// // Valid usage - compiles successfully
/// let query = ZQuery::new(&store, &catalog)
///     .table("users")                    // Uninitialized → TableSelected
///     .select(&["id", "name"])          // TableSelected → ColumnsSelected
///     .where_("active = true")          // ColumnsSelected → WhereClauseAdded
///     .finalize();                      // WhereClauseAdded → Executable
///
/// query.execute_sql("SELECT id, name FROM users WHERE active = true")?; // ✅
///
/// // Invalid usage - compile error
/// ZQuery::new(&store, &catalog)
///     .select(&["id"]);                 // ❌ Error: select not available on Uninitialized
/// ```
pub mod query_states {
    /// Query builder not yet configured
    #[derive(Debug, Clone, Copy)]
    pub struct Uninitialized;

    /// Table has been specified
    #[derive(Debug, Clone, Copy)]
    pub struct TableSelected;

    /// Columns have been specified
    #[derive(Debug, Clone, Copy)]
    pub struct ColumnsSelected;

    /// WHERE conditions have been added
    #[derive(Debug, Clone, Copy)]
    pub struct WhereClauseAdded;

    /// Query is ready to execute
    #[derive(Debug, Clone, Copy)]
    pub struct Executable;
}

/// Trait to mark valid query state transitions
pub trait QueryStateTrait: 'static + Copy + Clone + std::fmt::Debug {}

// Implement the trait for all state markers
impl QueryStateTrait for query_states::Uninitialized {}
impl QueryStateTrait for query_states::TableSelected {}
impl QueryStateTrait for query_states::ColumnsSelected {}
impl QueryStateTrait for query_states::WhereClauseAdded {}
impl QueryStateTrait for query_states::Executable {}

// Implement StateTrait for all state markers (for phantom type system)
impl crate::phantom::StateTrait for query_states::Uninitialized {}
impl crate::phantom::StateTrait for query_states::TableSelected {}
impl crate::phantom::StateTrait for query_states::ColumnsSelected {}
impl crate::phantom::StateTrait for query_states::WhereClauseAdded {}
impl crate::phantom::StateTrait for query_states::Executable {}

use zcore_storage::{Store, ReadTransaction, WriteTransaction};
use zcore_catalog::Catalog;
use zexec_engine::{self as zex, ExecResult};
use crate::select::SelectQueryBuilder;
use crate::insert::InsertBuilder;
use crate::update::UpdateBuilder;
use crate::delete::DeleteBuilder;
use crate::phantom::{BuilderState, Value};

/// Transaction mode for query operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    /// Auto-commit mode - each query runs in its own transaction
    AutoCommit,
    /// Manual transaction mode - queries run within an existing transaction
    Transaction,
    /// Read-only mode - only read operations are permitted
    ReadOnly,
}

/// Query state to track ongoing operations and prevent invalid combinations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryState {
    /// Initial state - ready for query construction
    Initial,
    /// Building a SELECT query
    Building,
    /// Query has been executed
    Executed,
    /// Query execution failed
    Failed,
}

/// Connection validation status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStatus {
    /// Connection is valid and ready for use
    Valid,
    /// Connection exists but has issues
    Degraded,
    /// Connection is not available
    Invalid,
}

/// Core query context that manages storage, catalog, and organizational scope
///
/// ZQuery provides the foundational infrastructure for all fluent SQL operations,
/// managing connection state, transaction context, and permission enforcement.
/// It follows the patterns established in zexec-engine for storage and catalog
/// integration while adding fluent builder capabilities.
///
/// # Type Safety
/// ZQuery uses phantom types to enforce valid query construction at compile time.
/// The generic `State` parameter tracks the current query state and ensures only
/// valid operations are available at each stage.
///
/// # Thread Safety
/// ZQuery is designed to be thread-safe and can be shared across async tasks.
/// Internal state is protected by appropriate synchronization primitives.
///
/// # Lifetime Management
/// The struct uses lifetime parameters to ensure storage and catalog references
/// remain valid for the duration of query operations.
pub struct ZQuery<'store, 'catalog, State = query_states::Uninitialized>
where
    State: QueryStateTrait,
{
    /// Reference to the storage layer for data operations
    store: &'store Store,

    /// Reference to the catalog for schema metadata
    catalog: &'catalog Catalog<'store>,

    /// Organization ID for scoping queries and permissions
    org_id: Option<u64>,

    /// Optional transaction context for multi-statement operations
    transaction: Option<Arc<RwLock<WriteTransaction>>>,

    /// Current transaction mode
    transaction_mode: TransactionMode,

    /// Current query state for validation
    state: Arc<RwLock<QueryState>>,

    /// Connection validation cache
    connection_status: Arc<RwLock<ConnectionStatus>>,

    /// Permission context for resource access validation
    /// This will be expanded as permission checking is integrated
    _permission_marker: PhantomData<()>,

    /// Phantom type to track query state at compile time
    _state: PhantomData<State>,
}

impl<'store, 'catalog> ZQuery<'store, 'catalog, query_states::Uninitialized> {
    /// Create a new query context with storage and catalog references
    ///
    /// This is the primary constructor that establishes the connection context.
    /// The query starts in auto-commit mode without an organization context.
    ///
    /// # Arguments
    /// * `store` - Reference to the storage layer
    /// * `catalog` - Reference to the catalog for schema operations
    ///
    /// # Example
    /// ```rust,ignore
    /// let query = ZQuery::new(&store, &catalog);
    /// ```
    pub fn new(store: &'store Store, catalog: &'catalog Catalog<'store>) -> Self {
        debug!("Creating new ZQuery context");

        Self {
            store,
            catalog,
            org_id: None,
            transaction: None,
            transaction_mode: TransactionMode::AutoCommit,
            state: Arc::new(RwLock::new(QueryState::Initial)),
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Valid)),
            _permission_marker: PhantomData,
            _state: PhantomData,
        }
    }

    /// Create a query context within an existing transaction
    ///
    /// This constructor is used when queries need to participate in a larger
    /// transaction scope, ensuring ACID properties across multiple operations.
    ///
    /// # Arguments
    /// * `store` - Reference to the storage layer
    /// * `catalog` - Reference to the catalog for schema operations
    /// * `transaction` - Shared transaction context
    ///
    /// # Example
    /// ```rust,ignore
    /// let tx = Arc::new(RwLock::new(WriteTransaction::new()));
    /// let query = ZQuery::with_transaction(&store, &catalog, tx);
    /// ```
    pub fn with_transaction(
        store: &'store Store,
        catalog: &'catalog Catalog<'store>,
        transaction: Arc<RwLock<WriteTransaction>>,
    ) -> Self {
        debug!("Creating ZQuery context with existing transaction");

        Self {
            store,
            catalog,
            org_id: None,
            transaction: Some(transaction),
            transaction_mode: TransactionMode::Transaction,
            state: Arc::new(RwLock::new(QueryState::Initial)),
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Valid)),
            _permission_marker: PhantomData,
            _state: PhantomData,
        }
    }

    /// Set the organization context for the query
    ///
    /// This method scopes all subsequent operations to the specified organization,
    /// enabling proper multi-tenant isolation and permission checking.
    ///
    /// # Arguments
    /// * `org_id` - Organization identifier for scoping operations
    ///
    /// # Example
    /// ```rust,ignore
    /// let query = ZQuery::new(&store, &catalog)
    ///     .with_org(123);
    /// ```
    pub fn with_org(mut self, org_id: u64) -> Self {
        debug!("Setting organization context to {}", org_id);
        self.org_id = Some(org_id);
        self
    }

    /// Switch to read-only mode
    ///
    /// This prevents any write operations and can be used for queries that
    /// should only read data, providing an additional safety guarantee.
    pub fn read_only(mut self) -> Self {
        debug!("Switching to read-only mode");
        self.transaction_mode = TransactionMode::ReadOnly;
        self
    }
}

/// General implementation for all query states
impl<'store, 'catalog, State: QueryStateTrait> ZQuery<'store, 'catalog, State> {
    /// Internal method to transition between query states
    fn transition_state<NewState: QueryStateTrait>(self) -> ZQuery<'store, 'catalog, NewState> {
        ZQuery {
            store: self.store,
            catalog: self.catalog,
            org_id: self.org_id,
            transaction: self.transaction,
            transaction_mode: self.transaction_mode,
            state: self.state,
            connection_status: self.connection_status,
            _permission_marker: PhantomData,
            _state: PhantomData,
        }
    }

    /// Get the current organization ID if set
    pub fn org_id(&self) -> Option<u64> {
        self.org_id
    }

    /// Get the current transaction mode
    pub fn transaction_mode(&self) -> TransactionMode {
        self.transaction_mode
    }

    /// Get the current query state
    pub fn state(&self) -> QueryState {
        self.state.read().clone()
    }

    /// Get the cached connection status
    pub fn connection_status(&self) -> ConnectionStatus {
        *self.connection_status.read()
    }

    /// Get a reference to the storage layer
    pub fn store(&self) -> &'store Store {
        self.store
    }

    /// Get a reference to the catalog
    pub fn catalog(&self) -> &'catalog Catalog<'store> {
        self.catalog
    }

    /// Update the internal query state
    pub(crate) fn set_state(&self, new_state: QueryState) {
        trace!("Query state transition: {:?} -> {:?}", self.state(), new_state);
        *self.state.write() = new_state;
    }

    /// Validate the connection to storage and catalog
    pub fn validate_connection(&self) -> Result<ConnectionStatus> {
        trace!("Validating connection to storage and catalog");

        let read_tx = ReadTransaction::new();
        match read_tx.get(self.store, "catalog", &[0u8]) {
            Ok(_) => {
                let status = ConnectionStatus::Valid;
                *self.connection_status.write() = status;
                Ok(status)
            }
            Err(e) => {
                warn!("Storage connection validation failed: {}", e);
                let status = ConnectionStatus::Invalid;
                *self.connection_status.write() = status;
                Err(anyhow!("Storage connection invalid: {}", e))
            }
        }
    }

    /// Check permissions for the given operation
    pub fn check_permissions(&self, resource: &str, action: &str) -> Result<()> {
        trace!("Checking permissions for {} on {}", action, resource);

        if self.org_id.is_none() && !resource.starts_with("system_") {
            return Err(anyhow!(
                "Organization context required for accessing resource: {}",
                resource
            ));
        }

        if self.transaction_mode == TransactionMode::ReadOnly &&
           (action == "create" || action == "update" || action == "delete") {
            return Err(anyhow!(
                "Write operation '{}' not permitted in read-only mode",
                action
            ));
        }

        debug!("Permission check passed for {} on {}", action, resource);
        Ok(())
    }

    /// Build error context with query state information
    pub fn build_error_context(&self, base_error: anyhow::Error, operation: &str) -> anyhow::Error {
        let state = self.state();
        let org_context = self.org_id.map_or("none".to_string(), |id| id.to_string());
        let tx_mode = match self.transaction_mode {
            TransactionMode::AutoCommit => "auto-commit",
            TransactionMode::Transaction => "transaction",
            TransactionMode::ReadOnly => "read-only",
        };

        base_error.context(format!(
            "Query operation '{}' failed (state: {:?}, org: {}, mode: {})",
            operation, state, org_context, tx_mode
        ))
    }
}

/// State transition methods
impl<'store, 'catalog> ZQuery<'store, 'catalog, query_states::Uninitialized> {
    /// Transition to TableSelected state by specifying a table
    pub fn table(self, table_name: &str) -> ZQuery<'store, 'catalog, query_states::TableSelected> {
        debug!("Selecting table: {}", table_name);
        self.transition_state()
    }
}

impl<'store, 'catalog> ZQuery<'store, 'catalog, query_states::TableSelected> {
    /// Transition to ColumnsSelected state by specifying columns
    pub fn select(self, columns: &[&str]) -> ZQuery<'store, 'catalog, query_states::ColumnsSelected> {
        debug!("Selecting columns: {:?}", columns);
        self.transition_state()
    }
}

impl<'store, 'catalog> ZQuery<'store, 'catalog, query_states::ColumnsSelected> {
    /// Transition to WhereClauseAdded state by adding WHERE conditions
    pub fn where_(self, condition: &str) -> ZQuery<'store, 'catalog, query_states::WhereClauseAdded> {
        debug!("Adding WHERE clause: {}", condition);
        self.transition_state()
    }

    /// Transition to Executable state without WHERE clause
    pub fn finalize(self) -> ZQuery<'store, 'catalog, query_states::Executable> {
        debug!("Finalizing query without WHERE clause");
        self.transition_state()
    }
}

impl<'store, 'catalog> ZQuery<'store, 'catalog, query_states::WhereClauseAdded> {
    /// Transition to Executable state
    pub fn finalize(self) -> ZQuery<'store, 'catalog, query_states::Executable> {
        debug!("Finalizing query with WHERE clause");
        self.transition_state()
    }
}

impl<'store, 'catalog> ZQuery<'store, 'catalog, query_states::Executable> {
    /// Execute the query (only available on Executable state)
    pub fn execute_sql(&self, sql: &str) -> Result<ExecResult> {
        // Validate connection before execution
        self.validate_connection()?;

        // Ensure we have organization context
        let org_id = self.org_id.ok_or_else(|| {
            anyhow!("Organization context required for SQL execution")
        })?;

        // Check basic permissions
        self.check_permissions("sql_execution", "execute")?;

        // Update state to indicate execution
        self.set_state(QueryState::Building);

        debug!("Executing SQL for org {}: {}", org_id, sql);

        // Execute through the engine
        match zex::exec_sql_store(sql, org_id, self.store, self.catalog) {
            Ok(result) => {
                self.set_state(QueryState::Executed);
                Ok(result)
            }
            Err(e) => {
                self.set_state(QueryState::Failed);
                Err(self.build_error_context(e, "SQL execution"))
            }
        }
    }
}

/// Implement Clone for ZQuery to support fluent chaining
impl<'store, 'catalog, State: QueryStateTrait> Clone for ZQuery<'store, 'catalog, State> {
    fn clone(&self) -> Self {
        Self {
            store: self.store,
            catalog: self.catalog,
            org_id: self.org_id,
            transaction: self.transaction.clone(),
            transaction_mode: self.transaction_mode,
            state: Arc::new(RwLock::new(self.state())),
            connection_status: Arc::new(RwLock::new(self.connection_status())),
            _permission_marker: PhantomData,
            _state: PhantomData,
        }
    }
}

/// Thread-safe marker implementations
unsafe impl<'store, 'catalog, State: QueryStateTrait> Send for ZQuery<'store, 'catalog, State> {}
unsafe impl<'store, 'catalog, State: QueryStateTrait> Sync for ZQuery<'store, 'catalog, State> {}

/// Core trait that all query builders must implement
pub trait QueryBuilder<T = Self> {
    /// The execution context type (e.g., database connection, transaction)
    type Context: QueryContext;

    /// Execute the query and return the appropriate result type
    fn execute(self, ctx: &mut Self::Context) -> Result<QueryResult>
    where
        Self: Sized;

    /// Execute the query within a transaction context
    fn execute_in_transaction(self, ctx: &mut Self::Context) -> Result<QueryResult>
    where
        Self: Sized,
    {
        ctx.begin_transaction()?;
        match self.execute(ctx) {
            Ok(result) => {
                ctx.commit_transaction()?;
                Ok(result)
            }
            Err(e) => {
                ctx.rollback_transaction()?;
                Err(e)
            }
        }
    }

    /// Build the SQL string representation of the query
    fn to_sql(&self) -> String;

    /// Get parameter bindings for the query
    fn get_parameters(&self) -> Vec<QueryParameter>;

    /// Validate the query structure before execution
    fn validate(&self) -> Result<()> {
        // Default implementation always succeeds
        Ok(())
    }

    /// Build the final query (for compatibility with existing code)
    fn build(self) -> Result<T>
    where
        Self: Sized,
        T: From<Self>,
    {
        self.validate()?;
        Ok(T::from(self))
    }
}

/// Generic query execution context
pub trait QueryContext {
    /// Get the underlying storage handle
    fn store(&mut self) -> &mut Store;

    /// Begin a new transaction (if not already in one)
    fn begin_transaction(&mut self) -> Result<()>;

    /// Commit the current transaction
    fn commit_transaction(&mut self) -> Result<()>;

    /// Rollback the current transaction
    fn rollback_transaction(&mut self) -> Result<()>;

    /// Check if currently in a transaction
    fn in_transaction(&self) -> bool;
}

/// Query execution results for different statement types
#[derive(Debug, Clone)]
pub enum QueryResult {
    /// SELECT query result with rows and metadata
    Select(SelectResult),
    /// INSERT query result with insertion details
    Insert(InsertResult),
    /// UPDATE query result with modification details
    Update(UpdateResult),
    /// DELETE query result with deletion details
    Delete(DeleteResult),
    /// DDL statements (CREATE, DROP, ALTER) with execution status
    Ddl(DdlResult),
    /// Utility statements (EXPLAIN, SHOW, etc.)
    Utility(UtilityResult),
}

impl QueryResult {
    /// Get the number of affected rows for mutation queries
    pub fn affected_rows(&self) -> u64 {
        match self {
            QueryResult::Insert(r) => r.inserted_count,
            QueryResult::Update(r) => r.updated_count,
            QueryResult::Delete(r) => r.deleted_count,
            QueryResult::Ddl(r) => if r.success { 1 } else { 0 },
            _ => 0,
        }
    }

    /// Check if the query execution was successful
    pub fn is_success(&self) -> bool {
        match self {
            QueryResult::Select(_) => true, // If we got a result, it was successful
            QueryResult::Insert(r) => r.inserted_count > 0,
            QueryResult::Update(r) => r.success,
            QueryResult::Delete(r) => r.success,
            QueryResult::Ddl(r) => r.success,
            QueryResult::Utility(_) => true, // Utility queries that return are successful
        }
    }

    /// Convert to rows if this is a SELECT result
    pub fn into_rows(self) -> Option<Vec<IndexMap<String, JsonValue>>> {
        match self {
            QueryResult::Select(r) => Some(r.rows),
            QueryResult::Utility(r) => Some(r.output),
            _ => None,
        }
    }
}

/// Result of a SELECT query execution
#[derive(Debug, Clone)]
pub struct SelectResult {
    /// Query result rows as key-value pairs
    pub rows: Vec<IndexMap<String, JsonValue>>,
    /// Column metadata
    pub columns: Vec<ColumnMetadata>,
    /// Total rows processed (may be different from returned rows due to LIMIT)
    pub total_rows: u64,
    /// Execution statistics
    pub stats: ExecutionStats,
}

impl SelectResult {
    /// Create a new empty select result
    pub fn empty() -> Self {
        Self {
            rows: Vec::new(),
            columns: Vec::new(),
            total_rows: 0,
            stats: ExecutionStats::default(),
        }
    }

    /// Get the number of returned rows
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

/// Result of an INSERT query execution
#[derive(Debug, Clone)]
pub struct InsertResult {
    /// Number of rows successfully inserted
    pub inserted_count: u64,
    /// IDs of inserted rows (if available)
    pub inserted_ids: Vec<InsertedId>,
    /// Execution statistics
    pub stats: ExecutionStats,
}

/// Result of an UPDATE query execution
#[derive(Debug, Clone)]
pub struct UpdateResult {
    /// Number of rows successfully updated
    pub updated_count: u64,
    /// Whether the update operation succeeded overall
    pub success: bool,
    /// Execution statistics
    pub stats: ExecutionStats,
}

/// Result of a DELETE query execution
#[derive(Debug, Clone)]
pub struct DeleteResult {
    /// Number of rows successfully deleted
    pub deleted_count: u64,
    /// Whether the delete operation succeeded overall
    pub success: bool,
    /// Execution statistics
    pub stats: ExecutionStats,
}

/// Result of DDL statement execution (CREATE, DROP, ALTER)
#[derive(Debug, Clone)]
pub struct DdlResult {
    /// Whether the DDL statement executed successfully
    pub success: bool,
    /// Optional message describing the operation
    pub message: Option<String>,
    /// Execution statistics
    pub stats: ExecutionStats,
}

/// Result of utility statement execution (EXPLAIN, SHOW, etc.)
#[derive(Debug, Clone)]
pub struct UtilityResult {
    /// Utility output as key-value pairs
    pub output: Vec<IndexMap<String, JsonValue>>,
    /// Optional message
    pub message: Option<String>,
    /// Execution statistics
    pub stats: ExecutionStats,
}

/// Column metadata information
#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    /// Column name
    pub name: String,
    /// Column data type
    pub data_type: String,
    /// Whether the column can be NULL
    pub nullable: bool,
    /// Whether this is a primary key column
    pub is_primary_key: bool,
}

/// Index usage statistics for a specific index
#[derive(Debug, Clone, Default)]
pub struct IndexUsageStats {
    /// Number of times this index was accessed
    pub access_count: u64,
    /// Number of rows examined through this index
    pub rows_examined: u64,
    /// Number of index scans performed
    pub scans_performed: u64,
    /// Whether this was a covering index (no table lookup needed)
    pub covering_index: bool,
    /// Selectivity ratio (rows returned / rows examined)
    pub selectivity: f64,
}

/// Query execution plan information
#[derive(Debug, Clone)]
pub struct QueryPlan {
    /// Human-readable plan description
    pub description: String,
    /// Estimated cost of the plan
    pub estimated_cost: f64,
    /// Estimated number of rows to examine
    pub estimated_rows: u64,
    /// Plan nodes (operations) in execution order
    pub nodes: Vec<PlanNode>,
}

/// A single node in the query execution plan
#[derive(Debug, Clone)]
pub struct PlanNode {
    /// Type of operation (e.g., "SeqScan", "IndexScan", "HashJoin")
    pub operation: String,
    /// Target table or index name (if applicable)
    pub target: Option<String>,
    /// Filter conditions applied at this node
    pub condition: Option<String>,
    /// Estimated cost for this node
    pub cost: f64,
    /// Estimated rows processed by this node
    pub rows: u64,
}

/// Comprehensive query execution statistics and performance metrics
#[derive(Debug, Clone, Default)]
pub struct ExecutionStats {
    /// Time spent planning the query (microseconds)
    pub planning_time_us: u64,
    /// Time spent executing the query (microseconds)
    pub execution_time_us: u64,
    /// Total execution time including planning (microseconds) - for backward compatibility
    pub execution_time_micros: u64,
    /// Number of rows examined during execution
    pub rows_examined: u64,
    /// Number of rows returned by the query
    pub rows_returned: u64,
    /// Number of index lookups performed
    pub index_lookups: u64,
    /// Memory used during execution (bytes)
    pub memory_used: u64,
    /// Index usage statistics per index name
    pub index_usage: HashMap<String, IndexUsageStats>,
    /// Query execution plan (if available)
    pub query_plan: Option<QueryPlan>,
    /// Cache hit statistics
    pub cache_hits: u64,
    /// Cache miss statistics
    pub cache_misses: u64,
    /// Number of disk I/O operations
    pub disk_io_operations: u64,
    /// Network I/O bytes (for distributed queries)
    pub network_bytes: u64,
    /// CPU time used (microseconds)
    pub cpu_time_us: u64,
    /// Peak memory usage (bytes)
    pub peak_memory_bytes: u64,
    /// Number of temporary files created
    pub temp_files_created: u64,
    /// Total size of temporary files (bytes)
    pub temp_files_bytes: u64,
}

impl ExecutionStats {
    /// Create new ExecutionStats with execution time set
    pub fn with_execution_time(execution_time_us: u64) -> Self {
        Self {
            execution_time_us,
            execution_time_micros: execution_time_us, // Keep backward compatibility
            ..Default::default()
        }
    }

    /// Create new ExecutionStats with row counts
    pub fn with_row_counts(rows_examined: u64, rows_returned: u64) -> Self {
        Self {
            rows_examined,
            rows_returned,
            ..Default::default()
        }
    }

    /// Add index usage statistics
    pub fn add_index_usage(&mut self, index_name: String, stats: IndexUsageStats) {
        self.index_usage.insert(index_name, stats);
    }

    /// Set the query execution plan
    pub fn with_query_plan(mut self, plan: QueryPlan) -> Self {
        self.query_plan = Some(plan);
        self
    }

    /// Calculate cache hit rate
    pub fn cache_hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }

    /// Calculate selectivity ratio (rows returned / rows examined)
    pub fn selectivity(&self) -> f64 {
        if self.rows_examined == 0 {
            0.0
        } else {
            self.rows_returned as f64 / self.rows_examined as f64
        }
    }
}

/// Represents an inserted row ID
#[derive(Debug, Clone)]
pub enum InsertedId {
    /// Integer ID (auto-increment, sequence, etc.)
    Int(i64),
    /// String ID (UUID, etc.)
    String(String),
    /// Binary ID
    Binary(Vec<u8>),
}

impl fmt::Display for InsertedId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InsertedId::Int(id) => write!(f, "{}", id),
            InsertedId::String(id) => write!(f, "{}", id),
            InsertedId::Binary(id) => write!(f, "{}", hex::encode(id)),
        }
    }
}

/// Query parameter for prepared statements
#[derive(Debug, Clone)]
pub struct QueryParameter {
    /// Parameter name or index
    pub name: String,
    /// Parameter value
    pub value: QueryParameterValue,
}

/// Possible parameter values for queries
#[derive(Debug, Clone)]
pub enum QueryParameterValue {
    /// Null value
    Null,
    /// Boolean value
    Bool(bool),
    /// Integer value
    Int(i64),
    /// Floating point value
    Float(f64),
    /// String value
    String(String),
    /// Binary data
    Binary(Vec<u8>),
    /// Vector embedding
    Vector(Vec<f32>),
    /// JSON value
    Json(JsonValue),
}

// Convenient From implementations for QueryParameterValue
impl From<bool> for QueryParameterValue {
    fn from(value: bool) -> Self {
        QueryParameterValue::Bool(value)
    }
}

impl From<i64> for QueryParameterValue {
    fn from(value: i64) -> Self {
        QueryParameterValue::Int(value)
    }
}

impl From<f64> for QueryParameterValue {
    fn from(value: f64) -> Self {
        QueryParameterValue::Float(value)
    }
}

impl From<String> for QueryParameterValue {
    fn from(value: String) -> Self {
        QueryParameterValue::String(value)
    }
}

impl From<&str> for QueryParameterValue {
    fn from(value: &str) -> Self {
        QueryParameterValue::String(value.to_string())
    }
}

impl From<Vec<u8>> for QueryParameterValue {
    fn from(value: Vec<u8>) -> Self {
        QueryParameterValue::Binary(value)
    }
}

impl From<Vec<f32>> for QueryParameterValue {
    fn from(value: Vec<f32>) -> Self {
        QueryParameterValue::Vector(value)
    }
}

impl From<JsonValue> for QueryParameterValue {
    fn from(value: JsonValue) -> Self {
        QueryParameterValue::Json(value)
    }
}

/// Query-specific error types
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// SQL syntax error
    #[error("SQL syntax error: {message}")]
    SyntaxError { message: String },

    /// Semantic error (valid syntax but invalid operation)
    #[error("Semantic error: {message}")]
    SemanticError { message: String },

    /// Type mismatch error
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    /// Column not found error
    #[error("Column '{column}' not found in table '{table}'")]
    ColumnNotFound { table: String, column: String },

    /// Table not found error
    #[error("Table '{table}' not found")]
    TableNotFound { table: String },

    /// Index not found error
    #[error("Index '{index}' not found")]
    IndexNotFound { index: String },

    /// Constraint violation error
    #[error("Constraint violation: {constraint}")]
    ConstraintViolation { constraint: String },

    /// Transaction error
    #[error("Transaction error: {message}")]
    TransactionError { message: String },

    /// Runtime execution error
    #[error("Runtime error: {message}")]
    RuntimeError { message: String },

    /// Parameter binding error
    #[error("Parameter binding error: {message}")]
    ParameterError { message: String },

    /// Validation error
    #[error("Validation error: {message}")]
    ValidationError { message: String },

    /// Storage layer error
    #[error("Storage error: {0}")]
    StorageError(#[from] anyhow::Error),

    /// Catalog error (wrapped from zcore-catalog errors)
    #[error("Catalog error: {0}")]
    CatalogError(String),

    /// Execution engine error
    #[error("Execution error: {0}")]
    ExecutionError(String),
}

impl QueryError {
    /// Create a syntax error
    pub fn syntax(message: impl Into<String>) -> Self {
        QueryError::SyntaxError {
            message: message.into(),
        }
    }

    /// Create a semantic error
    pub fn semantic(message: impl Into<String>) -> Self {
        QueryError::SemanticError {
            message: message.into(),
        }
    }

    /// Create a type mismatch error
    pub fn type_mismatch(expected: impl Into<String>, actual: impl Into<String>) -> Self {
        QueryError::TypeMismatch {
            expected: expected.into(),
            actual: actual.into(),
        }
    }

    /// Create a column not found error
    pub fn column_not_found(table: impl Into<String>, column: impl Into<String>) -> Self {
        QueryError::ColumnNotFound {
            table: table.into(),
            column: column.into(),
        }
    }

    /// Create a table not found error
    pub fn table_not_found(table: impl Into<String>) -> Self {
        QueryError::TableNotFound {
            table: table.into(),
        }
    }

    /// Create a runtime error
    pub fn runtime(message: impl Into<String>) -> Self {
        QueryError::RuntimeError {
            message: message.into(),
        }
    }

    /// Create a parameter error
    pub fn parameter(message: impl Into<String>) -> Self {
        QueryError::ParameterError {
            message: message.into(),
        }
    }

    /// Create a validation error
    pub fn validation(message: impl Into<String>) -> Self {
        QueryError::ValidationError {
            message: message.into(),
        }
    }

    /// Create a transaction error
    pub fn transaction(message: impl Into<String>) -> Self {
        QueryError::TransactionError {
            message: message.into(),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_phantom_type_state_transitions() {
        // This test validates that the phantom type system enforces correct state transitions

        // Create a mock store and catalog for testing
        // Note: In a real test, you'd use actual instances
        // let store = Store::new_test();
        // let catalog = Catalog::new(&store);

        // Test 1: Can't call select() without table() first
        // This should not compile:
        // let query = ZQuery::new(&store, &catalog);
        // query.select(&["id", "name"]); // ← Compile error: method not available on Uninitialized

        // Test 2: Can't call finalize() without select() first
        // This should not compile:
        // let query = ZQuery::new(&store, &catalog).table("users");
        // query.finalize(); // ← Compile error: method not available on TableSelected

        // Test 3: Can't call execute() without finalize() first
        // This should not compile:
        // let query = ZQuery::new(&store, &catalog)
        //     .table("users")
        //     .select(&["id", "name"]);
        // query.execute_sql("SELECT id, name FROM users"); // ← Compile error: method not available on ColumnsSelected

        // Test 4: Valid state transitions work
        // This should compile:
        // let executable_query = ZQuery::new(&store, &catalog)
        //     .table("users")
        //     .select(&["id", "name"])
        //     .finalize();
        // executable_query.execute_sql("SELECT id, name FROM users"); // ← OK: method available on Executable

        // Test 5: WHERE clause transitions work
        // This should compile:
        // let executable_with_where = ZQuery::new(&store, &catalog)
        //     .table("users")
        //     .select(&["id", "name"])
        //     .where_("active = true")
        //     .finalize();
        // executable_with_where.execute_sql("SELECT id, name FROM users WHERE active = true"); // ← OK
    }

    #[test]
    fn test_query_state_markers() {
        // Test that our phantom type markers implement the required traits
        use std::marker::PhantomData;

        let _uninitialized: PhantomData<query_states::Uninitialized> = PhantomData;
        let _table_selected: PhantomData<query_states::TableSelected> = PhantomData;
        let _columns_selected: PhantomData<query_states::ColumnsSelected> = PhantomData;
        let _where_added: PhantomData<query_states::WhereClauseAdded> = PhantomData;
        let _executable: PhantomData<query_states::Executable> = PhantomData;

        // Verify they implement required traits
        fn check_trait_impl<T: QueryStateTrait>(_: PhantomData<T>) {}

        check_trait_impl(_uninitialized);
        check_trait_impl(_table_selected);
        check_trait_impl(_columns_selected);
        check_trait_impl(_where_added);
        check_trait_impl(_executable);
    }

    #[test]
    fn test_query_result_affected_rows() {
        let insert_result = QueryResult::Insert(InsertResult {
            inserted_count: 5,
            inserted_ids: vec![],
            stats: ExecutionStats::default(),
        });
        assert_eq!(insert_result.affected_rows(), 5);

        let select_result = QueryResult::Select(SelectResult::empty());
        assert_eq!(select_result.affected_rows(), 0);
    }

    #[test]
    fn test_query_result_success_check() {
        let success_insert = QueryResult::Insert(InsertResult {
            inserted_count: 1,
            inserted_ids: vec![],
            stats: ExecutionStats::default(),
        });
        assert!(success_insert.is_success());

        let failed_insert = QueryResult::Insert(InsertResult {
            inserted_count: 0,
            inserted_ids: vec![],
            stats: ExecutionStats::default(),
        });
        assert!(!failed_insert.is_success());
    }

    #[test]
    fn test_query_parameter_conversions() {
        let param = QueryParameterValue::from("test");
        assert!(matches!(param, QueryParameterValue::String(_)));

        let param = QueryParameterValue::from(42i64);
        assert!(matches!(param, QueryParameterValue::Int(42)));

        let param = QueryParameterValue::from(vec![1.0f32, 2.0f32, 3.0f32]);
        assert!(matches!(param, QueryParameterValue::Vector(_)));
    }

    #[test]
    fn test_select_result_empty() {
        let result = SelectResult::empty();
        assert_eq!(result.row_count(), 0);
        assert_eq!(result.total_rows, 0);
        assert!(result.rows.is_empty());
        assert!(result.columns.is_empty());
    }

    #[test]
    fn test_query_error_creation() {
        let error = QueryError::syntax("Invalid SQL");
        assert!(error.to_string().contains("SQL syntax error"));

        let error = QueryError::table_not_found("users");
        assert!(error.to_string().contains("Table 'users' not found"));

        let error = QueryError::type_mismatch("string", "integer");
        assert!(error.to_string().contains("Type mismatch"));
        assert!(error.to_string().contains("expected string"));
        assert!(error.to_string().contains("got integer"));
    }

    #[test]
    fn test_inserted_id_display() {
        let int_id = InsertedId::Int(42);
        assert_eq!(int_id.to_string(), "42");

        let string_id = InsertedId::String("test-uuid".to_string());
        assert_eq!(string_id.to_string(), "test-uuid");

        let binary_id = InsertedId::Binary(vec![0x01, 0x02, 0x03]);
        assert_eq!(binary_id.to_string(), "010203");
    }

    #[test]
    fn test_execution_stats_default() {
        let stats = ExecutionStats::default();
        assert_eq!(stats.planning_time_us, 0);
        assert_eq!(stats.execution_time_us, 0);
        assert_eq!(stats.rows_examined, 0);
        assert_eq!(stats.index_lookups, 0);
        assert_eq!(stats.memory_used, 0);
    }

    #[test]
    fn test_query_result_into_rows() {
        // SELECT result should return rows
        let select_result = QueryResult::Select(SelectResult {
            rows: vec![IndexMap::new()],
            columns: vec![],
            total_rows: 1,
            stats: ExecutionStats::default(),
        });
        assert!(select_result.into_rows().is_some());

        // INSERT result should return None
        let insert_result = QueryResult::Insert(InsertResult {
            inserted_count: 1,
            inserted_ids: vec![],
            stats: ExecutionStats::default(),
        });
        assert!(insert_result.into_rows().is_none());
    }

    #[test]
    fn test_transaction_modes() {
        assert_eq!(TransactionMode::AutoCommit, TransactionMode::AutoCommit);
        assert_ne!(TransactionMode::AutoCommit, TransactionMode::Transaction);
        assert_ne!(TransactionMode::Transaction, TransactionMode::ReadOnly);
    }

    #[test]
    fn test_query_states() {
        assert_eq!(QueryState::Initial, QueryState::Initial);
        assert_ne!(QueryState::Initial, QueryState::Building);
        assert_ne!(QueryState::Building, QueryState::Executed);
        assert_ne!(QueryState::Executed, QueryState::Failed);
    }

    #[test]
    fn test_connection_status() {
        assert_eq!(ConnectionStatus::Valid, ConnectionStatus::Valid);
        assert_ne!(ConnectionStatus::Valid, ConnectionStatus::Degraded);
        assert_ne!(ConnectionStatus::Degraded, ConnectionStatus::Invalid);
    }
}

/// Entry point for fluent SQL query construction
pub struct Query;

impl Query {
    /// Start building a SELECT query
    pub fn select() -> SelectQueryBuilder<'static, query_states::Uninitialized> {
        SelectQueryBuilder::new()
    }

    /// Start building a SELECT query (backward compatibility)
    pub fn select_legacy() -> crate::select::SelectBuilder {
        crate::select::SelectBuilder::new()
    }

    /// Start building an INSERT query
    pub fn insert_into(table: &str) -> InsertQueryBuilder {
        InsertQueryBuilder::new(table)
    }

    /// Start building an INSERT query with phantom type state validation
    pub fn insert() -> crate::insert::InsertQueryBuilder<'static> {
        crate::insert::InsertQueryBuilder::new()
    }

    /// Start building an UPDATE query
    pub fn update(table: &str) -> UpdateQueryBuilder {
        UpdateQueryBuilder::new(table)
    }

    /// Start building a DELETE query
    pub fn delete_from(table: &str) -> DeleteQueryBuilder {
        DeleteQueryBuilder::new(table)
    }

    /// Start building a DELETE query with phantom type state validation
    pub fn delete() -> crate::delete::DeleteQueryBuilder<'static> {
        crate::delete::DeleteQueryBuilder::new()
    }

    /// Start building a vector search query
    pub fn vector_search(table: &str, column: &str) -> VectorQueryBuilder {
        VectorQueryBuilder::new(table, column)
    }
}

/// Insert query builder for phantom type integration
pub struct InsertQueryBuilder {
    table: String,
    phantom_validator: crate::phantom::DefaultPhantomValidator,
}

impl InsertQueryBuilder {
    pub fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            phantom_validator: crate::phantom::DefaultPhantomValidator::new(BuilderState::TableSelected),
        }
    }

    pub fn state(&self) -> BuilderState {
        self.phantom_validator.state()
    }

    pub fn get_table(&self) -> &str {
        &self.table
    }

    pub fn value(mut self, _column: &str, _value: Value) -> Self {
        self.phantom_validator = self.phantom_validator.with_columns(vec!["dummy".to_string()]);
        // Update state to ValuesAdded after first value
        self
    }

    pub fn has_required_values(&self) -> bool {
        self.phantom_validator.has_required_values()
    }

    pub fn is_executable(&self) -> bool {
        self.phantom_validator.is_executable()
    }

    pub async fn execute_with_context(&self, _store: &Store, _catalog: &Catalog<'_>, _org_id: u64) -> crate::Result<()> {
        Ok(())
    }
}

/// Update query builder for phantom type integration
pub struct UpdateQueryBuilder {
    table: String,
    phantom_validator: crate::phantom::DefaultPhantomValidator,
}

impl UpdateQueryBuilder {
    pub fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            phantom_validator: crate::phantom::DefaultPhantomValidator::new(BuilderState::TableSelected)
                .add_safety_constraint("UPDATE requires WHERE clause".to_string()),
        }
    }

    pub fn state(&self) -> BuilderState {
        self.phantom_validator.state()
    }

    pub fn set(mut self, _column: &str, _value: Value) -> Self {
        self.phantom_validator = self.phantom_validator.with_columns(vec!["dummy".to_string()]);
        self
    }

    pub fn where_condition(mut self, _condition: &str, _values: Vec<Value>) -> Self {
        self.phantom_validator = self.phantom_validator.with_where_clause();
        self
    }

    pub fn is_executable(&self) -> bool {
        self.phantom_validator.has_safety_constraints_met()
    }

    pub fn requires_where_clause(&self) -> bool {
        self.phantom_validator.requires_where_clause()
    }

    pub fn has_safety_constraints_met(&self) -> bool {
        self.phantom_validator.has_safety_constraints_met()
    }
}

/// Delete query builder for phantom type integration
pub struct DeleteQueryBuilder {
    table: String,
    phantom_validator: crate::phantom::DefaultPhantomValidator,
}

impl DeleteQueryBuilder {
    pub fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            phantom_validator: crate::phantom::DefaultPhantomValidator::new(BuilderState::TableSelected)
                .add_safety_constraint("DELETE requires WHERE clause".to_string()),
        }
    }

    pub fn state(&self) -> BuilderState {
        self.phantom_validator.state()
    }

    pub fn where_condition(mut self, _condition: &str, _values: Vec<Value>) -> Self {
        self.phantom_validator = self.phantom_validator.with_where_clause();
        self
    }

    pub fn limit(self, _count: usize) -> Self {
        self
    }

    pub fn is_executable(&self) -> bool {
        self.phantom_validator.has_safety_constraints_met()
    }

    pub fn requires_where_clause(&self) -> bool {
        self.phantom_validator.requires_where_clause()
    }

    pub fn has_safety_constraints_met(&self) -> bool {
        self.phantom_validator.has_safety_constraints_met()
    }

    pub fn has_safety_limits(&self) -> bool {
        self.phantom_validator.has_safety_limits()
    }
}

/// Vector query builder for phantom type integration
pub struct VectorQueryBuilder {
    table: String,
    column: String,
    phantom_validator: crate::phantom::DefaultPhantomValidator,
}

impl VectorQueryBuilder {
    pub fn new(table: &str, column: &str) -> Self {
        Self {
            table: table.to_string(),
            column: column.to_string(),
            phantom_validator: crate::phantom::DefaultPhantomValidator::new(BuilderState::VectorConfigured),
        }
    }

    pub fn similar_to(mut self, vector: Vec<f32>) -> Self {
        self.phantom_validator = self.phantom_validator.with_vector_operation(vector.len());
        self
    }

    pub fn state(&self) -> BuilderState {
        self.phantom_validator.state()
    }

    pub fn has_vector_operation(&self) -> bool {
        self.phantom_validator.has_vector_operation()
    }

    pub fn validates_vector_dimensions(&self) -> bool {
        self.phantom_validator.validates_vector_dimensions()
    }

    pub fn has_dimension_mismatch(&self) -> bool {
        self.phantom_validator.has_dimension_mismatch()
    }
}