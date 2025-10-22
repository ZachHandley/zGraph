/*!
# zfluent-sql

A fluent, type-safe SQL query builder for ZRUSTDB with advanced transaction support.

This crate provides a fluent interface for constructing SQL queries with compile-time
type safety, runtime validation, and comprehensive transaction management. It integrates
deeply with the ZRUSTDB ecosystem, leveraging the storage layer, catalog metadata,
and execution engine with full ACID semantics and MVCC support.

## Features

- **Type-safe query construction** - Compile-time validation of column types and table schemas
- **Fluent API** - Chainable methods for intuitive query building
- **Transaction support** - Full ACID transactions with Fjall MVCC integration
- **Batch operations** - Execute multiple queries atomically with dependency resolution
- **Conflict detection** - Automatic detection and resolution of transaction conflicts
- **Error recovery** - Comprehensive error handling with automatic retry mechanisms
- **Vector operations** - First-class support for vector similarity queries
- **Integration** - Deep integration with ZRUSTDB storage, catalog, and execution layers
- **Performance** - Zero-cost abstractions and efficient query compilation

## Basic Usage

```rust,ignore
use zfluent_sql::Query;

// Basic SELECT query
let query = Query::select()
    .from("users")
    .where_col("age").gt(18)
    .and_where_col("active").eq(true)
    .build();

// Vector similarity query
let query = Query::select()
    .from("embeddings")
    .order_by_vector_distance("embedding", &target_vector)
    .limit(10)
    .build();

// INSERT with fluent syntax
let query = Query::insert_into("users")
    .values([
        ("name", "Alice"),
        ("email", "alice@example.com"),
    ])
    .build();
```

## Transaction Support

### Transaction-Aware Query Builders

```rust,ignore
use zfluent_sql::{Query, TransactionContext};

// Create transaction context
let mut ctx = TransactionContext::new(&store, &catalog, org_id);
ctx.begin_transaction()?;

// Execute queries within transaction
let select_query = Query::select()
    .from("users")
    .select(&["id", "name"])?
    .with_transaction("tx_123".to_string());

select_query.execute_in_transaction(&mut ctx)?;

// Commit all operations atomically
ctx.commit()?;
```

### Batch Operations

```rust,ignore
use zfluent_sql::{BatchQueryBuilder, BatchExecutionConfig};

// Create batch with custom configuration
let config = BatchExecutionConfig {
    max_batch_size: 100,
    continue_on_error: false,
    validate_before_execution: true,
    batch_timeout_ms: 30_000,
    detailed_logging: true,
};

let mut batch = BatchQueryBuilder::with_config(&store, &catalog, org_id, config);

// Add operations to batch
batch.add_select("get_users".to_string(), Query::select().from("users").select_all())?;
batch.add_insert("add_user".to_string(), Query::insert_into("users"))?;

// Execute all operations atomically
let result = batch.execute()?;
println!("Success rate: {:.1}%", result.success_rate());
```

### Error Handling and Recovery

```rust,ignore
use zfluent_sql::{TransactionErrorHandler, ErrorHandlingConfig, RecoveryStrategy};

// Configure error handling with retry strategies
let config = ErrorHandlingConfig {
    enable_auto_recovery: true,
    conflict_recovery_strategy: RecoveryStrategy::Retry {
        max_attempts: 5,
        initial_delay_ms: 50,
        max_delay_ms: 2000,
        backoff_multiplier: 1.5,
    },
    ..Default::default()
};

let handler = TransactionErrorHandler::with_config(config);

// Automatically handle errors with recovery
match handler.handle_error(error, &mut ctx, "operation_id") {
    Ok(Some(result)) => println!("Operation recovered successfully"),
    Err(e) => println!("Recovery failed: {}", e),
}
```

## Architecture

The crate is organized into several modules:

- [`query`] - Core query builder types and traits
- [`select`] - SELECT query construction
- [`insert`] - INSERT query construction
- [`update`] - UPDATE query construction
- [`delete`] - DELETE query construction
- [`vector`] - Vector-specific query operations
- [`transaction`] - Transaction context and MVCC integration
- [`batch`] - Batch operations with dependency resolution
- [`error_handling`] - Error recovery and retry mechanisms

*/

pub mod query;
pub mod select;
pub mod insert;
pub mod update;
pub mod delete;
pub mod vector;
pub mod where_clause;
pub mod enhanced_select;
pub mod transaction;
pub mod batch;
pub mod error_handling;
pub mod phantom;
pub mod prelude;
// pub mod execution;

#[cfg(test)]
mod tests;

// Re-export main public API
pub use query::{
    ZQuery, Query, QueryBuilder, QueryResult, QueryError, QueryContext,
    SelectResult, InsertResult, UpdateResult, DeleteResult,
    DdlResult, UtilityResult, QueryParameter, QueryParameterValue,
    InsertedId, ColumnMetadata, ExecutionStats,
    TransactionMode, QueryState, ConnectionStatus,
    query_states, QueryStateTrait,
};
pub use select::{SelectBuilder, SelectQueryBuilder};
pub use enhanced_select::EnhancedSelectBuilder;
pub use insert::{InsertBuilder, InsertQueryBuilder, ConflictStrategy, InsertValueChain};
pub use update::{UpdateBuilder, UpdateQueryBuilder};
pub use delete::{DeleteBuilder, DeleteQueryBuilder};
pub use vector::{VectorQuery, VectorDistance, VectorQueryBuilder, VectorQueryExt, VectorQueryResult, HybridQuery};
pub use where_clause::{
    WhereClauseBuilder, Condition, ConditionGroup, ComparisonOperator, LogicalOperator
};
pub use transaction::{
    TransactionContext, TransactionState, IsolationLevel, ConflictInfo, ConflictType,
    TransactionQueryBuilder, WithTransaction, TransactionStats
};
pub use batch::{
    BatchQueryBuilder, BatchExecutionConfig, BatchedQuery, BatchQueryType,
    BatchExecutionResult, BatchStats
};
pub use error_handling::{
    TransactionErrorHandler, ErrorHandlingConfig, RecoveryStrategy, TransactionError,
    ErrorCategory, ErrorStats
};
pub use phantom::{
    BuilderState, PhantomQueryBuilder, ValidationError, ValidationResult,
    StateTrait, StateValidator, PhantomValidation, CompileTimeValidator,
    ColumnType, Value, OrderDirection, JoinType
};
// pub use execution::ExecutionLayer;

/// Result type alias for query builder operations
pub type Result<T> = anyhow::Result<T>;

/// Error types specific to fluent SQL operations
#[derive(Debug, thiserror::Error)]
pub enum FluentSqlError {
    #[error("Invalid column reference: {0}")]
    InvalidColumn(String),

    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("Missing required clause: {0}")]
    MissingClause(String),

    #[error("Invalid vector dimension: expected {expected}, got {actual}")]
    InvalidVectorDimension { expected: usize, actual: usize },

    #[error("Catalog error: {0}")]
    Catalog(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("Execution error: {0}")]
    Execution(#[from] anyhow::Error),
}