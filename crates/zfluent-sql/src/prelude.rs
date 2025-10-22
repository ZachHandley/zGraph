/*!
FluentSQL Prelude - Phantom Type System

This module provides a convenient prelude for importing commonly used types
and traits for the FluentSQL phantom type system, which provides compile-time
query validation and type safety.

## Usage

```rust
use zfluent_sql::prelude::*;

// Phantom types are now available
let query = Query::select()
    .from("users")
    .select(&["id", "name"])
    .expect("Valid column selection")
    .where_condition("active = ?", vec![Value::Boolean(true)])
    .build()?;
```

## Phantom Type System

The phantom type system ensures:
- Compile-time validation of query construction
- Type-safe column operations
- Prevention of invalid SQL generation
- Clear API guidance through type signatures

## State Transitions

```text
Initial → from() → TableSelected → select() → ColumnsSelected → build() → Complete
```

Each state transition enforces certain constraints and makes different methods available.
*/

// Re-export core query types
pub use crate::query::{
    ZQuery, Query, QueryBuilder, QueryResult, QueryError, QueryContext,
    SelectResult, InsertResult, UpdateResult, DeleteResult,
    DdlResult, UtilityResult, QueryParameter, QueryParameterValue,
    InsertedId, ColumnMetadata, ExecutionStats,
    TransactionMode, QueryState, ConnectionStatus,
    query_states, QueryStateTrait,
};

// Re-export phantom type infrastructure
pub use crate::phantom::{
    BuilderState, PhantomQueryBuilder, ValidationError, ValidationResult,
    StateTrait, StateValidator, PhantomValidation, CompileTimeValidator,
    ColumnType, Value, OrderDirection, JoinType,
};

// Re-export enhanced query builders
pub use crate::select::{SelectBuilder, SelectQueryBuilder};
pub use crate::insert::{InsertBuilder, InsertQueryBuilder};
pub use crate::update::{UpdateBuilder, UpdateQueryBuilder};
pub use crate::delete::{DeleteBuilder, DeleteQueryBuilder};

// Re-export WHERE clause types
pub use crate::where_clause::{
    WhereClauseBuilder, Condition, ConditionGroup,
    ComparisonOperator, LogicalOperator
};

// Re-export vector operations
pub use crate::vector::{
    VectorQuery, VectorDistance, VectorQueryBuilder,
    VectorQueryExt, VectorQueryResult, HybridQuery
};

// Re-export transaction support
pub use crate::transaction::{
    TransactionContext, TransactionState, IsolationLevel,
    ConflictInfo, ConflictType, TransactionQueryBuilder,
    WithTransaction, TransactionStats
};

// Re-export common Result type
pub use crate::{Result, FluentSqlError};

// Convenience type aliases for common phantom state combinations
pub type InitialQuery<'q> = crate::select::SelectQueryBuilder<'q, crate::query::query_states::Uninitialized>;
pub type TableQuery<'q> = crate::select::SelectQueryBuilder<'q, crate::query::query_states::TableSelected>;
pub type ColumnsQuery<'q> = crate::select::SelectQueryBuilder<'q, crate::query::query_states::ColumnsSelected>;
pub type ExecutableQuery<'q> = crate::select::SelectQueryBuilder<'q, crate::query::query_states::Executable>;

/// Convenient trait for working with phantom type validations
pub trait PhantomTypeValidation {
    /// Check if the current state allows execution
    fn is_executable(&self) -> bool;

    /// Get the current builder state
    fn state(&self) -> BuilderState;

    /// Validate the current phantom type constraints
    fn passes_phantom_validation(&self) -> bool;

    /// Get validation errors if any
    fn get_validation_errors(&self) -> Vec<ValidationError>;
}

/// Helper trait for state-specific method availability
pub trait MethodAvailability {
    /// Check if a specific method is supported in the current state
    fn supports_method(&self, method_name: &str) -> bool;

    /// Get list of available methods for the current state
    fn available_methods(&self) -> Vec<&'static str>;
}

/// Helper trait for query type checking
pub trait QueryTypeValidation {
    /// Check if query can perform updates (INSERT/UPDATE/DELETE)
    fn can_perform_updates(&self) -> bool;

    /// Check if query can add conditions (WHERE clauses)
    fn can_add_conditions(&self) -> bool;

    /// Check if query can select columns
    fn can_select_columns(&self) -> bool;
}