# zfluent-sql

> **⚠️ DEVELOPMENT STATUS**: This crate is currently under active development and **does not compile** due to workspace dependency issues. See [Current Status](#current-status) for details.

A comprehensive, type-safe fluent SQL query builder for ZRUSTDB with advanced transaction support, compile-time validation, and zero-cost abstractions.

## Overview

zfluent-sql provides a modern, ergonomic interface for constructing SQL queries with compile-time type safety, transaction support, and deep integration with the ZRUSTDB ecosystem. The crate leverages Rust's powerful type system through phantom types to ensure query correctness at compile time while maintaining runtime performance.

## Key Features

### 🔒 **Compile-Time Type Safety**
- **Phantom Type System**: Enforces valid query construction at compile time
- **State Machine**: Progressive disclosure of operations based on query state
- **Column Type Validation**: Compile-time checking of column references and types
- **Prevention of SQL Injection**: Type-safe parameter binding

### ⚡ **Performance & Efficiency**
- **Zero-Cost Abstractions**: No runtime overhead from type safety
- **SIMD-Optimized Operations**: Leverages zvec-kernels for vector operations
- **Efficient Query Compilation**: Minimal allocation during query building
- **Streaming Parameter Binding**: Memory-efficient parameter handling

### 🧠 **Advanced Query Capabilities**
- **Vector Similarity Search**: First-class support for embedding queries
- **Complex WHERE Clauses**: Nested conditions with logical operators
- **Batch Operations**: Execute multiple queries atomically
- **Transaction Management**: Full ACID compliance with MVCC support
- **Hybrid Queries**: Combine relational and vector operations

### 🔄 **Transaction & Consistency**
- **MVCC Integration**: Multi-Version Concurrency Control with fjall LSM-tree
- **Conflict Detection**: Automatic transaction conflict resolution
- **Error Recovery**: Comprehensive retry mechanisms with backoff
- **Isolation Levels**: Configurable transaction isolation
- **Deadlock Prevention**: Advanced locking strategies

## Architecture

The crate is organized into focused modules:

```
src/
├── lib.rs              # Public API and comprehensive documentation
├── query.rs            # Core query infrastructure (6,000+ lines)
│   ├── query_states    # Phantom state markers
│   ├── ZQuery          # Main query context struct
│   └── QueryBuilder    # Core trait for all query types
├── phantom.rs          # Phantom type system (3,400+ lines)
│   ├── BuilderState    # State enumeration
│   ├── StateValidator  # Compile-time validation
│   └── PhantomQueryBuilder # Generic builder
├── prelude.rs          # Convenient imports and re-exports
├── select.rs           # SELECT query builders (7,800+ lines)
│   ├── SelectBuilder   # Basic SELECT builder
│   └── SelectQueryBuilder # Enhanced version with phantom types
├── insert.rs           # INSERT query builders (6,200+ lines)
│   ├── InsertBuilder   # Basic INSERT builder
│   └── InsertQueryBuilder # Enhanced version with phantom types
├── update.rs           # UPDATE query builders (3,100+ lines)
├── delete.rs           # DELETE query builders (4,100+ lines)
├── where_clause.rs     # WHERE clause construction (3,400+ lines)
│   ├── Condition       # Individual conditions
│   ├── ConditionGroup  # Logical groups
│   └── ComparisonOperator # Type-safe operators
├── vector.rs           # Vector similarity operations (1,900+ lines)
├── transaction.rs      # Transaction management (2,600+ lines)
├── batch.rs            # Batch operations (2,600+ lines)
├── enhanced_select.rs  # Enhanced SELECT features (1,300+ lines)
├── error_handling.rs   # Error recovery strategies (2,200+ lines)
├── execution.rs        # Query execution layer (commented out)
└── tests/              # Internal unit tests
```

## Current Status

### 🔴 **BLOCKING ISSUES**

- **❌ Compilation Fails**: The crate cannot compile due to workspace dependency issues in the ZDB workspace
- **⚠️ Dependencies**: Issues with multiple workspace dependencies including:
  - **Missing workspace members**: `zcore-storage`, `zcore-catalog`, `ztransaction` are not listed in workspace Cargo.toml
  - `zperf-engine` compilation errors (SIMD API conflicts)
  - `ztransaction` move semantics issues
  - Workspace configuration problems
- **🚫 Tests**: Cannot run tests due to compilation failures
- **⏳ Timeline**: Estimated 1-2 weeks to resolve blocking issues

### ✅ **WHAT'S WORKING**

- **Architecture**: Excellent phantom type system and state machine design
  - Sophisticated compile-time validation using Rust's type system
  - State machine pattern preventing invalid query construction
  - Zero-cost abstractions with no runtime overhead
- **Documentation**: Comprehensive inline documentation and examples
- **Structure**: Well-organized modular design with clear separation of concerns
- **Type Safety**: Sophisticated compile-time validation framework
  - `query_states` module providing state markers
  - `StateTrait` and `StateValidator` for compile-time checks
  - Progressive API disclosure based on query state
- **Code Quality**: Clean, well-structured Rust code with good error handling patterns

### 📋 **NEXT STEPS**

1. **Fix workspace dependency compilation issues** (CRITICAL)
   - Add missing crates to workspace members: `zcore-storage`, `zcore-catalog`, `ztransaction`
   - Resolve dependency version conflicts
   - Fix `zperf-engine` SIMD API compatibility issues

2. **Complete execution layer integration** (HIGH)
   - Uncomment `pub mod execution;` in `lib.rs`
   - Integrate `ExecutionLayer` with query builders
   - Implement `execute()` methods for all query types

3. **Implement missing QueryBuilder trait methods** (HIGH)
   - Replace `unimplemented!()` calls in `select.rs`, `insert.rs`, `update.rs`, `delete.rs`
   - Implement `build_sql()`, `get_parameters()`, `validate()`, `execute_with_context()`

4. **Fix error handling panics** (MEDIUM)
   - Replace `panic!("Expected retry strategy")` in `error_handling.rs:604`
   - Implement proper error recovery mechanisms

5. **Complete test suite** (MEDIUM)
   - Fix test compilation once dependencies are resolved
   - Add integration tests for phantom type system
   - Verify transaction management functionality

## Quick Start (Intended Usage)

> **Note**: The following examples show the intended API once compilation issues are resolved.

Add to your `Cargo.toml`:

```toml
[dependencies]
zfluent-sql = "0.1.0"
```

### Basic Query Construction (Intended)

```rust,ignore
use zfluent_sql::prelude::*;

// Simple SELECT with WHERE conditions
let query = Query::select()
    .from("users")
    .select(&["id", "name", "email"])
    .expect("Valid columns")
    .where_col("active").eq(true)
    .and_where_col("age").gt(18)
    .build()?;

// Execute the query
let results = query.execute().await?;
```

### Vector Similarity Search (Intended)

```rust,ignore
use zfluent_sql::prelude::*;

// Find similar embeddings
let target_embedding = vec![0.1, 0.2, 0.3, /* ... */];

let query = Query::select()
    .from("documents")
    .select(&["id", "title", "content"])
    .expect("Valid columns")
    .order_by_vector_distance("embedding", &target_embedding)
    .limit(10)
    .build()?;

let similar_docs = query.execute().await?;
```

### Type-Safe INSERT Operations (Intended)

```rust,ignore
use zfluent_sql::prelude::*;

// Insert with conflict resolution
let query = Query::insert_into("users")
    .values([
        ("name", Value::Text("Alice".to_string())),
        ("email", Value::Text("alice@example.com".to_string())),
        ("age", Value::Integer(25)),
    ])?
    .on_conflict_do_update("email")
    .build()?;

let result = query.execute().await?;
```

## Phantom Type System

The phantom type system provides compile-time validation through sophisticated state transitions using Rust's type system:

### State Machine Architecture

```text
Uninitialized → table() → TableSelected → select() → ColumnsSelected
                                     ↙         ↘
                                where_()       finalize()
                                   ↓               ↓
                        WhereClauseAdded → Complete → Executable
```

### Implementation Details

The phantom type system is implemented using:

1. **State Markers** (`query_states` module):
   - `Uninitialized` - Query builder in initial state
   - `TableSelected` - Table has been specified via `from()`
   - `ColumnsSelected` - Columns have been selected via `select()`
   - `WhereClauseAdded` - WHERE conditions have been added
   - `Executable` - Query is ready for execution

2. **State Validation Traits**:
   - `StateTrait` - Marker trait for all valid states
   - `StateValidator<From, To>` - Validates state transitions
   - `PhantomValidation` - Compile-time validation logic

3. **Progressive API Disclosure**:
   - Each state only exposes valid next operations
   - Invalid operations are prevented at compile time
   - Type signatures guide correct usage

### State Enforcement

```rust
// ✅ Valid: Proper state transitions
let query = Query::select()
    .from("users")                    // Initial → TableSelected
    .select(&["id", "name"])          // TableSelected → ColumnsSelected
    .expect("Valid columns")
    .where_col("active").eq(true)     // ColumnsSelected → Complete
    .build()?;                        // Complete → Executable

// ❌ Compile Error: select() not available on Initial state
let invalid = Query::select()
    .select(&["id"]);                 // COMPILE ERROR!

// ❌ Compile Error: Missing table selection
let invalid = Query::select()
    .where_col("active").eq(true);    // COMPILE ERROR!
```

### Benefits

- **Early Error Detection**: Catch mistakes at compile time
- **API Guidance**: Type signatures guide correct usage
- **Zero Runtime Cost**: No performance impact
- **Better IDE Support**: Improved autocomplete and error messages

## Transaction Management

### Basic Transactions

```rust
use zfluent_sql::prelude::*;

// Create transaction context
let mut ctx = TransactionContext::new(&store, &catalog, org_id);
ctx.begin_transaction()?;

// Execute queries within transaction
let select_query = Query::select()
    .from("users")
    .select(&["id", "name"])?
    .with_transaction("tx_123".to_string());

let result = select_query.execute_in_transaction(&mut ctx)?;

// Commit atomically
ctx.commit()?;
```

### Advanced Transaction Features

```rust
use zfluent_sql::prelude::*;

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

// Automatically handle conflicts and retries
match handler.handle_error(error, &mut ctx, "operation_id") {
    Ok(Some(result)) => println!("Operation recovered successfully"),
    Err(e) => println!("Recovery failed: {}", e),
}
```

## Batch Operations

Execute multiple queries atomically with dependency resolution:

```rust
use zfluent_sql::prelude::*;

// Create batch with configuration
let config = BatchExecutionConfig {
    max_batch_size: 100,
    continue_on_error: false,
    validate_before_execution: true,
    batch_timeout_ms: 30_000,
    detailed_logging: true,
};

let mut batch = BatchQueryBuilder::with_config(&store, &catalog, org_id, config);

// Add operations to batch
batch.add_select("get_users".to_string(),
    Query::select().from("users").select_all())?;

batch.add_insert("add_user".to_string(),
    Query::insert_into("users")
        .values([("name", Value::Text("Bob".to_string()))])?)?;

batch.add_update("update_user".to_string(),
    Query::update("users")
        .set("last_login", Value::Timestamp(Utc::now()))
        .where_col("id").eq(123))?;

// Execute all operations atomically
let result = batch.execute()?;
println!("Success rate: {:.1}%", result.success_rate());
```

## Vector Operations

### Distance Metrics

Support for multiple distance metrics optimized with SIMD:

```rust
use zfluent_sql::prelude::*;

// L2 (Euclidean) distance - default
let l2_query = Query::select()
    .from("embeddings")
    .vector_search("embedding", &target_vector, VectorDistance::L2)
    .limit(10);

// Cosine similarity
let cosine_query = Query::select()
    .from("embeddings")
    .vector_search("embedding", &target_vector, VectorDistance::Cosine)
    .limit(10);

// Inner product
let inner_query = Query::select()
    .from("embeddings")
    .vector_search("embedding", &target_vector, VectorDistance::InnerProduct)
    .limit(10);
```

### Hybrid Queries

Combine vector similarity with traditional filters:

```rust
use zfluent_sql::prelude::*;

let hybrid_query = Query::select()
    .from("documents")
    .select(&["id", "title", "content", "category"])
    .expect("Valid columns")
    .where_col("category").eq("technology")
    .and_where_col("published_date").gt("2024-01-01")
    .order_by_vector_distance("embedding", &query_vector)
    .limit(20)
    .build()?;

// Efficient execution: filter first, then vector search
let results = hybrid_query.execute().await?;
```

## Error Handling

### Comprehensive Error Types

```rust
use zfluent_sql::prelude::*;

match query.execute().await {
    Ok(result) => println!("Success: {} rows", result.row_count()),
    Err(FluentSqlError::InvalidColumn(col)) => {
        eprintln!("Invalid column reference: {}", col);
    },
    Err(FluentSqlError::TypeMismatch { expected, actual }) => {
        eprintln!("Type mismatch: expected {}, got {}", expected, actual);
    },
    Err(FluentSqlError::InvalidVectorDimension { expected, actual }) => {
        eprintln!("Vector dimension mismatch: expected {}, got {}", expected, actual);
    },
    Err(e) => eprintln!("Other error: {}", e),
}
```

### Recovery Strategies

```rust
use zfluent_sql::prelude::*;

// Configure automatic retry with exponential backoff
let recovery = RecoveryStrategy::Retry {
    max_attempts: 3,
    initial_delay_ms: 100,
    max_delay_ms: 5000,
    backoff_multiplier: 2.0,
};

let config = ErrorHandlingConfig {
    conflict_recovery_strategy: recovery,
    enable_auto_recovery: true,
    ..Default::default()
};
```

## Performance Considerations

### Query Optimization

- **Column Selection**: Only select needed columns to reduce I/O
- **WHERE Clause Ordering**: Most selective conditions first
- **Index Usage**: Leverage vector and traditional indexes
- **Batch Size**: Optimal batch sizes for bulk operations

### Memory Management

- **Streaming Results**: Use iterators for large result sets
- **Parameter Binding**: Efficient memory usage for parameters
- **Connection Pooling**: Reuse database connections
- **Vector Caching**: Cache frequently used embeddings

## Development

### Prerequisites

To work on this crate, you must first resolve the workspace dependency issues:

```bash
# Navigate to ZDB root
cd /path/to/ZDB

# Attempt to fix dependencies (these currently fail)
cargo check
```

### Current Development Tasks

See [TODO_STILL.md](./TODO_STILL.md) for a comprehensive list of issues and priorities.

### Building (Currently Fails)

```bash
# These commands currently fail due to dependency issues
cargo check -p zfluent-sql
cargo test -p zfluent-sql
cargo clippy -p zfluent-sql
```

### Testing (Currently Blocked)

Once dependencies are fixed:

```bash
# Unit tests
cargo test -p zfluent-sql

# Integration tests
cargo test -p zfluent-sql --test integration_test

# Benchmarks
cargo bench -p zfluent-sql --bench fluent_sql_benchmarks

# Example programs
cargo run -p zfluent-sql --example phantom_types
cargo run -p zfluent-sql --example transaction_demo
cargo run -p zfluent-sql --example vector_operations
```

## Performance Benchmarks (Intended)

The crate is designed to include comprehensive benchmarks measuring:

- Query construction time
- Parameter binding performance
- Vector similarity search latency
- Transaction throughput
- Batch operation efficiency
- Memory allocation patterns

Run benchmarks with (once dependencies are fixed):

```bash
cargo bench -p zfluent-sql --bench fluent_sql_benchmarks
```

## Examples (Currently Non-runnable)

The `examples/` directory contains comprehensive examples:

- `phantom_types.rs` - Phantom type system demonstration
- `transaction_demo.rs` - Transaction management patterns
- `vector_operations.rs` - Vector similarity search examples
- `where_clause_demo.rs` - Complex WHERE clause construction
- `insert_demo.rs` - INSERT operations with conflict resolution

> **Note**: These examples currently cannot be run due to compilation issues.

## Dependencies

### External Dependencies

```toml
[dependencies]
# Core workspace dependencies
anyhow = "1"                    # Error handling
serde = { version = "1", features = ["derive"] }  # Serialization
tokio = { version = "1", features = ["full"] }    # Async runtime
tracing = "0.1"               # Structured logging
async-trait = "0.1"           # Async trait support

# Type safety and utilities
once_cell = "1"               # Lazy initialization
parking_lot = "0.12"          # Fast locking primitives
serde_json = "1"              # JSON handling
indexmap = "2"                 # Insertion-ordered maps
base64 = "0.4"                # Base64 encoding
thiserror = "1"               # Custom error types
hex = "0.4"                   # Hex encoding
```

### Internal ZRUSTDB Dependencies

zfluent-sql integrates deeply with the ZRUSTDB ecosystem:

- **zcore-storage**: Persistent storage layer with fjall LSM-tree backend
  - Provides `Store`, `ReadTransaction`, `WriteTransaction`
  - Handles data persistence and transactional integrity with MVCC support
- **zcore-catalog**: Table and column metadata management
  - Provides `Catalog`, `TableDef`, `ColumnType`
  - Manages schema information and validation
- **zexec-engine**: SQL execution with hash joins and optimization
  - Provides `exec_sql_store` function
  - Handles query execution and result processing
- **zann-hnsw**: Vector indexing for similarity search
  - HNSW (Hierarchical Navigable Small World) index implementation
  - Optimized for approximate nearest neighbor search
- **zvec-kernels**: SIMD-optimized distance calculations
  - Provides vector distance metrics (L2, Cosine, Inner Product)
  - Runtime CPU feature detection for optimal performance
- **ztransaction**: Transaction management and MVCC
  - Multi-Version Concurrency Control with fjall integration
  - ACID transaction semantics

## Contributing

### Current State

This crate is not currently accepting contributions until the compilation issues are resolved. However, you can help by:

1. **Fixing Dependencies**: Resolve issues in `zperf-engine`, `ztransaction`, and other dependent crates
2. **Code Review**: Review the existing architecture and design patterns
3. **Documentation**: Help improve documentation clarity

### Future Contribution Process

Once dependencies are resolved:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass: `cargo test -p zfluent-sql`
5. Run benchmarks: `cargo bench -p zfluent-sql`
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Changelog

### Development Status (Not Yet Released)

**Planned for v0.1.0**:
- Phantom type system for compile-time validation
- Basic query builders (SELECT, INSERT, UPDATE, DELETE)
- Transaction management with MVCC support
- Vector similarity search capabilities
- Comprehensive error handling and recovery
- Batch operations with dependency resolution
- Performance benchmarks and optimization

**Current Blockers**:
- Workspace dependency compilation issues
- Missing execution layer integration
- Unimplemented QueryBuilder trait methods
- Test suite compilation failures