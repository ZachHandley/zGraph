# zcommon

Common interfaces and utilities for ZRUSTDB - a shared foundation crate designed to break circular dependencies between service crates while maintaining clean, abstract interfaces.

## Purpose

The `zcommon` crate serves as the foundational layer for ZRUSTDB's microservices architecture, providing:

- **Abstract interfaces** for cross-cutting concerns (metrics, health checks, tracing)
- **Dependency injection** capabilities through trait-based design
- **Circular dependency resolution** by centralizing common abstractions
- **Null implementations** for testing and development scenarios

## Compilation Status

✅ **Successfully compiles** as a standalone crate with minor warnings:
- Warning: unused field `span` in `StdSpan` struct
- Warning: unused field `name` in `NullSpan` struct

⚠️ **Workspace integration issues**: The crate cannot be compiled within the main workspace due to dependency conflicts in other workspace members (specifically `argon2` dependency missing from workspace root). The crate itself has no dependency issues.

## Key Features

### Health Check Framework
- Abstract `HealthCheck` trait for service health monitoring
- Specialized interfaces for authentication and job queue services
- Structured health status reporting with metadata support
- Null health check implementation for testing

### Metrics Collection System
- Generic `MetricsCollector` trait for metrics aggregation
- Domain-specific metrics for authentication and job processing
- Automatic timer functionality with RAII-based measurements
- Null metrics collector for development/testing

### Enhanced Tracing Interface
- Abstract `ZTracer` trait for cross-crate logging
- Span-based tracing with guard semantics
- Structured logging support with field injection
- Null tracer implementation for performance-critical scenarios

## Dependencies

The crate uses minimal, well-established dependencies:
- **async-trait** - Async trait definitions
- **serde** & **serde_json** - Serialization framework
- **tokio** - Async runtime with full feature set
- **anyhow** - Error handling
- **thiserror** - Derived error types
- **chrono** - Date/time handling with serde support
- **uuid** - UUID generation with serde support
- **tracing** - Structured logging framework

**Note**: The crate does **NOT** use any storage backends (redb or fjall) as it focuses solely on abstract interfaces for observability and health checking.

## Architecture

The crate follows a trait-based design pattern:

```rust
// Abstract interface
pub trait HealthCheck: Send + Sync {
    async fn check(&self) -> HealthCheckResult;
    fn is_critical(&self) -> bool;
}

// Concrete implementation
pub struct NullHealthCheck { /* ... */ }

// Service-specific extension
#[async_trait]
pub trait AuthHealthCheck: Send + Sync {
    async fn get_auth_stats(&self) -> Result<AuthStats, HealthCheckError>;
}
```

## Usage Examples

### Health Checks

```rust
use zcommon::{HealthCheck, HealthCheckResult, HealthStatus, NullHealthCheck};

let health_check = NullHealthCheck::new("my-service".to_string());
let result = health_check.check().await;

match result.status {
    HealthStatus::Healthy => println!("Service is healthy"),
    HealthStatus::Warning => println!("Service has warnings"),
    HealthStatus::Critical => println!("Service is critical"),
}
```

### Metrics Collection

```rust
use zcommon::{MetricsCollector, NullMetricsCollector};

let metrics = NullMetricsCollector::new();

// Record counters
metrics.increment_counter("requests_total", &[("method", "GET")]);

// Record gauges
metrics.set_gauge("active_connections", 42.0, &[("service", "api")]);

// Time operations
let timer = metrics.start_timer("operation_duration", &[("type", "query")]);
// ... do work ...
timer.stop();
```

### Tracing

```rust
use zcommon::{ZTracer, StdTracer};

let tracer = StdTracer::new("my-service".to_string());

// Basic logging
tracer.info("Service started", &[]);
tracer.error("Database connection failed", &[("db", "postgres")]);

// Span-based tracing
let span = tracer.span("process_request", &[("request_id", "12345")]);
let _guard = span.enter();
// ... work within span ...
```

### Service Integration

```rust
use zcommon::{HealthCheck, MetricsCollector, AuthHealthCheck};
use std::sync::Arc;

struct MyService {
    metrics: Arc<dyn MetricsCollector>,
    health_check: Arc<dyn HealthCheck>,
}

impl MyService {
    async fn handle_request(&self) {
        let timer = self.metrics.start_timer("request_duration", &[]);

        // ... process request ...

        timer.stop();
        self.metrics.increment_counter("requests_processed", &[]);
    }
}
```

## API Reference

### Health Check Types

- `HealthStatus` - Enum representing health states (Healthy, Warning, Critical)
- `HealthCheckResult` - Structured health check result with metadata
- `HealthCheckError` - Error types for health check failures
- `AuthStats` - Authentication service statistics
- `QueueStats` - Job queue statistics

### Metrics Types

- `MetricsCollector` - Main trait for metrics collection
- `MetricsTimer` - RAII timer for automatic duration measurement
- `AuthMetrics` - Authentication-specific metrics
- `JobMetrics` - Job processing metrics

### Tracing Types

- `ZTracer` - Main trait for structured logging
- `ZSpan` - Span-based tracing interface
- `ZSpanGuard` - RAII guard for span lifecycle management

## Null Implementations

The crate provides null implementations for all interfaces:

- `NullHealthCheck` - Always returns healthy status
- `NullMetricsCollector` - No-op metrics collection
- `NullTracer` - No-op tracing implementation

These are useful for:
- Testing scenarios
- Performance-critical paths where overhead must be minimized
- Development environments where full observability isn't required

## Error Handling

The crate uses structured error types:

```rust
#[derive(Debug, thiserror::Error)]
pub enum HealthCheckError {
    #[error("Service unavailable: {0}")]
    ServiceUnavailable(String),

    #[error("Timeout after {0}ms")]
    Timeout(u64),

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}
```

## Thread Safety

All traits require `Send + Sync` implementation, ensuring thread safety across async contexts. The use of `Arc<dyn Trait>` enables safe sharing across service boundaries.

## Known Issues and Limitations

### Current Issues
- **Lifetime management**: `StdSpanGuard` has problematic lifetime issues with `Entered<'static>`
- **Incomplete field support**: `StdSpan::add_fields()` is currently stubbed out and non-functional
- **Span guard implementation**: `StdSpan::enter()` returns a `NullSpanGuard` instead of proper span management
- **Unused fields**: Compiler warnings for unused `span` and `name` fields in span implementations

### Workspace Integration
The crate cannot be compiled within the main ZRUSTDB workspace due to dependency conflicts in other crates, but compiles successfully as a standalone crate.

### Storage Backend Usage
This crate does **NOT** use any storage backends (redb or fjall). It is purely an interface definition crate for observability and health checking abstractions.

## Contributing

When adding new interfaces to zcommon:

1. Follow the existing trait-based design pattern
2. Provide null implementations for testing
3. Include comprehensive documentation
4. Ensure all traits are `Send + Sync`
5. Use `async-trait` for async methods
6. Address the known lifetime and field support issues in span implementations

## License

This crate is part of the ZRUSTDB project and follows the same licensing terms.