//! Enhanced tracing interfaces to break circular dependencies

use tracing::{debug, error, info, warn, span, Level, Span};
use std::sync::Arc;

/// Abstract tracing interface for cross-crate logging
pub trait ZTracer: Send + Sync {
    /// Log a debug message
    fn debug(&self, message: &str, fields: &[(&str, &str)]);

    /// Log an info message
    fn info(&self, message: &str, fields: &[(&str, &str)]);

    /// Log a warning message
    fn warn(&self, message: &str, fields: &[(&str, &str)]);

    /// Log an error message
    fn error(&self, message: &str, fields: &[(&str, &str)]);

    /// Create a new span
    fn span(&self, name: &str, fields: &[(&str, &str)]) -> Arc<dyn ZSpan>;

    /// Get tracer name
    fn name(&self) -> &str;
}

/// Abstract span interface
pub trait ZSpan: Send + Sync {
    /// Enter the span context
    fn enter(&self) -> Arc<dyn ZSpanGuard>;

    /// Add fields to the span
    fn add_fields(&self, fields: &[(&str, &str)]);
}

/// Abstract span guard interface
pub trait ZSpanGuard: Send + Sync {
    /// Exit the span context
    fn exit(self: Box<Self>);
}

/// Standard tracing implementation
pub struct StdTracer {
    name: String,
}

impl StdTracer {
    pub fn new(name: String) -> Arc<Self> {
        Arc::new(Self { name })
    }
}

impl ZTracer for StdTracer {
    fn debug(&self, message: &str, _fields: &[(&str, &str)]) {
        debug!(target = %self.name, "{}", message);
    }

    fn info(&self, message: &str, _fields: &[(&str, &str)]) {
        info!(target = %self.name, "{}", message);
    }

    fn warn(&self, message: &str, _fields: &[(&str, &str)]) {
        warn!(target = %self.name, "{}", message);
    }

    fn error(&self, message: &str, _fields: &[(&str, &str)]) {
        error!(target = %self.name, "{}", message);
    }

    fn span(&self, _name: &str, _fields: &[(&str, &str)]) -> Arc<dyn ZSpan> {
        let span = span!(Level::INFO, "span");
        Arc::new(StdSpan { span })
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Standard span implementation
pub struct StdSpan {
    span: Span,
}

impl ZSpan for StdSpan {
    fn enter(&self) -> Arc<dyn ZSpanGuard> {
        // This creates a lifetime issue, so we'll use a simpler approach
        Arc::new(NullSpanGuard)
    }

    fn add_fields(&self, _fields: &[(&str, &str)]) {
        // Fields not supported in simplified implementation
    }
}

/// Standard span guard implementation
pub struct StdSpanGuard {
    _guard: tracing::span::Entered<'static>,
}

impl ZSpanGuard for StdSpanGuard {
    fn exit(self: Box<Self>) {
        // The guard will be dropped automatically
    }
}

/// Null tracer that does nothing (useful for testing)
pub struct NullTracer {
    name: String,
}

impl NullTracer {
    pub fn new(name: String) -> Arc<Self> {
        Arc::new(Self { name })
    }
}

impl ZTracer for NullTracer {
    fn debug(&self, _message: &str, _fields: &[(&str, &str)]) {}
    fn info(&self, _message: &str, _fields: &[(&str, &str)]) {}
    fn warn(&self, _message: &str, _fields: &[(&str, &str)]) {}
    fn error(&self, _message: &str, _fields: &[(&str, &str)]) {}

    fn span(&self, _name: &str, _fields: &[(&str, &str)]) -> Arc<dyn ZSpan> {
        Arc::new(NullSpan { name: "null".to_string() })
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Null span implementation
pub struct NullSpan {
    name: String,
}

impl ZSpan for NullSpan {
    fn enter(&self) -> Arc<dyn ZSpanGuard> {
        Arc::new(NullSpanGuard)
    }

    fn add_fields(&self, _fields: &[(&str, &str)]) {}
}

/// Null span guard implementation
pub struct NullSpanGuard;

impl ZSpanGuard for NullSpanGuard {
    fn exit(self: Box<Self>) {}
}