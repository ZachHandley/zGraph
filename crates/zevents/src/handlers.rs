use crate::events::{Event, EventType};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Result of event handling
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HandlerResult {
    /// Event was processed successfully
    Success,
    /// Event processing failed, should be retried
    Retry,
    /// Event should be discarded (no retry)
    Discard,
    /// Event should be moved to dead letter queue with reason
    DeadLetter(String),
}

/// Error types for event handlers
#[derive(Debug, Clone, thiserror::Error)]
pub enum HandlerError {
    #[error("Handler processing failed: {message}")]
    ProcessingFailed { message: String },

    #[error("Handler configuration error: {message}")]
    ConfigurationError { message: String },

    #[error("Handler timeout after {duration_ms}ms")]
    Timeout { duration_ms: u64 },

    #[error("Handler dependency unavailable: {dependency}")]
    DependencyUnavailable { dependency: String },

    #[error("Handler resource exhausted: {resource}")]
    ResourceExhausted { resource: String },

    #[error("Event validation failed: {reason}")]
    ValidationFailed { reason: String },
}

impl HandlerError {
    pub fn should_retry(&self) -> bool {
        match self {
            HandlerError::ProcessingFailed { .. } => true,
            HandlerError::ConfigurationError { .. } => false,
            HandlerError::Timeout { .. } => false, // Timeouts should go to dead letter
            HandlerError::DependencyUnavailable { .. } => true,
            HandlerError::ResourceExhausted { .. } => true,
            HandlerError::ValidationFailed { .. } => false,
        }
    }

    pub fn to_handler_result(&self) -> HandlerResult {
        if self.should_retry() {
            HandlerResult::Retry
        } else {
            HandlerResult::DeadLetter(self.to_string())
        }
    }
}

/// Trait for handling events
#[async_trait]
pub trait EventHandler: Send + Sync {
    /// Handle an event and return the result
    async fn handle(&self, event: &Event) -> HandlerResult;

    /// Check if this handler is interested in a specific event type
    fn interested_in(&self, event_type: &EventType) -> bool;

    /// Get handler name for logging/debugging
    fn name(&self) -> &str {
        "EventHandler"
    }

    /// Get handler configuration
    fn config(&self) -> HandlerConfig {
        HandlerConfig::default()
    }

    /// Validate event before processing (optional)
    async fn validate(&self, _event: &Event) -> Result<(), HandlerError> {
        Ok(())
    }

    /// Initialize handler (called once at startup)
    async fn initialize(&self) -> Result<(), HandlerError> {
        Ok(())
    }

    /// Cleanup handler (called at shutdown)
    async fn cleanup(&self) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Configuration for event handlers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandlerConfig {
    /// Maximum number of retries for failed events
    pub max_retries: u32,
    /// Timeout for event processing in milliseconds
    pub timeout_ms: u64,
    /// Whether to validate events before processing
    pub validate_events: bool,
    /// Batch size for bulk processing (if supported)
    pub batch_size: Option<usize>,
    /// Handler-specific configuration
    pub custom: serde_json::Value,
}

impl Default for HandlerConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            timeout_ms: 30000, // 30 seconds
            validate_events: true,
            batch_size: None,
            custom: serde_json::Value::Null,
        }
    }
}

/// Wrapper for handlers with timeout and validation
pub struct ManagedHandler<H: EventHandler> {
    handler: H,
    config: HandlerConfig,
}

impl<H: EventHandler> ManagedHandler<H> {
    pub fn new(handler: H, config: HandlerConfig) -> Self {
        Self { handler, config }
    }
}

#[async_trait]
impl<H: EventHandler> EventHandler for ManagedHandler<H> {
    async fn handle(&self, event: &Event) -> HandlerResult {
        // Validate event if configured
        if self.config.validate_events {
            if let Err(e) = self.handler.validate(event).await {
                return e.to_handler_result();
            }
        }

        // Apply timeout
        let timeout_duration = tokio::time::Duration::from_millis(self.config.timeout_ms);

        match tokio::time::timeout(timeout_duration, self.handler.handle(event)).await {
            Ok(result) => result,
            Err(_) => HandlerError::Timeout {
                duration_ms: self.config.timeout_ms,
            }
            .to_handler_result(),
        }
    }

    fn interested_in(&self, event_type: &EventType) -> bool {
        self.handler.interested_in(event_type)
    }

    fn name(&self) -> &str {
        self.handler.name()
    }

    fn config(&self) -> HandlerConfig {
        self.config.clone()
    }

    async fn initialize(&self) -> Result<(), HandlerError> {
        self.handler.initialize().await
    }

    async fn cleanup(&self) -> Result<(), HandlerError> {
        self.handler.cleanup().await
    }
}

/// Logging handler for debugging events
pub struct LoggingHandler {
    name: String,
    log_level: LogLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl LoggingHandler {
    pub fn new(name: String, log_level: LogLevel) -> Self {
        Self { name, log_level }
    }
}

#[async_trait]
impl EventHandler for LoggingHandler {
    async fn handle(&self, event: &Event) -> HandlerResult {
        let message = format!(
            "Event processed: id={}, type={}, org_id={}, timestamp={}",
            event.id, event.event_type, event.org_id, event.timestamp
        );

        match self.log_level {
            LogLevel::Debug => tracing::debug!(handler = %self.name, "{}", message),
            LogLevel::Info => tracing::info!(handler = %self.name, "{}", message),
            LogLevel::Warn => tracing::warn!(handler = %self.name, "{}", message),
            LogLevel::Error => tracing::error!(handler = %self.name, "{}", message),
        }

        HandlerResult::Success
    }

    fn interested_in(&self, _event_type: &EventType) -> bool {
        true // Log all events
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Metrics handler for collecting event statistics
pub struct MetricsHandler {
    event_counts: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, u64>>>,
}

impl MetricsHandler {
    pub fn new() -> Self {
        Self {
            event_counts: std::sync::Arc::new(tokio::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
        }
    }

    pub async fn get_counts(&self) -> std::collections::HashMap<String, u64> {
        self.event_counts.read().await.clone()
    }

    pub async fn reset_counts(&self) {
        self.event_counts.write().await.clear();
    }
}

impl Default for MetricsHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventHandler for MetricsHandler {
    async fn handle(&self, event: &Event) -> HandlerResult {
        let event_type_key = event.event_type.to_string();
        let mut counts = self.event_counts.write().await;
        *counts.entry(event_type_key).or_insert(0) += 1;

        HandlerResult::Success
    }

    fn interested_in(&self, _event_type: &EventType) -> bool {
        true // Count all event types
    }

    fn name(&self) -> &str {
        "MetricsHandler"
    }
}

/// Filter handler that only processes events matching certain criteria
pub struct FilterHandler<H: EventHandler> {
    inner: H,
    filter: Box<dyn Fn(&Event) -> bool + Send + Sync>,
}

impl<H: EventHandler> FilterHandler<H> {
    pub fn new<F>(inner: H, filter: F) -> Self
    where
        F: Fn(&Event) -> bool + Send + Sync + 'static,
    {
        Self {
            inner,
            filter: Box::new(filter),
        }
    }
}

#[async_trait]
impl<H: EventHandler> EventHandler for FilterHandler<H> {
    async fn handle(&self, event: &Event) -> HandlerResult {
        if (self.filter)(event) {
            self.inner.handle(event).await
        } else {
            HandlerResult::Discard
        }
    }

    fn interested_in(&self, event_type: &EventType) -> bool {
        self.inner.interested_in(event_type)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn config(&self) -> HandlerConfig {
        self.inner.config()
    }

    async fn initialize(&self) -> Result<(), HandlerError> {
        self.inner.initialize().await
    }

    async fn cleanup(&self) -> Result<(), HandlerError> {
        self.inner.cleanup().await
    }
}

/// Transform handler that modifies events before passing to inner handler
pub struct TransformHandler<H: EventHandler> {
    inner: H,
    transform: Box<dyn Fn(Event) -> Event + Send + Sync>,
}

impl<H: EventHandler> TransformHandler<H> {
    pub fn new<F>(inner: H, transform: F) -> Self
    where
        F: Fn(Event) -> Event + Send + Sync + 'static,
    {
        Self {
            inner,
            transform: Box::new(transform),
        }
    }
}

#[async_trait]
impl<H: EventHandler> EventHandler for TransformHandler<H> {
    async fn handle(&self, event: &Event) -> HandlerResult {
        let transformed = (self.transform)(event.clone());
        self.inner.handle(&transformed).await
    }

    fn interested_in(&self, event_type: &EventType) -> bool {
        self.inner.interested_in(event_type)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn config(&self) -> HandlerConfig {
        self.inner.config()
    }

    async fn initialize(&self) -> Result<(), HandlerError> {
        self.inner.initialize().await
    }

    async fn cleanup(&self) -> Result<(), HandlerError> {
        self.inner.cleanup().await
    }
}

/// Conditional handler that runs different handlers based on conditions
pub struct ConditionalHandler {
    conditions: Vec<(Box<dyn Fn(&Event) -> bool + Send + Sync>, Box<dyn EventHandler>)>,
    default_handler: Option<Box<dyn EventHandler>>,
}

impl ConditionalHandler {
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
            default_handler: None,
        }
    }

    pub fn add_condition<F, H>(mut self, condition: F, handler: H) -> Self
    where
        F: Fn(&Event) -> bool + Send + Sync + 'static,
        H: EventHandler + 'static,
    {
        self.conditions
            .push((Box::new(condition), Box::new(handler)));
        self
    }

    pub fn with_default<H>(mut self, handler: H) -> Self
    where
        H: EventHandler + 'static,
    {
        self.default_handler = Some(Box::new(handler));
        self
    }
}

impl Default for ConditionalHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventHandler for ConditionalHandler {
    async fn handle(&self, event: &Event) -> HandlerResult {
        // Check conditions in order
        for (condition, handler) in &self.conditions {
            if condition(event) {
                return handler.handle(event).await;
            }
        }

        // Use default handler if available
        if let Some(ref default) = self.default_handler {
            return default.handle(event).await;
        }

        HandlerResult::Discard
    }

    fn interested_in(&self, event_type: &EventType) -> bool {
        // Check if any handler is interested
        for (_, handler) in &self.conditions {
            if handler.interested_in(event_type) {
                return true;
            }
        }

        if let Some(ref default) = self.default_handler {
            return default.interested_in(event_type);
        }

        false
    }

    fn name(&self) -> &str {
        "ConditionalHandler"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{Event, EventData, EventType};

    struct TestHandler {
        should_fail: bool,
    }

    impl TestHandler {
        fn new(should_fail: bool) -> Self {
            Self { should_fail }
        }
    }

    #[async_trait]
    impl EventHandler for TestHandler {
        async fn handle(&self, _event: &Event) -> HandlerResult {
            if self.should_fail {
                HandlerResult::Retry
            } else {
                HandlerResult::Success
            }
        }

        fn interested_in(&self, _event_type: &EventType) -> bool {
            true
        }

        fn name(&self) -> &str {
            "TestHandler"
        }
    }

    #[tokio::test]
    async fn test_handler_success() {
        let handler = TestHandler::new(false);
        let event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 1,
                status: None,
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );

        let result = handler.handle(&event).await;
        assert_eq!(result, HandlerResult::Success);
    }

    #[tokio::test]
    async fn test_handler_retry() {
        let handler = TestHandler::new(true);
        let event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 1,
                status: None,
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );

        let result = handler.handle(&event).await;
        assert_eq!(result, HandlerResult::Retry);
    }

    #[tokio::test]
    async fn test_managed_handler_timeout() {
        struct SlowHandler;

        #[async_trait]
        impl EventHandler for SlowHandler {
            async fn handle(&self, _event: &Event) -> HandlerResult {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                HandlerResult::Success
            }

            fn interested_in(&self, _event_type: &EventType) -> bool {
                true
            }
        }

        let config = HandlerConfig {
            timeout_ms: 50, // 50ms timeout
            ..Default::default()
        };

        let managed = ManagedHandler::new(SlowHandler, config);
        let event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 1,
                status: None,
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );

        let result = managed.handle(&event).await;
        assert!(matches!(result, HandlerResult::DeadLetter(_)));
    }

    #[tokio::test]
    async fn test_filter_handler() {
        let inner = TestHandler::new(false);
        let filter = FilterHandler::new(inner, |event| event.org_id == 1);

        // Should process org_id = 1
        let event1 = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 1,
                status: None,
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );

        let result1 = filter.handle(&event1).await;
        assert_eq!(result1, HandlerResult::Success);

        // Should discard org_id = 2
        let event2 = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 2,
                status: None,
                worker_id: None,
                error: None,
                output: None,
            },
            2,
        );

        let result2 = filter.handle(&event2).await;
        assert_eq!(result2, HandlerResult::Discard);
    }

    #[tokio::test]
    async fn test_metrics_handler() {
        let handler = MetricsHandler::new();

        let event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 1,
                status: None,
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );

        handler.handle(&event).await;
        handler.handle(&event).await;

        let counts = handler.get_counts().await;
        assert_eq!(counts.get("job.created"), Some(&2));
    }
}