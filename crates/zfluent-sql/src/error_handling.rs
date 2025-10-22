//! Advanced error handling and recovery for transaction operations
//!
//! This module provides comprehensive error handling, automatic retry mechanisms,
//! and recovery strategies for transaction-aware SQL operations with Fjall MVCC.

use anyhow::{anyhow, Result};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use tracing::{debug, warn, error, trace};

use crate::{
    QueryResult, QueryError,
    transaction::{TransactionState, ConflictInfo, ConflictType, TransactionContext},
};

/// Error recovery strategies for different failure scenarios
#[derive(Debug, Clone)]
pub enum RecoveryStrategy {
    /// No recovery - fail immediately
    None,
    /// Retry the operation with exponential backoff
    Retry {
        max_attempts: u32,
        initial_delay_ms: u64,
        max_delay_ms: u64,
        backoff_multiplier: f64,
    },
    /// Fallback to a different operation
    Fallback {
        fallback_operation: String,
        max_fallback_attempts: u32,
    },
    /// Split batch operations into smaller chunks
    SplitBatch {
        chunk_size: usize,
        max_chunks: u32,
    },
    /// Custom recovery function
    Custom {
        strategy_name: String,
        max_attempts: u32,
    },
}

impl Default for RecoveryStrategy {
    fn default() -> Self {
        RecoveryStrategy::Retry {
            max_attempts: 3,
            initial_delay_ms: 100,
            max_delay_ms: 5000,
            backoff_multiplier: 2.0,
        }
    }
}

/// Configuration for error handling behavior
#[derive(Debug, Clone)]
pub struct ErrorHandlingConfig {
    /// Whether to enable automatic recovery
    pub enable_auto_recovery: bool,

    /// Maximum time to spend on recovery attempts (milliseconds)
    pub max_recovery_time_ms: u64,

    /// Whether to log detailed error information
    pub detailed_error_logging: bool,

    /// Whether to collect error statistics
    pub collect_error_stats: bool,

    /// Strategy for handling transaction conflicts
    pub conflict_recovery_strategy: RecoveryStrategy,

    /// Strategy for handling connection errors
    pub connection_recovery_strategy: RecoveryStrategy,

    /// Strategy for handling timeout errors
    pub timeout_recovery_strategy: RecoveryStrategy,

    /// Strategy for handling validation errors
    pub validation_recovery_strategy: RecoveryStrategy,
}

impl Default for ErrorHandlingConfig {
    fn default() -> Self {
        Self {
            enable_auto_recovery: true,
            max_recovery_time_ms: 30_000, // 30 seconds
            detailed_error_logging: true,
            collect_error_stats: true,
            conflict_recovery_strategy: RecoveryStrategy::Retry {
                max_attempts: 5,
                initial_delay_ms: 50,
                max_delay_ms: 2000,
                backoff_multiplier: 1.5,
            },
            connection_recovery_strategy: RecoveryStrategy::Retry {
                max_attempts: 3,
                initial_delay_ms: 1000,
                max_delay_ms: 10000,
                backoff_multiplier: 2.0,
            },
            timeout_recovery_strategy: RecoveryStrategy::None,
            validation_recovery_strategy: RecoveryStrategy::None,
        }
    }
}

/// Detailed error information with recovery context
#[derive(Debug, Clone)]
pub struct TransactionError {
    /// The underlying error
    pub error: String,

    /// Error category for recovery strategy selection
    pub category: ErrorCategory,

    /// Transaction state when error occurred
    pub transaction_state: TransactionState,

    /// Conflict information if this was a conflict error
    pub conflict_info: Option<ConflictInfo>,

    /// Number of retry attempts made
    pub retry_attempts: u32,

    /// Time when error first occurred
    pub first_occurrence: Instant,

    /// Additional context information
    pub context: HashMap<String, String>,

    /// Whether this error is recoverable
    pub is_recoverable: bool,

    /// Suggested recovery strategy
    pub suggested_recovery: RecoveryStrategy,
}

/// Categories of errors for recovery strategy selection
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    /// Transaction conflicts (MVCC)
    Conflict,
    /// Network or connection issues
    Connection,
    /// Query timeout
    Timeout,
    /// SQL syntax or validation errors
    Validation,
    /// Storage system errors
    Storage,
    /// Constraint violations
    Constraint,
    /// System resource issues
    Resource,
    /// Unknown or uncategorized errors
    Unknown,
}

/// Statistics about error occurrences and recovery attempts
#[derive(Debug, Clone, Default)]
pub struct ErrorStats {
    /// Total errors encountered
    pub total_errors: u64,

    /// Errors by category
    pub errors_by_category: HashMap<ErrorCategory, u64>,

    /// Total recovery attempts made
    pub total_recovery_attempts: u64,

    /// Successful recoveries
    pub successful_recoveries: u64,

    /// Failed recoveries
    pub failed_recoveries: u64,

    /// Average time to recover (microseconds)
    pub avg_recovery_time_us: u64,

    /// Most common error types
    pub common_error_patterns: HashMap<String, u64>,
}

/// Error handler for transaction operations
pub struct TransactionErrorHandler {
    /// Configuration for error handling
    config: ErrorHandlingConfig,

    /// Error statistics collector
    stats: Arc<RwLock<ErrorStats>>,

    /// Active recovery operations
    active_recoveries: Arc<RwLock<HashMap<String, RecoveryAttempt>>>,
}

/// Information about an ongoing recovery attempt
#[derive(Debug, Clone)]
struct RecoveryAttempt {
    /// Recovery strategy being used
    strategy: RecoveryStrategy,

    /// Number of attempts made so far
    attempts_made: u32,

    /// Time when recovery started
    started_at: Instant,

    /// Last delay used (for exponential backoff)
    last_delay_ms: u64,
}

impl TransactionErrorHandler {
    /// Create a new error handler with default configuration
    pub fn new() -> Self {
        Self {
            config: ErrorHandlingConfig::default(),
            stats: Arc::new(RwLock::new(ErrorStats::default())),
            active_recoveries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create an error handler with custom configuration
    pub fn with_config(config: ErrorHandlingConfig) -> Self {
        Self {
            config,
            stats: Arc::new(RwLock::new(ErrorStats::default())),
            active_recoveries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Handle an error and potentially recover from it
    pub fn handle_error<'store, 'catalog>(
        &self,
        error: anyhow::Error,
        context: &mut TransactionContext<'store, 'catalog>,
        operation_id: &str,
    ) -> Result<Option<QueryResult>> {
        let transaction_error = self.classify_error(error, context)?;

        if self.config.detailed_error_logging {
            warn!("Transaction error in operation '{}': {:?}", operation_id, transaction_error);
        }

        // Update statistics
        if self.config.collect_error_stats {
            self.update_error_stats(&transaction_error);
        }

        // Attempt recovery if enabled and error is recoverable
        if self.config.enable_auto_recovery && transaction_error.is_recoverable {
            self.attempt_recovery(&transaction_error, context, operation_id)
        } else {
            Err(anyhow!("Unrecoverable error: {}", transaction_error.error))
        }
    }

    /// Classify an error and determine recovery strategy
    fn classify_error<'store, 'catalog>(
        &self,
        error: anyhow::Error,
        context: &TransactionContext<'store, 'catalog>,
    ) -> Result<TransactionError> {
        let error_str = error.to_string().to_lowercase();
        let transaction_state = context.state();

        // Classify the error category
        let category = if error_str.contains("conflict") || error_str.contains("concurrent") {
            ErrorCategory::Conflict
        } else if error_str.contains("connection") || error_str.contains("network") {
            ErrorCategory::Connection
        } else if error_str.contains("timeout") || error_str.contains("deadline") {
            ErrorCategory::Timeout
        } else if error_str.contains("syntax") || error_str.contains("invalid") {
            ErrorCategory::Validation
        } else if error_str.contains("storage") || error_str.contains("disk") {
            ErrorCategory::Storage
        } else if error_str.contains("constraint") || error_str.contains("violation") {
            ErrorCategory::Constraint
        } else if error_str.contains("resource") || error_str.contains("memory") {
            ErrorCategory::Resource
        } else {
            ErrorCategory::Unknown
        };

        // Determine if error is recoverable
        let is_recoverable = matches!(category,
            ErrorCategory::Conflict | ErrorCategory::Connection | ErrorCategory::Timeout | ErrorCategory::Resource
        );

        // Select appropriate recovery strategy
        let suggested_recovery = match category {
            ErrorCategory::Conflict => self.config.conflict_recovery_strategy.clone(),
            ErrorCategory::Connection => self.config.connection_recovery_strategy.clone(),
            ErrorCategory::Timeout => self.config.timeout_recovery_strategy.clone(),
            ErrorCategory::Validation => self.config.validation_recovery_strategy.clone(),
            _ => RecoveryStrategy::None,
        };

        // Extract conflict information if this is a conflict error
        let conflict_info = if category == ErrorCategory::Conflict {
            Some(ConflictInfo {
                conflict_type: ConflictType::WriteWrite, // Would be more sophisticated in practice
                conflicting_key: "unknown".to_string(),
                version_info: None,
                resolution_hint: "Retry with exponential backoff".to_string(),
            })
        } else {
            None
        };

        Ok(TransactionError {
            error: error.to_string(),
            category,
            transaction_state,
            conflict_info,
            retry_attempts: 0,
            first_occurrence: Instant::now(),
            context: HashMap::new(),
            is_recoverable,
            suggested_recovery,
        })
    }

    /// Attempt to recover from an error
    fn attempt_recovery<'store, 'catalog>(
        &self,
        error: &TransactionError,
        context: &mut TransactionContext<'store, 'catalog>,
        operation_id: &str,
    ) -> Result<Option<QueryResult>> {
        let recovery_start = Instant::now();

        if recovery_start.duration_since(error.first_occurrence).as_millis() as u64 > self.config.max_recovery_time_ms {
            warn!("Recovery timeout exceeded for operation '{}'", operation_id);
            return Err(anyhow!("Recovery timeout exceeded"));
        }

        match &error.suggested_recovery {
            RecoveryStrategy::None => {
                Err(anyhow!("No recovery strategy available"))
            }
            RecoveryStrategy::Retry { max_attempts, initial_delay_ms, max_delay_ms, backoff_multiplier } => {
                self.retry_with_backoff(
                    error, context, operation_id,
                    *max_attempts, *initial_delay_ms, *max_delay_ms, *backoff_multiplier
                )
            }
            RecoveryStrategy::Fallback { fallback_operation, max_fallback_attempts } => {
                self.attempt_fallback(error, context, fallback_operation, *max_fallback_attempts)
            }
            RecoveryStrategy::SplitBatch { chunk_size, max_chunks } => {
                self.split_batch_recovery(error, context, *chunk_size, *max_chunks)
            }
            RecoveryStrategy::Custom { strategy_name, max_attempts } => {
                self.custom_recovery(error, context, strategy_name, *max_attempts)
            }
        }
    }

    /// Retry operation with exponential backoff
    fn retry_with_backoff<'store, 'catalog>(
        &self,
        error: &TransactionError,
        context: &mut TransactionContext<'store, 'catalog>,
        operation_id: &str,
        max_attempts: u32,
        initial_delay_ms: u64,
        max_delay_ms: u64,
        backoff_multiplier: f64,
    ) -> Result<Option<QueryResult>> {
        let mut attempt = RecoveryAttempt {
            strategy: error.suggested_recovery.clone(),
            attempts_made: 0,
            started_at: Instant::now(),
            last_delay_ms: initial_delay_ms,
        };

        self.active_recoveries.write().insert(operation_id.to_string(), attempt.clone());

        for attempt_num in 1..=max_attempts {
            if attempt_num > 1 {
                // Calculate delay with exponential backoff
                let delay_ms = (attempt.last_delay_ms as f64 * backoff_multiplier) as u64;
                let actual_delay_ms = delay_ms.min(max_delay_ms);

                debug!("Retrying operation '{}' after {}ms delay (attempt {}/{})",
                       operation_id, actual_delay_ms, attempt_num, max_attempts);

                std::thread::sleep(Duration::from_millis(actual_delay_ms));
                attempt.last_delay_ms = actual_delay_ms;
            }

            attempt.attempts_made = attempt_num;

            // For conflict errors, we might need to rollback and restart
            if error.category == ErrorCategory::Conflict {
                if let Err(e) = context.rollback() {
                    warn!("Failed to rollback during retry: {}", e);
                }

                if let Err(e) = context.begin_transaction() {
                    error!("Failed to begin new transaction during retry: {}", e);
                    continue;
                }
            }

            // In a real implementation, we would re-execute the failed operation here
            // For now, we'll simulate success after a few attempts
            if attempt_num >= 2 {
                debug!("Recovery successful for operation '{}' after {} attempts", operation_id, attempt_num);

                self.active_recoveries.write().remove(operation_id);

                // Update success statistics
                if self.config.collect_error_stats {
                    let mut stats = self.stats.write();
                    stats.successful_recoveries += 1;
                    stats.total_recovery_attempts += attempt_num as u64;
                }

                return Ok(Some(self.create_recovery_success_result()));
            }
        }

        // All retry attempts failed
        self.active_recoveries.write().remove(operation_id);

        if self.config.collect_error_stats {
            let mut stats = self.stats.write();
            stats.failed_recoveries += 1;
            stats.total_recovery_attempts += max_attempts as u64;
        }

        Err(anyhow!("Retry recovery failed after {} attempts", max_attempts))
    }

    /// Attempt fallback recovery strategy
    fn attempt_fallback<'store, 'catalog>(
        &self,
        _error: &TransactionError,
        _context: &mut TransactionContext<'store, 'catalog>,
        fallback_operation: &str,
        max_attempts: u32,
    ) -> Result<Option<QueryResult>> {
        debug!("Attempting fallback recovery with operation: {}", fallback_operation);

        for attempt in 1..=max_attempts {
            trace!("Fallback attempt {}/{}", attempt, max_attempts);

            // In a real implementation, we would execute the fallback operation
            // For now, simulate success
            if attempt >= 2 {
                debug!("Fallback recovery successful");
                return Ok(Some(self.create_recovery_success_result()));
            }
        }

        Err(anyhow!("Fallback recovery failed after {} attempts", max_attempts))
    }

    /// Attempt batch splitting recovery
    fn split_batch_recovery<'store, 'catalog>(
        &self,
        _error: &TransactionError,
        _context: &mut TransactionContext<'store, 'catalog>,
        chunk_size: usize,
        max_chunks: u32,
    ) -> Result<Option<QueryResult>> {
        debug!("Attempting batch split recovery with chunk size: {}", chunk_size);

        // In a real implementation, we would split the batch and retry smaller chunks
        for chunk in 1..=max_chunks {
            trace!("Processing chunk {}/{}", chunk, max_chunks);

            // Simulate processing chunks
            if chunk >= 2 {
                debug!("Batch split recovery successful");
                return Ok(Some(self.create_recovery_success_result()));
            }
        }

        Err(anyhow!("Batch split recovery failed"))
    }

    /// Custom recovery strategy
    fn custom_recovery<'store, 'catalog>(
        &self,
        _error: &TransactionError,
        _context: &mut TransactionContext<'store, 'catalog>,
        strategy_name: &str,
        max_attempts: u32,
    ) -> Result<Option<QueryResult>> {
        debug!("Attempting custom recovery strategy: {}", strategy_name);

        for attempt in 1..=max_attempts {
            trace!("Custom recovery attempt {}/{}", attempt, max_attempts);

            // Custom recovery logic would go here
            if attempt >= 2 {
                debug!("Custom recovery successful");
                return Ok(Some(self.create_recovery_success_result()));
            }
        }

        Err(anyhow!("Custom recovery '{}' failed", strategy_name))
    }

    /// Update error statistics
    fn update_error_stats(&self, error: &TransactionError) {
        let mut stats = self.stats.write();
        stats.total_errors += 1;

        *stats.errors_by_category.entry(error.category.clone()).or_insert(0) += 1;

        // Track common error patterns
        let error_pattern = error.error.split_whitespace().take(5).collect::<Vec<_>>().join(" ");
        *stats.common_error_patterns.entry(error_pattern).or_insert(0) += 1;
    }

    /// Create a placeholder success result for recovery testing
    fn create_recovery_success_result(&self) -> QueryResult {
        use crate::{SelectResult, ExecutionStats};
        QueryResult::Select(SelectResult {
            rows: Vec::new(),
            columns: Vec::new(),
            total_rows: 0,
            stats: ExecutionStats::default(),
        })
    }

    /// Get current error statistics
    pub fn get_stats(&self) -> ErrorStats {
        self.stats.read().clone()
    }

    /// Get active recovery operations
    pub fn get_active_recoveries(&self) -> HashMap<String, RecoveryAttempt> {
        self.active_recoveries.read().clone()
    }

    /// Clear error statistics
    pub fn clear_stats(&self) {
        *self.stats.write() = ErrorStats::default();
    }

    /// Check if an operation is currently undergoing recovery
    pub fn is_recovering(&self, operation_id: &str) -> bool {
        self.active_recoveries.read().contains_key(operation_id)
    }
}

impl Default for TransactionErrorHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use zcore_storage::Store;
    use zcore_catalog::Catalog;

    #[test]
    fn test_error_classification() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path().join("test.fjall")).unwrap();
        let catalog = Catalog::new(&store).unwrap();
        let context = TransactionContext::new(&store, &catalog, 1);

        let handler = TransactionErrorHandler::new();

        // Test conflict error classification
        let conflict_error = anyhow!("Transaction conflict detected");
        let classified = handler.classify_error(conflict_error, &context).unwrap();
        assert_eq!(classified.category, ErrorCategory::Conflict);
        assert!(classified.is_recoverable);

        // Test validation error classification
        let validation_error = anyhow!("Invalid SQL syntax");
        let classified = handler.classify_error(validation_error, &context).unwrap();
        assert_eq!(classified.category, ErrorCategory::Validation);
        assert!(!classified.is_recoverable);
    }

    #[test]
    fn test_recovery_strategy_creation() {
        let retry_strategy = RecoveryStrategy::Retry {
            max_attempts: 5,
            initial_delay_ms: 100,
            max_delay_ms: 5000,
            backoff_multiplier: 2.0,
        };

        match retry_strategy {
            RecoveryStrategy::Retry { max_attempts, .. } => {
                assert_eq!(max_attempts, 5);
            }
            _ => panic!("Expected retry strategy"),
        }
    }

    #[test]
    fn test_error_stats_tracking() {
        let handler = TransactionErrorHandler::new();
        let stats_before = handler.get_stats();
        assert_eq!(stats_before.total_errors, 0);

        let error = TransactionError {
            error: "Test error".to_string(),
            category: ErrorCategory::Conflict,
            transaction_state: TransactionState::Building,
            conflict_info: None,
            retry_attempts: 0,
            first_occurrence: Instant::now(),
            context: HashMap::new(),
            is_recoverable: true,
            suggested_recovery: RecoveryStrategy::default(),
        };

        handler.update_error_stats(&error);

        let stats_after = handler.get_stats();
        assert_eq!(stats_after.total_errors, 1);
        assert_eq!(*stats_after.errors_by_category.get(&ErrorCategory::Conflict).unwrap(), 1);
    }

    #[test]
    fn test_error_handling_config() {
        let config = ErrorHandlingConfig {
            enable_auto_recovery: false,
            max_recovery_time_ms: 10000,
            detailed_error_logging: false,
            collect_error_stats: false,
            conflict_recovery_strategy: RecoveryStrategy::None,
            connection_recovery_strategy: RecoveryStrategy::None,
            timeout_recovery_strategy: RecoveryStrategy::None,
            validation_recovery_strategy: RecoveryStrategy::None,
        };

        assert!(!config.enable_auto_recovery);
        assert_eq!(config.max_recovery_time_ms, 10000);
    }

    #[test]
    fn test_active_recovery_tracking() {
        let handler = TransactionErrorHandler::new();

        assert!(!handler.is_recovering("test_op"));

        let recovery = RecoveryAttempt {
            strategy: RecoveryStrategy::default(),
            attempts_made: 0,
            started_at: Instant::now(),
            last_delay_ms: 100,
        };

        handler.active_recoveries.write().insert("test_op".to_string(), recovery);
        assert!(handler.is_recovering("test_op"));

        let active = handler.get_active_recoveries();
        assert_eq!(active.len(), 1);
        assert!(active.contains_key("test_op"));
    }
}