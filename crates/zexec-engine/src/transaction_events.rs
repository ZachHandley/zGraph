use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use zcore_catalog::Catalog;
use zcore_storage::Store;
use zevents::{Event, EventData, EventSystem, EventType, TransactionOperation};

/// Database isolation levels
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        Self::ReadCommitted
    }
}

impl std::fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IsolationLevel::ReadUncommitted => write!(f, "READ_UNCOMMITTED"),
            IsolationLevel::ReadCommitted => write!(f, "READ_COMMITTED"),
            IsolationLevel::RepeatableRead => write!(f, "REPEATABLE_READ"),
            IsolationLevel::Serializable => write!(f, "SERIALIZABLE"),
        }
    }
}

/// Configuration for change data capture
#[derive(Debug, Clone)]
pub struct CdcConfig {
    /// Enable data capture for change events
    pub enable_data_capture: bool,
    /// Capture old values for UPDATE and DELETE operations
    pub capture_old_values: bool,
    /// Capture new values for INSERT and UPDATE operations
    pub capture_new_values: bool,
    /// Capture WHERE clause conditions
    pub capture_where_clause: bool,
    /// Maximum size in bytes for captured values
    pub max_capture_size: usize,
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            enable_data_capture: true,
            capture_old_values: true,
            capture_new_values: true,
            capture_where_clause: true,
            max_capture_size: 1024 * 1024, // 1MB
        }
    }
}

/// Transaction-aware SQL execution context
pub struct TransactionContext<'a> {
    /// Unique transaction identifier
    pub transaction_id: String,
    /// Connection identifier
    pub connection_id: String,
    /// Organization ID
    pub org_id: u64,
    /// Event system for publishing events
    pub event_system: Arc<EventSystem>,
    /// Storage layer
    pub store: Arc<Store>,
    /// Catalog for schema metadata
    pub catalog: Catalog<'a>,
    /// List of operations performed in this transaction
    pub operations: Vec<TransactionOperation>,
    /// Tables affected by this transaction
    pub affected_tables: Vec<String>,
    /// Transaction isolation level
    pub isolation_level: IsolationLevel,
    /// User context for audit purposes
    pub user_context: Option<UserContext>,
    /// Change data capture configuration
    pub cdc_config: CdcConfig,
}

/// User context for audit purposes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserContext {
    pub user_id: u64,
    pub username: String,
    pub roles: Vec<String>,
    pub session_id: Option<String>,
}

impl<'a> TransactionContext<'a> {
    pub fn new(
        transaction_id: String,
        connection_id: String,
        org_id: u64,
        event_system: Arc<EventSystem>,
        store: Arc<Store>,
        catalog: Catalog<'a>,
    ) -> Self {
        Self {
            transaction_id,
            connection_id,
            org_id,
            event_system,
            store,
            catalog,
            operations: Vec::new(),
            affected_tables: Vec::new(),
            isolation_level: IsolationLevel::default(),
            user_context: None,
            cdc_config: CdcConfig::default(),
        }
    }

    /// Create a new transaction context with user context
    pub fn with_user_context(
        transaction_id: String,
        connection_id: String,
        org_id: u64,
        event_system: Arc<EventSystem>,
        store: Arc<Store>,
        catalog: Catalog<'a>,
        user_context: Option<UserContext>,
        isolation_level: Option<IsolationLevel>,
        cdc_config: Option<CdcConfig>,
    ) -> Self {
        Self {
            transaction_id,
            connection_id,
            org_id,
            event_system,
            store,
            catalog,
            operations: Vec::new(),
            affected_tables: Vec::new(),
            isolation_level: isolation_level.unwrap_or_default(),
            user_context,
            cdc_config: cdc_config.unwrap_or_default(),
        }
    }

    /// Set user context from zauth session information
    pub fn set_user_context(&mut self, user_context: UserContext) {
        self.user_context = Some(user_context);
    }

    /// Set isolation level
    pub fn set_isolation_level(&mut self, level: IsolationLevel) {
        self.isolation_level = level;
    }

    /// Set CDC configuration
    pub fn set_cdc_config(&mut self, config: CdcConfig) {
        self.cdc_config = config;
    }

    /// Begin a new transaction with event emission
    pub async fn begin(&mut self) -> Result<()> {
        // Emit transaction begin event
        self.event_system
            .transactions
            .begin_transaction(
                self.transaction_id.clone(),
                self.connection_id.clone(),
                self.org_id,
                Some(self.isolation_level.to_string())
            )
            .await?;

        tracing::info!(
            transaction_id = %self.transaction_id,
            connection_id = %self.connection_id,
            org_id = self.org_id,
            "Transaction began with event tracking"
        );

        Ok(())
    }

    /// Add an operation to the transaction context with enhanced data capture
    pub async fn add_operation_with_data(
        &mut self,
        operation_type: &str,
        table_name: Option<&str>,
        rows_affected: Option<u64>,
        duration_ms: Option<u64>,
        old_values: Option<HashMap<String, serde_json::Value>>,
        new_values: Option<HashMap<String, serde_json::Value>>,
        where_clause: Option<String>,
    ) -> Result<()> {
        let operation = TransactionOperation {
            operation_type: operation_type.to_string(),
            table_name: table_name.map(|s| s.to_string()),
            rows_affected,
            duration_ms,
            metadata: std::collections::HashMap::new(),
        };

        // Add to affected tables if not already tracked
        if let Some(table) = table_name {
            if !self.affected_tables.contains(&table.to_string()) {
                self.affected_tables.push(table.to_string());
            }
        }

        self.operations.push(operation.clone());

        // Notify transaction event manager
        self.event_system
            .transactions
            .add_operation(&self.transaction_id, operation)
            .await?;

        // Emit database change event for data operations
        if matches!(operation_type, "INSERT" | "UPDATE" | "DELETE") {
            let event = Event::new(
                match operation_type {
                    "INSERT" => EventType::RowInserted,
                    "UPDATE" => EventType::RowUpdated,
                    "DELETE" => EventType::RowDeleted,
                    _ => EventType::QueryExecuted,
                },
                EventData::DatabaseChange {
                    org_id: self.org_id,
                    table_name: table_name.unwrap_or("unknown").to_string(),
                    operation: operation_type.to_string(),
                    transaction_id: Some(self.transaction_id.clone()),
                    user_id: self.user_context.as_ref().map(|u| u.user_id),
                    connection_id: Some(self.connection_id.clone()),
                    old_values: if self.cdc_config.capture_old_values { old_values } else { None },
                    new_values: if self.cdc_config.capture_new_values { new_values } else { None },
                    where_clause: if self.cdc_config.capture_where_clause { where_clause } else { None },
                    rows_affected: rows_affected.unwrap_or(0),
                    schema_changes: None,
                    timestamp_ms: chrono::Utc::now().timestamp_millis(),
                },
                self.org_id,
            )
            .with_source("zexec-engine".to_string())
            .with_correlation_id(self.transaction_id.clone());

            self.event_system.publish(event).await?;
        }

        tracing::debug!(
            transaction_id = %self.transaction_id,
            operation_type = %operation_type,
            table_name = ?table_name,
            rows_affected = ?rows_affected,
            user_id = ?self.user_context.as_ref().map(|u| u.user_id),
            "Added operation with data capture to transaction"
        );

        Ok(())
    }

    /// Add an operation to the transaction context (legacy method)
    pub async fn add_operation(
        &mut self,
        operation_type: &str,
        table_name: Option<&str>,
        rows_affected: Option<u64>,
        duration_ms: Option<u64>,
    ) -> Result<()> {
        let operation = TransactionOperation {
            operation_type: operation_type.to_string(),
            table_name: table_name.map(|s| s.to_string()),
            rows_affected,
            duration_ms,
            metadata: std::collections::HashMap::new(),
        };

        // Add to affected tables if not already tracked
        if let Some(table) = table_name {
            if !self.affected_tables.contains(&table.to_string()) {
                self.affected_tables.push(table.to_string());
            }
        }

        self.operations.push(operation.clone());

        // Notify transaction event manager
        self.event_system
            .transactions
            .add_operation(&self.transaction_id, operation)
            .await?;

        // Emit database change event for data operations
        if matches!(operation_type, "INSERT" | "UPDATE" | "DELETE") {
            let event = Event::new(
                match operation_type {
                    "INSERT" => EventType::RowInserted,
                    "UPDATE" => EventType::RowUpdated,
                    "DELETE" => EventType::RowDeleted,
                    _ => EventType::QueryExecuted,
                },
                EventData::DatabaseChange {
                    org_id: self.org_id,
                    table_name: table_name.unwrap_or("unknown").to_string(),
                    operation: operation_type.to_string(),
                    transaction_id: Some(self.transaction_id.clone()),
                    user_id: self.user_context.as_ref().map(|u| u.user_id),
                    connection_id: Some(self.connection_id.clone()),
                    old_values: None, // Use add_operation_with_data for data capture
                    new_values: None, // Use add_operation_with_data for data capture
                    where_clause: None, // Use add_operation_with_data for data capture
                    rows_affected: rows_affected.unwrap_or(0),
                    schema_changes: None,
                    timestamp_ms: chrono::Utc::now().timestamp_millis(),
                },
                self.org_id,
            )
            .with_source("zexec-engine".to_string())
            .with_correlation_id(self.transaction_id.clone());

            self.event_system.publish(event).await?;
        }

        tracing::debug!(
            transaction_id = %self.transaction_id,
            operation_type = %operation_type,
            table_name = ?table_name,
            rows_affected = ?rows_affected,
            "Added operation to transaction"
        );

        Ok(())
    }

    /// Commit the transaction with event emission
    pub async fn commit(self) -> Result<()> {
        // Extract values before the move
        let transaction_id = self.transaction_id.clone();
        let operations_count = self.operations.len();
        let affected_tables = self.affected_tables.clone();
        let _org_id = self.org_id;

        // Emit transaction commit event
        self.event_system
            .transactions
            .commit_transaction(&self.transaction_id)
            .await?;

        // Create cache invalidation events for affected tables
        let invalidation_events = self
            .event_system
            .transactions
            .create_change_events(&self.transaction_id, self.affected_tables, self.org_id)
            .await;

        // Publish cache invalidation events
        for event in invalidation_events {
            self.event_system.publish(event).await?;
        }

        tracing::info!(
            transaction_id = %transaction_id,
            operations_count = operations_count,
            affected_tables = ?affected_tables,
            "Transaction committed with event tracking"
        );

        Ok(())
    }

    /// Rollback the transaction with event emission
    pub async fn rollback(self, error: Option<String>) -> Result<()> {
        // Emit transaction rollback event
        self.event_system
            .transactions
            .rollback_transaction(&self.transaction_id, error.clone())
            .await?;

        tracing::info!(
            transaction_id = %self.transaction_id,
            operations_count = self.operations.len(),
            error = ?error,
            "Transaction rolled back with event tracking"
        );

        Ok(())
    }

    /// Abort the transaction due to system error
    pub async fn abort(self, error: String) -> Result<()> {
        // Emit transaction abort event
        self.event_system
            .transactions
            .abort_transaction(&self.transaction_id, error.clone())
            .await?;

        tracing::warn!(
            transaction_id = %self.transaction_id,
            operations_count = self.operations.len(),
            error = %error,
            "Transaction aborted with event tracking"
        );

        Ok(())
    }
}

/// Enhanced SQL execution with transaction event integration
pub struct EventAwareSqlExecutor {
    /// Event system for publishing events
    pub event_system: Arc<EventSystem>,
}

impl EventAwareSqlExecutor {
    pub fn new(event_system: Arc<EventSystem>) -> Self {
        Self { event_system }
    }

    /// Execute SQL with transaction event tracking
    pub async fn execute_with_events<'a>(
        &self,
        sql: &str,
        org_id: u64,
        store: Arc<Store>,
        catalog: &'a Store,
        connection_id: String,
    ) -> Result<crate::ExecResult> {
        let transaction_id = format!("tx_{}", ulid::Ulid::new());
        let catalog_instance = Catalog::new(catalog);

        let mut tx_context = TransactionContext::new(
            transaction_id.clone(),
            connection_id,
            org_id,
            self.event_system.clone(),
            store.clone(),
            catalog_instance,
        );

        // Begin transaction with events
        tx_context.begin().await?;

        // Execute the SQL statement
        let start_time = std::time::Instant::now();
        let exec_catalog = Catalog::new(catalog);
        let result = match crate::exec_sql_store(sql, org_id, &store, &exec_catalog) {
            Ok(result) => {
                let duration_ms = start_time.elapsed().as_millis() as u64;

                // Determine operation type from SQL
                let operation_type = self.determine_operation_type(sql);
                let table_name = self.extract_table_name(sql, &operation_type);
                let rows_affected = self.extract_rows_affected(&result);

                // Add operation to transaction context
                tx_context
                    .add_operation(
                        &operation_type,
                        table_name.as_deref(),
                        rows_affected,
                        Some(duration_ms),
                    )
                    .await?;

                // Commit transaction
                tx_context.commit().await?;

                Ok(result)
            }
            Err(e) => {
                // Rollback transaction on error
                tx_context.rollback(Some(e.to_string())).await?;
                Err(e)
            }
        };

        result
    }

    /// Execute multiple SQL statements in a single transaction
    pub async fn execute_batch_with_events<'a>(
        &self,
        statements: Vec<&str>,
        org_id: u64,
        store: Arc<Store>,
        catalog: &'a Store,
        connection_id: String,
    ) -> Result<Vec<crate::ExecResult>> {
        let transaction_id = format!("tx_{}", ulid::Ulid::new());
        let catalog_instance = Catalog::new(catalog);

        let mut tx_context = TransactionContext::new(
            transaction_id.clone(),
            connection_id,
            org_id,
            self.event_system.clone(),
            store.clone(),
            catalog_instance,
        );

        // Begin transaction with events
        tx_context.begin().await?;

        let mut results = Vec::new();
        let mut _total_duration = 0u64;

        // Execute statements
        for sql in statements {
            let start_time = std::time::Instant::now();
            let exec_catalog = Catalog::new(catalog);

            match crate::exec_sql_store(sql, org_id, &store, &exec_catalog) {
                Ok(result) => {
                    let duration_ms = start_time.elapsed().as_millis() as u64;
                    _total_duration += duration_ms;

                    // Determine operation type from SQL
                    let operation_type = self.determine_operation_type(sql);
                    let table_name = self.extract_table_name(sql, &operation_type);
                    let rows_affected = self.extract_rows_affected(&result);

                    // Add operation to transaction context
                    tx_context
                        .add_operation(
                            &operation_type,
                            table_name.as_deref(),
                            rows_affected,
                            Some(duration_ms),
                        )
                        .await?;

                    results.push(result);
                }
                Err(e) => {
                    // Rollback transaction on any error
                    tx_context.rollback(Some(e.to_string())).await?;
                    return Err(e);
                }
            }
        }

        // Commit transaction
        tx_context.commit().await?;

        Ok(results)
    }

    /// Determine operation type from SQL statement
    fn determine_operation_type(&self, sql: &str) -> String {
        let sql_upper = sql.trim().to_uppercase();

        if sql_upper.starts_with("SELECT") {
            "SELECT"
        } else if sql_upper.starts_with("INSERT") {
            "INSERT"
        } else if sql_upper.starts_with("UPDATE") {
            "UPDATE"
        } else if sql_upper.starts_with("DELETE") {
            "DELETE"
        } else if sql_upper.starts_with("CREATE TABLE") {
            "CREATE_TABLE"
        } else if sql_upper.starts_with("CREATE INDEX") {
            "CREATE_INDEX"
        } else if sql_upper.starts_with("DROP TABLE") {
            "DROP_TABLE"
        } else if sql_upper.starts_with("DROP INDEX") {
            "DROP_INDEX"
        } else if sql_upper.starts_with("ALTER TABLE") {
            "ALTER_TABLE"
        } else {
            "UNKNOWN"
        }.to_string()
    }

    /// Extract table name from SQL statement
    fn extract_table_name(&self, sql: &str, operation_type: &str) -> Option<String> {
        let sql_upper = sql.trim().to_uppercase();
        let words: Vec<&str> = sql_upper.split_whitespace().collect();

        match operation_type {
            "SELECT" => {
                // Look for FROM clause
                if let Some(from_idx) = words.iter().position(|&w| w == "FROM") {
                    words.get(from_idx + 1).map(|s| s.to_string())
                } else {
                    None
                }
            }
            "INSERT" => {
                // Look for INTO clause
                if let Some(into_idx) = words.iter().position(|&w| w == "INTO") {
                    words.get(into_idx + 1).map(|s| s.to_string())
                } else {
                    None
                }
            }
            "UPDATE" | "DELETE" => {
                // Table name follows operation
                words.get(1).map(|s| s.to_string())
            }
            "CREATE_TABLE" | "DROP_TABLE" | "ALTER_TABLE" => {
                // Table name follows TABLE keyword
                if let Some(table_idx) = words.iter().position(|&w| w == "TABLE") {
                    words.get(table_idx + 1).map(|s| s.to_string())
                } else {
                    None
                }
            }
            "CREATE_INDEX" | "DROP_INDEX" => {
                // For indexes, we might want to track the table name from ON clause
                if let Some(on_idx) = words.iter().position(|&w| w == "ON") {
                    words.get(on_idx + 1).map(|s| s.to_string())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Extract number of rows affected from execution result
    fn extract_rows_affected(&self, result: &crate::ExecResult) -> Option<u64> {
        match result {
            crate::ExecResult::Affected(count) => Some(*count),
            crate::ExecResult::Rows(rows) => Some(rows.len() as u64),
            crate::ExecResult::None => Some(0), // No rows affected for schema operations
        }
    }

    /// Extract WHERE clause from SQL statement
    fn extract_where_clause(&self, sql: &str) -> Option<String> {
        let sql_upper = sql.trim().to_uppercase();

        // Find WHERE keyword
        if let Some(where_pos) = sql_upper.find(" WHERE ") {
            let where_start = where_pos + 7; // " WHERE ".len()
            let sql_after_where = &sql[where_start..];

            // Find potential end of WHERE clause (ORDER BY, GROUP BY, LIMIT, etc.)
            let end_keywords = ["ORDER BY", "GROUP BY", "HAVING", "LIMIT", "OFFSET"];
            let mut end_pos = sql_after_where.len();

            for keyword in &end_keywords {
                if let Some(pos) = sql_after_where.to_uppercase().find(keyword) {
                    end_pos = end_pos.min(pos);
                }
            }

            let where_clause = sql_after_where[..end_pos].trim();
            if !where_clause.is_empty() {
                Some(where_clause.to_string())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Create user context from zauth session
    pub fn create_user_context_from_session(
        session: &zauth::AuthSession,
    ) -> UserContext {
        UserContext {
            user_id: session.user_id.map(|id| id.0 as u64).unwrap_or(0),
            username: format!("user_{}", session.user_id.map(|id| id.to_string()).unwrap_or_else(|| "unknown".to_string())),
            roles: session.roles.clone(),
            session_id: None, // Could be extracted from connection context
        }
    }

    /// Capture old values for UPDATE/DELETE operations
    async fn capture_old_values(
        &self,
        table_name: &str,
        where_clause: Option<&str>,
        org_id: u64,
        store: &Store,
        catalog: &Store,
    ) -> Result<Option<HashMap<String, serde_json::Value>>> {
        if let Some(where_condition) = where_clause {
            // Build a SELECT query to get current values
            let select_sql = format!("SELECT * FROM {} WHERE {}", table_name, where_condition);

            match crate::exec_sql_store(&select_sql, org_id, store, &Catalog::new(catalog)) {
                Ok(crate::ExecResult::Rows(rows)) => {
                    if rows.is_empty() {
                        return Ok(None);
                    }

                    // Convert the first matching row to HashMap
                    let first_row = &rows[0];
                    let mut old_values = HashMap::new();

                    // Convert IndexMap<String, JsonValue> to HashMap<String, serde_json::Value>
                    for (column_name, json_value) in first_row.iter() {
                        old_values.insert(column_name.clone(), json_value.clone());
                    }

                    Ok(Some(old_values))
                }
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    /// Capture new values from INSERT/UPDATE statements
    fn capture_new_values_from_sql(&self, sql: &str) -> Option<HashMap<String, serde_json::Value>> {
        let sql_upper = sql.trim().to_uppercase();

        if sql_upper.starts_with("INSERT") {
            self.extract_insert_values(sql)
        } else if sql_upper.starts_with("UPDATE") {
            self.extract_update_values(sql)
        } else {
            None
        }
    }

    /// Extract values from INSERT statement
    fn extract_insert_values(&self, sql: &str) -> Option<HashMap<String, serde_json::Value>> {
        // This is a simplified implementation
        // In practice, you'd want to use a proper SQL parser
        let sql_upper = sql.to_uppercase();

        if let Some(values_pos) = sql_upper.find("VALUES") {
            let values_part = &sql[values_pos + 6..].trim();
            if let Some(start) = values_part.find('(') {
                if let Some(end) = values_part.find(')') {
                    let values_str = &values_part[start + 1..end];
                    let mut new_values = HashMap::new();

                    // Simple comma-separated value parsing
                    for (idx, value) in values_str.split(',').enumerate() {
                        let trimmed = value.trim();
                        let json_value = self.parse_sql_literal(trimmed);
                        new_values.insert(format!("col_{}", idx), json_value);
                    }

                    return Some(new_values);
                }
            }
        }

        None
    }

    /// Extract values from UPDATE statement
    fn extract_update_values(&self, sql: &str) -> Option<HashMap<String, serde_json::Value>> {
        let sql_upper = sql.to_uppercase();

        if let Some(set_pos) = sql_upper.find(" SET ") {
            let set_part = &sql[set_pos + 5..];
            let where_pos = set_part.to_uppercase().find(" WHERE ").unwrap_or(set_part.len());
            let assignments = &set_part[..where_pos];

            let mut new_values = HashMap::new();

            for assignment in assignments.split(',') {
                let trimmed = assignment.trim();
                if let Some(eq_pos) = trimmed.find('=') {
                    let column = trimmed[..eq_pos].trim().to_string();
                    let value = trimmed[eq_pos + 1..].trim();
                    let json_value = self.parse_sql_literal(value);
                    new_values.insert(column, json_value);
                }
            }

            return Some(new_values);
        }

        None
    }

    /// Parse SQL literal to JSON value
    fn parse_sql_literal(&self, literal: &str) -> serde_json::Value {
        let trimmed = literal.trim();

        // Handle string literals
        if (trimmed.starts_with('\'') && trimmed.ends_with('\'')) ||
           (trimmed.starts_with('"') && trimmed.ends_with('"')) {
            let content = &trimmed[1..trimmed.len() - 1];
            return serde_json::Value::String(content.to_string());
        }

        // Handle NULL
        if trimmed.to_uppercase() == "NULL" {
            return serde_json::Value::Null;
        }

        // Handle boolean
        match trimmed.to_uppercase().as_str() {
            "TRUE" => return serde_json::Value::Bool(true),
            "FALSE" => return serde_json::Value::Bool(false),
            _ => {}
        }

        // Handle numbers
        if let Ok(int_val) = trimmed.parse::<i64>() {
            return serde_json::Value::Number(serde_json::Number::from(int_val));
        }

        if let Ok(float_val) = trimmed.parse::<f64>() {
            if let Some(num) = serde_json::Number::from_f64(float_val) {
                return serde_json::Value::Number(num);
            }
        }

        // Default to string
        serde_json::Value::String(trimmed.to_string())
    }

    /// Execute SQL with enhanced change data capture
    pub async fn execute_with_cdc<'a>(
        &self,
        sql: &str,
        org_id: u64,
        store: Arc<Store>,
        catalog: &'a Store,
        connection_id: String,
        user_context: Option<UserContext>,
        cdc_config: Option<CdcConfig>,
    ) -> Result<crate::ExecResult> {
        let transaction_id = format!("tx_{}", ulid::Ulid::new());
        let catalog_instance = Catalog::new(catalog);

        let mut tx_context = TransactionContext::with_user_context(
            transaction_id.clone(),
            connection_id,
            org_id,
            self.event_system.clone(),
            store.clone(),
            catalog_instance,
            user_context,
            None,
            cdc_config.clone(),
        );

        // Begin transaction with events
        tx_context.begin().await?;

        // Determine operation type and extract metadata
        let operation_type = self.determine_operation_type(sql);
        let table_name = self.extract_table_name(sql, &operation_type);
        let where_clause = self.extract_where_clause(sql);

        // Capture old values for UPDATE/DELETE if enabled
        let old_values = if tx_context.cdc_config.capture_old_values &&
                           matches!(operation_type.as_str(), "UPDATE" | "DELETE") {
            if let Some(table) = &table_name {
                self.capture_old_values(
                    table,
                    where_clause.as_deref(),
                    org_id,
                    &store,
                    catalog,
                ).await?
            } else {
                None
            }
        } else {
            None
        };

        // Execute the SQL statement
        let start_time = std::time::Instant::now();
        let exec_catalog = Catalog::new(catalog);
        let result = match crate::exec_sql_store(sql, org_id, &store, &exec_catalog) {
            Ok(result) => {
                let duration_ms = start_time.elapsed().as_millis() as u64;
                let rows_affected = self.extract_rows_affected(&result);

                // Capture new values for INSERT/UPDATE
                let new_values = if tx_context.cdc_config.capture_new_values &&
                                   matches!(operation_type.as_str(), "INSERT" | "UPDATE") {
                    self.capture_new_values_from_sql(sql)
                } else {
                    None
                };

                // Add operation with captured data to transaction context
                tx_context
                    .add_operation_with_data(
                        &operation_type,
                        table_name.as_deref(),
                        rows_affected,
                        Some(duration_ms),
                        old_values,
                        new_values,
                        where_clause,
                    )
                    .await?;

                // Commit transaction
                tx_context.commit().await?;

                Ok(result)
            }
            Err(e) => {
                // Rollback transaction on error
                tx_context.rollback(Some(e.to_string())).await?;
                Err(e)
            }
        };

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use zevents::EventSystemConfig;

    #[tokio::test]
    async fn test_transaction_context_lifecycle() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());
        let storage_clone = storage.clone();

        let event_system = Arc::new(
            EventSystem::new(storage.clone(), EventSystemConfig::default())
                .await
                .unwrap()
        );

        let catalog = Catalog::new(&storage_clone);
        let mut tx_context = TransactionContext::new(
            "test_tx".to_string(),
            "test_conn".to_string(),
            1,
            event_system,
            storage,
            catalog,
        );

        // Test transaction lifecycle
        tx_context.begin().await.unwrap();

        tx_context
            .add_operation("INSERT", Some("users"), Some(1), Some(10))
            .await
            .unwrap();

        tx_context.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_operation_type_detection() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let event_system = Arc::new(
            EventSystem::new(storage.clone(), EventSystemConfig::default())
                .await
                .unwrap()
        );

        let executor = EventAwareSqlExecutor::new(event_system);

        assert_eq!(executor.determine_operation_type("SELECT * FROM users"), "SELECT");
        assert_eq!(executor.determine_operation_type("INSERT INTO users VALUES (1)"), "INSERT");
        assert_eq!(executor.determine_operation_type("UPDATE users SET name = 'test'"), "UPDATE");
        assert_eq!(executor.determine_operation_type("DELETE FROM users WHERE id = 1"), "DELETE");
        assert_eq!(executor.determine_operation_type("CREATE TABLE users (id INT)"), "CREATE_TABLE");
    }

    #[tokio::test]
    async fn test_table_name_extraction() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let event_system = Arc::new(
            EventSystem::new(storage.clone(), EventSystemConfig::default())
                .await
                .unwrap()
        );

        let executor = EventAwareSqlExecutor::new(event_system);

        assert_eq!(
            executor.extract_table_name("SELECT * FROM users", "SELECT"),
            Some("USERS".to_string())
        );

        assert_eq!(
            executor.extract_table_name("INSERT INTO products VALUES (1)", "INSERT"),
            Some("PRODUCTS".to_string())
        );

        assert_eq!(
            executor.extract_table_name("UPDATE orders SET status = 'complete'", "UPDATE"),
            Some("ORDERS".to_string())
        );
    }

    #[tokio::test]
    async fn test_where_clause_extraction() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let event_system = Arc::new(
            EventSystem::new(storage.clone(), EventSystemConfig::default())
                .await
                .unwrap()
        );

        let executor = EventAwareSqlExecutor::new(event_system);

        // Test simple WHERE clause
        assert_eq!(
            executor.extract_where_clause("SELECT * FROM users WHERE id = 1"),
            Some("id = 1".to_string())
        );

        // Test WHERE with ORDER BY
        assert_eq!(
            executor.extract_where_clause("SELECT * FROM users WHERE name = 'John' ORDER BY created_at"),
            Some("name = 'John'".to_string())
        );

        // Test UPDATE with WHERE
        assert_eq!(
            executor.extract_where_clause("UPDATE users SET status = 'active' WHERE id = 1"),
            Some("id = 1".to_string())
        );

        // Test DELETE with WHERE
        assert_eq!(
            executor.extract_where_clause("DELETE FROM users WHERE id = 1"),
            Some("id = 1".to_string())
        );

        // Test no WHERE clause
        assert_eq!(
            executor.extract_where_clause("SELECT * FROM users"),
            None
        );
    }

    #[tokio::test]
    async fn test_insert_value_extraction() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let event_system = Arc::new(
            EventSystem::new(storage.clone(), EventSystemConfig::default())
                .await
                .unwrap()
        );

        let executor = EventAwareSqlExecutor::new(event_system);

        // Test simple INSERT values
        let values = executor.extract_insert_values(
            "INSERT INTO users (name, age) VALUES ('John', 25)"
        );

        assert!(values.is_some());
        let values = values.unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values.get("col_0"), Some(&serde_json::Value::String("John".to_string())));
        assert_eq!(values.get("col_1"), Some(&serde_json::Value::Number(serde_json::Number::from(25))));
    }

    #[tokio::test]
    async fn test_update_value_extraction() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let event_system = Arc::new(
            EventSystem::new(storage.clone(), EventSystemConfig::default())
                .await
                .unwrap()
        );

        let executor = EventAwareSqlExecutor::new(event_system);

        // Test UPDATE values
        let values = executor.extract_update_values(
            "UPDATE users SET name = 'Jane', age = 30 WHERE id = 1"
        );

        assert!(values.is_some());
        let values = values.unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values.get("name"), Some(&serde_json::Value::String("Jane".to_string())));
        assert_eq!(values.get("age"), Some(&serde_json::Value::Number(serde_json::Number::from(30))));
    }

    #[tokio::test]
    async fn test_sql_literal_parsing() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let event_system = Arc::new(
            EventSystem::new(storage.clone(), EventSystemConfig::default())
                .await
                .unwrap()
        );

        let executor = EventAwareSqlExecutor::new(event_system);

        // Test string literals
        assert_eq!(
            executor.parse_sql_literal("'hello'"),
            serde_json::Value::String("hello".to_string())
        );

        assert_eq!(
            executor.parse_sql_literal("\"world\""),
            serde_json::Value::String("world".to_string())
        );

        // Test integers
        assert_eq!(
            executor.parse_sql_literal("42"),
            serde_json::Value::Number(serde_json::Number::from(42))
        );

        // Test floats
        assert_eq!(
            executor.parse_sql_literal("3.14"),
            serde_json::Value::Number(serde_json::Number::from_f64(3.14).unwrap())
        );

        // Test boolean
        assert_eq!(
            executor.parse_sql_literal("TRUE"),
            serde_json::Value::Bool(true)
        );

        assert_eq!(
            executor.parse_sql_literal("false"),
            serde_json::Value::Bool(false)
        );

        // Test NULL
        assert_eq!(
            executor.parse_sql_literal("NULL"),
            serde_json::Value::Null
        );
    }

    #[tokio::test]
    async fn test_user_context_creation() {
        use ulid::Ulid;

        // Create a mock auth session
        let session = zauth::AuthSession {
            token_id: "test_token".to_string(),
            user_id: Some(Ulid::new()),
            org_id: 123,
            env_id: Ulid::new(),
            env_slug: "test".to_string(),
            roles: vec!["admin".to_string(), "user".to_string()],
            labels: vec!["internal".to_string()],
            session_id: Some(Ulid::new()),
            token_kind: zauth::TokenKind::Access,
            credential_context: None,
            hardware_attested: false,
            biometric_verified: false,
            trust_score: 0.8,
        };

        let user_context = EventAwareSqlExecutor::create_user_context_from_session(&session);

        assert_eq!(user_context.user_id, session.user_id.map(|id| id.0 as u64).unwrap_or(0));
        assert_eq!(user_context.roles, session.roles);
        assert!(user_context.username.starts_with("user_"));
    }

    #[tokio::test]
    async fn test_cdc_config() {
        let config = CdcConfig::default();

        assert_eq!(config.enable_data_capture, true);
        assert_eq!(config.capture_old_values, true);
        assert_eq!(config.capture_new_values, true);
        assert_eq!(config.capture_where_clause, true);
        assert_eq!(config.max_capture_size, 1024 * 1024);

        let custom_config = CdcConfig {
            enable_data_capture: false,
            capture_old_values: false,
            capture_new_values: true,
            capture_where_clause: false,
            max_capture_size: 512 * 1024,
        };

        assert_eq!(custom_config.enable_data_capture, false);
        assert_eq!(custom_config.capture_old_values, false);
        assert_eq!(custom_config.capture_new_values, true);
        assert_eq!(custom_config.capture_where_clause, false);
        assert_eq!(custom_config.max_capture_size, 512 * 1024);
    }

    #[tokio::test]
    async fn test_isolation_levels() {
        assert_eq!(IsolationLevel::default(), IsolationLevel::ReadCommitted);

        assert_eq!(IsolationLevel::ReadUncommitted.to_string(), "READ_UNCOMMITTED");
        assert_eq!(IsolationLevel::ReadCommitted.to_string(), "READ_COMMITTED");
        assert_eq!(IsolationLevel::RepeatableRead.to_string(), "REPEATABLE_READ");
        assert_eq!(IsolationLevel::Serializable.to_string(), "SERIALIZABLE");
    }

    #[tokio::test]
    async fn test_transaction_with_user_context() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let event_system = Arc::new(
            EventSystem::new(storage.clone(), EventSystemConfig::default())
                .await
                .unwrap()
        );

        let storage_clone = storage.clone();
        let catalog = Catalog::new(&storage_clone);

        let user_context = UserContext {
            user_id: 12345,
            username: "test_user".to_string(),
            roles: vec!["admin".to_string()],
            session_id: Some("session_123".to_string()),
        };

        let mut tx_context = TransactionContext::with_user_context(
            "test_tx".to_string(),
            "test_conn".to_string(),
            1,
            event_system,
            storage,
            catalog,
            Some(user_context.clone()),
            Some(IsolationLevel::Serializable),
            Some(CdcConfig::default()),
        );

        assert_eq!(tx_context.isolation_level, IsolationLevel::Serializable);
        assert!(tx_context.user_context.is_some());
        assert_eq!(tx_context.user_context.as_ref().unwrap().user_id, 12345);
        assert_eq!(tx_context.user_context.as_ref().unwrap().username, "test_user");

        // Test transaction lifecycle with user context
        tx_context.begin().await.unwrap();

        tx_context
            .add_operation_with_data(
                "INSERT",
                Some("users"),
                Some(1),
                Some(10),
                None,
                Some({
                    let mut values = HashMap::new();
                    values.insert("name".to_string(), serde_json::Value::String("John".to_string()));
                    values.insert("age".to_string(), serde_json::Value::Number(serde_json::Number::from(25)));
                    values
                }),
                None,
            )
            .await
            .unwrap();

        tx_context.commit().await.unwrap();
    }
}