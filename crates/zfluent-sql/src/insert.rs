/*!
INSERT query construction with fluent API.

This module provides type-safe INSERT query building with support for:
- Single and bulk inserts with phantom type state validation
- Column-value mapping with comprehensive type checking
- Conflict resolution (ON CONFLICT IGNORE/REPLACE/UPDATE)
- ZQuery integration for connection context
- Returning clauses for retrieving inserted data
*/

use crate::{
    query::{
        ZQuery, QueryBuilder, QueryContext, QueryResult, QueryParameter, QueryParameterValue,
        InsertResult, ExecutionStats, InsertedId, query_states
    },
    transaction::TransactionContext,
    Result, FluentSqlError
};
use anyhow::anyhow;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::marker::PhantomData;
use tracing::debug;
use zcore_catalog::{Catalog, TableDef, ColumnType};
use zcore_storage::Store;
use zexec_engine::{exec_sql_store, ExecResult};

/// INSERT query builder with phantom type state validation
///
/// This builder uses phantom types to enforce proper query construction at compile time.
/// The generic `State` parameter tracks the current query state and ensures only
/// valid operations are available at each stage.
///
/// # State Flow
///
/// ```text
/// Uninitialized → into() → TableSelected
///                              ↓ values()/bulk_values()/value()
///                         ValuesSet
///                           ↙         ↘
///                     on_conflict()  build()
///                       ↓               ↓
///               ConflictHandled → build()
///                       ↓               ↓
///                     Executable → execute()
/// ```
pub struct InsertQueryBuilder<'q, State = query_states::Uninitialized> {
    /// Reference to the query context for execution
    query_context: Option<&'q ZQuery<'q, 'q>>,

    /// Target table name
    table_name: Option<String>,

    /// Column-value pairs for insertion
    values: IndexMap<String, QueryParameterValue>,

    /// Bulk rows for bulk insertion
    bulk_rows: Vec<IndexMap<String, QueryParameterValue>>,

    /// Conflict resolution strategy
    conflict_strategy: ConflictStrategy,

    /// Columns to return after insertion
    returning_columns: Vec<String>,

    /// Query parameters for prepared statement execution
    parameters: Vec<QueryParameter>,

    /// Cached table schema for validation
    table_schema: Option<TableDef>,

    /// Phantom type for state validation
    _state: PhantomData<State>,
}

/// Builder for INSERT queries (backward compatibility)
pub struct InsertBuilder {
    table: String,
    /// Optional transaction ID for transaction-aware operations
    transaction_id: Option<String>,
    /// Column-value pairs for insertion
    values: IndexMap<String, QueryParameterValue>,
    /// Bulk rows for bulk insertion
    bulk_rows: Vec<IndexMap<String, QueryParameterValue>>,
    /// Conflict resolution strategy
    conflict_strategy: ConflictStrategy,
    /// Columns to return after insertion
    returning_columns: Vec<String>,
}

/// Strategies for handling insert conflicts
#[derive(Debug, Clone)]
pub enum ConflictStrategy {
    /// Fail on conflict (default behavior)
    Fail,
    /// Ignore conflicts (INSERT IGNORE behavior)
    Ignore,
    /// Replace on conflict (INSERT ... ON DUPLICATE KEY UPDATE)
    Replace(Vec<String>),
    /// Update specific columns on conflict
    Update(Vec<String>),
}

impl Default for ConflictStrategy {
    fn default() -> Self {
        ConflictStrategy::Fail
    }
}

// Implementations for InsertQueryBuilder with phantom type states
impl<'q> InsertQueryBuilder<'q, query_states::Uninitialized> {
    /// Create a new InsertQueryBuilder
    pub fn new() -> Self {
        debug!("Creating new InsertQueryBuilder");
        Self {
            query_context: None,
            table_name: None,
            values: IndexMap::new(),
            bulk_rows: Vec::new(),
            conflict_strategy: ConflictStrategy::default(),
            returning_columns: Vec::new(),
            parameters: Vec::new(),
            table_schema: None,
            _state: PhantomData,
        }
    }

    /// Create a new InsertQueryBuilder with query context
    pub fn with_context(context: &'q ZQuery<'q, 'q>) -> Self {
        debug!("Creating InsertQueryBuilder with context");
        Self {
            query_context: Some(context),
            table_name: None,
            values: IndexMap::new(),
            bulk_rows: Vec::new(),
            conflict_strategy: ConflictStrategy::default(),
            returning_columns: Vec::new(),
            parameters: Vec::new(),
            table_schema: None,
            _state: PhantomData,
        }
    }

    /// Specify the target table - transitions to TableSelected state
    pub fn into(mut self, table: &str) -> InsertQueryBuilder<'q, query_states::TableSelected> {
        debug!("Setting target table: {}", table);
        self.table_name = Some(table.to_string());

        InsertQueryBuilder {
            query_context: self.query_context,
            table_name: self.table_name,
            values: self.values,
            bulk_rows: self.bulk_rows,
            conflict_strategy: self.conflict_strategy,
            returning_columns: self.returning_columns,
            parameters: self.parameters,
            table_schema: self.table_schema,
            _state: PhantomData,
        }
    }
}

impl<'q> InsertQueryBuilder<'q, query_states::TableSelected> {
    /// Insert a single row with column-value mapping - transitions to ValuesSet state
    pub fn values(mut self, row_data: HashMap<&str, impl Into<QueryParameterValue>>) -> InsertQueryBuilder<'q, query_states::ColumnsSelected> {
        debug!("Setting single row values with {} columns", row_data.len());

        // Clear any existing values and bulk rows
        self.values.clear();
        self.bulk_rows.clear();

        // Convert HashMap to IndexMap with proper typing
        for (column, value) in row_data {
            self.values.insert(column.to_string(), value.into());
        }

        InsertQueryBuilder {
            query_context: self.query_context,
            table_name: self.table_name,
            values: self.values,
            bulk_rows: self.bulk_rows,
            conflict_strategy: self.conflict_strategy,
            returning_columns: self.returning_columns,
            parameters: self.parameters,
            table_schema: self.table_schema,
            _state: PhantomData,
        }
    }

    /// Insert multiple rows in bulk - transitions to ValuesSet state
    pub fn bulk_values(mut self, rows: Vec<HashMap<&str, impl Into<QueryParameterValue>>>) -> InsertQueryBuilder<'q, query_states::ColumnsSelected> {
        debug!("Setting bulk values with {} rows", rows.len());

        // Clear existing values
        self.values.clear();
        self.bulk_rows.clear();

        // Convert each row
        for row_data in rows {
            let mut row = IndexMap::new();
            for (column, value) in row_data {
                row.insert(column.to_string(), value.into());
            }
            self.bulk_rows.push(row);
        }

        InsertQueryBuilder {
            query_context: self.query_context,
            table_name: self.table_name,
            values: self.values,
            bulk_rows: self.bulk_rows,
            conflict_strategy: self.conflict_strategy,
            returning_columns: self.returning_columns,
            parameters: self.parameters,
            table_schema: self.table_schema,
            _state: PhantomData,
        }
    }

    /// Add individual column-value pairs - transitions to ValuesSet state
    pub fn value(mut self, column: &str, value: impl Into<QueryParameterValue>) -> InsertQueryBuilder<'q, query_states::ColumnsSelected> {
        debug!("Adding single value: {} = <value>", column);

        // Clear bulk rows if switching to single row mode
        if !self.bulk_rows.is_empty() {
            debug!("Switching from bulk mode to single row mode");
            self.bulk_rows.clear();
        }

        self.values.insert(column.to_string(), value.into());

        InsertQueryBuilder {
            query_context: self.query_context,
            table_name: self.table_name,
            values: self.values,
            bulk_rows: self.bulk_rows,
            conflict_strategy: self.conflict_strategy,
            returning_columns: self.returning_columns,
            parameters: self.parameters,
            table_schema: self.table_schema,
            _state: PhantomData,
        }
    }

    /// Chain multiple column-value pairs
    pub fn values_chain(self) -> InsertValueChain<'q> {
        InsertValueChain {
            builder: InsertQueryBuilder {
                query_context: self.query_context,
                table_name: self.table_name,
                values: IndexMap::new(),
                bulk_rows: Vec::new(),
                conflict_strategy: self.conflict_strategy,
                returning_columns: self.returning_columns,
                parameters: self.parameters,
                table_schema: self.table_schema,
                _state: PhantomData::<query_states::ColumnsSelected>,
            },
        }
    }
}

impl<'q> InsertQueryBuilder<'q, query_states::ColumnsSelected> {
    /// Add more column-value pairs to the current row
    pub fn value(mut self, column: &str, value: impl Into<QueryParameterValue>) -> Self {
        debug!("Adding additional value: {} = <value>", column);

        if !self.bulk_rows.is_empty() {
            debug!("Warning: Adding individual value while in bulk mode - this may not work as expected");
        }

        self.values.insert(column.to_string(), value.into());
        self
    }

    /// Set conflict resolution strategy - IGNORE conflicts
    pub fn on_conflict_ignore(mut self) -> Self {
        debug!("Setting conflict strategy to IGNORE");
        self.conflict_strategy = ConflictStrategy::Ignore;
        self
    }

    /// Set conflict resolution strategy - REPLACE with specified columns
    pub fn on_conflict_replace(mut self, columns: &[&str]) -> Self {
        debug!("Setting conflict strategy to REPLACE with columns: {:?}", columns);
        self.conflict_strategy = ConflictStrategy::Replace(
            columns.iter().map(|c| c.to_string()).collect()
        );
        self
    }

    /// Set conflict resolution strategy - UPDATE specific columns on conflict
    pub fn on_conflict_update(mut self, update_columns: &[&str]) -> Self {
        debug!("Setting conflict strategy to UPDATE columns: {:?}", update_columns);
        self.conflict_strategy = ConflictStrategy::Update(
            update_columns.iter().map(|c| c.to_string()).collect()
        );
        self
    }

    /// Specify columns to return after insertion
    pub fn returning(mut self, columns: &[&str]) -> Self {
        debug!("Setting RETURNING columns: {:?}", columns);
        self.returning_columns = columns.iter().map(|c| c.to_string()).collect();
        self
    }

    /// Return all columns after insertion
    pub fn returning_all(mut self) -> Self {
        debug!("Setting RETURNING *");
        self.returning_columns = vec!["*".to_string()];
        self
    }

    /// Build the final query - transitions to Executable state
    pub fn build(self) -> Result<InsertQueryBuilder<'q, query_states::Executable>> {
        debug!("Building complete INSERT query");

        // Validate that we have required components
        if self.table_name.is_none() {
            return Err(anyhow!("Target table is required").into());
        }

        if self.values.is_empty() && self.bulk_rows.is_empty() {
            return Err(anyhow!("At least one row of values must be provided").into());
        }

        // Validate bulk rows have consistent columns
        if !self.bulk_rows.is_empty() {
            let first_row_columns: Vec<&String> = self.bulk_rows[0].keys().collect();
            for (idx, row) in self.bulk_rows.iter().enumerate().skip(1) {
                let row_columns: Vec<&String> = row.keys().collect();
                if first_row_columns.len() != row_columns.len() ||
                   !first_row_columns.iter().all(|c| row_columns.contains(c)) {
                    return Err(anyhow!(
                        "Bulk row {} has inconsistent columns compared to first row", idx
                    ).into());
                }
            }
        }

        Ok(InsertQueryBuilder {
            query_context: self.query_context,
            table_name: self.table_name,
            values: self.values,
            bulk_rows: self.bulk_rows,
            conflict_strategy: self.conflict_strategy,
            returning_columns: self.returning_columns,
            parameters: self.parameters,
            table_schema: self.table_schema,
            _state: PhantomData,
        })
    }
}

impl<'q> InsertQueryBuilder<'q, query_states::Executable> {
    /// Execute the INSERT query through zexec-engine
    pub fn execute(self) -> Result<InsertResult> {
        debug!("Executing INSERT query");

        // Check if we have a query context
        let context = self.query_context.ok_or_else(|| {
            anyhow!("Query context is required for execution. Use with_context() to provide one.")
        })?;

        // Generate SQL
        let sql = self.to_sql();
        debug!("Generated SQL: {}", sql);

        // Get organization ID from context
        let org_id = context.org_id().ok_or_else(|| {
            anyhow!("Organization context is required for query execution")
        })?;

        // Validate schema if available
        if let Some(ref table_name) = self.table_name {
            self.validate_schema(context.catalog(), org_id, table_name)?;
        }

        // Execute through zexec-engine
        let exec_result = exec_sql_store(&sql, org_id, context.store(), context.catalog())?;

        // Convert ExecResult to InsertResult
        match exec_result {
            ExecResult::Affected(count) => {
                Ok(InsertResult {
                    inserted_count: count,
                    inserted_ids: vec![], // No IDs available from Affected result - use RETURNING clause for ID extraction
                    stats: ExecutionStats::default(),
                })
            }
            ExecResult::Rows(rows) => {
                // This happens when using RETURNING clause
                let inserted_count = rows.len() as u64;
                let inserted_ids = self.extract_inserted_ids(&rows);

                Ok(InsertResult {
                    inserted_count,
                    inserted_ids,
                    stats: ExecutionStats::default(),
                })
            }
            ExecResult::None => Ok(InsertResult {
                inserted_count: 0,
                inserted_ids: vec![],
                stats: ExecutionStats::default(),
            }),
        }
    }

    /// Validate that a value matches the expected column type
    fn validate_value_type(
        column: &str,
        value: &QueryParameterValue,
        expected_type: &ColumnType,
        table_name: &str,
    ) -> Result<()> {
        let type_name = match expected_type {
            ColumnType::Int | ColumnType::Integer => "integer",
            ColumnType::Float => "float",
            ColumnType::Text => "text",
            ColumnType::Vector => "vector",
            ColumnType::Decimal => "decimal",
        };

        let is_valid = match (expected_type, value) {
            // NULL is always valid
            (_, QueryParameterValue::Null) => true,
            // Type matching
            (ColumnType::Int | ColumnType::Integer, QueryParameterValue::Int(_)) => true,
            (ColumnType::Float, QueryParameterValue::Float(_)) => true,
            (ColumnType::Float, QueryParameterValue::Int(_)) => true, // Int can be promoted to float
            (ColumnType::Text, QueryParameterValue::String(_)) => true,
            (ColumnType::Vector, QueryParameterValue::Vector(_)) => true,
            (ColumnType::Decimal, QueryParameterValue::Float(_)) => true, // Float can be used for decimal
            (ColumnType::Decimal, QueryParameterValue::Int(_)) => true, // Int can be used for decimal
            // Allow JSON values for text columns (will be serialized)
            (ColumnType::Text, QueryParameterValue::Json(_)) => true,
            // Allow boolean values for text columns (will be converted)
            (ColumnType::Text, QueryParameterValue::Bool(_)) => true,
            // Allow binary data for text columns (will be hex encoded)
            (ColumnType::Text, QueryParameterValue::Binary(_)) => true,
            _ => false,
        };

        if !is_valid {
            let actual_type = match value {
                QueryParameterValue::Null => "null",
                QueryParameterValue::Bool(_) => "boolean",
                QueryParameterValue::Int(_) => "integer",
                QueryParameterValue::Float(_) => "float",
                QueryParameterValue::String(_) => "string",
                QueryParameterValue::Binary(_) => "binary",
                QueryParameterValue::Vector(_) => "vector",
                QueryParameterValue::Json(_) => "json",
            };

            return Err(FluentSqlError::TypeMismatch {
                expected: format!("{}::{}", type_name, column),
                actual: format!("{}::{}", actual_type, column),
            }.into());
        }

        // Additional validation for vectors
        if let (ColumnType::Vector, QueryParameterValue::Vector(vec)) = (expected_type, value) {
            if vec.is_empty() {
                return Err(FluentSqlError::InvalidVectorDimension {
                    expected: 1, // Minimum expected dimension
                    actual: 0,
                }.into());
            }
        }

        Ok(())
    }

    /// Extract inserted IDs from RETURNING result rows
    fn extract_inserted_ids(&self, rows: &[IndexMap<String, serde_json::Value>]) -> Vec<InsertedId> {
        let mut ids = Vec::new();

        for row in rows {
            // Look for common ID column names
            let id_value = row.get("id")
                .or_else(|| row.get("rowid"))
                .or_else(|| row.get("_id"));

            if let Some(value) = id_value {
                match value {
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            ids.push(InsertedId::Int(i));
                        }
                    }
                    serde_json::Value::String(s) => {
                        ids.push(InsertedId::String(s.clone()));
                    }
                    _ => {
                        // Skip non-ID values
                    }
                }
            }
        }

        ids
    }

    /// Validate column types against table schema
    fn validate_schema(&self, catalog: &Catalog, org_id: u64, table_name: &str) -> Result<()> {
        let table_def = catalog.get_table(org_id, table_name)?
            .ok_or_else(|| FluentSqlError::Catalog(
                format!("Table '{}' not found", table_name).into()
            ))?;

        let available_columns: HashMap<&str, &ColumnType> = table_def.columns
            .iter()
            .map(|col| (col.name.as_str(), &col.ty))
            .collect();

        // Validate single row values
        if !self.values.is_empty() {
            for (column, value) in &self.values {
                if !available_columns.contains_key(column.as_str()) {
                    return Err(FluentSqlError::InvalidColumn(
                        format!("Column '{}' not found in table '{}'", column, table_name)
                    ).into());
                }

                // Validate value type against column type
                if let Some(column_type) = available_columns.get(column.as_str()) {
                    Self::validate_value_type(column, value, column_type, table_name)?;
                }
            }
        }

        // Validate bulk rows
        for (row_idx, row) in self.bulk_rows.iter().enumerate() {
            for (column, value) in row {
                if !available_columns.contains_key(column.as_str()) {
                    return Err(FluentSqlError::InvalidColumn(
                        format!("Column '{}' in row {} not found in table '{}'",
                               column, row_idx, table_name)
                    ).into());
                }

                // Validate value type against column type
                if let Some(column_type) = available_columns.get(column.as_str()) {
                    Self::validate_value_type(column, value, column_type, table_name)?;
                }
            }
        }

        Ok(())
    }

    /// Convert the query to SQL string
    pub fn to_sql(&self) -> String {
        let mut sql = String::new();

        // Handle conflict strategy in INSERT clause
        match &self.conflict_strategy {
            ConflictStrategy::Ignore => sql.push_str("INSERT IGNORE INTO "),
            _ => sql.push_str("INSERT INTO "),
        }

        // Table name
        if let Some(ref table) = self.table_name {
            sql.push_str(table);
        }

        // Generate SQL based on single row vs bulk rows
        if !self.bulk_rows.is_empty() {
            self.generate_bulk_sql(&mut sql);
        } else if !self.values.is_empty() {
            self.generate_single_sql(&mut sql);
        }

        // Handle other conflict strategies
        match &self.conflict_strategy {
            ConflictStrategy::Replace(columns) => {
                sql.push_str(" ON DUPLICATE KEY UPDATE ");
                let updates: Vec<String> = columns.iter()
                    .map(|col| format!("{} = VALUES({})", col, col))
                    .collect();
                sql.push_str(&updates.join(", "));
            }
            ConflictStrategy::Update(columns) => {
                sql.push_str(" ON CONFLICT DO UPDATE SET ");
                let updates: Vec<String> = columns.iter()
                    .map(|col| format!("{} = EXCLUDED.{}", col, col))
                    .collect();
                sql.push_str(&updates.join(", "));
            }
            _ => {} // Ignore and Fail handled above or are default behavior
        }

        // RETURNING clause
        if !self.returning_columns.is_empty() {
            sql.push_str(" RETURNING ");
            sql.push_str(&self.returning_columns.join(", "));
        }

        sql
    }

    /// Generate SQL for single row insertion
    fn generate_single_sql(&self, sql: &mut String) {
        if self.values.is_empty() {
            return;
        }

        // Column names
        sql.push_str(" (");
        let columns: Vec<&String> = self.values.keys().collect();
        sql.push_str(&columns.iter().map(|c| c.as_str()).collect::<Vec<_>>().join(", "));
        sql.push_str(") VALUES (");

        // Values
        let value_placeholders: Vec<String> = self.values.values()
            .map(|value| self.value_to_sql(value))
            .collect();
        sql.push_str(&value_placeholders.join(", "));
        sql.push(')');
    }

    /// Generate SQL for bulk insertion
    fn generate_bulk_sql(&self, sql: &mut String) {
        if self.bulk_rows.is_empty() {
            return;
        }

        // Use columns from first row
        let first_row = &self.bulk_rows[0];
        let columns: Vec<&String> = first_row.keys().collect();

        // Column names
        sql.push_str(" (");
        sql.push_str(&columns.iter().map(|c| c.as_str()).collect::<Vec<_>>().join(", "));
        sql.push_str(") VALUES ");

        // Values for each row
        let row_sqls: Vec<String> = self.bulk_rows.iter()
            .map(|row| {
                let values: Vec<String> = columns.iter()
                    .map(|col| {
                        row.get(*col)
                            .map(|v| self.value_to_sql(v))
                            .unwrap_or_else(|| "NULL".to_string())
                    })
                    .collect();
                format!("({})", values.join(", "))
            })
            .collect();

        sql.push_str(&row_sqls.join(", "));
    }

    /// Convert a QueryParameterValue to SQL representation
    fn value_to_sql(&self, value: &QueryParameterValue) -> String {
        match value {
            QueryParameterValue::Null => "NULL".to_string(),
            QueryParameterValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            QueryParameterValue::Int(i) => i.to_string(),
            QueryParameterValue::Float(f) => f.to_string(),
            QueryParameterValue::String(s) => format!("'{}'", s.replace('\'', "''")), // Escape quotes
            QueryParameterValue::Binary(b) => format!("'{}'", hex::encode(b)),
            QueryParameterValue::Vector(v) => {
                let values: Vec<String> = v.iter().map(|f| f.to_string()).collect();
                format!("ARRAY[{}]", values.join(","))
            }
            QueryParameterValue::Json(j) => format!("'{}'", j.to_string().replace('\'', "''")),
        }
    }
}

/// Helper struct for chaining value assignments
pub struct InsertValueChain<'q> {
    builder: InsertQueryBuilder<'q, query_states::ColumnsSelected>,
}

impl<'q> InsertValueChain<'q> {
    /// Add a column-value pair
    pub fn value(mut self, column: &str, value: impl Into<QueryParameterValue>) -> Self {
        self.builder = self.builder.value(column, value);
        self
    }

    /// Finish chaining and return to normal builder
    pub fn done(self) -> InsertQueryBuilder<'q, query_states::ColumnsSelected> {
        self.builder
    }
}

// Backward compatibility implementation
impl InsertBuilder {
    pub(crate) fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            transaction_id: None,
            values: IndexMap::new(),
            bulk_rows: Vec::new(),
            conflict_strategy: ConflictStrategy::default(),
            returning_columns: Vec::new(),
        }
    }

    /// Specify values to insert
    pub fn values<T, V>(mut self, values: T) -> Self
    where
        T: IntoIterator<Item = (&'static str, V)>,
        V: Into<QueryParameterValue>,
    {
        // Clear any existing values and bulk rows
        self.values.clear();
        self.bulk_rows.clear();

        // Add values to the map
        for (column, value) in values {
            self.values.insert(column.to_string(), value.into());
        }

        self
    }

    /// Insert multiple rows
    pub fn bulk_values<T, V>(mut self, rows: Vec<T>) -> Self
    where
        T: IntoIterator<Item = (&'static str, V)>,
        V: Into<QueryParameterValue>,
    {
        // Clear existing values
        self.values.clear();
        self.bulk_rows.clear();

        // Convert each row
        for row_data in rows {
            let mut row = IndexMap::new();
            for (column, value) in row_data {
                row.insert(column.to_string(), value.into());
            }
            self.bulk_rows.push(row);
        }

        self
    }

    /// Handle conflicts with ON CONFLICT
    pub fn on_conflict(mut self, strategy: ConflictStrategy) -> Self {
        self.conflict_strategy = strategy;
        self
    }

    /// Return specific columns after insertion
    pub fn returning(mut self, columns: &[&str]) -> Self {
        self.returning_columns = columns.iter().map(|c| c.to_string()).collect();
        self
    }

    /// Add transaction context to this INSERT builder
    pub fn with_transaction(mut self, tx_id: String) -> Self {
        debug!("Adding transaction context to INSERT: {}", tx_id);
        self.transaction_id = Some(tx_id);
        self
    }

    /// Check if this builder has transaction context
    pub fn has_transaction(&self) -> bool {
        self.transaction_id.is_some()
    }

    /// Get the transaction ID if present
    pub fn transaction_id(&self) -> Option<&String> {
        self.transaction_id.as_ref()
    }

    /// Add individual column-value pairs
    pub fn value(mut self, column: &str, value: impl Into<QueryParameterValue>) -> Self {
        debug!("Adding single value to legacy builder: {} = <value>", column);

        // Clear bulk rows if switching to single row mode
        if !self.bulk_rows.is_empty() {
            debug!("Switching from bulk mode to single row mode in legacy builder");
            self.bulk_rows.clear();
        }

        self.values.insert(column.to_string(), value.into());
        self
    }

    /// Set conflict resolution strategy - IGNORE conflicts
    pub fn on_conflict_ignore(mut self) -> Self {
        debug!("Setting conflict strategy to IGNORE in legacy builder");
        self.conflict_strategy = ConflictStrategy::Ignore;
        self
    }

    /// Set conflict resolution strategy - REPLACE with specified columns
    pub fn on_conflict_replace(mut self, columns: &[&str]) -> Self {
        debug!("Setting conflict strategy to REPLACE with columns: {:?}", columns);
        self.conflict_strategy = ConflictStrategy::Replace(
            columns.iter().map(|c| c.to_string()).collect()
        );
        self
    }

    /// Set conflict resolution strategy - UPDATE specific columns on conflict
    pub fn on_conflict_update(mut self, update_columns: &[&str]) -> Self {
        debug!("Setting conflict strategy to UPDATE columns: {:?}", update_columns);
        self.conflict_strategy = ConflictStrategy::Update(
            update_columns.iter().map(|c| c.to_string()).collect()
        );
        self
    }

    /// Return all columns after insertion
    pub fn returning_all(mut self) -> Self {
        debug!("Setting RETURNING * in legacy builder");
        self.returning_columns = vec!["*".to_string()];
        self
    }

    /// Execute this INSERT query within a transaction context
    pub fn execute_in_transaction<'store, 'catalog>(
        &self,
        ctx: &mut TransactionContext<'store, 'catalog>,
    ) -> Result<QueryResult> {
        let sql = self.to_sql();
        let params = self.get_parameters();

        debug!("Executing INSERT in transaction: {}", sql);

        // Add operation to the transaction context
        ctx.add_operation(sql, params)?;

        // Return a placeholder result - actual result will be available after commit
        Ok(QueryResult::Insert(InsertResult {
            inserted_count: 0, // Will be determined after commit
            inserted_ids: Vec::new(),
            stats: ExecutionStats::default(),
        }))
    }

    // Getter methods for ExecutionLayer integration
    /// Get the table name
    pub fn table_name(&self) -> Option<&String> {
        Some(&self.table)
    }

    /// Get the values for INSERT
    pub fn values_list(&self) -> Vec<&IndexMap<String, QueryParameterValue>> {
        if !self.bulk_rows.is_empty() {
            self.bulk_rows.iter().collect()
        } else if !self.values.is_empty() {
            vec![&self.values]
        } else {
            vec![]
        }
    }

    /// Get WHERE clause (not applicable for INSERT)
    pub fn where_clause(&self) -> &crate::where_clause::WhereClauseBuilder {
        static EMPTY: std::sync::OnceLock<crate::where_clause::WhereClauseBuilder> = std::sync::OnceLock::new();
        EMPTY.get_or_init(|| crate::where_clause::WhereClauseBuilder::new())
    }

    /// Get set clauses (not applicable for INSERT)
    pub fn set_clauses(&self) -> &[(String, QueryParameterValue)] {
        &[]
    }

    /// Generate SQL for single row insertion
    fn generate_single_sql(&self, sql: &mut String) {
        if self.values.is_empty() {
            return;
        }

        // Column names
        sql.push_str(" (");
        let columns: Vec<&String> = self.values.keys().collect();
        sql.push_str(&columns.iter().map(|c| c.as_str()).collect::<Vec<_>>().join(", "));
        sql.push_str(") VALUES (");

        // Values
        let value_placeholders: Vec<String> = self.values.values()
            .map(|value| self.value_to_sql(value))
            .collect();
        sql.push_str(&value_placeholders.join(", "));
        sql.push(')');
    }

    /// Generate SQL for bulk insertion
    fn generate_bulk_sql(&self, sql: &mut String) {
        if self.bulk_rows.is_empty() {
            return;
        }

        // Use columns from first row
        let first_row = &self.bulk_rows[0];
        let columns: Vec<&String> = first_row.keys().collect();

        // Column names
        sql.push_str(" (");
        sql.push_str(&columns.iter().map(|c| c.as_str()).collect::<Vec<_>>().join(", "));
        sql.push_str(") VALUES ");

        // Values for each row
        let row_sqls: Vec<String> = self.bulk_rows.iter()
            .map(|row| {
                let values: Vec<String> = columns.iter()
                    .map(|col| {
                        row.get(*col)
                            .map(|v| self.value_to_sql(v))
                            .unwrap_or_else(|| "NULL".to_string())
                    })
                    .collect();
                format!("({})", values.join(", "))
            })
            .collect();

        sql.push_str(&row_sqls.join(", "));
    }

    /// Convert a QueryParameterValue to SQL representation
    fn value_to_sql(&self, value: &QueryParameterValue) -> String {
        match value {
            QueryParameterValue::Null => "NULL".to_string(),
            QueryParameterValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            QueryParameterValue::Int(i) => i.to_string(),
            QueryParameterValue::Float(f) => f.to_string(),
            QueryParameterValue::String(s) => format!("'{}'", s.replace('\'', "''")), // Escape quotes
            QueryParameterValue::Binary(b) => format!("'{}'", hex::encode(b)),
            QueryParameterValue::Vector(v) => {
                let values: Vec<String> = v.iter().map(|f| f.to_string()).collect();
                format!("ARRAY[{}]", values.join(","))
            },
            QueryParameterValue::Json(j) => format!("'{}'", j.to_string().replace('\'', "''"))
        }
    }
}

/// Stub QueryContext for compilation
pub struct InsertQueryContext;

impl QueryContext for InsertQueryContext {
    fn store(&mut self) -> &mut Store {
        unimplemented!("InsertQueryBuilder uses direct execution, not this context")
    }

    fn begin_transaction(&mut self) -> Result<()> {
        unimplemented!("InsertQueryBuilder uses direct execution, not this context")
    }

    fn commit_transaction(&mut self) -> Result<()> {
        unimplemented!("InsertQueryBuilder uses direct execution, not this context")
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        unimplemented!("InsertQueryBuilder uses direct execution, not this context")
    }

    fn in_transaction(&self) -> bool {
        false
    }
}

/// QueryBuilder trait implementation for InsertQueryBuilder
impl<'q> QueryBuilder for InsertQueryBuilder<'q, query_states::Executable> {
    type Context = InsertQueryContext;

    fn execute(self, _ctx: &mut Self::Context) -> Result<QueryResult> {
        // Execute using the built-in execute method
        match self.execute() {
            Ok(insert_result) => Ok(QueryResult::Insert(insert_result)),
            Err(e) => Err(e),
        }
    }

    fn to_sql(&self) -> String {
        self.to_sql()
    }

    fn get_parameters(&self) -> Vec<QueryParameter> {
        self.parameters.clone()
    }

    fn validate(&self) -> Result<()> {
        if self.table_name.is_none() {
            return Err(anyhow!("Target table is required").into());
        }

        if self.values.is_empty() && self.bulk_rows.is_empty() {
            return Err(anyhow!("At least one row of values must be provided").into());
        }

        Ok(())
    }
}

/// Backward compatibility - QueryBuilder trait implementation for InsertBuilder
impl QueryBuilder for InsertBuilder {
    type Context = InsertQueryContext;

    fn execute(self, _ctx: &mut Self::Context) -> Result<QueryResult> {
        // For backward compatibility, this is a stub implementation
        Ok(QueryResult::Insert(InsertResult {
            inserted_count: 0,
            inserted_ids: vec![],
            stats: ExecutionStats::default(),
        }))
    }

    fn to_sql(&self) -> String {
        let mut sql = String::new();

        // Handle conflict strategy in INSERT clause
        match &self.conflict_strategy {
            ConflictStrategy::Ignore => sql.push_str("INSERT IGNORE INTO "),
            _ => sql.push_str("INSERT INTO "),
        }

        // Table name
        sql.push_str(&self.table);

        // Generate SQL based on single row vs bulk rows
        if !self.bulk_rows.is_empty() {
            self.generate_bulk_sql(&mut sql);
        } else if !self.values.is_empty() {
            self.generate_single_sql(&mut sql);
        }

        // Handle other conflict strategies
        match &self.conflict_strategy {
            ConflictStrategy::Replace(columns) => {
                sql.push_str(" ON DUPLICATE KEY UPDATE ");
                let updates: Vec<String> = columns.iter()
                    .map(|col| format!("{} = VALUES({})", col, col))
                    .collect();
                sql.push_str(&updates.join(", "));
            }
            ConflictStrategy::Update(columns) => {
                sql.push_str(" ON CONFLICT DO UPDATE SET ");
                let updates: Vec<String> = columns.iter()
                    .map(|col| format!("{} = EXCLUDED.{}", col, col))
                    .collect();
                sql.push_str(&updates.join(", "));
            }
            _ => {} // Ignore and Fail handled above or are default behavior
        }

        // RETURNING clause
        if !self.returning_columns.is_empty() {
            sql.push_str(" RETURNING ");
            sql.push_str(&self.returning_columns.join(", "));
        }

        sql
    }

    fn get_parameters(&self) -> Vec<QueryParameter> {
        Vec::new()
    }

    fn validate(&self) -> Result<()> {
        if self.values.is_empty() && self.bulk_rows.is_empty() {
            return Err(anyhow!("At least one row of values must be provided").into());
        }

        // Validate bulk rows have consistent columns
        if !self.bulk_rows.is_empty() {
            let first_row_columns: Vec<&String> = self.bulk_rows[0].keys().collect();
            for (idx, row) in self.bulk_rows.iter().enumerate().skip(1) {
                let row_columns: Vec<&String> = row.keys().collect();
                if first_row_columns.len() != row_columns.len() ||
                   !first_row_columns.iter().all(|c| row_columns.contains(c)) {
                    return Err(anyhow!(
                        "Bulk row {} has inconsistent columns compared to first row", idx
                    ).into());
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::QueryParameterValue;
    use std::collections::HashMap;

    #[test]
    fn test_insert_query_builder_creation() {
        let builder = InsertQueryBuilder::new();
        assert!(builder.table_name.is_none());
        assert!(builder.values.is_empty());
        assert!(builder.bulk_rows.is_empty());
        assert!(matches!(builder.conflict_strategy, ConflictStrategy::Fail));
    }

    #[test]
    fn test_insert_query_builder_into_clause() {
        let builder = InsertQueryBuilder::new()
            .into("users");

        assert_eq!(builder.table_name, Some("users".to_string()));
    }

    #[test]
    fn test_insert_query_builder_single_values() {
        let mut values = HashMap::new();
        values.insert("name", "Alice");
        values.insert("age", 30i64);
        values.insert("active", true);

        let builder = InsertQueryBuilder::new()
            .into("users")
            .values(values);

        assert_eq!(builder.values.len(), 3);
        assert!(matches!(
            builder.values.get("name").unwrap(),
            QueryParameterValue::String(_)
        ));
        assert!(matches!(
            builder.values.get("age").unwrap(),
            QueryParameterValue::Int(30)
        ));
        assert!(matches!(
            builder.values.get("active").unwrap(),
            QueryParameterValue::Bool(true)
        ));
    }

    #[test]
    fn test_insert_query_builder_bulk_values() {
        let mut row1 = HashMap::new();
        row1.insert("name", "Alice");
        row1.insert("age", 30i64);

        let mut row2 = HashMap::new();
        row2.insert("name", "Bob");
        row2.insert("age", 25i64);

        let builder = InsertQueryBuilder::new()
            .into("users")
            .bulk_values(vec![row1, row2]);

        assert_eq!(builder.bulk_rows.len(), 2);
        assert!(builder.values.is_empty()); // Should be empty when using bulk

        let first_row = &builder.bulk_rows[0];
        assert_eq!(first_row.len(), 2);
        assert!(matches!(
            first_row.get("name").unwrap(),
            QueryParameterValue::String(_)
        ));
    }

    #[test]
    fn test_insert_query_builder_individual_values() {
        let builder = InsertQueryBuilder::new()
            .into("users")
            .value("name", "Alice")
            .value("age", 30i64)
            .value("active", true);

        assert_eq!(builder.values.len(), 3);
        assert!(matches!(
            builder.values.get("name").unwrap(),
            QueryParameterValue::String(_)
        ));
    }

    #[test]
    fn test_insert_query_builder_conflict_strategies() {
        let builder = InsertQueryBuilder::new()
            .into("users")
            .value("name", "Alice")
            .on_conflict_ignore();

        assert!(matches!(builder.conflict_strategy, ConflictStrategy::Ignore));

        let builder = InsertQueryBuilder::new()
            .into("users")
            .value("name", "Alice")
            .on_conflict_replace(&["name", "email"]);

        assert!(matches!(
            builder.conflict_strategy,
            ConflictStrategy::Replace(ref cols) if cols.len() == 2
        ));

        let builder = InsertQueryBuilder::new()
            .into("users")
            .value("name", "Alice")
            .on_conflict_update(&["updated_at"]);

        assert!(matches!(
            builder.conflict_strategy,
            ConflictStrategy::Update(ref cols) if cols.len() == 1
        ));
    }

    #[test]
    fn test_insert_query_builder_returning() {
        let builder = InsertQueryBuilder::new()
            .into("users")
            .value("name", "Alice")
            .returning(&["id", "name"]);

        assert_eq!(builder.returning_columns, vec!["id", "name"]);

        let builder = InsertQueryBuilder::new()
            .into("users")
            .value("name", "Alice")
            .returning_all();

        assert_eq!(builder.returning_columns, vec!["*"]);
    }

    #[test]
    fn test_insert_query_builder_build() {
        let result = InsertQueryBuilder::new()
            .into("users")
            .value("name", "Alice")
            .build();

        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_query_builder_build_missing_table() {
        let result = InsertQueryBuilder::new()
            .value("name", "Alice")
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Target table is required"));
    }

    #[test]
    fn test_insert_query_builder_build_missing_values() {
        let result = InsertQueryBuilder::new()
            .into("users")
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("At least one row of values"));
    }

    #[test]
    fn test_insert_query_builder_bulk_consistency_validation() {
        let mut row1 = HashMap::new();
        row1.insert("name", "Alice");
        row1.insert("age", 30i64);

        let mut row2 = HashMap::new();
        row2.insert("name", "Bob");
        row2.insert("email", "bob@example.com"); // Different columns

        let result = InsertQueryBuilder::new()
            .into("users")
            .bulk_values(vec![row1, row2])
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("inconsistent columns"));
    }

    #[test]
    fn test_insert_query_builder_to_sql_single_row() {
        let builder = InsertQueryBuilder::new()
            .into("users")
            .value("name", "Alice")
            .value("age", 30i64)
            .build()
            .unwrap();

        let sql = builder.to_sql();
        assert!(sql.starts_with("INSERT INTO users"));
        assert!(sql.contains("(name, age)"));
        assert!(sql.contains("VALUES"));
        assert!(sql.contains("'Alice'"));
        assert!(sql.contains("30"));
    }

    #[test]
    fn test_insert_query_builder_to_sql_bulk_rows() {
        let mut row1 = HashMap::new();
        row1.insert("name", "Alice");
        row1.insert("age", 30i64);

        let mut row2 = HashMap::new();
        row2.insert("name", "Bob");
        row2.insert("age", 25i64);

        let builder = InsertQueryBuilder::new()
            .into("users")
            .bulk_values(vec![row1, row2])
            .build()
            .unwrap();

        let sql = builder.to_sql();
        assert!(sql.starts_with("INSERT INTO users"));
        assert!(sql.contains("(name, age)"));
        assert!(sql.contains("VALUES"));
        assert!(sql.contains("'Alice'"));
        assert!(sql.contains("'Bob'"));
        assert!(sql.contains("30"));
        assert!(sql.contains("25"));
    }

    #[test]
    fn test_insert_query_builder_to_sql_ignore_conflict() {
        let builder = InsertQueryBuilder::new()
            .into("users")
            .value("name", "Alice")
            .on_conflict_ignore()
            .build()
            .unwrap();

        let sql = builder.to_sql();
        assert!(sql.starts_with("INSERT IGNORE INTO users"));
    }

    #[test]
    fn test_insert_query_builder_to_sql_replace_conflict() {
        let builder = InsertQueryBuilder::new()
            .into("users")
            .value("name", "Alice")
            .on_conflict_replace(&["name", "updated_at"])
            .build()
            .unwrap();

        let sql = builder.to_sql();
        assert!(sql.contains("ON DUPLICATE KEY UPDATE"));
        assert!(sql.contains("name = VALUES(name)"));
        assert!(sql.contains("updated_at = VALUES(updated_at)"));
    }

    #[test]
    fn test_insert_query_builder_to_sql_update_conflict() {
        let builder = InsertQueryBuilder::new()
            .into("users")
            .value("name", "Alice")
            .on_conflict_update(&["name"])
            .build()
            .unwrap();

        let sql = builder.to_sql();
        assert!(sql.contains("ON CONFLICT DO UPDATE SET"));
        assert!(sql.contains("name = EXCLUDED.name"));
    }

    #[test]
    fn test_insert_query_builder_to_sql_returning() {
        let builder = InsertQueryBuilder::new()
            .into("users")
            .value("name", "Alice")
            .returning(&["id", "name"])
            .build()
            .unwrap();

        let sql = builder.to_sql();
        assert!(sql.ends_with(" RETURNING id, name"));
    }

    #[test]
    fn test_insert_query_builder_to_sql_returning_all() {
        let builder = InsertQueryBuilder::new()
            .into("users")
            .value("name", "Alice")
            .returning_all()
            .build()
            .unwrap();

        let sql = builder.to_sql();
        assert!(sql.ends_with(" RETURNING *"));
    }

    #[test]
    fn test_insert_value_chain() {
        let builder = InsertQueryBuilder::new()
            .into("users")
            .values_chain()
            .value("name", "Alice")
            .value("age", 30i64)
            .value("active", true)
            .done();

        assert_eq!(builder.values.len(), 3);
        assert!(matches!(
            builder.values.get("name").unwrap(),
            QueryParameterValue::String(_)
        ));
    }

    #[test]
    fn test_query_parameter_value_conversions() {
        let val_str: QueryParameterValue = "test".into();
        assert!(matches!(val_str, QueryParameterValue::String(_)));

        let val_int: QueryParameterValue = 42i64.into();
        assert!(matches!(val_int, QueryParameterValue::Int(42)));

        let val_bool: QueryParameterValue = true.into();
        assert!(matches!(val_bool, QueryParameterValue::Bool(true)));

        let val_float: QueryParameterValue = 3.14f64.into();
        assert!(matches!(val_float, QueryParameterValue::Float(_)));

        let val_vec: QueryParameterValue = vec![1.0f32, 2.0f32, 3.0f32].into();
        assert!(matches!(val_vec, QueryParameterValue::Vector(_)));
    }

    #[test]
    fn test_value_to_sql_conversion() {
        let builder = InsertQueryBuilder::new()
            .into("test")
            .value("name", "Alice")
            .build()
            .unwrap();

        // Test different value types
        assert_eq!(builder.value_to_sql(&QueryParameterValue::Null), "NULL");
        assert_eq!(builder.value_to_sql(&QueryParameterValue::Bool(true)), "TRUE");
        assert_eq!(builder.value_to_sql(&QueryParameterValue::Bool(false)), "FALSE");
        assert_eq!(builder.value_to_sql(&QueryParameterValue::Int(42)), "42");
        assert_eq!(builder.value_to_sql(&QueryParameterValue::Float(3.14)), "3.14");
        assert_eq!(builder.value_to_sql(&QueryParameterValue::String("test".to_string())), "'test'");

        // Test quote escaping
        assert_eq!(
            builder.value_to_sql(&QueryParameterValue::String("test'quote".to_string())),
            "'test''quote'"
        );

        // Test vector conversion
        let vector_val = QueryParameterValue::Vector(vec![1.0, 2.0, 3.0]);
        assert_eq!(builder.value_to_sql(&vector_val), "ARRAY[1,2,3]");

        // Test binary conversion
        let binary_val = QueryParameterValue::Binary(vec![0x01, 0x02, 0x03]);
        assert_eq!(builder.value_to_sql(&binary_val), "'010203'");
    }

    #[test]
    fn test_conflict_strategy_default() {
        let strategy = ConflictStrategy::default();
        assert!(matches!(strategy, ConflictStrategy::Fail));
    }

    #[test]
    fn test_extracted_inserted_ids() {
        let builder = InsertQueryBuilder::new()
            .into("test")
            .value("name", "Alice")
            .build()
            .unwrap();

        // Test ID extraction from mock rows
        let mut row1 = IndexMap::new();
        row1.insert("id".to_string(), serde_json::Value::Number(serde_json::Number::from(1i64)));
        row1.insert("name".to_string(), serde_json::Value::String("Alice".to_string()));

        let mut row2 = IndexMap::new();
        row2.insert("rowid".to_string(), serde_json::Value::String("uuid-123".to_string()));
        row2.insert("name".to_string(), serde_json::Value::String("Bob".to_string()));

        let rows = vec![row1, row2];
        let ids = builder.extract_inserted_ids(&rows);

        assert_eq!(ids.len(), 2);
        assert!(matches!(ids[0], InsertedId::Int(1)));
        assert!(matches!(ids[1], InsertedId::String(ref s) if s == "uuid-123"));
    }

    #[test]
    fn test_insert_builder_backward_compatibility() {
        let builder = InsertBuilder::new("users");
        assert_eq!(builder.table, "users");

        let sql = builder.to_sql();
        assert_eq!(sql, "INSERT INTO users");

        assert!(builder.validate().is_err()); // Should require values
    }

    #[test]
    fn test_legacy_insert_builder_values() {
        let values = vec![("name", "Alice"), ("age", "30")];
        let builder = InsertBuilder::new("users")
            .values(values);

        assert_eq!(builder.values.len(), 2);
        assert!(builder.values.contains_key("name"));
        assert!(builder.values.contains_key("age"));

        let sql = builder.to_sql();
        assert!(sql.contains("INSERT INTO users"));
        assert!(sql.contains("(name, age)"));
        assert!(sql.contains("VALUES"));
        assert!(sql.contains("'Alice'"));
        assert!(sql.contains("'30'"));
    }

    #[test]
    fn test_legacy_insert_builder_bulk_values() {
        let row1 = vec![("name", "Alice"), ("age", "30")];
        let row2 = vec![("name", "Bob"), ("age", "25")];
        let builder = InsertBuilder::new("users")
            .bulk_values(vec![row1, row2]);

        assert_eq!(builder.bulk_rows.len(), 2);
        assert!(builder.values.is_empty());

        let sql = builder.to_sql();
        assert!(sql.contains("INSERT INTO users"));
        assert!(sql.contains("(name, age)"));
        assert!(sql.contains("VALUES"));
        assert!(sql.contains("'Alice'"));
        assert!(sql.contains("'Bob'"));
    }

    #[test]
    fn test_legacy_insert_builder_conflict_strategies() {
        let builder = InsertBuilder::new("users")
            .value("name", "Alice")
            .on_conflict_ignore();

        assert!(matches!(builder.conflict_strategy, ConflictStrategy::Ignore));
        let sql = builder.to_sql();
        assert!(sql.starts_with("INSERT IGNORE INTO"));

        let builder = InsertBuilder::new("users")
            .value("name", "Alice")
            .on_conflict_replace(&["name", "updated_at"]);

        assert!(matches!(
            builder.conflict_strategy,
            ConflictStrategy::Replace(ref cols) if cols.len() == 2
        ));
        let sql = builder.to_sql();
        assert!(sql.contains("ON DUPLICATE KEY UPDATE"));

        let builder = InsertBuilder::new("users")
            .value("name", "Alice")
            .on_conflict_update(&["updated_at"]);

        assert!(matches!(
            builder.conflict_strategy,
            ConflictStrategy::Update(ref cols) if cols.len() == 1
        ));
        let sql = builder.to_sql();
        assert!(sql.contains("ON CONFLICT DO UPDATE SET"));
    }

    #[test]
    fn test_legacy_insert_builder_returning() {
        let builder = InsertBuilder::new("users")
            .value("name", "Alice")
            .returning(&["id", "name"]);

        assert_eq!(builder.returning_columns, vec!["id", "name"]);
        let sql = builder.to_sql();
        assert!(sql.ends_with(" RETURNING id, name"));

        let builder = InsertBuilder::new("users")
            .value("name", "Alice")
            .returning_all();

        assert_eq!(builder.returning_columns, vec!["*"]);
        let sql = builder.to_sql();
        assert!(sql.ends_with(" RETURNING *"));
    }

    #[test]
    fn test_legacy_insert_builder_comprehensive() {
        let builder = InsertBuilder::new("users")
            .value("name", "Alice")
            .value("email", "alice@example.com")
            .value("age", 30i64)
            .on_conflict_update(&["email", "updated_at"])
            .returning(&["id", "name"]);

        let sql = builder.to_sql();
        assert!(sql.starts_with("INSERT INTO users"));
        assert!(sql.contains("(name, email, age)"));
        assert!(sql.contains("VALUES"));
        assert!(sql.contains("'Alice'"));
        assert!(sql.contains("'alice@example.com'"));
        assert!(sql.contains("30"));
        assert!(sql.contains("ON CONFLICT DO UPDATE SET"));
        assert!(sql.contains("email = EXCLUDED.email"));
        assert!(sql.contains("updated_at = EXCLUDED.updated_at"));
        assert!(sql.contains("RETURNING id, name"));
    }

    #[test]
    fn test_type_validation_success() {
        // Test successful type validation
        let result = InsertQueryBuilder::<query_states::Executable>::validate_value_type(
            "age",
            &QueryParameterValue::Int(30),
            &ColumnType::Int,
            "users"
        );
        assert!(result.is_ok());

        let result = InsertQueryBuilder::<query_states::Executable>::validate_value_type(
            "name",
            &QueryParameterValue::String("Alice".to_string()),
            &ColumnType::Text,
            "users"
        );
        assert!(result.is_ok());

        let result = InsertQueryBuilder::<query_states::Executable>::validate_value_type(
            "embedding",
            &QueryParameterValue::Vector(vec![1.0, 2.0, 3.0]),
            &ColumnType::Vector,
            "users"
        );
        assert!(result.is_ok());

        let result = InsertQueryBuilder::<query_states::Executable>::validate_value_type(
            "score",
            &QueryParameterValue::Float(3.14),
            &ColumnType::Float,
            "users"
        );
        assert!(result.is_ok());

        // Test type promotion - Int to Float
        let result = InsertQueryBuilder::<query_states::Executable>::validate_value_type(
            "score",
            &QueryParameterValue::Int(42),
            &ColumnType::Float,
            "users"
        );
        assert!(result.is_ok());

        // Test NULL is always valid
        let result = InsertQueryBuilder::<query_states::Executable>::validate_value_type(
            "age",
            &QueryParameterValue::Null,
            &ColumnType::Int,
            "users"
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_type_validation_failure() {
        // Test type mismatch errors
        let result = InsertQueryBuilder::<query_states::Executable>::validate_value_type(
            "age",
            &QueryParameterValue::String("not a number".to_string()),
            &ColumnType::Int,
            "users"
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Type mismatch"));

        let result = InsertQueryBuilder::<query_states::Executable>::validate_value_type(
            "embedding",
            &QueryParameterValue::String("not a vector".to_string()),
            &ColumnType::Vector,
            "users"
        );
        assert!(result.is_err());

        // Test empty vector validation
        let result = InsertQueryBuilder::<query_states::Executable>::validate_value_type(
            "embedding",
            &QueryParameterValue::Vector(vec![]),
            &ColumnType::Vector,
            "users"
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("InvalidVectorDimension"));
    }

    #[test]
    fn test_legacy_insert_builder_validation() {
        // Test successful validation
        let builder = InsertBuilder::new("users")
            .value("name", "Alice");
        assert!(builder.validate().is_ok());

        // Test empty validation failure
        let builder = InsertBuilder::new("users");
        assert!(builder.validate().is_err());

        // Test consistent bulk validation
        let row1 = vec![("name", "Alice"), ("age", "30")];
        let row2 = vec![("name", "Bob"), ("age", "25")];
        let builder = InsertBuilder::new("users")
            .bulk_values(vec![row1, row2]);
        assert!(builder.validate().is_ok());

        // Test inconsistent bulk validation failure
        let row1 = vec![("name", "Alice"), ("age", "30")];
        let row2 = vec![("name", "Bob"), ("email", "bob@example.com")]; // Different columns
        let builder = InsertBuilder::new("users")
            .bulk_values(vec![row1, row2]);
        assert!(builder.validate().is_err());
        assert!(builder.validate().unwrap_err().to_string().contains("inconsistent columns"));
    }

    #[test]
    fn test_legacy_insert_builder_values_list() {
        // Test single row
        let builder = InsertBuilder::new("users")
            .value("name", "Alice");
        let values_list = builder.values_list();
        assert_eq!(values_list.len(), 1);
        assert_eq!(values_list[0].len(), 1);
        assert!(values_list[0].contains_key("name"));

        // Test bulk rows
        let row1 = vec![("name", "Alice")];
        let row2 = vec![("name", "Bob")];
        let builder = InsertBuilder::new("users")
            .bulk_values(vec![row1, row2]);
        let values_list = builder.values_list();
        assert_eq!(values_list.len(), 2);

        // Test empty
        let builder = InsertBuilder::new("users");
        let values_list = builder.values_list();
        assert_eq!(values_list.len(), 0);
    }

    #[test]
    fn test_query_builder_trait_implementation() {
        let builder = InsertQueryBuilder::new()
            .into("users")
            .value("name", "Alice")
            .build()
            .unwrap();

        // Test trait methods
        let sql = builder.to_sql();
        assert!(sql.contains("INSERT INTO users"));

        let params = builder.get_parameters();
        assert!(params.is_empty()); // No parameters in our simple implementation

        assert!(builder.validate().is_ok());
    }

    #[test]
    fn test_switching_from_bulk_to_single() {
        let mut row1 = HashMap::new();
        row1.insert("name", "Alice");

        let builder = InsertQueryBuilder::new()
            .into("users")
            .bulk_values(vec![row1])
            .value("age", 30i64); // This should clear bulk_rows

        assert!(builder.bulk_rows.is_empty());
        assert!(!builder.values.is_empty());
        assert!(builder.values.contains_key("age"));
    }

    #[test]
    fn test_comprehensive_insert_query() {
        let builder = InsertQueryBuilder::new()
            .into("users")
            .value("name", "Alice Johnson")
            .value("email", "alice@example.com")
            .value("age", 30i64)
            .value("active", true)
            .on_conflict_update(&["email", "updated_at"])
            .returning(&["id", "name", "created_at"])
            .build()
            .unwrap();

        let sql = builder.to_sql();

        assert!(sql.starts_with("INSERT INTO users"));
        assert!(sql.contains("(name, email, age, active)"));
        assert!(sql.contains("VALUES"));
        assert!(sql.contains("'Alice Johnson'"));
        assert!(sql.contains("'alice@example.com'"));
        assert!(sql.contains("30"));
        assert!(sql.contains("TRUE"));
        assert!(sql.contains("ON CONFLICT DO UPDATE SET"));
        assert!(sql.contains("email = EXCLUDED.email"));
        assert!(sql.contains("updated_at = EXCLUDED.updated_at"));
        assert!(sql.contains("RETURNING id, name, created_at"));
    }
}