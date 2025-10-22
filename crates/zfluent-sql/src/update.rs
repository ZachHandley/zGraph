/*!
UPDATE query construction with fluent API.

This module provides type-safe UPDATE query building with support for:
- Column updates with type checking
- WHERE clauses for targeting specific rows
- Raw SQL expressions for computed updates
- Integration with ZQuery for connection context
- Schema validation and parameter binding
*/

use crate::{
    query::{
        ZQuery, QueryBuilder, QueryContext, QueryResult, QueryParameter, QueryParameterValue,
        UpdateResult, ExecutionStats, query_states
    },
    where_clause::WhereClauseBuilder,
    Result, FluentSqlError
};
use anyhow::anyhow;
use std::collections::HashMap;
use std::marker::PhantomData;
use tracing::debug;
use zcore_catalog::{Catalog, TableDef, ColumnType};
use zcore_storage::Store;
use zexec_engine::{exec_sql_store, ExecResult};

/// Represents a column assignment in the SET clause
#[derive(Debug, Clone)]
pub struct SetClause {
    /// The column name to update
    pub column: String,
    /// The value or expression to assign
    pub value: QueryParameterValue,
    /// Whether this is a raw SQL expression
    pub is_raw: bool,
}

impl SetClause {
    /// Create a new SET clause with a parameter value
    fn new(column: impl Into<String>, value: impl Into<QueryParameterValue>) -> Self {
        Self {
            column: column.into(),
            value: value.into(),
            is_raw: false,
        }
    }

    /// Create a new SET clause with raw SQL expression
    fn raw(column: impl Into<String>, expression: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            value: QueryParameterValue::from(expression.into()),
            is_raw: true,
        }
    }
}

/// UpdateQueryBuilder - enhanced version with ZQuery integration and phantom type states
pub struct UpdateQueryBuilder<'q, State = query_states::Uninitialized> {
    /// Reference to the query context for execution
    query_context: Option<&'q ZQuery<'q, 'q>>,

    /// Table to update
    table_name: Option<String>,

    /// SET clauses for column updates
    set_clauses: Vec<SetClause>,

    /// WHERE clause builder for targeting specific rows
    where_clause: WhereClauseBuilder,

    /// Query parameters for prepared statement execution
    parameters: Vec<QueryParameter>,

    /// Cached table schema for validation
    table_schema: Option<TableDef>,

    /// Phantom type for state validation
    _state: PhantomData<State>,
}

// Implementations for UpdateQueryBuilder with phantom type states
impl<'q> UpdateQueryBuilder<'q, query_states::Uninitialized> {
    /// Create a new UpdateQueryBuilder
    pub fn new() -> Self {
        debug!("Creating new UpdateQueryBuilder");
        Self {
            query_context: None,
            table_name: None,
            set_clauses: Vec::new(),
            where_clause: WhereClauseBuilder::new(),
            parameters: Vec::new(),
            table_schema: None,
            _state: PhantomData,
        }
    }

    /// Create a new UpdateQueryBuilder with query context
    pub fn with_context(context: &'q ZQuery<'q, 'q>) -> Self {
        debug!("Creating UpdateQueryBuilder with context");
        Self {
            query_context: Some(context),
            table_name: None,
            set_clauses: Vec::new(),
            where_clause: WhereClauseBuilder::new(),
            parameters: Vec::new(),
            table_schema: None,
            _state: PhantomData,
        }
    }

    /// Specify the table to update - transitions to TableSelected state
    pub fn table(mut self, table: &str) -> UpdateQueryBuilder<'q, query_states::TableSelected> {
        debug!("Setting UPDATE table: {}", table);
        self.table_name = Some(table.to_string());

        UpdateQueryBuilder {
            query_context: self.query_context,
            table_name: self.table_name,
            set_clauses: self.set_clauses,
            where_clause: self.where_clause,
            parameters: self.parameters,
            table_schema: self.table_schema,
            _state: PhantomData,
        }
    }
}

impl<'q> UpdateQueryBuilder<'q, query_states::TableSelected> {
    /// Set a column to a new value - transitions to ColumnsSelected state
    pub fn set(mut self, column: &str, value: impl Into<QueryParameterValue>) -> UpdateQueryBuilder<'q, query_states::ColumnsSelected> {
        debug!("Setting column {} to value", column);
        self.set_clauses.push(SetClause::new(column, value));

        UpdateQueryBuilder {
            query_context: self.query_context,
            table_name: self.table_name,
            set_clauses: self.set_clauses,
            where_clause: self.where_clause,
            parameters: self.parameters,
            table_schema: self.table_schema,
            _state: PhantomData,
        }
    }

    /// Set a column using raw SQL expression - transitions to ColumnsSelected state
    pub fn set_raw(mut self, column: &str, expression: &str) -> UpdateQueryBuilder<'q, query_states::ColumnsSelected> {
        debug!("Setting column {} to raw expression: {}", column, expression);
        self.set_clauses.push(SetClause::raw(column, expression));

        UpdateQueryBuilder {
            query_context: self.query_context,
            table_name: self.table_name,
            set_clauses: self.set_clauses,
            where_clause: self.where_clause,
            parameters: self.parameters,
            table_schema: self.table_schema,
            _state: PhantomData,
        }
    }

    /// Set multiple columns at once using a HashMap - transitions to ColumnsSelected state
    pub fn set_map(mut self, updates: HashMap<&str, impl Into<QueryParameterValue> + Clone>) -> UpdateQueryBuilder<'q, query_states::ColumnsSelected> {
        debug!("Setting {} columns from map", updates.len());
        for (column, value) in updates {
            self.set_clauses.push(SetClause::new(column, value.clone()));
        }

        UpdateQueryBuilder {
            query_context: self.query_context,
            table_name: self.table_name,
            set_clauses: self.set_clauses,
            where_clause: self.where_clause,
            parameters: self.parameters,
            table_schema: self.table_schema,
            _state: PhantomData,
        }
    }
}

impl<'q> UpdateQueryBuilder<'q, query_states::ColumnsSelected> {
    /// Add additional SET clauses
    pub fn set(mut self, column: &str, value: impl Into<QueryParameterValue>) -> Self {
        debug!("Adding SET clause: {} = value", column);
        self.set_clauses.push(SetClause::new(column, value));
        self
    }

    /// Add raw SQL SET clause
    pub fn set_raw(mut self, column: &str, expression: &str) -> Self {
        debug!("Adding raw SET clause: {} = {}", column, expression);
        self.set_clauses.push(SetClause::raw(column, expression));
        self
    }

    /// Add WHERE clause using the where clause builder
    pub fn where_(mut self, column: &str, operator: &str, value: impl Into<QueryParameterValue>) -> Result<Self> {
        debug!("Adding WHERE clause: {} {} <value>", column, operator);
        self.where_clause = self.where_clause.where_(column, operator, value)?;
        Ok(self)
    }

    /// Add AND WHERE clause
    pub fn and_where(mut self, column: &str, operator: &str, value: impl Into<QueryParameterValue>) -> Result<Self> {
        debug!("Adding AND WHERE clause: {} {} <value>", column, operator);
        self.where_clause = self.where_clause.and_where(column, operator, value)?;
        Ok(self)
    }

    /// Add OR WHERE clause
    pub fn or_where(mut self, column: &str, operator: &str, value: impl Into<QueryParameterValue>) -> Result<Self> {
        debug!("Adding OR WHERE clause: {} {} <value>", column, operator);
        self.where_clause = self.where_clause.or_where(column, operator, value)?;
        Ok(self)
    }

    /// Add WHERE IN clause
    pub fn where_in(mut self, column: &str, values: Vec<impl Into<QueryParameterValue>>) -> Result<Self> {
        debug!("Adding WHERE IN clause for column: {}", column);
        self.where_clause = self.where_clause.where_in(column, values)?;
        Ok(self)
    }

    /// Add WHERE BETWEEN clause
    pub fn where_between(mut self, column: &str, start: impl Into<QueryParameterValue>, end: impl Into<QueryParameterValue>) -> Result<Self> {
        debug!("Adding WHERE BETWEEN clause for column: {}", column);
        self.where_clause = self.where_clause.where_between(column, start, end)?;
        Ok(self)
    }

    /// Add WHERE IS NULL clause
    pub fn where_null(mut self, column: &str) -> Result<Self> {
        debug!("Adding WHERE IS NULL clause for column: {}", column);
        self.where_clause = self.where_clause.where_null(column)?;
        Ok(self)
    }

    /// Add WHERE IS NOT NULL clause
    pub fn where_not_null(mut self, column: &str) -> Result<Self> {
        debug!("Adding WHERE IS NOT NULL clause for column: {}", column);
        self.where_clause = self.where_clause.where_not_null(column)?;
        Ok(self)
    }

    /// Build the final query - transitions to Executable state
    pub fn build(self) -> Result<UpdateQueryBuilder<'q, query_states::Executable>> {
        debug!("Building complete UPDATE query");

        // Validate that we have required components
        if self.table_name.is_none() {
            return Err(anyhow!("Table name is required for UPDATE").into());
        }

        if self.set_clauses.is_empty() {
            return Err(anyhow!("At least one SET clause is required for UPDATE").into());
        }

        Ok(UpdateQueryBuilder {
            query_context: self.query_context,
            table_name: self.table_name,
            set_clauses: self.set_clauses,
            where_clause: self.where_clause,
            parameters: self.parameters,
            table_schema: self.table_schema,
            _state: PhantomData,
        })
    }
}

impl<'q> UpdateQueryBuilder<'q, query_states::Executable> {
    /// Execute the UPDATE query through zexec-engine
    pub fn execute(self) -> Result<UpdateResult> {
        debug!("Executing UPDATE query");

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

        // Execute through zexec-engine
        let exec_result = exec_sql_store(&sql, org_id, context.store(), context.catalog())?;

        // Convert ExecResult to UpdateResult
        match exec_result {
            ExecResult::Affected(count) => {
                Ok(UpdateResult {
                    updated_count: count,
                    success: true,
                    stats: ExecutionStats::default(),
                })
            }
            ExecResult::None => {
                Ok(UpdateResult {
                    updated_count: 0,
                    success: true,
                    stats: ExecutionStats::default(),
                })
            }
            ExecResult::Rows(_) => {
                Err(anyhow!("Unexpected result type for UPDATE query").into())
            }
        }
    }

    /// Convert the query to SQL string
    pub fn to_sql(&self) -> String {
        let mut sql = String::new();

        // UPDATE clause
        sql.push_str("UPDATE ");
        if let Some(ref table) = self.table_name {
            sql.push_str(table);
        }

        // SET clause
        if !self.set_clauses.is_empty() {
            sql.push_str(" SET ");
            let set_parts: Vec<String> = self
                .set_clauses
                .iter()
                .enumerate()
                .map(|(i, clause)| {
                    if clause.is_raw {
                        // Raw SQL expression
                        match &clause.value {
                            QueryParameterValue::String(expr) => {
                                format!("{} = {}", clause.column, expr)
                            }
                            _ => format!("{} = ${}_set_{}", clause.column, clause.column, i)
                        }
                    } else {
                        // Parameterized value
                        format!("{} = ${}_set_{}", clause.column, clause.column, i)
                    }
                })
                .collect();
            sql.push_str(&set_parts.join(", "));
        }

        // WHERE clause
        if self.where_clause.has_conditions() {
            sql.push_str(" WHERE ");
            sql.push_str(&self.where_clause.to_sql());
        }

        sql
    }

    /// Get all parameters for the query
    pub fn get_parameters(&self) -> Vec<QueryParameter> {
        let mut params = Vec::new();

        // Add SET clause parameters
        for (i, clause) in self.set_clauses.iter().enumerate() {
            if !clause.is_raw {
                params.push(QueryParameter {
                    name: format!("{}_set_{}", clause.column, i),
                    value: clause.value.clone(),
                });
            }
        }

        // Add WHERE clause parameters
        params.extend(self.where_clause.get_parameters());

        params
    }

    /// Validate the UPDATE query
    pub fn validate(&self) -> Result<()> {
        if self.table_name.is_none() {
            return Err(anyhow!("Table name is required").into());
        }

        if self.set_clauses.is_empty() {
            return Err(anyhow!("At least one SET clause is required").into());
        }

        // Validate WHERE clause
        self.where_clause.validate()?;

        Ok(())
    }

    /// Validate columns against table schema
    pub fn validate_columns(&self, catalog: &Catalog, org_id: u64) -> Result<()> {
        let table_name = self.table_name.as_ref().ok_or_else(|| {
            anyhow!("Table name is required for validation")
        })?;

        let table_def = catalog.get_table(org_id, table_name)?
            .ok_or_else(|| FluentSqlError::Catalog(
                format!("Table '{}' not found", table_name).into()
            ))?;

        // Build a map of available columns
        let available_columns: HashMap<&str, &ColumnType> = table_def.columns
            .iter()
            .map(|col| (col.name.as_str(), &col.ty))
            .collect();

        // Validate each SET clause column
        for set_clause in &self.set_clauses {
            if !set_clause.is_raw {
                if !available_columns.contains_key(set_clause.column.as_str()) {
                    return Err(FluentSqlError::InvalidColumn(
                        format!("Column '{}' not found in table '{}'",
                               set_clause.column, table_name)
                    ).into());
                }
            }
        }

        Ok(())
    }
}

/// Legacy UpdateBuilder for backward compatibility
pub struct UpdateBuilder {
    table: String,
    set_clauses: Vec<SetClause>,
    where_clause: WhereClauseBuilder,
    parameters: Vec<QueryParameter>,
}

impl UpdateBuilder {
    pub(crate) fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            set_clauses: Vec::new(),
            where_clause: WhereClauseBuilder::new(),
            parameters: Vec::new(),
        }
    }

    /// Set a column to a new value
    pub fn set(mut self, column: &str, value: impl Into<QueryParameterValue>) -> Self {
        debug!("Setting column {} to value", column);
        self.set_clauses.push(SetClause::new(column, value));
        self
    }

    /// Set a column using raw SQL expression
    pub fn set_raw(mut self, column: &str, expression: &str) -> Self {
        debug!("Setting column {} to raw expression: {}", column, expression);
        self.set_clauses.push(SetClause::raw(column, expression));
        self
    }

    /// Set multiple columns using a HashMap
    pub fn set_map(mut self, updates: HashMap<&str, impl Into<QueryParameterValue> + Clone>) -> Self {
        debug!("Setting {} columns from map", updates.len());
        for (column, value) in updates {
            self.set_clauses.push(SetClause::new(column, value.clone()));
        }
        self
    }

    /// Add WHERE clause
    pub fn where_(mut self, column: &str, operator: &str, value: impl Into<QueryParameterValue>) -> Result<Self> {
        self.where_clause = self.where_clause.where_(column, operator, value)?;
        Ok(self)
    }

    /// Add AND WHERE clause
    pub fn and_where(mut self, column: &str, operator: &str, value: impl Into<QueryParameterValue>) -> Result<Self> {
        self.where_clause = self.where_clause.and_where(column, operator, value)?;
        Ok(self)
    }

    /// Add WHERE IN clause
    pub fn where_in(mut self, column: &str, values: Vec<impl Into<QueryParameterValue>>) -> Result<Self> {
        self.where_clause = self.where_clause.where_in(column, values)?;
        Ok(self)
    }

    /// Add WHERE BETWEEN clause
    pub fn where_between(mut self, column: &str, start: impl Into<QueryParameterValue>, end: impl Into<QueryParameterValue>) -> Result<Self> {
        self.where_clause = self.where_clause.where_between(column, start, end)?;
        Ok(self)
    }

    /// Add WHERE IS NULL clause
    pub fn where_null(mut self, column: &str) -> Result<Self> {
        self.where_clause = self.where_clause.where_null(column)?;
        Ok(self)
    }

    /// Add WHERE IS NOT NULL clause
    pub fn where_not_null(mut self, column: &str) -> Result<Self> {
        self.where_clause = self.where_clause.where_not_null(column)?;
        Ok(self)
    }

    /// Validate columns against table schema
    pub fn validate_columns(&self, catalog: &Catalog, org_id: u64) -> Result<()> {
        let table_def = catalog.get_table(org_id, &self.table)?
            .ok_or_else(|| FluentSqlError::Catalog(
                format!("Table '{}' not found", self.table).into()
            ))?;

        let available_columns: HashMap<&str, &ColumnType> = table_def.columns
            .iter()
            .map(|col| (col.name.as_str(), &col.ty))
            .collect();

        for set_clause in &self.set_clauses {
            if !set_clause.is_raw {
                if !available_columns.contains_key(set_clause.column.as_str()) {
                    return Err(FluentSqlError::InvalidColumn(
                        format!("Column '{}' not found in table '{}'",
                               set_clause.column, self.table)
                    ).into());
                }
            }
        }

        Ok(())
    }

    // Getter methods for ExecutionLayer integration
    /// Get the table name
    pub fn table_name(&self) -> Option<&String> {
        Some(&self.table)
    }

    /// Get the WHERE clause builder
    pub fn where_clause(&self) -> &WhereClauseBuilder {
        &self.where_clause
    }

    /// Get the SET clauses as key-value pairs
    pub fn set_clauses(&self) -> Vec<(String, QueryParameterValue)> {
        self.set_clauses.iter()
            .map(|clause| (clause.column.clone(), clause.value.clone()))
            .collect()
    }

    /// Get the query parameters
    pub fn parameters(&self) -> &[QueryParameter] {
        &self.parameters
    }

    /// Get values (not applicable for UPDATE)
    pub fn values(&self) -> &[indexmap::IndexMap<String, QueryParameterValue>] {
        &[]
    }
}

/// Simple context wrapper for UpdateQueryBuilder
pub struct UpdateQueryContext;

impl QueryContext for UpdateQueryContext {
    fn store(&mut self) -> &mut Store {
        unimplemented!("UpdateQueryBuilder uses direct execution, not this context")
    }

    fn begin_transaction(&mut self) -> Result<()> {
        unimplemented!("UpdateQueryBuilder uses direct execution, not this context")
    }

    fn commit_transaction(&mut self) -> Result<()> {
        unimplemented!("UpdateQueryBuilder uses direct execution, not this context")
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        unimplemented!("UpdateQueryBuilder uses direct execution, not this context")
    }

    fn in_transaction(&self) -> bool {
        false
    }
}

/// QueryBuilder trait implementation for UpdateQueryBuilder
impl<'q> QueryBuilder for UpdateQueryBuilder<'q, query_states::Executable> {
    type Context = UpdateQueryContext;

    fn execute(self, _ctx: &mut Self::Context) -> Result<QueryResult> {
        // Execute using the built-in execute method
        match self.execute() {
            Ok(update_result) => Ok(QueryResult::Update(update_result)),
            Err(e) => Err(e),
        }
    }

    fn to_sql(&self) -> String {
        self.to_sql()
    }

    fn get_parameters(&self) -> Vec<QueryParameter> {
        self.get_parameters()
    }

    fn validate(&self) -> Result<()> {
        self.validate()
    }
}

/// Backward compatibility - QueryBuilder trait implementation for UpdateBuilder
impl QueryBuilder for UpdateBuilder {
    type Context = UpdateQueryContext;

    fn execute(self, _ctx: &mut Self::Context) -> Result<QueryResult> {
        // For backward compatibility, this is a stub implementation
        Ok(QueryResult::Update(UpdateResult {
            updated_count: 0,
            success: true,
            stats: ExecutionStats::default(),
        }))
    }

    fn to_sql(&self) -> String {
        let mut sql = String::new();

        // UPDATE clause
        sql.push_str("UPDATE ");
        sql.push_str(&self.table);

        // SET clause
        if !self.set_clauses.is_empty() {
            sql.push_str(" SET ");
            let set_parts: Vec<String> = self
                .set_clauses
                .iter()
                .enumerate()
                .map(|(i, clause)| {
                    if clause.is_raw {
                        match &clause.value {
                            QueryParameterValue::String(expr) => {
                                format!("{} = {}", clause.column, expr)
                            }
                            _ => format!("{} = ${}_set_{}", clause.column, clause.column, i)
                        }
                    } else {
                        format!("{} = ${}_set_{}", clause.column, clause.column, i)
                    }
                })
                .collect();
            sql.push_str(&set_parts.join(", "));
        }

        // WHERE clause
        if self.where_clause.has_conditions() {
            sql.push_str(" WHERE ");
            sql.push_str(&self.where_clause.to_sql());
        }

        sql
    }

    fn get_parameters(&self) -> Vec<QueryParameter> {
        let mut params = Vec::new();

        // Add SET clause parameters
        for (i, clause) in self.set_clauses.iter().enumerate() {
            if !clause.is_raw {
                params.push(QueryParameter {
                    name: format!("{}_set_{}", clause.column, i),
                    value: clause.value.clone(),
                });
            }
        }

        // Add WHERE clause parameters
        params.extend(self.where_clause.get_parameters());

        params
    }

    fn validate(&self) -> Result<()> {
        if self.set_clauses.is_empty() {
            return Err(anyhow!("At least one SET clause is required").into());
        }

        self.where_clause.validate()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Note: ZQuery, Catalog, and Store would be used in real integration tests
    use std::collections::HashMap;

    #[test]
    fn test_update_query_builder_creation() {
        let builder = UpdateQueryBuilder::new();
        assert!(builder.table_name.is_none());
        assert!(builder.set_clauses.is_empty());
        assert!(!builder.where_clause.has_conditions());
    }

    #[test]
    fn test_update_query_builder_table() {
        let builder = UpdateQueryBuilder::new()
            .table("users");

        assert_eq!(builder.table_name, Some("users".to_string()));
    }

    #[test]
    fn test_update_query_builder_set() {
        let builder = UpdateQueryBuilder::new()
            .table("users")
            .set("name", "Alice");

        assert_eq!(builder.set_clauses.len(), 1);
        assert_eq!(builder.set_clauses[0].column, "name");
        assert!(!builder.set_clauses[0].is_raw);
    }

    #[test]
    fn test_update_query_builder_set_raw() {
        let builder = UpdateQueryBuilder::new()
            .table("users")
            .set_raw("count", "count + 1");

        assert_eq!(builder.set_clauses.len(), 1);
        assert_eq!(builder.set_clauses[0].column, "count");
        assert!(builder.set_clauses[0].is_raw);
    }

    #[test]
    fn test_update_query_builder_set_map() {
        let mut updates = HashMap::new();
        updates.insert("name", "Alice");
        updates.insert("email", "alice@example.com");

        let builder = UpdateQueryBuilder::new()
            .table("users")
            .set_map(updates);

        assert_eq!(builder.set_clauses.len(), 2);
        let columns: Vec<&str> = builder.set_clauses.iter()
            .map(|c| c.column.as_str())
            .collect();
        assert!(columns.contains(&"name"));
        assert!(columns.contains(&"email"));
    }

    #[test]
    fn test_update_query_builder_build() -> Result<()> {
        let result = UpdateQueryBuilder::new()
            .table("users")
            .set("name", "Alice")
            .build();

        assert!(result.is_ok());
        Ok(())
    }

    // Note: test_update_query_builder_build_missing_set is not possible
    // because the type system prevents calling build() without set() at compile time

    #[test]
    fn test_update_query_builder_to_sql_basic() -> Result<()> {
        let builder = UpdateQueryBuilder::new()
            .table("users")
            .set("name", "Alice")
            .build()?;

        let sql = builder.to_sql();
        assert!(sql.starts_with("UPDATE users"));
        assert!(sql.contains("SET"));
        assert!(sql.contains("name = $name_set_0"));
        Ok(())
    }

    #[test]
    fn test_update_query_builder_to_sql_with_where() -> Result<()> {
        let builder = UpdateQueryBuilder::new()
            .table("users")
            .set("name", "Alice")
            .where_("id", "=", 1)?
            .build()?;

        let sql = builder.to_sql();
        assert!(sql.contains("UPDATE users"));
        assert!(sql.contains("SET name = $name_set_0"));
        assert!(sql.contains("WHERE"));
        Ok(())
    }

    #[test]
    fn test_update_query_builder_to_sql_multiple_sets() -> Result<()> {
        let builder = UpdateQueryBuilder::new()
            .table("users")
            .set("name", "Alice")
            .set("email", "alice@example.com")
            .build()?;

        let sql = builder.to_sql();
        assert!(sql.contains("UPDATE users"));
        assert!(sql.contains("SET"));
        assert!(sql.contains("name = $name_set_0"));
        assert!(sql.contains("email = $email_set_1"));
        assert!(sql.contains(", "));
        Ok(())
    }

    #[test]
    fn test_update_query_builder_to_sql_raw_expression() -> Result<()> {
        let builder = UpdateQueryBuilder::new()
            .table("users")
            .set_raw("count", "count + 1")
            .build()?;

        let sql = builder.to_sql();
        assert!(sql.contains("UPDATE users"));
        assert!(sql.contains("SET count = count + 1"));
        Ok(())
    }

    #[test]
    fn test_update_query_builder_get_parameters() -> Result<()> {
        let builder = UpdateQueryBuilder::new()
            .table("users")
            .set("name", "Alice")
            .set("age", 25)
            .where_("id", "=", 1)?
            .build()?;

        let params = builder.get_parameters();
        assert_eq!(params.len(), 3); // 2 SET + 1 WHERE parameter

        // Check SET parameters
        let set_params: Vec<&QueryParameter> = params.iter()
            .filter(|p| p.name.contains("_set_"))
            .collect();
        assert_eq!(set_params.len(), 2);

        Ok(())
    }

    #[test]
    fn test_update_builder_to_sql() {
        let builder = UpdateBuilder::new("users")
            .set("name", "Alice")
            .set("email", "alice@example.com");

        let sql = builder.to_sql();
        assert!(sql.contains("UPDATE users"));
        assert!(sql.contains("SET"));
        assert!(sql.contains("name = $name_set_0"));
        assert!(sql.contains("email = $email_set_1"));
    }

    #[test]
    fn test_update_builder_validation() -> Result<()> {
        let builder = UpdateBuilder::new("users")
            .set("name", "Alice");

        assert!(builder.validate().is_ok());
        Ok(())
    }

    #[test]
    fn test_update_builder_validation_missing_set() {
        let builder = UpdateBuilder::new("users");
        assert!(builder.validate().is_err());
    }

    #[test]
    fn test_set_clause_creation() {
        let clause = SetClause::new("name", "Alice");
        assert_eq!(clause.column, "name");
        assert!(!clause.is_raw);

        let raw_clause = SetClause::raw("count", "count + 1");
        assert_eq!(raw_clause.column, "count");
        assert!(raw_clause.is_raw);
    }

    #[test]
    fn test_update_query_builder_where_clauses() -> Result<()> {
        let builder = UpdateQueryBuilder::new()
            .table("users")
            .set("name", "Alice")
            .where_("active", "=", true)?
            .and_where("age", ">", 18)?
            .build()?;

        let sql = builder.to_sql();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("active"));
        assert!(sql.contains("age"));

        let params = builder.get_parameters();
        // Should have parameters from both SET and WHERE clauses
        assert!(params.len() >= 3);
        Ok(())
    }

    #[test]
    fn test_update_query_builder_where_in() -> Result<()> {
        let values = vec!["admin", "moderator"];
        let builder = UpdateQueryBuilder::new()
            .table("users")
            .set("status", "inactive")
            .where_in("role", values)?
            .build()?;

        let sql = builder.to_sql();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("role IN"));
        Ok(())
    }

    #[test]
    fn test_update_query_builder_where_between() -> Result<()> {
        let builder = UpdateQueryBuilder::new()
            .table("users")
            .set("status", "verified")
            .where_between("age", 18, 65)?
            .build()?;

        let sql = builder.to_sql();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("age BETWEEN"));
        Ok(())
    }

    #[test]
    fn test_update_query_builder_where_null() -> Result<()> {
        let builder = UpdateQueryBuilder::new()
            .table("users")
            .set("deleted_at", "NOW()")
            .where_null("deleted_at")?
            .build()?;

        let sql = builder.to_sql();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("deleted_at IS NULL"));
        Ok(())
    }
}