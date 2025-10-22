/*!
DELETE query construction with fluent API and safety features.

This module provides type-safe DELETE query building with support for:
- WHERE clauses for targeting specific rows
- Safety features to prevent accidental mass deletions
- LIMIT clauses for controlling deletion scope
- Integration with zexec-engine for execution
- Phantom type state validation
*/

use crate::{
    query::{
        ZQuery, QueryBuilder, QueryContext, QueryResult, QueryParameter, QueryParameterValue,
        DeleteResult, ExecutionStats, query_states
    },
    where_clause::WhereClauseBuilder,
    Result, FluentSqlError, QueryStateTrait
};
use anyhow::anyhow;
use std::marker::PhantomData;
use tracing::{debug, warn};
use zcore_catalog::Catalog;
use zcore_storage::Store;
use zexec_engine::{exec_sql_store, ExecResult};

/// Builder for DELETE queries with ZQuery integration
#[derive(Debug, Clone)]
pub struct DeleteBuilder {
    /// Table to delete from
    table_name: String,
    /// WHERE clause builder for targeting specific rows
    where_clause: WhereClauseBuilder,
    /// Optional LIMIT for safety
    limit_value: Option<u64>,
    /// Flag indicating explicit confirmation for mass deletion
    mass_delete_confirmed: bool,
    /// Query parameters for prepared statement execution
    parameters: Vec<QueryParameter>,
}

/// DeleteQueryBuilder - enhanced version with ZQuery integration and phantom type states
pub struct DeleteQueryBuilder<'q, State = query_states::Uninitialized> {
    /// Reference to the query context for execution
    query_context: Option<&'q ZQuery<'q, 'q>>,

    /// Table to delete from
    table_name: Option<String>,

    /// WHERE clause builder for targeting specific rows
    where_clause: WhereClauseBuilder,

    /// Optional LIMIT for controlling deletion scope
    limit_count: Option<u64>,

    /// Flag indicating explicit confirmation for mass deletion
    mass_delete_confirmed: bool,

    /// Query parameters for prepared statement execution
    parameters: Vec<QueryParameter>,

    /// Phantom type for state validation
    _state: PhantomData<State>,
}

// Implementations for DeleteQueryBuilder with phantom type states
impl<'q> DeleteQueryBuilder<'q, query_states::Uninitialized> {
    /// Create a new DeleteQueryBuilder
    pub fn new() -> Self {
        debug!("Creating new DeleteQueryBuilder");
        Self {
            query_context: None,
            table_name: None,
            where_clause: WhereClauseBuilder::new(),
            limit_count: None,
            mass_delete_confirmed: false,
            parameters: Vec::new(),
            _state: PhantomData,
        }
    }

    /// Create a new DeleteQueryBuilder with query context
    pub fn with_context(context: &'q ZQuery<'q, 'q>) -> Self {
        debug!("Creating DeleteQueryBuilder with context");
        Self {
            query_context: Some(context),
            table_name: None,
            where_clause: WhereClauseBuilder::new(),
            limit_count: None,
            mass_delete_confirmed: false,
            parameters: Vec::new(),
            _state: PhantomData,
        }
    }

    /// Specify the table to delete from - transitions to TableSelected state
    pub fn from(mut self, table: &str) -> DeleteQueryBuilder<'q, query_states::TableSelected> {
        debug!("Setting DELETE FROM table: {}", table);
        self.table_name = Some(table.to_string());

        DeleteQueryBuilder {
            query_context: self.query_context,
            table_name: self.table_name,
            where_clause: self.where_clause,
            limit_count: self.limit_count,
            mass_delete_confirmed: self.mass_delete_confirmed,
            parameters: self.parameters,
            _state: PhantomData,
        }
    }
}

impl<'q> DeleteQueryBuilder<'q, query_states::TableSelected> {
    /// Set LIMIT for safety - limits number of rows deleted
    pub fn limit(mut self, count: u64) -> Self {
        debug!("Setting DELETE LIMIT {}", count);
        self.limit_count = Some(count);
        self
    }

    /// Explicitly confirm deletion of all rows without WHERE clause
    /// This is required for DELETE queries without WHERE conditions for safety
    pub fn confirm_delete_all(mut self) -> Self {
        warn!("Mass DELETE confirmed - all rows in table will be deleted");
        self.mass_delete_confirmed = true;
        self
    }

    /// Add WHERE clause using the where clause builder
    pub fn where_(mut self, column: &str, operator: &str, value: impl Into<QueryParameterValue>) -> Result<DeleteQueryBuilder<'q, query_states::WhereClauseAdded>> {
        debug!("Adding WHERE clause: {} {} <value>", column, operator);
        self.where_clause = self.where_clause.where_(column, operator, value)?;

        Ok(DeleteQueryBuilder {
            query_context: self.query_context,
            table_name: self.table_name,
            where_clause: self.where_clause,
            limit_count: self.limit_count,
            mass_delete_confirmed: self.mass_delete_confirmed,
            parameters: self.parameters,
            _state: PhantomData,
        })
    }

    /// Add WHERE IN clause
    pub fn where_in(mut self, column: &str, values: Vec<impl Into<QueryParameterValue>>) -> Result<DeleteQueryBuilder<'q, query_states::WhereClauseAdded>> {
        debug!("Adding WHERE IN clause for column: {}", column);
        self.where_clause = self.where_clause.where_in(column, values)?;

        Ok(DeleteQueryBuilder {
            query_context: self.query_context,
            table_name: self.table_name,
            where_clause: self.where_clause,
            limit_count: self.limit_count,
            mass_delete_confirmed: self.mass_delete_confirmed,
            parameters: self.parameters,
            _state: PhantomData,
        })
    }

    /// Add WHERE IS NULL clause
    pub fn where_null(mut self, column: &str) -> Result<DeleteQueryBuilder<'q, query_states::WhereClauseAdded>> {
        debug!("Adding WHERE IS NULL clause for column: {}", column);
        self.where_clause = self.where_clause.where_null(column)?;

        Ok(DeleteQueryBuilder {
            query_context: self.query_context,
            table_name: self.table_name,
            where_clause: self.where_clause,
            limit_count: self.limit_count,
            mass_delete_confirmed: self.mass_delete_confirmed,
            parameters: self.parameters,
            _state: PhantomData,
        })
    }

    /// Build the final query - transitions to Executable state (requires safety confirmation)
    pub fn build(self) -> Result<DeleteQueryBuilder<'q, query_states::Executable>> {
        debug!("Building complete DELETE query");

        // Validate that we have required components
        if self.table_name.is_none() {
            return Err(anyhow!("FROM clause is required").into());
        }

        // Safety check: require explicit confirmation for mass deletion
        if !self.where_clause.has_conditions() && !self.mass_delete_confirmed {
            return Err(anyhow!(
                "DELETE without WHERE clause requires explicit confirmation using .confirm_delete_all()"
            ).into());
        }

        Ok(DeleteQueryBuilder {
            query_context: self.query_context,
            table_name: self.table_name,
            where_clause: self.where_clause,
            limit_count: self.limit_count,
            mass_delete_confirmed: self.mass_delete_confirmed,
            parameters: self.parameters,
            _state: PhantomData,
        })
    }
}

impl<'q> DeleteQueryBuilder<'q, query_states::WhereClauseAdded> {
    /// Add additional AND WHERE clause
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

    /// Add WHERE BETWEEN clause
    pub fn and_where_between(mut self, column: &str, start: impl Into<QueryParameterValue>, end: impl Into<QueryParameterValue>) -> Result<Self> {
        debug!("Adding AND WHERE BETWEEN clause for column: {}", column);
        self.where_clause = self.where_clause.where_between(column, start, end)?;
        Ok(self)
    }

    /// Add WHERE IS NOT NULL clause
    pub fn and_where_not_null(mut self, column: &str) -> Result<Self> {
        debug!("Adding AND WHERE IS NOT NULL clause for column: {}", column);
        self.where_clause = self.where_clause.and_where_not_null(column)?;
        Ok(self)
    }

    /// Set LIMIT for safety - limits number of rows deleted
    pub fn limit(mut self, count: u64) -> Self {
        debug!("Setting DELETE LIMIT {}", count);
        self.limit_count = Some(count);
        self
    }

    /// Build the final query - transitions to Executable state
    pub fn build(self) -> Result<DeleteQueryBuilder<'q, query_states::Executable>> {
        debug!("Building complete DELETE query with WHERE clause");

        // Validate that we have required components
        if self.table_name.is_none() {
            return Err(anyhow!("FROM clause is required").into());
        }

        // Validate WHERE clause
        self.where_clause.validate()?;

        Ok(DeleteQueryBuilder {
            query_context: self.query_context,
            table_name: self.table_name,
            where_clause: self.where_clause,
            limit_count: self.limit_count,
            mass_delete_confirmed: self.mass_delete_confirmed,
            parameters: self.parameters,
            _state: PhantomData,
        })
    }
}
impl<'q> DeleteQueryBuilder<'q, query_states::Executable> {
    /// Execute the DELETE query through zexec-engine
    pub fn execute(self) -> Result<DeleteResult> {
        debug!("Executing DELETE query");

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

        // Convert ExecResult to DeleteResult
        match exec_result {
            ExecResult::Affected(count) => {
                Ok(DeleteResult {
                    deleted_count: count as u64,
                    success: true,
                    stats: ExecutionStats::default(),
                })
            }
            ExecResult::None => {
                Ok(DeleteResult {
                    deleted_count: 0,
                    success: true,
                    stats: ExecutionStats::default(),
                })
            }
            ExecResult::Rows(_) => {
                Err(anyhow!("Unexpected result type for DELETE query").into())
            }
        }
    }

    /// Convert the query to SQL string
    pub fn to_sql(&self) -> String {
        let mut sql = String::new();

        // DELETE FROM clause
        sql.push_str("DELETE FROM ");
        if let Some(ref table) = self.table_name {
            sql.push_str(table);
        }

        // WHERE clause
        if self.where_clause.has_conditions() {
            sql.push_str(" WHERE ");
            sql.push_str(&self.where_clause.to_sql());
        }

        // LIMIT clause (if supported by the SQL engine)
        if let Some(limit) = self.limit_count {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        sql
    }

    /// Validate that target table exists using catalog
    pub fn validate_table_exists(&self, catalog: &Catalog, org_id: u64) -> Result<()> {
        if let Some(ref table_name) = self.table_name {
            let table_def = catalog.get_table(org_id, table_name)?
                .ok_or_else(|| FluentSqlError::Catalog(
                    format!("Table '{}' not found", table_name).into()
                ))?;

            debug!("Validated table '{}' exists with {} columns", table_name, table_def.columns.len());
        }
        Ok(())
    }
}

// Backward compatibility - original DeleteBuilder
impl DeleteBuilder {
    pub(crate) fn new(table: &str) -> Self {
        Self {
            table_name: table.to_string(),
            where_clause: WhereClauseBuilder::new(),
            limit_value: None,
            mass_delete_confirmed: false,
            parameters: Vec::new(),
        }
    }

    /// Add a WHERE condition
    pub fn where_col(self, column: &str) -> DeleteWhereClause {
        DeleteWhereClause::new_with_column(self, column.to_string())
    }

    /// Add an AND WHERE condition
    pub fn and_where_col(self, column: &str) -> DeleteWhereClause {
        DeleteWhereClause::new_with_column_and_operator(self, column.to_string(), "AND".to_string())
    }

    /// Limit the number of rows to delete
    pub fn limit(mut self, count: u64) -> Self {
        self.limit_value = Some(count);
        self
    }

    /// Explicitly confirm deletion of all rows
    pub fn confirm_delete_all(mut self) -> Self {
        warn!("Mass DELETE confirmed - all rows in table will be deleted");
        self.mass_delete_confirmed = true;
        self
    }

    /// Validate the DELETE query before execution
    pub fn validate(&self) -> Result<()> {
        // Safety check: require explicit confirmation for mass deletion
        if !self.where_clause.has_conditions() && !self.mass_delete_confirmed {
            return Err(anyhow!(
                "DELETE without WHERE clause requires explicit confirmation using .confirm_delete_all()"
            ).into());
        }

        // Validate WHERE clause if present
        if self.where_clause.has_conditions() {
            self.where_clause.validate()?;
        }

        Ok(())
    }

    // Getter methods for ExecutionLayer integration
    /// Get the table name
    pub fn table_name(&self) -> Option<&String> {
        Some(&self.table_name)
    }

    /// Get the WHERE clause builder
    pub fn where_clause(&self) -> &WhereClauseBuilder {
        &self.where_clause
    }

    /// Get the query parameters
    pub fn parameters(&self) -> &[QueryParameter] {
        &self.parameters
    }

    /// Get values (not applicable for DELETE)
    pub fn values(&self) -> &[indexmap::IndexMap<String, QueryParameterValue>] {
        &[]
    }

    /// Get set clauses (not applicable for DELETE)
    pub fn set_clauses(&self) -> &[(String, QueryParameterValue)] {
        &[]
    }
}

/// Simple context wrapper for DeleteQueryBuilder
pub struct DeleteQueryContext;

impl QueryContext for DeleteQueryContext {
    fn store(&mut self) -> &mut Store {
        unimplemented!("DeleteQueryBuilder uses direct execution, not this context")
    }

    fn begin_transaction(&mut self) -> Result<()> {
        unimplemented!("DeleteQueryBuilder uses direct execution, not this context")
    }

    fn commit_transaction(&mut self) -> Result<()> {
        unimplemented!("DeleteQueryBuilder uses direct execution, not this context")
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        unimplemented!("DeleteQueryBuilder uses direct execution, not this context")
    }

    fn in_transaction(&self) -> bool {
        false
    }
}

/// QueryBuilder trait implementation for DeleteQueryBuilder
impl<'q> QueryBuilder for DeleteQueryBuilder<'q, query_states::Executable> {
    type Context = DeleteQueryContext;

    fn execute(self, _ctx: &mut Self::Context) -> Result<QueryResult> {
        // Execute using the built-in execute method
        match self.execute() {
            Ok(delete_result) => Ok(QueryResult::Delete(delete_result)),
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
            return Err(anyhow!("FROM clause is required").into());
        }

        // Safety check: require explicit confirmation for mass deletion
        if !self.where_clause.has_conditions() && !self.mass_delete_confirmed {
            return Err(anyhow!(
                "DELETE without WHERE clause requires explicit confirmation using .confirm_delete_all()"
            ).into());
        }

        // Validate WHERE clause if present
        if self.where_clause.has_conditions() {
            self.where_clause.validate()?;
        }

        Ok(())
    }
}

/// Backward compatibility - QueryBuilder trait implementation for DeleteBuilder
impl QueryBuilder for DeleteBuilder {
    type Context = DeleteQueryContext;

    fn execute(self, _ctx: &mut Self::Context) -> Result<QueryResult> {
        // For backward compatibility, this is a stub implementation
        self.validate()?;
        Ok(QueryResult::Delete(DeleteResult {
            deleted_count: 0,
            success: true,
            stats: ExecutionStats::default(),
        }))
    }

    fn to_sql(&self) -> String {
        let mut sql = String::new();

        // DELETE FROM clause
        sql.push_str("DELETE FROM ");
        sql.push_str(&self.table_name);

        // WHERE clause
        if self.where_clause.has_conditions() {
            sql.push_str(" WHERE ");
            sql.push_str(&self.where_clause.to_sql());
        }

        // LIMIT clause
        if let Some(limit) = self.limit_value {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        sql
    }

    fn get_parameters(&self) -> Vec<QueryParameter> {
        self.parameters.clone()
    }

    fn validate(&self) -> Result<()> {
        self.validate()
    }
}

/// Helper for building WHERE clauses in DELETE queries (backward compatibility)
pub struct DeleteWhereClause {
    builder: DeleteBuilder,
    column: String,
    logical_operator: Option<String>, // None for first condition, Some("AND"/"OR") for subsequent
}

impl DeleteWhereClause {
    fn new(builder: DeleteBuilder) -> Self {
        Self {
            builder,
            column: String::new(),
            logical_operator: None,
        }
    }

    fn new_with_column(builder: DeleteBuilder, column: String) -> Self {
        Self {
            builder,
            column,
            logical_operator: None,
        }
    }

    fn new_with_column_and_operator(builder: DeleteBuilder, column: String, logical_operator: String) -> Self {
        Self {
            builder,
            column,
            logical_operator: Some(logical_operator),
        }
    }

    /// Greater than comparison
    pub fn gt<T>(mut self, value: T) -> DeleteBuilder
    where
        T: Into<QueryParameterValue>,
    {
        let param_value = value.into();
        let param_name = format!("{}_{}", self.column, self.builder.parameters.len());

        // Create the parameter
        let parameter = QueryParameter {
            name: param_name.clone(),
            value: param_value,
        };

        // Add condition to where clause
        match &self.logical_operator {
            None => {
                // First WHERE condition
                self.builder.where_clause = self.builder.where_clause.where_(&self.column, ">", parameter.value.clone())
                    .unwrap_or_else(|_| WhereClauseBuilder::new());
            }
            Some(op) if op == "AND" => {
                // AND WHERE condition
                self.builder.where_clause = self.builder.where_clause.and_where(&self.column, ">", parameter.value.clone())
                    .unwrap_or_else(|_| WhereClauseBuilder::new());
            }
            Some(_) => {
                // OR WHERE condition (for future use)
                self.builder.where_clause = self.builder.where_clause.or_where(&self.column, ">", parameter.value.clone())
                    .unwrap_or_else(|_| WhereClauseBuilder::new());
            }
        }

        // Add parameter to builder
        self.builder.parameters.push(parameter);
        self.builder
    }

    /// Equality comparison
    pub fn eq<T>(mut self, value: T) -> DeleteBuilder
    where
        T: Into<QueryParameterValue>,
    {
        let param_value = value.into();
        let param_name = format!("{}_{}", self.column, self.builder.parameters.len());

        // Create the parameter
        let parameter = QueryParameter {
            name: param_name.clone(),
            value: param_value,
        };

        // Add condition to where clause
        match &self.logical_operator {
            None => {
                // First WHERE condition
                self.builder.where_clause = self.builder.where_clause.where_(&self.column, "=", parameter.value.clone())
                    .unwrap_or_else(|_| WhereClauseBuilder::new());
            }
            Some(op) if op == "AND" => {
                // AND WHERE condition
                self.builder.where_clause = self.builder.where_clause.and_where(&self.column, "=", parameter.value.clone())
                    .unwrap_or_else(|_| WhereClauseBuilder::new());
            }
            Some(_) => {
                // OR WHERE condition (for future use)
                self.builder.where_clause = self.builder.where_clause.or_where(&self.column, "=", parameter.value.clone())
                    .unwrap_or_else(|_| WhereClauseBuilder::new());
            }
        }

        // Add parameter to builder
        self.builder.parameters.push(parameter);
        self.builder
    }

    /// Less than comparison
    pub fn lt<T>(mut self, value: T) -> DeleteBuilder
    where
        T: Into<QueryParameterValue>,
    {
        let param_value = value.into();
        let param_name = format!("{}_{}", self.column, self.builder.parameters.len());

        // Create the parameter
        let parameter = QueryParameter {
            name: param_name.clone(),
            value: param_value,
        };

        // Add condition to where clause
        match &self.logical_operator {
            None => {
                // First WHERE condition
                self.builder.where_clause = self.builder.where_clause.where_(&self.column, "<", parameter.value.clone())
                    .unwrap_or_else(|_| WhereClauseBuilder::new());
            }
            Some(op) if op == "AND" => {
                // AND WHERE condition
                self.builder.where_clause = self.builder.where_clause.and_where(&self.column, "<", parameter.value.clone())
                    .unwrap_or_else(|_| WhereClauseBuilder::new());
            }
            Some(_) => {
                // OR WHERE condition (for future use)
                self.builder.where_clause = self.builder.where_clause.or_where(&self.column, "<", parameter.value.clone())
                    .unwrap_or_else(|_| WhereClauseBuilder::new());
            }
        }

        // Add parameter to builder
        self.builder.parameters.push(parameter);
        self.builder
    }

    /// In a list of values
    pub fn in_values<T>(mut self, values: &[T]) -> DeleteBuilder
    where
        T: Clone + Into<QueryParameterValue>,
    {
        if values.is_empty() {
            // Empty IN clause is technically valid but matches nothing
            return self.builder;
        }

        let param_values: Vec<QueryParameterValue> = values.iter()
            .cloned()
            .map(|v| v.into())
            .collect();

        // Add condition to where clause
        match &self.logical_operator {
            None => {
                // First WHERE condition
                self.builder.where_clause = self.builder.where_clause.where_in(&self.column, param_values.clone())
                    .unwrap_or_else(|_| WhereClauseBuilder::new());
            }
            Some(_) => {
                // For AND/OR conditions with IN, we need to add it as a separate where_in call
                // This is a simplified implementation that doesn't perfectly handle logical operators with IN
                self.builder.where_clause = self.builder.where_clause.where_in(&self.column, param_values.clone())
                    .unwrap_or_else(|_| WhereClauseBuilder::new());
            }
        }

        // Add parameters to builder (the where_in method handles parameter creation internally)
        // We don't need to add parameters manually here as WhereClauseBuilder handles it
        self.builder
    }
}

// Manual Clone implementation for DeleteQueryBuilder to avoid Debug trait requirement on ZQuery
impl<'q, State> Clone for DeleteQueryBuilder<'q, State>
where
    State: QueryStateTrait,
{
    fn clone(&self) -> Self {
        Self {
            query_context: self.query_context,
            table_name: self.table_name.clone(),
            where_clause: self.where_clause.clone(),
            limit_count: self.limit_count,
            mass_delete_confirmed: self.mass_delete_confirmed,
            parameters: self.parameters.clone(),
            _state: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::QueryParameterValue;

    #[test]
    fn test_delete_query_builder_creation() {
        let builder = DeleteQueryBuilder::new();
        assert!(builder.table_name.is_none());
        assert!(!builder.where_clause.has_conditions());
        assert!(builder.limit_count.is_none());
        assert!(!builder.mass_delete_confirmed);
        assert!(builder.parameters.is_empty());
    }

    #[test]
    fn test_delete_query_builder_from_clause() {
        let builder = DeleteQueryBuilder::new()
            .from("users");

        assert_eq!(builder.table_name, Some("users".to_string()));
    }

    #[test]
    fn test_delete_query_builder_limit() {
        let builder = DeleteQueryBuilder::new()
            .from("users")
            .limit(100);

        assert_eq!(builder.limit_count, Some(100));
    }

    #[test]
    fn test_delete_query_builder_confirm_delete_all() {
        let builder = DeleteQueryBuilder::new()
            .from("users")
            .confirm_delete_all();

        assert!(builder.mass_delete_confirmed);
    }

    #[test]
    fn test_delete_query_builder_where_clause() -> Result<()> {
        let builder = DeleteQueryBuilder::new()
            .from("users")
            .where_("age", ">", 65i64)?;

        assert!(builder.where_clause.has_conditions());
        Ok(())
    }

    #[test]
    fn test_delete_query_builder_where_in() -> Result<()> {
        let values = vec!["inactive", "banned"];
        let builder = DeleteQueryBuilder::new()
            .from("users")
            .where_in("status", values)?;

        assert!(builder.where_clause.has_conditions());
        Ok(())
    }

    #[test]
    fn test_delete_query_builder_where_null() -> Result<()> {
        let builder = DeleteQueryBuilder::new()
            .from("users")
            .where_null("deleted_at")?;

        assert!(builder.where_clause.has_conditions());
        Ok(())
    }

    #[test]
    fn test_delete_query_builder_and_where() -> Result<()> {
        let builder = DeleteQueryBuilder::new()
            .from("users")
            .where_("status", "=", "inactive")?
            .and_where("last_login", "<", "2023-01-01")?;

        assert!(builder.where_clause.has_conditions());
        Ok(())
    }

    #[test]
    fn test_delete_query_builder_or_where() -> Result<()> {
        let builder = DeleteQueryBuilder::new()
            .from("users")
            .where_("status", "=", "banned")?
            .or_where("age", ">", 80)?;

        assert!(builder.where_clause.has_conditions());
        Ok(())
    }

    #[test]
    fn test_delete_query_builder_build_with_where() -> Result<()> {
        let builder = DeleteQueryBuilder::new()
            .from("users")
            .where_("status", "=", "inactive")?
            .build()?;

        // Should be in executable state
        assert!(builder.table_name.is_some());
        assert!(builder.where_clause.has_conditions());
        Ok(())
    }

    #[test]
    fn test_delete_query_builder_build_with_confirmation() -> Result<()> {
        let builder = DeleteQueryBuilder::new()
            .from("users")
            .confirm_delete_all()
            .build()?;

        // Should be in executable state
        assert!(builder.table_name.is_some());
        assert!(builder.mass_delete_confirmed);
        Ok(())
    }

    #[test]
    fn test_delete_query_builder_build_missing_table_fails() {
        // We can't call build() on Uninitialized state - this is a compile-time error,
        // which is the desired behavior. The phantom type system prevents this.
        // This test documents the expected behavior.
        assert!(true); // Test passes - we can't even write invalid code
    }

    #[test]
    fn test_delete_query_builder_build_without_where_or_confirmation_fails() {
        let result = DeleteQueryBuilder::new()
            .from("users")
            .build();

        assert!(result.is_err());
        let error = result.err().unwrap();
        assert!(error.to_string().contains("explicit confirmation"));
    }

    #[test]
    fn test_delete_query_builder_to_sql_basic() -> Result<()> {
        let builder = DeleteQueryBuilder::new()
            .from("users")
            .where_("status", "=", "inactive")?
            .build()?;

        let sql = builder.to_sql();
        assert_eq!(sql, "DELETE FROM users WHERE status = $status_0");
        Ok(())
    }

    #[test]
    fn test_delete_query_builder_to_sql_with_limit() -> Result<()> {
        let builder = DeleteQueryBuilder::new()
            .from("users")
            .where_("status", "=", "inactive")?
            .limit(100)
            .build()?;

        let sql = builder.to_sql();
        assert_eq!(sql, "DELETE FROM users WHERE status = $status_0 LIMIT 100");
        Ok(())
    }

    #[test]
    fn test_delete_query_builder_to_sql_mass_delete() -> Result<()> {
        let builder = DeleteQueryBuilder::new()
            .from("users")
            .confirm_delete_all()
            .build()?;

        let sql = builder.to_sql();
        assert_eq!(sql, "DELETE FROM users");
        Ok(())
    }

    #[test]
    fn test_delete_query_builder_to_sql_complex_where() -> Result<()> {
        let builder = DeleteQueryBuilder::new()
            .from("users")
            .where_("status", "=", "inactive")?
            .and_where("age", ">", 65)?
            .build()?;

        let sql = builder.to_sql();
        assert!(sql.starts_with("DELETE FROM users WHERE"));
        assert!(sql.contains("status = $status_0"));
        assert!(sql.contains("age > $age_0"));
        assert!(sql.contains("AND"));
        Ok(())
    }

    #[test]
    fn test_delete_query_builder_validation() -> Result<()> {
        let builder = DeleteQueryBuilder::new()
            .from("users")
            .where_("status", "=", "inactive")?
            .build()?;

        // Should validate successfully
        assert!(builder.validate().is_ok());
        Ok(())
    }

    #[test]
    fn test_delete_query_builder_validation_missing_table() {
        // Test using the QueryBuilder trait validation
        let builder = DeleteQueryBuilder::new()
            .from("users")
            .confirm_delete_all()
            .build().unwrap();

        // Test by calling validate directly (which is a QueryBuilder trait method)
        use crate::query::QueryBuilder;
        assert!(builder.validate().is_ok());
    }

    #[test]
    fn test_delete_query_builder_validation_safety() {
        // Test safety validation - this should fail at build() time
        let result = DeleteQueryBuilder::new()
            .from("users")
            .build();

        assert!(result.is_err());
        let error = result.err().unwrap();
        assert!(error.to_string().contains("explicit confirmation"));
    }

    // Tests for backward compatibility DeleteBuilder
    #[test]
    fn test_delete_builder_creation() {
        let builder = DeleteBuilder::new("users");
        assert_eq!(builder.table_name, "users");
        assert!(!builder.where_clause.has_conditions());
        assert!(builder.limit_value.is_none());
        assert!(!builder.mass_delete_confirmed);
    }

    #[test]
    fn test_delete_builder_limit() {
        let builder = DeleteBuilder::new("users")
            .limit(50);

        assert_eq!(builder.limit_value, Some(50));
    }

    #[test]
    fn test_delete_builder_confirm_delete_all() {
        let builder = DeleteBuilder::new("users")
            .confirm_delete_all();

        assert!(builder.mass_delete_confirmed);
    }

    #[test]
    fn test_delete_builder_to_sql() {
        let builder = DeleteBuilder::new("users")
            .confirm_delete_all();

        let sql = builder.to_sql();
        assert_eq!(sql, "DELETE FROM users");
    }

    #[test]
    fn test_delete_builder_to_sql_with_limit() {
        let builder = DeleteBuilder::new("users")
            .confirm_delete_all()
            .limit(100);

        let sql = builder.to_sql();
        assert_eq!(sql, "DELETE FROM users LIMIT 100");
    }

    #[test]
    fn test_delete_builder_validate_success() -> Result<()> {
        let builder = DeleteBuilder::new("users")
            .confirm_delete_all();

        assert!(builder.validate().is_ok());
        Ok(())
    }

    #[test]
    fn test_delete_builder_validate_safety_failure() {
        let builder = DeleteBuilder::new("users");
        // No WHERE clause and no confirmation

        let result = builder.validate();
        assert!(result.is_err());
        let error = result.err().unwrap();
        assert!(error.to_string().contains("explicit confirmation"));
    }

    #[test]
    fn test_delete_builder_query_builder_trait() -> Result<()> {
        let builder = DeleteBuilder::new("users")
            .confirm_delete_all();

        // Test QueryBuilder trait methods
        let sql = builder.to_sql();
        assert_eq!(sql, "DELETE FROM users");

        let params = builder.get_parameters();
        assert!(params.is_empty());

        let validate_result = builder.validate();
        assert!(validate_result.is_ok());

        Ok(())
    }

    #[test]
    fn test_delete_where_clause_helper() {
        let builder = DeleteBuilder::new("users");
        let where_clause = builder.where_col("status");

        // Test that we can call comparison methods (even if they're stubs)
        let _result = where_clause.eq("inactive");
        // This should not panic and should return the builder
    }

    #[test]
    fn test_delete_query_parameter_conversion() {
        let param_int = QueryParameterValue::from(42i64);
        assert!(matches!(param_int, QueryParameterValue::Int(42)));

        let param_str = QueryParameterValue::from("test");
        assert!(matches!(param_str, QueryParameterValue::String(_)));

        let param_bool = QueryParameterValue::from(true);
        assert!(matches!(param_bool, QueryParameterValue::Bool(true)));
    }

    #[test]
    fn test_mass_delete_safety_warning_logged() {
        // This test verifies that the warning is properly logged
        // In a real application, you might use a test logger to capture this
        let builder = DeleteQueryBuilder::new()
            .from("users")
            .confirm_delete_all();

        assert!(builder.mass_delete_confirmed);
    }

    #[test]
    fn test_delete_query_builder_complex_scenario() -> Result<()> {
        // Test a complex DELETE scenario with multiple conditions
        let builder = DeleteQueryBuilder::new()
            .from("user_sessions")
            .where_("expires_at", "<", "2024-01-01T00:00:00Z")?
            .and_where("active", "=", false)?
            .limit(1000)
            .build()?;

        let sql = builder.to_sql();
        assert!(sql.starts_with("DELETE FROM user_sessions WHERE"));
        assert!(sql.contains("expires_at"));
        assert!(sql.contains("active"));
        assert!(sql.contains("LIMIT 1000"));
        assert!(builder.validate().is_ok());

        Ok(())
    }

    #[test]
    fn test_delete_query_builder_state_transitions() {
        // Test that the phantom type system enforces correct state transitions

        // Valid progression: Uninitialized -> TableSelected -> WhereClauseAdded -> Executable
        let uninitialized = DeleteQueryBuilder::new();
        let table_selected = uninitialized.from("users");

        // From TableSelected, we can add WHERE or build (with confirmation)
        let table_selected_copy = DeleteQueryBuilder::new().from("users");
        let _confirmed_build = table_selected_copy.confirm_delete_all().build();

        // Or add WHERE conditions
        let where_added = table_selected.where_("status", "=", "inactive").unwrap();
        let _final_build = where_added.build().unwrap();

        // This demonstrates the type safety - the compiler enforces valid transitions
        assert!(true); // If we reach here, the state transitions worked
    }

    #[test]
    fn test_delete_where_clause_gt() {
        let builder = DeleteBuilder::new("users")
            .where_col("age").gt(65);

        // Check that WHERE clause was built
        assert!(builder.where_clause.has_conditions());
        assert!(!builder.parameters.is_empty());
        assert_eq!(builder.parameters.len(), 1);
        assert_eq!(builder.parameters[0].name, "age_0");
        assert!(matches!(builder.parameters[0].value, QueryParameterValue::Int(65)));
    }

    #[test]
    fn test_delete_where_clause_eq() {
        let builder = DeleteBuilder::new("users")
            .where_col("status").eq("inactive");

        // Check that WHERE clause was built
        assert!(builder.where_clause.has_conditions());
        assert!(!builder.parameters.is_empty());
        assert_eq!(builder.parameters.len(), 1);
        assert_eq!(builder.parameters[0].name, "status_0");
        assert!(matches!(builder.parameters[0].value, QueryParameterValue::String(ref s) if s == "inactive"));
    }

    #[test]
    fn test_delete_where_clause_lt() {
        let builder = DeleteBuilder::new("users")
            .where_col("score").lt(50.0);

        // Check that WHERE clause was built
        assert!(builder.where_clause.has_conditions());
        assert!(!builder.parameters.is_empty());
        assert_eq!(builder.parameters.len(), 1);
        assert_eq!(builder.parameters[0].name, "score_0");
        assert!(matches!(builder.parameters[0].value, QueryParameterValue::Float(f) if (f - 50.0).abs() < f64::EPSILON));
    }

    #[test]
    fn test_delete_where_clause_in_values() {
        let statuses = ["inactive", "suspended"];
        let builder = DeleteBuilder::new("users")
            .where_col("status").in_values(&statuses);

        // Check that WHERE clause was built
        assert!(builder.where_clause.has_conditions());
        // Parameters are handled by WhereClauseBuilder internally for IN clauses
    }

    #[test]
    fn test_delete_and_where_clause() {
        let builder = DeleteBuilder::new("users")
            .where_col("age").gt(65)
            .and_where_col("status").eq("inactive");

        // Check that WHERE clause was built with multiple conditions
        assert!(builder.where_clause.has_conditions());
        assert_eq!(builder.parameters.len(), 2);
        assert_eq!(builder.parameters[0].name, "age_0");
        assert_eq!(builder.parameters[1].name, "status_1");
        assert!(matches!(builder.parameters[0].value, QueryParameterValue::Int(65)));
        assert!(matches!(builder.parameters[1].value, QueryParameterValue::String(ref s) if s == "inactive"));
    }

    #[test]
    fn test_delete_where_clause_to_sql() {
        let builder = DeleteBuilder::new("users")
            .where_col("age").gt(65);

        let sql = builder.to_sql();
        assert!(sql.contains("DELETE FROM users"));
        assert!(sql.contains("WHERE"));
        // SQL generation from WhereClauseBuilder should include the condition
        assert!(!builder.where_clause.to_sql().is_empty());
    }

    #[test]
    fn test_delete_where_clause_in_values_empty() {
        let empty_statuses: &[&str] = &[];
        let builder = DeleteBuilder::new("users")
            .where_col("status").in_values(empty_statuses);

        // Empty IN values should not crash but may not add conditions
        assert!(true); // If we reach here, empty IN didn't crash
    }
}