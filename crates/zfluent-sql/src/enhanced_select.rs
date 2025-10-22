/*!
Enhanced SELECT query construction with comprehensive WHERE clause support.

This module provides a complete implementation of SELECT query building with
the comprehensive WHERE clause builder integration.
*/

use crate::{
    query::{QueryParameter, QueryParameterValue},
    where_clause::WhereClauseBuilder,
    Result
};
use anyhow::anyhow;

/// Enhanced SelectBuilder with full WHERE clause support
#[derive(Debug, Clone)]
pub struct EnhancedSelectBuilder {
    /// Selected columns
    selected_columns: Vec<String>,
    /// FROM table name
    table_name: Option<String>,
    /// WHERE clause builder with comprehensive support
    where_clause: WhereClauseBuilder,
    /// ORDER BY clauses
    order_by: Vec<String>,
    /// LIMIT value
    limit_value: Option<usize>,
    /// OFFSET value
    offset_value: Option<usize>,
}

impl EnhancedSelectBuilder {
    /// Create a new SelectBuilder
    pub fn new() -> Self {
        Self {
            selected_columns: Vec::new(),
            table_name: None,
            where_clause: WhereClauseBuilder::new(),
            order_by: Vec::new(),
            limit_value: None,
            offset_value: None,
        }
    }

    /// Specify the table to select from
    pub fn from(mut self, table: &str) -> Self {
        self.table_name = Some(table.to_string());
        self
    }

    /// Select specific columns
    pub fn select(mut self, columns: &[&str]) -> Self {
        self.selected_columns = columns.iter().map(|&c| c.to_string()).collect();
        self
    }

    /// Select all columns (*)
    pub fn select_all(mut self) -> Self {
        self.selected_columns = vec!["*".to_string()];
        self
    }

    /// Add a WHERE condition with operator and value
    pub fn where_(mut self, column: &str, operator: &str, value: impl Into<QueryParameterValue>) -> Result<Self> {
        self.where_clause = self.where_clause.where_(column, operator, value)?;
        Ok(self)
    }

    /// Add an AND WHERE condition
    pub fn and_where(mut self, column: &str, operator: &str, value: impl Into<QueryParameterValue>) -> Result<Self> {
        self.where_clause = self.where_clause.and_where(column, operator, value)?;
        Ok(self)
    }

    /// Add an OR WHERE condition
    pub fn or_where(mut self, column: &str, operator: &str, value: impl Into<QueryParameterValue>) -> Result<Self> {
        self.where_clause = self.where_clause.or_where(column, operator, value)?;
        Ok(self)
    }

    /// Add a WHERE IN condition
    pub fn where_in(mut self, column: &str, values: Vec<impl Into<QueryParameterValue>>) -> Result<Self> {
        self.where_clause = self.where_clause.where_in(column, values)?;
        Ok(self)
    }

    /// Add a WHERE NOT IN condition
    pub fn where_not_in(mut self, column: &str, values: Vec<impl Into<QueryParameterValue>>) -> Result<Self> {
        self.where_clause = self.where_clause.where_not_in(column, values)?;
        Ok(self)
    }

    /// Add a WHERE BETWEEN condition
    pub fn where_between(
        mut self,
        column: &str,
        start: impl Into<QueryParameterValue>,
        end: impl Into<QueryParameterValue>,
    ) -> Result<Self> {
        self.where_clause = self.where_clause.where_between(column, start, end)?;
        Ok(self)
    }

    /// Add a WHERE NOT BETWEEN condition
    pub fn where_not_between(
        mut self,
        column: &str,
        start: impl Into<QueryParameterValue>,
        end: impl Into<QueryParameterValue>,
    ) -> Result<Self> {
        self.where_clause = self.where_clause.where_not_between(column, start, end)?;
        Ok(self)
    }

    /// Add an AND WHERE IS NOT NULL condition
    pub fn and_where_not_null(mut self, column: &str) -> Result<Self> {
        self.where_clause = self.where_clause.and_where_not_null(column)?;
        Ok(self)
    }

    /// Add a WHERE IS NULL condition
    pub fn where_null(mut self, column: &str) -> Result<Self> {
        self.where_clause = self.where_clause.where_null(column)?;
        Ok(self)
    }

    /// Add a WHERE IS NOT NULL condition
    pub fn where_not_null(mut self, column: &str) -> Result<Self> {
        self.where_clause = self.where_clause.where_not_null(column)?;
        Ok(self)
    }

    /// Add a grouped WHERE condition
    pub fn where_group(
        mut self,
        group_builder: impl FnOnce(WhereClauseBuilder) -> Result<WhereClauseBuilder>
    ) -> Result<Self> {
        self.where_clause = self.where_clause.where_group(group_builder)?;
        Ok(self)
    }

    /// Add ORDER BY clause
    pub fn order_by(mut self, column: &str, direction: &str) -> Self {
        self.order_by.push(format!("{} {}", column, direction));
        self
    }

    /// Add ORDER BY ASC
    pub fn order_by_asc(self, column: &str) -> Self {
        self.order_by(column, "ASC")
    }

    /// Add ORDER BY DESC
    pub fn order_by_desc(self, column: &str) -> Self {
        self.order_by(column, "DESC")
    }

    /// Add LIMIT clause
    pub fn limit(mut self, count: usize) -> Self {
        self.limit_value = Some(count);
        self
    }

    /// Add OFFSET clause
    pub fn offset(mut self, count: usize) -> Self {
        self.offset_value = Some(count);
        self
    }

    /// Build the SQL query string
    pub fn to_sql(&self) -> String {
        let mut sql = String::new();

        // SELECT clause
        sql.push_str("SELECT ");
        if self.selected_columns.is_empty() || self.selected_columns == vec!["*"] {
            sql.push('*');
        } else {
            sql.push_str(&self.selected_columns.join(", "));
        }

        // FROM clause
        if let Some(ref table) = self.table_name {
            sql.push_str(" FROM ");
            sql.push_str(table);
        }

        // WHERE clause
        if self.where_clause.has_conditions() {
            sql.push_str(" WHERE ");
            sql.push_str(&self.where_clause.to_sql());
        }

        // ORDER BY clause
        if !self.order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            sql.push_str(&self.order_by.join(", "));
        }

        // LIMIT clause
        if let Some(limit) = self.limit_value {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        // OFFSET clause
        if let Some(offset) = self.offset_value {
            sql.push_str(&format!(" OFFSET {}", offset));
        }

        sql
    }

    /// Get all parameters from the query
    pub fn get_parameters(&self) -> Vec<QueryParameter> {
        self.where_clause.get_parameters()
    }

    /// Validate the query structure
    pub fn validate(&self) -> Result<()> {
        if self.table_name.is_none() {
            return Err(anyhow!("FROM clause is required"));
        }

        // Validate WHERE clause
        self.where_clause.validate()?;

        Ok(())
    }
}

impl Default for EnhancedSelectBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_select_query() -> Result<()> {
        let builder = EnhancedSelectBuilder::new()
            .from("users")
            .select(&["id", "name", "email"]);

        let sql = builder.to_sql();
        assert_eq!(sql, "SELECT id, name, email FROM users");
        Ok(())
    }

    #[test]
    fn test_select_all() -> Result<()> {
        let builder = EnhancedSelectBuilder::new()
            .from("users")
            .select_all();

        let sql = builder.to_sql();
        assert_eq!(sql, "SELECT * FROM users");
        Ok(())
    }

    #[test]
    fn test_simple_where_clause() -> Result<()> {
        let builder = EnhancedSelectBuilder::new()
            .from("users")
            .select_all()
            .where_("age", ">", 18i64)?;

        let sql = builder.to_sql();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("age"));

        let params = builder.get_parameters();
        assert_eq!(params.len(), 1);
        Ok(())
    }

    #[test]
    fn test_complex_where_clause() -> Result<()> {
        let builder = EnhancedSelectBuilder::new()
            .from("users")
            .select_all()
            .where_("age", ">", 18i64)?
            .and_where("status", "=", "active")?
            .or_where("role", "=", "admin")?;

        let sql = builder.to_sql();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("age"));
        assert!(sql.contains("status"));
        assert!(sql.contains("role"));

        let params = builder.get_parameters();
        assert_eq!(params.len(), 3);
        Ok(())
    }

    #[test]
    fn test_where_in_clause() -> Result<()> {
        let builder = EnhancedSelectBuilder::new()
            .from("users")
            .select_all()
            .where_in("role", vec!["admin", "moderator", "user"])?;

        let sql = builder.to_sql();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("role"));
        assert!(sql.contains("IN"));

        let params = builder.get_parameters();
        assert_eq!(params.len(), 3);
        Ok(())
    }

    #[test]
    fn test_where_between_clause() -> Result<()> {
        let builder = EnhancedSelectBuilder::new()
            .from("products")
            .select_all()
            .where_between("price", 10.0, 100.0)?;

        let sql = builder.to_sql();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("price"));
        assert!(sql.contains("BETWEEN"));

        let params = builder.get_parameters();
        assert_eq!(params.len(), 2);
        Ok(())
    }

    #[test]
    fn test_where_null_clause() -> Result<()> {
        let builder = EnhancedSelectBuilder::new()
            .from("users")
            .select_all()
            .where_null("deleted_at")?;

        let sql = builder.to_sql();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("deleted_at"));
        assert!(sql.contains("IS NULL"));

        let params = builder.get_parameters();
        assert_eq!(params.len(), 0); // NULL checks have no parameters
        Ok(())
    }

    #[test]
    fn test_order_by_and_limit() -> Result<()> {
        let builder = EnhancedSelectBuilder::new()
            .from("users")
            .select_all()
            .where_("active", "=", true)?
            .order_by_desc("created_at")
            .limit(10)
            .offset(20);

        let sql = builder.to_sql();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("ORDER BY created_at DESC"));
        assert!(sql.contains("LIMIT 10"));
        assert!(sql.contains("OFFSET 20"));
        Ok(())
    }

    #[test]
    fn test_where_group() -> Result<()> {
        let builder = EnhancedSelectBuilder::new()
            .from("users")
            .select_all()
            .where_("status", "=", "active")?
            .where_group(|group| {
                group
                    .where_("age", ">", 18)?
                    .or_where("role", "=", "admin")
            })?;

        let sql = builder.to_sql();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("status"));
        assert!(sql.contains("age"));
        assert!(sql.contains("role"));
        assert!(sql.contains("(")); // Should have parentheses for grouping

        let params = builder.get_parameters();
        assert_eq!(params.len(), 3);
        Ok(())
    }

    #[test]
    fn test_validation() -> Result<()> {
        // Valid query
        let valid_builder = EnhancedSelectBuilder::new()
            .from("users")
            .select_all();
        assert!(valid_builder.validate().is_ok());

        // Invalid query - missing FROM
        let invalid_builder = EnhancedSelectBuilder::new()
            .select_all();
        assert!(invalid_builder.validate().is_err());

        Ok(())
    }

    #[test]
    fn test_parameter_types() -> Result<()> {
        let builder = EnhancedSelectBuilder::new()
            .from("users")
            .select_all()
            .where_("id", "=", 42i64)?
            .and_where("name", "=", "Alice")?
            .and_where("active", "=", true)?
            .and_where("score", "=", 95.5)?
            .and_where("embedding", "=", vec![1.0f32, 2.0f32, 3.0f32])?;

        let params = builder.get_parameters();
        assert_eq!(params.len(), 5);

        // Check parameter types
        let param_values: Vec<&QueryParameterValue> = params.iter().map(|p| &p.value).collect();
        assert!(matches!(param_values[0], QueryParameterValue::Int(_)));
        assert!(matches!(param_values[1], QueryParameterValue::String(_)));
        assert!(matches!(param_values[2], QueryParameterValue::Bool(_)));
        assert!(matches!(param_values[3], QueryParameterValue::Float(_)));
        assert!(matches!(param_values[4], QueryParameterValue::Vector(_)));

        Ok(())
    }

    #[test]
    fn test_empty_in_values() {
        let result = EnhancedSelectBuilder::new()
            .from("users")
            .select_all()
            .where_in("role", Vec::<&str>::new());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("at least one value"));
    }
}