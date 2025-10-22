/*!
WHERE clause construction with type safety and parameter binding.

This module provides a comprehensive WHERE clause builder that supports:
- Type-safe parameter binding to prevent SQL injection
- Multiple comparison operators (=, !=, <, >, <=, >=, LIKE, etc.)
- Logical operators (AND, OR, NOT)
- Array operations (IN, NOT IN)
- NULL checks (IS NULL, IS NOT NULL)
- BETWEEN clauses
- Complex grouping with parentheses
- Integration with existing zexec-engine parameter patterns
*/

use crate::query::{QueryParameter, QueryParameterValue};
use anyhow::{anyhow, Result};
use std::fmt;

/// Type-safe comparison operators
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Like,
    ILike,
    NotLike,
    In,
    NotIn,
    Between,
    NotBetween,
    IsNull,
    IsNotNull,
}

impl fmt::Display for ComparisonOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let op = match self {
            ComparisonOperator::Equal => "=",
            ComparisonOperator::NotEqual => "!=",
            ComparisonOperator::LessThan => "<",
            ComparisonOperator::LessThanOrEqual => "<=",
            ComparisonOperator::GreaterThan => ">",
            ComparisonOperator::GreaterThanOrEqual => ">=",
            ComparisonOperator::Like => "LIKE",
            ComparisonOperator::ILike => "ILIKE",
            ComparisonOperator::NotLike => "NOT LIKE",
            ComparisonOperator::In => "IN",
            ComparisonOperator::NotIn => "NOT IN",
            ComparisonOperator::Between => "BETWEEN",
            ComparisonOperator::NotBetween => "NOT BETWEEN",
            ComparisonOperator::IsNull => "IS NULL",
            ComparisonOperator::IsNotNull => "IS NOT NULL",
        };
        write!(f, "{}", op)
    }
}

/// Logical operators for combining conditions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalOperator {
    And,
    Or,
}

impl fmt::Display for LogicalOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let op = match self {
            LogicalOperator::And => "AND",
            LogicalOperator::Or => "OR",
        };
        write!(f, "{}", op)
    }
}

/// A single WHERE condition
#[derive(Debug, Clone)]
pub struct Condition {
    /// Column name
    pub column: String,
    /// Comparison operator
    pub operator: ComparisonOperator,
    /// Parameter values (empty for IS NULL/IS NOT NULL)
    pub values: Vec<QueryParameterValue>,
    /// Whether this condition is negated
    pub negated: bool,
}

impl Condition {
    /// Create a new condition with a single value
    pub fn new(
        column: impl Into<String>,
        operator: ComparisonOperator,
        value: impl Into<QueryParameterValue>,
    ) -> Self {
        Self {
            column: column.into(),
            operator,
            values: vec![value.into()],
            negated: false,
        }
    }

    /// Create a new condition with multiple values (for IN clauses)
    pub fn new_with_values(
        column: impl Into<String>,
        operator: ComparisonOperator,
        values: Vec<QueryParameterValue>,
    ) -> Self {
        Self {
            column: column.into(),
            operator,
            values,
            negated: false,
        }
    }

    /// Create a null check condition
    pub fn new_null_check(column: impl Into<String>, is_null: bool) -> Self {
        let operator = if is_null {
            ComparisonOperator::IsNull
        } else {
            ComparisonOperator::IsNotNull
        };

        Self {
            column: column.into(),
            operator,
            values: vec![],
            negated: false,
        }
    }

    /// Create a BETWEEN condition
    pub fn new_between(
        column: impl Into<String>,
        start: impl Into<QueryParameterValue>,
        end: impl Into<QueryParameterValue>,
        negated: bool,
    ) -> Self {
        let operator = if negated {
            ComparisonOperator::NotBetween
        } else {
            ComparisonOperator::Between
        };

        Self {
            column: column.into(),
            operator,
            values: vec![start.into(), end.into()],
            negated: false,
        }
    }

    /// Negate this condition
    pub fn negate(mut self) -> Self {
        self.negated = !self.negated;
        self
    }

    /// Validate the condition
    pub fn validate(&self) -> Result<()> {
        match &self.operator {
            ComparisonOperator::IsNull | ComparisonOperator::IsNotNull => {
                if !self.values.is_empty() {
                    return Err(anyhow!(
                        "NULL check conditions should not have values"
                    ));
                }
            }
            ComparisonOperator::Between | ComparisonOperator::NotBetween => {
                if self.values.len() != 2 {
                    return Err(anyhow!(
                        "BETWEEN conditions require exactly 2 values, got {}",
                        self.values.len()
                    ));
                }
            }
            ComparisonOperator::In | ComparisonOperator::NotIn => {
                if self.values.is_empty() {
                    return Err(anyhow!(
                        "IN conditions require at least 1 value"
                    ));
                }
            }
            _ => {
                if self.values.len() != 1 {
                    return Err(anyhow!(
                        "Comparison conditions require exactly 1 value, got {}",
                        self.values.len()
                    ));
                }
            }
        }

        // Validate column name is not empty
        if self.column.is_empty() {
            return Err(anyhow!("Column name cannot be empty"));
        }

        // Basic SQL injection protection - check for suspicious patterns
        if self.column.contains(';') || self.column.contains("--") || self.column.contains("/*") {
            return Err(anyhow!(
                "Column name contains suspicious characters: {}",
                self.column
            ));
        }

        Ok(())
    }
}

/// A group of conditions that can be combined with logical operators
#[derive(Debug, Clone)]
pub enum ConditionGroup {
    /// A single condition
    Condition(Condition),
    /// A group of conditions combined with a logical operator
    Group {
        conditions: Vec<ConditionGroup>,
        operator: LogicalOperator,
        negated: bool,
    },
}

impl ConditionGroup {
    /// Create a new condition group
    pub fn new(operator: LogicalOperator) -> Self {
        Self::Group {
            conditions: Vec::new(),
            operator,
            negated: false,
        }
    }

    /// Add a condition to this group
    pub fn add_condition(mut self, condition: Condition) -> Self {
        match self {
            ConditionGroup::Group { ref mut conditions, .. } => {
                conditions.push(ConditionGroup::Condition(condition));
                self
            }
            ConditionGroup::Condition(existing) => {
                // Convert single condition to a group
                ConditionGroup::Group {
                    conditions: vec![
                        ConditionGroup::Condition(existing),
                        ConditionGroup::Condition(condition),
                    ],
                    operator: LogicalOperator::And, // Default to AND
                    negated: false,
                }
            }
        }
    }

    /// Add a condition group to this group
    pub fn add_group(mut self, group: ConditionGroup) -> Self {
        match self {
            ConditionGroup::Group { ref mut conditions, .. } => {
                conditions.push(group);
                self
            }
            ConditionGroup::Condition(existing) => {
                // Convert single condition to a group
                ConditionGroup::Group {
                    conditions: vec![
                        ConditionGroup::Condition(existing),
                        group,
                    ],
                    operator: LogicalOperator::And, // Default to AND
                    negated: false,
                }
            }
        }
    }

    /// Negate this condition group
    pub fn negate(mut self) -> Self {
        match self {
            ConditionGroup::Condition(ref mut condition) => {
                *condition = condition.clone().negate();
                self
            }
            ConditionGroup::Group { ref mut negated, .. } => {
                *negated = !*negated;
                self
            }
        }
    }

    /// Validate all conditions in this group
    pub fn validate(&self) -> Result<()> {
        match self {
            ConditionGroup::Condition(condition) => condition.validate(),
            ConditionGroup::Group { conditions, .. } => {
                if conditions.is_empty() {
                    return Err(anyhow!("Condition group cannot be empty"));
                }
                for condition in conditions {
                    condition.validate()?;
                }
                Ok(())
            }
        }
    }

    /// Get all parameters from this condition group
    pub fn get_parameters(&self) -> Vec<QueryParameter> {
        match self {
            ConditionGroup::Condition(condition) => {
                condition.values.iter()
                    .enumerate()
                    .map(|(i, value)| QueryParameter {
                        name: format!("{}_{}", condition.column, i),
                        value: value.clone(),
                    })
                    .collect()
            }
            ConditionGroup::Group { conditions, .. } => {
                conditions.iter()
                    .flat_map(|group| group.get_parameters())
                    .collect()
            }
        }
    }

    /// Convert to SQL string with parameter placeholders
    pub fn to_sql(&self, param_prefix: &str) -> String {
        match self {
            ConditionGroup::Condition(condition) => {
                self.condition_to_sql(condition, param_prefix)
            }
            ConditionGroup::Group { conditions, operator, negated } => {
                if conditions.is_empty() {
                    return "1=1".to_string(); // Always true condition
                }

                let sql_parts: Vec<String> = conditions.iter()
                    .map(|group| group.to_sql(param_prefix))
                    .collect();

                let combined = sql_parts.join(&format!(" {} ", operator));
                let result = if conditions.len() > 1 {
                    format!("({})", combined)
                } else {
                    combined
                };

                if *negated {
                    format!("NOT {}", result)
                } else {
                    result
                }
            }
        }
    }

    fn condition_to_sql(&self, condition: &Condition, param_prefix: &str) -> String {
        let column = &condition.column;

        match &condition.operator {
            ComparisonOperator::IsNull | ComparisonOperator::IsNotNull => {
                let op = if condition.negated {
                    match condition.operator {
                        ComparisonOperator::IsNull => "IS NOT NULL",
                        ComparisonOperator::IsNotNull => "IS NULL",
                        _ => unreachable!(),
                    }
                } else {
                    match condition.operator {
                        ComparisonOperator::IsNull => "IS NULL",
                        ComparisonOperator::IsNotNull => "IS NOT NULL",
                        _ => unreachable!(),
                    }
                };
                format!("{} {}", column, op)
            }
            ComparisonOperator::Between | ComparisonOperator::NotBetween => {
                let op = if condition.negated {
                    match condition.operator {
                        ComparisonOperator::Between => "NOT BETWEEN",
                        ComparisonOperator::NotBetween => "BETWEEN",
                        _ => unreachable!(),
                    }
                } else {
                    match condition.operator {
                        ComparisonOperator::Between => "BETWEEN",
                        ComparisonOperator::NotBetween => "NOT BETWEEN",
                        _ => unreachable!(),
                    }
                };
                format!("{} {} {}{}_0 AND {}{}_1",
                    column, op, param_prefix, column, param_prefix, column)
            }
            ComparisonOperator::In | ComparisonOperator::NotIn => {
                let op = if condition.negated {
                    match condition.operator {
                        ComparisonOperator::In => "NOT IN",
                        ComparisonOperator::NotIn => "IN",
                        _ => unreachable!(),
                    }
                } else {
                    match condition.operator {
                        ComparisonOperator::In => "IN",
                        ComparisonOperator::NotIn => "NOT IN",
                        _ => unreachable!(),
                    }
                };
                let placeholders: Vec<String> = (0..condition.values.len())
                    .map(|i| format!("{}{}_{}",param_prefix, column, i))
                    .collect();
                format!("{} {} ({})", column, op, placeholders.join(", "))
            }
            _ => {
                let op = if condition.negated {
                    match condition.operator {
                        ComparisonOperator::Equal => "!=",
                        ComparisonOperator::NotEqual => "=",
                        ComparisonOperator::LessThan => ">=",
                        ComparisonOperator::LessThanOrEqual => ">",
                        ComparisonOperator::GreaterThan => "<=",
                        ComparisonOperator::GreaterThanOrEqual => "<",
                        ComparisonOperator::Like => "NOT LIKE",
                        ComparisonOperator::ILike => "NOT ILIKE",
                        ComparisonOperator::NotLike => "LIKE",
                        _ => {
                            match condition.operator {
                                ComparisonOperator::Equal => "=",
                                ComparisonOperator::NotEqual => "!=",
                                ComparisonOperator::LessThan => "<",
                                ComparisonOperator::LessThanOrEqual => "<=",
                                ComparisonOperator::GreaterThan => ">",
                                ComparisonOperator::GreaterThanOrEqual => ">=",
                                ComparisonOperator::Like => "LIKE",
                                ComparisonOperator::ILike => "ILIKE",
                                ComparisonOperator::NotLike => "NOT LIKE",
                                _ => "=", // fallback
                            }
                        },
                    }
                } else {
                    match condition.operator {
                        ComparisonOperator::Equal => "=",
                        ComparisonOperator::NotEqual => "!=",
                        ComparisonOperator::LessThan => "<",
                        ComparisonOperator::LessThanOrEqual => "<=",
                        ComparisonOperator::GreaterThan => ">",
                        ComparisonOperator::GreaterThanOrEqual => ">=",
                        ComparisonOperator::Like => "LIKE",
                        ComparisonOperator::ILike => "ILIKE",
                        ComparisonOperator::NotLike => "NOT LIKE",
                        _ => "=", // fallback
                    }
                };
                format!("{} {} {}{}_0", column, op, param_prefix, column)
            }
        }
    }
}

/// WHERE clause builder that provides a fluent API for building complex conditions
#[derive(Debug, Clone)]
pub struct WhereClauseBuilder {
    /// Root condition group
    conditions: Option<ConditionGroup>,
    /// Parameter counter for unique parameter names
    param_counter: u32,
}

impl WhereClauseBuilder {
    /// Create a new WHERE clause builder
    pub fn new() -> Self {
        Self {
            conditions: None,
            param_counter: 0,
        }
    }

    /// Add a basic WHERE condition
    pub fn where_(
        mut self,
        column: &str,
        operator: &str,
        value: impl Into<QueryParameterValue>,
    ) -> Result<Self> {
        let op = self.parse_operator(operator)?;
        let condition = Condition::new(column, op, value);
        condition.validate()?;

        self.conditions = Some(match self.conditions {
            Some(existing) => existing.add_condition(condition),
            None => ConditionGroup::Condition(condition),
        });

        Ok(self)
    }

    /// Add an AND WHERE condition
    pub fn and_where(
        mut self,
        column: &str,
        operator: &str,
        value: impl Into<QueryParameterValue>,
    ) -> Result<Self> {
        let op = self.parse_operator(operator)?;
        let condition = Condition::new(column, op, value);
        condition.validate()?;

        self.conditions = Some(match self.conditions {
            Some(existing) => {
                // If existing is a single condition or an OR group, wrap in AND group
                match existing {
                    ConditionGroup::Group { operator: LogicalOperator::And, .. } => {
                        existing.add_condition(condition)
                    }
                    _ => {
                        let mut and_group = ConditionGroup::new(LogicalOperator::And);
                        and_group = and_group.add_group(existing);
                        and_group.add_condition(condition)
                    }
                }
            }
            None => ConditionGroup::Condition(condition),
        });

        Ok(self)
    }

    /// Add an OR WHERE condition
    pub fn or_where(
        mut self,
        column: &str,
        operator: &str,
        value: impl Into<QueryParameterValue>,
    ) -> Result<Self> {
        let op = self.parse_operator(operator)?;
        let condition = Condition::new(column, op, value);
        condition.validate()?;

        self.conditions = Some(match self.conditions {
            Some(existing) => {
                // If existing is a single condition or an AND group, wrap in OR group
                match existing {
                    ConditionGroup::Group { operator: LogicalOperator::Or, .. } => {
                        existing.add_condition(condition)
                    }
                    _ => {
                        let mut or_group = ConditionGroup::new(LogicalOperator::Or);
                        or_group = or_group.add_group(existing);
                        or_group.add_condition(condition)
                    }
                }
            }
            None => ConditionGroup::Condition(condition),
        });

        Ok(self)
    }

    /// Add a WHERE IN condition
    pub fn where_in(
        mut self,
        column: &str,
        values: Vec<impl Into<QueryParameterValue>>,
    ) -> Result<Self> {
        if values.is_empty() {
            return Err(anyhow!("WHERE IN requires at least one value"));
        }

        let param_values: Vec<QueryParameterValue> = values.into_iter()
            .map(|v| v.into())
            .collect();

        let condition = Condition::new_with_values(column, ComparisonOperator::In, param_values);
        condition.validate()?;

        self.conditions = Some(match self.conditions {
            Some(existing) => existing.add_condition(condition),
            None => ConditionGroup::Condition(condition),
        });

        Ok(self)
    }

    /// Add a WHERE NOT IN condition
    pub fn where_not_in(
        mut self,
        column: &str,
        values: Vec<impl Into<QueryParameterValue>>,
    ) -> Result<Self> {
        if values.is_empty() {
            return Err(anyhow!("WHERE NOT IN requires at least one value"));
        }

        let param_values: Vec<QueryParameterValue> = values.into_iter()
            .map(|v| v.into())
            .collect();

        let condition = Condition::new_with_values(column, ComparisonOperator::NotIn, param_values);
        condition.validate()?;

        self.conditions = Some(match self.conditions {
            Some(existing) => existing.add_condition(condition),
            None => ConditionGroup::Condition(condition),
        });

        Ok(self)
    }

    /// Add a WHERE BETWEEN condition
    pub fn where_between(
        mut self,
        column: &str,
        start: impl Into<QueryParameterValue>,
        end: impl Into<QueryParameterValue>,
    ) -> Result<Self> {
        let condition = Condition::new_between(column, start, end, false);
        condition.validate()?;

        self.conditions = Some(match self.conditions {
            Some(existing) => existing.add_condition(condition),
            None => ConditionGroup::Condition(condition),
        });

        Ok(self)
    }

    /// Add a WHERE NOT BETWEEN condition
    pub fn where_not_between(
        mut self,
        column: &str,
        start: impl Into<QueryParameterValue>,
        end: impl Into<QueryParameterValue>,
    ) -> Result<Self> {
        let condition = Condition::new_between(column, start, end, true);
        condition.validate()?;

        self.conditions = Some(match self.conditions {
            Some(existing) => existing.add_condition(condition),
            None => ConditionGroup::Condition(condition),
        });

        Ok(self)
    }

    /// Add a WHERE IS NULL condition
    pub fn where_null(mut self, column: &str) -> Result<Self> {
        let condition = Condition::new_null_check(column, true);
        condition.validate()?;

        self.conditions = Some(match self.conditions {
            Some(existing) => existing.add_condition(condition),
            None => ConditionGroup::Condition(condition),
        });

        Ok(self)
    }

    /// Add a WHERE IS NOT NULL condition
    pub fn where_not_null(mut self, column: &str) -> Result<Self> {
        let condition = Condition::new_null_check(column, false);
        condition.validate()?;

        self.conditions = Some(match self.conditions {
            Some(existing) => existing.add_condition(condition),
            None => ConditionGroup::Condition(condition),
        });

        Ok(self)
    }

    /// Add an AND WHERE IS NOT NULL condition
    pub fn and_where_not_null(mut self, column: &str) -> Result<Self> {
        let condition = Condition::new_null_check(column, false);
        condition.validate()?;

        self.conditions = Some(match self.conditions {
            Some(existing) => {
                // If existing is a single condition or an OR group, wrap in AND group
                match existing {
                    ConditionGroup::Group { operator: LogicalOperator::And, .. } => {
                        existing.add_condition(condition)
                    }
                    _ => {
                        let mut and_group = ConditionGroup::new(LogicalOperator::And);
                        and_group = and_group.add_group(existing);
                        and_group.add_condition(condition)
                    }
                }
            }
            None => ConditionGroup::Condition(condition),
        });

        Ok(self)
    }

    /// Start a new condition group with parentheses
    pub fn where_group(mut self, group_builder: impl FnOnce(WhereClauseBuilder) -> Result<WhereClauseBuilder>) -> Result<Self> {
        let group = group_builder(WhereClauseBuilder::new())?;

        if let Some(group_conditions) = group.conditions {
            group_conditions.validate()?;

            self.conditions = Some(match self.conditions {
                Some(existing) => existing.add_group(group_conditions),
                None => group_conditions,
            });
        }

        Ok(self)
    }

    /// Parse string operator to ComparisonOperator enum
    fn parse_operator(&self, operator: &str) -> Result<ComparisonOperator> {
        match operator.to_uppercase().as_str() {
            "=" | "EQ" => Ok(ComparisonOperator::Equal),
            "!=" | "<>" | "NE" => Ok(ComparisonOperator::NotEqual),
            "<" | "LT" => Ok(ComparisonOperator::LessThan),
            "<=" | "LE" => Ok(ComparisonOperator::LessThanOrEqual),
            ">" | "GT" => Ok(ComparisonOperator::GreaterThan),
            ">=" | "GE" => Ok(ComparisonOperator::GreaterThanOrEqual),
            "LIKE" => Ok(ComparisonOperator::Like),
            "ILIKE" => Ok(ComparisonOperator::ILike),
            "NOT LIKE" => Ok(ComparisonOperator::NotLike),
            "IN" => Ok(ComparisonOperator::In),
            "NOT IN" => Ok(ComparisonOperator::NotIn),
            "BETWEEN" => Ok(ComparisonOperator::Between),
            "NOT BETWEEN" => Ok(ComparisonOperator::NotBetween),
            "IS NULL" => Ok(ComparisonOperator::IsNull),
            "IS NOT NULL" => Ok(ComparisonOperator::IsNotNull),
            _ => Err(anyhow!("Unsupported operator: {}", operator)),
        }
    }

    /// Convert to SQL string with parameter placeholders
    pub fn to_sql(&self) -> String {
        match &self.conditions {
            Some(conditions) => {
                conditions.validate().unwrap_or_else(|e| {
                    tracing::warn!("WHERE clause validation failed: {}", e);
                });
                conditions.to_sql("$")
            }
            None => "1=1".to_string(), // Always true when no conditions
        }
    }

    /// Get all parameters from the WHERE clause
    pub fn get_parameters(&self) -> Vec<QueryParameter> {
        match &self.conditions {
            Some(conditions) => conditions.get_parameters(),
            None => Vec::new(),
        }
    }

    /// Validate the entire WHERE clause
    pub fn validate(&self) -> Result<()> {
        if let Some(conditions) = &self.conditions {
            conditions.validate()?;
        }
        Ok(())
    }

    /// Check if the WHERE clause has any conditions
    pub fn has_conditions(&self) -> bool {
        self.conditions.is_some()
    }

    /// Clone the current state for chaining
    pub fn clone_state(&self) -> Self {
        Self {
            conditions: self.conditions.clone(),
            param_counter: self.param_counter,
        }
    }
}

impl Default for WhereClauseBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_comparison_operators() {
        assert_eq!(ComparisonOperator::Equal.to_string(), "=");
        assert_eq!(ComparisonOperator::NotEqual.to_string(), "!=");
        assert_eq!(ComparisonOperator::Like.to_string(), "LIKE");
        assert_eq!(ComparisonOperator::In.to_string(), "IN");
        assert_eq!(ComparisonOperator::IsNull.to_string(), "IS NULL");
    }

    #[test]
    fn test_logical_operators() {
        assert_eq!(LogicalOperator::And.to_string(), "AND");
        assert_eq!(LogicalOperator::Or.to_string(), "OR");
    }

    #[test]
    fn test_simple_condition() {
        let condition = Condition::new("age", ComparisonOperator::GreaterThan, 18i64);
        assert!(condition.validate().is_ok());
        assert_eq!(condition.column, "age");
        assert_eq!(condition.operator, ComparisonOperator::GreaterThan);
        assert_eq!(condition.values.len(), 1);
        assert!(!condition.negated);
    }

    #[test]
    fn test_null_condition() {
        let condition = Condition::new_null_check("email", true);
        assert!(condition.validate().is_ok());
        assert_eq!(condition.column, "email");
        assert_eq!(condition.operator, ComparisonOperator::IsNull);
        assert!(condition.values.is_empty());
    }

    #[test]
    fn test_between_condition() {
        let condition = Condition::new_between("price", 10.0, 100.0, false);
        assert!(condition.validate().is_ok());
        assert_eq!(condition.column, "price");
        assert_eq!(condition.operator, ComparisonOperator::Between);
        assert_eq!(condition.values.len(), 2);
    }

    #[test]
    fn test_in_condition() {
        let values = vec![
            QueryParameterValue::from("admin"),
            QueryParameterValue::from("user"),
        ];
        let condition = Condition::new_with_values("role", ComparisonOperator::In, values);
        assert!(condition.validate().is_ok());
        assert_eq!(condition.column, "role");
        assert_eq!(condition.operator, ComparisonOperator::In);
        assert_eq!(condition.values.len(), 2);
    }

    #[test]
    fn test_condition_validation() {
        // Test empty column name
        let condition = Condition::new("", ComparisonOperator::Equal, "test");
        assert!(condition.validate().is_err());

        // Test SQL injection patterns
        let condition = Condition::new("col; DROP TABLE users--", ComparisonOperator::Equal, "test");
        assert!(condition.validate().is_err());

        // Test BETWEEN with wrong number of values
        let mut condition = Condition::new("age", ComparisonOperator::Between, 18);
        condition.values.clear(); // Remove the value
        assert!(condition.validate().is_err());
    }

    #[test]
    fn test_where_clause_builder() -> Result<()> {
        let where_clause = WhereClauseBuilder::new()
            .where_("age", ">", 18i64)?
            .and_where("status", "=", "active")?
            .or_where("role", "=", "admin")?;

        assert!(where_clause.has_conditions());
        assert!(where_clause.validate().is_ok());

        let sql = where_clause.to_sql();
        assert!(!sql.is_empty());
        assert!(sql.contains("age"));
        assert!(sql.contains("status"));
        assert!(sql.contains("role"));

        Ok(())
    }

    #[test]
    fn test_where_in_clause() -> Result<()> {
        let values = vec!["admin", "moderator", "user"];
        let where_clause = WhereClauseBuilder::new()
            .where_in("role", values)?;

        assert!(where_clause.has_conditions());
        let sql = where_clause.to_sql();
        assert!(sql.contains("role IN"));
        assert!(sql.contains("$role_0"));
        assert!(sql.contains("$role_1"));
        assert!(sql.contains("$role_2"));

        let params = where_clause.get_parameters();
        assert_eq!(params.len(), 3);

        Ok(())
    }

    #[test]
    fn test_where_between_clause() -> Result<()> {
        let where_clause = WhereClauseBuilder::new()
            .where_between("age", 18, 65)?;

        assert!(where_clause.has_conditions());
        let sql = where_clause.to_sql();
        assert!(sql.contains("age BETWEEN"));
        assert!(sql.contains("$age_0"));
        assert!(sql.contains("$age_1"));

        let params = where_clause.get_parameters();
        assert_eq!(params.len(), 2);

        Ok(())
    }

    #[test]
    fn test_where_null_clause() -> Result<()> {
        let where_clause = WhereClauseBuilder::new()
            .where_null("deleted_at")?;

        assert!(where_clause.has_conditions());
        let sql = where_clause.to_sql();
        assert!(sql.contains("deleted_at IS NULL"));

        let params = where_clause.get_parameters();
        assert_eq!(params.len(), 0); // NULL checks have no parameters

        Ok(())
    }

    #[test]
    fn test_complex_where_group() -> Result<()> {
        let where_clause = WhereClauseBuilder::new()
            .where_("status", "=", "active")?
            .where_group(|group| {
                group
                    .where_("age", ">", 18)?
                    .or_where("role", "=", "admin")
            })?;

        assert!(where_clause.has_conditions());
        let sql = where_clause.to_sql();
        assert!(sql.contains("status"));
        assert!(sql.contains("age"));
        assert!(sql.contains("role"));
        assert!(sql.contains("(")); // Should have parentheses for grouping

        Ok(())
    }

    #[test]
    fn test_operator_parsing() {
        let builder = WhereClauseBuilder::new();

        assert_eq!(builder.parse_operator("=").unwrap(), ComparisonOperator::Equal);
        assert_eq!(builder.parse_operator("EQ").unwrap(), ComparisonOperator::Equal);
        assert_eq!(builder.parse_operator("!=").unwrap(), ComparisonOperator::NotEqual);
        assert_eq!(builder.parse_operator("<>").unwrap(), ComparisonOperator::NotEqual);
        assert_eq!(builder.parse_operator("LIKE").unwrap(), ComparisonOperator::Like);
        assert_eq!(builder.parse_operator("like").unwrap(), ComparisonOperator::Like);

        assert!(builder.parse_operator("INVALID").is_err());
    }

    #[test]
    fn test_parameter_types() {
        let builder = WhereClauseBuilder::new();

        // Test different parameter types
        let _clause1 = builder.clone_state().where_("id", "=", 42i64).unwrap();
        let _clause2 = builder.clone_state().where_("name", "=", "Alice").unwrap();
        let _clause3 = builder.clone_state().where_("active", "=", true).unwrap();
        let _clause4 = builder.clone_state().where_("price", "=", 19.99).unwrap();
        let _clause5 = builder.clone_state().where_("embedding", "=", vec![1.0f32, 2.0f32]).unwrap();
    }

    #[test]
    fn test_empty_where_clause() {
        let where_clause = WhereClauseBuilder::new();
        assert!(!where_clause.has_conditions());
        assert_eq!(where_clause.to_sql(), "1=1"); // Should return always-true condition
        assert!(where_clause.get_parameters().is_empty());
    }

    #[test]
    fn test_condition_negation() {
        let condition = Condition::new("age", ComparisonOperator::GreaterThan, 18)
            .negate();
        assert!(condition.negated);

        // Test SQL generation with negation through condition group
        let group = ConditionGroup::Condition(condition);
        let sql = group.to_sql("$");
        // The negation should be handled in the SQL generation
        assert!(sql.contains("age"));
    }

    #[test]
    fn test_empty_in_values() {
        let result = WhereClauseBuilder::new()
            .where_in("role", Vec::<&str>::new());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("at least one value"));
    }
}