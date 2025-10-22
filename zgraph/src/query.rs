//! Query execution interface that integrates zexec-engine with zgraph database types
//!
//! This module provides a unified interface for executing SQL queries through
//! the zexec-engine while maintaining compatibility with existing zgraph database
//! implementations.

use crate::error::{ZgError, ZgResult};
use crate::fjall_db::FjDatabase;
use crate::types::Row;
use indexmap::IndexMap;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Query execution result compatible with zgraph types
#[derive(Debug, Clone)]
pub enum QueryResult {
    /// No result (e.g., from DDL statements)
    None,
    /// Rows returned from SELECT queries
    Rows(Vec<Row>),
    /// Number of affected rows (e.g., from INSERT, UPDATE, DELETE)
    Affected(u64),
}

impl QueryResult {
    /// Convert zexec-engine ExecResult to zgraph QueryResult
    pub fn from_exec_result(exec_result: zexec_engine::ExecResult) -> ZgResult<Self> {
        match exec_result {
            zexec_engine::ExecResult::None => Ok(QueryResult::None),
            zexec_engine::ExecResult::Rows(rows) => {
                let converted_rows = rows
                    .into_iter()
                    .map(|row| convert_exec_row_to_zgraph_row(row))
                    .collect::<ZgResult<Vec<_>>>()?;
                Ok(QueryResult::Rows(converted_rows))
            }
            zexec_engine::ExecResult::Affected(count) => Ok(QueryResult::Affected(count)),
        }
    }

    /// Get the number of affected rows if applicable
    pub fn affected_rows(&self) -> Option<u64> {
        match self {
            QueryResult::Affected(count) => Some(*count),
            _ => None,
        }
    }

    /// Get the rows if this is a row result
    pub fn rows(&self) -> Option<&Vec<Row>> {
        match self {
            QueryResult::Rows(rows) => Some(rows),
            _ => None,
        }
    }
}

/// Convert a row from zexec-engine format to zgraph Row format
fn convert_exec_row_to_zgraph_row(
    exec_row: IndexMap<String, JsonValue>,
) -> ZgResult<Row> {
    let mut row = Row::default();
    for (key, value) in exec_row {
        let zgraph_value = convert_json_value_to_zgraph_value(value)?;
        row.insert(key, zgraph_value);
    }
    Ok(row)
}

/// Convert JSON value from zexec-engine to zgraph Value type
fn convert_json_value_to_zgraph_value(value: JsonValue) -> ZgResult<crate::types::Value> {
    match value {
        JsonValue::Null => Ok(crate::types::Value::Null),
        JsonValue::Bool(b) => Ok(crate::types::Value::Bool(b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(crate::types::Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(crate::types::Value::Float(f))
            } else {
                Err(ZgError::Invalid(format!("Unsupported number format: {}", n)))
            }
        }
        JsonValue::String(s) => Ok(crate::types::Value::Text(s)),
        JsonValue::Array(ref arr) => {
            // Try to interpret as vector
            let vec_result: Result<Vec<f32>, _> = arr
                .into_iter()
                .map(|v| match v {
                    JsonValue::Number(n) => n.as_f64().ok_or_else(|| {
                        ZgError::Invalid("Array contains non-numeric values".to_string())
                    }) as Result<f64, ZgError>,
                    _ => Err(ZgError::Invalid("Array contains non-numeric values".to_string())),
                })
                .collect::<Result<Vec<f64>, _>>()
                .map(|float_vec| float_vec.into_iter().map(|f| f as f32).collect());

            match vec_result {
                Ok(f32_vec) => {
                    Ok(crate::types::Value::Vector(f32_vec))
                }
                Err(_) => {
                    // If not a numeric vector, store as JSON
                    Ok(crate::types::Value::Json(value))
                }
            }
        }
        JsonValue::Object(_) => Ok(crate::types::Value::Json(value)),
    }
}

/// Trait for database implementations that can execute SQL queries
pub trait QueryExecutor {
    /// Execute a SQL query and return results
    fn execute_sql(&self, sql: &str) -> ZgResult<QueryResult>;

    /// Execute a SQL query with user context for permission checking
    fn execute_sql_as_user(&self, sql: &str, user: &str) -> ZgResult<QueryResult>;
}

/// In-memory Database implementation with query execution
impl QueryExecutor for crate::Database {
    fn execute_sql(&self, sql: &str) -> ZgResult<QueryResult> {
        self.execute_sql_as_user(sql, "admin") // Default to admin for in-memory
    }

    fn execute_sql_as_user(&self, sql: &str, _user: &str) -> ZgResult<QueryResult> {
        // For in-memory database, we use the basic exec_sql function from zexec-engine
        let exec_result = zexec_engine::exec_sql(sql)
            .map_err(|e| ZgError::Anyhow(e))?;
        QueryResult::from_exec_result(exec_result)
    }
}

/// FjDatabase (persistent) implementation with query execution
impl QueryExecutor for FjDatabase {
    fn execute_sql(&self, sql: &str) -> ZgResult<QueryResult> {
        self.execute_sql_as_user(sql, "admin") // Default to admin
    }

    fn execute_sql_as_user(&self, sql: &str, user: &str) -> ZgResult<QueryResult> {
        // For persistent database, we use the storage-aware execution
        use zcore_catalog as zcat;
        use zcore_storage as zs;

        let catalog = zcat::Catalog::new(&self.store);

        // Note: zexec-engine doesn't currently support user context in exec_sql_store
        // We'll need to extend it or implement permission checking here
        let exec_result = zexec_engine::exec_sql_store(sql, self.org_id, &self.store, &catalog)
            .map_err(|e| ZgError::Anyhow(e))?;
        QueryResult::from_exec_result(exec_result)
    }
}

/// Convenience functions for common query patterns
pub struct QueryHelpers;

impl QueryHelpers {
    /// Execute a SELECT query and return rows
    pub fn select_all(db: &dyn QueryExecutor, table: &str) -> ZgResult<Vec<Row>> {
        let sql = format!("SELECT * FROM {}", table);
        match db.execute_sql(&sql)? {
            QueryResult::Rows(rows) => Ok(rows),
            _ => Err(ZgError::Invalid("Expected rows from SELECT query".to_string())),
        }
    }

    /// Execute a SELECT query with WHERE clause and return rows
    pub fn select_where(db: &dyn QueryExecutor, table: &str, where_clause: &str) -> ZgResult<Vec<Row>> {
        let sql = format!("SELECT * FROM {} WHERE {}", table, where_clause);
        match db.execute_sql(&sql)? {
            QueryResult::Rows(rows) => Ok(rows),
            _ => Err(ZgError::Invalid("Expected rows from SELECT query".to_string())),
        }
    }

    /// Execute a KNN query for vector similarity
    pub fn knn_search(
        db: &dyn QueryExecutor,
        table: &str,
        vector_column: &str,
        query_vector: &[f32],
        k: usize,
    ) -> ZgResult<Vec<Row>> {
        let vector_str = query_vector
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT * FROM {} ORDER BY {} <-> ARRAY[{}] LIMIT {}",
            table, vector_column, vector_str, k
        );
        match db.execute_sql(&sql)? {
            QueryResult::Rows(rows) => Ok(rows),
            _ => Err(ZgError::Invalid("Expected rows from KNN query".to_string())),
        }
    }

    /// Execute an INSERT statement
    pub fn insert(db: &dyn QueryExecutor, table: &str, values: &str) -> ZgResult<u64> {
        let sql = format!("INSERT INTO {} VALUES {}", table, values);
        match db.execute_sql(&sql)? {
            QueryResult::Affected(count) => Ok(count),
            _ => Err(ZgError::Invalid("Expected affected count from INSERT".to_string())),
        }
    }

    /// Execute a CREATE TABLE statement
    pub fn create_table(db: &dyn QueryExecutor, table_def: &str) -> ZgResult<()> {
        let sql = format!("CREATE TABLE {}", table_def);
        match db.execute_sql(&sql)? {
            QueryResult::None => Ok(()),
            _ => Err(ZgError::Invalid("Expected no result from CREATE TABLE".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    #[test]
    fn test_convert_json_value_to_zgraph_value() {
        // Test basic conversions
        assert_eq!(
            convert_json_value_to_zgraph_value(JsonValue::Null).unwrap(),
            Value::Null
        );
        assert_eq!(
            convert_json_value_to_zgraph_value(JsonValue::Bool(true)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            convert_json_value_to_zgraph_value(JsonValue::Number(42.into())).unwrap(),
            Value::Int(42)
        );
        assert_eq!(
            convert_json_value_to_zgraph_value(JsonValue::Number(3.14.into())).unwrap(),
            Value::Float(3.14)
        );
        assert_eq!(
            convert_json_value_to_zgraph_value(JsonValue::String("test".to_string())).unwrap(),
            Value::Text("test".to_string())
        );

        // Test vector conversion
        let json_array = JsonValue::Array(vec![1.0.into(), 2.0.into(), 3.0.into()]);
        assert_eq!(
            convert_json_value_to_zgraph_value(json_array).unwrap(),
            Value::Vector(vec![1.0, 2.0, 3.0])
        );
    }

    #[test]
    fn test_query_result_from_exec_result() {
        // Test None result
        let none_result = QueryResult::from_exec_result(zexec_engine::ExecResult::None).unwrap();
        assert!(matches!(none_result, QueryResult::None));

        // Test Affected result
        let affected_result =
            QueryResult::from_exec_result(zexec_engine::ExecResult::Affected(5)).unwrap();
        assert_eq!(affected_result.affected_rows(), Some(5));
    }
}