/*!
Integration tests for WHERE clause builder with SELECT queries.

This module tests the complete integration of the WHERE clause builder
with the SELECT query builders, ensuring type safety and proper SQL generation.
*/

#[cfg(test)]
mod tests {
    use crate::{
        enhanced_select::EnhancedSelectBuilder,
        where_clause::{WhereClauseBuilder, ComparisonOperator, LogicalOperator},
        query::QueryParameterValue,
        Result,
    };

    #[test]
    fn test_enhanced_select_basic_where() -> Result<()> {
        let query = EnhancedSelectBuilder::new()
            .from("users")
            .select(&["id", "name", "email"])
            .where_("age", ">", 18i64)?
            .and_where("status", "=", "active")?;

        let sql = query.to_sql();
        assert!(sql.contains("SELECT id, name, email FROM users"));
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("age"));
        assert!(sql.contains("status"));

        let params = query.get_parameters();
        assert_eq!(params.len(), 2);
        assert_eq!(params[0].name.contains("age"), true);
        assert_eq!(params[1].name.contains("status"), true);

        match &params[0].value {
            QueryParameterValue::Int(value) => assert_eq!(*value, 18),
            _ => panic!("Expected Int parameter"),
        }

        match &params[1].value {
            QueryParameterValue::String(value) => assert_eq!(value, "active"),
            _ => panic!("Expected String parameter"),
        }

        Ok(())
    }

    #[test]
    fn test_enhanced_select_complex_where() -> Result<()> {
        let query = EnhancedSelectBuilder::new()
            .from("products")
            .select_all()
            .where_between("price", 10.0, 100.0)?
            .and_where("category", "=", "electronics")?
            .or_where("featured", "=", true)?;

        let sql = query.to_sql();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("price"));
        assert!(sql.contains("BETWEEN"));
        assert!(sql.contains("category"));
        assert!(sql.contains("featured"));

        let params = query.get_parameters();
        // price BETWEEN has 2 params, category has 1, featured has 1 = 4 total
        assert_eq!(params.len(), 4);

        Ok(())
    }

    #[test]
    fn test_where_in_with_multiple_values() -> Result<()> {
        let roles = vec!["admin", "moderator", "editor"];
        let query = EnhancedSelectBuilder::new()
            .from("users")
            .select(&["id", "username", "role"])
            .where_in("role", roles)?;

        let sql = query.to_sql();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("role"));
        assert!(sql.contains("IN"));

        let params = query.get_parameters();
        assert_eq!(params.len(), 3);

        // Check that all role values are properly bound
        let param_values: Vec<&str> = params.iter()
            .filter_map(|p| match &p.value {
                QueryParameterValue::String(s) => Some(s.as_str()),
                _ => None,
            })
            .collect();

        assert!(param_values.contains(&"admin"));
        assert!(param_values.contains(&"moderator"));
        assert!(param_values.contains(&"editor"));

        Ok(())
    }

    #[test]
    fn test_where_null_conditions() -> Result<()> {
        let query = EnhancedSelectBuilder::new()
            .from("users")
            .select_all()
            .where_null("deleted_at")?
            .and_where_not_null("email")?;

        let sql = query.to_sql();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("deleted_at IS NULL"));
        assert!(sql.contains("email IS NOT NULL"));

        let params = query.get_parameters();
        // NULL checks don't have parameters
        assert_eq!(params.len(), 0);

        Ok(())
    }

    #[test]
    fn test_where_group_complex_logic() -> Result<()> {
        let query = EnhancedSelectBuilder::new()
            .from("orders")
            .select_all()
            .where_("status", "=", "pending")?
            .where_group(|group| {
                group
                    .where_("priority", "=", "high")?
                    .or_where("customer_tier", "=", "premium")
            })?;

        let sql = query.to_sql();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("status"));
        assert!(sql.contains("priority"));
        assert!(sql.contains("customer_tier"));
        assert!(sql.contains("(")); // Should have parentheses for grouping

        let params = query.get_parameters();
        assert_eq!(params.len(), 3); // status, priority, customer_tier

        Ok(())
    }

    #[test]
    fn test_operator_variations() -> Result<()> {
        let query = EnhancedSelectBuilder::new()
            .from("products")
            .select_all()
            .where_("price", ">=", 50.0)?
            .and_where("name", "LIKE", "%phone%")?
            .and_where("stock", "!=", 0)?;

        let sql = query.to_sql();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("price"));
        assert!(sql.contains("name"));
        assert!(sql.contains("stock"));

        let params = query.get_parameters();
        assert_eq!(params.len(), 3);

        // Verify parameter types
        let price_param = params.iter().find(|p| p.name.contains("price")).unwrap();
        assert!(matches!(price_param.value, QueryParameterValue::Float(_)));

        let name_param = params.iter().find(|p| p.name.contains("name")).unwrap();
        assert!(matches!(name_param.value, QueryParameterValue::String(_)));

        let stock_param = params.iter().find(|p| p.name.contains("stock")).unwrap();
        assert!(matches!(stock_param.value, QueryParameterValue::Int(_)));

        Ok(())
    }

    #[test]
    fn test_vector_parameter_binding() -> Result<()> {
        let embedding = vec![1.0f32, 2.0, 3.0, 4.0];
        let query = EnhancedSelectBuilder::new()
            .from("documents")
            .select(&["id", "title"])
            .where_("embedding", "=", embedding.clone())?;

        let sql = query.to_sql();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("embedding"));

        let params = query.get_parameters();
        assert_eq!(params.len(), 1);

        match &params[0].value {
            QueryParameterValue::Vector(vec) => {
                assert_eq!(vec.len(), 4);
                assert_eq!(vec[0], 1.0);
                assert_eq!(vec[3], 4.0);
            }
            _ => panic!("Expected Vector parameter"),
        }

        Ok(())
    }

    #[test]
    fn test_combined_with_order_and_limit() -> Result<()> {
        let query = EnhancedSelectBuilder::new()
            .from("users")
            .select(&["id", "name", "created_at"])
            .where_("active", "=", true)?
            .and_where("age", ">=", 21)?
            .order_by_desc("created_at")
            .limit(50)
            .offset(10);

        let sql = query.to_sql();
        assert!(sql.contains("SELECT id, name, created_at FROM users"));
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("active"));
        assert!(sql.contains("age"));
        assert!(sql.contains("ORDER BY created_at DESC"));
        assert!(sql.contains("LIMIT 50"));
        assert!(sql.contains("OFFSET 10"));

        let params = query.get_parameters();
        assert_eq!(params.len(), 2); // active and age

        Ok(())
    }

    #[test]
    fn test_empty_where_clause() -> Result<()> {
        let query = EnhancedSelectBuilder::new()
            .from("users")
            .select_all();

        let sql = query.to_sql();
        assert!(!sql.contains("WHERE"));

        let params = query.get_parameters();
        assert_eq!(params.len(), 0);

        Ok(())
    }

    #[test]
    fn test_validation_errors() {
        // Test empty IN values
        let result = EnhancedSelectBuilder::new()
            .from("users")
            .select_all()
            .where_in("role", Vec::<&str>::new());
        assert!(result.is_err());

        // Test invalid operator
        let result = EnhancedSelectBuilder::new()
            .from("users")
            .select_all()
            .where_("age", "INVALID_OP", 18);
        assert!(result.is_err());
    }

    #[test]
    fn test_where_clause_builder_standalone() -> Result<()> {
        let mut where_builder = WhereClauseBuilder::new();

        where_builder = where_builder
            .where_("status", "=", "active")?
            .and_where("priority", "=", "high")?
            .or_where("urgent", "=", true)?;

        let sql = where_builder.to_sql();
        assert!(!sql.is_empty());
        assert!(sql.contains("status"));
        assert!(sql.contains("priority"));
        assert!(sql.contains("urgent"));

        let params = where_builder.get_parameters();
        assert_eq!(params.len(), 3);

        assert!(where_builder.validate().is_ok());

        Ok(())
    }

    #[test]
    fn test_comparison_operators() -> Result<()> {
        // Test all supported comparison operators
        let operators = vec![
            ("=", "equal"),
            ("!=", "not_equal"),
            ("<", "less_than"),
            ("<=", "less_equal"),
            (">", "greater_than"),
            (">=", "greater_equal"),
            ("LIKE", "like_pattern"),
            ("NOT LIKE", "not_like_pattern"),
        ];

        for (op, test_value) in operators {
            let query = EnhancedSelectBuilder::new()
                .from("test_table")
                .select_all()
                .where_("test_column", op, test_value)?;

            let sql = query.to_sql();
            assert!(sql.contains("WHERE"));
            assert!(sql.contains("test_column"));

            let params = query.get_parameters();
            assert_eq!(params.len(), 1);
        }

        Ok(())
    }

    #[test]
    fn test_logical_operator_precedence() -> Result<()> {
        // Test that AND has higher precedence than OR by using groups
        let query = EnhancedSelectBuilder::new()
            .from("items")
            .select_all()
            .where_("category", "=", "electronics")?
            .where_group(|group| {
                group
                    .where_("price", "<", 100.0)?
                    .and_where("rating", ">=", 4.0)
            })?
            .or_where("featured", "=", true)?;

        let sql = query.to_sql();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("category"));
        assert!(sql.contains("price"));
        assert!(sql.contains("rating"));
        assert!(sql.contains("featured"));
        assert!(sql.contains("(")); // Should have grouping parentheses

        Ok(())
    }
}