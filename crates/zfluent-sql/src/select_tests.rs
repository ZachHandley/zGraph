//! Additional comprehensive tests for SELECT query builders

#[cfg(test)]
mod additional_tests {
    use super::super::*;

    #[test]
    fn test_select_query_builder_select_method() {
        let builder = SelectQueryBuilder::new()
            .from("users")
            .select(&["id", "name", "email"]);

        assert_eq!(builder.selected_columns, vec!["id", "name", "email"]);
    }

    #[test]
    fn test_select_query_builder_select_raw() {
        let builder = SelectQueryBuilder::new()
            .from("users")
            .select_raw("COUNT(*) as total");

        assert_eq!(builder.selected_columns, vec!["COUNT(*) as total"]);
    }

    #[test]
    fn test_select_query_builder_select_as() {
        let builder = SelectQueryBuilder::new()
            .from("users")
            .select_as("user_name", "name");

        assert_eq!(builder.selected_columns, vec!["user_name AS name"]);
    }

    #[test]
    fn test_select_query_builder_select_count() {
        let builder = SelectQueryBuilder::new()
            .from("users")
            .select_count(Some("id"));

        assert_eq!(builder.selected_columns, vec!["COUNT(id)"]);

        let builder_all = SelectQueryBuilder::new()
            .from("users")
            .select_count(None);

        assert_eq!(builder_all.selected_columns, vec!["COUNT(*)"]);
    }

    #[test]
    fn test_select_query_builder_select_expr() {
        let builder = SelectQueryBuilder::new()
            .from("orders")
            .select_expr("price * quantity", Some("total_amount"));

        assert_eq!(builder.selected_columns, vec!["price * quantity AS total_amount"]);

        let builder_no_alias = SelectQueryBuilder::new()
            .from("orders")
            .select_expr("price + tax", None);

        assert_eq!(builder_no_alias.selected_columns, vec!["price + tax"]);
    }

    #[test]
    fn test_select_query_builder_select_upper() {
        let builder = SelectQueryBuilder::new()
            .from("users")
            .select_upper("name", Some("upper_name"));

        assert_eq!(builder.selected_columns, vec!["UPPER(name) AS upper_name"]);

        let builder_no_alias = SelectQueryBuilder::new()
            .from("users")
            .select_upper("name", None);

        assert_eq!(builder_no_alias.selected_columns, vec!["UPPER(name)"]);
    }

    #[test]
    fn test_select_query_builder_select_lower() {
        let builder = SelectQueryBuilder::new()
            .from("users")
            .select_lower("email", Some("lower_email"));

        assert_eq!(builder.selected_columns, vec!["LOWER(email) AS lower_email"]);

        let builder_no_alias = SelectQueryBuilder::new()
            .from("users")
            .select_lower("email", None);

        assert_eq!(builder_no_alias.selected_columns, vec!["LOWER(email)"]);
    }

    #[test]
    fn test_select_query_builder_select_case() {
        let when_conditions = [
            ("status = 'active'", "'Active User'"),
            ("status = 'inactive'", "'Inactive User'"),
        ];

        let builder = SelectQueryBuilder::new()
            .from("users")
            .select_case(&when_conditions, Some("'Unknown'"), Some("status_label"));

        let expected = "CASE WHEN status = 'active' THEN 'Active User' WHEN status = 'inactive' THEN 'Inactive User' ELSE 'Unknown' END AS status_label";
        assert_eq!(builder.selected_columns, vec![expected]);

        let builder_no_else = SelectQueryBuilder::new()
            .from("users")
            .select_case(&when_conditions[..1], None, None);

        let expected_no_else = "CASE WHEN status = 'active' THEN 'Active User' END";
        assert_eq!(builder_no_else.selected_columns, vec![expected_no_else]);
    }

    #[test]
    fn test_select_builder_comprehensive_functions() {
        let builder = SelectBuilder::new()
            .from("products")
            .select(&["id", "name"])
            .unwrap()
            .select_expr("price * quantity", Some("total"))
            .select_upper("category", Some("upper_category"))
            .unwrap()
            .select_lower("description", None)
            .unwrap()
            .select_case(&[("stock > 0", "'In Stock'")], Some("'Out of Stock'"), Some("availability"));

        assert_eq!(builder.selected_columns.len(), 6);

        // Check that all expected columns are present
        let column_exprs: Vec<_> = builder.selected_columns.iter().map(|c| &c.expression).collect();
        assert!(column_exprs.contains(&&"id".to_string()));
        assert!(column_exprs.contains(&&"name".to_string()));
        assert!(column_exprs.iter().any(|expr| expr.contains("price * quantity")));
        assert!(column_exprs.iter().any(|expr| expr.contains("UPPER(category)")));
        assert!(column_exprs.iter().any(|expr| expr.contains("LOWER(description)")));
        assert!(column_exprs.iter().any(|expr| expr.contains("CASE WHEN stock > 0")));
    }

    #[test]
    fn test_select_builder_duplicate_prevention() {
        let mut builder = SelectBuilder::new()
            .from("users")
            .select(&["id", "name"])
            .unwrap();

        // Try to add the same column again
        builder.add_selected_column(SelectedColumn::new("id"));

        // Should still only have 2 columns (duplicate ignored)
        assert_eq!(builder.selected_columns.len(), 2);
    }

    #[test]
    fn test_select_builder_to_sql_with_expressions() {
        let builder = SelectBuilder::new()
            .from("orders")
            .select(&["id"])
            .unwrap()
            .select_expr("price * quantity", Some("total"))
            .select_upper("status", Some("status_upper"))
            .unwrap();

        let sql = builder.to_sql();
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("FROM orders"));
        assert!(sql.contains("id"));
        assert!(sql.contains("price * quantity AS total"));
        assert!(sql.contains("UPPER(status) AS status_upper"));
    }

    #[test]
    fn test_select_query_builder_sql_generation_with_expressions() {
        let builder = SelectQueryBuilder::new()
            .from("products")
            .select_expr("price * 1.08", Some("price_with_tax"))
            .build()
            .unwrap();

        let sql = builder.to_sql();
        assert_eq!(sql, "SELECT price * 1.08 AS price_with_tax FROM products");
    }

    #[test]
    fn test_select_query_builder_sql_generation_count() {
        let builder = SelectQueryBuilder::new()
            .from("users")
            .select_count(Some("id"))
            .build()
            .unwrap();

        let sql = builder.to_sql();
        assert_eq!(sql, "SELECT COUNT(id) FROM users");
    }

    #[test]
    fn test_select_query_builder_sql_generation_case() {
        let when_conditions = [("age >= 18", "'Adult'")];
        let builder = SelectQueryBuilder::new()
            .from("users")
            .select_case(&when_conditions, Some("'Minor'"), Some("age_group"))
            .build()
            .unwrap();

        let sql = builder.to_sql();
        let expected = "SELECT CASE WHEN age >= 18 THEN 'Adult' ELSE 'Minor' END AS age_group FROM users";
        assert_eq!(sql, expected);
    }

    #[test]
    fn test_select_builder_validation_with_expressions() {
        let builder = SelectBuilder::new()
            .from("products")
            .select_expr("price + tax", Some("total_price"))
            .select_count(Some("id"));

        // Should validate successfully even with expressions
        assert!(builder.validate().is_ok());
    }

    #[test]
    fn test_select_query_builder_state_transitions() {
        // Test the phantom type state transitions work correctly
        let uninitialized = SelectQueryBuilder::new();
        let table_selected = uninitialized.from("users");
        let columns_selected = table_selected.select(&["id", "name"]);
        let executable = columns_selected.build().unwrap();

        // If we get here without compilation errors, the state transitions work
        let sql = executable.to_sql();
        assert!(!sql.is_empty());
    }
}