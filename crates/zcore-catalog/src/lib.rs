use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use zcore_storage as zs;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ColumnType {
    Int,
    Float,
    Text,
    Vector,
    // Additional variants for compatibility
    Integer, // Alias for Int
    Decimal, // For financial/precision calculations
}

// Enhanced vector type with dimension information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DataType {
    Int,
    Float,
    Text,
    Vector(usize), // Vector with explicit dimension
    Integer,       // Alias for Int
    Decimal,       // For financial/precision calculations
}

// Convert between ColumnType and DataType
impl From<ColumnType> for DataType {
    fn from(col_type: ColumnType) -> Self {
        match col_type {
            ColumnType::Int | ColumnType::Integer => DataType::Integer,
            ColumnType::Float => DataType::Float,
            ColumnType::Text => DataType::Text,
            ColumnType::Vector => DataType::Vector(128), // Default dimension
            ColumnType::Decimal => DataType::Decimal,
        }
    }
}

impl From<DataType> for ColumnType {
    fn from(data_type: DataType) -> Self {
        match data_type {
            DataType::Int | DataType::Integer => ColumnType::Integer,
            DataType::Float => ColumnType::Float,
            DataType::Text => ColumnType::Text,
            DataType::Vector(_) => ColumnType::Vector,
            DataType::Decimal => ColumnType::Decimal,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub ty: ColumnType,
    /// Whether the column can contain NULL values
    #[serde(default = "default_nullable")]
    pub nullable: bool,
    /// Whether this column is part of the primary key
    #[serde(default = "default_primary_key")]
    pub primary_key: bool,
}

fn default_nullable() -> bool {
    true // Existing columns default to nullable for backward compatibility
}

fn default_primary_key() -> bool {
    false // Existing columns default to non-primary key
}

impl ColumnDef {
    /// Create a new column definition
    pub fn new(name: String, ty: ColumnType) -> Self {
        Self {
            name,
            ty,
            nullable: true,
            primary_key: false,
        }
    }

    /// Create a new column definition from DataType (compatibility)
    pub fn from_data_type(name: String, data_type: DataType, nullable: bool, primary_key: bool) -> Self {
        Self {
            name,
            ty: data_type.into(),
            nullable,
            primary_key,
        }
    }

    /// Create a new column definition with explicit nullable and primary key settings
    pub fn with_constraints(name: String, ty: ColumnType, nullable: bool, primary_key: bool) -> Self {
        Self {
            name,
            ty,
            nullable,
            primary_key,
        }
    }

    /// Create a primary key column (automatically non-nullable)
    pub fn primary_key(name: String, ty: ColumnType) -> Self {
        Self {
            name,
            ty,
            nullable: false, // Primary key columns are non-nullable
            primary_key: true,
        }
    }

    /// Create a non-nullable column
    pub fn not_null(name: String, ty: ColumnType) -> Self {
        Self {
            name,
            ty,
            nullable: false,
            primary_key: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

impl TableDef {
    /// Create a new table definition
    pub fn new(name: String, columns: Vec<ColumnDef>) -> Self {
        Self { name, columns }
    }

    /// Get all primary key columns
    pub fn primary_key_columns(&self) -> Vec<&ColumnDef> {
        self.columns.iter().filter(|col| col.primary_key).collect()
    }

    /// Check if the table has any primary key columns
    pub fn has_primary_key(&self) -> bool {
        self.columns.iter().any(|col| col.primary_key)
    }

    /// Get a column by name
    pub fn get_column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|col| col.name == name)
    }

    /// Validate table definition constraints
    pub fn validate(&self) -> Result<()> {
        // Check for duplicate column names
        let mut seen_names = std::collections::HashSet::new();
        for column in &self.columns {
            if !seen_names.insert(&column.name) {
                return Err(anyhow!("duplicate column name: {}", column.name));
            }
        }

        // Validate primary key constraints
        for column in &self.columns {
            if column.primary_key && column.nullable {
                return Err(anyhow!("primary key column '{}' cannot be nullable", column.name));
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct Catalog<'a> {
    store: &'a zs::Store,
}

impl<'a> Catalog<'a> {
    pub fn new(store: &'a zs::Store) -> Self {
        Self { store }
    }

    pub fn create_table(&self, org_id: u64, def: &TableDef) -> Result<()> {
        // Validate table definition first
        def.validate()?;

        let mut tx = zs::WriteTransaction::new();
        let key = key_catalog_table(org_id, &def.name);

        // Check if table already exists
        if tx.get(self.store, zs::COL_CATALOG, &key)?.is_some() {
            return Err(anyhow!("table exists"));
        }

        let val = bincode::serialize(def)?;
        tx.set(zs::COL_CATALOG, key, val);
        tx.commit(self.store)?;
        Ok(())
    }

    /// Create table from TableSchema (compatibility wrapper)
    pub fn create_table_schema(&self, org_id: u64, schema: &TableSchema) -> Result<()> {
        let table_def = schema.clone().to_table_def();
        self.create_table(org_id, &table_def)
    }

    pub fn get_table(&self, org_id: u64, name: &str) -> Result<Option<TableDef>> {
        let tx = zs::ReadTransaction::new();
        let key = key_catalog_table(org_id, name);

        if let Some(val_bytes) = tx.get(self.store, zs::COL_CATALOG, &key)? {
            let def: TableDef = bincode::deserialize(&val_bytes)?;
            Ok(Some(def))
        } else {
            Ok(None)
        }
    }

    pub fn list_tables(&self, org_id: u64) -> Result<Vec<TableDef>> {
        let tx = zs::ReadTransaction::new();
        let org_bytes = org_id.to_be_bytes();
        let prefix = zs::encode_key(&[b"catalog:table", &org_bytes]);
        let items = tx.scan_prefix(self.store, zs::COL_CATALOG, &prefix)?;

        let mut out = Vec::new();
        for (_, val_bytes) in items {
            let def: TableDef = bincode::deserialize(&val_bytes)?;
            out.push(def);
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// Drop a table
    pub fn drop_table(&self, org_id: u64, name: &str) -> Result<()> {
        let mut tx = zs::WriteTransaction::new();
        let key = key_catalog_table(org_id, name);

        // Check if table exists
        if tx.get(self.store, zs::COL_CATALOG, &key)?.is_none() {
            return Err(anyhow!("table '{}' does not exist", name));
        }

        tx.delete(zs::COL_CATALOG, key);
        tx.commit(self.store)?;
        Ok(())
    }

    /// Check if a table exists
    pub fn table_exists(&self, org_id: u64, name: &str) -> Result<bool> {
        let tx = zs::ReadTransaction::new();
        let key = key_catalog_table(org_id, name);
        Ok(tx.get(self.store, zs::COL_CATALOG, &key)?.is_some())
    }

    /// Drop a table if it exists (no error if it doesn't exist)
    pub fn drop_table_if_exists(&self, org_id: u64, name: &str) -> Result<bool> {
        if self.table_exists(org_id, name)? {
            self.drop_table(org_id, name)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Add a column to an existing table
    pub fn add_column(&self, org_id: u64, table_name: &str, column: ColumnDef) -> Result<()> {
        // Validate the column definition
        if column.name.is_empty() {
            return Err(anyhow!("column name cannot be empty"));
        }

        // Get the current table definition
        let mut table = self.get_table(org_id, table_name)?
            .ok_or_else(|| anyhow!("table '{}' does not exist", table_name))?;

        // Check if column already exists
        if table.get_column(&column.name).is_some() {
            return Err(anyhow!("column '{}' already exists in table '{}'", column.name, table_name));
        }

        // Add the new column
        table.columns.push(column);

        // Validate the updated table
        table.validate()?;

        // Update the table definition
        let mut tx = zs::WriteTransaction::new();
        let key = key_catalog_table(org_id, table_name);
        let val = bincode::serialize(&table)?;
        tx.set(zs::COL_CATALOG, key, val);
        tx.commit(self.store)?;
        Ok(())
    }

    /// Drop a column from an existing table
    pub fn drop_column(&self, org_id: u64, table_name: &str, column_name: &str) -> Result<()> {
        // Get the current table definition
        let mut table = self.get_table(org_id, table_name)?
            .ok_or_else(|| anyhow!("table '{}' does not exist", table_name))?;

        // Check if column exists
        let col_index = table.columns.iter()
            .position(|col| col.name == column_name)
            .ok_or_else(|| anyhow!("column '{}' does not exist in table '{}'", column_name, table_name))?;

        // Cannot drop primary key columns
        if table.columns[col_index].primary_key {
            return Err(anyhow!("cannot drop primary key column '{}' from table '{}'", column_name, table_name));
        }

        // Remove the column
        table.columns.remove(col_index);

        // Validate the updated table
        table.validate()?;

        // Update the table definition
        let mut tx = zs::WriteTransaction::new();
        let key = key_catalog_table(org_id, table_name);
        let val = bincode::serialize(&table)?;
        tx.set(zs::COL_CATALOG, key, val);
        tx.commit(self.store)?;
        Ok(())
    }

    /// Rename a table
    pub fn rename_table(&self, org_id: u64, old_name: &str, new_name: &str) -> Result<()> {
        if old_name == new_name {
            return Ok(());
        }

        if new_name.is_empty() {
            return Err(anyhow!("table name cannot be empty"));
        }

        // Check if old table exists
        let table = self.get_table(org_id, old_name)?
            .ok_or_else(|| anyhow!("table '{}' does not exist", old_name))?;

        // Check if new table name already exists
        if self.table_exists(org_id, new_name)? {
            return Err(anyhow!("table '{}' already exists", new_name));
        }

        let mut tx = zs::WriteTransaction::new();

        // Remove old table
        let old_key = key_catalog_table(org_id, old_name);
        tx.delete(zs::COL_CATALOG, old_key);

        // Add table with new name
        let mut renamed_table = table;
        renamed_table.name = new_name.to_string();
        let new_key = key_catalog_table(org_id, new_name);
        let val = bincode::serialize(&renamed_table)?;
        tx.set(zs::COL_CATALOG, new_key, val);

        tx.commit(self.store)?;
        Ok(())
    }
}

fn key_catalog_table(org_id: u64, name: &str) -> Vec<u8> {
    let org = org_id.to_be_bytes();
    zs::encode_key(&[b"catalog:table", &org, name.as_bytes()])
}

fn key_catalog_index(org_id: u64, name: &str) -> Vec<u8> {
    let org = org_id.to_be_bytes();
    zs::encode_key(&[b"catalog:index", &org, name.as_bytes()])
}

// ============================================================================
// COMPATIBILITY LAYER - Type aliases and wrappers for API compatibility
// ============================================================================

/// CatalogManager is the main interface expected by dependent crates
/// This is a static lifetime wrapper around Catalog for easier use
pub type CatalogManager = Catalog<'static>;

/// TableSchema wrapper for compatibility with expected constructor signature
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
}

impl TableSchema {
    /// Constructor expected by other crates: (name, columns)
    pub fn new(name: &str, columns: Vec<ColumnDefinition>) -> Self {
        Self {
            name: name.to_string(),
            columns,
        }
    }

    /// Convert from TableDef
    pub fn from_table_def(table_def: TableDef) -> Self {
        Self {
            name: table_def.name,
            columns: table_def.columns.into_iter().map(ColumnDefinition::from_column_def).collect(),
        }
    }

    /// Convert to TableDef
    pub fn to_table_def(self) -> TableDef {
        TableDef {
            name: self.name,
            columns: self.columns.into_iter().map(|c| c.to_column_def()).collect(),
        }
    }
}

/// ColumnDefinition wrapper for compatibility with expected constructor signature
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub ty: ColumnType,
    pub nullable: bool,
    pub primary_key: bool,
}

impl ColumnDefinition {
    /// Constructor expected by other crates: (name, data_type, nullable, primary_key)
    pub fn new(name: &str, data_type: DataType, nullable: bool, primary_key: bool) -> Self {
        Self {
            name: name.to_string(),
            ty: data_type.into(),
            nullable,
            primary_key,
        }
    }

    /// Convert from ColumnDef
    pub fn from_column_def(column_def: ColumnDef) -> Self {
        Self {
            name: column_def.name,
            ty: column_def.ty,
            nullable: column_def.nullable,
            primary_key: column_def.primary_key,
        }
    }

    /// Convert to ColumnDef
    pub fn to_column_def(self) -> ColumnDef {
        ColumnDef {
            name: self.name,
            ty: self.ty,
            nullable: self.nullable,
            primary_key: self.primary_key,
        }
    }
}

// Additional convenience methods for backward compatibility
impl TableSchema {
    /// Constructor that takes a &str name for convenience
    pub fn new_with_columns(name: &str, columns: Vec<ColumnDefinition>) -> Self {
        Self::new(name, columns)
    }
}

impl ColumnDefinition {
    /// Constructor that takes a DataType for convenience
    pub fn new_with_data_type(name: &str, data_type: DataType, nullable: bool, primary_key: bool) -> Self {
        Self::new(name, data_type, nullable, primary_key)
    }
}

/// Enhanced CatalogManager with async constructor for compatibility
impl CatalogManager {
    /// Async constructor expected by dependent crates
    pub async fn new_async(store: &'static zs::Store) -> Result<Self> {
        Ok(Catalog::new(store))
    }

    /// Create CatalogManager from non-static store (unsafe but needed for compatibility)
    pub fn from_store<'a>(store: &'a zs::Store) -> Catalog<'a> {
        Catalog::new(store)
    }
}

// Re-export everything for convenience
pub use ColumnType as ColType;

// Index metadata structures for future extensibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub index_type: IndexType,
    pub unique: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexType {
    BTree,
    Hash,
    Vector { dimension: usize, metric: VectorMetric },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum VectorMetric {
    L2,
    Cosine,
    InnerProduct,
}

// Index operations implementation
impl<'a> Catalog<'a> {
    /// Create an index
    pub fn create_index(&self, org_id: u64, def: &IndexDef) -> Result<()> {
        // Validate index definition
        self.validate_index_def(org_id, def)?;

        let mut tx = zs::WriteTransaction::new();
        let key = key_catalog_index(org_id, &def.name);

        // Check if index already exists
        if tx.get(self.store, zs::COL_CATALOG, &key)?.is_some() {
            return Err(anyhow!("index '{}' already exists", def.name));
        }

        // Verify that the referenced table exists
        if !self.table_exists(org_id, &def.table_name)? {
            return Err(anyhow!("table '{}' does not exist", def.table_name));
        }

        // Verify that all referenced columns exist
        let table_def = self.get_table(org_id, &def.table_name)?
            .ok_or_else(|| anyhow!("table '{}' not found", def.table_name))?;

        for col_name in &def.columns {
            if table_def.get_column(col_name).is_none() {
                return Err(anyhow!("column '{}' does not exist in table '{}'", col_name, def.table_name));
            }
        }

        // Validate index-specific constraints
        match &def.index_type {
            IndexType::Vector { dimension: _, .. } => {
                // For vector indexes, verify we have exactly one column
                if def.columns.len() != 1 {
                    return Err(anyhow!("vector indexes must have exactly one column"));
                }

                // Verify the column is a vector type
                let col = table_def.get_column(&def.columns[0]).unwrap();
                if !matches!(col.ty, ColumnType::Vector) {
                    return Err(anyhow!("vector indexes can only be created on vector columns"));
                }
            }
            IndexType::BTree | IndexType::Hash => {
                // For BTree/Hash indexes, we allow multiple columns but limit the count
                if def.columns.len() > 16 {
                    return Err(anyhow!("indexes cannot have more than 16 columns"));
                }
            }
        }

        // Store the index definition
        let val = bincode::serialize(def)?;
        tx.set(zs::COL_CATALOG, key, val);
        tx.commit(self.store)?;
        Ok(())
    }

    /// Drop an index
    pub fn drop_index(&self, org_id: u64, name: &str) -> Result<()> {
        let mut tx = zs::WriteTransaction::new();
        let key = key_catalog_index(org_id, name);

        // Check if index exists
        if tx.get(self.store, zs::COL_CATALOG, &key)?.is_none() {
            return Err(anyhow!("index '{}' does not exist", name));
        }

        tx.delete(zs::COL_CATALOG, key);
        tx.commit(self.store)?;
        Ok(())
    }

    /// List indexes for a table
    pub fn list_indexes(&self, org_id: u64, table_name: &str) -> Result<Vec<IndexDef>> {
        let tx = zs::ReadTransaction::new();
        let org_bytes = org_id.to_be_bytes();
        let prefix = zs::encode_key(&[b"catalog:index", &org_bytes]);
        let items = tx.scan_prefix(self.store, zs::COL_CATALOG, &prefix)?;

        let mut out = Vec::new();
        for (_, val_bytes) in items {
            let def: IndexDef = bincode::deserialize(&val_bytes)?;
            if def.table_name == table_name {
                out.push(def);
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// List all indexes for an organization
    pub fn list_all_indexes(&self, org_id: u64) -> Result<Vec<IndexDef>> {
        let tx = zs::ReadTransaction::new();
        let org_bytes = org_id.to_be_bytes();
        let prefix = zs::encode_key(&[b"catalog:index", &org_bytes]);
        let items = tx.scan_prefix(self.store, zs::COL_CATALOG, &prefix)?;

        let mut out = Vec::new();
        for (_, val_bytes) in items {
            let def: IndexDef = bincode::deserialize(&val_bytes)?;
            out.push(def);
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// Check if an index exists
    pub fn index_exists(&self, org_id: u64, name: &str) -> Result<bool> {
        let tx = zs::ReadTransaction::new();
        let key = key_catalog_index(org_id, name);
        Ok(tx.get(self.store, zs::COL_CATALOG, &key)?.is_some())
    }

    /// Get an index definition by name
    pub fn get_index(&self, org_id: u64, name: &str) -> Result<Option<IndexDef>> {
        let tx = zs::ReadTransaction::new();
        let key = key_catalog_index(org_id, name);

        if let Some(val_bytes) = tx.get(self.store, zs::COL_CATALOG, &key)? {
            let def: IndexDef = bincode::deserialize(&val_bytes)?;
            Ok(Some(def))
        } else {
            Ok(None)
        }
    }

    /// Drop an index if it exists (no error if it doesn't exist)
    pub fn drop_index_if_exists(&self, org_id: u64, name: &str) -> Result<bool> {
        if self.index_exists(org_id, name)? {
            self.drop_index(org_id, name)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Validate index definition
    fn validate_index_def(&self, _org_id: u64, def: &IndexDef) -> Result<()> {
        // Validate index name
        if def.name.is_empty() {
            return Err(anyhow!("index name cannot be empty"));
        }

        // Validate table name
        if def.table_name.is_empty() {
            return Err(anyhow!("table name cannot be empty"));
        }

        // Validate columns
        if def.columns.is_empty() {
            return Err(anyhow!("index must have at least one column"));
        }

        // Check for duplicate column names
        let mut seen = std::collections::HashSet::new();
        for col_name in &def.columns {
            if !seen.insert(col_name) {
                return Err(anyhow!("duplicate column name '{}' in index", col_name));
            }
        }

        // Validate unique constraint compatibility
        if def.unique {
            match &def.index_type {
                IndexType::Hash => {
                    // Hash indexes can be unique
                }
                IndexType::BTree => {
                    // BTree indexes can be unique
                }
                IndexType::Vector { .. } => {
                    // Vector indexes cannot be unique (approximate similarity)
                    return Err(anyhow!("vector indexes cannot be unique"));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    #[test]
    fn create_and_get_table() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("cat.redb")).unwrap();
        let cat = Catalog::new(&store);
        let def = TableDef::new(
            "docs".into(),
            vec![ColumnDef::new("id".into(), ColumnType::Int)]
        );
        cat.create_table(1, &def).unwrap();
        let got = cat.get_table(1, "docs").unwrap().unwrap();
        assert_eq!(got.name, "docs");
    }

    #[test]
    fn list_tables_returns_sorted_defs() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("cat-list.redb")).unwrap();
        let cat = Catalog::new(&store);
        let def_a = TableDef::new(
            "projects".into(),
            vec![ColumnDef::new("id".into(), ColumnType::Int)]
        );
        let def_b = TableDef::new(
            "docs".into(),
            vec![ColumnDef::new("body".into(), ColumnType::Text)]
        );
        cat.create_table(1, &def_a).unwrap();
        cat.create_table(1, &def_b).unwrap();

        let names: Vec<String> = cat
            .list_tables(1)
            .unwrap()
            .into_iter()
            .map(|t| t.name)
            .collect();

        assert_eq!(names, vec!["docs".to_string(), "projects".to_string()]);
    }

    #[test]
    fn test_column_def_constructors() {
        // Test default constructor
        let col1 = ColumnDef::new("name".into(), ColumnType::Text);
        assert_eq!(col1.name, "name");
        assert_eq!(col1.ty, ColumnType::Text);
        assert!(col1.nullable);
        assert!(!col1.primary_key);

        // Test primary key constructor
        let col2 = ColumnDef::primary_key("id".into(), ColumnType::Int);
        assert_eq!(col2.name, "id");
        assert_eq!(col2.ty, ColumnType::Int);
        assert!(!col2.nullable); // Primary key columns should be non-nullable
        assert!(col2.primary_key);

        // Test not null constructor
        let col3 = ColumnDef::not_null("email".into(), ColumnType::Text);
        assert_eq!(col3.name, "email");
        assert_eq!(col3.ty, ColumnType::Text);
        assert!(!col3.nullable);
        assert!(!col3.primary_key);

        // Test with_constraints constructor
        let col4 = ColumnDef::with_constraints("score".into(), ColumnType::Float, false, false);
        assert_eq!(col4.name, "score");
        assert_eq!(col4.ty, ColumnType::Float);
        assert!(!col4.nullable);
        assert!(!col4.primary_key);
    }

    #[test]
    fn test_table_def_methods() {
        let table = TableDef::new(
            "users".into(),
            vec![
                ColumnDef::primary_key("id".into(), ColumnType::Int),
                ColumnDef::not_null("email".into(), ColumnType::Text),
                ColumnDef::new("bio".into(), ColumnType::Text),
            ]
        );

        // Test primary key methods
        assert!(table.has_primary_key());
        let pk_cols = table.primary_key_columns();
        assert_eq!(pk_cols.len(), 1);
        assert_eq!(pk_cols[0].name, "id");

        // Test get_column method
        let email_col = table.get_column("email").unwrap();
        assert_eq!(email_col.name, "email");
        assert!(!email_col.nullable);
        assert!(!email_col.primary_key);

        let bio_col = table.get_column("bio").unwrap();
        assert!(bio_col.nullable);

        assert!(table.get_column("nonexistent").is_none());
    }

    #[test]
    fn test_table_validation_success() {
        let table = TableDef::new(
            "users".into(),
            vec![
                ColumnDef::primary_key("id".into(), ColumnType::Int),
                ColumnDef::not_null("email".into(), ColumnType::Text),
                ColumnDef::new("bio".into(), ColumnType::Text),
            ]
        );

        assert!(table.validate().is_ok());
    }

    #[test]
    fn test_table_validation_duplicate_columns() {
        let table = TableDef::new(
            "users".into(),
            vec![
                ColumnDef::new("name".into(), ColumnType::Text),
                ColumnDef::new("name".into(), ColumnType::Text), // Duplicate
            ]
        );

        let result = table.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("duplicate column name"));
    }

    #[test]
    fn test_table_validation_nullable_primary_key() {
        let table = TableDef::new(
            "users".into(),
            vec![
                ColumnDef::with_constraints("id".into(), ColumnType::Int, true, true), // Invalid: nullable primary key
            ]
        );

        let result = table.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("primary key column 'id' cannot be nullable"));
    }

    #[test]
    fn test_create_table_with_validation() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("validation.redb")).unwrap();
        let cat = Catalog::new(&store);

        // Valid table should succeed
        let valid_table = TableDef::new(
            "users".into(),
            vec![
                ColumnDef::primary_key("id".into(), ColumnType::Int),
                ColumnDef::not_null("email".into(), ColumnType::Text),
            ]
        );
        assert!(cat.create_table(1, &valid_table).is_ok());

        // Invalid table should fail
        let invalid_table = TableDef::new(
            "invalid".into(),
            vec![
                ColumnDef::with_constraints("id".into(), ColumnType::Int, true, true), // Invalid: nullable primary key
            ]
        );
        let result = cat.create_table(1, &invalid_table);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("primary key column 'id' cannot be nullable"));
    }

    #[test]
    fn test_backward_compatibility_deserialization() {
        use serde_json::json;

        // Test deserializing old format without nullable/primary_key fields
        let old_format_json = json!({
            "name": "test_col",
            "ty": "Text"
        });

        let column: ColumnDef = serde_json::from_value(old_format_json).unwrap();
        assert_eq!(column.name, "test_col");
        assert_eq!(column.ty, ColumnType::Text);
        assert!(column.nullable); // Should default to true
        assert!(!column.primary_key); // Should default to false
    }

    #[test]
    fn test_compatibility_wrapper_types() {
        // Test that wrapper types work correctly with the expected signatures
        let table_schema = TableSchema::new("users", vec![
            ColumnDefinition::new("id", DataType::Integer, false, true),
            ColumnDefinition::new("name", DataType::Text, false, false),
            ColumnDefinition::new("email", DataType::Text, true, false),
            ColumnDefinition::new("embedding", DataType::Vector(128), true, false),
        ]);

        assert_eq!(table_schema.name, "users");
        assert_eq!(table_schema.columns.len(), 4);

        let id_col = &table_schema.columns[0];
        assert_eq!(id_col.name, "id");
        assert_eq!(id_col.ty, ColumnType::Integer);
        assert!(!id_col.nullable);
        assert!(id_col.primary_key);

        let embedding_col = &table_schema.columns[3];
        assert_eq!(embedding_col.name, "embedding");
        assert_eq!(embedding_col.ty, ColumnType::Vector);
        assert!(embedding_col.nullable);
        assert!(!embedding_col.primary_key);

        // Test conversion to TableDef
        let table_def = table_schema.to_table_def();
        assert_eq!(table_def.name, "users");
        assert_eq!(table_def.columns.len(), 4);
    }

    #[test]
    fn test_data_type_conversions() {
        // Test conversion from DataType to ColumnType
        assert_eq!(ColumnType::from(DataType::Integer), ColumnType::Integer);
        assert_eq!(ColumnType::from(DataType::Float), ColumnType::Float);
        assert_eq!(ColumnType::from(DataType::Text), ColumnType::Text);
        assert_eq!(ColumnType::from(DataType::Vector(256)), ColumnType::Vector);
        assert_eq!(ColumnType::from(DataType::Decimal), ColumnType::Decimal);

        // Test conversion from ColumnType to DataType
        assert_eq!(DataType::from(ColumnType::Integer), DataType::Integer);
        assert_eq!(DataType::from(ColumnType::Float), DataType::Float);
        assert_eq!(DataType::from(ColumnType::Text), DataType::Text);
        assert_eq!(DataType::from(ColumnType::Vector), DataType::Vector(128)); // Default dimension
        assert_eq!(DataType::from(ColumnType::Decimal), DataType::Decimal);
    }

    #[test]
    fn test_drop_table() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("drop.redb")).unwrap();
        let cat = Catalog::new(&store);

        // Create a table
        let def = TableDef::new(
            "test_table".into(),
            vec![ColumnDef::new("id".into(), ColumnType::Int)]
        );
        cat.create_table(1, &def).unwrap();

        // Verify it exists
        assert!(cat.table_exists(1, "test_table").unwrap());

        // Drop it
        cat.drop_table(1, "test_table").unwrap();

        // Verify it's gone
        assert!(!cat.table_exists(1, "test_table").unwrap());

        // Trying to drop non-existent table should fail
        let result = cat.drop_table(1, "nonexistent");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn test_drop_table_if_exists() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("drop_if_exists.redb")).unwrap();
        let cat = Catalog::new(&store);

        // Create a table
        let def = TableDef::new(
            "test_table".into(),
            vec![ColumnDef::new("id".into(), ColumnType::Int)]
        );
        cat.create_table(1, &def).unwrap();

        // Drop existing table - should return true
        assert!(cat.drop_table_if_exists(1, "test_table").unwrap());

        // Drop non-existent table - should return false (no error)
        assert!(!cat.drop_table_if_exists(1, "nonexistent").unwrap());
    }

    #[test]
    fn test_table_exists() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("exists.redb")).unwrap();
        let cat = Catalog::new(&store);

        // Non-existent table
        assert!(!cat.table_exists(1, "nonexistent").unwrap());

        // Create a table
        let def = TableDef::new(
            "test_table".into(),
            vec![ColumnDef::new("id".into(), ColumnType::Int)]
        );
        cat.create_table(1, &def).unwrap();

        // Should now exist
        assert!(cat.table_exists(1, "test_table").unwrap());

        // Different org should not see it
        assert!(!cat.table_exists(2, "test_table").unwrap());
    }

    #[test]
    fn test_enhanced_column_constructors() {
        // Test from_data_type constructor
        let col = ColumnDef::from_data_type(
            "test_col".into(),
            DataType::Vector(256),
            false,
            true
        );

        assert_eq!(col.name, "test_col");
        assert_eq!(col.ty, ColumnType::Vector);
        assert!(!col.nullable);
        assert!(col.primary_key);
    }

    #[test]
    fn test_catalog_manager_compatibility() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("catalog_manager.redb")).unwrap();

        // Test that we can create a CatalogManager (should be the same as Catalog)
        let catalog = Catalog::from_store(&store);

        // Test table operations work the same way
        let table_schema = TableSchema::new("test", vec![
            ColumnDefinition::new("id", DataType::Integer, false, true),
        ]);

        catalog.create_table_schema(1, &table_schema).unwrap();
        assert!(catalog.table_exists(1, "test").unwrap());

        let retrieved = catalog.get_table(1, "test").unwrap().unwrap();
        assert_eq!(retrieved.name, "test");
        assert_eq!(retrieved.columns.len(), 1);
    }

    // Index Management Tests
    #[test]
    fn test_create_and_get_index() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("index.redb")).unwrap();
        let cat = Catalog::new(&store);

        // First create a table
        let table = TableDef::new(
            "docs".into(),
            vec![
                ColumnDef::primary_key("id".into(), ColumnType::Int),
                ColumnDef::not_null("content".into(), ColumnType::Text),
                ColumnDef::new("embedding".into(), ColumnType::Vector),
            ]
        );
        cat.create_table(1, &table).unwrap();

        // Create a BTree index
        let index_def = IndexDef {
            name: "docs_content_idx".to_string(),
            table_name: "docs".to_string(),
            columns: vec!["content".to_string()],
            index_type: IndexType::BTree,
            unique: false,
        };

        cat.create_index(1, &index_def).unwrap();

        // Verify index exists
        assert!(cat.index_exists(1, "docs_content_idx").unwrap());

        // Retrieve and verify the index
        let retrieved = cat.get_index(1, "docs_content_idx").unwrap().unwrap();
        assert_eq!(retrieved.name, "docs_content_idx");
        assert_eq!(retrieved.table_name, "docs");
        assert_eq!(retrieved.columns, vec!["content".to_string()]);
        assert_eq!(retrieved.unique, false);
    }

    #[test]
    fn test_create_vector_index() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("vector_index.redb")).unwrap();
        let cat = Catalog::new(&store);

        // Create table with vector column
        let table = TableDef::new(
            "embeddings".into(),
            vec![
                ColumnDef::primary_key("id".into(), ColumnType::Int),
                ColumnDef::new("vector".into(), ColumnType::Vector),
            ]
        );
        cat.create_table(1, &table).unwrap();

        // Create vector index
        let index_def = IndexDef {
            name: "embedding_hnsw_idx".to_string(),
            table_name: "embeddings".to_string(),
            columns: vec!["vector".to_string()],
            index_type: IndexType::Vector {
                dimension: 128,
                metric: VectorMetric::Cosine
            },
            unique: false,
        };

        cat.create_index(1, &index_def).unwrap();

        let retrieved = cat.get_index(1, "embedding_hnsw_idx").unwrap().unwrap();
        match retrieved.index_type {
            IndexType::Vector { dimension, metric } => {
                assert_eq!(dimension, 128);
                assert_eq!(metric, VectorMetric::Cosine);
            }
            _ => panic!("Expected vector index type"),
        }
    }

    #[test]
    fn test_index_validation_errors() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("index_validation.redb")).unwrap();
        let cat = Catalog::new(&store);

        // Create a table first
        let table = TableDef::new(
            "test_table".into(),
            vec![
                ColumnDef::primary_key("id".into(), ColumnType::Int),
                ColumnDef::new("vector_col".into(), ColumnType::Vector),
            ]
        );
        cat.create_table(1, &table).unwrap();

        // Test empty index name
        let invalid_index = IndexDef {
            name: "".to_string(),
            table_name: "test_table".to_string(),
            columns: vec!["id".to_string()],
            index_type: IndexType::BTree,
            unique: false,
        };
        assert!(cat.create_index(1, &invalid_index).is_err());

        // Test non-existent table
        let invalid_index = IndexDef {
            name: "test_idx".to_string(),
            table_name: "nonexistent_table".to_string(),
            columns: vec!["id".to_string()],
            index_type: IndexType::BTree,
            unique: false,
        };
        assert!(cat.create_index(1, &invalid_index).is_err());

        // Test non-existent column
        let invalid_index = IndexDef {
            name: "test_idx".to_string(),
            table_name: "test_table".to_string(),
            columns: vec!["nonexistent_col".to_string()],
            index_type: IndexType::BTree,
            unique: false,
        };
        assert!(cat.create_index(1, &invalid_index).is_err());

        // Test vector index on non-vector column
        let invalid_index = IndexDef {
            name: "invalid_vector_idx".to_string(),
            table_name: "test_table".to_string(),
            columns: vec!["id".to_string()], // id is Int, not Vector
            index_type: IndexType::Vector { dimension: 128, metric: VectorMetric::Cosine },
            unique: false,
        };
        assert!(cat.create_index(1, &invalid_index).is_err());

        // Test unique vector index (should fail)
        let invalid_index = IndexDef {
            name: "unique_vector_idx".to_string(),
            table_name: "test_table".to_string(),
            columns: vec!["vector_col".to_string()],
            index_type: IndexType::Vector { dimension: 128, metric: VectorMetric::Cosine },
            unique: true, // Vector indexes cannot be unique
        };
        assert!(cat.create_index(1, &invalid_index).is_err());

        // Test duplicate column names
        let invalid_index = IndexDef {
            name: "duplicate_col_idx".to_string(),
            table_name: "test_table".to_string(),
            columns: vec!["id".to_string(), "id".to_string()], // Duplicate
            index_type: IndexType::BTree,
            unique: false,
        };
        assert!(cat.create_index(1, &invalid_index).is_err());
    }

    #[test]
    fn test_drop_index() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("drop_index.redb")).unwrap();
        let cat = Catalog::new(&store);

        // Create table and index
        let table = TableDef::new(
            "test".into(),
            vec![ColumnDef::primary_key("id".into(), ColumnType::Int)]
        );
        cat.create_table(1, &table).unwrap();

        let index_def = IndexDef {
            name: "test_idx".to_string(),
            table_name: "test".to_string(),
            columns: vec!["id".to_string()],
            index_type: IndexType::BTree,
            unique: true,
        };
        cat.create_index(1, &index_def).unwrap();

        // Verify it exists
        assert!(cat.index_exists(1, "test_idx").unwrap());

        // Drop it
        cat.drop_index(1, "test_idx").unwrap();

        // Verify it's gone
        assert!(!cat.index_exists(1, "test_idx").unwrap());

        // Trying to drop non-existent index should fail
        let result = cat.drop_index(1, "nonexistent");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn test_drop_index_if_exists() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("drop_if_exists_index.redb")).unwrap();
        let cat = Catalog::new(&store);

        // Create table and index
        let table = TableDef::new(
            "test".into(),
            vec![ColumnDef::primary_key("id".into(), ColumnType::Int)]
        );
        cat.create_table(1, &table).unwrap();

        let index_def = IndexDef {
            name: "test_idx".to_string(),
            table_name: "test".to_string(),
            columns: vec!["id".to_string()],
            index_type: IndexType::BTree,
            unique: false,
        };
        cat.create_index(1, &index_def).unwrap();

        // Drop existing index - should return true
        assert!(cat.drop_index_if_exists(1, "test_idx").unwrap());

        // Drop non-existent index - should return false (no error)
        assert!(!cat.drop_index_if_exists(1, "nonexistent").unwrap());
    }

    #[test]
    fn test_list_indexes() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("list_indexes.redb")).unwrap();
        let cat = Catalog::new(&store);

        // Create tables
        let table1 = TableDef::new(
            "users".into(),
            vec![
                ColumnDef::primary_key("id".into(), ColumnType::Int),
                ColumnDef::not_null("email".into(), ColumnType::Text),
            ]
        );
        let table2 = TableDef::new(
            "docs".into(),
            vec![
                ColumnDef::primary_key("id".into(), ColumnType::Int),
                ColumnDef::new("content".into(), ColumnType::Text),
                ColumnDef::new("embedding".into(), ColumnType::Vector),
            ]
        );
        cat.create_table(1, &table1).unwrap();
        cat.create_table(1, &table2).unwrap();

        // Create indexes
        let indexes = vec![
            IndexDef {
                name: "users_email_idx".to_string(),
                table_name: "users".to_string(),
                columns: vec!["email".to_string()],
                index_type: IndexType::BTree,
                unique: true,
            },
            IndexDef {
                name: "docs_content_idx".to_string(),
                table_name: "docs".to_string(),
                columns: vec!["content".to_string()],
                index_type: IndexType::Hash,
                unique: false,
            },
            IndexDef {
                name: "docs_embedding_idx".to_string(),
                table_name: "docs".to_string(),
                columns: vec!["embedding".to_string()],
                index_type: IndexType::Vector { dimension: 128, metric: VectorMetric::L2 },
                unique: false,
            },
        ];

        for index_def in &indexes {
            cat.create_index(1, index_def).unwrap();
        }

        // List indexes for users table
        let users_indexes = cat.list_indexes(1, "users").unwrap();
        assert_eq!(users_indexes.len(), 1);
        assert_eq!(users_indexes[0].name, "users_email_idx");

        // List indexes for docs table
        let docs_indexes = cat.list_indexes(1, "docs").unwrap();
        assert_eq!(docs_indexes.len(), 2);
        let mut doc_index_names: Vec<String> = docs_indexes.iter().map(|i| i.name.clone()).collect();
        doc_index_names.sort();
        assert_eq!(doc_index_names, vec!["docs_content_idx", "docs_embedding_idx"]);

        // List all indexes
        let all_indexes = cat.list_all_indexes(1).unwrap();
        assert_eq!(all_indexes.len(), 3);

        // Verify all indexes are present and sorted
        let mut all_index_names: Vec<String> = all_indexes.iter().map(|i| i.name.clone()).collect();
        all_index_names.sort();
        assert_eq!(all_index_names, vec!["docs_content_idx", "docs_embedding_idx", "users_email_idx"]);

        // List indexes for table with no indexes
        let no_indexes = cat.list_indexes(1, "nonexistent_table").unwrap();
        assert_eq!(no_indexes.len(), 0);
    }

    #[test]
    fn test_index_comprehensive_workflow() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("workflow.redb")).unwrap();
        let cat = Catalog::new(&store);

        // Create a complex table
        let products_table = TableDef::new(
            "products".into(),
            vec![
                ColumnDef::primary_key("id".into(), ColumnType::Int),
                ColumnDef::not_null("name".into(), ColumnType::Text),
                ColumnDef::not_null("sku".into(), ColumnType::Text),
                ColumnDef::new("description".into(), ColumnType::Text),
                ColumnDef::new("price".into(), ColumnType::Decimal),
                ColumnDef::new("category".into(), ColumnType::Text),
                ColumnDef::new("embedding".into(), ColumnType::Vector),
            ]
        );
        cat.create_table(1, &products_table).unwrap();

        // Create multiple types of indexes
        let mut indexes = Vec::new();

        // Unique BTree index on SKU
        indexes.push(IndexDef {
            name: "products_sku_unique".to_string(),
            table_name: "products".to_string(),
            columns: vec!["sku".to_string()],
            index_type: IndexType::BTree,
            unique: true,
        });

        // Composite BTree index on category + name
        indexes.push(IndexDef {
            name: "products_category_name_idx".to_string(),
            table_name: "products".to_string(),
            columns: vec!["category".to_string(), "name".to_string()],
            index_type: IndexType::BTree,
            unique: false,
        });

        // Hash index on category
        indexes.push(IndexDef {
            name: "products_category_hash_idx".to_string(),
            table_name: "products".to_string(),
            columns: vec!["category".to_string()],
            index_type: IndexType::Hash,
            unique: false,
        });

        // Vector index on embedding
        indexes.push(IndexDef {
            name: "products_embedding_idx".to_string(),
            table_name: "products".to_string(),
            columns: vec!["embedding".to_string()],
            index_type: IndexType::Vector { dimension: 256, metric: VectorMetric::InnerProduct },
            unique: false,
        });

        // Create all indexes
        for index_def in &indexes {
            cat.create_index(1, index_def).unwrap();
        }

        // Verify all indexes exist
        for index_def in &indexes {
            assert!(cat.index_exists(1, &index_def.name).unwrap());
        }

        // List and verify all indexes for the table
        let table_indexes = cat.list_indexes(1, "products").unwrap();
        assert_eq!(table_indexes.len(), indexes.len());

        // Verify index properties
        for created_index in &table_indexes {
            let original_index = indexes.iter()
                .find(|i| i.name == created_index.name)
                .expect("Index should exist");

            assert_eq!(created_index.table_name, original_index.table_name);
            assert_eq!(created_index.columns, original_index.columns);
            assert_eq!(created_index.unique, original_index.unique);

            match (&created_index.index_type, &original_index.index_type) {
                (IndexType::BTree, IndexType::BTree) => {},
                (IndexType::Hash, IndexType::Hash) => {},
                (IndexType::Vector { dimension: d1, metric: m1 },
                 IndexType::Vector { dimension: d2, metric: m2 }) => {
                    assert_eq!(d1, d2);
                    assert_eq!(m1, m2);
                }
                _ => panic!("Index type mismatch"),
            }
        }

        // Drop one index
        cat.drop_index(1, "products_category_hash_idx").unwrap();
        assert!(!cat.index_exists(1, "products_category_hash_idx").unwrap());

        // Verify remaining indexes still exist
        let remaining_indexes = cat.list_indexes(1, "products").unwrap();
        assert_eq!(remaining_indexes.len(), indexes.len() - 1);
        assert!(!remaining_indexes.iter().any(|i| i.name == "products_category_hash_idx"));
    }

    // ALTER TABLE Tests
    #[test]
    fn test_add_column() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("add_column.redb")).unwrap();
        let cat = Catalog::new(&store);

        // Create initial table
        let table = TableDef::new(
            "users".into(),
            vec![
                ColumnDef::primary_key("id".into(), ColumnType::Int),
                ColumnDef::not_null("email".into(), ColumnType::Text),
            ]
        );
        cat.create_table(1, &table).unwrap();

        // Add a new column
        let new_column = ColumnDef::new("bio".into(), ColumnType::Text);
        cat.add_column(1, "users", new_column).unwrap();

        // Verify the column was added
        let updated_table = cat.get_table(1, "users").unwrap().unwrap();
        assert_eq!(updated_table.columns.len(), 3);
        assert!(updated_table.get_column("bio").is_some());

        // Try to add duplicate column (should fail)
        let duplicate_column = ColumnDef::new("email".into(), ColumnType::Text);
        assert!(cat.add_column(1, "users", duplicate_column).is_err());

        // Try to add column to non-existent table (should fail)
        let another_column = ColumnDef::new("name".into(), ColumnType::Text);
        assert!(cat.add_column(1, "nonexistent", another_column).is_err());
    }

    #[test]
    fn test_drop_column() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("drop_column.redb")).unwrap();
        let cat = Catalog::new(&store);

        // Create table with multiple columns
        let table = TableDef::new(
            "products".into(),
            vec![
                ColumnDef::primary_key("id".into(), ColumnType::Int),
                ColumnDef::not_null("name".into(), ColumnType::Text),
                ColumnDef::new("description".into(), ColumnType::Text),
                ColumnDef::new("price".into(), ColumnType::Decimal),
            ]
        );
        cat.create_table(1, &table).unwrap();

        // Drop a column
        cat.drop_column(1, "products", "description").unwrap();

        // Verify the column was dropped
        let updated_table = cat.get_table(1, "products").unwrap().unwrap();
        assert_eq!(updated_table.columns.len(), 3);
        assert!(updated_table.get_column("description").is_none());
        assert!(updated_table.get_column("price").is_some()); // Other columns remain

        // Try to drop primary key column (should fail)
        assert!(cat.drop_column(1, "products", "id").is_err());

        // Try to drop non-existent column (should fail)
        assert!(cat.drop_column(1, "products", "nonexistent").is_err());

        // Try to drop column from non-existent table (should fail)
        assert!(cat.drop_column(1, "nonexistent", "name").is_err());
    }

    #[test]
    fn test_rename_table() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("rename_table.redb")).unwrap();
        let cat = Catalog::new(&store);

        // Create initial table
        let table = TableDef::new(
            "old_name".into(),
            vec![ColumnDef::primary_key("id".into(), ColumnType::Int)]
        );
        cat.create_table(1, &table).unwrap();

        // Rename the table
        cat.rename_table(1, "old_name", "new_name").unwrap();

        // Verify the rename worked
        assert!(!cat.table_exists(1, "old_name").unwrap());
        assert!(cat.table_exists(1, "new_name").unwrap());

        let renamed_table = cat.get_table(1, "new_name").unwrap().unwrap();
        assert_eq!(renamed_table.name, "new_name");
        assert_eq!(renamed_table.columns.len(), 1);

        // Try to rename to existing name (should fail)
        let table2 = TableDef::new(
            "another_table".into(),
            vec![ColumnDef::primary_key("id".into(), ColumnType::Int)]
        );
        cat.create_table(1, &table2).unwrap();
        assert!(cat.rename_table(1, "new_name", "another_table").is_err());

        // Try to rename non-existent table (should fail)
        assert!(cat.rename_table(1, "nonexistent", "new_name").is_err());

        // Try to rename to empty name (should fail)
        assert!(cat.rename_table(1, "another_table", "").is_err());
    }

    #[test]
    fn test_alter_table_workflow() {
        let dir = tempdir().unwrap();
        let store = zs::Store::open(dir.path().join("alter_workflow.redb")).unwrap();
        let cat = Catalog::new(&store);

        // Start with a simple table
        let initial_table = TableDef::new(
            "employees".into(),
            vec![
                ColumnDef::primary_key("id".into(), ColumnType::Int),
                ColumnDef::not_null("name".into(), ColumnType::Text),
            ]
        );
        cat.create_table(1, &initial_table).unwrap();

        // Add multiple columns
        cat.add_column(1, "employees", ColumnDef::new("email".into(), ColumnType::Text)).unwrap();
        cat.add_column(1, "employees", ColumnDef::new("department".into(), ColumnType::Text)).unwrap();
        cat.add_column(1, "employees", ColumnDef::not_null("salary".into(), ColumnType::Decimal)).unwrap();

        // Verify columns were added
        let table = cat.get_table(1, "employees").unwrap().unwrap();
        assert_eq!(table.columns.len(), 5);
        assert!(table.get_column("email").is_some());
        assert!(table.get_column("department").is_some());
        assert!(table.get_column("salary").is_some());

        // Drop a column
        cat.drop_column(1, "employees", "department").unwrap();

        // Rename table
        cat.rename_table(1, "employees", "staff").unwrap();

        // Verify final state
        assert!(!cat.table_exists(1, "employees").unwrap());
        assert!(cat.table_exists(1, "staff").unwrap());

        let final_table = cat.get_table(1, "staff").unwrap().unwrap();
        assert_eq!(final_table.name, "staff");
        assert_eq!(final_table.columns.len(), 4);
        assert!(final_table.get_column("department").is_none());
        assert!(final_table.get_column("email").is_some());
        assert!(final_table.get_column("salary").is_some());

        // Create indexes on the renamed table
        let index_def = IndexDef {
            name: "staff_email_idx".to_string(),
            table_name: "staff".to_string(),
            columns: vec!["email".to_string()],
            index_type: IndexType::BTree,
            unique: true,
        };
        cat.create_index(1, &index_def).unwrap();
        assert!(cat.index_exists(1, "staff_email_idx").unwrap());
    }
}
