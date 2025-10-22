//! Type-safe table definitions with redb-style API compatibility
//!
//! This module provides type-safe table definitions using a #[derive(Table)] macro
//! pattern, along with enhanced table wrappers that provide full redb API compatibility.

use anyhow::{anyhow, Result};
use serde::{Serialize, Deserialize};
use std::marker::PhantomData;
use std::borrow::Cow;

/// Trait for types that can be used as table keys
pub trait Key: Send + Sync + 'static {
    /// Serialize the key to bytes
    fn as_bytes(&self) -> Result<Cow<'_, [u8]>>;

    /// Deserialize key from bytes
    fn from_bytes(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

/// Trait for types that can be used as table values
pub trait Value: Send + Sync + 'static {
    /// Serialize the value to bytes
    fn as_bytes(&self) -> Result<Cow<'_, [u8]>>;

    /// Deserialize value from bytes
    fn from_bytes(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

/// Table definition trait that must be implemented by table structs
pub trait TableDefinition: Send + Sync + 'static {
    /// The key type for this table
    type Key: Key;

    /// The value type for this table
    type Value: Value;

    /// Table name
    const NAME: &'static str;
}

/// Macro to derive Key trait for simple types
#[macro_export]
macro_rules! impl_key_for {
    ($type:ty, $as_bytes:expr, $from_bytes:expr) => {
        impl $crate::table::Key for $type {
            fn as_bytes(&self) -> Result<Cow<'_, [u8]>> {
                $as_bytes(self)
            }

            fn from_bytes(bytes: &[u8]) -> Result<Self>
            where
                Self: Sized,
            {
                $from_bytes(bytes)
            }
        }
    };
}

/// Macro to derive Value trait for simple types
#[macro_export]
macro_rules! impl_value_for {
    ($type:ty, $as_bytes:expr, $from_bytes:expr) => {
        impl $crate::table::Value for $type {
            fn as_bytes(&self) -> Result<Cow<'_, [u8]>> {
                $as_bytes(self)
            }

            fn from_bytes(bytes: &[u8]) -> Result<Self>
            where
                Self: Sized,
            {
                $from_bytes(bytes)
            }
        }
    };
}

// Implement Key for common primitive types
impl_key_for!(u8, |v: &u8| Ok(Cow::Owned([*v].to_vec())), |bytes: &[u8]| {
    if bytes.len() == 1 { Ok(bytes[0]) } else { Err(anyhow!("Invalid u8 encoding")) }
});

impl_key_for!(u16, |v: &u16| Ok(Cow::Owned(v.to_be_bytes().to_vec())), |bytes: &[u8]| {
    if bytes.len() == 2 { Ok(u16::from_be_bytes(bytes.try_into()?)) } else { Err(anyhow!("Invalid u16 encoding")) }
});

impl_key_for!(u32, |v: &u32| Ok(Cow::Owned(v.to_be_bytes().to_vec())), |bytes: &[u8]| {
    if bytes.len() == 4 { Ok(u32::from_be_bytes(bytes.try_into()?)) } else { Err(anyhow!("Invalid u32 encoding")) }
});

impl_key_for!(u64, |v: &u64| Ok(Cow::Owned(v.to_be_bytes().to_vec())), |bytes: &[u8]| {
    if bytes.len() == 8 { Ok(u64::from_be_bytes(bytes.try_into()?)) } else { Err(anyhow!("Invalid u64 encoding")) }
});

impl_key_for!(i32, |v: &i32| Ok(Cow::Owned(v.to_be_bytes().to_vec())), |bytes: &[u8]| {
    if bytes.len() == 4 { Ok(i32::from_be_bytes(bytes.try_into()?)) } else { Err(anyhow!("Invalid i32 encoding")) }
});

impl_key_for!(i64, |v: &i64| Ok(Cow::Owned(v.to_be_bytes().to_vec())), |bytes: &[u8]| {
    if bytes.len() == 8 { Ok(i64::from_be_bytes(bytes.try_into()?)) } else { Err(anyhow!("Invalid i64 encoding")) }
});

impl_key_for!(String, |v: &String| Ok(Cow::Owned(v.as_bytes().to_vec())), |bytes: &[u8]| {
    Ok(String::from_utf8(bytes.to_vec())?)
});

// &'static str requires a different approach due to lifetime constraints
impl Key for &'static str {
    fn as_bytes(&self) -> Result<Cow<'_, [u8]>> {
        Ok(Cow::Borrowed(str::as_bytes(self)))
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        // Convert to owned string since we can't return &'static str from arbitrary bytes
        let owned = String::from_utf8(bytes.to_vec())?;
        // Leak the string to get &'static str - this is generally not recommended
        // but needed for the trait interface
        Ok(Box::leak(owned.into_boxed_str()))
    }
}

impl_key_for!(Vec<u8>, |v: &Vec<u8>| Ok(Cow::Owned(v.clone())), |bytes: &[u8]| {
    Ok(bytes.to_vec())
});

// Note: &[u8] implementation removed due to lifetime conflicts
// Use Vec<u8> instead for byte array keys

// Note: Value trait is implemented generically for all Serialize + Deserialize types
// Specific implementations are not needed due to the blanket implementation above

// Special handling for JSON-serializable types
impl<T: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static> Value for T {
    fn as_bytes(&self) -> Result<Cow<'_, [u8]>> {
        Ok(Cow::Owned(serde_json::to_vec(self)?))
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// Type-safe table wrapper for read operations
pub struct TypedReadTable<'a, T: TableDefinition> {
    inner: crate::ReadTableWrapper<'a>,
    _phantom: PhantomData<T>,
}

impl<'a, T: TableDefinition> TypedReadTable<'a, T> {
    pub fn new(inner: crate::ReadTableWrapper<'a>) -> Self {
        Self {
            inner,
            _phantom: PhantomData,
        }
    }

    /// Get a value by key
    pub fn get(&self, key: &T::Key) -> Result<Option<T::Value>> {
        let key_bytes = key.as_bytes()?;
        match self.inner.get(&key_bytes)? {
            Some(value_wrapper) => {
                let value = T::Value::from_bytes(value_wrapper.value())?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Get an iterator over all key-value pairs
    pub fn iter(&self) -> Result<impl Iterator<Item = Result<(T::Key, T::Value)>>> {
        let inner_iter = self.inner.iter()?;
        Ok(inner_iter.map(move |result| {
            let (key_bytes, value_bytes) = result?;
            let key = T::Key::from_bytes(&key_bytes)?;
            let value = T::Value::from_bytes(&value_bytes)?;
            Ok((key, value))
        }))
    }

    /// Get a range iterator
    pub fn range<R>(&self, range: R) -> Result<impl Iterator<Item = Result<(T::Key, T::Value)>>>
    where
        R: std::ops::RangeBounds<T::Key>,
    {
        // Convert key range to byte range
        let start_key = match range.start_bound() {
            std::ops::Bound::Included(key) => key.as_bytes()?.to_vec(),
            std::ops::Bound::Excluded(key) => key.as_bytes()?.to_vec(),
            std::ops::Bound::Unbounded => vec![],
        };

        let end_key = match range.end_bound() {
            std::ops::Bound::Included(key) => {
                let mut bytes = key.as_bytes()?.to_vec();
                bytes.push(0xFF); // Simple upper bound
                bytes
            }
            std::ops::Bound::Excluded(key) => key.as_bytes()?.to_vec(),
            std::ops::Bound::Unbounded => vec![0xFF, 0xFF, 0xFF, 0xFF],
        };

        let range_result = self.inner.range(&start_key..&end_key)?;
        Ok(range_result.into_iter().map(move |(key_bytes, value_bytes)| {
            let key = T::Key::from_bytes(&key_bytes)?;
            let value = T::Value::from_bytes(&value_bytes)?;
            Ok((key, value))
        }))
    }
}

/// Type-safe table wrapper for write operations
pub struct TypedWriteTable<'a, T: TableDefinition> {
    inner: crate::WriteTableWrapper<'a>,
    _phantom: PhantomData<T>,
}

impl<'a, T: TableDefinition> TypedWriteTable<'a, T> {
    pub fn new(inner: crate::WriteTableWrapper<'a>) -> Self {
        Self {
            inner,
            _phantom: PhantomData,
        }
    }

    /// Insert a key-value pair
    pub fn insert(&mut self, key: &T::Key, value: &T::Value) -> Result<()> {
        let key_bytes = key.as_bytes()?;
        let value_bytes = value.as_bytes()?;
        self.inner.insert(&key_bytes, &value_bytes)
    }

    /// Get a value by key (reads pending changes)
    pub fn get(&self, key: &T::Key) -> Result<Option<T::Value>> {
        let key_bytes = key.as_bytes()?;
        match self.inner.get(&key_bytes)? {
            Some(value_wrapper) => {
                let value = T::Value::from_bytes(value_wrapper.value())?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Remove a key-value pair
    pub fn remove(&mut self, key: &T::Key) -> Result<bool> {
        let key_bytes = key.as_bytes()?;
        self.inner.remove(&key_bytes)
    }

    /// Reserve space for multiple insertions (performance optimization)
    pub fn reserve(&mut self, _additional: usize) {
        // This is a hint for performance optimization
        // The underlying implementation may or may not use it
    }

    /// Get the number of entries in the table (including pending changes)
    pub fn len(&self) -> Result<usize> {
        // Count items in the iterator
        let iter = self.inner.iter()?;
        let count = iter.count();
        Ok(count)
    }

    /// Check if the table is empty
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }
}

/// Macro to easily define a table structure
#[macro_export]
macro_rules! define_table {
    (
        $name:ident {
            key: $key_type:ty,
            value: $value_type:ty,
        }
    ) => {
        #[derive(Debug, Clone)]
        pub struct $name;

        impl $crate::table::TableDefinition for $name {
            type Key = $key_type;
            type Value = $value_type;
            const NAME: &'static str = stringify!($name);
        }
    };
}

/// Example usage of the table system
#[cfg(test)]
mod tests {
    use super::*;

    // Define a simple table for testing
    define_table! {
        UsersTable {
            key: String,
            value: UserData,
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct UserData {
        id: u64,
        name: String,
        email: String,
        created_at: u64,
    }

    #[test]
    fn test_primitive_key_value_operations() {
        // Test primitive key serialization
        let key: u32 = 42;
        let bytes = Key::as_bytes(&key).unwrap();
        assert_eq!(bytes.len(), 4);

        let deserialized: u32 = <u32 as Key>::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized, 42);

        // Test primitive value serialization (now JSON)
        let value: u64 = 123456789;
        let bytes = Value::as_bytes(&value).unwrap();
        assert_eq!(bytes.len(), 9); // JSON: "123456789"

        let deserialized: u64 = <u64 as Value>::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized, 123456789);
    }

    #[test]
    fn test_string_key_value_operations() {
        // Test string key
        let key = "test_key".to_string();
        let bytes = Key::as_bytes(&key).unwrap();
        assert_eq!(&*bytes, b"test_key");

        let deserialized: String = <String as Key>::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized, "test_key");

        // Test string value (now JSON)
        let value = "test_value".to_string();
        let bytes = Value::as_bytes(&value).unwrap();
        assert_eq!(&*bytes, b"\"test_value\""); // JSON string has quotes

        let deserialized: String = <String as Value>::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized, "test_value");
    }

    #[test]
    fn test_json_serialization() {
        use serde::{Serialize, Deserialize};

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct TestStruct {
            name: String,
            age: u32,
            active: bool,
        }

        let test_value = TestStruct {
            name: "Alice".to_string(),
            age: 30,
            active: true,
        };

        // Test JSON serialization
        let bytes = test_value.as_bytes().unwrap();
        let deserialized: TestStruct = TestStruct::from_bytes(&bytes).unwrap();

        assert_eq!(deserialized, test_value);
    }

    #[test]
    fn test_table_definition_macro() {
        // Test that the table definition macro works
        assert_eq!(UsersTable::NAME, "UsersTable");
        assert_eq!(std::any::type_name::<<UsersTable as TableDefinition>::Key>(), "alloc::string::String");
        assert!(std::any::type_name::<<UsersTable as TableDefinition>::Value>().contains("UserData"));
    }

    #[test]
    fn test_byte_slice_operations() {
        // Test &[u8] as key (convert to Vec<u8> first)
        let key: Vec<u8> = b"test_key".to_vec();
        let bytes = Key::as_bytes(&key).unwrap();
        assert_eq!(&*bytes, b"test_key");

        let deserialized: Vec<u8> = <Vec<u8> as Key>::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized, b"test_key");

        // Test Vec<u8> as value (now JSON)
        let value = vec![1, 2, 3, 4, 5];
        let bytes = Value::as_bytes(&value).unwrap();
        assert_eq!(&*bytes, b"[1,2,3,4,5]"); // JSON array format

        let deserialized: Vec<u8> = <Vec<u8> as Value>::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_composite_key_handling() {
        // Test that we can use composite keys by using Vec<u8>
        let composite_key = vec![0x01, 0x02, 0x03, 0x04];
        let bytes = Key::as_bytes(&composite_key).unwrap();
        assert_eq!(&*bytes, &[0x01, 0x02, 0x03, 0x04]);

        let deserialized: Vec<u8> = <Vec<u8> as Key>::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized, composite_key);
    }

    #[test]
    fn test_error_handling() {
        // Test invalid byte length for u32
        let result = <u32 as Key>::from_bytes(&[1, 2, 3]);
        assert!(result.is_err());

        // Test invalid UTF-8 for string
        let result = <String as Key>::from_bytes(&[0xFF, 0xFF, 0xFF]);
        assert!(result.is_err());

        // Test invalid JSON
        let result = <serde_json::Value as Value>::from_bytes(b"{invalid json}");
        assert!(result.is_err());
    }

    #[test]
    fn test_phantom_data_usage() {
        // Test that phantom data works correctly
        let store = crate::Store::open(tempfile::tempdir().unwrap()).unwrap();
        let tx = store.begin_read().unwrap();
        let wrapper = tx.open_table(&store, "test").unwrap();

        let typed_table: TypedReadTable<UsersTable> = TypedReadTable::new(wrapper);

        // This should compile and not use the data
        assert_eq!(std::mem::size_of_val(&typed_table), std::mem::size_of::<crate::ReadTableWrapper>());
    }
}