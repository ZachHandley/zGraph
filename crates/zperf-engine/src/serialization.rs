//! Zero-copy serialization optimization for database operations
//!
//! This module provides optimized serialization and deserialization using
//! bincode with zero-copy techniques where possible.

use std::sync::OnceLock;
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};
use bytes::{Bytes, BytesMut};
use memmap2::MmapOptions;
use std::fs::File;
use std::path::Path;

/// Serialization configuration optimized for database workloads
#[derive(Debug, Clone)]
pub struct SerializationConfig {
    /// Use little-endian byte order (optimal for x86_64)
    pub little_endian: bool,
    /// Use variable-length integer encoding
    pub varint_encoding: bool,
    /// Maximum serialized size before compression
    pub compression_threshold: usize,
    /// Enable zero-copy optimizations
    pub zero_copy: bool,
}

impl Default for SerializationConfig {
    fn default() -> Self {
        Self {
            little_endian: true,
            varint_encoding: true,
            compression_threshold: 1024,
            zero_copy: true,
        }
    }
}

/// Global serialization configuration
static SERIALIZATION_CONFIG: OnceLock<SerializationConfig> = OnceLock::new();

/// Initialize serialization with the given configuration
pub fn init_serialization(config: SerializationConfig) {
    let _ = SERIALIZATION_CONFIG.set(config);
}

/// Get the current serialization configuration
pub fn get_serialization_config() -> SerializationConfig {
    SERIALIZATION_CONFIG.get().cloned().unwrap_or_default()
}

/// High-performance serializer for database objects
pub struct DatabaseSerializer;

impl DatabaseSerializer {
    /// Create a new database serializer with optimal settings
    pub fn new() -> Self {
        Self
    }

    /// Serialize an object to bytes
    pub fn serialize<T>(&self, value: &T) -> Result<Vec<u8>>
    where
        T: Serialize,
    {
        bincode::serialize(value)
            .context("Failed to serialize object")
    }

    /// Serialize an object to a byte buffer
    pub fn serialize_into<T>(&self, value: &T, buffer: &mut Vec<u8>) -> Result<()>
    where
        T: Serialize,
    {
        let encoded = bincode::serialize(value)
            .context("Failed to serialize object")?;
        buffer.extend_from_slice(&encoded);
        Ok(())
    }

    /// Serialize directly to a writer
    pub fn serialize_to_writer<T, W>(&self, value: &T, writer: W) -> Result<()>
    where
        T: Serialize,
        W: std::io::Write,
    {
        bincode::serialize_into(writer, value)
            .context("Failed to serialize to writer")
    }

    /// Deserialize an object from bytes
    pub fn deserialize<T>(&self, bytes: &[u8]) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        bincode::deserialize(bytes)
            .context("Failed to deserialize object")
    }

    /// Deserialize from a reader
    pub fn deserialize_from_reader<T, R>(&self, reader: R) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
        R: std::io::Read,
    {
        bincode::deserialize_from(reader)
            .context("Failed to deserialize from reader")
    }

    /// Get the size of a serialized object without actually serializing
    pub fn serialized_size<T>(&self, value: &T) -> Result<u64>
    where
        T: Serialize,
    {
        bincode::serialized_size(value)
            .context("Failed to calculate serialized size")
    }
}

impl Default for DatabaseSerializer {
    fn default() -> Self {
        Self::new()
    }
}

/// Zero-copy buffer for database operations
pub struct ZeroCopyBuffer {
    data: Bytes,
    position: usize,
}

impl ZeroCopyBuffer {
    /// Create a new zero-copy buffer from bytes
    pub fn new(data: Bytes) -> Self {
        Self { data, position: 0 }
    }

    /// Create from a vector (will copy data)
    pub fn from_vec(data: Vec<u8>) -> Self {
        Self::new(Bytes::from(data))
    }

    /// Create from a slice (will copy data)
    pub fn from_slice(data: &[u8]) -> Self {
        Self::new(Bytes::copy_from_slice(data))
    }

    /// Get remaining bytes
    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.position)
    }

    /// Check if buffer has remaining data
    pub fn has_remaining(&self) -> bool {
        self.remaining() > 0
    }

    /// Get a slice of remaining data
    pub fn remaining_slice(&self) -> &[u8] {
        &self.data[self.position..]
    }

    /// Advance the position
    pub fn advance(&mut self, cnt: usize) {
        self.position = (self.position + cnt).min(self.data.len());
    }

    /// Reset to beginning
    pub fn reset(&mut self) {
        self.position = 0;
    }

    /// Get the total length
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Clone the underlying bytes (zero-copy)
    pub fn clone_bytes(&self) -> Bytes {
        self.data.clone()
    }

    /// Read a value without copying
    pub fn read_value<T>(&mut self) -> Result<T>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let serializer = DatabaseSerializer::new();
        let value = serializer.deserialize(self.remaining_slice())?;

        // Calculate how much data was consumed
        let consumed = serializer.serialized_size(&value)? as usize;
        self.advance(consumed);

        Ok(value)
    }

    /// Peek at a value without advancing position
    pub fn peek_value<T>(&self) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let serializer = DatabaseSerializer::new();
        serializer.deserialize(self.remaining_slice())
    }
}

/// Zero-copy writer for building buffers efficiently
pub struct ZeroCopyWriter {
    buffer: BytesMut,
}

impl ZeroCopyWriter {
    /// Create a new writer with initial capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
        }
    }

    /// Write a value to the buffer
    pub fn write_value<T>(&mut self, value: &T) -> Result<()>
    where
        T: Serialize,
    {
        let serializer = DatabaseSerializer::new();
        let serialized = serializer.serialize(value)?;
        self.buffer.extend_from_slice(&serialized);
        Ok(())
    }

    /// Write raw bytes
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    /// Reserve additional capacity
    pub fn reserve(&mut self, additional: usize) {
        self.buffer.reserve(additional);
    }

    /// Get the current length
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Freeze the buffer into immutable Bytes
    pub fn freeze(self) -> Bytes {
        self.buffer.freeze()
    }

    /// Split off the first `at` bytes
    pub fn split_to(&mut self, at: usize) -> Bytes {
        self.buffer.split_to(at).freeze()
    }

    /// Clear the buffer
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

impl Default for ZeroCopyWriter {
    fn default() -> Self {
        Self::with_capacity(4096)
    }
}

/// Memory-mapped file reader for zero-copy access to large data files
pub struct MmapReader {
    _file: File,
    mmap: memmap2::Mmap,
}

impl MmapReader {
    /// Open a file for memory-mapped reading
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path.as_ref())
            .with_context(|| format!("Failed to open file: {:?}", path.as_ref()))?;

        let mmap = unsafe {
            MmapOptions::new()
                .map(&file)
                .context("Failed to create memory map")?
        };

        Ok(Self { _file: file, mmap })
    }

    /// Get the file data as a slice
    pub fn as_slice(&self) -> &[u8] {
        &self.mmap[..]
    }

    /// Get the file size
    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    /// Check if the file is empty
    pub fn is_empty(&self) -> bool {
        self.mmap.is_empty()
    }

    /// Create a zero-copy buffer from the mapped data
    pub fn as_zero_copy_buffer(&self) -> ZeroCopyBuffer {
        ZeroCopyBuffer::from_slice(self.as_slice())
    }

    /// Read a value from a specific offset
    pub fn read_value_at<T>(&self, offset: usize) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        if offset >= self.len() {
            anyhow::bail!("Offset {} beyond file size {}", offset, self.len());
        }

        let serializer = DatabaseSerializer::new();
        serializer.deserialize(&self.mmap[offset..])
    }

    /// Read multiple values starting from an offset
    pub fn read_values_from<T>(&self, offset: usize, count: usize) -> Result<Vec<T>>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let mut values = Vec::with_capacity(count);
        let mut buffer = ZeroCopyBuffer::from_slice(&self.mmap[offset..]);

        for _ in 0..count {
            if !buffer.has_remaining() {
                break;
            }
            values.push(buffer.read_value()?);
        }

        Ok(values)
    }
}

/// Batch serialization for multiple objects
pub struct BatchSerializer {
    serializer: DatabaseSerializer,
    buffer: ZeroCopyWriter,
}

impl BatchSerializer {
    /// Create a new batch serializer
    pub fn new() -> Self {
        Self {
            serializer: DatabaseSerializer::new(),
            buffer: ZeroCopyWriter::with_capacity(64 * 1024), // 64KB initial capacity
        }
    }

    /// Add an object to the batch
    pub fn add<T>(&mut self, value: &T) -> Result<()>
    where
        T: Serialize,
    {
        self.buffer.write_value(value)
    }

    /// Add multiple objects
    pub fn add_all<T, I>(&mut self, values: I) -> Result<()>
    where
        T: Serialize,
        I: IntoIterator<Item = T>,
    {
        for value in values {
            self.add(&value)?;
        }
        Ok(())
    }

    /// Finish the batch and return the serialized data
    pub fn finish(self) -> Bytes {
        self.buffer.freeze()
    }

    /// Get the current batch size
    pub fn size(&self) -> usize {
        self.buffer.len()
    }

    /// Clear the batch (reuse the serializer)
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

impl Default for BatchSerializer {
    fn default() -> Self {
        Self::new()
    }
}

/// Batch deserializer for multiple objects
pub struct BatchDeserializer {
    serializer: DatabaseSerializer,
    buffer: ZeroCopyBuffer,
}

impl BatchDeserializer {
    /// Create a new batch deserializer from bytes
    pub fn new(data: Bytes) -> Self {
        Self {
            serializer: DatabaseSerializer::new(),
            buffer: ZeroCopyBuffer::new(data),
        }
    }

    /// Read the next object from the batch
    pub fn next<T>(&mut self) -> Result<Option<T>>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        if !self.buffer.has_remaining() {
            return Ok(None);
        }

        match self.buffer.read_value() {
            Ok(value) => Ok(Some(value)),
            Err(_) if !self.buffer.has_remaining() => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Read all remaining objects
    pub fn read_all<T>(&mut self) -> Result<Vec<T>>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let mut values = Vec::new();

        while let Some(value) = self.next()? {
            values.push(value);
        }

        Ok(values)
    }

    /// Peek at the next object without consuming it
    pub fn peek<T>(&self) -> Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        if !self.buffer.has_remaining() {
            return Ok(None);
        }

        match self.buffer.peek_value() {
            Ok(value) => Ok(Some(value)),
            Err(_) => Ok(None),
        }
    }

    /// Check if there are more objects to read
    pub fn has_remaining(&self) -> bool {
        self.buffer.has_remaining()
    }

    /// Get the number of remaining bytes
    pub fn remaining_bytes(&self) -> usize {
        self.buffer.remaining()
    }
}

/// Serialization utilities for common database operations
pub struct SerializationUtils;

impl SerializationUtils {
    /// Serialize a key-value pair efficiently
    pub fn serialize_kv<K, V>(key: &K, value: &V) -> Result<Vec<u8>>
    where
        K: Serialize,
        V: Serialize,
    {
        let _serializer = DatabaseSerializer::new();
        let mut buffer = ZeroCopyWriter::with_capacity(1024);

        buffer.write_value(key)?;
        buffer.write_value(value)?;

        Ok(buffer.freeze().to_vec())
    }

    /// Deserialize a key-value pair
    pub fn deserialize_kv<K, V>(data: &[u8]) -> Result<(K, V)>
    where
        K: for<'de> Deserialize<'de> + Serialize,
        V: for<'de> Deserialize<'de> + Serialize,
    {
        let mut buffer = ZeroCopyBuffer::from_slice(data);
        let key = buffer.read_value()?;
        let value = buffer.read_value()?;
        Ok((key, value))
    }

    /// Calculate the total size of multiple objects when serialized
    pub fn batch_size<T, I>(values: I) -> Result<usize>
    where
        T: Serialize,
        I: IntoIterator<Item = T>,
    {
        let mut total_size = 0;

        for value in values {
            total_size += DatabaseSerializer::new().serialized_size(&value)? as usize;
        }

        Ok(total_size)
    }

    /// Serialize with a custom header
    pub fn serialize_with_header<T>(value: &T, header: &[u8]) -> Result<Vec<u8>>
    where
        T: Serialize,
    {
        let _serializer = DatabaseSerializer::new();
        let mut buffer = ZeroCopyWriter::with_capacity(header.len() + 1024);

        buffer.write_bytes(header);
        buffer.write_value(value)?;

        Ok(buffer.freeze().to_vec())
    }

    /// Deserialize with header validation
    pub fn deserialize_with_header<T>(data: &[u8], expected_header: &[u8]) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        if data.len() < expected_header.len() {
            anyhow::bail!("Data too short for header");
        }

        let (header, payload) = data.split_at(expected_header.len());

        if header != expected_header {
            anyhow::bail!("Header mismatch");
        }

        let serializer = DatabaseSerializer::new();
        serializer.deserialize(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestStruct {
        id: u64,
        name: String,
        value: f64,
    }

    #[test]
    fn test_database_serializer() {
        let serializer = DatabaseSerializer::new();
        let test_data = TestStruct {
            id: 12345,
            name: "test".to_string(),
            value: 3.14159,
        };

        let serialized = serializer.serialize(&test_data).unwrap();
        let deserialized: TestStruct = serializer.deserialize(&serialized).unwrap();

        assert_eq!(test_data, deserialized);
    }

    #[test]
    fn test_zero_copy_buffer() {
        let serializer = DatabaseSerializer::new();
        let test_data = TestStruct {
            id: 456,
            name: "zero_copy".to_string(),
            value: 2.71828,
        };

        let serialized = serializer.serialize(&test_data).unwrap();
        let mut buffer = ZeroCopyBuffer::from_vec(serialized);

        let read_data: TestStruct = buffer.read_value().unwrap();
        assert_eq!(test_data, read_data);
        assert!(!buffer.has_remaining());
    }

    #[test]
    fn test_zero_copy_writer() {
        let mut writer = ZeroCopyWriter::with_capacity(1024);
        let test_data1 = TestStruct {
            id: 1,
            name: "first".to_string(),
            value: 1.0,
        };
        let test_data2 = TestStruct {
            id: 2,
            name: "second".to_string(),
            value: 2.0,
        };

        writer.write_value(&test_data1).unwrap();
        writer.write_value(&test_data2).unwrap();

        let bytes = writer.freeze();
        let mut buffer = ZeroCopyBuffer::new(bytes);

        let read1: TestStruct = buffer.read_value().unwrap();
        let read2: TestStruct = buffer.read_value().unwrap();

        assert_eq!(test_data1, read1);
        assert_eq!(test_data2, read2);
    }

    #[test]
    fn test_batch_serializer() {
        let mut batch = BatchSerializer::new();
        let test_data = vec![
            TestStruct { id: 1, name: "one".to_string(), value: 1.0 },
            TestStruct { id: 2, name: "two".to_string(), value: 2.0 },
            TestStruct { id: 3, name: "three".to_string(), value: 3.0 },
        ];

        for item in &test_data {
            batch.add(item).unwrap();
        }

        let bytes = batch.finish();
        let mut deserializer = BatchDeserializer::new(bytes);

        let mut results = Vec::new();
        while let Some(item) = deserializer.next::<TestStruct>().unwrap() {
            results.push(item);
        }

        assert_eq!(test_data, results);
    }

    #[test]
    fn test_serialization_utils() {
        let key = "test_key";
        let value = TestStruct {
            id: 999,
            name: "utils_test".to_string(),
            value: 42.0,
        };

        let serialized = SerializationUtils::serialize_kv(&key, &value).unwrap();
        let (read_key, read_value): (String, TestStruct) =
            SerializationUtils::deserialize_kv(&serialized).unwrap();

        assert_eq!(key, read_key);
        assert_eq!(value, read_value);
    }

    #[test]
    fn test_header_serialization() {
        let header = b"ZRUSTDB";
        let test_data = TestStruct {
            id: 777,
            name: "header_test".to_string(),
            value: 7.77,
        };

        let serialized = SerializationUtils::serialize_with_header(&test_data, header).unwrap();
        let deserialized: TestStruct =
            SerializationUtils::deserialize_with_header(&serialized, header).unwrap();

        assert_eq!(test_data, deserialized);

        // Test header mismatch
        let wrong_header = b"WRONGDB";
        let result = SerializationUtils::deserialize_with_header::<TestStruct>(&serialized, wrong_header);
        assert!(result.is_err());
    }

    #[test]
    fn test_batch_size_calculation() {
        let test_data = vec![
            TestStruct { id: 1, name: "size1".to_string(), value: 1.0 },
            TestStruct { id: 2, name: "size2".to_string(), value: 2.0 },
        ];

        let total_size = SerializationUtils::batch_size(test_data.iter()).unwrap();
        assert!(total_size > 0);

        // Verify by actually serializing
        let mut batch = BatchSerializer::new();
        for item in &test_data {
            batch.add(item).unwrap();
        }

        let actual_size = batch.size();
        assert_eq!(total_size, actual_size);
    }
}