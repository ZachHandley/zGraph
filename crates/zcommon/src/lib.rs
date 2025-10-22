//! Common interfaces and utilities for ZRUSTDB
//!
//! This crate provides shared abstractions to break circular dependencies
//! between service crates while maintaining clean interfaces.

pub mod metrics;
pub mod health;
pub mod tracing;

pub use metrics::*;
pub use health::*;
pub use tracing::*;