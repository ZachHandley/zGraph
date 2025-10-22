//! Handler modules for zGraph API endpoints
//!
//! Organizes all HTTP handlers by functional area with proper
//! authentication, validation, and error handling.

pub mod auth;
pub mod health;
pub mod tables;
pub mod sql;
pub mod cypher;
pub mod knn;
pub mod queries;
pub mod services;