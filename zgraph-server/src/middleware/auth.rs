//! Authentication middleware for zGraph web server
//!
//! Extracts and validates JWT tokens from Authorization headers
//! and adds user session information to request extensions.

use axum::{
    extract::Request,
    http::{StatusCode, HeaderMap},
    middleware::Next,
    response::{IntoResponse, Json, Response},
    Json,
};
use serde_json;
use tower::ServiceExt;
use tracing::{debug, info, warn};

use crate::auth::AuthSession;

/// Re-export auth middleware
pub use super::auth::auth_layer;