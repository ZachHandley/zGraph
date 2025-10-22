//! Simplified tracing setup for ZRUSTDB

use anyhow::Result;
use tracing_subscriber::{
    fmt,
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
    Registry,
};

use crate::ObservabilityConfig;

/// Initialize structured logging
pub async fn init_structured_logging(config: &ObservabilityConfig) -> Result<()> {
    let fmt_layer = fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_file(true)
        .with_line_number(true)
        .json();

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| {
            EnvFilter::new("info,zrustdb=debug,zobservability=debug")
        });

    // Compose subscriber with layers
    Registry::default()
        .with(filter)
        .with(fmt_layer)
        .init();

    tracing::info!(
        service.name = %config.service_name,
        environment = %config.environment,
        instance.id = %config.instance_id,
        "Structured logging initialized"
    );

    Ok(())
}

/// Tracing instrumentation utilities
pub mod instrumentation {
    use tracing::Span;

    /// Add common fields to current span
    pub fn enrich_span_with_request_info(
        method: &str,
        path: &str,
        user_id: Option<&str>,
        org_id: Option<&str>,
    ) {
        let span = Span::current();
        span.record("http.method", method);
        span.record("http.route", path);

        if let Some(uid) = user_id {
            span.record("user.id", uid);
        }

        if let Some(oid) = org_id {
            span.record("organization.id", oid);
        }
    }

    /// Add vector operation info to span
    pub fn enrich_span_with_vector_info(
        operation: &str,
        dimensions: usize,
        metric_type: &str,
        batch_size: Option<usize>,
    ) {
        let span = Span::current();
        span.record("vector.operation", operation);
        span.record("vector.dimensions", dimensions);
        span.record("vector.metric_type", metric_type);

        if let Some(size) = batch_size {
            span.record("vector.batch_size", size);
        }
    }

    /// Add SQL query info to span
    pub fn enrich_span_with_sql_info(
        query_type: &str,
        table_name: Option<&str>,
        has_vector_ops: bool,
        estimated_rows: Option<usize>,
    ) {
        let span = Span::current();
        span.record("sql.query_type", query_type);
        span.record("sql.has_vector_ops", has_vector_ops);

        if let Some(table) = table_name {
            span.record("sql.table", table);
        }

        if let Some(rows) = estimated_rows {
            span.record("sql.estimated_rows", rows);
        }
    }

    /// Add storage operation info to span
    pub fn enrich_span_with_storage_info(
        operation: &str,
        storage_type: &str,
        key_count: Option<usize>,
        data_size_bytes: Option<usize>,
    ) {
        let span = Span::current();
        span.record("storage.operation", operation);
        span.record("storage.type", storage_type);

        if let Some(keys) = key_count {
            span.record("storage.key_count", keys);
        }

        if let Some(size) = data_size_bytes {
            span.record("storage.data_size_bytes", size);
        }
    }

    /// Add WebSocket/WebRTC info to span
    pub fn enrich_span_with_realtime_info(
        connection_type: &str,
        message_type: Option<&str>,
        connection_id: Option<&str>,
        room_id: Option<&str>,
    ) {
        let span = Span::current();
        span.record("realtime.connection_type", connection_type);

        if let Some(msg_type) = message_type {
            span.record("realtime.message_type", msg_type);
        }

        if let Some(conn_id) = connection_id {
            span.record("realtime.connection_id", conn_id);
        }

        if let Some(room) = room_id {
            span.record("realtime.room_id", room);
        }
    }

    /// Add job execution info to span
    pub fn enrich_span_with_job_info(
        job_type: &str,
        job_id: &str,
        worker_type: &str,
        priority: Option<i32>,
    ) {
        let span = Span::current();
        span.record("job.type", job_type);
        span.record("job.id", job_id);
        span.record("job.worker_type", worker_type);

        if let Some(prio) = priority {
            span.record("job.priority", prio);
        }
    }
}