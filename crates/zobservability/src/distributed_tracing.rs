//! Distributed tracing system for ZRUSTDB
//!
//! This module provides comprehensive distributed tracing capabilities that track
//! requests across service boundaries, enabling performance monitoring and
//! debugging of complex operations.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;
use uuid::Uuid;
use chrono::{DateTime, Utc};

/// Unique identifier for a distributed trace
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceId(pub String);

impl TraceId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

/// Unique identifier for a span within a trace
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpanId(pub String);

impl SpanId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

/// Span context for propagating trace information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanContext {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub parent_span_id: Option<SpanId>,
    pub baggage: HashMap<String, String>,
}

impl SpanContext {
    pub fn new(trace_id: TraceId, span_id: SpanId, parent_span_id: Option<SpanId>) -> Self {
        Self {
            trace_id,
            span_id,
            parent_span_id,
            baggage: HashMap::new(),
        }
    }

    /// Create a child span context
    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            span_id: SpanId::new(),
            parent_span_id: Some(self.span_id.clone()),
            baggage: self.baggage.clone(),
        }
    }

    /// Add baggage item for cross-service data propagation
    pub fn set_baggage(&mut self, key: String, value: String) {
        self.baggage.insert(key, value);
    }

    /// Get baggage item
    pub fn get_baggage(&self, key: &str) -> Option<&String> {
        self.baggage.get(key)
    }
}

/// Span represents a single operation within a trace
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Span {
    pub context: SpanContext,
    pub operation_name: String,
    pub service_name: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub duration_ms: Option<u64>,
    pub tags: HashMap<String, String>,
    pub logs: Vec<SpanLog>,
    pub status: SpanStatus,
}

/// Span log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanLog {
    pub timestamp: DateTime<Utc>,
    pub level: LogLevel,
    pub message: String,
    pub fields: HashMap<String, String>,
}

/// Log levels for span logs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

/// Span completion status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpanStatus {
    Ok,
    Error,
    Timeout,
    Cancelled,
}

impl Span {
    pub fn new(
        context: SpanContext,
        operation_name: String,
        service_name: String,
    ) -> Self {
        Self {
            context,
            operation_name,
            service_name,
            start_time: Utc::now(),
            end_time: None,
            duration_ms: None,
            tags: HashMap::new(),
            logs: Vec::new(),
            status: SpanStatus::Ok,
        }
    }

    /// Add a tag to the span
    pub fn set_tag(&mut self, key: String, value: String) {
        self.tags.insert(key, value);
    }

    /// Add a log entry to the span
    pub fn log(&mut self, level: LogLevel, message: String, fields: HashMap<String, String>) {
        self.logs.push(SpanLog {
            timestamp: Utc::now(),
            level,
            message,
            fields,
        });
    }

    /// Finish the span
    pub fn finish(&mut self, status: SpanStatus) {
        let end_time = Utc::now();
        self.end_time = Some(end_time);
        self.duration_ms = Some((end_time - self.start_time).num_milliseconds() as u64);
        self.status = status;
    }

    /// Check if span is finished
    pub fn is_finished(&self) -> bool {
        self.end_time.is_some()
    }
}

/// Distributed trace containing multiple spans
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trace {
    pub trace_id: TraceId,
    pub root_span_id: SpanId,
    pub spans: HashMap<SpanId, Span>,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub duration_ms: Option<u64>,
    pub service_count: usize,
    pub error_count: usize,
}

impl Trace {
    pub fn new(trace_id: TraceId, root_span_id: SpanId) -> Self {
        Self {
            trace_id,
            root_span_id,
            spans: HashMap::new(),
            start_time: Utc::now(),
            end_time: None,
            duration_ms: None,
            service_count: 0,
            error_count: 0,
        }
    }

    /// Add a span to the trace
    pub fn add_span(&mut self, span: Span) {
        let services: std::collections::HashSet<_> = self.spans.values()
            .map(|s| &s.service_name)
            .collect();

        if !services.contains(&span.service_name) {
            self.service_count += 1;
        }

        if matches!(span.status, SpanStatus::Error) {
            self.error_count += 1;
        }

        self.spans.insert(span.context.span_id.clone(), span);
    }

    /// Get span by ID
    pub fn get_span(&self, span_id: &SpanId) -> Option<&Span> {
        self.spans.get(span_id)
    }

    /// Get all spans in chronological order
    pub fn get_spans_chronological(&self) -> Vec<&Span> {
        let mut spans: Vec<_> = self.spans.values().collect();
        spans.sort_by_key(|s| s.start_time);
        spans
    }

    /// Calculate trace duration and finish
    pub fn finish(&mut self) {
        let end_time = Utc::now();
        self.end_time = Some(end_time);
        self.duration_ms = Some((end_time - self.start_time).num_milliseconds() as u64);
    }

    /// Get critical path (longest path through the trace)
    pub fn get_critical_path(&self) -> Vec<SpanId> {
        // Simple implementation - in production this would be more sophisticated
        let mut path = Vec::new();
        if let Some(root_span) = self.spans.get(&self.root_span_id) {
            path.push(root_span.context.span_id.clone());
            self.find_longest_child_path(&root_span.context.span_id, &mut path);
        }
        path
    }

    fn find_longest_child_path(&self, parent_id: &SpanId, path: &mut Vec<SpanId>) {
        let mut longest_child: Option<&Span> = None;
        let mut max_duration = 0;

        for span in self.spans.values() {
            if let Some(parent) = &span.context.parent_span_id {
                if parent == parent_id {
                    if let Some(duration) = span.duration_ms {
                        if duration > max_duration {
                            max_duration = duration;
                            longest_child = Some(span);
                        }
                    }
                }
            }
        }

        if let Some(child) = longest_child {
            path.push(child.context.span_id.clone());
            self.find_longest_child_path(&child.context.span_id, path);
        }
    }
}

/// Tracer for creating and managing spans
pub struct Tracer {
    service_name: String,
    traces: Arc<RwLock<HashMap<TraceId, Trace>>>,
    active_spans: Arc<RwLock<HashMap<SpanId, Span>>>,
}

impl Tracer {
    pub fn new(service_name: String) -> Self {
        Self {
            service_name,
            traces: Arc::new(RwLock::new(HashMap::new())),
            active_spans: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start a new trace with root span
    pub async fn start_trace(&self, operation_name: String) -> Result<SpanContext> {
        let trace_id = TraceId::new();
        let span_id = SpanId::new();
        let context = SpanContext::new(trace_id.clone(), span_id.clone(), None);

        let span = Span::new(context.clone(), operation_name, self.service_name.clone());
        let trace = Trace::new(trace_id.clone(), span_id.clone());

        {
            let mut traces = self.traces.write().await;
            traces.insert(trace_id, trace);
        }

        {
            let mut active_spans = self.active_spans.write().await;
            active_spans.insert(span_id, span);
        }

        Ok(context)
    }

    /// Start a child span
    pub async fn start_span(
        &self,
        parent_context: &SpanContext,
        operation_name: String,
    ) -> Result<SpanContext> {
        let child_context = parent_context.child();
        let span = Span::new(child_context.clone(), operation_name, self.service_name.clone());

        {
            let mut active_spans = self.active_spans.write().await;
            active_spans.insert(child_context.span_id.clone(), span);
        }

        Ok(child_context)
    }

    /// Get active span for modification
    pub async fn with_span<F, R>(&self, span_id: &SpanId, f: F) -> Result<R>
    where
        F: FnOnce(&mut Span) -> R,
    {
        let mut active_spans = self.active_spans.write().await;
        match active_spans.get_mut(span_id) {
            Some(span) => Ok(f(span)),
            None => Err(anyhow::anyhow!("Span not found: {:?}", span_id)),
        }
    }

    /// Finish a span and add it to the trace
    pub async fn finish_span(&self, span_id: &SpanId, status: SpanStatus) -> Result<()> {
        let span = {
            let mut active_spans = self.active_spans.write().await;
            active_spans.remove(span_id)
        };

        if let Some(mut span) = span {
            span.finish(status);

            let mut traces = self.traces.write().await;
            if let Some(trace) = traces.get_mut(&span.context.trace_id) {
                trace.add_span(span);
            }
        }

        Ok(())
    }

    /// Get completed trace
    pub async fn get_trace(&self, trace_id: &TraceId) -> Option<Trace> {
        let traces = self.traces.read().await;
        traces.get(trace_id).cloned()
    }

    /// Get all traces (for debugging/monitoring)
    pub async fn get_all_traces(&self) -> Vec<Trace> {
        let traces = self.traces.read().await;
        traces.values().cloned().collect()
    }

    /// Clean up old traces
    pub async fn cleanup_old_traces(&self, max_age: Duration) -> Result<()> {
        let cutoff = Utc::now() - chrono::Duration::from_std(max_age)?;
        let mut traces = self.traces.write().await;

        traces.retain(|_, trace| {
            trace.start_time > cutoff
        });

        Ok(())
    }
}

/// Trace exporter for sending traces to external systems
#[async_trait::async_trait]
pub trait TraceExporter: Send + Sync {
    async fn export_traces(&self, traces: Vec<Trace>) -> Result<()>;
}

/// Console trace exporter for development
pub struct ConsoleTraceExporter;

#[async_trait::async_trait]
impl TraceExporter for ConsoleTraceExporter {
    async fn export_traces(&self, traces: Vec<Trace>) -> Result<()> {
        for trace in traces {
            tracing::info!("Trace exported: {} - {}ms, {} spans, {} services",
                trace.trace_id.0,
                trace.duration_ms.unwrap_or(0),
                trace.spans.len(),
                trace.service_count
            );
        }
        Ok(())
    }
}

/// HTTP trace exporter for external tracing systems (Jaeger, Zipkin, etc.)
pub struct HttpTraceExporter {
    endpoint: String,
    client: reqwest::Client,
}

impl HttpTraceExporter {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl TraceExporter for HttpTraceExporter {
    async fn export_traces(&self, traces: Vec<Trace>) -> Result<()> {
        let payload = serde_json::to_value(&traces)?;

        let response = self.client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Failed to export traces: {}", response.status()));
        }

        tracing::debug!("Exported {} traces to {}", traces.len(), self.endpoint);
        Ok(())
    }
}

/// Span context propagation utilities
pub mod propagation {
    use super::*;
    use std::collections::HashMap;

    /// Extract span context from HTTP headers
    pub fn extract_from_headers(headers: &HashMap<String, String>) -> Option<SpanContext> {
        let trace_id = headers.get("x-trace-id")?;
        let span_id = headers.get("x-span-id")?;
        let parent_span_id = headers.get("x-parent-span-id")
            .map(|id| SpanId(id.clone()));

        let mut baggage = HashMap::new();
        for (key, value) in headers {
            if key.starts_with("x-baggage-") {
                let baggage_key = key.strip_prefix("x-baggage-").unwrap();
                baggage.insert(baggage_key.to_string(), value.clone());
            }
        }

        Some(SpanContext {
            trace_id: TraceId(trace_id.clone()),
            span_id: SpanId(span_id.clone()),
            parent_span_id,
            baggage,
        })
    }

    /// Inject span context into HTTP headers
    pub fn inject_into_headers(context: &SpanContext) -> HashMap<String, String> {
        let mut headers = HashMap::new();

        headers.insert("x-trace-id".to_string(), context.trace_id.0.clone());
        headers.insert("x-span-id".to_string(), context.span_id.0.clone());

        if let Some(parent_id) = &context.parent_span_id {
            headers.insert("x-parent-span-id".to_string(), parent_id.0.clone());
        }

        for (key, value) in &context.baggage {
            headers.insert(format!("x-baggage-{}", key), value.clone());
        }

        headers
    }
}

/// Tracing middleware for Axum
pub mod middleware {
    use super::*;
    use axum::{
        extract::Request,
        http::HeaderMap,
        middleware::Next,
        response::Response,
    };
    use std::sync::Arc;

    /// Axum middleware for distributed tracing
    pub async fn tracing_middleware(
        headers: HeaderMap,
        mut request: Request,
        next: Next,
    ) -> Response {
        // Extract trace context from headers
        let header_map: HashMap<String, String> = headers
            .iter()
            .filter_map(|(name, value)| {
                value.to_str().ok().map(|v| (name.to_string(), v.to_string()))
            })
            .collect();

        let context = propagation::extract_from_headers(&header_map);

        // Store context in request extensions for handlers to use
        if let Some(ctx) = context {
            request.extensions_mut().insert(ctx);
        }

        let response = next.run(request).await;

        // TODO: Add response headers with trace info
        response
    }
}

/// Performance analysis utilities
pub mod analysis {
    use super::*;

    /// Analyze trace performance and identify bottlenecks
    pub fn analyze_trace_performance(trace: &Trace) -> TraceAnalysis {
        let total_duration = trace.duration_ms.unwrap_or(0);
        let critical_path = trace.get_critical_path();

        let mut service_durations: HashMap<String, u64> = HashMap::new();
        let mut slowest_spans = Vec::new();

        for span in trace.spans.values() {
            if let Some(duration) = span.duration_ms {
                *service_durations.entry(span.service_name.clone()).or_insert(0) += duration;

                slowest_spans.push((span.context.span_id.clone(), duration));
            }
        }

        slowest_spans.sort_by(|a, b| b.1.cmp(&a.1));
        slowest_spans.truncate(5);

        TraceAnalysis {
            total_duration_ms: total_duration,
            critical_path_span_count: critical_path.len(),
            service_breakdown: service_durations,
            slowest_operations: slowest_spans,
            error_count: trace.error_count,
            service_count: trace.service_count,
        }
    }
}

/// Trace performance analysis result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceAnalysis {
    pub total_duration_ms: u64,
    pub critical_path_span_count: usize,
    pub service_breakdown: HashMap<String, u64>,
    pub slowest_operations: Vec<(SpanId, u64)>,
    pub error_count: usize,
    pub service_count: usize,
}

/// Global tracing manager
pub struct TracingManager {
    tracer: Arc<Tracer>,
    exporters: Vec<Box<dyn TraceExporter>>,
    export_interval: Duration,
}

impl TracingManager {
    pub fn new(service_name: String) -> Self {
        Self {
            tracer: Arc::new(Tracer::new(service_name)),
            exporters: Vec::new(),
            export_interval: Duration::from_secs(10),
        }
    }

    /// Add a trace exporter
    pub fn add_exporter(&mut self, exporter: Box<dyn TraceExporter>) {
        self.exporters.push(exporter);
    }

    /// Get the tracer
    pub fn tracer(&self) -> Arc<Tracer> {
        self.tracer.clone()
    }

    /// Start periodic trace export
    pub async fn start_export_loop(self: Arc<Self>) -> Result<()> {
        let mut interval = tokio::time::interval(self.export_interval);

        loop {
            interval.tick().await;

            let traces = self.tracer.get_all_traces().await;
            if !traces.is_empty() {
                for exporter in &self.exporters {
                    if let Err(e) = exporter.export_traces(traces.clone()).await {
                        tracing::error!("Failed to export traces: {}", e);
                    }
                }
            }

            // Clean up old traces
            if let Err(e) = self.tracer.cleanup_old_traces(Duration::from_secs(3600)).await {
                tracing::error!("Failed to cleanup old traces: {}", e);
            }
        }
    }
}