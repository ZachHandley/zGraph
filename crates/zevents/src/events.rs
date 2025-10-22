use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use ulid::Ulid;

/// Unique identifier for events
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId(pub Ulid);

impl EventId {
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    pub fn from_ulid(ulid: Ulid) -> Self {
        Self(ulid)
    }

    pub fn as_ulid(&self) -> Ulid {
        self.0
    }
}

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for EventId {
    fn from(s: String) -> Self {
        Self(Ulid::from_string(&s).unwrap_or_else(|_| Ulid::new()))
    }
}

/// Event type classification
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventType {
    /// Job lifecycle events
    JobCreated,
    JobStarted,
    JobCompleted,
    JobFailed,
    JobCanceled,
    JobRetrying,

    /// Database operation events
    TableCreated,
    TableDropped,
    IndexBuilt,
    IndexDropped,
    DataIngested,
    QueryExecuted,

    /// User management events
    UserCreated,
    UserUpdated,
    UserDeleted,
    UserLoggedIn,
    UserLoggedOut,

    /// System events
    SystemStarted,
    SystemStopped,
    ConfigurationChanged,
    HealthCheckFailed,
    MemoryPressure,
    DiskSpaceWarning,

    /// Real-time events
    WebSocketConnected,
    WebSocketDisconnected,
    WebRTCConnected,
    WebRTCDisconnected,

    /// Database change events
    RowInserted,
    RowUpdated,
    RowDeleted,
    SchemaChanged,

    /// Transaction events
    TransactionBegun,
    TransactionCommitted,
    TransactionRolledBack,
    TransactionAborted,

    /// Collection events
    CollectionCreated,
    CollectionUpdated,
    CollectionDeleted,
    ProjectionUpdated,

    /// Identity and credential events
    CredentialCreated,
    CredentialVerified,
    ProofGenerated,
    ProofVerified,
    IdentityAudit,

    /// Function execution events
    FunctionCall,

    /// HTTP request events
    HttpRequest,

    /// Error events
    Error,

    /// Custom application events
    Custom(String),
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EventType::JobCreated => write!(f, "job.created"),
            EventType::JobStarted => write!(f, "job.started"),
            EventType::JobCompleted => write!(f, "job.completed"),
            EventType::JobFailed => write!(f, "job.failed"),
            EventType::JobCanceled => write!(f, "job.canceled"),
            EventType::JobRetrying => write!(f, "job.retrying"),
            EventType::TableCreated => write!(f, "table.created"),
            EventType::TableDropped => write!(f, "table.dropped"),
            EventType::IndexBuilt => write!(f, "index.built"),
            EventType::IndexDropped => write!(f, "index.dropped"),
            EventType::DataIngested => write!(f, "data.ingested"),
            EventType::QueryExecuted => write!(f, "query.executed"),
            EventType::UserCreated => write!(f, "user.created"),
            EventType::UserUpdated => write!(f, "user.updated"),
            EventType::UserDeleted => write!(f, "user.deleted"),
            EventType::UserLoggedIn => write!(f, "user.logged_in"),
            EventType::UserLoggedOut => write!(f, "user.logged_out"),
            EventType::SystemStarted => write!(f, "system.started"),
            EventType::SystemStopped => write!(f, "system.stopped"),
            EventType::ConfigurationChanged => write!(f, "system.config_changed"),
            EventType::HealthCheckFailed => write!(f, "system.health_check_failed"),
            EventType::MemoryPressure => write!(f, "system.memory_pressure"),
            EventType::DiskSpaceWarning => write!(f, "system.disk_space_warning"),
            EventType::WebSocketConnected => write!(f, "websocket.connected"),
            EventType::WebSocketDisconnected => write!(f, "websocket.disconnected"),
            EventType::WebRTCConnected => write!(f, "webrtc.connected"),
            EventType::WebRTCDisconnected => write!(f, "webrtc.disconnected"),
            EventType::RowInserted => write!(f, "row.inserted"),
            EventType::RowUpdated => write!(f, "row.updated"),
            EventType::RowDeleted => write!(f, "row.deleted"),
            EventType::SchemaChanged => write!(f, "schema.changed"),
            EventType::TransactionBegun => write!(f, "transaction.begun"),
            EventType::TransactionCommitted => write!(f, "transaction.committed"),
            EventType::TransactionRolledBack => write!(f, "transaction.rolled_back"),
            EventType::TransactionAborted => write!(f, "transaction.aborted"),
            EventType::CollectionCreated => write!(f, "collection.created"),
            EventType::CollectionUpdated => write!(f, "collection.updated"),
            EventType::CollectionDeleted => write!(f, "collection.deleted"),
            EventType::ProjectionUpdated => write!(f, "projection.updated"),
            EventType::CredentialCreated => write!(f, "credential.created"),
            EventType::CredentialVerified => write!(f, "credential.verified"),
            EventType::ProofGenerated => write!(f, "proof.generated"),
            EventType::ProofVerified => write!(f, "proof.verified"),
            EventType::IdentityAudit => write!(f, "identity.audit"),
            EventType::FunctionCall => write!(f, "function.call"),
            EventType::HttpRequest => write!(f, "http.request"),
            EventType::Error => write!(f, "error"),
            EventType::Custom(name) => write!(f, "custom.{}", name),
        }
    }
}

impl From<String> for EventType {
    fn from(s: String) -> Self {
        match s.as_str() {
            "job.created" => EventType::JobCreated,
            "job.started" => EventType::JobStarted,
            "job.completed" => EventType::JobCompleted,
            "job.failed" => EventType::JobFailed,
            "job.canceled" => EventType::JobCanceled,
            "job.retrying" => EventType::JobRetrying,
            "table.created" => EventType::TableCreated,
            "table.dropped" => EventType::TableDropped,
            "index.built" => EventType::IndexBuilt,
            "index.dropped" => EventType::IndexDropped,
            "data.ingested" => EventType::DataIngested,
            "query.executed" => EventType::QueryExecuted,
            "user.created" => EventType::UserCreated,
            "user.updated" => EventType::UserUpdated,
            "user.deleted" => EventType::UserDeleted,
            "user.logged_in" => EventType::UserLoggedIn,
            "user.logged_out" => EventType::UserLoggedOut,
            "system.started" => EventType::SystemStarted,
            "system.stopped" => EventType::SystemStopped,
            "system.config_changed" => EventType::ConfigurationChanged,
            "system.health_check_failed" => EventType::HealthCheckFailed,
            "system.memory_pressure" => EventType::MemoryPressure,
            "system.disk_space_warning" => EventType::DiskSpaceWarning,
            "websocket.connected" => EventType::WebSocketConnected,
            "websocket.disconnected" => EventType::WebSocketDisconnected,
            "webrtc.connected" => EventType::WebRTCConnected,
            "webrtc.disconnected" => EventType::WebRTCDisconnected,
            "row.inserted" => EventType::RowInserted,
            "row.updated" => EventType::RowUpdated,
            "row.deleted" => EventType::RowDeleted,
            "schema.changed" => EventType::SchemaChanged,
            "transaction.begun" => EventType::TransactionBegun,
            "transaction.committed" => EventType::TransactionCommitted,
            "transaction.rolled_back" => EventType::TransactionRolledBack,
            "transaction.aborted" => EventType::TransactionAborted,
            "collection.created" => EventType::CollectionCreated,
            "collection.updated" => EventType::CollectionUpdated,
            "collection.deleted" => EventType::CollectionDeleted,
            "projection.updated" => EventType::ProjectionUpdated,
            "credential.created" => EventType::CredentialCreated,
            "credential.verified" => EventType::CredentialVerified,
            "proof.generated" => EventType::ProofGenerated,
            "proof.verified" => EventType::ProofVerified,
            "identity.audit" => EventType::IdentityAudit,
            "function.call" => EventType::FunctionCall,
            "http.request" => EventType::HttpRequest,
            "error" => EventType::Error,
            custom if custom.starts_with("custom.") => {
                EventType::Custom(custom.strip_prefix("custom.").unwrap().to_string())
            }
            _ => EventType::Custom(s),
        }
    }
}

/// Event data payload
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EventData {
    /// Job-related event data
    Job {
        job_id: uuid::Uuid,
        org_id: u64,
        status: Option<String>,
        worker_id: Option<String>,
        error: Option<String>,
        output: Option<serde_json::Value>,
    },

    /// Database operation data
    Database {
        org_id: u64,
        table_name: Option<String>,
        index_name: Option<String>,
        operation: String,
        rows_affected: Option<u64>,
        duration_ms: Option<u64>,
    },

    /// User management data
    User {
        user_id: Option<u64>,
        org_id: u64,
        email: Option<String>,
        action: String,
        ip_address: Option<String>,
    },

    /// System monitoring data
    System {
        component: String,
        status: String,
        metrics: Option<HashMap<String, serde_json::Value>>,
        error: Option<String>,
    },

    /// Real-time connection data
    Connection {
        connection_id: String,
        user_id: Option<u64>,
        org_id: u64,
        protocol: String, // "websocket" or "webrtc"
        status: String,
    },

    /// Collection management data
    Collection {
        collection_name: String,
        org_id: u64,
        operation: String,
        projection: Option<serde_json::Value>,
        records_count: Option<u64>,
    },

    /// Database change data capture
    DatabaseChange {
        org_id: u64,
        table_name: String,
        operation: String, // "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP"
        transaction_id: Option<String>,
        user_id: Option<u64>,
        connection_id: Option<String>,
        old_values: Option<HashMap<String, serde_json::Value>>,
        new_values: Option<HashMap<String, serde_json::Value>>,
        where_clause: Option<String>,
        rows_affected: u64,
        schema_changes: Option<serde_json::Value>, // For DDL operations
        timestamp_ms: i64,
    },

    /// Transaction management data
    Transaction {
        transaction_id: String,
        connection_id: String,
        org_id: u64,
        operations_count: Option<u64>,
        duration_ms: Option<u64>,
        error: Option<String>,
    },

    /// Generic custom data
    Custom(serde_json::Value),
}

/// Event metadata for tracing and debugging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    /// Event source component
    pub source: String,
    /// Event correlation ID for tracing
    pub correlation_id: Option<String>,
    /// Event causation ID (what caused this event)
    pub causation_id: Option<EventId>,
    /// Event version for schema evolution
    pub version: String,
    /// Additional arbitrary metadata
    pub attributes: HashMap<String, String>,
}

impl Default for EventMetadata {
    fn default() -> Self {
        Self {
            source: "zevents".to_string(),
            correlation_id: None,
            causation_id: None,
            version: "1.0".to_string(),
            attributes: HashMap::new(),
        }
    }
}

/// Core event structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Unique event identifier
    pub id: EventId,
    /// Event type classification
    pub event_type: EventType,
    /// Event payload data
    pub data: EventData,
    /// Event metadata
    pub metadata: EventMetadata,
    /// Event timestamp
    pub timestamp: DateTime<Utc>,
    /// Organization ID for multi-tenant isolation
    pub org_id: u64,
}

impl Event {
    pub fn new(event_type: EventType, data: EventData, org_id: u64) -> Self {
        Self {
            id: EventId::new(),
            event_type,
            data,
            metadata: EventMetadata::default(),
            timestamp: Utc::now(),
            org_id,
        }
    }

    pub fn with_metadata(mut self, metadata: EventMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: String) -> Self {
        self.metadata.correlation_id = Some(correlation_id);
        self
    }

    pub fn with_causation_id(mut self, causation_id: EventId) -> Self {
        self.metadata.causation_id = Some(causation_id);
        self
    }

    pub fn with_source(mut self, source: String) -> Self {
        self.metadata.source = source;
        self
    }

    /// Check if this event matches a topic pattern
    pub fn matches_topic(&self, topic: &str) -> bool {
        let event_topic = self.to_topic();

        // Exact match
        if event_topic == topic {
            return true;
        }

        // Wildcard matching
        // "*" matches any single segment
        // "**" matches any number of segments
        if topic.contains('*') {
            return self.matches_wildcard_topic(&event_topic, topic);
        }

        false
    }

    /// Generate a topic string for this event
    pub fn to_topic(&self) -> String {
        match &self.data {
            EventData::Job { job_id, .. } => format!("jobs/{}", job_id),
            EventData::Database { table_name: Some(table), .. } => {
                format!("tables/{}/{}", table, self.event_type)
            }
            EventData::Database { .. } => format!("database/{}", self.event_type),
            EventData::User { user_id: Some(uid), .. } => format!("users/{}/{}", uid, self.event_type),
            EventData::User { .. } => format!("users/{}", self.event_type),
            EventData::System { component, .. } => format!("system/{}/{}", component, self.event_type),
            EventData::Connection { connection_id, .. } => {
                format!("connections/{}/{}", connection_id, self.event_type)
            }
            EventData::Collection { collection_name, .. } => {
                format!("collections/{}/{}", collection_name, self.event_type)
            }
            EventData::DatabaseChange { table_name, operation, .. } => {
                format!("tables/{}/{}/{}", table_name, operation.to_lowercase(), self.event_type)
            }
            EventData::Transaction { transaction_id, .. } => {
                format!("transactions/{}/{}", transaction_id, self.event_type)
            }
            EventData::Custom(_) => format!("custom/{}", self.event_type),
        }
    }

    fn matches_wildcard_topic(&self, event_topic: &str, pattern: &str) -> bool {
        let event_parts: Vec<&str> = event_topic.split('/').collect();
        let pattern_parts: Vec<&str> = pattern.split('/').collect();

        self.matches_parts(&event_parts, &pattern_parts, 0, 0)
    }

    fn matches_parts(&self, event_parts: &[&str], pattern_parts: &[&str], e_idx: usize, p_idx: usize) -> bool {
        // Both exhausted - match
        if e_idx >= event_parts.len() && p_idx >= pattern_parts.len() {
            return true;
        }

        // Pattern exhausted but event has more - no match unless last pattern was **
        if p_idx >= pattern_parts.len() {
            return false;
        }

        // Event exhausted but pattern has more - check if remaining are **
        if e_idx >= event_parts.len() {
            return pattern_parts[p_idx..].iter().all(|&p| p == "**");
        }

        let pattern_part = pattern_parts[p_idx];

        match pattern_part {
            "**" => {
                // Try matching ** with 0, 1, 2, ... segments
                for skip in 0..=(event_parts.len() - e_idx) {
                    if self.matches_parts(event_parts, pattern_parts, e_idx + skip, p_idx + 1) {
                        return true;
                    }
                }
                false
            }
            "*" => {
                // Match exactly one segment
                self.matches_parts(event_parts, pattern_parts, e_idx + 1, p_idx + 1)
            }
            _ => {
                // Exact match required
                if event_parts[e_idx] == pattern_part {
                    self.matches_parts(event_parts, pattern_parts, e_idx + 1, p_idx + 1)
                } else {
                    false
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_id_generation() {
        let id1 = EventId::new();
        let id2 = EventId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_event_type_display() {
        assert_eq!(EventType::JobCreated.to_string(), "job.created");
        assert_eq!(EventType::Custom("test".to_string()).to_string(), "custom.test");
    }

    #[test]
    fn test_event_topic_generation() {
        let event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 1,
                status: Some("pending".to_string()),
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );

        let topic = event.to_topic();
        assert!(topic.starts_with("jobs/"));
    }

    #[test]
    fn test_wildcard_matching() {
        let event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 1,
                status: Some("pending".to_string()),
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );

        assert!(event.matches_topic("jobs/*"));
        assert!(event.matches_topic("jobs/**"));
        assert!(event.matches_topic("**"));
        assert!(!event.matches_topic("tables/*"));
    }
}