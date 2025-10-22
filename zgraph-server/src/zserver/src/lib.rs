mod webrtc;
mod events;
mod websocket_exactly_once;
mod security;

// Re-export WebRTC types for testing
pub use webrtc::{WebRTCState, SubscriptionManager, FunctionTriggerManager, WebRTCConfig};

use async_compression::tokio::bufread::GzipDecoder;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path as AxumPath, Query, ConnectInfo};
use axum::response::IntoResponse;
use axum::{
    http::header::{CONTENT_DISPOSITION, CONTENT_TYPE},
    http::{HeaderValue, StatusCode},
    routing::{delete, get, post},
    Json, Router,
};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use simd_json as simdj;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::BufReader;
use tokio::sync::{Mutex, Notify, RwLock as TokioRwLock};
use tokio_util::{
    codec::{FramedRead, LinesCodec},
    io::{StreamReader, SyncIoBridge},
};
use tower_http::services::ServeDir;
use tracing_subscriber::{fmt, EnvFilter};
use zann_hnsw as zann;
use zauth::{
    AuthConfig, AuthRepository, AuthService, AuthSession, AuthState, EnvironmentSummary,
    LoginTokens, TokenKind,
};
use zbrain::{self as zjob, CancelResult, JobState};
use zcollections as zcoll;
use zcore_catalog::Catalog;
use zcore_storage::Store;
use zexec_engine::{exec_sql_store, sample_table_rows, ExecResult};
use zpermissions::{
    Crud, PermissionError, PermissionRecord, PermissionService, Principal, ResourceKind,
};
use zvec_kernels::{batch_distance, Metric};
use zworker::LocalWorker;
use zmesh_network::{ZMeshNetwork, MeshConfig, types::NodeId};
use zidentity::{IdentityService, IdentityConfig, CredentialRequest, VerificationRequest, CredentialMetadata, VerificationResult, RevocationResult};
use ztransaction::distributed::{
    GlobalTransactionCoordinator, DistributedTransactionConfig, ParticipantId, ProtocolType,
    TransactionOperation, SagaCoordinator, SagaStep, SagaConfig, GlobalTransactionId,
    DistributedTransaction, DistributedTransactionStatus, DistributedTransactionMetrics,
    SagaId,
};
use zobservability::{
    ObservabilityManager, ObservabilityConfig,
    collectors::MetricsAggregator,
    health::{HealthManager, DatabaseHealthCheck, VectorIndexHealthCheck, WebSocketHealthCheck, SystemResourceHealthCheck},
};
// Temporarily commented out to isolate zserver compilation issues
// use zevents::{EventSystem, EventSystemConfig};
// use zfunctions::FunctionExecutor;
// Simplified WebRTC imports - stub implementations
use std::collections::HashMap;
use crate::webrtc::{
    // MediaEngineConfig,
    // create_media_engine,
    // create_setting_engine,
    handlers::*,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricReq {
    L2,
    Ip,
    Cosine,
}

impl From<MetricReq> for Metric {
    fn from(m: MetricReq) -> Self {
        match m {
            MetricReq::L2 => Metric::L2,
            MetricReq::Ip => Metric::InnerProduct,
            MetricReq::Cosine => Metric::Cosine,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct KnnRequest {
    pub metric: MetricReq,
    pub query: Vec<f32>,
    pub vectors: Vec<Vec<f32>>, // input dataset
    pub top_k: usize,
}

#[derive(Debug, Serialize)]
pub struct KnnResponse {
    pub results: Vec<KnnHit>,
}

#[derive(Debug, Serialize)]
pub struct KnnHit {
    pub index: usize,
    pub distance: f32,
}

#[derive(Debug, Deserialize)]
struct SignupRequest {
    email: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct LoginRequest {
    email: String,
    password: String,
    environment: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RefreshRequest {
    refresh_token: String,
}

#[derive(Debug, Deserialize)]
struct SwitchEnvironmentRequest {
    refresh_token: String,
    environment: String,
}

#[derive(Debug, Deserialize)]
struct LogoutRequest {
    refresh_token: String,
}

// WebRTC request/response structures
#[derive(Debug, Deserialize)]
struct RtcOfferRequest {
    offer: RTCSessionDescriptionPayload,
    ice_servers: Option<Vec<IceServerConfig>>,
}

#[derive(Debug, Serialize)]
struct RtcOfferResponse {
    answer: RTCSessionDescriptionPayload,
    ice_servers: Vec<IceServerConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct RTCSessionDescriptionPayload {
    r#type: String, // "offer" or "answer"
    sdp: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct IceServerConfig {
    pub urls: Vec<String>,
    pub username: Option<String>,
    pub credential: Option<String>,
}

#[derive(Debug, Deserialize)]
struct _WebRtcSignalingMessage {
    _action: String, // "ice-candidate" | "connection-state"
    _peer_id: String,
    _data: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct LoginResponse {
    access_token: String,
    refresh_token: String,
    access_expires_at: i64,
    refresh_expires_at: i64,
    environment: EnvironmentSummary,
}

// Transaction request/response structures
#[derive(Debug, Deserialize)]
struct BeginTransactionRequest {
    isolation_level: Option<IsolationLevel>,
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum IsolationLevel {
    ReadCommitted,
    ReadUncommitted,
    RepeatableRead,
    Serializable,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        Self::ReadCommitted
    }
}

#[derive(Debug, Serialize)]
struct BeginTransactionResponse {
    transaction_id: String,
    isolation_level: IsolationLevel,
    expires_at: i64,
}

#[derive(Debug, Deserialize)]
struct ExecuteTransactionRequest {
    sql: String,
}

#[derive(Debug, Serialize)]
struct ExecuteTransactionResponse {
    rows: Vec<serde_json::Map<String, serde_json::Value>>,
    affected_rows: Option<u64>,
}

#[derive(Debug, Serialize)]
struct TransactionStatusResponse {
    transaction_id: String,
    status: TransactionStatus,
    isolation_level: IsolationLevel,
    created_at: i64,
    expires_at: i64,
    operations_count: u64,
    last_activity: i64,
}

#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TransactionStatus {
    Active,
    Committed,
    RolledBack,
    Expired,
}

#[derive(Debug, Serialize)]
struct TransactionCommitResponse {
    transaction_id: String,
    committed_at: i64,
    operations_count: u64,
}

#[derive(Debug, Serialize)]
struct TransactionRollbackResponse {
    transaction_id: String,
    rolled_back_at: i64,
}

impl From<LoginTokens> for LoginResponse {
    fn from(value: LoginTokens) -> Self {
        Self {
            access_token: value.access_token,
            refresh_token: value.refresh_token,
            access_expires_at: value.access_expires_at,
            refresh_expires_at: value.refresh_expires_at,
            environment: value.environment,
        }
    }
}

async fn knn_handler(
    _auth: AuthSession,
    Json(req): Json<KnnRequest>,
) -> Result<Json<KnnResponse>, axum::http::StatusCode> {
    let metric: Metric = req.metric.into();
    let vec_refs: Vec<&[f32]> = req.vectors.iter().map(|v| v.as_slice()).collect();
    match batch_distance(&req.query, &vec_refs, metric, req.top_k) {
        Ok(v) => {
            let results = v
                .into_iter()
                .map(|(i, d)| KnnHit {
                    index: i,
                    distance: d,
                })
                .collect();
            Ok(Json(KnnResponse { results }))
        }
        Err(_) => Err(axum::http::StatusCode::BAD_REQUEST),
    }
}

pub struct AppState {
    pub auth: AuthConfig,
    pub store: &'static Store,
    pub catalog: Catalog<'static>,
    pub files_root: PathBuf,
    pub job_queue: Arc<zjob::JobQueue>,
    pub ws_metrics: Arc<WsMetrics>,
    pub permissions: PermissionService<'static>,
    pub default_org_id: u64,
    // Transaction management
    pub transaction_manager: Arc<TransactionManager>,
    // Distributed transaction coordination
    pub distributed_coordinator: Option<Arc<GlobalTransactionCoordinator>>,
    pub saga_coordinator: Option<Arc<SagaCoordinator>>,
    // Identity service
    pub identity: Arc<IdentityService>,
    // Mesh networking
    pub mesh_network: Option<Arc<ZMeshNetwork>>,
    // Legacy WebRTC fields (kept for backward compatibility)
    pub webrtc_api: Arc<String>,
    pub ice_servers: Vec<IceServerConfig>,
    pub rtc_connections: Arc<TokioRwLock<HashMap<String, Arc<String>>>>,
    // Enhanced WebRTC features
    pub webrtc_state: WebRTCState,
    pub subscription_manager: Arc<SubscriptionManager>,
    pub function_trigger_manager: Arc<FunctionTriggerManager>,
    // Observability
    pub observability: Arc<ObservabilityManager>,
    pub metrics: Arc<MetricsAggregator>,
    // Temporarily commented out to isolate zserver compilation issues
    // pub function_executor: Arc<FunctionExecutor>,
    // pub event_system: Option<Arc<EventSystem>>,
}

#[derive(Default)]
pub struct WsMetrics {
    active: AtomicU64,
    dropped_total: AtomicU64,
}

impl WsMetrics {
    fn inc_active(&self) {
        self.active.fetch_add(1, Ordering::Relaxed);
    }
    fn dec_active(&self) {
        let _ = self
            .active
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_sub(1));
    }
    fn add_dropped(&self, n: u64) {
        self.dropped_total.fetch_add(n, Ordering::Relaxed);
    }
    fn snapshot(&self) -> (u64, u64) {
        (
            self.active.load(Ordering::Relaxed),
            self.dropped_total.load(Ordering::Relaxed),
        )
    }
}

// Transaction management structures
#[derive(Clone)]
pub struct TransactionContext {
    pub id: String,
    pub org_id: u64,
    pub user_id: u64,
    pub token_id: String,
    pub isolation_level: IsolationLevel,
    pub status: TransactionStatus,
    pub created_at: i64,
    pub expires_at: i64,
    pub operations_count: u64,
    pub last_activity: i64,
    pub write_tx: Option<Arc<Mutex<zcore_storage::WriteTransaction>>>,
}

impl std::fmt::Debug for TransactionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionContext")
            .field("id", &self.id)
            .field("org_id", &self.org_id)
            .field("user_id", &self.user_id)
            .field("token_id", &self.token_id)
            .field("isolation_level", &self.isolation_level)
            .field("status", &self.status)
            .field("created_at", &self.created_at)
            .field("expires_at", &self.expires_at)
            .field("operations_count", &self.operations_count)
            .field("last_activity", &self.last_activity)
            .field("write_tx", &self.write_tx.is_some())
            .finish()
    }
}

impl TransactionContext {
    pub fn new(
        org_id: u64,
        user_id: u64,
        token_id: String,
        isolation_level: IsolationLevel,
        timeout_seconds: u64,
    ) -> Self {
        let now = chrono::Utc::now().timestamp();
        let id = uuid::Uuid::new_v4().to_string();

        Self {
            id,
            org_id,
            user_id,
            token_id,
            isolation_level,
            status: TransactionStatus::Active,
            created_at: now,
            expires_at: now + timeout_seconds as i64,
            operations_count: 0,
            last_activity: now,
            write_tx: None,
        }
    }

    pub fn is_expired(&self) -> bool {
        chrono::Utc::now().timestamp() > self.expires_at
    }

    pub fn touch(&mut self) {
        self.last_activity = chrono::Utc::now().timestamp();
    }

    pub fn increment_operations(&mut self) {
        self.operations_count += 1;
        self.touch();
    }

    pub fn belongs_to_session(&self, auth: &AuthSession) -> bool {
        self.org_id == auth.org_id && self.token_id == auth.token_id
    }
}

#[derive(Debug)]
pub struct TransactionManager {
    transactions: Arc<TokioRwLock<HashMap<String, TransactionContext>>>,
    store: &'static Store,
    cleanup_interval: std::time::Duration,
    max_transactions_per_user: usize,
}

impl TransactionManager {
    pub fn new(store: &'static Store) -> Self {
        Self {
            transactions: Arc::new(TokioRwLock::new(HashMap::new())),
            store,
            cleanup_interval: std::time::Duration::from_secs(60), // Clean up every minute
            max_transactions_per_user: 10,
        }
    }

    pub async fn begin_transaction(
        &self,
        auth: &AuthSession,
        isolation_level: IsolationLevel,
        timeout_seconds: u64,
    ) -> Result<TransactionContext, String> {
        let mut transactions = self.transactions.write().await;

        // Check user transaction limits
        let user_tx_count = transactions
            .values()
            .filter(|tx| tx.org_id == auth.org_id && tx.token_id == auth.token_id && tx.status == TransactionStatus::Active)
            .count();

        if user_tx_count >= self.max_transactions_per_user {
            return Err(format!(
                "Transaction limit exceeded: {} active transactions (max {})",
                user_tx_count, self.max_transactions_per_user
            ));
        }

        // Create new transaction context
        let tx_context = TransactionContext::new(
            auth.org_id,
            auth.user_id,
            auth.token_id.clone(),
            isolation_level,
            timeout_seconds,
        );

        // Initialize write transaction if needed
        let mut tx_context = tx_context;
        let write_tx = self.store.begin_write()
            .map_err(|e| format!("Failed to begin database transaction: {}", e))?;
        tx_context.write_tx = Some(Arc::new(Mutex::new(write_tx)));

        let tx_id = tx_context.id.clone();
        transactions.insert(tx_id.clone(), tx_context.clone());

        tracing::info!(
            transaction_id = %tx_id,
            org_id = auth.org_id,
            user_id = auth.user_id,
            isolation_level = ?isolation_level,
            "Transaction started"
        );

        Ok(tx_context)
    }

    pub async fn get_transaction(&self, tx_id: &str, auth: &AuthSession) -> Result<TransactionContext, String> {
        let transactions = self.transactions.read().await;

        match transactions.get(tx_id) {
            Some(tx) if tx.belongs_to_session(auth) => {
                if tx.is_expired() {
                    Err("Transaction has expired".to_string())
                } else if tx.status != TransactionStatus::Active {
                    Err(format!("Transaction is not active (status: {:?})", tx.status))
                } else {
                    Ok(tx.clone())
                }
            },
            Some(_) => Err("Transaction not found or access denied".to_string()),
            None => Err("Transaction not found".to_string()),
        }
    }

    pub async fn update_transaction<F>(&self, tx_id: &str, auth: &AuthSession, updater: F) -> Result<TransactionContext, String>
    where
        F: FnOnce(&mut TransactionContext),
    {
        let mut transactions = self.transactions.write().await;

        match transactions.get_mut(tx_id) {
            Some(tx) if tx.belongs_to_session(auth) => {
                if tx.is_expired() {
                    tx.status = TransactionStatus::Expired;
                    Err("Transaction has expired".to_string())
                } else if tx.status != TransactionStatus::Active {
                    Err(format!("Transaction is not active (status: {:?})", tx.status))
                } else {
                    updater(tx);
                    Ok(tx.clone())
                }
            },
            Some(_) => Err("Transaction not found or access denied".to_string()),
            None => Err("Transaction not found".to_string()),
        }
    }

    pub async fn commit_transaction(&self, tx_id: &str, auth: &AuthSession) -> Result<(u64, i64), String> {
        let mut transactions = self.transactions.write().await;

        match transactions.get_mut(tx_id) {
            Some(tx) if tx.belongs_to_session(auth) => {
                if tx.is_expired() {
                    tx.status = TransactionStatus::Expired;
                    Err("Transaction has expired".to_string())
                } else if tx.status != TransactionStatus::Active {
                    Err(format!("Transaction is not active (status: {:?})", tx.status))
                } else {
                    // Commit the underlying database transaction
                    if let Some(write_tx_arc) = &tx.write_tx {
                        let mut write_tx = write_tx_arc.lock().await;
                        write_tx.commit(self.store)
                            .map_err(|e| format!("Failed to commit transaction: {}", e))?;
                    }

                    let operations_count = tx.operations_count;
                    let committed_at = chrono::Utc::now().timestamp();
                    tx.status = TransactionStatus::Committed;

                    tracing::info!(
                        transaction_id = %tx_id,
                        org_id = auth.org_id,
                        operations_count = operations_count,
                        "Transaction committed"
                    );

                    Ok((operations_count, committed_at))
                }
            },
            Some(_) => Err("Transaction not found or access denied".to_string()),
            None => Err("Transaction not found".to_string()),
        }
    }

    pub async fn rollback_transaction(&self, tx_id: &str, auth: &AuthSession) -> Result<i64, String> {
        let mut transactions = self.transactions.write().await;

        match transactions.get_mut(tx_id) {
            Some(tx) if tx.belongs_to_session(auth) => {
                if tx.status == TransactionStatus::RolledBack {
                    Err("Transaction already rolled back".to_string())
                } else if tx.status == TransactionStatus::Committed {
                    Err("Cannot rollback committed transaction".to_string())
                } else {
                    // Drop the write transaction to trigger rollback
                    tx.write_tx = None;

                    let rolled_back_at = chrono::Utc::now().timestamp();
                    tx.status = TransactionStatus::RolledBack;

                    tracing::info!(
                        transaction_id = %tx_id,
                        org_id = auth.org_id,
                        "Transaction rolled back"
                    );

                    Ok(rolled_back_at)
                }
            },
            Some(_) => Err("Transaction not found or access denied".to_string()),
            None => Err("Transaction not found".to_string()),
        }
    }

    pub async fn cleanup_expired_transactions(&self) {
        let mut transactions = self.transactions.write().await;
        let now = chrono::Utc::now().timestamp();

        let expired_ids: Vec<String> = transactions
            .iter_mut()
            .filter_map(|(id, tx)| {
                if tx.status == TransactionStatus::Active && now > tx.expires_at {
                    tx.status = TransactionStatus::Expired;
                    tx.write_tx = None; // Drop to trigger rollback
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();

        for tx_id in &expired_ids {
            tracing::warn!(transaction_id = %tx_id, "Transaction expired and rolled back");
        }

        if !expired_ids.is_empty() {
            tracing::info!(count = expired_ids.len(), "Cleaned up expired transactions");
        }
    }

    pub fn start_cleanup_task(manager: Arc<Self>) {
        let cleanup_manager = Arc::clone(&manager);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_manager.cleanup_interval);
            loop {
                interval.tick().await;
                cleanup_manager.cleanup_expired_transactions().await;
            }
        });
    }
}

impl Clone for AppState {
    fn clone(&self) -> Self {
        Self {
            auth: self.auth.clone(),
            store: self.store,
            catalog: Catalog::new(self.store),
            files_root: self.files_root.clone(),
            job_queue: self.job_queue.clone(),
            ws_metrics: self.ws_metrics.clone(),
            permissions: self.permissions,
            default_org_id: self.default_org_id,
            // Transaction management
            transaction_manager: self.transaction_manager.clone(),
            distributed_coordinator: self.distributed_coordinator.clone(),
            saga_coordinator: self.saga_coordinator.clone(),
            // Identity service
            identity: self.identity.clone(),
            // Mesh networking
            mesh_network: self.mesh_network.clone(),
            // Legacy WebRTC fields
            webrtc_api: self.webrtc_api.clone(),
            ice_servers: self.ice_servers.clone(),
            rtc_connections: self.rtc_connections.clone(),
            // Enhanced WebRTC features
            webrtc_state: self.webrtc_state.clone(),
            subscription_manager: self.subscription_manager.clone(),
            function_trigger_manager: self.function_trigger_manager.clone(),
            // Observability
            observability: self.observability.clone(),
            metrics: self.metrics.clone(),
            // function_executor: self.function_executor.clone(),
            // event_system: self.event_system.clone(),
        }
    }
}

fn principals_for(auth: &AuthSession) -> Vec<Principal> {
    let mut principals = Vec::with_capacity(1 + auth.roles.len() + auth.labels.len());
    let user_principal = auth
        .user_id
        .map(|uid| Principal::User(uid.to_string()))
        .unwrap_or_else(|| Principal::User(auth.token_id.clone()));
    principals.push(user_principal);
    for role in &auth.roles {
        principals.push(Principal::Role(role.clone()));
    }
    for label in &auth.labels {
        principals.push(Principal::Label(label.clone()));
    }
    principals
}

fn ensure_permission(
    perms: PermissionService<'static>,
    org_id: u64,
    principals: &[Principal],
    resource: ResourceKind,
    required: Crud,
) -> Result<(), axum::http::StatusCode> {
    match perms.ensure(org_id, principals, resource, required) {
        Ok(()) => Ok(()),
        Err(PermissionError::Forbidden { .. }) => Err(axum::http::StatusCode::FORBIDDEN),
        Err(PermissionError::InvalidPrincipalId(msg)) => {
            tracing::error!(?resource, error = %msg, "invalid principal id");
            Err(axum::http::StatusCode::BAD_REQUEST)
        }
        Err(PermissionError::InvalidOrgId(id)) => {
            tracing::error!(?resource, org_id = id, "invalid org id");
            Err(axum::http::StatusCode::BAD_REQUEST)
        }
        Err(PermissionError::InsufficientPrivileges) => {
            tracing::warn!(?resource, "insufficient privileges");
            Err(axum::http::StatusCode::FORBIDDEN)
        }
        Err(PermissionError::ValidationFailed) => {
            tracing::error!(?resource, "permission validation failed");
            Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
        }
        Err(PermissionError::Storage(err)) => {
            tracing::error!(?resource, error = ?err, "permission check failed");
            Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

fn resource_name(r: ResourceKind) -> &'static str {
    match r {
        ResourceKind::Collections => "collections",
        ResourceKind::Tables => "tables",
        ResourceKind::Indexes => "indexes",
        ResourceKind::Jobs => "jobs",
        ResourceKind::Files => "files",
        ResourceKind::Permissions => "permissions",
    }
}

impl axum::extract::FromRef<AppState> for AuthConfig {
    fn from_ref(s: &AppState) -> Self {
        s.auth.clone()
    }
}

impl axum::extract::FromRef<AppState> for AuthState {
    fn from_ref(s: &AppState) -> Self {
        AuthState {
            config: s.auth.clone(),
            store: s.store,
        }
    }
}

// WebRTC Signaling Handler
async fn rtc_offer_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(req): Json<RtcOfferRequest>,
) -> Result<Json<RtcOfferResponse>, axum::http::StatusCode> {
    // Ensure user has permission for real-time data access
    let principals = principals_for(&auth);
    ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Jobs, // Jobs covers real-time data streaming
        Crud::READ,
    )?;

    let peer_id = format!("{}:{}", auth.org_id, auth.token_id);

    // Create WebRTC configuration with ICE servers
    let ice_servers = if req.ice_servers.is_some() {
        req.ice_servers.unwrap()
    } else {
        state.ice_servers.clone()
    };

    // Stub: ice server configuration would be implemented here
    tracing::debug!("ICE server configuration (stubbed)");

    // Create peer connection (stub)
    let peer_connection_arc = Arc::new("stub-peer-connection".to_string());

    // Set up data channels for logs, metrics, events
    let data_channels = setup_data_channels(&peer_connection_arc, &state, &auth).await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    // Set up each data channel
    if let Some(logs_channel) = data_channels.get("logs") {
        setup_logs_channel(logs_channel.clone(), &state, &auth).await;
    }
    if let Some(metrics_channel) = data_channels.get("metrics") {
        setup_metrics_channel(metrics_channel.clone(), &state, &auth).await;
    }
    if let Some(events_channel) = data_channels.get("events") {
        setup_events_channel(events_channel.clone(), &state, &auth).await;
    }

    // Stub: WebRTC offer/answer exchange would be implemented here
    tracing::debug!("WebRTC offer/answer exchange (stubbed)");

    // Create mock answer
    let answer = RTCSessionDescriptionPayload {
        r#type: "answer".to_string(),
        sdp: "stub-answer-sdp".to_string(),
    };

    // Store the peer connection for later use
    state
        .rtc_connections
        .write()
        .await
        .insert(peer_id.clone(), peer_connection_arc.clone());

    // Set up connection state monitoring
    setup_connection_monitoring(peer_connection_arc, &state, peer_id).await;

    let response = RtcOfferResponse {
        answer,
        ice_servers,
    };

    tracing::info!(
        org_id = auth.org_id,
        user = auth.token_id,
        "WebRTC peer connection established"
    );

    Ok(Json(response))
}

// Set up data channels for different types of real-time data
async fn setup_data_channels(
    peer_connection: &Arc<String>,
    _state: &AppState,
    _auth: &AuthSession,
) -> Result<HashMap<String, Arc<String>>, Box<dyn std::error::Error + Send + Sync>> {
    let mut channels = HashMap::new();

    // Logs data channel (ordered)
    let logs_channel = Arc::new("logs-channel".to_string());
    channels.insert("logs".to_string(), logs_channel.clone());

    // Metrics data channel (unordered for real-time performance)
    let metrics_channel = Arc::new("metrics-channel".to_string());
    channels.insert("metrics".to_string(), metrics_channel.clone());

    // Events data channel (ordered, reliable)
    let events_channel = Arc::new("events-channel".to_string());
    channels.insert("events".to_string(), events_channel.clone());

    Ok(channels)
}

// Set up logs data channel with job log streaming
async fn setup_logs_channel(
    channel: Arc<String>,
    state: &AppState,
    auth: &AuthSession,
) {
    let job_queue = state.job_queue.clone();
    let org_id = auth.org_id;
    let mut rx = job_queue.subscribe();

    // Stub: channel setup would be implemented here
    tracing::debug!("WebRTC logs data channel setup (stubbed)");

    let channel_for_logs = channel.clone();
    tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            // Filter events for this org and log-related topics
            if event.topic.starts_with(&format!("jobs/{}", org_id))
                && (event.topic.contains("/logs") || event.topic.contains("/state")) {

                if let Ok(_payload) = serde_json::to_vec(&event) {
                    // Stub: would send payload over channel
                    if false { // Simulated error condition
                        let e = "stub error";
                        tracing::warn!(error = %e, "Failed to send log data over WebRTC");
                        break;
                    }
                }
            }
        }
        tracing::debug!("WebRTC logs channel sender terminated");
    });
}

// Set up metrics data channel with system metrics
async fn setup_metrics_channel(
    channel: Arc<String>,
    state: &AppState,
    _auth: &AuthSession,
) {
    let ws_metrics = state.ws_metrics.clone();
    let rtc_connections = state.rtc_connections.clone();

    // Stub: channel setup would be implemented here
    tracing::debug!("WebRTC metrics data channel setup (stubbed)");

    let channel_for_metrics = channel.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));

        loop {
            interval.tick().await;

            let (ws_active, ws_dropped) = ws_metrics.snapshot();
            let rtc_active = rtc_connections.read().await.len();

            let metrics = serde_json::json!({
                "timestamp": chrono::Utc::now(),
                "websocket": {
                    "active_connections": ws_active,
                    "dropped_total": ws_dropped
                },
                "webrtc": {
                    "active_connections": rtc_active
                }
            });

            if let Ok(_payload) = serde_json::to_vec(&metrics) {
                // Stub: would send payload over channel
                if false { // Simulated error condition
                    let e = "stub error";
                    tracing::warn!(error = %e, "Failed to send metrics over WebRTC");
                    break;
                }
            }
        }
    });
}

// Set up events data channel with general system events
async fn setup_events_channel(
    channel: Arc<String>,
    state: &AppState,
    _auth: &AuthSession,
) {
    let job_queue = state.job_queue.clone();
    let org_id = _auth.org_id;
    let mut rx = job_queue.subscribe();

    // Stub: channel setup would be implemented here
    tracing::debug!("WebRTC events data channel setup (stubbed)");

    let channel_for_events = channel.clone();
    tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            // Filter for general events for this org
            if event.topic.starts_with(&format!("jobs/{}", org_id))
                && !event.topic.contains("/logs") {

                if let Ok(_payload) = serde_json::to_vec(&event) {
                    // Stub: would send payload over channel
                    if false { // Simulated error condition
                        let e = "stub error";
                        tracing::warn!(error = %e, "Failed to send event over WebRTC");
                        break;
                    }
                }
            }
        }
        tracing::debug!("WebRTC events channel sender terminated");
    });
}

// Monitor connection state and clean up when disconnected
async fn setup_connection_monitoring(
    peer_connection: Arc<String>,
    state: &AppState,
    peer_id: String,
) {
    let rtc_connections = state.rtc_connections.clone();
    let peer_id_for_cleanup = peer_id.clone();

    // Stub: connection monitoring would be implemented here
    tracing::debug!(peer_id = %peer_id, "WebRTC connection monitoring setup (stubbed)");
}

async fn whoami(auth: AuthSession) -> impl IntoResponse {
    Json(serde_json::json!({
        "token_id": auth.token_id,
        "org_id": auth.org_id,
        "roles": auth.roles,
        "labels": auth.labels,
        "env_id": auth.env_id.to_string(),
        "env_slug": auth.env_slug,
        "token_kind": match auth.token_kind {
            TokenKind::Access => "access",
            TokenKind::Dev => "dev",
            TokenKind::ApiKey => "api_key",
        },
    }))
}

async fn auth_signup(
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(req): Json<SignupRequest>,
) -> impl IntoResponse {
    // Validate email format
    if let Err(e) = security::InputValidator::validate_email(&req.email) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "invalid_email",
                "message": "Please provide a valid email address"
            }))
        ).into_response();
    }

    // Sanitize and validate email
    let email = match security::InputValidator::sanitize_string(&req.email, 254) {
        Ok(email) => email.trim().to_lowercase(),
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "invalid_email",
                    "message": "Email address is invalid or too long"
                }))
            ).into_response();
        }
    };

    // Sanitize and validate password
    let password = match security::InputValidator::sanitize_string(&req.password, 128) {
        Ok(password) => password,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "invalid_password",
                    "message": "Password contains invalid characters"
                }))
            ).into_response();
        }
    };

    if email.is_empty() || password.len() < 8 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "invalid_credentials",
                "message": "email and password (min 8 chars) are required"
            }))
        ).into_response();
    }
    let service = AuthService::new(state.store, state.auth.clone());
    match service.signup_initial_admin(state.default_org_id, &email, &password) {
        Ok(user) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "user_id": user.id.to_string(),
                "org_id": user.org_id,
                "status": format!("{:?}", user.status),
            })),
        )
            .into_response(),
        Err(err) => {
            tracing::error!(error = ?err, "signup failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "failed to create user").into_response()
        }
    }
}

async fn auth_login(
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(req): Json<LoginRequest>,
) -> impl IntoResponse {
    // Validate email format
    if let Err(e) = security::InputValidator::validate_email(&req.email) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "invalid_email",
                "message": "Please provide a valid email address"
            }))
        ).into_response();
    }

    // Sanitize and validate email
    let email = match security::InputValidator::sanitize_string(&req.email, 254) {
        Ok(email) => email.trim().to_string(),
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "invalid_email",
                    "message": "Email address is invalid or too long"
                }))
            ).into_response();
        }
    };

    // Sanitize and validate password
    let password = match security::InputValidator::sanitize_string(&req.password, 128) {
        Ok(password) => password,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "invalid_password",
                    "message": "Password contains invalid characters"
                }))
            ).into_response();
        }
    };

    if email.is_empty() || password.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "invalid_credentials",
                "message": "email and password required"
            }))
        ).into_response();
    }
    let env_slug = req.environment.map(|s| s.trim().to_lowercase());
    let service = AuthService::new(state.store, state.auth.clone());
    match service.login(state.default_org_id, &email, &password, env_slug) {
        Ok(tokens) => (StatusCode::OK, Json(LoginResponse::from(tokens))).into_response(),
        Err(err) => {
            tracing::warn!(error = ?err, "login failed");
            if err.to_string().contains("invalid credentials") {
                (StatusCode::UNAUTHORIZED, "invalid credentials").into_response()
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, "login failed").into_response()
            }
        }
    }
}

async fn auth_refresh(
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(req): Json<RefreshRequest>,
) -> impl IntoResponse {
    let service = AuthService::new(state.store, state.auth.clone());
    match service.refresh(req.refresh_token.trim()) {
        Ok(tokens) => (StatusCode::OK, Json(LoginResponse::from(tokens))).into_response(),
        Err(err) => {
            tracing::warn!(error = ?err, "refresh failed");
            (StatusCode::UNAUTHORIZED, "invalid refresh token").into_response()
        }
    }
}

async fn auth_logout(
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(req): Json<LogoutRequest>,
) -> impl IntoResponse {
    let service = AuthService::new(state.store, state.auth.clone());
    match service.logout(req.refresh_token.trim()) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => {
            tracing::warn!(error = ?err, "logout failed");
            (StatusCode::UNAUTHORIZED, "invalid refresh token").into_response()
        }
    }
}

async fn auth_switch_environment(
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(req): Json<SwitchEnvironmentRequest>,
) -> impl IntoResponse {
    let service = AuthService::new(state.store, state.auth.clone());
    match service.switch_environment(req.refresh_token.trim(), req.environment.trim()) {
        Ok(tokens) => (StatusCode::OK, Json(LoginResponse::from(tokens))).into_response(),
        Err(err) => {
            tracing::warn!(error = ?err, "switch environment failed");
            (StatusCode::UNAUTHORIZED, "unable to switch environment").into_response()
        }
    }
}

async fn list_environments_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl IntoResponse {
    let repo = AuthRepository::new(state.store);
    match repo.list_environments(auth.org_id) {
        Ok(envs) => {
            let summaries: Vec<EnvironmentSummary> =
                envs.into_iter().map(EnvironmentSummary::from).collect();
            Json(summaries).into_response()
        }
        Err(err) => {
            tracing::error!(error = ?err, "list environments failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to list environments",
            )
                .into_response()
        }
    }
}

async fn healthz() -> impl IntoResponse {
    axum::http::StatusCode::OK
}

async fn get_ws_metrics(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl IntoResponse {
    let (active, dropped) = state.ws_metrics.snapshot();
    Json(serde_json::json!({
        "active_connections": active,
        "dropped_total": dropped,
    }))
    .into_response()
}

async fn get_effective_permissions(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    match state.permissions.effective_map(auth.org_id, &principals) {
        Ok(map) => {
            let mut payload = serde_json::Map::new();
            for (resource, actions) in map {
                match serde_json::to_value(actions) {
                    Ok(value) => {
                        payload.insert(resource_name(resource).to_string(), value);
                    }
                    Err(err) => {
                        tracing::error!(error = ?err, "serialize effective permissions failed");
                        return (
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            "permission encoding failed",
                        )
                            .into_response();
                    }
                }
            }
            Json(payload).into_response()
        }
        Err(err) => {
            tracing::error!(error = ?err, "load effective permissions failed");
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "permission lookup failed",
            )
                .into_response()
        }
    }
}

async fn list_permissions(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        &state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Permissions,
        Crud::READ,
    ) {
        return status.into_response();
    }
    match state.permissions.list_org(auth.org_id) {
        Ok(list) => Json(list).into_response(),
        Err(err) => {
            tracing::error!(error = ?err, "list permissions failed");
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "failed to list permissions",
            )
                .into_response()
        }
    }
}

async fn upsert_permission(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(record): Json<PermissionRecord>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        &state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Permissions,
        Crud::UPDATE,
    ) {
        return status.into_response();
    }
    if let Err(err) = state.permissions.upsert(auth.org_id, &record) {
        tracing::error!(error = ?err, "upsert permission failed");
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update permission",
        )
            .into_response();
    }
    Json(serde_json::json!({"ok": true})).into_response()
}

// Identity endpoints

async fn issue_credential_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(request): Json<CredentialRequest>,
) -> impl IntoResponse {
    // Check permissions for credential creation
    if let Err(err) = ensure_permission(
        &state.permissions,
        auth.org_id,
        Principal::User(auth.token_id),
        Crud::CREATE,
        ResourceKind::Collections, // Using Collections as proxy for credentials
    ) {
        return match err {
            PermissionError::Forbidden { .. } => (
                StatusCode::FORBIDDEN,
                "Insufficient permissions to issue credentials",
            ).into_response(),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Permission check failed",
            ).into_response(),
        };
    }

    let org_id = uuid::Uuid::from_u64_pair(auth.org_id, 0);
    let user_id = uuid::Uuid::from_u64_pair(auth.token_id, 0);

    match state.identity.issue_credential(org_id, user_id, &request).await {
        Ok(credential) => Json(credential).into_response(),
        Err(e) => {
            tracing::error!("Failed to issue credential: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to issue credential").into_response()
        }
    }
}

async fn list_credentials_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl IntoResponse {
    // Check permissions for credential reading
    if let Err(err) = ensure_permission(
        &state.permissions,
        auth.org_id,
        Principal::User(auth.token_id),
        Crud::READ,
        ResourceKind::Collections,
    ) {
        return match err {
            PermissionError::Forbidden { .. } => (
                StatusCode::FORBIDDEN,
                "Insufficient permissions to list credentials",
            ).into_response(),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Permission check failed",
            ).into_response(),
        };
    }

    let org_id = uuid::Uuid::from_u64_pair(auth.org_id, 0);
    let user_id = uuid::Uuid::from_u64_pair(auth.token_id, 0);

    match state.identity.list_credentials(org_id, user_id).await {
        Ok(credentials) => Json(credentials).into_response(),
        Err(e) => {
            tracing::error!("Failed to list credentials: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to list credentials").into_response()
        }
    }
}

async fn verify_credential_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(request): Json<VerificationRequest>,
) -> impl IntoResponse {
    // Check permissions for credential verification
    if let Err(err) = ensure_permission(
        &state.permissions,
        auth.org_id,
        Principal::User(auth.token_id),
        Crud::READ,
        ResourceKind::Collections,
    ) {
        return match err {
            PermissionError::Forbidden { .. } => (
                StatusCode::FORBIDDEN,
                "Insufficient permissions to verify credentials",
            ).into_response(),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Permission check failed",
            ).into_response(),
        };
    }

    if let Some(credential_id) = request.credential_id {
        let org_id = uuid::Uuid::from_u64_pair(auth.org_id, 0);

        match state.identity.verify_credential(org_id, credential_id).await {
            Ok(result) => Json(result).into_response(),
            Err(e) => {
                tracing::error!("Failed to verify credential: {:?}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, "Failed to verify credential").into_response()
            }
        }
    } else {
        (StatusCode::BAD_REQUEST, "credential_id is required").into_response()
    }
}

async fn revoke_credential_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    AxumPath(credential_id_str): AxumPath<String>,
) -> impl IntoResponse {
    // Check permissions for credential deletion
    if let Err(err) = ensure_permission(
        &state.permissions,
        auth.org_id,
        Principal::User(auth.token_id),
        Crud::DELETE,
        ResourceKind::Collections,
    ) {
        return match err {
            PermissionError::Forbidden { .. } => (
                StatusCode::FORBIDDEN,
                "Insufficient permissions to revoke credentials",
            ).into_response(),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Permission check failed",
            ).into_response(),
        };
    }

    let credential_id = match ulid::Ulid::from_string(&credential_id_str) {
        Ok(id) => id,
        Err(_) => {
            return (StatusCode::BAD_REQUEST, "Invalid credential ID format").into_response();
        }
    };

    let org_id = uuid::Uuid::from_u64_pair(auth.org_id, 0);

    match state.identity.revoke_credential(org_id, credential_id, Some("Revoked via API".to_string())).await {
        Ok(result) => Json(result).into_response(),
        Err(e) => {
            tracing::error!("Failed to revoke credential: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to revoke credential").into_response()
        }
    }
}

pub fn create_app(state: AppState) -> Router {
    Router::new()
        .nest_service(
            "/console",
            ServeDir::new("./web/zconsole/dist").append_index_html_on_directories(true),
        )
        .route("/v1/auth/signup", post(auth_signup))
        .route("/v1/auth/login", post(auth_login))
        .route("/v1/auth/refresh", post(auth_refresh))
        .route("/v1/auth/logout", post(auth_logout))
        .route("/v1/auth/switch", post(auth_switch_environment))
        .route("/v1/environments", get(list_environments_handler))
        .route("/healthz", get(healthz))
        .route("/v1/whoami", get(whoami))
        .route("/v1/metrics/ws", get(get_ws_metrics))
        .route("/v1/mesh/status", get(mesh_status_handler))
        .route("/v1/mesh/peers", get(mesh_peers_handler))
        .route("/v1/mesh/peers/:node_id/connect", post(mesh_connect_peer_handler))
        .route("/v1/mesh/peers/:node_id/disconnect", post(mesh_disconnect_peer_handler))
        .route("/v1/mesh/economics", get(mesh_economics_handler))
        .route("/v1/mesh/economics/settle", post(mesh_settle_handler))
        .route("/v1/files", post(upload_file))
        .route("/v1/files/:sha", get(get_file))
        .route(
            "/v1/collections",
            get(collection_list).post(create_collection),
        )
        .route(
            "/v1/collections/:name/projection",
            get(get_projection).put(put_projection),
        )
        .route("/v1/collections/:name/ingest", post(ingest_collection))
        .route("/v1/knn", post(knn_handler))
        .route("/v1/sql", post(sql_handler))
        // Transaction endpoints
        .route("/v1/transactions/begin", post(transaction_begin_handler))
        .route("/v1/transactions/:id/execute", post(transaction_execute_handler))
        .route("/v1/transactions/:id/commit", post(transaction_commit_handler))
        .route("/v1/transactions/:id/rollback", post(transaction_rollback_handler))
        .route("/v1/transactions/:id/status", get(transaction_status_handler))
        // Distributed transaction endpoints
        .route("/v1/distributed-transactions/begin", post(distributed_transaction_begin_handler))
        .route("/v1/distributed-transactions/:id/status", get(distributed_transaction_status_handler))
        .route("/v1/distributed-transactions/:id/abort", post(distributed_transaction_abort_handler))
        .route("/v1/distributed-transactions", get(list_distributed_transactions_handler))
        .route("/v1/distributed-transactions/metrics", get(distributed_transaction_metrics_handler))
        // Saga endpoints
        .route("/v1/sagas/start", post(saga_start_handler))
        .route("/v1/sagas/:id/status", get(saga_status_handler))
        .route("/v1/sagas/:id/abort", post(saga_abort_handler))
        .route("/v1/sagas", get(list_sagas_handler))
        .route("/v1/indexes/hnsw/build", post(hnsw_build))
        .route("/v1/indexes/hnsw/search", post(hnsw_search))
        .route(
            "/v1/indexes/hnsw/:table/:column/stats",
            get(index_stats_handler),
        )
        .route("/v1/indexes/hnsw", get(hnsw_list))
        .route("/v1/tables", get(list_tables))
        .route("/v1/tables/:table/sample", get(table_sample))
        .route("/v1/jobs", post(job_submit).get(job_list))
        .route("/v1/jobs/:id", get(job_get).delete(job_cancel))
        .route("/v1/jobs/:id/logs", get(get_job_logs))
        .route("/v1/jobs/:id/artifacts", get(list_job_artifacts))
        .route("/v1/jobs/:id/artifacts/*name", get(download_job_artifact))
        .route(
            "/v1/permissions",
            get(list_permissions).put(upsert_permission),
        )
        .route("/v1/permissions/effective", get(get_effective_permissions))
        // Identity endpoints
        .route("/v1/identity/credentials", post(issue_credential_handler).get(list_credentials_handler))
        .route("/v1/identity/verify", post(verify_credential_handler))
        .route("/v1/identity/credentials/:id", delete(revoke_credential_handler))
        // Event system endpoints
        .route("/v1/events", post(events::publish_event).get(events::query_events))
        .route("/v1/events/replay", post(events::replay_events))
        .route("/v1/events/stats", get(events::get_event_stats))
        .route("/v1/webhooks", post(events::create_webhook).get(events::list_webhooks))
        .route("/v1/webhooks/:id", get(events::get_webhook).delete(events::delete_webhook))
        .route("/v1/triggers", post(events::create_trigger).get(events::list_triggers))
        .route("/v1/tasks", post(events::create_task).get(events::list_tasks))
        .route("/ws", get(ws_upgrade_handler))
        .route("/ws/exactly-once", get(websocket_exactly_once::enhanced_websocket_handler))
        // Enhanced WebRTC endpoints
        .route("/v2/rtc/rooms", post(create_room_handler).get(list_rooms_handler))
        .route("/v2/rtc/rooms/:room_id", get(get_room_handler).delete(delete_room_handler))
        .route("/v2/rtc/rooms/:room_id/join", post(join_room_handler))
        .route("/v2/rtc/rooms/:room_id/leave", post(leave_room_handler))
        .route("/v2/rtc/rooms/:room_id/participants", get(list_participants_handler))
        .route("/v2/rtc/rooms/:room_id/participants/:session_id",
                get(get_participant_handler).put(update_participant_handler))
        .route("/v2/rtc/sessions", get(list_sessions_handler))
        .route("/v2/rtc/sessions/:session_id", get(get_session_handler).put(update_session_handler))
        .route("/v2/rtc/sessions/:session_id/transactions", get(get_session_transaction_status_handler))
        .route("/v2/rtc/sessions/:session_id/transactions/:tx_id/rollback", post(force_rollback_transaction_handler))
        .route("/v2/rtc/transactions", get(list_org_transactions_handler))
        .route("/v2/rtc/transactions/stats", get(get_transaction_stats_handler))
        .route("/v2/rtc/subscriptions", post(create_subscription_handler).get(list_subscriptions_handler))
        .route("/v2/rtc/subscriptions/:subscription_id",
                get(get_subscription_handler).put(update_subscription_handler).delete(delete_subscription_handler))
        .route("/v2/rtc/triggers", post(create_trigger_handler).get(list_triggers_handler))
        .route("/v2/rtc/triggers/:trigger_id",
                get(get_trigger_handler).put(update_trigger_handler).delete(delete_trigger_handler))
        .route("/v2/rtc/triggers/:trigger_id/execute", post(execute_trigger_handler))
        .route("/v2/rtc/triggers/:trigger_id/history", get(get_trigger_history_handler))
        // Legacy WebRTC endpoint (backward compatibility)
        .route("/v2/rtc/offer", post(rtc_offer_handler))
        .with_state(state)
        .layer(security::create_security_middleware())
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            security::apply_rate_limit
        ))
}

pub async fn run(addr: SocketAddr) {
    let dev_org_id = std::env::var("ZAUTH_DEV_ORG_ID")
        .ok()
        .and_then(|s| s.parse::<u64>().ok());
    let dev_secret = std::env::var("ZAUTH_DEV_SECRET").ok();
    let jwt_secret = std::env::var("ZJWT_SECRET").unwrap_or_else(|_| {
        dev_secret
            .clone()
            .unwrap_or_else(|| "dev-jwt-secret".to_string())
    });
    let refresh_secret =
        std::env::var("ZJWT_REFRESH_SECRET").unwrap_or_else(|_| format!("{jwt_secret}-refresh"));
    let access_ttl_secs = std::env::var("ZJWT_ACCESS_TTL_SECS")
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(900);
    let refresh_ttl_secs = std::env::var("ZJWT_REFRESH_TTL_SECS")
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(7 * 24 * 3600);
    let auth = AuthConfig::new(
        dev_org_id,
        dev_secret.clone(),
        jwt_secret,
        refresh_secret,
        access_ttl_secs,
        refresh_ttl_secs,
    );
    if auth.dev_secret().is_some() {
        match zauth::issue_dev_token(&auth) {
            Ok(tok) => {
                tracing::info!("Dev token: {}", tok);
            }
            Err(err) => {
                tracing::warn!("Dev token issuance failed: {}", err);
            }
        }
    }

    // Open store at ./data/zdb.redb by default
    let data_path = std::env::var("ZDB_DATA").unwrap_or_else(|_| "./data".to_string());
    std::fs::create_dir_all(&data_path).ok();
    let data_dir = std::path::Path::new(&data_path);
    let store = Store::open(data_dir.join("zdb.redb")).expect("open store");
    // Leak store to extend lifetime for Catalog<'static>
    let store_static: &'static Store = Box::leak(Box::new(store));
    let catalog = Catalog::new(store_static);
    let files_root = data_dir.join("files");
    std::fs::create_dir_all(&files_root).ok();
    // Create job queue storage under data dir
    let jobs_db = data_dir.join("jobs.redb");
    let storage = zjob::Storage::new(&jobs_db).expect("open jobs store");
    let artifacts_root = data_dir.join("artifacts");
    std::fs::create_dir_all(&artifacts_root).ok();
    let (job_queue, _job_events) = zjob::JobQueue::new(storage, &artifacts_root);
    let job_queue_arc = Arc::new(job_queue);
    let ws_metrics = Arc::new(WsMetrics::default());
    let permissions = PermissionService::new(store_static);
    let permissions_arc = Arc::new(permissions);
    let default_org_id = std::env::var("ZDEFAULT_ORG_ID")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(1);

    // Initialize WebRTC with enhanced features (stub)
    let webrtc_config = WebRTCConfig::default();
    // TODO: Create proper media engine when WebRTC is fully integrated
    // let media_config = MediaEngineConfig::default();
    // let media_engine = create_media_engine(&media_config)
    //     .expect("Failed to create media engine");
    // let setting_engine = create_setting_engine();

    // TODO: Create enhanced WebRTC API when fully integrated
    // let webrtc_api_string = APIBuilder::new()
    //     .with_media_engine(media_engine)
    //     .with_setting_engine(setting_engine)
    //     .build();

    // Initialize real WebRTC API with ICE servers
    let ice_servers_rtc = webrtc::API::parse_ice_servers_from_env();
    let webrtc_api = webrtc::API::new(ice_servers_rtc).await
        .expect("Failed to create WebRTC API");
    let webrtc_api_arc = Arc::new(webrtc_api);

    // Initialize function trigger manager (needs to be before WebRTCState)
    // let function_trigger_manager = Arc::new(FunctionTriggerManager::new(function_executor.clone()));
    let function_trigger_manager = Arc::new(FunctionTriggerManager::new());

    // Create Arc<Store> from static reference
    // SAFETY: store_static is a leaked Box, so it lives forever
    // We increment ref count manually and never decrement to prevent drop
    let store_arc = unsafe {
        let arc = Arc::from_raw(store_static as *const Store);
        let cloned = Arc::clone(&arc);
        std::mem::forget(arc); // Prevent drop to keep static reference valid
        cloned
    };

    let webrtc_state = WebRTCState::new_simple(
        webrtc_api_arc,
        webrtc_config,
        function_trigger_manager.clone(),
        store_arc,
        store_static,
    ).await;

    // Initialize subscription manager
    let subscription_manager = Arc::new(SubscriptionManager::new(permissions_arc.clone()));

    // Default ICE servers (STUN)
    let default_ice_servers = vec![
        IceServerConfig {
            urls: vec!["stun:stun.l.google.com:19302".to_string()],
            username: None,
            credential: None,
        },
        IceServerConfig {
            urls: vec!["stun:stun1.l.google.com:19302".to_string()],
            username: None,
            credential: None,
        },
    ];

    // Allow configuration via environment variables
    let ice_servers = if let Ok(ice_config) = std::env::var("ZWEBRTC_ICE_SERVERS") {
        match serde_json::from_str::<Vec<IceServerConfig>>(&ice_config) {
            Ok(servers) => {
                tracing::info!(servers_count = servers.len(), "Loaded ICE servers from environment");
                servers
            },
            Err(e) => {
                tracing::warn!(error = %e, "Failed to parse ICE servers config, using defaults");
                default_ice_servers
            }
        }
    } else {
        tracing::info!("Using default ICE servers (Google STUN)");
        default_ice_servers
    };

    let _rtc_connections = Arc::new(TokioRwLock::new(HashMap::<String, Arc<String>>::new()));

    // Initialize database tables (redb creates tables on first open)
    {
        use zcore_storage::{
            COL_CATALOG, COL_ROWS, COL_SEQ, COL_INDEX, COL_PERMISSIONS,
            COL_ENVIRONMENTS, COL_USERS, COL_USER_EMAIL, COL_SESSIONS, COL_API_KEYS
        };
        let _db = store_static.begin_read().expect("Failed to begin read transaction");
        let mut w = store_static.begin_write().expect("failed to begin write transaction");

        // Core storage tables
        let _ = w.open_table(store_static, COL_CATALOG).expect("failed to open catalog table");
        let _ = w.open_table(store_static, COL_ROWS).expect("failed to open rows table");
        let _ = w.open_table(store_static, COL_SEQ).expect("failed to open seq table");
        let _ = w.open_table(store_static, COL_INDEX).expect("failed to open index table");
        let _ = w.open_table(store_static, COL_PERMISSIONS).expect("failed to open permissions table");

        // Auth tables
        let _ = w.open_table(store_static, COL_ENVIRONMENTS).expect("failed to open environments table");
        let _ = w.open_table(store_static, COL_USERS).expect("failed to open users table");
        let _ = w.open_table(store_static, COL_USER_EMAIL).expect("failed to open user email table");
        let _ = w.open_table(store_static, COL_SESSIONS).expect("failed to open sessions table");
        let _ = w.open_table(store_static, COL_API_KEYS).expect("failed to open api keys table");

        w.commit(store_static).expect("failed to commit table initialization");
    }

    let auth_bootstrap = AuthService::new(store_static, auth.clone());
    auth_bootstrap
        .repo()
        .ensure_default_environments(default_org_id)
        .expect("failed to create default environments");
    // Start a simple local worker scheduler for default org in dev mode
    if std::env::var("ZWORKER_MODE").unwrap_or_else(|_| "auto".into()) != "docker" {
        let org = default_org_id.to_string();
        let q = job_queue_arc.clone();
        let worker = std::sync::Arc::new(LocalWorker::new("local-worker".into()));
        let sched = zjob::FifoScheduler::new(q.clone(), worker, org)
            .with_poll_interval(std::time::Duration::from_secs(2));
        tokio::spawn(async move {
            let _ = sched.run().await;
        });
    }

    // Temporarily commented out to isolate zserver compilation issues
    // Initialize event system - fully disabled until dependencies are available
    let _event_system: Option<()> = None;
    tracing::info!("Event system disabled (compilation stub)");

    // Initialize stub WebRTC fields
    let webrtc_api_legacy = Arc::new("stub-api".to_string());
    let rtc_connections_legacy = Arc::new(TokioRwLock::new(HashMap::new()));

    // Initialize mesh network (optional)
    let mesh_network = if std::env::var("ZMESH_ENABLED").as_deref() == Ok("true") {
        tracing::info!("Initializing mesh network...");
        match MeshConfig::development() {
            config => {
                match ZMeshNetwork::new(config).await {
                    Ok(mesh) => {
                        if let Err(e) = mesh.start().await {
                            tracing::error!("Failed to start mesh network: {}", e);
                            None
                        } else {
                            tracing::info!("Mesh network started successfully");
                            Some(Arc::new(mesh))
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to create mesh network: {}", e);
                        None
                    }
                }
            }
        }
    } else {
        tracing::info!("Mesh network disabled (set ZMESH_ENABLED=true to enable)");
        None
    };

    // Initialize identity service (simplified for observability integration)
    let identity_config = IdentityConfig::default();
    let identity_store_arc = unsafe {
        let arc = Arc::from_raw(store_static as *const Store);
        let cloned = Arc::clone(&arc);
        std::mem::forget(arc);
        cloned
    };
    let identity = Arc::new(
        IdentityService::new(identity_store_arc, identity_config)
            .expect("Failed to initialize IdentityService")
    );

    // Initialize transaction manager
    let transaction_manager = Arc::new(TransactionManager::new(store_static));

    // Start transaction cleanup task
    TransactionManager::start_cleanup_task(transaction_manager.clone());
    tracing::info!("Transaction manager initialized with cleanup task");

    // Initialize distributed transaction coordinator (optional - enabled via env var)
    let distributed_coordinator = if std::env::var("ENABLE_DISTRIBUTED_TRANSACTIONS")
        .unwrap_or_default()
        .parse::<bool>()
        .unwrap_or(false)
    {
        let config = DistributedTransactionConfig::default();
        match GlobalTransactionCoordinator::new("coordinator-1".to_string(), config).await {
            Ok(coordinator) => {
                tracing::info!("Distributed transaction coordinator initialized");
                Some(Arc::new(coordinator))
            }
            Err(e) => {
                tracing::error!("Failed to initialize distributed transaction coordinator: {}", e);
                None
            }
        }
    } else {
        None
    };

    // Initialize saga coordinator (optional - enabled via env var)
    let saga_coordinator = if std::env::var("ENABLE_SAGAS")
        .unwrap_or_default()
        .parse::<bool>()
        .unwrap_or(false)
    {
        // Create a messaging system for the saga coordinator
        match ztransaction::distributed::CrossProtocolMessaging::new().await {
            Ok(messaging) => {
                match SagaCoordinator::new("saga-coordinator-1".to_string(), Arc::new(messaging)).await {
                    Ok(coordinator) => {
                        tracing::info!("Saga coordinator initialized");
                        Some(Arc::new(coordinator))
                    }
                    Err(e) => {
                        tracing::error!("Failed to initialize saga coordinator: {}", e);
                        None
                    }
                }
            }
            Err(e) => {
                tracing::error!("Failed to initialize messaging for saga coordinator: {}", e);
                None
            }
        }
    } else {
        None
    };

    // Initialize observability stack
    let observability_config = ObservabilityConfig {
        metrics_enabled: std::env::var("ZOBS_METRICS_ENABLED")
            .map(|v| v.parse().unwrap_or(true))
            .unwrap_or(true),
        metrics_addr: std::env::var("ZOBS_METRICS_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:9090".to_string())
            .parse()
            .expect("Invalid metrics address"),
        health_enabled: std::env::var("ZOBS_HEALTH_ENABLED")
            .map(|v| v.parse().unwrap_or(true))
            .unwrap_or(true),
        health_addr: std::env::var("ZOBS_HEALTH_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
            .parse()
            .expect("Invalid health check address"),
        structured_logging: std::env::var("ZOBS_STRUCTURED_LOGGING")
            .map(|v| v.parse().unwrap_or(false))
            .unwrap_or(false),
        service_name: std::env::var("ZOBS_SERVICE_NAME")
            .unwrap_or_else(|_| "zrustdb".to_string()),
        environment: std::env::var("ZOBS_ENVIRONMENT")
            .unwrap_or_else(|_| "development".to_string()),
        instance_id: std::env::var("ZOBS_INSTANCE_ID")
            .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string()),
    };

    let observability = Arc::new(
        ObservabilityManager::new(observability_config.clone())
            .expect("Failed to create observability manager")
    );

    // Initialize observability
    if let Err(e) = observability.initialize().await {
        tracing::error!("Failed to initialize observability: {}", e);
    } else {
        tracing::info!("Observability stack initialized");
    }

    // Start observability servers
    if let Err(e) = observability.start_servers().await {
        tracing::error!("Failed to start observability servers: {}", e);
    } else {
        tracing::info!(
            metrics_addr = %observability_config.metrics_addr,
            health_addr = %observability_config.health_addr,
            "Observability servers started"
        );
    }

    // Setup health checks
    let health_manager = observability.health_manager();

    // Register database health check
    health_manager.register_check(Box::new(
        DatabaseHealthCheck::new("zrustdb-database".to_string())
    )).await;

    // Register vector index health check
    health_manager.register_check(Box::new(
        VectorIndexHealthCheck::new("hnsw-indexes".to_string())
    )).await;

    // Register WebSocket health check
    health_manager.register_check(Box::new(
        WebSocketHealthCheck::new("websocket-server".to_string())
    )).await;

    // Register system resource health check
    health_manager.register_check(Box::new(
        SystemResourceHealthCheck::new(
            "system-resources".to_string(),
            80.0, // 80% CPU threshold
            85.0, // 85% memory threshold
        )
    )).await;

    // Create metrics collector
    let zdb_metrics = Arc::new(
        zobservability::metrics::ZdbMetrics::new()
            .expect("Failed to create ZDB metrics")
    );

    // Register metrics with observability registry
    if let Err(e) = zdb_metrics.register(observability.metrics_registry().as_ref()) {
        tracing::error!("Failed to register ZDB metrics: {}", e);
    }

    let metrics = Arc::new(MetricsAggregator::new(zdb_metrics.clone()));

    // Start system metrics collection
    if let Err(e) = metrics.start_system_collection().await {
        tracing::error!("Failed to start system metrics collection: {}", e);
    }

    let state = AppState {
        auth,
        store: store_static,
        catalog,
        files_root,
        job_queue: job_queue_arc,
        ws_metrics,
        permissions: (*permissions_arc).clone(),
        default_org_id,
        // Transaction management
        transaction_manager,
        // Distributed transaction coordination
        distributed_coordinator,
        saga_coordinator,
        // Identity service
        identity,
        // Mesh networking
        mesh_network,
        // Legacy WebRTC fields
        webrtc_api: webrtc_api_legacy,
        ice_servers,
        rtc_connections: rtc_connections_legacy,
        // Enhanced WebRTC features
        webrtc_state,
        subscription_manager,
        function_trigger_manager,
        // Observability
        observability,
        metrics,
        // Temporarily commented out to isolate zserver compilation issues
        // function_executor,
        // event_system,
    };

    let app = create_app(state);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind failed");
    axum::serve(listener, app).await.expect("server error");
}

pub fn init_tracing() {
    let _ = fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("info".parse().unwrap_or_default()),
        )
        .try_init();
}

#[derive(Debug, Deserialize)]
pub struct SqlRequest {
    pub sql: String,
}

#[derive(Debug, Serialize)]
pub struct SqlResponse {
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
}

fn sql_required_permission(sql: &str) -> Crud {
    let first = sql
        .trim_start()
        .split_whitespace()
        .next()
        .map(|w| w.to_ascii_uppercase())
        .unwrap_or_default();
    match first.as_str() {
        "SELECT" | "EXPLAIN" | "SHOW" | "DESCRIBE" => Crud::READ,
        "INSERT" => Crud::CREATE,
        "CREATE" => Crud::CREATE,
        "UPDATE" | "ALTER" | "MERGE" => Crud::UPDATE,
        "DELETE" | "DROP" | "TRUNCATE" => Crud::DELETE,
        _ => Crud::UPDATE,
    }
}

async fn sql_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(req): Json<SqlRequest>,
) -> Result<Json<SqlResponse>, (axum::http::StatusCode, String)> {
    // Validate SQL query for injection attempts
    if let Err(e) = security::InputValidator::validate_sql_query(&req.sql) {
        tracing::warn!(sql = %req.sql, org_id = auth.org_id, "SQL injection attempt detected");
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid SQL query: potential security risk detected".to_string()
        ));
    }

    let principals = principals_for(&auth);
    let required = sql_required_permission(&req.sql);
    ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Tables,
        required,
    ).map_err(|status| (status, "Permission denied".to_string()))?;

    tracing::debug!(sql = %req.sql, org_id = auth.org_id, "Executing SQL query");

    match exec_sql_store(&req.sql, auth.org_id, &state.store, &state.catalog) {
        Ok(ExecResult::Rows(rows)) => {
            let rows: Vec<serde_json::Map<String, serde_json::Value>> = rows.into_iter().map(|m| m.into_iter().collect()).collect();
            tracing::debug!(row_count = rows.len(), "SQL query completed successfully");
            Ok(Json(SqlResponse { rows }))
        }
        Ok(ExecResult::Affected(count)) => {
            tracing::debug!(affected_rows = count, "SQL statement affected rows");
            Ok(Json(SqlResponse { rows: Vec::new() }))
        }
        Ok(ExecResult::None) => {
            tracing::debug!("SQL statement completed with no result");
            Ok(Json(SqlResponse { rows: Vec::new() }))
        }
        Err(err) => {
            tracing::error!(
                error = %err,
                sql = %req.sql,
                org_id = auth.org_id,
                "SQL execution failed"
            );
            Err((axum::http::StatusCode::BAD_REQUEST, err.to_string()))
        }
    }
}

// Transaction handler functions
async fn transaction_begin_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(req): Json<BeginTransactionRequest>,
) -> Result<Json<BeginTransactionResponse>, (axum::http::StatusCode, String)> {
    let principals = principals_for(&auth);
    ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Tables,
        Crud::UPDATE,
    ).map_err(|status| (status, "Permission denied for transactions".to_string()))?;

    let isolation_level = req.isolation_level.unwrap_or_default();
    let timeout_seconds = req.timeout_seconds.unwrap_or(300); // Default 5 minutes

    // Validate timeout limits
    if timeout_seconds > 3600 {
        return Err((axum::http::StatusCode::BAD_REQUEST, "Transaction timeout cannot exceed 1 hour".to_string()));
    }

    match state.transaction_manager.begin_transaction(&auth, isolation_level, timeout_seconds).await {
        Ok(tx_context) => {
            tracing::info!(
                transaction_id = %tx_context.id,
                org_id = auth.org_id,
                isolation_level = ?isolation_level,
                timeout_seconds = timeout_seconds,
                "Transaction started successfully"
            );

            Ok(Json(BeginTransactionResponse {
                transaction_id: tx_context.id,
                isolation_level: tx_context.isolation_level,
                expires_at: tx_context.expires_at,
            }))
        },
        Err(err) => {
            tracing::error!(
                error = %err,
                org_id = auth.org_id,
                "Failed to begin transaction"
            );
            Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, err))
        }
    }
}

async fn transaction_execute_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    AxumPath(transaction_id): AxumPath<String>,
    Json(req): Json<ExecuteTransactionRequest>,
) -> Result<Json<ExecuteTransactionResponse>, (axum::http::StatusCode, String)> {
    // Get and validate transaction
    let tx_context = match state.transaction_manager.get_transaction(&transaction_id, &auth).await {
        Ok(tx) => tx,
        Err(err) => {
            tracing::error!(
                error = %err,
                transaction_id = %transaction_id,
                org_id = auth.org_id,
                "Transaction not found or access denied"
            );
            return Err((axum::http::StatusCode::NOT_FOUND, err));
        }
    };

    // Permission check based on SQL operation
    let principals = principals_for(&auth);
    let required = sql_required_permission(&req.sql);
    ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Tables,
        required,
    ).map_err(|status| (status, "Permission denied for SQL operation".to_string()))?;

    tracing::debug!(
        transaction_id = %transaction_id,
        sql = %req.sql,
        org_id = auth.org_id,
        "Executing SQL in transaction"
    );

    // Execute SQL within the transaction context
    if let Some(write_tx_arc) = &tx_context.write_tx {
        // For now, we'll use the existing exec_sql_store but with transaction context
        // In a full implementation, we'd modify exec_sql_store to accept a transaction parameter
        match exec_sql_store(&req.sql, auth.org_id, &state.store, &state.catalog) {
            Ok(ExecResult::Rows(rows)) => {
                let rows: Vec<serde_json::Map<String, serde_json::Value>> = rows.into_iter().map(|m| m.into_iter().collect()).collect();

                // Update transaction metrics
                if let Err(err) = state.transaction_manager.update_transaction(&transaction_id, &auth, |tx| {
                    tx.increment_operations();
                }).await {
                    tracing::warn!(
                        error = %err,
                        transaction_id = %transaction_id,
                        "Failed to update transaction metrics"
                    );
                }

                tracing::debug!(
                    transaction_id = %transaction_id,
                    row_count = rows.len(),
                    "SQL query completed successfully in transaction"
                );

                Ok(Json(ExecuteTransactionResponse {
                    rows,
                    affected_rows: None,
                }))
            }
            Ok(ExecResult::Affected(count)) => {
                // Update transaction metrics
                if let Err(err) = state.transaction_manager.update_transaction(&transaction_id, &auth, |tx| {
                    tx.increment_operations();
                }).await {
                    tracing::warn!(
                        error = %err,
                        transaction_id = %transaction_id,
                        "Failed to update transaction metrics"
                    );
                }

                tracing::debug!(
                    transaction_id = %transaction_id,
                    affected_rows = count,
                    "SQL statement affected rows in transaction"
                );

                Ok(Json(ExecuteTransactionResponse {
                    rows: Vec::new(),
                    affected_rows: Some(count),
                }))
            }
            Ok(ExecResult::None) => {
                // Update transaction metrics
                if let Err(err) = state.transaction_manager.update_transaction(&transaction_id, &auth, |tx| {
                    tx.increment_operations();
                }).await {
                    tracing::warn!(
                        error = %err,
                        transaction_id = %transaction_id,
                        "Failed to update transaction metrics"
                    );
                }

                tracing::debug!(
                    transaction_id = %transaction_id,
                    "SQL statement completed with no result in transaction"
                );

                Ok(Json(ExecuteTransactionResponse {
                    rows: Vec::new(),
                    affected_rows: None,
                }))
            }
            Err(err) => {
                tracing::error!(
                    error = %err,
                    transaction_id = %transaction_id,
                    sql = %req.sql,
                    org_id = auth.org_id,
                    "SQL execution failed in transaction"
                );
                Err((axum::http::StatusCode::BAD_REQUEST, err.to_string()))
            }
        }
    } else {
        Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, "Transaction has no active database transaction".to_string()))
    }
}

async fn transaction_commit_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    AxumPath(transaction_id): AxumPath<String>,
) -> Result<Json<TransactionCommitResponse>, (axum::http::StatusCode, String)> {
    match state.transaction_manager.commit_transaction(&transaction_id, &auth).await {
        Ok((operations_count, committed_at)) => {
            tracing::info!(
                transaction_id = %transaction_id,
                org_id = auth.org_id,
                operations_count = operations_count,
                "Transaction committed successfully"
            );

            Ok(Json(TransactionCommitResponse {
                transaction_id,
                committed_at,
                operations_count,
            }))
        },
        Err(err) => {
            tracing::error!(
                error = %err,
                transaction_id = %transaction_id,
                org_id = auth.org_id,
                "Failed to commit transaction"
            );

            let status_code = if err.contains("not found") || err.contains("access denied") {
                axum::http::StatusCode::NOT_FOUND
            } else if err.contains("expired") {
                axum::http::StatusCode::GONE
            } else if err.contains("not active") {
                axum::http::StatusCode::CONFLICT
            } else {
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            };

            Err((status_code, err))
        }
    }
}

async fn transaction_rollback_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    AxumPath(transaction_id): AxumPath<String>,
) -> Result<Json<TransactionRollbackResponse>, (axum::http::StatusCode, String)> {
    match state.transaction_manager.rollback_transaction(&transaction_id, &auth).await {
        Ok(rolled_back_at) => {
            tracing::info!(
                transaction_id = %transaction_id,
                org_id = auth.org_id,
                "Transaction rolled back successfully"
            );

            Ok(Json(TransactionRollbackResponse {
                transaction_id,
                rolled_back_at,
            }))
        },
        Err(err) => {
            tracing::error!(
                error = %err,
                transaction_id = %transaction_id,
                org_id = auth.org_id,
                "Failed to rollback transaction"
            );

            let status_code = if err.contains("not found") || err.contains("access denied") {
                axum::http::StatusCode::NOT_FOUND
            } else if err.contains("already rolled back") || err.contains("Cannot rollback committed") {
                axum::http::StatusCode::CONFLICT
            } else {
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            };

            Err((status_code, err))
        }
    }
}

async fn transaction_status_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    AxumPath(transaction_id): AxumPath<String>,
) -> Result<Json<TransactionStatusResponse>, (axum::http::StatusCode, String)> {
    // For status, we allow checking even expired/inactive transactions
    let transactions = state.transaction_manager.transactions.read().await;

    match transactions.get(&transaction_id) {
        Some(tx) if tx.belongs_to_session(&auth) => {
            tracing::debug!(
                transaction_id = %transaction_id,
                org_id = auth.org_id,
                status = ?tx.status,
                "Retrieved transaction status"
            );

            Ok(Json(TransactionStatusResponse {
                transaction_id: tx.id.clone(),
                status: tx.status,
                isolation_level: tx.isolation_level,
                created_at: tx.created_at,
                expires_at: tx.expires_at,
                operations_count: tx.operations_count,
                last_activity: tx.last_activity,
            }))
        },
        Some(_) => {
            tracing::error!(
                transaction_id = %transaction_id,
                org_id = auth.org_id,
                "Access denied to transaction"
            );
            Err((axum::http::StatusCode::NOT_FOUND, "Transaction not found or access denied".to_string()))
        },
        None => {
            tracing::error!(
                transaction_id = %transaction_id,
                org_id = auth.org_id,
                "Transaction not found"
            );
            Err((axum::http::StatusCode::NOT_FOUND, "Transaction not found".to_string()))
        }
    }
}

// Distributed transaction handlers

#[derive(Deserialize)]
struct DistributedTransactionBeginRequest {
    participants: Vec<DistributedParticipantRequest>,
    config: Option<DistributedTransactionConfigRequest>,
}

#[derive(Deserialize)]
struct DistributedParticipantRequest {
    protocol: String, // "rest", "websocket", "webrtc"
    id: String,
    node_id: String,
}

#[derive(Deserialize)]
struct DistributedTransactionConfigRequest {
    timeout_secs: Option<u64>,
    priority: Option<u32>,
    enable_saga_compensation: Option<bool>,
}

#[derive(Serialize)]
struct DistributedTransactionResponse {
    transaction_id: String,
    status: String,
    participants: Vec<String>,
    created_at: String,
}

#[derive(Serialize)]
struct DistributedTransactionListResponse {
    transactions: Vec<DistributedTransactionResponse>,
    total: usize,
}

async fn distributed_transaction_begin_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(request): Json<DistributedTransactionBeginRequest>,
) -> Result<Json<DistributedTransactionResponse>, (StatusCode, String)> {
    // Check permissions for distributed transactions
    let principals = principals_for(&auth);
    ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Tables, // Using Tables as a proxy for transaction permission
        Crud::CREATE,
    ).map_err(|e| (StatusCode::FORBIDDEN, format!("Permission denied: {}", e)))?;

    let coordinator = state.distributed_coordinator
        .as_ref()
        .ok_or_else(|| (StatusCode::SERVICE_UNAVAILABLE, "Distributed transaction coordinator not available".to_string()))?;

    // Convert participants
    let participants: Result<Vec<_>, _> = request.participants.into_iter()
        .map(|p| {
            let protocol = match p.protocol.as_str() {
                "rest" => ProtocolType::Rest,
                "websocket" => ProtocolType::WebSocket,
                "webrtc" => ProtocolType::WebRTC,
                _ => return Err("Invalid protocol type"),
            };
            Ok(ParticipantId::new(protocol, p.id, p.node_id))
        })
        .collect();

    let participants = participants
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid participants: {}", e)))?;

    // Build config
    let config = request.config.map(|c| {
        let mut config = DistributedTransactionConfig::default();
        if let Some(timeout) = c.timeout_secs {
            config.global_timeout = std::time::Duration::from_secs(timeout);
        }
        if let Some(priority) = c.priority {
            config.priority = priority;
        }
        if let Some(saga) = c.enable_saga_compensation {
            config.enable_saga_compensation = saga;
        }
        config
    });

    match coordinator.begin_transaction(participants.clone(), config).await {
        Ok(tx_id) => {
            tracing::info!(
                transaction_id = %tx_id,
                org_id = auth.org_id,
                participant_count = participants.len(),
                "Distributed transaction started"
            );

            let response = DistributedTransactionResponse {
                transaction_id: tx_id.to_string(),
                status: "Initializing".to_string(),
                participants: participants.iter().map(|p| p.to_string()).collect(),
                created_at: chrono::Utc::now().to_rfc3339(),
            };

            Ok(Json(response))
        }
        Err(e) => {
            tracing::error!(
                org_id = auth.org_id,
                error = %e,
                "Failed to start distributed transaction"
            );
            Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to start transaction: {}", e)))
        }
    }
}

async fn distributed_transaction_status_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    AxumPath(transaction_id): AxumPath<String>,
) -> Result<Json<DistributedTransactionResponse>, (StatusCode, String)> {
    let coordinator = state.distributed_coordinator
        .as_ref()
        .ok_or_else(|| (StatusCode::SERVICE_UNAVAILABLE, "Distributed transaction coordinator not available".to_string()))?;

    let tx_id = GlobalTransactionId::from_transaction_id(
        ztransaction::TransactionId::from_str(&transaction_id)
            .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid transaction ID format".to_string()))?,
        "coordinator".to_string(), // This should be more dynamic in a real implementation
        0,
    );

    match coordinator.get_transaction(tx_id) {
        Ok(tx) => {
            let response = DistributedTransactionResponse {
                transaction_id: tx.id.to_string(),
                status: tx.status.to_string(),
                participants: tx.participants.iter().map(|p| p.to_string()).collect(),
                created_at: tx.created_at.to_rfc3339(),
            };
            Ok(Json(response))
        }
        Err(_) => {
            Err((StatusCode::NOT_FOUND, "Transaction not found".to_string()))
        }
    }
}

async fn distributed_transaction_abort_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    AxumPath(transaction_id): AxumPath<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let coordinator = state.distributed_coordinator
        .as_ref()
        .ok_or_else(|| (StatusCode::SERVICE_UNAVAILABLE, "Distributed transaction coordinator not available".to_string()))?;

    let tx_id = GlobalTransactionId::from_transaction_id(
        ztransaction::TransactionId::from_str(&transaction_id)
            .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid transaction ID format".to_string()))?,
        "coordinator".to_string(),
        0,
    );

    match coordinator.abort_transaction(tx_id, "User requested abort".to_string()).await {
        Ok(()) => {
            Ok(Json(serde_json::json!({"status": "aborted", "transaction_id": transaction_id})))
        }
        Err(e) => {
            tracing::error!(
                transaction_id = %transaction_id,
                error = %e,
                "Failed to abort transaction"
            );
            Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to abort transaction: {}", e)))
        }
    }
}

async fn list_distributed_transactions_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Json<DistributedTransactionListResponse>, (StatusCode, String)> {
    let coordinator = state.distributed_coordinator
        .as_ref()
        .ok_or_else(|| (StatusCode::SERVICE_UNAVAILABLE, "Distributed transaction coordinator not available".to_string()))?;

    let transactions = coordinator.list_active_transactions();
    let transaction_responses: Vec<_> = transactions.into_iter()
        .map(|tx| DistributedTransactionResponse {
            transaction_id: tx.id.to_string(),
            status: tx.status.to_string(),
            participants: tx.participants.iter().map(|p| p.to_string()).collect(),
            created_at: tx.created_at.to_rfc3339(),
        })
        .collect();

    let response = DistributedTransactionListResponse {
        total: transaction_responses.len(),
        transactions: transaction_responses,
    };

    Ok(Json(response))
}

async fn distributed_transaction_metrics_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Json<DistributedTransactionMetrics>, (StatusCode, String)> {
    let coordinator = state.distributed_coordinator
        .as_ref()
        .ok_or_else(|| (StatusCode::SERVICE_UNAVAILABLE, "Distributed transaction coordinator not available".to_string()))?;

    let metrics = coordinator.get_metrics().await;
    Ok(Json(metrics))
}

// Saga handlers

#[derive(Deserialize)]
struct SagaStartRequest {
    name: String,
    description: Option<String>,
    steps: Vec<SagaStepRequest>,
    config: Option<SagaConfigRequest>,
}

#[derive(Deserialize)]
struct SagaStepRequest {
    step_id: String,
    operation: TransactionOperationRequest,
    compensation: Option<TransactionOperationRequest>,
    participant: DistributedParticipantRequest,
    timeout_secs: Option<u64>,
    retryable: Option<bool>,
    max_retries: Option<u32>,
    dependencies: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct TransactionOperationRequest {
    operation_type: String,
    payload: serde_json::Value,
    parameters: Option<std::collections::HashMap<String, serde_json::Value>>,
    resources: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct SagaConfigRequest {
    timeout_secs: Option<u64>,
    failure_strategy: Option<String>,
    continue_on_error: Option<bool>,
    execution_mode: Option<String>,
    max_concurrent_steps: Option<usize>,
}

#[derive(Serialize)]
struct SagaResponse {
    saga_id: String,
    name: String,
    status: String,
    created_at: String,
    steps_total: usize,
    steps_completed: usize,
}

async fn saga_start_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(request): Json<SagaStartRequest>,
) -> Result<Json<SagaResponse>, (StatusCode, String)> {
    let saga_coordinator = state.saga_coordinator
        .as_ref()
        .ok_or_else(|| (StatusCode::SERVICE_UNAVAILABLE, "Saga coordinator not available".to_string()))?;

    // Convert steps
    let steps: Result<Vec<_>, _> = request.steps.into_iter()
        .map(|s| {
            let protocol = match s.participant.protocol.as_str() {
                "rest" => ProtocolType::Rest,
                "websocket" => ProtocolType::WebSocket,
                "webrtc" => ProtocolType::WebRTC,
                _ => return Err("Invalid protocol type"),
            };
            let participant = ParticipantId::new(protocol, s.participant.id, s.participant.node_id);

            let operation = TransactionOperation {
                id: format!("{}_{}", s.step_id, "op"),
                operation_type: s.operation.operation_type,
                payload: s.operation.payload,
                parameters: s.operation.parameters.unwrap_or_default(),
                resources: s.operation.resources.unwrap_or_default(),
            };

            let mut step = SagaStep::new(s.step_id, operation, participant);

            if let Some(comp) = s.compensation {
                let compensation_op = TransactionOperation {
                    id: format!("{}_{}", step.step_id, "comp"),
                    operation_type: comp.operation_type,
                    payload: comp.payload,
                    parameters: comp.parameters.unwrap_or_default(),
                    resources: comp.resources.unwrap_or_default(),
                };
                step = step.with_compensation(compensation_op);
            }

            if let Some(timeout) = s.timeout_secs {
                step = step.with_timeout(std::time::Duration::from_secs(timeout));
            }

            if let Some(deps) = s.dependencies {
                step = step.with_dependencies(deps);
            }

            if let Some(false) = s.retryable {
                step = step.non_retryable();
            }

            Ok(step)
        })
        .collect();

    let steps = steps
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid saga steps: {}", e)))?;

    // Build config
    let config = request.config.map(|c| {
        let mut config = SagaConfig::default();
        if let Some(timeout) = c.timeout_secs {
            config.timeout = std::time::Duration::from_secs(timeout);
        }
        // Add other config mappings as needed
        config
    });

    match saga_coordinator.start_saga(request.name.clone(), steps, config).await {
        Ok(saga_id) => {
            let response = SagaResponse {
                saga_id: saga_id.to_string(),
                name: request.name,
                status: "Planning".to_string(),
                created_at: chrono::Utc::now().to_rfc3339(),
                steps_total: request.steps.len(),
                steps_completed: 0,
            };

            Ok(Json(response))
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to start saga");
            Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to start saga: {}", e)))
        }
    }
}

async fn saga_status_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    AxumPath(saga_id): AxumPath<String>,
) -> Result<Json<SagaResponse>, (StatusCode, String)> {
    let saga_coordinator = state.saga_coordinator
        .as_ref()
        .ok_or_else(|| (StatusCode::SERVICE_UNAVAILABLE, "Saga coordinator not available".to_string()))?;

    let saga_uuid = uuid::Uuid::from_str(&saga_id)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid saga ID format".to_string()))?;

    match saga_coordinator.get_saga(&SagaId::from(saga_uuid)) {
        Some(saga) => {
            let completed_count = saga.get_completed_steps().len();
            let response = SagaResponse {
                saga_id: saga.id.to_string(),
                name: saga.name,
                status: saga.status.to_string(),
                created_at: saga.created_at.to_rfc3339(),
                steps_total: saga.steps.len(),
                steps_completed: completed_count,
            };
            Ok(Json(response))
        }
        None => {
            Err((StatusCode::NOT_FOUND, "Saga not found".to_string()))
        }
    }
}

async fn saga_abort_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    AxumPath(saga_id): AxumPath<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let saga_coordinator = state.saga_coordinator
        .as_ref()
        .ok_or_else(|| (StatusCode::SERVICE_UNAVAILABLE, "Saga coordinator not available".to_string()))?;

    let saga_uuid = uuid::Uuid::from_str(&saga_id)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid saga ID format".to_string()))?;

    match saga_coordinator.abort_saga(SagaId::from(saga_uuid), "User requested abort".to_string()).await {
        Ok(()) => {
            Ok(Json(serde_json::json!({"status": "aborted", "saga_id": saga_id})))
        }
        Err(e) => {
            tracing::error!(saga_id = %saga_id, error = %e, "Failed to abort saga");
            Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to abort saga: {}", e)))
        }
    }
}

async fn list_sagas_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Json<Vec<SagaResponse>>, (StatusCode, String)> {
    let saga_coordinator = state.saga_coordinator
        .as_ref()
        .ok_or_else(|| (StatusCode::SERVICE_UNAVAILABLE, "Saga coordinator not available".to_string()))?;

    let sagas = saga_coordinator.list_active_sagas();
    let responses: Vec<_> = sagas.into_iter()
        .map(|saga| {
            let completed_count = saga.get_completed_steps().len();
            SagaResponse {
                saga_id: saga.id.to_string(),
                name: saga.name,
                status: saga.status.to_string(),
                created_at: saga.created_at.to_rfc3339(),
                steps_total: saga.steps.len(),
                steps_completed: completed_count,
            }
        })
        .collect();

    Ok(Json(responses))
}

async fn list_tables(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Json<Vec<zcore_catalog::TableDef>>, axum::http::StatusCode> {
    let principals = principals_for(&auth);
    ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Tables,
        Crud::READ,
    )?;
    match state.catalog.list_tables(auth.org_id) {
        Ok(tables) => Ok(Json(tables)),
        Err(err) => {
            tracing::error!(error = ?err, "list tables failed");
            Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[derive(Debug, Deserialize)]
struct SampleQuery {
    limit: Option<usize>,
}

async fn table_sample(
    auth: AuthSession,
    AxumPath(table): AxumPath<String>,
    Query(q): Query<SampleQuery>,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Json<SqlResponse>, axum::http::StatusCode> {
    let principals = principals_for(&auth);
    ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Tables,
        Crud::READ,
    )?;
    let limit = q.limit.unwrap_or(20).max(1).min(100);
    match sample_table_rows(&state.store, &state.catalog, auth.org_id, &table, limit) {
        Ok(ExecResult::Rows(rows)) => {
            let rows: Vec<serde_json::Map<String, serde_json::Value>> = rows.into_iter().map(|m| m.into_iter().collect()).collect();
            Ok(Json(SqlResponse { rows }))
        }
        Ok(_) => Ok(Json(SqlResponse { rows: Vec::new() })),
        Err(err) => {
            let msg = err.to_string();
            if msg.contains("no such table") {
                Err(axum::http::StatusCode::NOT_FOUND)
            } else {
                tracing::error!(error = ?err, table = %table, "sample rows failed");
                Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }
}

async fn index_stats_handler(
    auth: AuthSession,
    AxumPath((table, column)): AxumPath<(String, String)>,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Json<zann::IndexStats>, axum::http::StatusCode> {
    let principals = principals_for(&auth);
    ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Indexes,
        Crud::READ,
    )?;
    match zann::index_stats(&state.store, auth.org_id, &table, &column) {
        Ok(Some(stats)) => Ok(Json(stats)),
        Ok(None) => Err(axum::http::StatusCode::NOT_FOUND),
        Err(err) => {
            tracing::error!(
                error = ?err,
                table = %table,
                column = %column,
                "index stats failed"
            );
            Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn ws_upgrade_handler(
    ws: WebSocketUpgrade,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

#[derive(serde::Deserialize)]
struct WsSub {
    action: String,
    topic: String,
}

/// WebSocket transaction protocol messages
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum WsTransactionMessage {
    /// Begin a new transaction
    BeginTransaction {
        transaction_id: String,
    },
    /// Execute SQL within the transaction
    ExecuteInTransaction {
        transaction_id: String,
        sql: String,
        params: Option<serde_json::Value>,
    },
    /// Commit the transaction
    CommitTransaction {
        transaction_id: String,
    },
    /// Rollback the transaction
    RollbackTransaction {
        transaction_id: String,
    },
}

/// WebSocket transaction response messages
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum WsTransactionResponse {
    /// Transaction operation result
    TransactionResult {
        transaction_id: String,
        operation: String,
        success: bool,
        result: Option<serde_json::Value>,
        duration_ms: Option<u64>,
    },
    /// Transaction error
    TransactionError {
        transaction_id: String,
        operation: String,
        error: String,
        error_code: String,
    },
    /// Transaction status update
    TransactionStatus {
        transaction_id: String,
        status: TransactionState,
        operations_count: u64,
    },
}

/// Transaction state representation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TransactionState {
    Active,
    Committed,
    RolledBack,
    Aborted,
}

/// Per-connection transaction state
#[derive(Debug)]
struct ConnectionTransaction {
    id: String,
    state: TransactionState,
    operations_count: u64,
    started_at: std::time::Instant,
}

/// Handle transaction message and return response
async fn handle_transaction_message(
    msg: WsTransactionMessage,
    connection_id: &str,
    active_transactions: &Arc<TokioRwLock<std::collections::HashMap<String, ConnectionTransaction>>>,
    state: &AppState,
) -> WsTransactionResponse {
    let start_time = std::time::Instant::now();

    match msg {
        WsTransactionMessage::BeginTransaction { transaction_id } => {
            let mut transactions = active_transactions.write().await;

            if transactions.contains_key(&transaction_id) {
                return WsTransactionResponse::TransactionError {
                    transaction_id,
                    operation: "begin".to_string(),
                    error: "Transaction already exists".to_string(),
                    error_code: "TRANSACTION_EXISTS".to_string(),
                };
            }

            transactions.insert(transaction_id.clone(), ConnectionTransaction {
                id: transaction_id.clone(),
                state: TransactionState::Active,
                operations_count: 0,
                started_at: start_time,
            });

            // Emit transaction begun event
            emit_transaction_event(
                zevents::EventType::TransactionBegun,
                &transaction_id,
                connection_id,
                1, // default org_id for now
                None,
                None,
                state,
            ).await;

            tracing::info!(
                transaction_id = %transaction_id,
                connection_id = %connection_id,
                "Transaction begun"
            );

            WsTransactionResponse::TransactionResult {
                transaction_id,
                operation: "begin".to_string(),
                success: true,
                result: None,
                duration_ms: Some(start_time.elapsed().as_millis() as u64),
            }
        }

        WsTransactionMessage::ExecuteInTransaction { transaction_id, sql, params } => {
            let mut transactions = active_transactions.write().await;

            let transaction = match transactions.get_mut(&transaction_id) {
                Some(tx) if tx.state == TransactionState::Active => tx,
                Some(_) => {
                    return WsTransactionResponse::TransactionError {
                        transaction_id,
                        operation: "execute".to_string(),
                        error: "Transaction not in active state".to_string(),
                        error_code: "TRANSACTION_NOT_ACTIVE".to_string(),
                    };
                }
                None => {
                    return WsTransactionResponse::TransactionError {
                        transaction_id,
                        operation: "execute".to_string(),
                        error: "Transaction not found".to_string(),
                        error_code: "TRANSACTION_NOT_FOUND".to_string(),
                    };
                }
            };

            // Execute SQL (simplified - in real implementation, this would be in a transaction context)
            let result = match exec_sql_store(&sql, 1, &state.store, &state.catalog) {
                Ok(exec_result) => {
                    transaction.operations_count += 1;

                    let result_json = match exec_result {
                        ExecResult::Rows(rows) => {
                            serde_json::to_value(rows).unwrap_or(serde_json::Value::Null)
                        }
                        ExecResult::Affected(count) => {
                            serde_json::json!({ "affected_rows": count })
                        }
                    };

                    tracing::debug!(
                        transaction_id = %transaction_id,
                        sql = %sql,
                        operations_count = transaction.operations_count,
                        "SQL executed in transaction"
                    );

                    WsTransactionResponse::TransactionResult {
                        transaction_id,
                        operation: "execute".to_string(),
                        success: true,
                        result: Some(result_json),
                        duration_ms: Some(start_time.elapsed().as_millis() as u64),
                    }
                }
                Err(e) => {
                    tracing::error!(
                        transaction_id = %transaction_id,
                        sql = %sql,
                        error = %e,
                        "SQL execution failed in transaction"
                    );

                    WsTransactionResponse::TransactionError {
                        transaction_id,
                        operation: "execute".to_string(),
                        error: e.to_string(),
                        error_code: "SQL_EXECUTION_ERROR".to_string(),
                    }
                }
            };

            result
        }

        WsTransactionMessage::CommitTransaction { transaction_id } => {
            let mut transactions = active_transactions.write().await;

            match transactions.get_mut(&transaction_id) {
                Some(tx) if tx.state == TransactionState::Active => {
                    tx.state = TransactionState::Committed;

                    let operations_count = tx.operations_count;
                    let duration = tx.started_at.elapsed().as_millis() as u64;

                    // Emit transaction committed event
                    emit_transaction_event(
                        zevents::EventType::TransactionCommitted,
                        &transaction_id,
                        connection_id,
                        1,
                        Some(operations_count),
                        Some(duration),
                        state,
                    ).await;

                    // Remove completed transaction
                    transactions.remove(&transaction_id);

                    tracing::info!(
                        transaction_id = %transaction_id,
                        connection_id = %connection_id,
                        operations_count = operations_count,
                        duration_ms = duration,
                        "Transaction committed"
                    );

                    WsTransactionResponse::TransactionResult {
                        transaction_id,
                        operation: "commit".to_string(),
                        success: true,
                        result: Some(serde_json::json!({
                            "operations_count": operations_count,
                            "duration_ms": duration
                        })),
                        duration_ms: Some(start_time.elapsed().as_millis() as u64),
                    }
                }
                Some(_) => {
                    WsTransactionResponse::TransactionError {
                        transaction_id,
                        operation: "commit".to_string(),
                        error: "Transaction not in active state".to_string(),
                        error_code: "TRANSACTION_NOT_ACTIVE".to_string(),
                    }
                }
                None => {
                    WsTransactionResponse::TransactionError {
                        transaction_id,
                        operation: "commit".to_string(),
                        error: "Transaction not found".to_string(),
                        error_code: "TRANSACTION_NOT_FOUND".to_string(),
                    }
                }
            }
        }

        WsTransactionMessage::RollbackTransaction { transaction_id } => {
            let mut transactions = active_transactions.write().await;

            match transactions.get_mut(&transaction_id) {
                Some(tx) if tx.state == TransactionState::Active => {
                    tx.state = TransactionState::RolledBack;

                    let operations_count = tx.operations_count;
                    let duration = tx.started_at.elapsed().as_millis() as u64;

                    // Emit transaction rolled back event
                    emit_transaction_event(
                        zevents::EventType::TransactionRolledBack,
                        &transaction_id,
                        connection_id,
                        1,
                        Some(operations_count),
                        Some(duration),
                        state,
                    ).await;

                    // Remove rolled back transaction
                    transactions.remove(&transaction_id);

                    tracing::info!(
                        transaction_id = %transaction_id,
                        connection_id = %connection_id,
                        operations_count = operations_count,
                        duration_ms = duration,
                        "Transaction rolled back"
                    );

                    WsTransactionResponse::TransactionResult {
                        transaction_id,
                        operation: "rollback".to_string(),
                        success: true,
                        result: Some(serde_json::json!({
                            "operations_count": operations_count,
                            "duration_ms": duration
                        })),
                        duration_ms: Some(start_time.elapsed().as_millis() as u64),
                    }
                }
                Some(_) => {
                    WsTransactionResponse::TransactionError {
                        transaction_id,
                        operation: "rollback".to_string(),
                        error: "Transaction not in active state".to_string(),
                        error_code: "TRANSACTION_NOT_ACTIVE".to_string(),
                    }
                }
                None => {
                    WsTransactionResponse::TransactionError {
                        transaction_id,
                        operation: "rollback".to_string(),
                        error: "Transaction not found".to_string(),
                        error_code: "TRANSACTION_NOT_FOUND".to_string(),
                    }
                }
            }
        }
    }
}

/// Emit transaction-related events for broadcasting
async fn emit_transaction_event(
    event_type: zevents::EventType,
    transaction_id: &str,
    connection_id: &str,
    org_id: u64,
    operations_count: Option<u64>,
    duration_ms: Option<u64>,
    state: &AppState,
) {
    let event_data = zevents::EventData::Transaction {
        transaction_id: transaction_id.to_string(),
        connection_id: connection_id.to_string(),
        org_id,
        operations_count,
        duration_ms,
        error: None,
    };

    let event = zevents::Event::new(event_type, event_data, org_id)
        .with_source("zserver-websocket".to_string());

    // Broadcast event to job queue for WebSocket subscribers
    if let Err(e) = state.job_queue.send(zbrain::BusEvent {
        topic: event.to_topic(),
        seq: 0,
        ts: chrono::Utc::now(),
        data: serde_json::to_value(&event).unwrap_or_default(),
    }) {
        tracing::warn!(
            transaction_id = transaction_id,
            error = %e,
            "Failed to broadcast transaction event"
        );
    }
}

async fn handle_ws(socket: WebSocket, state: AppState) {
    use std::collections::{HashSet, VecDeque};
    use std::sync::{atomic::Ordering as AtomicOrdering, Arc};
    use std::time::Instant;
    use uuid::Uuid;

    const WS_QUEUE_CAP: usize = 256;

    let metrics = state.ws_metrics.clone();
    metrics.inc_active();

    // Connection ID for tracking
    let connection_id = Uuid::new_v4().to_string();

    let subs: Arc<TokioRwLock<HashSet<String>>> = Arc::new(TokioRwLock::new(HashSet::new()));
    let queue: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::new()));

    // Transaction state management
    let active_transactions: Arc<TokioRwLock<std::collections::HashMap<String, ConnectionTransaction>>> =
        Arc::new(TokioRwLock::new(std::collections::HashMap::new()));
    let notify = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicU64::new(0));
    let mut rx = state.job_queue.subscribe();
    let (sink, mut stream) = socket.split();

    let subs_for_producer = subs.clone();
    let queue_for_producer = queue.clone();
    let notify_for_producer = notify.clone();
    let dropped_for_producer = dropped.clone();
    let metrics_for_producer = metrics.clone();
    let producer = tokio::spawn(async move {
        while let Ok(ev) = rx.recv().await {
            let subscribed = { subs_for_producer.read().await.contains(&ev.topic) };
            if !subscribed {
                continue;
            }
            match serde_json::to_string(&ev) {
                Ok(payload) => {
                    let mut q = queue_for_producer.lock().await;
                    if q.len() >= WS_QUEUE_CAP {
                        q.pop_front();
                        metrics_for_producer.add_dropped(1);
                        let total = dropped_for_producer.fetch_add(1, AtomicOrdering::Relaxed) + 1;
                        match serde_json::to_string(&serde_json::json!({
                            "topic": "meta/ws",
                            "seq": 0,
                            "ts": chrono::Utc::now(),
                            "data": {"event": "dropped", "count": total},
                        })) {
                            Ok(meta_str) => {
                                tracing::warn!(
                                    dropped_total = total,
                                    "ws queue backpressure: dropping oldest message"
                                );
                                if q.len() >= WS_QUEUE_CAP {
                                    q.pop_front();
                                    metrics_for_producer.add_dropped(1);
                                    dropped_for_producer.fetch_add(1, AtomicOrdering::Relaxed);
                                }
                                q.push_back(meta_str);
                                notify_for_producer.notify_one();
                            }
                            Err(meta_err) => {
                                tracing::error!(
                                    error = %meta_err,
                                    "Failed to serialize WebSocket metadata message"
                                );
                            }
                        }
                    }
                    q.push_back(payload);
                    notify_for_producer.notify_one();
                }
                Err(ser_err) => {
                    tracing::error!(
                        error = %ser_err,
                        topic = %ev.topic,
                        "Failed to serialize WebSocket event payload"
                    );
                }
            }
        }
        tracing::debug!("WebSocket producer task terminated");
    });

    let queue_for_sender = queue.clone();
    let notify_for_sender = notify.clone();
    let sender = tokio::spawn(async move {
        let mut sink = sink;
        loop {
            let msg = {
                let mut guard = queue_for_sender.lock().await;
                match guard.pop_front() {
                    Some(v) => v,
                    None => {
                        drop(guard);
                        notify_for_sender.notified().await;
                        continue;
                    }
                }
            };
            match sink.send(Message::Text(msg.clone())).await {
                Ok(()) => {
                    tracing::trace!("Successfully sent WebSocket message");
                }
                Err(send_err) => {
                    tracing::error!(
                        error = %send_err,
                        message_length = msg.len(),
                        "Failed to send WebSocket message, connection likely closed"
                    );
                    break;
                }
            }
        }
        tracing::debug!("WebSocket sender task terminated");
    });

    // inbound loop: manage subscriptions
    while let Some(msg_result) = stream.next().await {
        match msg_result {
            Ok(msg) => match msg {
                Message::Text(t) => {
                    // Try parsing as subscription message first
                    if let Ok(ws) = serde_json::from_str::<WsSub>(&t) {
                        match ws.action.as_str() {
                            "subscribe" => {
                                tracing::debug!(topic = %ws.topic, "WebSocket subscription added");
                                subs.write().await.insert(ws.topic);
                            }
                            "unsubscribe" => {
                                tracing::debug!(topic = %ws.topic, "WebSocket subscription removed");
                                subs.write().await.remove(&ws.topic);
                            }
                            action => {
                                tracing::warn!(action = %action, "Unknown WebSocket action received");
                            }
                        }
                    }
                    // Try parsing as transaction message
                    else if let Ok(tx_msg) = serde_json::from_str::<WsTransactionMessage>(&t) {
                        let response = handle_transaction_message(
                            tx_msg,
                            &connection_id,
                            &active_transactions,
                            &state,
                        ).await;

                        // Send response back to client
                        if let Ok(response_json) = serde_json::to_string(&response) {
                            let mut q = queue.lock().await;
                            if q.len() >= WS_QUEUE_CAP {
                                let dropped_count = q.len() - WS_QUEUE_CAP / 2;
                                for _ in 0..dropped_count {
                                    q.pop_front();
                                }
                                dropped.store(
                                    dropped.load(AtomicOrdering::Relaxed) + dropped_count as u64,
                                    AtomicOrdering::Relaxed,
                                );
                                notify_for_producer.notify_one();
                            }
                            q.push_back(response_json);
                            notify_for_producer.notify_one();
                        }
                    }
                    else {
                        tracing::warn!(
                            message = %t,
                            "Failed to parse WebSocket message as subscription or transaction"
                        );
                    }
                }
                Message::Close(close_frame) => {
                    if let Some(cf) = close_frame {
                        tracing::info!(
                            code = cf.code,
                            reason = %cf.reason,
                            "WebSocket connection closed by client"
                        );
                    } else {
                        tracing::info!("WebSocket connection closed by client");
                    }
                    break;
                }
                Message::Ping(data) => {
                    tracing::trace!(data_len = data.len(), "WebSocket ping received");
                    // Axum automatically handles pong responses
                }
                Message::Pong(data) => {
                    tracing::trace!(data_len = data.len(), "WebSocket pong received");
                }
                Message::Binary(data) => {
                    tracing::warn!(data_len = data.len(), "Unexpected binary WebSocket message received");
                }
            },
            Err(ws_err) => {
                tracing::error!(
                    error = %ws_err,
                    "WebSocket message receive error"
                );
                break;
            }
        }
    }

    // Cleanup: Rollback any active transactions
    let mut transactions = active_transactions.write().await;
    let active_tx_count = transactions.len();

    if active_tx_count > 0 {
        tracing::warn!(
            connection_id = %connection_id,
            active_transactions = active_tx_count,
            "Rolling back active transactions due to connection close"
        );

        for (transaction_id, mut transaction) in transactions.drain() {
            transaction.state = TransactionState::Aborted;
            let operations_count = transaction.operations_count;
            let duration = transaction.started_at.elapsed().as_millis() as u64;

            // Emit transaction aborted event
            emit_transaction_event(
                zevents::EventType::TransactionAborted,
                &transaction_id,
                &connection_id,
                1,
                Some(operations_count),
                Some(duration),
                &state,
            ).await;

            tracing::info!(
                transaction_id = %transaction_id,
                connection_id = %connection_id,
                operations_count = operations_count,
                duration_ms = duration,
                "Transaction aborted due to connection close"
            );
        }
    }

    producer.abort();
    notify.notify_waiters();
    sender.abort();
    metrics.dec_active();

    tracing::debug!("WebSocket connection handler completed");
}

#[derive(serde::Deserialize, serde::Serialize)]
struct CreateCollectionReq {
    name: String,
    projection: zcoll::Projection,
}

async fn create_collection(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(req): Json<CreateCollectionReq>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Collections,
        Crud::CREATE,
    ) {
        return status.into_response();
    }
    if let Err(e) = zcoll::put_projection(&state.store, auth.org_id, &req.name, &req.projection) {
        return (axum::http::StatusCode::BAD_REQUEST, e.to_string()).into_response();
    }
    Json(serde_json::json!({"ok": true})).into_response()
}

async fn get_projection(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Collections,
        Crud::READ,
    ) {
        return status.into_response();
    }
    match zcoll::get_projection(&state.store, auth.org_id, &name) {
        Ok(Some(p)) => Json(p).into_response(),
        Ok(None) => axum::http::StatusCode::NOT_FOUND.into_response(),
        Err(e) => (axum::http::StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn put_projection(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(p): Json<zcoll::Projection>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Collections,
        Crud::UPDATE,
    ) {
        return status.into_response();
    }
    if let Err(e) = zcoll::put_projection(&state.store, auth.org_id, &name, &p) {
        return (axum::http::StatusCode::BAD_REQUEST, e.to_string()).into_response();
    }
    Json(serde_json::json!({"ok": true})).into_response()
}

// Unified ingest: detects NDJSON vs JSON array by content-type/sniffing
async fn ingest_collection(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
    req: axum::http::Request<axum::body::Body>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Collections,
        Crud::CREATE,
    ) {
        return status.into_response();
    }
    let headers = req.headers().clone();
    let ctype = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let enc = headers
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // Turn body into AsyncRead (streaming)
    let stream = req
        .into_body()
        .into_data_stream()
        .map(|r| r.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
        .map(|r: Result<Bytes, std::io::Error>| r);
    let reader = StreamReader::new(stream);
    let mut ok = 0usize;
    let mut err = 0usize;

    if ctype.contains("x-ndjson") {
        // NDJSON streaming + optional gzip
        let r: Box<dyn tokio::io::AsyncRead + Unpin + Send> = if enc.eq_ignore_ascii_case("gzip") {
            Box::new(GzipDecoder::new(BufReader::new(reader)))
        } else {
            Box::new(reader)
        };
        let mut lines = FramedRead::new(r, LinesCodec::new());
        while let Some(line) = lines.next().await {
            match line {
                Ok(mut l) => {
                    if l.trim().is_empty() {
                        continue;
                    }
                    match simdj::serde::from_slice::<serde_json::Value>(unsafe { l.as_bytes_mut() })
                    {
                        Ok(jv) => {
                            if zcoll::apply_and_insert(
                                &state.store,
                                &state.catalog,
                                auth.org_id,
                                &name,
                                &jv,
                            )
                            .is_ok()
                            {
                                ok += 1;
                            } else {
                                err += 1;
                            }
                        }
                        Err(_) => {
                            err += 1;
                        }
                    }
                }
                Err(_) => {
                    err += 1;
                }
            }
        }
        return Json(serde_json::json!({"ingested": ok, "errors": err})).into_response();
    }

    // Streaming JSON array: Deserializer over SyncIoBridge
    let r: Box<dyn tokio::io::AsyncRead + Unpin + Send> = if enc.eq_ignore_ascii_case("gzip") {
        Box::new(GzipDecoder::new(BufReader::new(reader)))
    } else {
        Box::new(reader)
    };
    let bridge = SyncIoBridge::new(r);
    let de = serde_json::Deserializer::from_reader(bridge);
    let mut stream = de.into_iter::<serde_json::Value>();
    if let Some(first) = stream.next() {
        match first {
            Ok(serde_json::Value::Array(arr)) => {
                for elem in arr {
                    if zcoll::apply_and_insert(
                        &state.store,
                        &state.catalog,
                        auth.org_id,
                        &name,
                        &elem,
                    )
                    .is_ok()
                    {
                        ok += 1;
                    } else {
                        err += 1;
                    }
                }
            }
            Ok(v) => {
                if zcoll::apply_and_insert(&state.store, &state.catalog, auth.org_id, &name, &v)
                    .is_ok()
                {
                    ok += 1;
                } else {
                    err += 1;
                }
                for item in stream {
                    match item {
                        Ok(v2) => {
                            if zcoll::apply_and_insert(
                                &state.store,
                                &state.catalog,
                                auth.org_id,
                                &name,
                                &v2,
                            )
                            .is_ok()
                            {
                                ok += 1;
                            } else {
                                err += 1;
                            }
                        }
                        Err(e) => {
                            return (axum::http::StatusCode::BAD_REQUEST, e.to_string())
                                .into_response();
                        }
                    }
                }
            }
            Err(e) => {
                return (axum::http::StatusCode::BAD_REQUEST, e.to_string()).into_response();
            }
        }
    }
    Json(serde_json::json!({"ingested": ok, "errors": err})).into_response()
}

fn job_state_str(state: &JobState) -> &'static str {
    match state {
        JobState::Pending => "pending",
        JobState::Running => "running",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::Retrying => "retrying",
        JobState::Canceled => "canceled",
    }
}

#[allow(dead_code)]
fn trim_ascii(s: &[u8]) -> &[u8] {
    let mut a = 0;
    let mut b = s.len();
    while a < b && s[a].is_ascii_whitespace() {
        a += 1;
    }
    while b > a && s[b - 1].is_ascii_whitespace() {
        b -= 1;
    }
    &s[a..b]
}

#[cfg(test)]
mod tests {
    use super::WsMetrics;

    #[test]
    fn ws_metrics_counts() {
        let metrics = WsMetrics::default();
        metrics.inc_active();
        metrics.inc_active();
        metrics.add_dropped(3);
        metrics.dec_active();
        let (active, dropped) = metrics.snapshot();
        assert_eq!(active, 1);
        assert_eq!(dropped, 3);
        metrics.dec_active();
        let (active_after, dropped_after) = metrics.snapshot();
        assert_eq!(active_after, 0);
        assert_eq!(dropped_after, 3);
    }
}

// Simple content-addressed file storage
async fn upload_file(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Files,
        Crud::CREATE,
    ) {
        return status.into_response();
    }

    // Validate file size (max 100MB)
    if body.len() > 100 * 1024 * 1024 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "file_too_large",
                "message": "File size exceeds maximum limit of 100MB"
            }))
        ).into_response();
    }

    let mut hasher = Sha256::new();
    hasher.update(&body);
    let digest = hasher.finalize();
    let sha = hex::encode(digest);

    // Create secure org directory with path validation
    let org_dir_str = format!("org_{}", auth.org_id);
    if let Err(_) = security::InputValidator::validate_file_path(&org_dir_str, &state.files_root.to_string_lossy()) {
        tracing::warn!(org_id = auth.org_id, "Invalid file path detected");
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "invalid_path",
                "message": "Invalid file path"
            }))
        ).into_response();
    }

    let org_dir = state.files_root.join(org_dir_str);

    // Additional path safety check
    if org_dir != state.files_root.join(format!("org_{}", auth.org_id)) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "path_traversal_attempt",
                "message": "Potential path traversal attack detected"
            }))
        ).into_response();
    }

    if let Err(e) = std::fs::create_dir_all(&org_dir) {
        tracing::error!(error = ?e, "Failed to create directory for file upload");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "storage_error",
                "message": "Failed to create storage directory"
            }))
        ).into_response();
    }
    let path = org_dir.join(&sha);
    if !path.exists() {
        if let Err(e) = std::fs::write(&path, &body) {
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    }
    Json(serde_json::json!({"sha256": sha, "size": body.len()})).into_response()
}

async fn get_file(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Path(sha): axum::extract::Path<String>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Files,
        Crud::READ,
    ) {
        return status.into_response();
    }

    // Validate SHA256 format (64 hex characters)
    if sha.len() != 64 || !sha.chars().all(|c| c.is_ascii_hexdigit()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "invalid_sha_format",
                "message": "SHA256 must be 64 hexadecimal characters"
            }))
        ).into_response();
    }

    // Validate path to prevent directory traversal
    let org_dir_str = format!("org_{}", auth.org_id);
    if let Err(_) = security::InputValidator::validate_file_path(&org_dir_str, &state.files_root.to_string_lossy()) {
        tracing::warn!(org_id = auth.org_id, "Invalid file path detected in download");
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "invalid_path",
                "message": "Invalid file path"
            }))
        ).into_response();
    }

    let expected_path = state.files_root.join(org_dir_str).join(&sha);
    let path = state
        .files_root
        .join(format!("org_{}", auth.org_id))
        .join(&sha);

    // Additional path safety check
    if path != expected_path {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "path_traversal_attempt",
                "message": "Potential path traversal attack detected"
            }))
        ).into_response();
    }

    match std::fs::read(&path) {
        Ok(bytes) => axum::body::Bytes::from(bytes).into_response(),
        Err(_) => axum::http::StatusCode::NOT_FOUND.into_response(),
    }
}

// ---- Jobs minimal API ----
#[derive(serde::Deserialize)]
struct SubmitJobReq {
    cmd: Vec<String>,
    #[serde(default)]
    input: serde_json::Value,
    #[serde(default)]
    env: std::collections::HashMap<String, String>,
    timeout_secs: Option<u64>,
    #[serde(default)]
    max_retries: Option<u32>,
}

#[derive(serde::Serialize)]
struct SubmitJobResp {
    job_id: String,
}

async fn job_submit(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(req): Json<SubmitJobReq>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Jobs,
        Crud::CREATE,
    ) {
        return status.into_response();
    }
    if req.cmd.is_empty() {
        return (axum::http::StatusCode::BAD_REQUEST, "cmd required").into_response();
    }
    let spec = zjob::JobSpec {
        org_id: auth.org_id.to_string(),
        cmd: req.cmd,
        input: req.input,
        env: req.env,
        timeout_secs: req.timeout_secs,
        max_retries: req.max_retries.unwrap_or(1),
    };
    let job = zjob::Job::new(spec);
    let id = job.id;
    if let Err(e) = state.job_queue.submit(job).await {
        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    Json(SubmitJobResp {
        job_id: id.to_string(),
    })
    .into_response()
}

async fn job_get(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Jobs,
        Crud::READ,
    ) {
        return status.into_response();
    }
    let Ok(uuid) = uuid::Uuid::parse_str(&id) else {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    };
    match state.job_queue.get(&uuid).await {
        Ok(Some(j)) if j.spec.org_id == auth.org_id.to_string() => Json(j).into_response(),
        Ok(Some(_)) => axum::http::StatusCode::FORBIDDEN.into_response(),
        Ok(None) => axum::http::StatusCode::NOT_FOUND.into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn job_cancel(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Jobs,
        Crud::DELETE,
    ) {
        return status.into_response();
    }
    let Ok(uuid) = uuid::Uuid::parse_str(&id) else {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    };
    match state.job_queue.get(&uuid).await {
        Ok(Some(job)) if job.spec.org_id == auth.org_id.to_string() => {
            match state.job_queue.cancel_job(&uuid).await {
                Ok(CancelResult::PendingCanceled) => {
                    Json(serde_json::json!({"status": "canceled"})).into_response()
                }
                Ok(CancelResult::RunningSignal) => {
                    Json(serde_json::json!({"status": "canceling"})).into_response()
                }
                Ok(CancelResult::NoActiveWorker) => {
                    (axum::http::StatusCode::CONFLICT, "no active worker for job").into_response()
                }
                Ok(CancelResult::AlreadyFinished(state)) => (
                    axum::http::StatusCode::CONFLICT,
                    format!("job already {}", job_state_str(&state)),
                )
                    .into_response(),
                Ok(CancelResult::NotFound) => axum::http::StatusCode::NOT_FOUND.into_response(),
                Err(e) => {
                    (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                }
            }
        }
        Ok(Some(_)) => axum::http::StatusCode::FORBIDDEN.into_response(),
        Ok(None) => axum::http::StatusCode::NOT_FOUND.into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn list_job_artifacts(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        &state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Jobs,
        Crud::READ,
    ) {
        return status.into_response();
    }
    if let Err(status) = ensure_permission(
        &state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Files,
        Crud::READ,
    ) {
        return status.into_response();
    }
    let Ok(uuid) = uuid::Uuid::parse_str(&id) else {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    };
    match state.job_queue.get(&uuid).await {
        Ok(Some(job)) if job.spec.org_id == auth.org_id.to_string() => {
            match state.job_queue.list_artifacts(&uuid) {
                Ok(manifest) => Json(manifest).into_response(),
                Err(e) => {
                    (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                }
            }
        }
        Ok(Some(_)) => axum::http::StatusCode::FORBIDDEN.into_response(),
        Ok(None) => axum::http::StatusCode::NOT_FOUND.into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn download_job_artifact(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Path((id, name)): axum::extract::Path<(String, String)>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        &state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Jobs,
        Crud::READ,
    ) {
        return status.into_response();
    }
    if let Err(status) = ensure_permission(
        &state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Files,
        Crud::READ,
    ) {
        return status.into_response();
    }
    let Ok(uuid) = uuid::Uuid::parse_str(&id) else {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    };
    if name.is_empty() {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    }
    match state.job_queue.get(&uuid).await {
        Ok(Some(job)) if job.spec.org_id == auth.org_id.to_string() => {
            let manifest = match state.job_queue.list_artifacts(&uuid) {
                Ok(m) => m,
                Err(e) => {
                    return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
                        .into_response();
                }
            };
            let Some(artifact) = manifest.into_iter().find(|a| a.name == name) else {
                return axum::http::StatusCode::NOT_FOUND.into_response();
            };
            let Some(path) =
                state
                    .job_queue
                    .artifact_fs_path(&job.spec.org_id, &uuid, &artifact.name)
            else {
                return axum::http::StatusCode::BAD_REQUEST.into_response();
            };
            match tokio::fs::read(&path).await {
                Ok(bytes) => {
                    let mut headers = axum::http::HeaderMap::new();
                    headers.insert(
                        CONTENT_TYPE,
                        HeaderValue::from_static("application/octet-stream"),
                    );
                    if let Some(fname) = Path::new(&artifact.name)
                        .file_name()
                        .and_then(|f| f.to_str())
                    {
                        let safe = fname.replace('"', "'");
                        if let Ok(val) =
                            HeaderValue::from_str(&format!("attachment; filename=\"{}\"", safe))
                        {
                            headers.insert(CONTENT_DISPOSITION, val);
                        }
                    }
                    (headers, axum::body::Bytes::from(bytes)).into_response()
                }
                Err(_) => axum::http::StatusCode::NOT_FOUND.into_response(),
            }
        }
        Ok(Some(_)) => axum::http::StatusCode::FORBIDDEN.into_response(),
        Ok(None) => axum::http::StatusCode::NOT_FOUND.into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn job_list(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Jobs,
        Crud::READ,
    ) {
        return status.into_response();
    }
    match state
        .job_queue
        .list(Some(&auth.org_id.to_string()), None)
        .await
    {
        Ok(v) => Json(v).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(serde::Deserialize)]
struct LogQuery {
    from_seq: Option<u64>,
    limit: Option<usize>,
}

async fn get_job_logs(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
    axum::extract::Query(q): axum::extract::Query<LogQuery>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Jobs,
        Crud::READ,
    ) {
        return status.into_response();
    }
    let Ok(uuid) = uuid::Uuid::parse_str(&id) else {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    };
    let from = q.from_seq.unwrap_or(0);
    let limit = q.limit.unwrap_or(500);
    match state.job_queue.get(&uuid).await {
        Ok(Some(j)) if j.spec.org_id == auth.org_id.to_string() => {
            match state.job_queue.get_logs(&uuid, from, limit) {
                Ok(entries) => Json(entries).into_response(),
                Err(e) => {
                    (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                }
            }
        }
        Ok(Some(_)) => axum::http::StatusCode::FORBIDDEN.into_response(),
        Ok(None) => axum::http::StatusCode::NOT_FOUND.into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(serde::Deserialize)]
struct HnswBuildReq {
    table: String,
    column: String,
    metric: Option<String>,
}

async fn hnsw_build(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(req): Json<HnswBuildReq>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Indexes,
        Crud::CREATE,
    ) {
        return status.into_response();
    }
    let metric = match req.metric.as_deref() {
        Some(s) => zann::Metric::from_str(s).unwrap_or(zann::Metric::L2),
        None => zann::Metric::L2,
    };
    let m = 16usize;
    let ef_construction = 64usize;
    match zann::build_index(
        &state.store,
        &state.catalog,
        auth.org_id,
        &req.table,
        &req.column,
        metric,
        m,
        ef_construction,
    ) {
        Ok(_) => Json(serde_json::json!({"ok": true})).into_response(),
        Err(e) => (axum::http::StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

#[derive(serde::Deserialize)]
struct HnswSearchReq {
    table: String,
    column: String,
    query: Vec<f32>,
    top_k: usize,
    ef_search: Option<usize>,
}

async fn hnsw_search(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(req): Json<HnswSearchReq>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Indexes,
        Crud::READ,
    ) {
        return status.into_response();
    }
    let ef = req.ef_search.unwrap_or(64);
    match zann::search(
        &state.store,
        &state.catalog,
        auth.org_id,
        &req.table,
        &req.column,
        &req.query,
        req.top_k,
        ef,
    ) {
        Ok(res) => Json(serde_json::json!({"hits": res})).into_response(),
        Err(e) => (axum::http::StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn collection_list(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Collections,
        Crud::READ,
    ) {
        return status.into_response();
    }
    match zcoll::list_projections(state.store, auth.org_id) {
        Ok(list) => Json(list).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(serde::Serialize)]
struct HnswIndexResp {
    table: String,
    column: String,
    metric: String,
    m: usize,
    ef_construction: usize,
    layers: usize,
}

async fn hnsw_list(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Indexes,
        Crud::READ,
    ) {
        return status.into_response();
    }
    match zann::list_indexes(state.store, auth.org_id) {
        Ok(items) => {
            let resp: Vec<HnswIndexResp> = items
                .into_iter()
                .map(|idx| HnswIndexResp {
                    table: idx.table,
                    column: idx.column,
                    metric: idx.metric.to_string(),
                    m: idx.m,
                    ef_construction: idx.ef_construction,
                    layers: idx.layers,
                })
                .collect();
            Json(resp).into_response()
        }
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// Mesh Network API Handlers

/// Response for mesh network status
#[derive(Debug, Serialize)]
struct MeshStatusResponse {
    enabled: bool,
    local_node_id: Option<String>,
    connected_peers: u32,
    avg_latency_ms: u32,
    total_bandwidth_mbps: u32,
    health_score: f64,
    uptime_seconds: u64,
    total_data_transferred: u64,
    total_earnings: String, // ZRT amount as string
}

/// Response for mesh peer list
#[derive(Debug, Serialize)]
struct MeshPeersResponse {
    peers: Vec<MeshPeerInfo>,
}

#[derive(Debug, Serialize)]
struct MeshPeerInfo {
    node_id: String,
    state: String,
    last_seen: String,
    avg_latency_ms: u32,
    bandwidth_mbps: u32,
}

/// Request for connecting to a peer
#[derive(Debug, Deserialize)]
struct ConnectPeerRequest {
    address: String,
}

/// Response for mesh economics
#[derive(Debug, Serialize)]
struct MeshEconomicsResponse {
    total_distributed: String, // ZRT amount
    total_bandwidth_gb: f64,
    active_contributors: u32,
    avg_earnings_per_node: String, // ZRT amount
    last_settlement: Option<String>,
}

/// Get mesh network status
async fn mesh_status_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Jobs, // Using Jobs permission for mesh operations
        Crud::READ,
    ) {
        return status.into_response();
    }

    if let Some(mesh) = &state.mesh_network {
        let stats = mesh.get_stats().await;

        let response = MeshStatusResponse {
            enabled: true,
            local_node_id: Some(mesh.local_node_id().to_string()),
            connected_peers: stats.connected_peers,
            avg_latency_ms: stats.avg_latency_ms,
            total_bandwidth_mbps: stats.total_bandwidth_mbps,
            health_score: stats.health_score,
            uptime_seconds: stats.uptime.as_secs(),
            total_data_transferred: stats.total_data_transferred,
            total_earnings: stats.total_earnings.to_string(),
        };

        Json(response).into_response()
    } else {
        Json(MeshStatusResponse {
            enabled: false,
            local_node_id: None,
            connected_peers: 0,
            avg_latency_ms: 0,
            total_bandwidth_mbps: 0,
            health_score: 0.0,
            uptime_seconds: 0,
            total_data_transferred: 0,
            total_earnings: "0 ZRT".to_string(),
        }).into_response()
    }
}

/// Get connected mesh peers
async fn mesh_peers_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Jobs,
        Crud::READ,
    ) {
        return status.into_response();
    }

    if let Some(mesh) = &state.mesh_network {
        let peer_ids = mesh.get_connected_peers().await;

        // For now, return simplified peer info
        // In a full implementation, this would fetch detailed peer information
        let peers: Vec<MeshPeerInfo> = peer_ids
            .into_iter()
            .map(|node_id| MeshPeerInfo {
                node_id: node_id.to_string(),
                state: "Connected".to_string(),
                last_seen: chrono::Utc::now().to_rfc3339(),
                avg_latency_ms: 50, // Would be actual measurement
                bandwidth_mbps: 100, // Would be actual measurement
            })
            .collect();

        Json(MeshPeersResponse { peers }).into_response()
    } else {
        Json(MeshPeersResponse { peers: vec![] }).into_response()
    }
}

/// Connect to a specific peer
async fn mesh_connect_peer_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Path(node_id_str): axum::extract::Path<String>,
    Json(req): Json<ConnectPeerRequest>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Jobs,
        Crud::CREATE,
    ) {
        return status.into_response();
    }

    let Ok(node_uuid) = uuid::Uuid::parse_str(&node_id_str) else {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    };

    let Ok(address) = req.address.parse::<SocketAddr>() else {
        return (axum::http::StatusCode::BAD_REQUEST, "Invalid address format").into_response();
    };

    if let Some(mesh) = &state.mesh_network {
        let node_id = NodeId::from_uuid(node_uuid);

        match mesh.connect_peer(node_id, address).await {
            Ok(()) => {
                tracing::info!("Successfully connected to peer {} at {}", node_id, address);
                Json(serde_json::json!({"success": true, "message": "Connected to peer"})).into_response()
            }
            Err(e) => {
                tracing::error!("Failed to connect to peer {}: {}", node_id, e);
                (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    } else {
        (axum::http::StatusCode::SERVICE_UNAVAILABLE, "Mesh network not enabled").into_response()
    }
}

/// Disconnect from a specific peer
async fn mesh_disconnect_peer_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Path(node_id_str): axum::extract::Path<String>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Jobs,
        Crud::DELETE,
    ) {
        return status.into_response();
    }

    let Ok(node_uuid) = uuid::Uuid::parse_str(&node_id_str) else {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    };

    if let Some(mesh) = &state.mesh_network {
        let node_id = NodeId::from_uuid(node_uuid);

        match mesh.disconnect_peer(node_id).await {
            Ok(()) => {
                tracing::info!("Successfully disconnected from peer {}", node_id);
                Json(serde_json::json!({"success": true, "message": "Disconnected from peer"})).into_response()
            }
            Err(e) => {
                tracing::error!("Failed to disconnect from peer {}: {}", node_id, e);
                (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    } else {
        (axum::http::StatusCode::SERVICE_UNAVAILABLE, "Mesh network not enabled").into_response()
    }
}

/// Get mesh economics statistics
async fn mesh_economics_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Jobs,
        Crud::READ,
    ) {
        return status.into_response();
    }

    if let Some(mesh) = &state.mesh_network {
        match mesh.get_economic_stats().await {
            Ok(stats) => {
                let response = MeshEconomicsResponse {
                    total_distributed: stats.total_distributed.to_string(),
                    total_bandwidth_gb: stats.total_bandwidth_gb,
                    active_contributors: stats.active_contributors,
                    avg_earnings_per_node: stats.avg_earnings_per_node.to_string(),
                    last_settlement: stats.last_settlement.map(|t| {
                        chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339()
                    }),
                };
                Json(response).into_response()
            }
            Err(e) => {
                tracing::error!("Failed to get economic stats: {}", e);
                (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    } else {
        (axum::http::StatusCode::SERVICE_UNAVAILABLE, "Mesh network not enabled").into_response()
    }
}

/// Trigger economic settlement
async fn mesh_settle_handler(
    auth: AuthSession,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl IntoResponse {
    let principals = principals_for(&auth);
    if let Err(status) = ensure_permission(
        state.permissions,
        auth.org_id,
        &principals,
        ResourceKind::Jobs,
        Crud::CREATE,
    ) {
        return status.into_response();
    }

    if let Some(mesh) = &state.mesh_network {
        match mesh.settle_economics().await {
            Ok(()) => {
                tracing::info!("Economic settlement triggered successfully");
                Json(serde_json::json!({"success": true, "message": "Settlement initiated"})).into_response()
            }
            Err(e) => {
                tracing::error!("Failed to trigger settlement: {}", e);
                (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    } else {
        (axum::http::StatusCode::SERVICE_UNAVAILABLE, "Mesh network not enabled").into_response()
    }
}
