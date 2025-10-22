mod room;
mod session;
mod media;
mod subscriptions;
mod functions;
mod transaction;
pub mod database_triggers;
pub mod conditional_handlers;
pub mod transaction_notifications;
pub mod realtime_streaming;
pub mod handlers;

pub use room::*;
pub use session::*;
pub use media::*;
pub use subscriptions::*;
pub use functions::*;
pub use transaction::*;
pub use database_triggers::*;
pub use conditional_handlers::*;
pub use transaction_notifications::*;
pub use realtime_streaming::*;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// WebRTC API imports - real webrtc crate integration
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::setting_engine::SettingEngine;
use webrtc::api::APIBuilder;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::api::API as WebRTCAPI;

/// WebRTC API wrapper for session management
pub struct API {
    pub webrtc_api: Arc<WebRTCAPI>,
    pub ice_servers: Vec<RTCIceServer>,
}

impl API {
    /// Create a new WebRTC API with proper media engine and settings
    pub async fn new(ice_servers: Vec<RTCIceServer>) -> Result<Self, WebRTCError> {
        // Create media engine with codec support
        let mut media_engine = MediaEngine::default();

        // Register default codecs (H.264, VP8, VP9, Opus, etc.)
        if let Err(e) = media_engine.register_default_codecs() {
            return Err(WebRTCError::InternalError(format!("Failed to register codecs: {}", e)));
        }

        // Create interceptor registry for handling RTCP, NACK, etc.
        let mut registry = Registry::new();

        // Register default interceptors (NACK, PLI, etc.)
        registry = register_default_interceptors(registry, &mut media_engine)
            .map_err(|e| WebRTCError::InternalError(format!("Failed to register interceptors: {}", e)))?;

        // Create setting engine for connection configuration
        let mut setting_engine = SettingEngine::default();

        // Configure setting engine for better performance
        setting_engine.detach_data_channels();
        setting_engine.set_ice_timeouts(
            Some(std::time::Duration::from_secs(30)), // Disconnect timeout
            Some(std::time::Duration::from_secs(5)),  // Failed timeout
            Some(std::time::Duration::from_secs(1)),  // Keepalive interval
        );

        // Build the WebRTC API
        let webrtc_api = APIBuilder::new()
            .with_media_engine(media_engine)
            .with_interceptor_registry(registry)
            .with_setting_engine(setting_engine)
            .build();

        Ok(Self {
            webrtc_api: Arc::new(webrtc_api),
            ice_servers,
        })
    }

    /// Create a new peer connection with the configured ICE servers
    pub async fn create_peer_connection(&self) -> Result<Arc<webrtc::peer_connection::RTCPeerConnection>, WebRTCError> {
        let config = RTCConfiguration {
            ice_servers: self.ice_servers.clone(),
            ..Default::default()
        };

        let peer_connection = self.webrtc_api.new_peer_connection(config).await
            .map_err(|e| WebRTCError::InternalError(format!("Failed to create peer connection: {}", e)))?;

        Ok(Arc::new(peer_connection))
    }

    /// Get default ICE servers (Google STUN servers)
    pub fn default_ice_servers() -> Vec<RTCIceServer> {
        vec![
            RTCIceServer {
                urls: vec![
                    "stun:stun.l.google.com:19302".to_string(),
                    "stun:stun1.l.google.com:19302".to_string(),
                ],
                ..Default::default()
            }
        ]
    }

    /// Parse ICE servers from environment variable
    pub fn parse_ice_servers_from_env() -> Vec<RTCIceServer> {
        match std::env::var("ZWEBRTC_ICE_SERVERS") {
            Ok(ice_servers_json) => {
                match serde_json::from_str::<Vec<IceServerConfig>>(&ice_servers_json) {
                    Ok(configs) => {
                        configs.into_iter().map(|config| config.into()).collect()
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "Failed to parse ZWEBRTC_ICE_SERVERS, using defaults"
                        );
                        Self::default_ice_servers()
                    }
                }
            }
            Err(_) => {
                tracing::debug!("ZWEBRTC_ICE_SERVERS not set, using default Google STUN servers");
                Self::default_ice_servers()
            }
        }
    }
}

/// ICE server configuration for environment variable parsing
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct IceServerConfig {
    pub urls: Vec<String>,
    pub username: Option<String>,
    pub credential: Option<String>,
}

impl From<IceServerConfig> for RTCIceServer {
    fn from(config: IceServerConfig) -> Self {
        RTCIceServer {
            urls: config.urls,
            username: config.username.unwrap_or_default(),
            credential: config.credential.unwrap_or_default(),
            credential_type: Default::default(),
        }
    }
}

/// Configuration for WebRTC features
#[derive(Debug, Clone)]
pub struct WebRTCConfig {
    pub enable_video: bool,
    pub enable_audio: bool,
    pub max_participants_per_room: usize,
    pub max_rooms_per_org: usize,
    pub max_subscriptions_per_user: usize,
    pub allow_function_triggers: bool,
    pub media_relay_enabled: bool,
    pub enable_transactions: bool,
    pub max_transactions_per_session: usize,
    pub transaction_timeout_ms: u64,
}

impl Default for WebRTCConfig {
    fn default() -> Self {
        Self {
            enable_video: true,
            enable_audio: true,
            max_participants_per_room: 50,
            max_rooms_per_org: 100,
            max_subscriptions_per_user: 10,
            allow_function_triggers: true,
            media_relay_enabled: true,
            enable_transactions: true,
            max_transactions_per_session: 10,
            transaction_timeout_ms: 300_000, // 5 minutes default
        }
    }
}

/// Enhanced WebRTC state for multi-participant sessions
#[derive(Clone)]
pub struct WebRTCState {
    pub api: Arc<API>,
    pub config: WebRTCConfig,
    pub rooms: Arc<RwLock<HashMap<String, Arc<WebRTCRoom>>>>,
    pub sessions: Arc<RwLock<HashMap<String, Arc<WebRTCSession>>>>,
    pub subscriptions: Arc<RwLock<HashMap<String, Vec<DataSubscription>>>>,
    pub function_registry: Arc<RwLock<HashMap<String, FunctionTrigger>>>,
    pub transaction_manager: Arc<TransactionManager>,
    pub database_trigger_manager: Arc<DatabaseTriggerManager>,
    pub conditional_handler_manager: Arc<ConditionalHandlerManager>,
    pub realtime_streaming_manager: Arc<RealtimeStreamingManager>,
}

impl WebRTCState {
    pub async fn new(
        api: Arc<API>,
        config: WebRTCConfig,
        function_trigger_manager: Arc<FunctionTriggerManager>,
        event_system: Option<Arc<zevents::EventSystem>>,
        store: Arc<zcore_storage::Store>,
        catalog_store: &'static zcore_storage::Store,
    ) -> Self {
        Self {
            api,
            config,
            rooms: Arc::new(RwLock::new(HashMap::new())),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            function_registry: Arc::new(RwLock::new(HashMap::new())),
            transaction_manager: Arc::new(TransactionManager::new(
                event_system,
                store,
                catalog_store,
            )),
            database_trigger_manager: Arc::new(DatabaseTriggerManager::new(function_trigger_manager.clone())),
            conditional_handler_manager: Arc::new(ConditionalHandlerManager::new(function_trigger_manager)),
            realtime_streaming_manager: Arc::new(RealtimeStreamingManager::new(1000)), // Buffer up to 1000 messages per channel
        }
    }

    /// Create WebRTC state without event system integration (for current server setup)
    pub async fn new_simple(
        api: Arc<API>,
        config: WebRTCConfig,
        function_trigger_manager: Arc<FunctionTriggerManager>,
        store: Arc<zcore_storage::Store>,
        catalog_store: &'static zcore_storage::Store,
    ) -> Self {
        Self {
            api,
            config,
            rooms: Arc::new(RwLock::new(HashMap::new())),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            function_registry: Arc::new(RwLock::new(HashMap::new())),
            transaction_manager: Arc::new(TransactionManager::new_simple(
                store,
                catalog_store,
            )),
            database_trigger_manager: Arc::new(DatabaseTriggerManager::new(function_trigger_manager.clone())),
            conditional_handler_manager: Arc::new(ConditionalHandlerManager::new(function_trigger_manager)),
            realtime_streaming_manager: Arc::new(RealtimeStreamingManager::new(1000)), // Buffer up to 1000 messages per channel
        }
    }

    pub async fn get_or_create_room(&self, room_id: &str, org_id: u64) -> Result<Arc<WebRTCRoom>, WebRTCError> {
        let mut rooms = self.rooms.write().await;

        if let Some(room) = rooms.get(room_id) {
            return Ok(room.clone());
        }

        // Check room limits for organization
        let org_room_count = rooms.values()
            .filter(|room| room.org_id == org_id)
            .count();

        if org_room_count >= self.config.max_rooms_per_org {
            return Err(WebRTCError::RoomLimitExceeded);
        }

        let room = Arc::new(WebRTCRoom::new(room_id.to_string(), org_id, self.config.clone()));
        rooms.insert(room_id.to_string(), room.clone());

        Ok(room)
    }

    pub async fn remove_room(&self, room_id: &str) {
        self.rooms.write().await.remove(room_id);
    }

    pub async fn get_session(&self, session_id: &str) -> Option<Arc<WebRTCSession>> {
        self.sessions.read().await.get(session_id).cloned()
    }

    pub async fn add_session(&self, session: Arc<WebRTCSession>) {
        self.sessions.write().await.insert(session.id.clone(), session);
    }

    pub async fn remove_session(&self, session_id: &str) {
        self.sessions.write().await.remove(session_id);
    }

    pub async fn cleanup_expired_sessions(&self) {
        let mut sessions = self.sessions.write().await;
        let now = chrono::Utc::now();

        sessions.retain(|_, session| {
            let elapsed = now.signed_duration_since(session.created_at);
            elapsed.num_hours() < 24 // Remove sessions older than 24 hours
        });
    }
}

/// WebRTC error types
#[derive(Debug, thiserror::Error)]
pub enum WebRTCError {
    #[error("Room limit exceeded")]
    RoomLimitExceeded,
    #[error("Participant limit exceeded")]
    ParticipantLimitExceeded,
    #[error("Session not found")]
    SessionNotFound,
    #[error("Room not found")]
    RoomNotFound,
    #[error("Permission denied")]
    PermissionDenied,
    #[error("Invalid media configuration")]
    InvalidMediaConfig,
    #[error("Function trigger failed: {0}")]
    FunctionTriggerFailed(String),
    #[error("Subscription limit exceeded")]
    SubscriptionLimitExceeded,
    #[error("WebRTC internal error: {0}")]
    InternalError(String),
}

impl axum::response::IntoResponse for WebRTCError {
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;
        use axum::Json;

        let (status, message) = match self {
            WebRTCError::RoomLimitExceeded | WebRTCError::ParticipantLimitExceeded |
            WebRTCError::SubscriptionLimitExceeded => (StatusCode::TOO_MANY_REQUESTS, self.to_string()),
            WebRTCError::SessionNotFound | WebRTCError::RoomNotFound => (StatusCode::NOT_FOUND, self.to_string()),
            WebRTCError::PermissionDenied => (StatusCode::FORBIDDEN, self.to_string()),
            WebRTCError::InvalidMediaConfig => (StatusCode::BAD_REQUEST, self.to_string()),
            WebRTCError::FunctionTriggerFailed(_) | WebRTCError::InternalError(_) =>
                (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };

        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}