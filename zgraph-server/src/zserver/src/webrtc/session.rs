use super::{MediaState, ParticipantPermissions, ParticipantRole, WebRTCError};
use super::transaction::{TransactionManager, TransactionMessage, TransactionResponse, codec};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use webrtc::data_channel::data_channel_state::RTCDataChannelState;
use webrtc::data_channel::RTCDataChannel;
use webrtc::peer_connection::RTCPeerConnection;
use bytes::Bytes;

/// Enhanced WebRTC session with media and permission support
pub struct WebRTCSession {
    pub id: String,
    pub user_id: String,
    pub org_id: u64,
    pub project_id: Option<String>,
    pub display_name: Option<String>,
    pub role: ParticipantRole,
    pub permissions: ParticipantPermissions,
    pub peer_connection: Arc<RTCPeerConnection>,
    pub data_channels: Arc<RwLock<HashMap<String, Arc<RTCDataChannel>>>>,
    pub media_state: Arc<RwLock<MediaState>>,
    pub subscriptions: Arc<RwLock<Vec<String>>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_activity: Arc<RwLock<chrono::DateTime<chrono::Utc>>>,
    pub metadata: Arc<RwLock<SessionMetadata>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMetadata {
    pub client_info: Option<ClientInfo>,
    pub connection_quality: ConnectionQuality,
    pub bandwidth_usage: BandwidthUsage,
    pub feature_flags: HashMap<String, bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientInfo {
    pub user_agent: String,
    pub platform: String,
    pub version: String,
    pub features: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionQuality {
    pub score: f32, // 0.0 to 1.0
    pub latency_ms: Option<u32>,
    pub packet_loss: Option<f32>,
    pub jitter_ms: Option<u32>,
    pub bandwidth_mbps: Option<f32>,
}

impl Default for ConnectionQuality {
    fn default() -> Self {
        Self {
            score: 1.0,
            latency_ms: None,
            packet_loss: None,
            jitter_ms: None,
            bandwidth_mbps: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BandwidthUsage {
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub packets_sent: u64,
    pub packets_received: u64,
    pub last_updated: chrono::DateTime<chrono::Utc>,
}

impl Default for BandwidthUsage {
    fn default() -> Self {
        Self {
            bytes_sent: 0,
            bytes_received: 0,
            packets_sent: 0,
            packets_received: 0,
            last_updated: chrono::Utc::now(),
        }
    }
}

impl Default for SessionMetadata {
    fn default() -> Self {
        Self {
            client_info: None,
            connection_quality: ConnectionQuality::default(),
            bandwidth_usage: BandwidthUsage::default(),
            feature_flags: HashMap::new(),
        }
    }
}

impl WebRTCSession {
    pub fn new(
        user_id: String,
        org_id: u64,
        project_id: Option<String>,
        peer_connection: Arc<RTCPeerConnection>,
        role: ParticipantRole,
        permissions: ParticipantPermissions,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            user_id,
            org_id,
            project_id,
            display_name: None,
            role,
            permissions,
            peer_connection,
            data_channels: Arc::new(RwLock::new(HashMap::new())),
            media_state: Arc::new(RwLock::new(MediaState::default())),
            subscriptions: Arc::new(RwLock::new(Vec::new())),
            created_at: chrono::Utc::now(),
            last_activity: Arc::new(RwLock::new(chrono::Utc::now())),
            metadata: Arc::new(RwLock::new(SessionMetadata::default())),
        }
    }

    /// Create a new WebRTC session with properly configured peer connection
    pub async fn create_with_api(
        api: &super::API,
        user_id: String,
        org_id: u64,
        project_id: Option<String>,
        role: ParticipantRole,
        permissions: ParticipantPermissions,
    ) -> Result<Arc<Self>, WebRTCError> {
        // Create peer connection using the WebRTC API
        let peer_connection = api.create_peer_connection().await?;

        // Create the session
        let session = Arc::new(Self::new(
            user_id.clone(),
            org_id,
            project_id,
            peer_connection.clone(),
            role,
            permissions,
        ));

        // Set up peer connection event handlers
        let session_clone = Arc::downgrade(&session);
        let user_id_clone = user_id.clone();
        peer_connection.on_peer_connection_state_change(Box::new(move |state| {
            let session_weak = session_clone.clone();
            let user_id = user_id_clone.clone();
            Box::pin(async move {
                if let Some(session) = session_weak.upgrade() {
                    tracing::info!(
                        session_id = %session.id,
                        user_id = %user_id,
                        state = ?state,
                        "WebRTC peer connection state changed"
                    );

                    match state {
                        webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState::Connected => {
                            session.update_activity().await;
                            tracing::info!(
                                session_id = %session.id,
                                user_id = %user_id,
                                "WebRTC peer connection established"
                            );
                        }
                        webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState::Disconnected |
                        webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState::Failed => {
                            tracing::warn!(
                                session_id = %session.id,
                                user_id = %user_id,
                                state = ?state,
                                "WebRTC peer connection lost"
                            );
                        }
                        _ => {}
                    }
                }
            })
        }));

        // Set up ICE connection state change handler
        let session_clone = Arc::downgrade(&session);
        let user_id_clone = user_id.clone();
        peer_connection.on_ice_connection_state_change(Box::new(move |state| {
            let session_weak = session_clone.clone();
            let user_id = user_id_clone.clone();
            Box::pin(async move {
                if let Some(session) = session_weak.upgrade() {
                    tracing::debug!(
                        session_id = %session.id,
                        user_id = %user_id,
                        ice_state = ?state,
                        "ICE connection state changed"
                    );

                    match state {
                        webrtc::ice_transport::ice_connection_state::RTCIceConnectionState::Connected |
                        webrtc::ice_transport::ice_connection_state::RTCIceConnectionState::Completed => {
                            session.update_activity().await;
                        }
                        _ => {}
                    }
                }
            })
        }));

        // Set up data channel handler for when remote peer creates channels
        let session_clone = Arc::downgrade(&session);
        let user_id_clone = user_id;
        peer_connection.on_data_channel(Box::new(move |data_channel| {
            let session_weak = session_clone.clone();
            let user_id = user_id_clone.clone();
            let channel_label = data_channel.label().to_string();

            Box::pin(async move {
                if let Some(session) = session_weak.upgrade() {
                    tracing::info!(
                        session_id = %session.id,
                        user_id = %user_id,
                        channel_label = %channel_label,
                        "Remote peer created data channel"
                    );

                    // Store the data channel
                    session.add_data_channel(&channel_label, data_channel.clone()).await;

                    // Set up message handler for this channel
                    let session_weak_inner = Arc::downgrade(&session);
                    data_channel.on_message(Box::new(move |msg| {
                        let session_weak = session_weak_inner.clone();
                        let channel_label = channel_label.clone();
                        Box::pin(async move {
                            if let Some(session) = session_weak.upgrade() {
                                tracing::debug!(
                                    session_id = %session.id,
                                    channel = %channel_label,
                                    data_len = msg.data.len(),
                                    is_string = msg.is_string,
                                    "Received message on remote data channel"
                                );
                                session.update_activity().await;
                            }
                        })
                    }));
                }
            })
        }));

        tracing::info!(
            session_id = %session.id,
            user_id = %session.user_id,
            org_id = session.org_id,
            "Created WebRTC session with peer connection"
        );

        Ok(session)
    }

    /// Create an SDP offer for the peer connection
    pub async fn create_offer(&self) -> Result<String, WebRTCError> {
        let offer = self.peer_connection.create_offer(None).await
            .map_err(|e| WebRTCError::InternalError(format!("Failed to create offer: {}", e)))?;

        // Set the local description
        self.peer_connection.set_local_description(offer.clone()).await
            .map_err(|e| WebRTCError::InternalError(format!("Failed to set local description: {}", e)))?;

        let offer_sdp = offer.sdp;

        tracing::info!(
            session_id = %self.id,
            user_id = %self.user_id,
            "Created SDP offer"
        );

        self.update_activity().await;
        Ok(offer_sdp)
    }

    /// Set the remote SDP answer
    pub async fn set_answer(&self, answer_sdp: &str) -> Result<(), WebRTCError> {
        use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

        let answer = RTCSessionDescription::answer(answer_sdp.to_string())
            .map_err(|e| WebRTCError::InternalError(format!("Invalid SDP answer: {}", e)))?;

        self.peer_connection.set_remote_description(answer).await
            .map_err(|e| WebRTCError::InternalError(format!("Failed to set remote description: {}", e)))?;

        tracing::info!(
            session_id = %self.id,
            user_id = %self.user_id,
            "Set SDP answer"
        );

        self.update_activity().await;
        Ok(())
    }

    /// Handle a remote SDP offer and create an answer
    pub async fn handle_offer(&self, offer_sdp: &str) -> Result<String, WebRTCError> {
        use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

        // Set the remote offer
        let offer = RTCSessionDescription::offer(offer_sdp.to_string())
            .map_err(|e| WebRTCError::InternalError(format!("Invalid SDP offer: {}", e)))?;

        self.peer_connection.set_remote_description(offer).await
            .map_err(|e| WebRTCError::InternalError(format!("Failed to set remote description: {}", e)))?;

        // Create an answer
        let answer = self.peer_connection.create_answer(None).await
            .map_err(|e| WebRTCError::InternalError(format!("Failed to create answer: {}", e)))?;

        // Set the local description
        self.peer_connection.set_local_description(answer.clone()).await
            .map_err(|e| WebRTCError::InternalError(format!("Failed to set local description: {}", e)))?;

        let answer_sdp = answer.sdp;

        tracing::info!(
            session_id = %self.id,
            user_id = %self.user_id,
            "Handled SDP offer and created answer"
        );

        self.update_activity().await;
        Ok(answer_sdp)
    }

    /// Add an ICE candidate
    pub async fn add_ice_candidate(&self, candidate: &str, sdp_mid: Option<&str>, sdp_mline_index: Option<u16>) -> Result<(), WebRTCError> {
        use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;

        let ice_candidate = RTCIceCandidateInit {
            candidate: candidate.to_string(),
            sdp_mid: sdp_mid.map(|s| s.to_string()),
            sdp_mline_index,
            username_fragment: None,
        };

        self.peer_connection.add_ice_candidate(ice_candidate).await
            .map_err(|e| WebRTCError::InternalError(format!("Failed to add ICE candidate: {}", e)))?;

        tracing::debug!(
            session_id = %self.id,
            user_id = %self.user_id,
            candidate = %candidate,
            "Added ICE candidate"
        );

        self.update_activity().await;
        Ok(())
    }

    /// Get the current connection state
    pub fn get_connection_state(&self) -> webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState {
        self.peer_connection.connection_state()
    }

    /// Get the current ICE connection state
    pub fn get_ice_connection_state(&self) -> webrtc::ice_transport::ice_connection_state::RTCIceConnectionState {
        self.peer_connection.ice_connection_state()
    }

    /// Check if the peer connection is in a connected state
    pub fn is_connected(&self) -> bool {
        matches!(
            self.peer_connection.connection_state(),
            webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState::Connected
        )
    }

    /// Close the peer connection
    pub async fn close(&self) -> Result<(), WebRTCError> {
        self.peer_connection.close().await
            .map_err(|e| WebRTCError::InternalError(format!("Failed to close peer connection: {}", e)))?;

        tracing::info!(
            session_id = %self.id,
            user_id = %self.user_id,
            "Closed WebRTC peer connection"
        );

        Ok(())
    }

    pub async fn update_activity(&self) {
        *self.last_activity.write().await = chrono::Utc::now();
    }

    pub async fn add_data_channel(&self, name: &str, channel: Arc<RTCDataChannel>) {
        self.data_channels.write().await.insert(name.to_string(), channel);
    }

    pub async fn get_data_channel(&self, name: &str) -> Option<Arc<RTCDataChannel>> {
        self.data_channels.read().await.get(name).cloned()
    }

    /// Send a text message through a specific data channel
    pub async fn send_data_channel_message(&self, channel_name: &str, message: &str) -> Result<(), WebRTCError> {
        let channel = self.get_data_channel(channel_name).await
            .ok_or_else(|| WebRTCError::InternalError(format!("Data channel '{}' not found", channel_name)))?;

        match channel.ready_state() {
            RTCDataChannelState::Open => {
                channel.send_text(message).await
                    .map_err(|e| WebRTCError::InternalError(format!("Failed to send text message to channel '{}': {}", channel_name, e)))?;

                tracing::debug!(
                    session_id = %self.id,
                    channel = channel_name,
                    message_len = message.len(),
                    "Sent text message through data channel"
                );

                self.update_activity().await;
                Ok(())
            }
            state => {
                let error_msg = format!("Data channel '{}' is not open (state: {:?})", channel_name, state);
                tracing::warn!(
                    session_id = %self.id,
                    channel = channel_name,
                    state = ?state,
                    "Cannot send message - data channel not open"
                );
                Err(WebRTCError::InternalError(error_msg))
            }
        }
    }

    /// Send binary data through a specific data channel
    pub async fn send_data_channel_binary(&self, channel_name: &str, data: &[u8]) -> Result<(), WebRTCError> {
        let channel = self.get_data_channel(channel_name).await
            .ok_or_else(|| WebRTCError::InternalError(format!("Data channel '{}' not found", channel_name)))?;

        match channel.ready_state() {
            RTCDataChannelState::Open => {
                let bytes = Bytes::from(data.to_vec());
                channel.send(&bytes).await
                    .map_err(|e| WebRTCError::InternalError(format!("Failed to send binary data to channel '{}': {}", channel_name, e)))?;

                tracing::debug!(
                    session_id = %self.id,
                    channel = channel_name,
                    data_len = data.len(),
                    "Sent binary data through data channel"
                );

                self.update_activity().await;
                Ok(())
            }
            state => {
                let error_msg = format!("Data channel '{}' is not open (state: {:?})", channel_name, state);
                tracing::warn!(
                    session_id = %self.id,
                    channel = channel_name,
                    state = ?state,
                    "Cannot send binary data - data channel not open"
                );
                Err(WebRTCError::InternalError(error_msg))
            }
        }
    }

    /// Send a text message through the default data channel (creates one if none exists)
    pub async fn send_text(&self, message: &str) -> Result<(), WebRTCError> {
        const DEFAULT_CHANNEL_NAME: &str = "default";

        // Check if default channel exists, create if not
        if self.get_data_channel(DEFAULT_CHANNEL_NAME).await.is_none() {
            self.create_default_data_channel().await?;
        }

        self.send_data_channel_message(DEFAULT_CHANNEL_NAME, message).await
    }

    /// Send binary data through the default data channel
    pub async fn send_binary(&self, data: &[u8]) -> Result<(), WebRTCError> {
        const DEFAULT_CHANNEL_NAME: &str = "default";

        // Check if default channel exists, create if not
        if self.get_data_channel(DEFAULT_CHANNEL_NAME).await.is_none() {
            self.create_default_data_channel().await?;
        }

        self.send_data_channel_binary(DEFAULT_CHANNEL_NAME, data).await
    }

    /// Create the default data channel if it doesn't exist
    pub async fn create_default_data_channel(&self) -> Result<(), WebRTCError> {
        const DEFAULT_CHANNEL_NAME: &str = "default";

        // Check if already exists
        if self.get_data_channel(DEFAULT_CHANNEL_NAME).await.is_some() {
            return Ok(());
        }

        // Create data channel
        let raw_data_channel = self.peer_connection
            .create_data_channel(DEFAULT_CHANNEL_NAME, None)
            .await
            .map_err(|e| WebRTCError::InternalError(format!("Failed to create default data channel: {}", e)))?;

        let data_channel = raw_data_channel;

        // Set up message handlers
        let session_id = self.id.clone();
        data_channel.on_open(Box::new(move || {
            let session_id = session_id.clone();
            Box::pin(async move {
                tracing::info!(
                    session_id = %session_id,
                    channel = DEFAULT_CHANNEL_NAME,
                    "Default data channel opened"
                );
            })
        }));

        let session_id = self.id.clone();
        data_channel.on_close(Box::new(move || {
            let session_id = session_id.clone();
            Box::pin(async move {
                tracing::info!(
                    session_id = %session_id,
                    channel = DEFAULT_CHANNEL_NAME,
                    "Default data channel closed"
                );
            })
        }));

        let session_id = self.id.clone();
        data_channel.on_message(Box::new(move |msg| {
            let session_id = session_id.clone();
            Box::pin(async move {
                tracing::debug!(
                    session_id = %session_id,
                    channel = DEFAULT_CHANNEL_NAME,
                    data_len = msg.data.len(),
                    is_string = msg.is_string,
                    "Received message on default data channel"
                );
            })
        }));

        // Store the data channel
        self.add_data_channel(DEFAULT_CHANNEL_NAME, data_channel).await;

        tracing::info!(
            session_id = %self.id,
            channel = DEFAULT_CHANNEL_NAME,
            "Created default data channel"
        );

        Ok(())
    }

    /// Get all data channel names and their states
    pub async fn get_data_channel_states(&self) -> HashMap<String, RTCDataChannelState> {
        let mut states = HashMap::new();
        let channels = self.data_channels.read().await;

        for (name, channel) in channels.iter() {
            states.insert(name.clone(), channel.ready_state());
        }

        states
    }

    /// Check if a specific data channel is ready for sending
    pub async fn is_data_channel_ready(&self, channel_name: &str) -> bool {
        if let Some(channel) = self.get_data_channel(channel_name).await {
            matches!(channel.ready_state(), RTCDataChannelState::Open)
        } else {
            false
        }
    }

    /// Broadcast a text message to all open data channels
    pub async fn broadcast_text(&self, message: &str) -> Result<Vec<String>, WebRTCError> {
        let channels = self.data_channels.read().await;
        let mut successful_channels = Vec::new();
        let mut errors = Vec::new();

        for (name, channel) in channels.iter() {
            if matches!(channel.ready_state(), RTCDataChannelState::Open) {
                match channel.send_text(message).await {
                    Ok(_) => {
                        successful_channels.push(name.clone());
                        tracing::debug!(
                            session_id = %self.id,
                            channel = name,
                            message_len = message.len(),
                            "Broadcast text message to data channel"
                        );
                    }
                    Err(e) => {
                        errors.push(format!("Channel '{}': {}", name, e));
                        tracing::warn!(
                            session_id = %self.id,
                            channel = name,
                            error = %e,
                            "Failed to broadcast to data channel"
                        );
                    }
                }
            }
        }

        if successful_channels.is_empty() && !errors.is_empty() {
            return Err(WebRTCError::InternalError(format!("Failed to broadcast to any channels: {}", errors.join(", "))));
        }

        if !successful_channels.is_empty() {
            self.update_activity().await;
        }

        Ok(successful_channels)
    }

    pub async fn add_subscription(&self, subscription: String) -> Result<(), WebRTCError> {
        let mut subs = self.subscriptions.write().await;

        // Check subscription limit
        if subs.len() >= 10 { // Configurable limit
            return Err(WebRTCError::SubscriptionLimitExceeded);
        }

        if !subs.contains(&subscription) {
            subs.push(subscription);
        }
        Ok(())
    }

    pub async fn remove_subscription(&self, subscription: &str) {
        self.subscriptions.write().await.retain(|s| s != subscription);
    }

    pub async fn get_subscriptions(&self) -> Vec<String> {
        self.subscriptions.read().await.clone()
    }

    pub async fn update_media_state(&self, new_state: MediaState) {
        *self.media_state.write().await = new_state;
        self.update_activity().await;
    }

    pub async fn get_media_state(&self) -> MediaState {
        self.media_state.read().await.clone()
    }

    pub async fn update_metadata(&self, metadata: SessionMetadata) {
        *self.metadata.write().await = metadata;
    }

    pub async fn get_metadata(&self) -> SessionMetadata {
        self.metadata.read().await.clone()
    }

    pub async fn update_connection_quality(&self, quality: ConnectionQuality) {
        self.metadata.write().await.connection_quality = quality;
    }

    pub async fn update_bandwidth_usage(&self, usage: BandwidthUsage) {
        self.metadata.write().await.bandwidth_usage = usage;
    }

    pub async fn set_feature_flag(&self, flag: &str, enabled: bool) {
        self.metadata.write().await.feature_flags.insert(flag.to_string(), enabled);
    }

    pub async fn get_feature_flag(&self, flag: &str) -> bool {
        self.metadata.read().await.feature_flags.get(flag).copied().unwrap_or(false)
    }

    pub async fn is_active(&self, timeout_minutes: i64) -> bool {
        let last_activity = *self.last_activity.read().await;
        let now = chrono::Utc::now();
        let duration = now.signed_duration_since(last_activity);
        duration.num_minutes() < timeout_minutes
    }

    pub async fn has_permission(&self, permission: &str) -> bool {
        match permission {
            "speak" => self.permissions.can_speak,
            "share_screen" => self.permissions.can_share_screen,
            "record" => self.permissions.can_record,
            "trigger_functions" => self.permissions.can_trigger_functions,
            "modify_room" => self.permissions.can_modify_room,
            "kick_participants" => self.permissions.can_kick_participants,
            _ => false,
        }
    }

    pub async fn get_session_info(&self) -> SessionInfo {
        let media_state = self.get_media_state().await;
        let metadata = self.get_metadata().await;
        let subscriptions = self.get_subscriptions().await;

        SessionInfo {
            id: self.id.clone(),
            user_id: self.user_id.clone(),
            org_id: self.org_id,
            project_id: self.project_id.clone(),
            display_name: self.display_name.clone(),
            role: self.role.clone(),
            permissions: self.permissions.clone(),
            media_state,
            subscriptions,
            created_at: self.created_at,
            last_activity: *self.last_activity.read().await,
            connection_quality: metadata.connection_quality,
            bandwidth_usage: metadata.bandwidth_usage,
        }
    }

    /// Transaction Data Channel Management

    /// Create dedicated transaction data channel
    pub async fn create_transaction_data_channel(&self) -> Result<(), WebRTCError> {
        const TRANSACTION_CHANNEL_NAME: &str = "transactions";

        // Check if already exists
        if self.get_data_channel(TRANSACTION_CHANNEL_NAME).await.is_some() {
            return Ok(());
        }

        // Create transaction-specific data channel
        let raw_data_channel = self.peer_connection
            .create_data_channel(TRANSACTION_CHANNEL_NAME, None)
            .await
            .map_err(|e| WebRTCError::InternalError(format!("Failed to create transaction data channel: {}", e)))?;

        let data_channel = raw_data_channel;

        // Set up transaction message handlers
        let session_id = self.id.clone();
        data_channel.on_open(Box::new(move || {
            let session_id = session_id.clone();
            Box::pin(async move {
                tracing::info!(
                    session_id = %session_id,
                    channel = TRANSACTION_CHANNEL_NAME,
                    "Transaction data channel opened and ready for binary protocol"
                );
            })
        }));

        let session_id = self.id.clone();
        data_channel.on_close(Box::new(move || {
            let session_id = session_id.clone();
            Box::pin(async move {
                tracing::info!(
                    session_id = %session_id,
                    channel = TRANSACTION_CHANNEL_NAME,
                    "Transaction data channel closed"
                );
            })
        }));

        // Set up binary message handler for transaction protocol
        let session_id = self.id.clone();
        let user_id = self.user_id.clone();
        let org_id = self.org_id;
        data_channel.on_message(Box::new(move |msg| {
            let session_id = session_id.clone();
            let user_id = user_id.clone();
            Box::pin(async move {
                tracing::debug!(
                    session_id = %session_id,
                    channel = TRANSACTION_CHANNEL_NAME,
                    data_len = msg.data.len(),
                    is_string = msg.is_string,
                    "Received transaction message"
                );

                // Handle binary transaction messages
                if !msg.is_string {
                    match codec::parse_frame(&msg.data) {
                        Ok(codec::FrameType::Message(transaction_msg)) => {
                            tracing::info!(
                                session_id = %session_id,
                                message_type = ?std::mem::discriminant(&transaction_msg),
                                "Parsed transaction message, routing to handler"
                            );

                            // Transaction message routing is now handled by setup_transaction_message_routing
                            // in handlers.rs when the session is created. This message handler is a fallback
                            // that should not normally be called if proper routing is set up.
                            tracing::warn!(
                                session_id = %session_id,
                                message_type = ?std::mem::discriminant(&transaction_msg),
                                "Transaction message received on fallback handler - this indicates routing may not be properly set up"
                            );
                        }
                        Ok(codec::FrameType::Response(_)) => {
                            tracing::warn!(
                                session_id = %session_id,
                                "Received response frame on server channel (unexpected)"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                session_id = %session_id,
                                error = %e,
                                "Failed to parse transaction frame"
                            );
                        }
                    }
                } else {
                    // Handle text messages for debugging/legacy support
                    if let Ok(text) = String::from_utf8(msg.data.to_vec()) {
                        tracing::debug!(
                            session_id = %session_id,
                            message = %text,
                            "Received text message on transaction channel"
                        );
                    }
                }
            })
        }));

        // Store the transaction data channel
        self.add_data_channel(TRANSACTION_CHANNEL_NAME, data_channel).await;

        tracing::info!(
            session_id = %self.id,
            channel = TRANSACTION_CHANNEL_NAME,
            "Created dedicated transaction data channel with binary protocol support"
        );

        Ok(())
    }

    /// Send transaction response through transaction data channel
    pub async fn send_transaction_response(&self, response: &TransactionResponse) -> Result<(), WebRTCError> {
        const TRANSACTION_CHANNEL_NAME: &str = "transactions";

        // Ensure transaction channel exists
        if self.get_data_channel(TRANSACTION_CHANNEL_NAME).await.is_none() {
            self.create_transaction_data_channel().await?;
        }

        // Encode response to binary frame
        let frame = codec::create_response_frame(response)
            .map_err(|e| WebRTCError::InternalError(format!("Failed to encode transaction response: {}", e)))?;

        // Send binary data
        self.send_data_channel_binary(TRANSACTION_CHANNEL_NAME, &frame).await?;

        tracing::debug!(
            session_id = %self.id,
            response_type = ?std::mem::discriminant(response),
            "Sent transaction response through WebRTC data channel"
        );

        Ok(())
    }

    /// Check if transaction data channel is ready
    pub async fn is_transaction_channel_ready(&self) -> bool {
        const TRANSACTION_CHANNEL_NAME: &str = "transactions";
        self.is_data_channel_ready(TRANSACTION_CHANNEL_NAME).await
    }

    /// Get transaction channel status
    pub async fn get_transaction_channel_status(&self) -> Option<RTCDataChannelState> {
        const TRANSACTION_CHANNEL_NAME: &str = "transactions";
        let channel = self.get_data_channel(TRANSACTION_CHANNEL_NAME).await?;
        Some(channel.ready_state())
    }

    /// Setup automatic transaction channel creation
    pub async fn setup_transaction_support(&self, enable: bool) -> Result<(), WebRTCError> {
        if enable {
            // Create transaction data channel
            self.create_transaction_data_channel().await?;

            // Set feature flag
            self.set_feature_flag("transactions_enabled", true).await;

            tracing::info!(
                session_id = %self.id,
                "Transaction support enabled for WebRTC session"
            );
        } else {
            self.set_feature_flag("transactions_enabled", false).await;

            tracing::info!(
                session_id = %self.id,
                "Transaction support disabled for WebRTC session"
            );
        }

        Ok(())
    }

    /// Check if session has transaction support enabled
    pub async fn has_transaction_support(&self) -> bool {
        self.get_feature_flag("transactions_enabled").await
    }

    /// Check if session matches project isolation requirements
    pub fn matches_project(&self, project_id: Option<&str>) -> bool {
        match (&self.project_id, project_id) {
            (None, None) => true,  // Both global
            (Some(a), Some(b)) => a == b,  // Both same project
            _ => false,  // Mismatch between global and project-specific
        }
    }

}

impl std::fmt::Debug for WebRTCSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebRTCSession")
            .field("id", &self.id)
            .field("user_id", &self.user_id)
            .field("org_id", &self.org_id)
            .field("project_id", &self.project_id)
            .field("display_name", &self.display_name)
            .field("role", &self.role)
            .field("permissions", &self.permissions)
            .field("peer_connection", &"<RTCPeerConnection>")
            .field("data_channels", &"<HashMap<String, Arc<RTCDataChannel>>>")
            .field("media_state", &self.media_state)
            .field("subscriptions", &self.subscriptions)
            .field("created_at", &self.created_at)
            .field("last_activity", &self.last_activity)
            .field("metadata", &self.metadata)
            .finish()
    }
}

#[derive(Debug, Serialize)]
pub struct SessionInfo {
    pub id: String,
    pub user_id: String,
    pub org_id: u64,
    pub project_id: Option<String>,
    pub display_name: Option<String>,
    pub role: ParticipantRole,
    pub permissions: ParticipantPermissions,
    pub media_state: MediaState,
    pub subscriptions: Vec<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_activity: chrono::DateTime<chrono::Utc>,
    pub connection_quality: ConnectionQuality,
    pub bandwidth_usage: BandwidthUsage,
}

/// Session creation parameters
#[derive(Debug, Deserialize)]
pub struct CreateSessionRequest {
    pub project_id: Option<String>,
    pub display_name: Option<String>,
    pub role: Option<ParticipantRole>,
    pub permissions: Option<ParticipantPermissions>,
    pub client_info: Option<ClientInfo>,
    pub feature_flags: Option<HashMap<String, bool>>,
}

/// Session update request
#[derive(Debug, Deserialize)]
pub struct UpdateSessionRequest {
    pub display_name: Option<String>,
    pub media_state: Option<MediaState>,
    pub connection_quality: Option<ConnectionQuality>,
    pub feature_flags: Option<HashMap<String, bool>>,
}