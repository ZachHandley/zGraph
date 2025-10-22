use super::*;
use axum::extract::{Path, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use zauth::AuthSession;
use ztransaction::distributed::TwoPhaseCommitMessage;

// WebRTC imports for real peer connection creation
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::setting_engine::SettingEngine;
use webrtc::api::APIBuilder;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;


/// Enhanced WebRTC Room Handlers

#[derive(Debug, Deserialize)]
pub struct CreateRoomRequest {
    pub room_id: Option<String>,
    pub metadata: Option<super::RoomMetadata>,
}

#[derive(Debug, Serialize)]
pub struct RoomResponse {
    pub id: String,
    pub org_id: u64,
    pub participant_count: usize,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub metadata: super::RoomMetadata,
}

pub async fn create_room_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Json(req): Json<CreateRoomRequest>,
) -> Result<Json<RoomResponse>, WebRTCError> {
    // Validate permissions
    crate::ensure_permission(
        state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs, // Use Jobs resource for WebRTC rooms
        zpermissions::Crud::CREATE,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    let room_id = req.room_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let room = state.webrtc_state.get_or_create_room(&room_id, auth.org_id).await?;

    if let Some(metadata) = req.metadata {
        room.update_metadata(metadata).await;
    }

    let response = RoomResponse {
        id: room.id.clone(),
        org_id: room.org_id,
        participant_count: room.participant_count().await,
        created_at: room.created_at,
        metadata: room.get_metadata().await,
    };

    tracing::info!(
        room_id = %room_id,
        user_id = ?auth.user_id,
        org_id = auth.org_id,
        "Created WebRTC room"
    );

    Ok(Json(response))
}

pub async fn list_rooms_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
) -> Result<Json<Vec<RoomResponse>>, WebRTCError> {
    crate::ensure_permission(
        &state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs,
        zpermissions::Crud::READ,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    let rooms = state.webrtc_state.rooms.read().await;
    let mut response = Vec::new();

    for room in rooms.values() {
        if room.org_id == auth.org_id {
            response.push(RoomResponse {
                id: room.id.clone(),
                org_id: room.org_id,
                participant_count: room.participant_count().await,
                created_at: room.created_at,
                metadata: room.get_metadata().await,
            });
        }
    }

    Ok(Json(response))
}

pub async fn get_room_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Path(room_id): Path<String>,
) -> Result<Json<RoomResponse>, WebRTCError> {
    crate::ensure_permission(
        &state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs,
        zpermissions::Crud::READ,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    let rooms = state.webrtc_state.rooms.read().await;
    if let Some(room) = rooms.get(&room_id) {
        if room.org_id != auth.org_id {
            return Err(WebRTCError::PermissionDenied);
        }

        let response = RoomResponse {
            id: room.id.clone(),
            org_id: room.org_id,
            participant_count: room.participant_count().await,
            created_at: room.created_at,
            metadata: room.get_metadata().await,
        };

        Ok(Json(response))
    } else {
        Err(WebRTCError::RoomNotFound)
    }
}

pub async fn delete_room_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Path(room_id): Path<String>,
) -> Result<Json<serde_json::Value>, WebRTCError> {
    crate::ensure_permission(
        state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs,
        zpermissions::Crud::DELETE,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    let rooms = state.webrtc_state.rooms.read().await;
    if let Some(room) = rooms.get(&room_id) {
        if room.org_id != auth.org_id {
            return Err(WebRTCError::PermissionDenied);
        }

        // Check if room is empty
        if !room.is_empty().await {
            return Err(WebRTCError::InternalError("Cannot delete room with active participants".to_string()));
        }

        drop(rooms);
        state.webrtc_state.remove_room(&room_id).await;

        tracing::info!(
            room_id = %room_id,
            user_id = ?auth.user_id,
            org_id = auth.org_id,
            "Deleted WebRTC room"
        );

        Ok(Json(serde_json::json!({ "success": true })))
    } else {
        Err(WebRTCError::RoomNotFound)
    }
}

/// Enhanced WebRTC Session Handlers

#[derive(Debug, Deserialize)]
pub struct JoinRoomRequest {
    pub display_name: Option<String>,
    pub role: Option<super::ParticipantRole>,
    pub ice_servers: Option<Vec<crate::IceServerConfig>>,
    pub media_constraints: Option<super::MediaConstraints>,
    pub project_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct JoinRoomResponse {
    pub session_id: String,
    pub answer: RTCSessionDescriptionPayload,
    pub ice_servers: Vec<crate::IceServerConfig>,
    pub room_info: RoomResponse,
    pub participants: Vec<super::ParticipantInfo>,
}

pub async fn join_room_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Path(room_id): Path<String>,
    Json(req): Json<JoinRoomRequest>,
) -> Result<Json<JoinRoomResponse>, WebRTCError> {
    crate::ensure_permission(
        &state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs,
        zpermissions::Crud::READ,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    // Get or create room
    let room = state.webrtc_state.get_or_create_room(&room_id, auth.org_id).await?;

    // Create media engine with audio/video codecs
    let mut media_engine = MediaEngine::default();

    // Register default codecs (VP8 for video, Opus for audio)
    media_engine.register_default_codecs()
        .map_err(|e| WebRTCError::InternalError(format!("Failed to register codecs: {}", e)))?;

    // Create setting engine
    let mut setting_engine = SettingEngine::default();
    setting_engine.detach_data_channels();

    // Create interceptor registry
    let mut registry = Registry::new();

    // Register default interceptors
    registry = register_default_interceptors(registry, &mut media_engine)
        .map_err(|e| WebRTCError::InternalError(format!("Failed to register interceptors: {}", e)))?;

    // Create API
    let api = APIBuilder::new()
        .with_media_engine(media_engine)
        .with_setting_engine(setting_engine)
        .with_interceptor_registry(registry)
        .build();

    // Create peer connection with ICE servers
    let ice_servers = req.ice_servers.unwrap_or_else(|| state.ice_servers.clone());
    let mut rtc_ice_servers = Vec::new();
    for ice_server in &ice_servers {
        let mut rtc_ice_server = RTCIceServer {
            urls: ice_server.urls.clone(),
            ..Default::default()
        };
        if let Some(username) = &ice_server.username {
            rtc_ice_server.username = username.clone();
        }
        if let Some(credential) = &ice_server.credential {
            rtc_ice_server.credential = credential.clone();
        }
        rtc_ice_servers.push(rtc_ice_server);
    }

    let config = RTCConfiguration {
        ice_servers: rtc_ice_servers,
        ..Default::default()
    };

    // Create the peer connection
    let peer_connection = Arc::new(
        api.new_peer_connection(config)
            .await
            .map_err(|e| WebRTCError::InternalError(format!("Failed to create peer connection: {}", e)))?
    );

    // Create offer
    let offer = peer_connection.create_offer(None)
        .await
        .map_err(|e| WebRTCError::InternalError(format!("Failed to create offer: {}", e)))?;

    // Set local description
    peer_connection.set_local_description(offer.clone())
        .await
        .map_err(|e| WebRTCError::InternalError(format!("Failed to set local description: {}", e)))?;

    // Handle ICE candidates
    let room_id_clone = room_id.clone();
    peer_connection.on_ice_candidate(Box::new(move |candidate| {
        let room_id = room_id_clone.clone();
        Box::pin(async move {
            if let Some(candidate) = candidate {
                tracing::debug!(
                    room_id = %room_id,
                    candidate = %candidate.to_string(),
                    "Generated ICE candidate"
                );
                // TODO: Send ICE candidate to remote peer via signaling
            }
        })
    }));

    // Create session with real peer connection
    let user_id = auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone());
    let role = req.role.unwrap_or(super::ParticipantRole::Participant);
    let permissions = super::ParticipantPermissions::default_for_role(&role);

    let mut session = super::WebRTCSession::new(
        user_id,
        auth.org_id,
        req.project_id.clone(),
        peer_connection,
        role.clone(),
        permissions.clone(),
    );

    // Set display name if provided
    if let Some(display_name) = req.display_name {
        session.display_name = Some(display_name);
    }

    let session = Arc::new(session);

    // Set up connection state monitoring with cleanup
    setup_connection_monitoring(&session, &state).await;

    // Set up enhanced data channels
    if let Err(e) = setup_enhanced_data_channels(&session, &state).await {
        tracing::warn!(
            session_id = %session.id,
            error = %e,
            "Failed to set up enhanced data channels, proceeding with basic setup"
        );
    }

    // Set up transaction support if enabled
    if state.webrtc_state.config.enable_transactions {
        if let Err(e) = session.setup_transaction_support(true).await {
            tracing::warn!(
                session_id = %session.id,
                error = %e,
                "Failed to enable transaction support, continuing without transactions"
            );
        } else {
            // Set up transaction message routing after successful transaction setup
            if let Err(e) = setup_transaction_message_routing(&session, &state).await {
                tracing::warn!(
                    session_id = %session.id,
                    error = %e,
                    "Failed to setup transaction message routing"
                );
            }

            // Set up distributed transaction message routing
            if let Err(e) = setup_distributed_transaction_routing(&session, &state).await {
                tracing::warn!(
                    session_id = %session.id,
                    error = %e,
                    "Failed to setup distributed transaction routing"
                );
            }
        }
    }

    // Start event streaming from job queue
    let subscription_manager = super::SubscriptionManager::new(state.permissions.clone().into());
    if let Err(e) = subscription_manager.start_streaming(session.clone(), state.job_queue.clone()).await {
        tracing::warn!(
            session_id = %session.id,
            error = %e,
            "Failed to start event streaming, continuing without real-time events"
        );
    } else {
        tracing::info!(
            session_id = %session.id,
            "Event streaming started successfully"
        );
    }

    // Add session to room and state
    room.add_participant(session.clone()).await?;
    state.webrtc_state.add_session(session.clone()).await;

    // Trigger participant joined event
    let trigger_event = super::TriggerEvent {
        event_type: super::TriggerEventType::ParticipantJoined {
            room_id: room_id.clone(),
            user_id: auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone()),
            role: role.clone(),
        },
        event_data: serde_json::json!({
            "room_id": room_id,
            "user_id": auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone()),
            "session_id": session.id,
            "role": role,
            "display_name": session.display_name.clone(),
        }),
        context: super::TriggerContext {
            user_id: auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone()),
            org_id: auth.org_id,
            project_id: session.project_id.clone(),
            room_id: Some(room_id.clone()),
            session_id: Some(session.id.clone()),
            event_data: serde_json::json!({
                "join_time": chrono::Utc::now(),
                "media_constraints": req.media_constraints,
            }),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("event_source".to_string(), "webrtc_join".to_string());
                meta.insert("peer_connection_id".to_string(), session.id.clone());
                meta
            },
        },
        participant_count: Some(room.participant_count().await),
        timestamp: chrono::Utc::now(),
    };

    // Execute triggers asynchronously with permission checking
    let trigger_manager = state.function_trigger_manager.clone();
    let permissions_service = state.permissions.clone().into();
    let streaming_manager = state.webrtc_state.realtime_streaming_manager.clone();
    let auth_clone = auth.clone();
    tokio::spawn(async move {
        let executions = trigger_manager.execute_triggers_with_permissions(&trigger_event, Some(permissions_service)).await;

        // Broadcast trigger executions to streaming subscribers
        for execution in executions {
            let metadata = super::StreamingMetadata {
                org_id: auth_clone.org_id,
                project_id: session.project_id.clone(),
                user_id: Some(auth_clone.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth_clone.token_id.clone())),
                session_id: Some(session.id.clone()),
                room_id: Some(room_id.clone()),
                priority: "normal".to_string(),
                tags: vec!["webrtc".to_string(), "participant_joined".to_string()],
                source_type: "webrtc_event".to_string(),
            };

            if let Err(e) = streaming_manager.broadcast_trigger_execution(&execution, &metadata).await {
                tracing::warn!(
                    error = %e,
                    "Failed to broadcast trigger execution to streaming subscribers"
                );
            }
        }
    });

    // Broadcast connection event to streaming subscribers
    if let Err(e) = state.webrtc_state.realtime_streaming_manager.broadcast_connection_event(
        "participant_joined",
        &session.id,
        &auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone()),
        auth.org_id,
        Some(&room_id),
    ).await {
        tracing::warn!(
            error = %e,
            "Failed to broadcast connection event to streaming subscribers"
        );
    }

    // Create response with real SDP offer
    let answer_sdp = RTCSessionDescriptionPayload {
        sdp_type: offer.sdp_type.to_string(),
        sdp: offer.sdp,
    };

    let room_info = RoomResponse {
        id: room.id.clone(),
        org_id: room.org_id,
        participant_count: room.participant_count().await,
        created_at: room.created_at,
        metadata: room.get_metadata().await,
    };

    let participants = room.list_participants().await;

    let response = JoinRoomResponse {
        session_id: session.id.clone(),
        answer: answer_sdp,
        ice_servers,
        room_info,
        participants,
    };

    tracing::info!(
        room_id = %room_id,
        session_id = %session.id,
        user_id = ?auth.user_id,
        org_id = auth.org_id,
        "User joined WebRTC room with real peer connection"
    );

    Ok(Json(response))
}

pub async fn leave_room_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Path(room_id): Path<String>,
) -> Result<Json<serde_json::Value>, WebRTCError> {
    // Find user's session in the room
    let rooms = state.webrtc_state.rooms.read().await;
    if let Some(room) = rooms.get(&room_id) {
        if room.org_id != auth.org_id {
            return Err(WebRTCError::PermissionDenied);
        }

        let participants = room.participants.read().await;
        let mut user_session_id = None;

        let auth_user_id = auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone());
        for (session_id, session) in participants.iter() {
            if session.user_id == auth_user_id {
                user_session_id = Some(session_id.clone());
                break;
            }
        }

        drop(participants);

        if let Some(session_id) = user_session_id {
            if let Some(session) = room.remove_participant(&session_id).await {
                // Clean up transactions before removing session
                state.webrtc_state.transaction_manager.cleanup_session_transactions(&session_id).await;

                state.webrtc_state.remove_session(&session_id).await;

                // Trigger participant left event
                let trigger_event = super::TriggerEvent {
                    event_type: super::TriggerEventType::ParticipantLeft {
                        room_id: room_id.clone(),
                        user_id: auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone()),
                        role: session.role.clone(),
                    },
                    event_data: serde_json::json!({
                        "room_id": room_id,
                        "user_id": auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone()),
                        "session_id": session_id,
                    }),
                    context: super::TriggerContext {
                        user_id: auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone()),
                        org_id: auth.org_id,
                        project_id: session.project_id.clone(),
                        room_id: Some(room_id.clone()),
                        session_id: Some(session_id.clone()),
                        event_data: serde_json::Value::Null,
                        metadata: HashMap::new(),
                    },
                    participant_count: Some(room.participant_count().await),
                    timestamp: chrono::Utc::now(),
                };

                // Execute triggers with permission checking
                state.function_trigger_manager.execute_triggers_with_permissions(
                    &trigger_event,
                    Some(state.permissions.clone().into())
                ).await;

                tracing::info!(
                    room_id = %room_id,
                    session_id = %session_id,
                    user_id = ?auth.user_id,
                    "User left WebRTC room"
                );

                Ok(Json(serde_json::json!({ "success": true })))
            } else {
                Err(WebRTCError::SessionNotFound)
            }
        } else {
            Err(WebRTCError::SessionNotFound)
        }
    } else {
        Err(WebRTCError::RoomNotFound)
    }
}

pub async fn list_participants_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Path(room_id): Path<String>,
) -> Result<Json<Vec<super::ParticipantInfo>>, WebRTCError> {
    crate::ensure_permission(
        &state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs,
        zpermissions::Crud::READ,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    let rooms = state.webrtc_state.rooms.read().await;
    if let Some(room) = rooms.get(&room_id) {
        if room.org_id != auth.org_id {
            return Err(WebRTCError::PermissionDenied);
        }

        let participants = room.list_participants().await;
        Ok(Json(participants))
    } else {
        Err(WebRTCError::RoomNotFound)
    }
}

/// Helper functions for enhanced WebRTC setup

async fn setup_enhanced_data_channels(
    session: &super::WebRTCSession,
    _state: &crate::AppState,
) -> Result<(), WebRTCError> {
    // Create default data channel for basic messaging
    session.create_default_data_channel().await?;

    // Create three specific data channels for different reliability requirements:
    // 1. Logs channel - Reliable, ordered (for structured log streaming)
    // 2. Metrics channel - Unreliable, unordered (for high-frequency metrics) - currently reliable due to API limitations
    // 3. Events channel - Reliable, ordered (for important event notifications)

    // NOTE: The webrtc-rs crate (v0.8.0) doesn't expose RTCDataChannelInit configuration
    // in the create_data_channel API. All channels created with None will be reliable by default.
    // Future enhancement: When webrtc-rs supports reliability configuration, update metrics
    // channel to use unreliable delivery (max_retransmits: Some(0), ordered: Some(false))
    let channel_configs = [
        ("logs", "Reliable log streaming - ordered delivery guaranteed"),
        ("metrics", "High-frequency metrics - currently reliable, should be unreliable when API supports it"),
        ("events", "Critical event notifications - reliable ordered delivery"),
    ];

    for (channel_name, description) in &channel_configs {
        match session.peer_connection.create_data_channel(channel_name, None).await {
            Ok(raw_data_channel) => {
                let data_channel = raw_data_channel;

                // Set up event handlers for the channel
                let session_id = session.id.clone();
                let channel_name_string = channel_name.to_string();

                data_channel.on_open(Box::new(move || {
                    let session_id = session_id.clone();
                    let channel_name = channel_name_string.clone();
                    Box::pin(async move {
                        tracing::info!(
                            session_id = %session_id,
                            channel = %channel_name,
                            "Enhanced data channel opened"
                        );
                    })
                }));

                let session_id2 = session.id.clone();
                let channel_name_string2 = channel_name.to_string();
                data_channel.on_close(Box::new(move || {
                    let session_id = session_id2.clone();
                    let channel_name = channel_name_string2.clone();
                    Box::pin(async move {
                        tracing::info!(
                            session_id = %session_id,
                            channel = %channel_name,
                            "Enhanced data channel closed"
                        );
                    })
                }));

                let session_id3 = session.id.clone();
                let channel_name_string3 = channel_name.to_string();
                data_channel.on_message(Box::new(move |msg| {
                    let session_id = session_id3.clone();
                    let channel_name = channel_name_string3.clone();
                    Box::pin(async move {
                        tracing::debug!(
                            session_id = %session_id,
                            channel = %channel_name,
                            data_len = msg.data.len(),
                            is_string = msg.is_string,
                            "Received message on enhanced data channel"
                        );

                        // Handle specific channel messages
                        if msg.is_string {
                            if let Ok(text) = String::from_utf8(msg.data.to_vec()) {
                                match channel_name.as_str() {
                                    "logs" => {
                                        tracing::debug!(
                                            session_id = %session_id,
                                            message = %text,
                                            "Log message received on logs channel"
                                        );
                                        // TODO: Parse and forward log entries to appropriate handlers
                                    }
                                    "metrics" => {
                                        tracing::trace!(
                                            session_id = %session_id,
                                            "Metrics data received on metrics channel (length: {})",
                                            text.len()
                                        );
                                        // TODO: Parse and process metrics data (high frequency, allow drops)
                                    }
                                    "events" => {
                                        tracing::debug!(
                                            session_id = %session_id,
                                            message = %text,
                                            "Event notification received on events channel"
                                        );
                                        // TODO: Parse and process critical event notifications
                                    }
                                    _ => {
                                        tracing::debug!(
                                            session_id = %session_id,
                                            channel = %channel_name,
                                            message = %text,
                                            "Message received on unknown channel"
                                        );
                                    }
                                }
                            }
                        }
                    })
                }));

                // Add to session's data channels
                session.add_data_channel(channel_name, data_channel).await;

                tracing::info!(
                    session_id = %session.id,
                    channel = %channel_name,
                    description = %description,
                    "Created specialized data channel"
                );
            }
            Err(e) => {
                tracing::warn!(
                    session_id = %session.id,
                    channel = %channel_name,
                    error = %e,
                    "Failed to create enhanced data channel, continuing with existing channels"
                );
                // Don't fail the entire setup if one channel fails
            }
        }
    }

    tracing::info!(
        session_id = %session.id,
        "Enhanced data channel setup completed with specialized channels: logs, metrics, events"
    );

    Ok(())
}

async fn setup_media_tracks(
    _session: &super::WebRTCSession,
    _constraints: &super::MediaConstraints,
) -> Result<(), WebRTCError> {
    // TODO: Implement media track creation with real WebRTC API
    Err(WebRTCError::InternalError("Media track setup not implemented yet".to_string()))
}

async fn setup_connection_monitoring(
    session: &Arc<super::WebRTCSession>,
    state: &crate::AppState,
) {
    let session_weak = Arc::downgrade(session);
    let sessions_weak = Arc::downgrade(&state.webrtc_state.sessions);
    let rooms_weak = Arc::downgrade(&state.webrtc_state.rooms);
    let transaction_manager_weak = Arc::downgrade(&state.webrtc_state.transaction_manager);
    let trigger_manager_weak = Arc::downgrade(&state.function_trigger_manager);
    let session_id = session.id.clone();
    let user_id = session.user_id.clone();
    let org_id = session.org_id;
    let project_id = session.project_id.clone();

    // Set up connection state change handler with proper cleanup
    let session_id_for_handler = session_id.clone();
    let user_id_for_handler = user_id.clone();
    session.peer_connection.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
        let session_id = session_id_for_handler.clone();
        let user_id = user_id_for_handler.clone();
        let session_weak = session_weak.clone();
        let sessions_weak = sessions_weak.clone();
        let rooms_weak = rooms_weak.clone();
        let transaction_manager_weak = transaction_manager_weak.clone();

        Box::pin(async move {
            tracing::info!(
                session_id = %session_id,
                user_id = %user_id,
                state = ?s,
                "WebRTC peer connection state changed"
            );

            match s {
                RTCPeerConnectionState::Connected => {
                    tracing::info!(
                        session_id = %session_id,
                        user_id = %user_id,
                        "WebRTC connection established successfully"
                    );

                    // Update session activity on successful connection
                    if let Some(session) = session_weak.upgrade() {
                        session.update_activity().await;
                    }

                    // Trigger connection event
                    if let Some(trigger_manager) = trigger_manager_weak.upgrade() {
                        let trigger_event = super::TriggerEvent {
                            event_type: super::TriggerEventType::ConnectionEvent {
                                user_id: user_id.clone(),
                                event_type: super::ConnectionEventType::Connected,
                            },
                            event_data: serde_json::json!({
                                "connection_state": "connected",
                                "session_id": session_id,
                                "user_id": user_id,
                            }),
                            context: super::TriggerContext {
                                user_id: user_id.clone(),
                                org_id,
                                project_id: project_id.clone(),
                                room_id: None, // We don't have room context here
                                session_id: Some(session_id.clone()),
                                event_data: serde_json::json!({
                                    "connection_established_at": chrono::Utc::now(),
                                }),
                                metadata: {
                                    let mut meta = HashMap::new();
                                    meta.insert("event_source".to_string(), "webrtc_connection".to_string());
                                    meta.insert("connection_state".to_string(), "connected".to_string());
                                    meta
                                },
                            },
                            participant_count: None,
                            timestamp: chrono::Utc::now(),
                        };

                        let trigger_manager_clone = trigger_manager.clone();
                        tokio::spawn(async move {
                            trigger_manager_clone.execute_triggers(&trigger_event).await;
                        });
                    }
                }
                RTCPeerConnectionState::Disconnected => {
                    tracing::warn!(
                        session_id = %session_id,
                        user_id = %user_id,
                        "WebRTC connection disconnected - connection may recover"
                    );

                    // Trigger disconnection event
                    if let Some(trigger_manager) = trigger_manager_weak.upgrade() {
                        let trigger_event = super::TriggerEvent {
                            event_type: super::TriggerEventType::ConnectionEvent {
                                user_id: user_id.clone(),
                                event_type: super::ConnectionEventType::Disconnected,
                            },
                            event_data: serde_json::json!({
                                "connection_state": "disconnected",
                                "session_id": session_id,
                                "user_id": user_id,
                            }),
                            context: super::TriggerContext {
                                user_id: user_id.clone(),
                                org_id,
                                project_id: project_id.clone(),
                                room_id: None,
                                session_id: Some(session_id.clone()),
                                event_data: serde_json::json!({
                                    "disconnection_time": chrono::Utc::now(),
                                }),
                                metadata: {
                                    let mut meta = HashMap::new();
                                    meta.insert("event_source".to_string(), "webrtc_connection".to_string());
                                    meta.insert("connection_state".to_string(), "disconnected".to_string());
                                    meta
                                },
                            },
                            participant_count: None,
                            timestamp: chrono::Utc::now(),
                        };

                        let trigger_manager_clone = trigger_manager.clone();
                        tokio::spawn(async move {
                            trigger_manager_clone.execute_triggers(&trigger_event).await;
                        });
                    }
                }
                RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed => {
                    tracing::warn!(
                        session_id = %session_id,
                        user_id = %user_id,
                        state = ?s,
                        "WebRTC connection terminated - performing cleanup"
                    );

                    // Trigger connection failure event
                    if let Some(trigger_manager) = trigger_manager_weak.upgrade() {
                        let connection_event_type = if matches!(s, RTCPeerConnectionState::Failed) {
                            super::ConnectionEventType::Failed
                        } else {
                            super::ConnectionEventType::Disconnected
                        };

                        let trigger_event = super::TriggerEvent {
                            event_type: super::TriggerEventType::ConnectionEvent {
                                user_id: user_id.clone(),
                                event_type: connection_event_type,
                            },
                            event_data: serde_json::json!({
                                "connection_state": if matches!(s, RTCPeerConnectionState::Failed) { "failed" } else { "closed" },
                                "session_id": session_id,
                                "user_id": user_id,
                            }),
                            context: super::TriggerContext {
                                user_id: user_id.clone(),
                                org_id,
                                project_id: project_id.clone(),
                                room_id: None,
                                session_id: Some(session_id.clone()),
                                event_data: serde_json::json!({
                                    "termination_time": chrono::Utc::now(),
                                    "termination_reason": if matches!(s, RTCPeerConnectionState::Failed) { "failed" } else { "closed" },
                                }),
                                metadata: {
                                    let mut meta = HashMap::new();
                                    meta.insert("event_source".to_string(), "webrtc_connection".to_string());
                                    meta.insert("connection_state".to_string(), if matches!(s, RTCPeerConnectionState::Failed) { "failed".to_string() } else { "closed".to_string() });
                                    meta.insert("cleanup_required".to_string(), "true".to_string());
                                    meta
                                },
                            },
                            participant_count: None,
                            timestamp: chrono::Utc::now(),
                        };

                        let trigger_manager_clone = trigger_manager.clone();
                        tokio::spawn(async move {
                            trigger_manager_clone.execute_triggers(&trigger_event).await;
                        });
                    }

                    // Perform cleanup when connection fails or closes

                    // Clean up transactions first
                    if let Some(transaction_manager) = transaction_manager_weak.upgrade() {
                        transaction_manager.cleanup_session_transactions(&session_id).await;
                        tracing::info!(
                            session_id = %session_id,
                            user_id = %user_id,
                            "Cleaned up transactions for terminated session"
                        );
                    }

                    if let Some(sessions) = sessions_weak.upgrade() {
                        // Remove session from WebRTC sessions
                        sessions.write().await.remove(&session_id);

                        tracing::info!(
                            session_id = %session_id,
                            user_id = %user_id,
                            "Removed session from WebRTC state due to connection termination"
                        );
                    }

                    // Remove from all rooms
                    if let Some(rooms) = rooms_weak.upgrade() {
                        let rooms_read = rooms.read().await;
                        for room in rooms_read.values() {
                            if let Some(_removed_session) = room.remove_participant(&session_id).await {
                                tracing::info!(
                                    session_id = %session_id,
                                    room_id = %room.id,
                                    user_id = %user_id,
                                    "Removed session from room due to connection termination"
                                );

                                // Check if room is now empty
                                let participant_count = room.participant_count().await;
                                if participant_count == 0 {
                                    tracing::debug!(
                                        room_id = %room.id,
                                        "Room is now empty after connection cleanup"
                                    );
                                }
                                break;
                            }
                        }
                    }
                }
                RTCPeerConnectionState::New | RTCPeerConnectionState::Connecting => {
                    tracing::debug!(
                        session_id = %session_id,
                        user_id = %user_id,
                        state = ?s,
                        "WebRTC connection in progress"
                    );
                }
                RTCPeerConnectionState::Unspecified => {
                    tracing::debug!(
                        session_id = %session_id,
                        user_id = %user_id,
                        "WebRTC connection state unspecified"
                    );
                }
            }
        })
    }));

    // Set up ICE connection state change handler for additional monitoring
    let session_id_for_ice = session_id.clone();
    let user_id_for_ice = user_id.clone();
    session.peer_connection.on_ice_connection_state_change(Box::new(move |s| {
        let session_id = session_id_for_ice.clone();
        let user_id = user_id_for_ice.clone();
        Box::pin(async move {
            tracing::debug!(
                session_id = %session_id,
                user_id = %user_id,
                ice_state = ?s,
                "ICE connection state changed"
            );
        })
    }));

    tracing::debug!(
        session_id = %session_id,
        user_id = %user_id,
        org_id = org_id,
        "Connection state monitoring configured for WebRTC session"
    );
}

/// WebRTC Transaction Message Handling

/// Handle distributed transaction messages received through WebRTC data channel
/// TODO: This function needs to be reimplemented to use the correct ztransaction message types
/// Currently stubbed out as the message types don't match the actual ztransaction implementation
#[allow(dead_code)]
pub async fn handle_distributed_transaction_webrtc_message(
    _session: &Arc<super::WebRTCSession>,
    envelope: ztransaction::distributed::messaging::MessageEnvelope,
    _state: &crate::AppState,
) -> Result<(), super::WebRTCError> {
    tracing::warn!(
        "Distributed transaction WebRTC handler not yet implemented - received message: {:?}",
        envelope
    );
    Err(super::WebRTCError::InternalError(
        "Distributed transaction WebRTC support not yet implemented".to_string()
    ))
}

/// Send distributed transaction response through WebRTC data channel
/// TODO: This function needs to be reimplemented to use the correct ztransaction message types
#[allow(dead_code)]
pub async fn send_distributed_transaction_response_webrtc(
    _session: &Arc<super::WebRTCSession>,
    _response: TwoPhaseCommitMessage,
) -> Result<(), super::WebRTCError> {
    Err(super::WebRTCError::InternalError(
        "Distributed transaction WebRTC support not yet implemented".to_string()
    ))
}

/// Handle transaction messages received through WebRTC data channel
pub async fn handle_transaction_message(
    session: &Arc<super::WebRTCSession>,
    message: super::transaction::TransactionMessage,
    state: &crate::AppState,
) -> Result<super::transaction::TransactionResponse, super::WebRTCError> {
    use super::transaction::{TransactionMessage, TransactionResponse, TransactionErrorCode};

    tracing::debug!(
        session_id = %session.id,
        message_type = ?std::mem::discriminant(&message),
        "Processing WebRTC transaction message"
    );

    // Check if session has transaction support enabled
    if !session.has_transaction_support().await {
        return Ok(TransactionResponse::Error {
            tx_id: None,
            operation_id: None,
            error_code: TransactionErrorCode::InternalError,
            message: "Transaction support not enabled for this session".to_string(),
        });
    }

    let transaction_manager = &state.webrtc_state.transaction_manager;

    match message {
        TransactionMessage::BeginTx {
            tx_id,
            isolation_level,
            read_only,
            timeout_ms,
        } => {
            // Check transaction limits per session
            let current_tx_count = transaction_manager
                .list_session_transactions(&session.id)
                .await
                .len();

            if current_tx_count >= state.webrtc_state.config.max_transactions_per_session {
                return Ok(TransactionResponse::Error {
                    tx_id: Some(tx_id),
                    operation_id: None,
                    error_code: TransactionErrorCode::InternalError,
                    message: "Maximum transactions per session exceeded".to_string(),
                });
            }

            // Use configured timeout if none provided
            let effective_timeout = timeout_ms.unwrap_or(state.webrtc_state.config.transaction_timeout_ms);

            // Extract org_id from session
            let org_id = session.org_id;

            // Create user context from session data (simplified)
            let user_context = Some(zexec_engine::transaction_events::UserContext {
                user_id: session.user_id.parse::<u64>().unwrap_or(0), // Convert string to u64
                username: session.user_id.clone(),
                roles: vec![], // Would be populated from actual user data
                session_id: Some(session.id.clone()),
            });

            match transaction_manager
                .begin_transaction(
                    session.id.clone(),
                    isolation_level,
                    read_only,
                    Some(effective_timeout),
                    org_id,
                    user_context,
                )
                .await
            {
                Ok(new_tx_id) => {
                    tracing::info!(
                        session_id = %session.id,
                        tx_id = %new_tx_id,
                        isolation_level = ?isolation_level,
                        read_only = read_only,
                        timeout_ms = effective_timeout,
                        "Started WebRTC transaction"
                    );

                    Ok(TransactionResponse::BeginOk {
                        tx_id: new_tx_id,
                        created_at: chrono::Utc::now(),
                    })
                }
                Err(error_code) => Ok(TransactionResponse::Error {
                    tx_id: Some(tx_id),
                    operation_id: None,
                    error_code,
                    message: format!("Failed to begin transaction: {:?}", error_code),
                }),
            }
        }

        TransactionMessage::ExecuteTx {
            tx_id,
            operation_id,
            sql,
            parameters,
        } => {
            // Use the session org_id from the handler context
            let org_id = session.org_id;

            match transaction_manager
                .execute_operation(&tx_id, operation_id.clone(), sql.clone(), parameters, org_id)
                .await
            {
                Ok(result) => {
                    tracing::debug!(
                        session_id = %session.id,
                        tx_id = %tx_id,
                        operation_id = %operation_id,
                        sql_len = sql.len(),
                        "Executed WebRTC transaction operation"
                    );

                    Ok(TransactionResponse::ExecuteOk {
                        tx_id,
                        operation_id,
                        result,
                    })
                }
                Err(error_code) => {
                    tracing::error!(
                        session_id = %session.id,
                        tx_id = %tx_id,
                        operation_id = %operation_id,
                        error_code = ?error_code,
                        "Failed to execute WebRTC transaction operation"
                    );

                    Ok(TransactionResponse::Error {
                        tx_id: Some(tx_id),
                        operation_id: Some(operation_id),
                        error_code,
                        message: format!("Operation failed: {:?}", error_code),
                    })
                }
            }
        }

        TransactionMessage::CommitTx { tx_id } => {
            match transaction_manager.commit_transaction(&tx_id).await {
                Ok(()) => {
                    tracing::info!(
                        session_id = %session.id,
                        tx_id = %tx_id,
                        "Committed WebRTC transaction"
                    );

                    Ok(TransactionResponse::CommitOk {
                        tx_id,
                        committed_at: chrono::Utc::now(),
                    })
                }
                Err(error_code) => {
                    tracing::error!(
                        session_id = %session.id,
                        tx_id = %tx_id,
                        error_code = ?error_code,
                        "Failed to commit WebRTC transaction"
                    );

                    Ok(TransactionResponse::Error {
                        tx_id: Some(tx_id),
                        operation_id: None,
                        error_code,
                        message: format!("Commit failed: {:?}", error_code),
                    })
                }
            }
        }

        TransactionMessage::RollbackTx { tx_id } => {
            match transaction_manager.rollback_transaction(&tx_id).await {
                Ok(()) => {
                    tracing::info!(
                        session_id = %session.id,
                        tx_id = %tx_id,
                        "Rolled back WebRTC transaction"
                    );

                    Ok(TransactionResponse::RollbackOk {
                        tx_id,
                        rolled_back_at: chrono::Utc::now(),
                    })
                }
                Err(error_code) => {
                    tracing::error!(
                        session_id = %session.id,
                        tx_id = %tx_id,
                        error_code = ?error_code,
                        "Failed to rollback WebRTC transaction"
                    );

                    Ok(TransactionResponse::Error {
                        tx_id: Some(tx_id),
                        operation_id: None,
                        error_code,
                        message: format!("Rollback failed: {:?}", error_code),
                    })
                }
            }
        }

        TransactionMessage::StatusTx { tx_id } => {
            match transaction_manager.get_transaction_status(&tx_id).await {
                Ok(status_response) => {
                    tracing::debug!(
                        session_id = %session.id,
                        tx_id = %tx_id,
                        "Retrieved WebRTC transaction status"
                    );
                    Ok(status_response)
                }
                Err(error_code) => {
                    tracing::warn!(
                        session_id = %session.id,
                        tx_id = %tx_id,
                        error_code = ?error_code,
                        "Transaction status request failed"
                    );

                    Ok(TransactionResponse::Error {
                        tx_id: Some(tx_id),
                        operation_id: None,
                        error_code,
                        message: format!("Status request failed: {:?}", error_code),
                    })
                }
            }
        }
    }
}

/// Setup distributed transaction message routing for a session
pub async fn setup_distributed_transaction_routing(
    session: &Arc<super::WebRTCSession>,
    state: &crate::AppState,
) -> Result<(), super::WebRTCError> {
    // Create a dedicated data channel for distributed transactions
    let distributed_tx_channel = session.peer_connection
        .create_data_channel("distributed_transactions", None)
        .await
        .map_err(|e| super::WebRTCError::InternalError(
            format!("Failed to create distributed transactions data channel: {}", e)
        ))?;

    let session_clone = session.clone();
    let state_clone = state.clone();

    // Set up message handler for distributed transaction messages
    distributed_tx_channel.on_message(Box::new(move |msg| {
        let session = session_clone.clone();
        let state = state_clone.clone();

        Box::pin(async move {
            if !msg.is_string {
                match serde_json::from_slice::<ztransaction::distributed::messaging::MessageEnvelope>(&msg.data) {
                    Ok(envelope) => {
                        tracing::debug!(
                            session_id = %session.id,
                            message_id = %envelope.id,
                            "Received distributed transaction message via WebRTC"
                        );

                        // Process distributed transaction message through the coordinator
                        if let Err(e) = handle_distributed_transaction_webrtc_message(&session, envelope, &state).await {
                            tracing::error!(
                                session_id = %session.id,
                                error = %e,
                                "Failed to handle distributed transaction message via WebRTC"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            session_id = %session.id,
                            error = %e,
                            "Failed to parse distributed transaction message from WebRTC"
                        );
                    }
                }
            }
        })
    }));

    // Set up channel open handler
    let session_id_clone = session.id.clone();
    distributed_tx_channel.on_open(Box::new(move || {
        let session_id = session_id_clone.clone();
        Box::pin(async move {
            tracing::info!(
                session_id = %session_id,
                "Distributed transactions WebRTC data channel opened"
            );
        })
    }));

    // Add the channel to session
    session.add_data_channel("distributed_transactions", distributed_tx_channel).await;

    tracing::info!(
        session_id = %session.id,
        "Distributed transaction WebRTC routing configured"
    );

    Ok(())
}

/// Setup transaction message routing for a session
pub async fn setup_transaction_message_routing(
    session: &Arc<super::WebRTCSession>,
    state: &crate::AppState,
) -> Result<(), super::WebRTCError> {
    // Get the transaction data channel
    if let Some(tx_channel) = session.get_data_channel("transactions").await {
        let session_clone = session.clone();
        let state_clone = state.clone();

        // Override the message handler to process transaction messages
        tx_channel.on_message(Box::new(move |msg| {
            let session = session_clone.clone();
            let state = state_clone.clone();

            Box::pin(async move {
                if !msg.is_string {
                    match super::transaction::codec::parse_frame(&msg.data) {
                        Ok(super::transaction::codec::FrameType::Message(transaction_msg)) => {
                            tracing::debug!(
                                session_id = %session.id,
                                message_type = ?std::mem::discriminant(&transaction_msg),
                                "Routing WebRTC transaction message to handler"
                            );

                            // Process the transaction message
                            match handle_transaction_message(&session, transaction_msg, &state).await {
                                Ok(response) => {
                                    // Send response back through data channel
                                    if let Err(e) = session.send_transaction_response(&response).await {
                                        tracing::error!(
                                            session_id = %session.id,
                                            error = %e,
                                            "Failed to send transaction response"
                                        );
                                    }
                                }
                                Err(webrtc_error) => {
                                    tracing::error!(
                                        session_id = %session.id,
                                        error = %webrtc_error,
                                        "WebRTC transaction handler error"
                                    );
                                }
                            }
                        }
                        Ok(super::transaction::codec::FrameType::Response(_)) => {
                            tracing::warn!(
                                session_id = %session.id,
                                "Received response frame on server channel (unexpected)"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                session_id = %session.id,
                                error = %e,
                                "Failed to parse transaction frame"
                            );
                        }
                    }
                }
            })
        }));

        tracing::info!(
            session_id = %session.id,
            "Transaction message routing configured for WebRTC session"
        );

        Ok(())
    } else {
        Err(super::WebRTCError::InternalError(
            "Transaction data channel not found".to_string()
        ))
    }
}

// Additional helper functions and other handlers would be implemented here...

#[derive(Debug, Serialize, Deserialize)]
pub struct RTCSessionDescriptionPayload {
    #[serde(rename = "type")]
    pub sdp_type: String,
    pub sdp: String,
}

/// WebRTC Transaction API Endpoints

#[derive(Debug, Serialize)]
pub struct TransactionStatusResponse {
    pub session_id: String,
    pub transaction_count: usize,
    pub active_transactions: Vec<String>,
    pub transaction_support_enabled: bool,
    pub max_transactions_per_session: usize,
}

/// Get transaction status for a WebRTC session
pub async fn get_session_transaction_status_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<TransactionStatusResponse>, WebRTCError> {
    // Check permissions
    crate::ensure_permission(
        &state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs,
        zpermissions::Crud::READ,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    // Get the session
    let session = state.webrtc_state.get_session(&session_id).await
        .ok_or(WebRTCError::SessionNotFound)?;

    // Check org access
    if session.org_id != auth.org_id {
        return Err(WebRTCError::PermissionDenied);
    }

    let transaction_manager = &state.webrtc_state.transaction_manager;
    let active_transactions = transaction_manager.list_session_transactions(&session_id).await;
    let transaction_support_enabled = session.has_transaction_support().await;

    let response = TransactionStatusResponse {
        session_id: session_id.clone(),
        transaction_count: active_transactions.len(),
        active_transactions,
        transaction_support_enabled,
        max_transactions_per_session: state.webrtc_state.config.max_transactions_per_session,
    };

    tracing::debug!(
        session_id = %session_id,
        user_id = ?auth.user_id,
        org_id = auth.org_id,
        transaction_count = active_transactions.len(),
        "Retrieved WebRTC session transaction status"
    );

    Ok(Json(response))
}

#[derive(Debug, Serialize)]
pub struct TransactionListResponse {
    pub transactions: Vec<TransactionInfo>,
    pub total_count: usize,
}

#[derive(Debug, Serialize)]
pub struct TransactionInfo {
    pub tx_id: String,
    pub session_id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_activity: chrono::DateTime<chrono::Utc>,
    pub state: super::transaction::TransactionState,
    pub isolation_level: super::transaction::IsolationLevel,
    pub read_only: bool,
    pub operations_count: u64,
}

/// List all active transactions for an organization
pub async fn list_org_transactions_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
) -> Result<Json<TransactionListResponse>, WebRTCError> {
    // Check permissions
    crate::ensure_permission(
        &state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs,
        zpermissions::Crud::READ,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    // Get all sessions for this org
    let sessions = state.webrtc_state.sessions.read().await;
    let org_sessions: Vec<_> = sessions.values()
        .filter(|session| session.org_id == auth.org_id)
        .collect();

    let mut transactions = Vec::new();
    let transaction_manager = &state.webrtc_state.transaction_manager;

    for session in org_sessions {
        let session_transactions = transaction_manager.list_session_transactions(&session.id).await;

        for tx_id in session_transactions {
            if let Ok(status) = transaction_manager.get_transaction_status(&tx_id).await {
                if let super::transaction::TransactionResponse::Status {
                    tx_id,
                    state,
                    created_at,
                    last_activity,
                    isolation_level,
                    read_only,
                    operations_count,
                } = status {
                    transactions.push(TransactionInfo {
                        tx_id,
                        session_id: session.id.clone(),
                        created_at,
                        last_activity,
                        state,
                        isolation_level,
                        read_only,
                        operations_count,
                    });
                }
            }
        }
    }

    let response = TransactionListResponse {
        total_count: transactions.len(),
        transactions,
    };

    tracing::debug!(
        user_id = ?auth.user_id,
        org_id = auth.org_id,
        transaction_count = response.total_count,
        "Listed WebRTC transactions for organization"
    );

    Ok(Json(response))
}

#[derive(Debug, Deserialize)]
pub struct ForceRollbackRequest {
    pub reason: Option<String>,
}

/// Force rollback a specific transaction (admin operation)
pub async fn force_rollback_transaction_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Path((session_id, tx_id)): Path<(String, String)>,
    Json(req): Json<ForceRollbackRequest>,
) -> Result<Json<serde_json::Value>, WebRTCError> {
    // Check admin permissions
    crate::ensure_permission(
        state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs,
        zpermissions::Crud::DELETE,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    // Get the session
    let session = state.webrtc_state.get_session(&session_id).await
        .ok_or(WebRTCError::SessionNotFound)?;

    // Check org access
    if session.org_id != auth.org_id {
        return Err(WebRTCError::PermissionDenied);
    }

    let transaction_manager = &state.webrtc_state.transaction_manager;

    match transaction_manager.rollback_transaction(&tx_id).await {
        Ok(()) => {
            let reason = req.reason.unwrap_or_else(|| "Admin force rollback".to_string());

            // Send rollback response through WebRTC if session is connected
            if session.is_transaction_channel_ready().await {
                let response = super::transaction::TransactionResponse::RollbackOk {
                    tx_id: tx_id.clone(),
                    rolled_back_at: chrono::Utc::now(),
                };

                if let Err(e) = session.send_transaction_response(&response).await {
                    tracing::warn!(
                        session_id = %session_id,
                        tx_id = %tx_id,
                        error = %e,
                        "Failed to send force rollback notification through WebRTC"
                    );
                }
            }

            tracing::warn!(
                session_id = %session_id,
                tx_id = %tx_id,
                user_id = ?auth.user_id,
                org_id = auth.org_id,
                reason = %reason,
                "Force rolled back WebRTC transaction"
            );

            Ok(Json(serde_json::json!({
                "success": true,
                "tx_id": tx_id,
                "rolled_back_at": chrono::Utc::now(),
                "reason": reason
            })))
        }
        Err(error_code) => {
            tracing::error!(
                session_id = %session_id,
                tx_id = %tx_id,
                user_id = ?auth.user_id,
                error_code = ?error_code,
                "Failed to force rollback WebRTC transaction"
            );

            Err(WebRTCError::InternalError(format!(
                "Force rollback failed: {:?}", error_code
            )))
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TransactionStatsResponse {
    pub total_active_transactions: usize,
    pub total_sessions_with_transactions: usize,
    pub transactions_by_session: std::collections::HashMap<String, usize>,
    pub config: TransactionConfigInfo,
}

#[derive(Debug, Serialize)]
pub struct TransactionConfigInfo {
    pub enabled: bool,
    pub max_transactions_per_session: usize,
    pub timeout_ms: u64,
}

/// Get transaction statistics for the organization
pub async fn get_transaction_stats_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
) -> Result<Json<TransactionStatsResponse>, WebRTCError> {
    // Check permissions
    crate::ensure_permission(
        &state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs,
        zpermissions::Crud::READ,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    let sessions = state.webrtc_state.sessions.read().await;
    let org_sessions: Vec<_> = sessions.values()
        .filter(|session| session.org_id == auth.org_id)
        .collect();

    let mut total_transactions = 0;
    let mut sessions_with_transactions = 0;
    let mut transactions_by_session = std::collections::HashMap::new();
    let transaction_manager = &state.webrtc_state.transaction_manager;

    for session in org_sessions {
        let session_transactions = transaction_manager.list_session_transactions(&session.id).await;
        let tx_count = session_transactions.len();

        if tx_count > 0 {
            sessions_with_transactions += 1;
            total_transactions += tx_count;
            transactions_by_session.insert(session.id.clone(), tx_count);
        }
    }

    let response = TransactionStatsResponse {
        total_active_transactions: total_transactions,
        total_sessions_with_transactions: sessions_with_transactions,
        transactions_by_session,
        config: TransactionConfigInfo {
            enabled: state.webrtc_state.config.enable_transactions,
            max_transactions_per_session: state.webrtc_state.config.max_transactions_per_session,
            timeout_ms: state.webrtc_state.config.transaction_timeout_ms,
        },
    };

    tracing::debug!(
        user_id = ?auth.user_id,
        org_id = auth.org_id,
        total_transactions = total_transactions,
        sessions_with_transactions = sessions_with_transactions,
        "Retrieved WebRTC transaction statistics"
    );

    Ok(Json(response))
}

// Placeholder implementations for remaining handlers
pub async fn get_participant_handler() -> Result<Json<serde_json::Value>, WebRTCError> {
    Ok(Json(serde_json::json!({"message": "Not implemented yet"})))
}

pub async fn update_participant_handler() -> Result<Json<serde_json::Value>, WebRTCError> {
    Ok(Json(serde_json::json!({"message": "Not implemented yet"})))
}

pub async fn list_sessions_handler() -> Result<Json<serde_json::Value>, WebRTCError> {
    Ok(Json(serde_json::json!({"message": "Not implemented yet"})))
}

pub async fn get_session_handler() -> Result<Json<serde_json::Value>, WebRTCError> {
    Ok(Json(serde_json::json!({"message": "Not implemented yet"})))
}

pub async fn update_session_handler() -> Result<Json<serde_json::Value>, WebRTCError> {
    Ok(Json(serde_json::json!({"message": "Not implemented yet"})))
}

/// SDP Signaling Handlers

#[derive(Debug, Deserialize)]
pub struct CreateOfferRequest {
    pub session_id: String,
    pub media_constraints: Option<super::MediaConstraints>,
}

#[derive(Debug, Serialize)]
pub struct CreateOfferResponse {
    pub offer: String,
    pub session_id: String,
}

/// Create an SDP offer for a WebRTC session
pub async fn create_offer_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Json(req): Json<CreateOfferRequest>,
) -> Result<Json<CreateOfferResponse>, WebRTCError> {
    // Get the session
    let session = state.webrtc_state.get_session(&req.session_id).await
        .ok_or(WebRTCError::SessionNotFound)?;

    // Verify session belongs to the authenticated user
    if session.user_id != auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone()) {
        return Err(WebRTCError::PermissionDenied);
    }

    // Create the SDP offer
    let offer_sdp = session.create_offer().await?;

    tracing::info!(
        session_id = %req.session_id,
        user_id = %session.user_id,
        "Created SDP offer"
    );

    Ok(Json(CreateOfferResponse {
        offer: offer_sdp,
        session_id: req.session_id,
    }))
}

#[derive(Debug, Deserialize)]
pub struct SetAnswerRequest {
    pub session_id: String,
    pub answer: String,
}

#[derive(Debug, Serialize)]
pub struct SetAnswerResponse {
    pub success: bool,
    pub session_id: String,
}

/// Set the SDP answer for a WebRTC session
pub async fn set_answer_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Json(req): Json<SetAnswerRequest>,
) -> Result<Json<SetAnswerResponse>, WebRTCError> {
    // Get the session
    let session = state.webrtc_state.get_session(&req.session_id).await
        .ok_or(WebRTCError::SessionNotFound)?;

    // Verify session belongs to the authenticated user
    if session.user_id != auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone()) {
        return Err(WebRTCError::PermissionDenied);
    }

    // Set the answer
    session.set_answer(&req.answer).await?;

    tracing::info!(
        session_id = %req.session_id,
        user_id = %session.user_id,
        "Set SDP answer"
    );

    Ok(Json(SetAnswerResponse {
        success: true,
        session_id: req.session_id,
    }))
}

#[derive(Debug, Deserialize)]
pub struct HandleOfferRequest {
    pub session_id: String,
    pub offer: String,
}

#[derive(Debug, Serialize)]
pub struct HandleOfferResponse {
    pub answer: String,
    pub session_id: String,
}

/// Handle an SDP offer and create an answer
pub async fn handle_offer_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Json(req): Json<HandleOfferRequest>,
) -> Result<Json<HandleOfferResponse>, WebRTCError> {
    // Get the session
    let session = state.webrtc_state.get_session(&req.session_id).await
        .ok_or(WebRTCError::SessionNotFound)?;

    // Verify session belongs to the authenticated user
    if session.user_id != auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone()) {
        return Err(WebRTCError::PermissionDenied);
    }

    // Handle the offer and create an answer
    let answer_sdp = session.handle_offer(&req.offer).await?;

    tracing::info!(
        session_id = %req.session_id,
        user_id = %session.user_id,
        "Handled SDP offer and created answer"
    );

    Ok(Json(HandleOfferResponse {
        answer: answer_sdp,
        session_id: req.session_id,
    }))
}

/// ICE Candidate Handlers

#[derive(Debug, Deserialize)]
pub struct AddIceCandidateRequest {
    pub session_id: String,
    pub candidate: String,
    pub sdp_mid: Option<String>,
    pub sdp_mline_index: Option<u16>,
}

#[derive(Debug, Serialize)]
pub struct AddIceCandidateResponse {
    pub success: bool,
    pub session_id: String,
}

/// Add an ICE candidate to a WebRTC session
pub async fn add_ice_candidate_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Json(req): Json<AddIceCandidateRequest>,
) -> Result<Json<AddIceCandidateResponse>, WebRTCError> {
    // Get the session
    let session = state.webrtc_state.get_session(&req.session_id).await
        .ok_or(WebRTCError::SessionNotFound)?;

    // Verify session belongs to the authenticated user
    if session.user_id != auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone()) {
        return Err(WebRTCError::PermissionDenied);
    }

    // Add the ICE candidate
    session.add_ice_candidate(
        &req.candidate,
        req.sdp_mid.as_deref(),
        req.sdp_mline_index,
    ).await?;

    tracing::debug!(
        session_id = %req.session_id,
        user_id = %session.user_id,
        candidate = %req.candidate,
        "Added ICE candidate"
    );

    Ok(Json(AddIceCandidateResponse {
        success: true,
        session_id: req.session_id,
    }))
}

/// Connection Quality Monitoring

#[derive(Debug, Serialize)]
pub struct ConnectionStatsResponse {
    pub session_id: String,
    pub connection_state: String,
    pub ice_connection_state: String,
    pub is_connected: bool,
    pub connection_quality: super::ConnectionQuality,
    pub bandwidth_usage: super::BandwidthUsage,
}

/// Get connection statistics for a WebRTC session
pub async fn get_connection_stats_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<ConnectionStatsResponse>, WebRTCError> {
    // Get the session
    let session = state.webrtc_state.get_session(&session_id).await
        .ok_or(WebRTCError::SessionNotFound)?;

    // Verify session belongs to the authenticated user
    if session.user_id != auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone()) {
        return Err(WebRTCError::PermissionDenied);
    }

    let metadata = session.get_metadata().await;

    let response = ConnectionStatsResponse {
        session_id: session_id.clone(),
        connection_state: format!("{:?}", session.get_connection_state()),
        ice_connection_state: format!("{:?}", session.get_ice_connection_state()),
        is_connected: session.is_connected(),
        connection_quality: metadata.connection_quality,
        bandwidth_usage: metadata.bandwidth_usage,
    };

    tracing::debug!(
        session_id = %session_id,
        user_id = %session.user_id,
        connection_state = %response.connection_state,
        ice_state = %response.ice_connection_state,
        "Retrieved connection statistics"
    );

    Ok(Json(response))
}

#[derive(Debug, Deserialize)]
pub struct UpdateConnectionQualityRequest {
    pub session_id: String,
    pub connection_quality: super::ConnectionQuality,
}

/// Update connection quality metrics for a session
pub async fn update_connection_quality_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Json(req): Json<UpdateConnectionQualityRequest>,
) -> Result<Json<serde_json::Value>, WebRTCError> {
    // Get the session
    let session = state.webrtc_state.get_session(&req.session_id).await
        .ok_or(WebRTCError::SessionNotFound)?;

    // Verify session belongs to the authenticated user
    if session.user_id != auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone()) {
        return Err(WebRTCError::PermissionDenied);
    }

    // Update connection quality
    session.update_connection_quality(req.connection_quality.clone()).await;

    tracing::debug!(
        session_id = %req.session_id,
        user_id = %session.user_id,
        quality_score = req.connection_quality.score,
        latency_ms = ?req.connection_quality.latency_ms,
        "Updated connection quality metrics"
    );

    Ok(Json(serde_json::json!({
        "success": true,
        "session_id": req.session_id
    })))
}

/// Session Management Handlers

#[derive(Debug, Serialize)]
pub struct SessionStatusResponse {
    pub session_id: String,
    pub user_id: String,
    pub org_id: u64,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_activity: chrono::DateTime<chrono::Utc>,
    pub is_active: bool,
    pub is_connected: bool,
    pub data_channels: std::collections::HashMap<String, String>,
    pub feature_flags: std::collections::HashMap<String, bool>,
}

/// Get detailed session status
pub async fn get_session_status_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionStatusResponse>, WebRTCError> {
    // Get the session
    let session = state.webrtc_state.get_session(&session_id).await
        .ok_or(WebRTCError::SessionNotFound)?;

    // Verify session belongs to the authenticated user
    if session.user_id != auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone()) {
        return Err(WebRTCError::PermissionDenied);
    }

    let data_channel_states = session.get_data_channel_states().await;
    let data_channels = data_channel_states.into_iter()
        .map(|(name, state)| (name, format!("{:?}", state)))
        .collect();

    let metadata = session.get_metadata().await;
    let last_activity = *session.last_activity.read().await;

    let response = SessionStatusResponse {
        session_id: session_id.clone(),
        user_id: session.user_id.clone(),
        org_id: session.org_id,
        created_at: session.created_at,
        last_activity,
        is_active: session.is_active(30).await, // 30 minute timeout
        is_connected: session.is_connected(),
        data_channels,
        feature_flags: metadata.feature_flags,
    };

    tracing::debug!(
        session_id = %session_id,
        user_id = %session.user_id,
        is_active = response.is_active,
        is_connected = response.is_connected,
        "Retrieved session status"
    );

    Ok(Json(response))
}

/// Close a WebRTC session
pub async fn close_session_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<serde_json::Value>, WebRTCError> {
    // Get the session
    let session = state.webrtc_state.get_session(&session_id).await
        .ok_or(WebRTCError::SessionNotFound)?;

    // Verify session belongs to the authenticated user
    if session.user_id != auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone()) {
        return Err(WebRTCError::PermissionDenied);
    }

    // Close the peer connection
    session.close().await?;

    // Remove the session from state
    state.webrtc_state.remove_session(&session_id).await;

    tracing::info!(
        session_id = %session_id,
        user_id = %session.user_id,
        "Closed WebRTC session"
    );

    Ok(Json(serde_json::json!({
        "success": true,
        "session_id": session_id,
        "message": "Session closed successfully"
    })))
}

pub async fn create_subscription_handler() -> Result<Json<serde_json::Value>, WebRTCError> {
    Ok(Json(serde_json::json!({"message": "Not implemented yet"})))
}

pub async fn list_subscriptions_handler() -> Result<Json<serde_json::Value>, WebRTCError> {
    Ok(Json(serde_json::json!({"message": "Not implemented yet"})))
}

pub async fn get_subscription_handler() -> Result<Json<serde_json::Value>, WebRTCError> {
    Ok(Json(serde_json::json!({"message": "Not implemented yet"})))
}

pub async fn update_subscription_handler() -> Result<Json<serde_json::Value>, WebRTCError> {
    Ok(Json(serde_json::json!({"message": "Not implemented yet"})))
}

pub async fn delete_subscription_handler() -> Result<Json<serde_json::Value>, WebRTCError> {
    Ok(Json(serde_json::json!({"message": "Not implemented yet"})))
}

pub async fn create_trigger_handler() -> Result<Json<serde_json::Value>, WebRTCError> {
    Ok(Json(serde_json::json!({"message": "Not implemented yet"})))
}

pub async fn list_triggers_handler() -> Result<Json<serde_json::Value>, WebRTCError> {
    Ok(Json(serde_json::json!({"message": "Not implemented yet"})))
}

pub async fn get_trigger_handler() -> Result<Json<serde_json::Value>, WebRTCError> {
    Ok(Json(serde_json::json!({"message": "Not implemented yet"})))
}

pub async fn update_trigger_handler() -> Result<Json<serde_json::Value>, WebRTCError> {
    Ok(Json(serde_json::json!({"message": "Not implemented yet"})))
}

pub async fn delete_trigger_handler() -> Result<Json<serde_json::Value>, WebRTCError> {
    Ok(Json(serde_json::json!({"message": "Not implemented yet"})))
}

pub async fn execute_trigger_handler() -> Result<Json<serde_json::Value>, WebRTCError> {
    Ok(Json(serde_json::json!({"message": "Not implemented yet"})))
}

pub async fn get_trigger_history_handler() -> Result<Json<serde_json::Value>, WebRTCError> {
    Ok(Json(serde_json::json!({"message": "Not implemented yet"})))
}

/// Database Trigger Management Handlers

#[derive(Debug, Serialize)]
pub struct DatabaseTriggerResponse {
    pub id: String,
    pub name: String,
    pub org_id: u64,
    pub project_id: Option<String>,
    pub trigger_config: super::DatabaseTriggerConfig,
    pub function_name: String,
    pub enabled: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub metadata: super::DatabaseTriggerMetadata,
}

impl From<super::DatabaseTrigger> for DatabaseTriggerResponse {
    fn from(trigger: super::DatabaseTrigger) -> Self {
        Self {
            id: trigger.id,
            name: trigger.name,
            org_id: trigger.org_id,
            project_id: trigger.project_id,
            trigger_config: trigger.trigger_config,
            function_name: trigger.function_name,
            enabled: trigger.enabled,
            created_at: trigger.created_at,
            metadata: trigger.metadata,
        }
    }
}

/// Create a new database trigger
pub async fn create_database_trigger_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Json(req): Json<super::CreateDatabaseTriggerRequest>,
) -> Result<Json<DatabaseTriggerResponse>, WebRTCError> {
    // Check permissions
    crate::ensure_permission(
        state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs, // Use Jobs resource for triggers
        zpermissions::Crud::CREATE,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    let trigger = super::DatabaseTrigger {
        id: uuid::Uuid::new_v4().to_string(),
        name: req.name,
        org_id: auth.org_id,
        project_id: req.project_id,
        trigger_config: req.trigger_config,
        function_name: req.function_name,
        enabled: req.enabled.unwrap_or(true),
        created_at: chrono::Utc::now(),
        metadata: req.metadata.unwrap_or_default(),
    };

    state.webrtc_state.database_trigger_manager.register_trigger(trigger.clone()).await?;

    tracing::info!(
        trigger_id = %trigger.id,
        trigger_name = %trigger.name,
        function_name = %trigger.function_name,
        tables = ?trigger.trigger_config.tables,
        user_id = ?auth.user_id,
        org_id = auth.org_id,
        "Created database trigger"
    );

    Ok(Json(DatabaseTriggerResponse::from(trigger)))
}

/// List database triggers for the organization
pub async fn list_database_triggers_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
) -> Result<Json<Vec<DatabaseTriggerResponse>>, WebRTCError> {
    crate::ensure_permission(
        &state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs,
        zpermissions::Crud::READ,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    let triggers = state.webrtc_state.database_trigger_manager.get_org_triggers(auth.org_id).await;
    let responses: Vec<DatabaseTriggerResponse> = triggers.into_iter().map(DatabaseTriggerResponse::from).collect();

    Ok(Json(responses))
}

/// Get a specific database trigger
pub async fn get_database_trigger_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Path(trigger_id): Path<String>,
) -> Result<Json<DatabaseTriggerResponse>, WebRTCError> {
    crate::ensure_permission(
        &state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs,
        zpermissions::Crud::READ,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    let triggers = state.webrtc_state.database_trigger_manager.get_org_triggers(auth.org_id).await;
    let trigger = triggers.into_iter()
        .find(|t| t.id == trigger_id)
        .ok_or(WebRTCError::SessionNotFound)?;

    Ok(Json(DatabaseTriggerResponse::from(trigger)))
}

/// Delete a database trigger
pub async fn delete_database_trigger_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Path(trigger_id): Path<String>,
) -> Result<Json<serde_json::Value>, WebRTCError> {
    crate::ensure_permission(
        state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs,
        zpermissions::Crud::DELETE,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    // Verify the trigger belongs to this org before deletion
    let triggers = state.webrtc_state.database_trigger_manager.get_org_triggers(auth.org_id).await;
    let _trigger = triggers.into_iter()
        .find(|t| t.id == trigger_id)
        .ok_or(WebRTCError::SessionNotFound)?;

    state.webrtc_state.database_trigger_manager.remove_trigger(&trigger_id).await?;

    tracing::info!(
        trigger_id = %trigger_id,
        user_id = ?auth.user_id,
        org_id = auth.org_id,
        "Deleted database trigger"
    );

    Ok(Json(serde_json::json!({ "success": true })))
}

/// Simulate a database change event (for testing)
pub async fn simulate_database_change_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Json(event): Json<super::DatabaseChangeEvent>,
) -> Result<Json<serde_json::Value>, WebRTCError> {
    crate::ensure_permission(
        state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs,
        zpermissions::Crud::CREATE,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    // Add user context to the event
    let mut event_with_context = event;
    event_with_context.user_context = Some(super::DatabaseUserContext {
        user_id: auth.user_id.map(|id| id.to_string()),
        org_id: auth.org_id,
        session_id: None,
        connection_info: HashMap::new(),
    });

    state.webrtc_state.database_trigger_manager.on_database_change(event_with_context).await?;

    tracing::info!(
        user_id = ?auth.user_id,
        org_id = auth.org_id,
        "Simulated database change event"
    );

    Ok(Json(serde_json::json!({ "success": true, "message": "Database change event processed" })))
}

/// Real-time Streaming Management Handlers

/// Subscribe a WebRTC session to a streaming channel
pub async fn subscribe_to_streaming_channel_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Path(session_id): Path<String>,
    Json(req): Json<super::SubscribeRequest>,
) -> Result<Json<super::SubscribeResponse>, WebRTCError> {
    // Check permissions
    crate::ensure_permission(
        &state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs,
        zpermissions::Crud::READ,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    // Get the WebRTC session
    let session = state.webrtc_state.get_session(&session_id).await
        .ok_or(WebRTCError::SessionNotFound)?;

    // Verify session belongs to this org and user
    if session.org_id != auth.org_id {
        return Err(WebRTCError::PermissionDenied);
    }

    let auth_user_id = auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone());
    if session.user_id != auth_user_id {
        return Err(WebRTCError::PermissionDenied);
    }

    // Parse channel request
    let channel = match req.channel_type.as_str() {
        "org_handler_executions" => {
            super::StreamingChannel::OrgHandlerExecutions(auth.org_id)
        }
        "project_handler_executions" => {
            let project_id = req.channel_params.get("project_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| WebRTCError::InternalError("Missing project_id parameter".to_string()))?;
            super::StreamingChannel::ProjectHandlerExecutions {
                org_id: auth.org_id,
                project_id: project_id.to_string(),
            }
        }
        "room_handler_executions" => {
            let room_id = req.channel_params.get("room_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| WebRTCError::InternalError("Missing room_id parameter".to_string()))?;
            super::StreamingChannel::RoomHandlerExecutions {
                room_id: room_id.to_string(),
            }
        }
        "user_handler_executions" => {
            super::StreamingChannel::UserHandlerExecutions {
                user_id: auth_user_id.clone(),
            }
        }
        "database_trigger_events" => {
            super::StreamingChannel::DatabaseTriggerEvents(auth.org_id)
        }
        "connection_events" => {
            super::StreamingChannel::ConnectionEvents(auth.org_id)
        }
        _ => return Err(WebRTCError::InternalError("Unknown channel type".to_string())),
    };

    let filters = req.filters.unwrap_or_default();

    // Subscribe to the channel
    state.webrtc_state.realtime_streaming_manager
        .subscribe_session(&session, channel.clone(), filters.clone()).await?;

    let subscription_id = uuid::Uuid::new_v4().to_string();

    tracing::info!(
        session_id = %session_id,
        user_id = %auth_user_id,
        org_id = auth.org_id,
        channel = ?channel,
        subscription_id = %subscription_id,
        "Subscribed WebRTC session to streaming channel"
    );

    Ok(Json(super::SubscribeResponse {
        success: true,
        channel: format!("{:?}", channel),
        subscription_id,
        filters_applied: filters,
    }))
}

/// Unsubscribe from a streaming channel
pub async fn unsubscribe_from_streaming_channel_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
    Path((session_id, channel_type)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, WebRTCError> {
    // Check permissions
    crate::ensure_permission(
        &state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs,
        zpermissions::Crud::READ,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    // Get the WebRTC session
    let session = state.webrtc_state.get_session(&session_id).await
        .ok_or(WebRTCError::SessionNotFound)?;

    // Verify session belongs to this org and user
    if session.org_id != auth.org_id {
        return Err(WebRTCError::PermissionDenied);
    }

    let auth_user_id = auth.user_id.map(|id| id.to_string()).unwrap_or_else(|| auth.token_id.clone());
    if session.user_id != auth_user_id {
        return Err(WebRTCError::PermissionDenied);
    }

    // Parse channel type (simplified - would need parameters for specific channels)
    let channel = match channel_type.as_str() {
        "org_handler_executions" => {
            super::StreamingChannel::OrgHandlerExecutions(auth.org_id)
        }
        "database_trigger_events" => {
            super::StreamingChannel::DatabaseTriggerEvents(auth.org_id)
        }
        "connection_events" => {
            super::StreamingChannel::ConnectionEvents(auth.org_id)
        }
        _ => return Err(WebRTCError::InternalError("Unknown channel type".to_string())),
    };

    // Unsubscribe from the channel
    state.webrtc_state.realtime_streaming_manager
        .unsubscribe_session(&session_id, &channel).await?;

    tracing::info!(
        session_id = %session_id,
        user_id = %auth_user_id,
        org_id = auth.org_id,
        channel = ?channel,
        "Unsubscribed WebRTC session from streaming channel"
    );

    Ok(Json(serde_json::json!({ "success": true })))
}

/// Get streaming statistics (admin only)
pub async fn get_streaming_statistics_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
) -> Result<Json<super::StreamingStatistics>, WebRTCError> {
    // Check admin permissions
    crate::ensure_permission(
        &state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs,
        zpermissions::Crud::READ,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    let stats = state.webrtc_state.realtime_streaming_manager.get_statistics().await;

    tracing::info!(
        user_id = ?auth.user_id,
        org_id = auth.org_id,
        channel_count = stats.channel_count,
        total_subscriptions = stats.total_subscriptions,
        "Retrieved streaming statistics"
    );

    Ok(Json(stats))
}

/// Send heartbeat to all streaming channels (system endpoint)
pub async fn send_streaming_heartbeat_handler(
    auth: AuthSession,
    State(state): State<crate::AppState>,
) -> Result<Json<serde_json::Value>, WebRTCError> {
    // Check admin permissions
    crate::ensure_permission(
        state.permissions,
        auth.org_id,
        &crate::principals_for(&auth),
        zpermissions::ResourceKind::Jobs,
        zpermissions::Crud::CREATE,
    ).map_err(|e| match e {
        axum::http::StatusCode::FORBIDDEN => WebRTCError::PermissionDenied,
        _ => WebRTCError::InternalError("Permission check failed".to_string()),
    })?;

    state.webrtc_state.realtime_streaming_manager.send_heartbeat().await?;

    tracing::info!(
        user_id = ?auth.user_id,
        org_id = auth.org_id,
        "Sent streaming heartbeat"
    );

    Ok(Json(serde_json::json!({ "success": true, "message": "Heartbeat sent to all streaming channels" })))
}