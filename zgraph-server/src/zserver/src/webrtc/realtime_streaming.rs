use super::{TriggerEvent, TriggerExecution, WebRTCSession, WebRTCError, HandlerExecution};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

/// Real-time streaming system for handler execution results
pub struct RealtimeStreamingManager {
    /// Broadcast channels for different event types
    channels: Arc<RwLock<HashMap<StreamingChannel, broadcast::Sender<StreamingMessage>>>>,
    /// Session subscriptions to channels
    subscriptions: Arc<RwLock<HashMap<String, Vec<StreamingSubscription>>>>,
    /// Maximum number of events to buffer per channel
    max_buffer_size: usize,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum StreamingChannel {
    /// All handler executions for an organization
    OrgHandlerExecutions(u64),
    /// Handler executions for a specific project
    ProjectHandlerExecutions { org_id: u64, project_id: String },
    /// Handler executions for a specific room
    RoomHandlerExecutions { room_id: String },
    /// Handler executions for a specific user
    UserHandlerExecutions { user_id: String },
    /// Database trigger events
    DatabaseTriggerEvents(u64),
    /// Connection events (join/leave/status changes)
    ConnectionEvents(u64),
    /// System-wide events (admin only)
    SystemEvents,
}

#[derive(Debug, Clone)]
pub struct StreamingSubscription {
    pub session_id: String,
    pub channel: StreamingChannel,
    pub filters: StreamingFilters,
    pub subscribed_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingFilters {
    /// Only include events with these handler types
    pub handler_types: Option<Vec<String>>,
    /// Only include events with these execution statuses
    pub execution_statuses: Option<Vec<String>>,
    /// Only include events above this priority level
    pub min_priority: Option<String>,
    /// Only include events matching these tags
    pub required_tags: Option<Vec<String>>,
    /// Exclude events matching these patterns
    pub exclude_patterns: Option<Vec<String>>,
}

impl Default for StreamingFilters {
    fn default() -> Self {
        Self {
            handler_types: None,
            execution_statuses: None,
            min_priority: None,
            required_tags: None,
            exclude_patterns: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamingMessage {
    /// Unique message ID
    pub id: String,
    /// Message type
    pub message_type: StreamingMessageType,
    /// Timestamp when the event occurred
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// The actual event data
    pub payload: serde_json::Value,
    /// Metadata about the event
    pub metadata: StreamingMetadata,
}

#[derive(Debug, Clone, Serialize)]
pub enum StreamingMessageType {
    HandlerExecution,
    TriggerExecution,
    DatabaseTriggerEvent,
    ConnectionEvent,
    SystemEvent,
    HeartbeatPing,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamingMetadata {
    pub org_id: u64,
    pub project_id: Option<String>,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub room_id: Option<String>,
    pub priority: String,
    pub tags: Vec<String>,
    pub source_type: String,
}

impl RealtimeStreamingManager {
    pub fn new(max_buffer_size: usize) -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            max_buffer_size,
        }
    }

    /// Subscribe a WebRTC session to a streaming channel
    pub async fn subscribe_session(
        &self,
        session: &Arc<WebRTCSession>,
        channel: StreamingChannel,
        filters: StreamingFilters,
    ) -> Result<(), WebRTCError> {
        // Ensure channel exists
        let sender = self.get_or_create_channel(&channel).await;

        // Create subscription
        let subscription = StreamingSubscription {
            session_id: session.id.clone(),
            channel: channel.clone(),
            filters,
            subscribed_at: chrono::Utc::now(),
        };

        // Store subscription
        self.subscriptions
            .write()
            .await
            .entry(session.id.clone())
            .or_insert_with(Vec::new)
            .push(subscription);

        // Set up receiver for the session
        let mut receiver = sender.subscribe();
        let session_weak = Arc::downgrade(session);
        let session_id = session.id.clone();

        // Spawn task to forward messages to WebRTC session
        tokio::spawn(async move {
            while let Ok(message) = receiver.recv().await {
                if let Some(session_arc) = session_weak.upgrade() {
                    // Check if session still exists and is active
                    if session_arc.is_data_channel_ready("events").await {
                        // Serialize message and send through events channel
                        match serde_json::to_string(&message) {
                            Ok(json_message) => {
                                if let Err(e) = session_arc.send_data_channel_message("events", &json_message).await {
                                    tracing::warn!(
                                        session_id = %session_id,
                                        error = %e,
                                        "Failed to send streaming message to WebRTC session"
                                    );
                                    break; // Stop forwarding if channel is broken
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    session_id = %session_id,
                                    error = %e,
                                    "Failed to serialize streaming message"
                                );
                            }
                        }
                    } else {
                        // Data channel not ready, skip this message
                        continue;
                    }
                } else {
                    // Session has been dropped, stop forwarding
                    break;
                }
            }

            tracing::debug!(
                session_id = %session_id,
                "Stopped streaming message forwarding for session"
            );
        });

        tracing::info!(
            session_id = %session.id,
            channel = ?channel,
            "Subscribed WebRTC session to streaming channel"
        );

        Ok(())
    }

    /// Unsubscribe a session from a specific channel
    pub async fn unsubscribe_session(
        &self,
        session_id: &str,
        channel: &StreamingChannel,
    ) -> Result<(), WebRTCError> {
        let mut subscriptions = self.subscriptions.write().await;
        if let Some(session_subscriptions) = subscriptions.get_mut(session_id) {
            session_subscriptions.retain(|sub| &sub.channel != channel);
            if session_subscriptions.is_empty() {
                subscriptions.remove(session_id);
            }
        }

        tracing::info!(
            session_id = %session_id,
            channel = ?channel,
            "Unsubscribed WebRTC session from streaming channel"
        );

        Ok(())
    }

    /// Unsubscribe a session from all channels
    pub async fn unsubscribe_session_all(&self, session_id: &str) -> Result<(), WebRTCError> {
        self.subscriptions.write().await.remove(session_id);

        tracing::info!(
            session_id = %session_id,
            "Unsubscribed WebRTC session from all streaming channels"
        );

        Ok(())
    }

    /// Broadcast a handler execution event
    pub async fn broadcast_handler_execution(
        &self,
        execution: &HandlerExecution,
        event_context: &StreamingMetadata,
    ) -> Result<(), WebRTCError> {
        let message = StreamingMessage {
            id: uuid::Uuid::new_v4().to_string(),
            message_type: StreamingMessageType::HandlerExecution,
            timestamp: execution.started_at,
            payload: serde_json::json!({
                "handler_id": execution.handler_id,
                "execution_id": execution.execution_id,
                "status": execution.status,
                "rule_result": execution.rule_result,
                "actions_executed": execution.actions_executed,
                "error": execution.error,
                "started_at": execution.started_at,
                "completed_at": execution.completed_at,
            }),
            metadata: event_context.clone(),
        };

        // Determine which channels to broadcast to
        let channels_to_broadcast = vec![
            StreamingChannel::OrgHandlerExecutions(event_context.org_id),
            StreamingChannel::SystemEvents,
        ];

        // Add project-specific channel if available
        let mut all_channels = channels_to_broadcast;
        if let Some(project_id) = &event_context.project_id {
            all_channels.push(StreamingChannel::ProjectHandlerExecutions {
                org_id: event_context.org_id,
                project_id: project_id.clone(),
            });
        }

        // Add room-specific channel if available
        if let Some(room_id) = &event_context.room_id {
            all_channels.push(StreamingChannel::RoomHandlerExecutions {
                room_id: room_id.clone(),
            });
        }

        // Add user-specific channel if available
        if let Some(user_id) = &event_context.user_id {
            all_channels.push(StreamingChannel::UserHandlerExecutions {
                user_id: user_id.clone(),
            });
        }

        // Broadcast to all relevant channels
        for channel in all_channels {
            self.broadcast_to_channel(&channel, &message).await?;
        }

        Ok(())
    }

    /// Broadcast a trigger execution event
    pub async fn broadcast_trigger_execution(
        &self,
        execution: &TriggerExecution,
        event_context: &StreamingMetadata,
    ) -> Result<(), WebRTCError> {
        let message = StreamingMessage {
            id: uuid::Uuid::new_v4().to_string(),
            message_type: StreamingMessageType::TriggerExecution,
            timestamp: execution.started_at,
            payload: serde_json::json!({
                "trigger_id": execution.trigger_id,
                "execution_id": execution.execution_id,
                "status": execution.status,
                "result": execution.result,
                "error": execution.error,
                "started_at": execution.started_at,
                "completed_at": execution.completed_at,
                "context": execution.context,
            }),
            metadata: event_context.clone(),
        };

        // Broadcast to organization channel
        self.broadcast_to_channel(
            &StreamingChannel::OrgHandlerExecutions(event_context.org_id),
            &message,
        ).await?;

        Ok(())
    }

    /// Broadcast a database trigger event
    pub async fn broadcast_database_trigger_event(
        &self,
        table_name: &str,
        operation: &super::DatabaseOperation,
        affected_rows: u32,
        org_id: u64,
        user_id: Option<&str>,
    ) -> Result<(), WebRTCError> {
        let metadata = StreamingMetadata {
            org_id,
            project_id: None,
            user_id: user_id.map(|s| s.to_string()),
            session_id: None,
            room_id: None,
            priority: "normal".to_string(),
            tags: vec!["database".to_string(), "trigger".to_string()],
            source_type: "database_trigger".to_string(),
        };

        let message = StreamingMessage {
            id: uuid::Uuid::new_v4().to_string(),
            message_type: StreamingMessageType::DatabaseTriggerEvent,
            timestamp: chrono::Utc::now(),
            payload: serde_json::json!({
                "table_name": table_name,
                "operation": format!("{:?}", operation),
                "affected_rows": affected_rows,
                "user_id": user_id,
            }),
            metadata,
        };

        self.broadcast_to_channel(
            &StreamingChannel::DatabaseTriggerEvents(org_id),
            &message,
        ).await?;

        Ok(())
    }

    /// Broadcast a connection event
    pub async fn broadcast_connection_event(
        &self,
        event_type: &str,
        session_id: &str,
        user_id: &str,
        org_id: u64,
        room_id: Option<&str>,
    ) -> Result<(), WebRTCError> {
        let metadata = StreamingMetadata {
            org_id,
            project_id: None,
            user_id: Some(user_id.to_string()),
            session_id: Some(session_id.to_string()),
            room_id: room_id.map(|s| s.to_string()),
            priority: "normal".to_string(),
            tags: vec!["connection".to_string(), event_type.to_string()],
            source_type: "webrtc_connection".to_string(),
        };

        let message = StreamingMessage {
            id: uuid::Uuid::new_v4().to_string(),
            message_type: StreamingMessageType::ConnectionEvent,
            timestamp: chrono::Utc::now(),
            payload: serde_json::json!({
                "event_type": event_type,
                "session_id": session_id,
                "user_id": user_id,
                "room_id": room_id,
            }),
            metadata,
        };

        // Broadcast to relevant channels
        let channels = vec![
            StreamingChannel::ConnectionEvents(org_id),
            StreamingChannel::UserHandlerExecutions { user_id: user_id.to_string() },
        ];

        // Add room channel if available
        let mut all_channels = channels;
        if let Some(room_id) = room_id {
            all_channels.push(StreamingChannel::RoomHandlerExecutions {
                room_id: room_id.to_string(),
            });
        }

        for channel in all_channels {
            self.broadcast_to_channel(&channel, &message).await?;
        }

        Ok(())
    }

    /// Send periodic heartbeat to maintain connections
    pub async fn send_heartbeat(&self) -> Result<(), WebRTCError> {
        let message = StreamingMessage {
            id: uuid::Uuid::new_v4().to_string(),
            message_type: StreamingMessageType::HeartbeatPing,
            timestamp: chrono::Utc::now(),
            payload: serde_json::json!({
                "ping": "heartbeat",
                "server_time": chrono::Utc::now(),
            }),
            metadata: StreamingMetadata {
                org_id: 0, // Global heartbeat
                project_id: None,
                user_id: None,
                session_id: None,
                room_id: None,
                priority: "low".to_string(),
                tags: vec!["heartbeat".to_string()],
                source_type: "system".to_string(),
            },
        };

        // Send heartbeat to all channels
        let channels = self.channels.read().await;
        for sender in channels.values() {
            let _ = sender.send(message.clone()); // Ignore errors for heartbeat
        }

        Ok(())
    }

    async fn get_or_create_channel(&self, channel: &StreamingChannel) -> broadcast::Sender<StreamingMessage> {
        let mut channels = self.channels.write().await;

        if let Some(sender) = channels.get(channel) {
            sender.clone()
        } else {
            let (sender, _) = broadcast::channel(self.max_buffer_size);
            channels.insert(channel.clone(), sender.clone());

            tracing::info!(
                channel = ?channel,
                buffer_size = self.max_buffer_size,
                "Created new streaming channel"
            );

            sender
        }
    }

    async fn broadcast_to_channel(
        &self,
        channel: &StreamingChannel,
        message: &StreamingMessage,
    ) -> Result<(), WebRTCError> {
        let sender = self.get_or_create_channel(channel).await;

        match sender.send(message.clone()) {
            Ok(subscriber_count) => {
                tracing::debug!(
                    channel = ?channel,
                    subscriber_count = subscriber_count,
                    message_id = %message.id,
                    "Broadcasted message to streaming channel"
                );
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    channel = ?channel,
                    error = %e,
                    "Failed to broadcast message to streaming channel (no subscribers)"
                );
                Ok(()) // Not an error if no subscribers
            }
        }
    }

    /// Get streaming statistics
    pub async fn get_statistics(&self) -> StreamingStatistics {
        let channels = self.channels.read().await;
        let subscriptions = self.subscriptions.read().await;

        StreamingStatistics {
            channel_count: channels.len(),
            total_subscriptions: subscriptions.values().map(|subs| subs.len()).sum(),
            channels: channels.keys().cloned().collect(),
            subscription_count_by_channel: {
                let mut counts = HashMap::new();
                for subs in subscriptions.values() {
                    for sub in subs {
                        *counts.entry(sub.channel.clone()).or_insert(0) += 1;
                    }
                }
                counts
            },
        }
    }

    /// Clean up expired subscriptions
    pub async fn cleanup_expired_subscriptions(&self, max_age_hours: i64) -> usize {
        let mut subscriptions = self.subscriptions.write().await;
        let cutoff_time = chrono::Utc::now() - chrono::Duration::hours(max_age_hours);
        let mut removed_count = 0;

        subscriptions.retain(|_session_id, session_subs| {
            let original_len = session_subs.len();
            session_subs.retain(|sub| sub.subscribed_at > cutoff_time);
            removed_count += original_len - session_subs.len();
            !session_subs.is_empty()
        });

        if removed_count > 0 {
            tracing::info!(
                removed_subscriptions = removed_count,
                max_age_hours = max_age_hours,
                "Cleaned up expired streaming subscriptions"
            );
        }

        removed_count
    }
}

#[derive(Debug, Serialize)]
pub struct StreamingStatistics {
    pub channel_count: usize,
    pub total_subscriptions: usize,
    pub channels: Vec<StreamingChannel>,
    pub subscription_count_by_channel: HashMap<StreamingChannel, usize>,
}

/// Request to subscribe to a streaming channel
#[derive(Debug, Deserialize)]
pub struct SubscribeRequest {
    pub channel_type: String,
    pub channel_params: serde_json::Value,
    pub filters: Option<StreamingFilters>,
}

/// Response for streaming subscription
#[derive(Debug, Serialize)]
pub struct SubscribeResponse {
    pub success: bool,
    pub channel: String,
    pub subscription_id: String,
    pub filters_applied: StreamingFilters,
}