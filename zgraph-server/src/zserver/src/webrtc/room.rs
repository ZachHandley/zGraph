use super::{WebRTCConfig, WebRTCError, WebRTCSession};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Represents a WebRTC room for multi-participant conferences
#[derive(Debug)]
pub struct WebRTCRoom {
    pub id: String,
    pub org_id: u64,
    pub config: WebRTCConfig,
    pub participants: Arc<RwLock<HashMap<String, Arc<WebRTCSession>>>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub metadata: Arc<RwLock<RoomMetadata>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomMetadata {
    pub name: Option<String>,
    pub description: Option<String>,
    pub max_participants: Option<usize>,
    pub recording_enabled: bool,
    pub allow_screen_share: bool,
    pub allow_chat: bool,
    pub moderator_only_functions: Vec<String>,
    pub tags: Vec<String>,
}

impl Default for RoomMetadata {
    fn default() -> Self {
        Self {
            name: None,
            description: None,
            max_participants: None,
            recording_enabled: false,
            allow_screen_share: true,
            allow_chat: true,
            moderator_only_functions: Vec::new(),
            tags: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParticipantInfo {
    pub session_id: String,
    pub user_id: String,
    pub display_name: Option<String>,
    pub role: ParticipantRole,
    pub joined_at: chrono::DateTime<chrono::Utc>,
    pub media_state: MediaState,
    pub permissions: ParticipantPermissions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParticipantRole {
    Moderator,
    Presenter,
    Participant,
    Observer,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaState {
    pub audio_enabled: bool,
    pub video_enabled: bool,
    pub screen_sharing: bool,
    pub recording: bool,
}

impl Default for MediaState {
    fn default() -> Self {
        Self {
            audio_enabled: false,
            video_enabled: false,
            screen_sharing: false,
            recording: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParticipantPermissions {
    pub can_speak: bool,
    pub can_share_screen: bool,
    pub can_record: bool,
    pub can_trigger_functions: bool,
    pub can_modify_room: bool,
    pub can_kick_participants: bool,
}

impl Default for ParticipantPermissions {
    fn default() -> Self {
        Self {
            can_speak: true,
            can_share_screen: true,
            can_record: false,
            can_trigger_functions: false,
            can_modify_room: false,
            can_kick_participants: false,
        }
    }
}

impl ParticipantPermissions {
    pub fn default_for_role(role: &ParticipantRole) -> Self {
        match role {
            ParticipantRole::Moderator => Self {
                can_speak: true,
                can_share_screen: true,
                can_record: true,
                can_trigger_functions: true,
                can_modify_room: true,
                can_kick_participants: true,
            },
            ParticipantRole::Presenter => Self {
                can_speak: true,
                can_share_screen: true,
                can_record: true,
                can_trigger_functions: true,
                can_modify_room: false,
                can_kick_participants: false,
            },
            ParticipantRole::Participant => Self {
                can_speak: true,
                can_share_screen: true,
                can_record: false,
                can_trigger_functions: false,
                can_modify_room: false,
                can_kick_participants: false,
            },
            ParticipantRole::Observer => Self {
                can_speak: false,
                can_share_screen: false,
                can_record: false,
                can_trigger_functions: false,
                can_modify_room: false,
                can_kick_participants: false,
            },
        }
    }
}

impl WebRTCRoom {
    pub fn new(id: String, org_id: u64, config: WebRTCConfig) -> Self {
        Self {
            id,
            org_id,
            config,
            participants: Arc::new(RwLock::new(HashMap::new())),
            created_at: chrono::Utc::now(),
            metadata: Arc::new(RwLock::new(RoomMetadata::default())),
        }
    }

    pub async fn add_participant(&self, session: Arc<WebRTCSession>) -> Result<(), WebRTCError> {
        let mut participants = self.participants.write().await;

        // Check participant limit
        if participants.len() >= self.config.max_participants_per_room {
            return Err(WebRTCError::ParticipantLimitExceeded);
        }

        // Check room-specific limit
        let metadata = self.metadata.read().await;
        if let Some(max) = metadata.max_participants {
            if participants.len() >= max {
                return Err(WebRTCError::ParticipantLimitExceeded);
            }
        }

        participants.insert(session.id.clone(), session);
        Ok(())
    }

    pub async fn remove_participant(&self, session_id: &str) -> Option<Arc<WebRTCSession>> {
        self.participants.write().await.remove(session_id)
    }

    pub async fn get_participant(&self, session_id: &str) -> Option<Arc<WebRTCSession>> {
        self.participants.read().await.get(session_id).cloned()
    }

    pub async fn list_participants(&self) -> Vec<ParticipantInfo> {
        let participants = self.participants.read().await;
        let mut result = Vec::new();

        for session in participants.values() {
            result.push(ParticipantInfo {
                session_id: session.id.clone(),
                user_id: session.user_id.clone(),
                display_name: session.display_name.clone(),
                role: session.role.clone(),
                joined_at: session.created_at,
                media_state: session.media_state.read().await.clone(),
                permissions: session.permissions.clone(),
            });
        }

        result
    }

    pub async fn participant_count(&self) -> usize {
        self.participants.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.participants.read().await.is_empty()
    }

    pub async fn update_metadata(&self, metadata: RoomMetadata) {
        *self.metadata.write().await = metadata;
    }

    pub async fn get_metadata(&self) -> RoomMetadata {
        self.metadata.read().await.clone()
    }

    /// Broadcast message to all participants in the room
    pub async fn broadcast_message(&self, message: &str, exclude_session: Option<&str>) {
        let participants = self.participants.read().await;

        for (session_id, session) in participants.iter() {
            if let Some(exclude) = exclude_session {
                if session_id == exclude {
                    continue;
                }
            }

            session.send_data_channel_message("events", message).await;
        }
    }

    /// Broadcast to specific participant role
    pub async fn broadcast_to_role(&self, message: &str, role: ParticipantRole) {
        let participants = self.participants.read().await;

        for session in participants.values() {
            if std::mem::discriminant(&session.role) == std::mem::discriminant(&role) {
                session.send_data_channel_message("events", message).await;
            }
        }
    }

    /// Get participants with specific permission
    pub async fn get_participants_with_permission<F>(&self, permission_check: F) -> Vec<Arc<WebRTCSession>>
    where
        F: Fn(&ParticipantPermissions) -> bool,
    {
        let participants = self.participants.read().await;
        participants
            .values()
            .filter(|session| permission_check(&session.permissions))
            .cloned()
            .collect()
    }

    /// Update participant media state
    pub async fn update_participant_media_state(
        &self,
        session_id: &str,
        media_state: MediaState,
    ) -> Result<(), WebRTCError> {
        let participants = self.participants.read().await;
        if let Some(session) = participants.get(session_id) {
            *session.media_state.write().await = media_state.clone();

            // Broadcast media state change to other participants
            let message = serde_json::json!({
                "type": "participant_media_changed",
                "session_id": session_id,
                "media_state": media_state
            });

            drop(participants); // Release the lock before broadcasting
            self.broadcast_message(&message.to_string(), Some(session_id)).await;
            Ok(())
        } else {
            Err(WebRTCError::SessionNotFound)
        }
    }

    /// Get room statistics
    pub async fn get_statistics(&self) -> RoomStatistics {
        let participants = self.participants.read().await;
        let mut audio_enabled = 0;
        let mut video_enabled = 0;
        let mut screen_sharing = 0;

        for session in participants.values() {
            let media_state = session.media_state.read().await;
            if media_state.audio_enabled {
                audio_enabled += 1;
            }
            if media_state.video_enabled {
                video_enabled += 1;
            }
            if media_state.screen_sharing {
                screen_sharing += 1;
            }
        }

        RoomStatistics {
            participant_count: participants.len(),
            audio_enabled_count: audio_enabled,
            video_enabled_count: video_enabled,
            screen_sharing_count: screen_sharing,
            created_at: self.created_at,
            duration_seconds: chrono::Utc::now()
                .signed_duration_since(self.created_at)
                .num_seconds(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct RoomStatistics {
    pub participant_count: usize,
    pub audio_enabled_count: usize,
    pub video_enabled_count: usize,
    pub screen_sharing_count: usize,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub duration_seconds: i64,
}

/// Room creation request
#[derive(Debug, Deserialize)]
pub struct CreateRoomRequest {
    pub room_id: Option<String>,
    pub metadata: Option<RoomMetadata>,
}

/// Room join request
#[derive(Debug, Deserialize)]
pub struct JoinRoomRequest {
    pub room_id: String,
    pub display_name: Option<String>,
    pub role: Option<ParticipantRole>,
    pub ice_servers: Option<Vec<crate::IceServerConfig>>,
    pub media_constraints: Option<super::MediaConstraints>,
}

