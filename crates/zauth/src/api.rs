use std::collections::HashMap;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use zcore_storage::Store;
use zpermissions::{ResourceKind, CrudAction};

use crate::{
    AuthSession, IntegratedAuthService, SessionService, TeamService, DelegationService,
    models::{
        SessionRecord, TeamRecord, TeamRole, TeamType, TeamSettings,
        DelegationScope, DelegationCondition,
    },
    integration::TeamResourceInfo,
    delegation::{DelegationContext, EffectiveRole},
};

/// Enhanced session management API endpoints
pub struct SessionApi;

impl SessionApi {
    /// Get current user's sessions
    pub async fn get_user_sessions(
        session: AuthSession,
        State(store): State<&'static Store>,
    ) -> Result<Json<Vec<SessionSummary>>, ApiError> {
        let service = SessionService::new(store);

        if let Some(user_id) = session.user_id {
            let sessions = service.get_user_sessions(user_id, session.org_id)?;
            let summaries: Vec<SessionSummary> = sessions.into_iter()
                .map(|s| SessionSummary::from(s))
                .collect();

            Ok(Json(summaries))
        } else {
            Err(ApiError::Unauthorized)
        }
    }

    /// Get sessions by device fingerprint
    pub async fn get_device_sessions(
        session: AuthSession,
        Path(device_fingerprint): Path<String>,
        State(store): State<&'static Store>,
    ) -> Result<Json<Vec<SessionSummary>>, ApiError> {
        let service = SessionService::new(store);

        if session.user_id.is_none() {
            return Err(ApiError::Unauthorized);
        }

        let sessions = service.get_device_sessions(&device_fingerprint, session.org_id)?;
        let summaries: Vec<SessionSummary> = sessions.into_iter()
            .map(|s| SessionSummary::from(s))
            .collect();

        Ok(Json(summaries))
    }

    /// Revoke a specific session
    pub async fn revoke_session(
        session: AuthSession,
        Path(session_id): Path<String>,
        Json(request): Json<RevokeSessionRequest>,
        State(store): State<&'static Store>,
    ) -> Result<Json<ApiResponse>, ApiError> {
        let service = SessionService::new(store);

        let session_id = Ulid::from_string(&session_id)
            .map_err(|_| ApiError::BadRequest("Invalid session ID".to_string()))?;

        service.revoke_session(&session_id, session.user_id, request.reason)?;

        Ok(Json(ApiResponse {
            success: true,
            message: "Session revoked successfully".to_string(),
        }))
    }

    /// Revoke all sessions except current
    pub async fn revoke_other_sessions(
        session: AuthSession,
        Json(request): Json<RevokeSessionRequest>,
        State(store): State<&'static Store>,
    ) -> Result<Json<RevokeOtherSessionsResponse>, ApiError> {
        let service = SessionService::new(store);

        if let (Some(user_id), Some(current_session_id)) = (session.user_id, session.session_id) {
            let revoked_count = service.revoke_other_sessions(
                user_id,
                session.org_id,
                &current_session_id,
                Some(user_id),
                request.reason,
            )?;

            Ok(Json(RevokeOtherSessionsResponse {
                success: true,
                revoked_count,
                message: format!("Revoked {} sessions", revoked_count),
            }))
        } else {
            Err(ApiError::Unauthorized)
        }
    }

    /// Mark session as suspicious
    pub async fn mark_session_suspicious(
        session: AuthSession,
        Path(session_id): Path<String>,
        Json(request): Json<MarkSuspiciousRequest>,
        State(store): State<&'static Store>,
    ) -> Result<Json<ApiResponse>, ApiError> {
        let service = SessionService::new(store);

        // Only admins can mark sessions as suspicious
        if !session.roles.contains(&"admin".to_string()) {
            return Err(ApiError::Forbidden);
        }

        let session_id = Ulid::from_string(&session_id)
            .map_err(|_| ApiError::BadRequest("Invalid session ID".to_string()))?;

        service.mark_suspicious(&session_id, &request.reason)?;

        Ok(Json(ApiResponse {
            success: true,
            message: "Session marked as suspicious".to_string(),
        }))
    }
}

/// Team management API endpoints
pub struct TeamApi;

impl TeamApi {
    /// Create a new team
    pub async fn create_team(
        session: AuthSession,
        Json(request): Json<CreateTeamRequest>,
        State(store): State<&'static Store>,
    ) -> Result<Json<TeamResponse>, ApiError> {
        let service = TeamService::new(store);

        if let Some(user_id) = session.user_id {
            let team = service.create_team(
                session.org_id,
                user_id,
                request.name,
                request.description,
                request.parent_team_id,
                request.team_type,
                request.settings,
            )?;

            Ok(Json(TeamResponse::from(team)))
        } else {
            Err(ApiError::Unauthorized)
        }
    }

    /// Get team by ID
    pub async fn get_team(
        session: AuthSession,
        Path(team_id): Path<String>,
        State(store): State<&'static Store>,
    ) -> Result<Json<TeamResponse>, ApiError> {
        let service = TeamService::new(store);

        let team_id = Ulid::from_string(&team_id)
            .map_err(|_| ApiError::BadRequest("Invalid team ID".to_string()))?;

        if let Some(team) = service.get_team(session.org_id, &team_id)? {
            // Check if user has access to this team
            if let Some(user_id) = session.user_id {
                if service.has_team_permission(session.org_id, &team_id, &user_id, &TeamRole::Viewer)? {
                    return Ok(Json(TeamResponse::from(team)));
                }
            }
            Err(ApiError::Forbidden)
        } else {
            Err(ApiError::NotFound("Team not found".to_string()))
        }
    }

    /// Get user's teams
    pub async fn get_user_teams(
        session: AuthSession,
        State(store): State<&'static Store>,
    ) -> Result<Json<Vec<UserTeamResponse>>, ApiError> {
        let service = TeamService::new(store);

        if let Some(user_id) = session.user_id {
            let user_teams = service.get_user_teams(session.org_id, &user_id)?;
            let responses: Vec<UserTeamResponse> = user_teams.into_iter()
                .map(|(team, member)| UserTeamResponse::from((team, member)))
                .collect();

            Ok(Json(responses))
        } else {
            Err(ApiError::Unauthorized)
        }
    }

    /// Invite user to team
    pub async fn invite_user(
        session: AuthSession,
        Path(team_id): Path<String>,
        Json(request): Json<InviteUserRequest>,
        State(store): State<&'static Store>,
    ) -> Result<Json<InvitationResponse>, ApiError> {
        let service = TeamService::new(store);

        let team_id = Ulid::from_string(&team_id)
            .map_err(|_| ApiError::BadRequest("Invalid team ID".to_string()))?;

        if let Some(user_id) = session.user_id {
            let invitation = service.invite_user(
                session.org_id,
                &team_id,
                user_id,
                request.email,
                request.role,
                request.message,
                request.expires_in_hours,
            )?;

            Ok(Json(InvitationResponse::from(invitation)))
        } else {
            Err(ApiError::Unauthorized)
        }
    }

    /// Accept team invitation
    pub async fn accept_invitation(
        session: AuthSession,
        Path(invitation_id): Path<String>,
        State(store): State<&'static Store>,
    ) -> Result<Json<ApiResponse>, ApiError> {
        let service = TeamService::new(store);

        let invitation_id = Ulid::from_string(&invitation_id)
            .map_err(|_| ApiError::BadRequest("Invalid invitation ID".to_string()))?;

        if let Some(user_id) = session.user_id {
            service.accept_invitation(&invitation_id, user_id)?;

            Ok(Json(ApiResponse {
                success: true,
                message: "Invitation accepted successfully".to_string(),
            }))
        } else {
            Err(ApiError::Unauthorized)
        }
    }

    /// Get team members
    pub async fn get_team_members(
        session: AuthSession,
        Path(team_id): Path<String>,
        State(store): State<&'static Store>,
    ) -> Result<Json<Vec<TeamMemberResponse>>, ApiError> {
        let service = TeamService::new(store);

        let team_id = Ulid::from_string(&team_id)
            .map_err(|_| ApiError::BadRequest("Invalid team ID".to_string()))?;

        // Check if user has access to view team members
        if let Some(user_id) = session.user_id {
            if service.has_team_permission(session.org_id, &team_id, &user_id, &TeamRole::Viewer)? {
                let members = service.get_team_members(&team_id)?;
                let responses: Vec<TeamMemberResponse> = members.into_iter()
                    .map(|m| TeamMemberResponse::from(m))
                    .collect();

                return Ok(Json(responses));
            }
        }

        Err(ApiError::Forbidden)
    }

    /// Remove team member
    pub async fn remove_member(
        session: AuthSession,
        Path((team_id, user_id)): Path<(String, String)>,
        State(store): State<&'static Store>,
    ) -> Result<Json<ApiResponse>, ApiError> {
        let service = TeamService::new(store);

        let team_id = Ulid::from_string(&team_id)
            .map_err(|_| ApiError::BadRequest("Invalid team ID".to_string()))?;
        let member_user_id = Ulid::from_string(&user_id)
            .map_err(|_| ApiError::BadRequest("Invalid user ID".to_string()))?;

        if let Some(remover_id) = session.user_id {
            service.remove_member(session.org_id, &team_id, &member_user_id, remover_id)?;

            Ok(Json(ApiResponse {
                success: true,
                message: "Member removed successfully".to_string(),
            }))
        } else {
            Err(ApiError::Unauthorized)
        }
    }
}

/// Integrated auth API endpoints
pub struct IntegratedApi;

impl IntegratedApi {
    /// Get effective permissions for current user
    pub async fn get_effective_permissions(
        session: AuthSession,
        State(store): State<&'static Store>,
    ) -> Result<Json<EffectivePermissionsResponse>, ApiError> {
        let service = IntegratedAuthService::new(store);

        if let Some(user_id) = session.user_id {
            let permissions = service.get_effective_permissions(&user_id, session.org_id)?;

            Ok(Json(EffectivePermissionsResponse { permissions }))
        } else {
            Err(ApiError::Unauthorized)
        }
    }

    /// Share resource with team
    pub async fn share_resource(
        session: AuthSession,
        Json(request): Json<ShareResourceRequest>,
        State(store): State<&'static Store>,
    ) -> Result<Json<ApiResponse>, ApiError> {
        let service = IntegratedAuthService::new(store);

        if let Some(user_id) = session.user_id {
            service.share_resource_with_team(
                user_id,
                session.org_id,
                &request.team_id,
                request.resource_type,
                request.resource_id,
                request.permissions,
            )?;

            Ok(Json(ApiResponse {
                success: true,
                message: "Resource shared successfully".to_string(),
            }))
        } else {
            Err(ApiError::Unauthorized)
        }
    }

    /// Get team resources
    pub async fn get_team_resources(
        session: AuthSession,
        Path(team_id): Path<String>,
        State(store): State<&'static Store>,
    ) -> Result<Json<Vec<TeamResourceInfo>>, ApiError> {
        let service = IntegratedAuthService::new(store);

        let team_id = Ulid::from_string(&team_id)
            .map_err(|_| ApiError::BadRequest("Invalid team ID".to_string()))?;

        // Check if user has access to view team resources
        if let Some(user_id) = session.user_id {
            let team_service = TeamService::new(store);
            if team_service.has_team_permission(session.org_id, &team_id, &user_id, &TeamRole::Viewer)? {
                let resources = service.get_team_resources(session.org_id, &team_id)?;
                return Ok(Json(resources));
            }
        }

        Err(ApiError::Forbidden)
    }

    /// Get team activity summary
    pub async fn get_team_activity(
        session: AuthSession,
        Path(team_id): Path<String>,
        Query(params): Query<ActivityQuery>,
        State(store): State<&'static Store>,
    ) -> Result<Json<crate::integration::TeamActivitySummary>, ApiError> {
        let service = IntegratedAuthService::new(store);

        let team_id = Ulid::from_string(&team_id)
            .map_err(|_| ApiError::BadRequest("Invalid team ID".to_string()))?;

        // Check if user has access to view team activity
        if let Some(user_id) = session.user_id {
            let team_service = TeamService::new(store);
            if team_service.has_team_permission(session.org_id, &team_id, &user_id, &TeamRole::Viewer)? {
                let activity = service.get_team_activity_summary(
                    session.org_id,
                    &team_id,
                    params.days.unwrap_or(7),
                )?;
                return Ok(Json(activity));
            }
        }

        Err(ApiError::Forbidden)
    }
}

/// Delegation API endpoints
pub struct DelegationApi;

impl DelegationApi {
    /// Create role delegation
    pub async fn create_delegation(
        session: AuthSession,
        Json(request): Json<CreateDelegationRequest>,
        State(store): State<&'static Store>,
    ) -> Result<Json<DelegationResponse>, ApiError> {
        let service = DelegationService::new(store);

        if let Some(user_id) = session.user_id {
            let delegation = service.create_delegation(
                user_id,
                request.delegatee_id,
                session.org_id,
                request.role,
                request.scope,
                request.conditions,
                request.expires_at,
            )?;

            Ok(Json(DelegationResponse::from(delegation)))
        } else {
            Err(ApiError::Unauthorized)
        }
    }

    /// Get effective roles for current user
    pub async fn get_effective_roles(
        session: AuthSession,
        Query(params): Query<ContextQuery>,
        State(store): State<&'static Store>,
    ) -> Result<Json<EffectiveRolesResponse>, ApiError> {
        let service = DelegationService::new(store);

        if let Some(user_id) = session.user_id {
            let context = DelegationContext {
                ip_address: params.ip_address,
                user_agent: params.user_agent,
                resource_access: None, // Would be populated from request context
                timestamp: chrono::Utc::now().timestamp(),
            };

            let roles = service.get_effective_roles(&user_id, session.org_id, &context)?;

            Ok(Json(EffectiveRolesResponse { roles }))
        } else {
            Err(ApiError::Unauthorized)
        }
    }

    /// Revoke delegation
    pub async fn revoke_delegation(
        session: AuthSession,
        Path(delegation_id): Path<String>,
        State(store): State<&'static Store>,
    ) -> Result<Json<ApiResponse>, ApiError> {
        let service = DelegationService::new(store);

        let delegation_id = Ulid::from_string(&delegation_id)
            .map_err(|_| ApiError::BadRequest("Invalid delegation ID".to_string()))?;

        if let Some(user_id) = session.user_id {
            service.revoke_delegation(&delegation_id, user_id)?;

            Ok(Json(ApiResponse {
                success: true,
                message: "Delegation revoked successfully".to_string(),
            }))
        } else {
            Err(ApiError::Unauthorized)
        }
    }
}

// Request/Response DTOs

#[derive(Debug, Deserialize)]
pub struct RevokeSessionRequest {
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MarkSuspiciousRequest {
    pub reason: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateTeamRequest {
    pub name: String,
    pub description: Option<String>,
    pub parent_team_id: Option<Ulid>,
    pub team_type: TeamType,
    pub settings: TeamSettings,
}

#[derive(Debug, Deserialize)]
pub struct InviteUserRequest {
    pub email: String,
    pub role: TeamRole,
    pub message: Option<String>,
    pub expires_in_hours: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct ShareResourceRequest {
    pub team_id: Ulid,
    pub resource_type: ResourceKind,
    pub resource_id: String,
    pub permissions: Vec<CrudAction>,
}

#[derive(Debug, Deserialize)]
pub struct CreateDelegationRequest {
    pub delegatee_id: Ulid,
    pub role: String,
    pub scope: DelegationScope,
    pub conditions: Vec<DelegationCondition>,
    pub expires_at: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct ActivityQuery {
    pub days: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct ContextQuery {
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
}

// Response DTOs

#[derive(Debug, Serialize)]
pub struct ApiResponse {
    pub success: bool,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct SessionSummary {
    pub id: String,
    pub device_type: String,
    pub device_trusted: bool,
    pub ip_address: Option<String>,
    pub created_at: i64,
    pub last_activity_at: i64,
    pub expires_at: i64,
    pub status: String,
}

impl From<SessionRecord> for SessionSummary {
    fn from(session: SessionRecord) -> Self {
        Self {
            id: session.id.to_string(),
            device_type: format!("{:?}", session.device_info.device_type),
            device_trusted: session.device_info.trusted,
            ip_address: session.ip_address,
            created_at: session.created_at,
            last_activity_at: session.last_activity_at,
            expires_at: session.expires_at,
            status: format!("{:?}", session.status),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct RevokeOtherSessionsResponse {
    pub success: bool,
    pub revoked_count: u32,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct TeamResponse {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub team_type: String,
    pub status: String,
    pub created_at: i64,
}

impl From<TeamRecord> for TeamResponse {
    fn from(team: TeamRecord) -> Self {
        Self {
            id: team.id.to_string(),
            name: team.name,
            description: team.description,
            team_type: format!("{:?}", team.team_type),
            status: format!("{:?}", team.status),
            created_at: team.created_at,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct UserTeamResponse {
    pub team: TeamResponse,
    pub role: String,
    pub joined_at: i64,
}

impl From<(TeamRecord, crate::models::TeamMemberRecord)> for UserTeamResponse {
    fn from((team, member): (TeamRecord, crate::models::TeamMemberRecord)) -> Self {
        Self {
            team: TeamResponse::from(team),
            role: format!("{:?}", member.role),
            joined_at: member.joined_at,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TeamMemberResponse {
    pub user_id: String,
    pub role: String,
    pub joined_at: i64,
    pub status: String,
}

impl From<crate::models::TeamMemberRecord> for TeamMemberResponse {
    fn from(member: crate::models::TeamMemberRecord) -> Self {
        Self {
            user_id: member.user_id.to_string(),
            role: format!("{:?}", member.role),
            joined_at: member.joined_at,
            status: format!("{:?}", member.status),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct InvitationResponse {
    pub id: String,
    pub team_id: String,
    pub invitee_email: String,
    pub role: String,
    pub expires_at: i64,
    pub status: String,
}

impl From<crate::models::TeamInvitation> for InvitationResponse {
    fn from(invitation: crate::models::TeamInvitation) -> Self {
        Self {
            id: invitation.id.to_string(),
            team_id: invitation.team_id.to_string(),
            invitee_email: invitation.invitee_email,
            role: format!("{:?}", invitation.role),
            expires_at: invitation.expires_at,
            status: format!("{:?}", invitation.status),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct EffectivePermissionsResponse {
    pub permissions: HashMap<ResourceKind, Vec<CrudAction>>,
}

#[derive(Debug, Serialize)]
pub struct DelegationResponse {
    pub id: String,
    pub delegatee_id: String,
    pub role: String,
    pub scope: DelegationScope,
    pub expires_at: Option<i64>,
    pub status: String,
}

impl From<crate::models::RoleDelegation> for DelegationResponse {
    fn from(delegation: crate::models::RoleDelegation) -> Self {
        Self {
            id: delegation.id.to_string(),
            delegatee_id: delegation.delegatee_id.to_string(),
            role: delegation.role,
            scope: delegation.scope,
            expires_at: delegation.expires_at,
            status: format!("{:?}", delegation.status),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct EffectiveRolesResponse {
    pub roles: Vec<EffectiveRole>,
}

// Error handling

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Forbidden")]
    Forbidden,
    #[error("Bad request: {0}")]
    BadRequest(String),
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            ApiError::Forbidden => (StatusCode::FORBIDDEN, "Forbidden".to_string()),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            ApiError::Internal(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        };

        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}