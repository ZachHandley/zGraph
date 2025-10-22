use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRecord {
    pub id: Ulid,
    pub org_id: u64,
    pub email: String,
    pub password_hash: String,
    pub roles: Vec<String>,
    pub labels: Vec<String>,
    pub status: UserStatus,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum UserStatus {
    Active,
    Disabled,
    Pending,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyRecord {
    pub id: Ulid,
    pub org_id: u64,
    pub environment: Ulid,
    pub name: String,
    pub hash: String,
    pub created_at: i64,
    pub last_used_at: Option<i64>,
    pub revoked_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentRecord {
    pub id: Ulid,
    pub org_id: u64,
    pub slug: String,
    pub display_name: String,
    pub auto_clear_policy: AutoClearPolicy,
    pub created_at: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AutoClearPolicy {
    Never,
    Daily,
    Weekly,
    OnDeploy,
}

// Enhanced Session Management Models

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    pub id: Ulid,
    pub user_id: Ulid,
    pub org_id: u64,
    pub environment_id: Ulid,
    pub device_info: DeviceInfo,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub created_at: i64,
    pub last_activity_at: i64,
    pub expires_at: i64,
    pub status: SessionStatus,
    pub revoked_at: Option<i64>,
    pub revoked_by: Option<Ulid>,
    pub revoked_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub fingerprint: String,
    pub platform: Option<String>,
    pub browser: Option<String>,
    pub browser_version: Option<String>,
    pub os: Option<String>,
    pub os_version: Option<String>,
    pub device_type: DeviceType,
    pub trusted: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DeviceType {
    Desktop,
    Mobile,
    Tablet,
    Api,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SessionStatus {
    Active,
    Expired,
    Revoked,
    Suspicious,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionLimit {
    pub max_concurrent_sessions: Option<u32>,
    pub max_sessions_per_device: Option<u32>,
    pub trusted_device_only: bool,
    pub geo_restriction: Option<GeoRestriction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoRestriction {
    pub allowed_countries: Vec<String>,
    pub allowed_regions: Vec<String>,
    pub blocked_ips: Vec<String>,
}

// Team Management Models

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamRecord {
    pub id: Ulid,
    pub org_id: u64,
    pub name: String,
    pub description: Option<String>,
    pub parent_team_id: Option<Ulid>,
    pub team_type: TeamType,
    pub status: TeamStatus,
    pub settings: TeamSettings,
    pub created_at: i64,
    pub updated_at: i64,
    pub created_by: Ulid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TeamType {
    Department,
    Project,
    Functional,
    CrossFunctional,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TeamStatus {
    Active,
    Inactive,
    Archived,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamSettings {
    pub visibility: TeamVisibility,
    pub join_approval_required: bool,
    pub resource_sharing_enabled: bool,
    pub audit_level: AuditLevel,
    pub session_limits: Option<SessionLimit>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TeamVisibility {
    Public,
    Private,
    Secret,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AuditLevel {
    None,
    Basic,
    Detailed,
    Full,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamMemberRecord {
    pub team_id: Ulid,
    pub user_id: Ulid,
    pub role: TeamRole,
    pub permissions: Vec<String>,
    pub joined_at: i64,
    pub invited_by: Option<Ulid>,
    pub status: MemberStatus,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TeamRole {
    Owner,
    Admin,
    Member,
    Viewer,
    Guest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MemberStatus {
    Active,
    Invited,
    Suspended,
    Left,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamInvitation {
    pub id: Ulid,
    pub team_id: Ulid,
    pub inviter_id: Ulid,
    pub invitee_email: String,
    pub role: TeamRole,
    pub message: Option<String>,
    pub expires_at: i64,
    pub created_at: i64,
    pub accepted_at: Option<i64>,
    pub declined_at: Option<i64>,
    pub status: InvitationStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum InvitationStatus {
    Pending,
    Accepted,
    Declined,
    Expired,
    Revoked,
}

// Role Delegation and Inheritance Models

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleDelegation {
    pub id: Ulid,
    pub delegator_id: Ulid,
    pub delegatee_id: Ulid,
    pub org_id: u64,
    pub role: String,
    pub scope: DelegationScope,
    pub conditions: Vec<DelegationCondition>,
    pub expires_at: Option<i64>,
    pub created_at: i64,
    pub status: DelegationStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DelegationScope {
    Global,
    Team(Ulid),
    Environment(Ulid),
    Resource { resource_type: String, resource_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationCondition {
    pub condition_type: ConditionType,
    pub value: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConditionType {
    TimeRange,
    IpAddress,
    UserAgent,
    GeographicLocation,
    ResourceAccess,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DelegationStatus {
    Active,
    Suspended,
    Expired,
    Revoked,
}

// Audit and Logging Models

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogEntry {
    pub id: Ulid,
    pub org_id: u64,
    pub session_id: Option<Ulid>,
    pub user_id: Option<Ulid>,
    pub team_id: Option<Ulid>,
    pub action: AuditAction,
    pub resource_type: Option<String>,
    pub resource_id: Option<String>,
    pub details: serde_json::Value,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub timestamp: i64,
    pub status: AuditStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AuditAction {
    // Session actions
    SessionCreate,
    SessionRefresh,
    SessionRevoke,
    SessionExpire,
    // Authentication actions
    LoginSuccess,
    LoginFailure,
    LogoutSuccess,
    PasswordChange,
    // Team actions
    TeamCreate,
    TeamUpdate,
    TeamDelete,
    TeamJoin,
    TeamLeave,
    TeamInvite,
    // Permission actions
    PermissionGrant,
    PermissionRevoke,
    RoleDelegate,
    RoleDelegationRevoke,
    // Resource actions
    ResourceAccess,
    ResourceCreate,
    ResourceUpdate,
    ResourceDelete,
    ResourceShare,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AuditStatus {
    Success,
    Failure,
    Warning,
    Blocked,
}

// Two-Factor Authentication Models

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TotpSecret {
    pub id: Ulid,
    pub user_id: Ulid,
    pub org_id: u64,
    pub secret: String, // Base32 encoded secret
    pub algorithm: TotpAlgorithm,
    pub digits: u32,
    pub period: u64,
    pub issuer: String,
    pub account_name: String,
    pub verified: bool,
    pub created_at: i64,
    pub verified_at: Option<i64>,
    pub last_used_at: Option<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TotpAlgorithm {
    Sha1,
    Sha256,
    Sha512,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupCode {
    pub id: Ulid,
    pub user_id: Ulid,
    pub org_id: u64,
    pub code: String, // Hashed backup code
    pub used: bool,
    pub used_at: Option<i64>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TwoFactorChallenge {
    pub id: Ulid,
    pub user_id: Ulid,
    pub org_id: u64,
    pub challenge_type: ChallengeType,
    pub challenge_data: String, // JSON encoded challenge data
    pub verified: bool,
    pub expires_at: i64,
    pub created_at: i64,
    pub verified_at: Option<i64>,
    pub attempts: u32,
    pub max_attempts: u32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ChallengeType {
    Totp,
    Sms,
    Email,
    WebAuthn,
    BackupCode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebAuthnCredential {
    pub id: Ulid,
    pub user_id: Ulid,
    pub org_id: u64,
    pub credential_id: Vec<u8>,
    pub public_key: Vec<u8>,
    pub aaguid: Vec<u8>,
    pub counter: u32,
    pub name: String,
    pub backup_eligible: bool,
    pub backup_state: bool,
    pub created_at: i64,
    pub last_used_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TwoFactorSettings {
    pub user_id: Ulid,
    pub org_id: u64,
    pub totp_enabled: bool,
    pub sms_enabled: bool,
    pub email_enabled: bool,
    pub webauthn_enabled: bool,
    pub backup_codes_enabled: bool,
    pub default_method: Option<ChallengeType>,
    pub phone_number: Option<String>,
    pub phone_verified: bool,
    pub recovery_email: Option<String>,
    pub recovery_email_verified: bool,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrgTwoFactorPolicy {
    pub org_id: u64,
    pub required: bool,
    pub grace_period_days: u32,
    pub allowed_methods: Vec<ChallengeType>,
    pub require_backup_codes: bool,
    pub max_totp_devices: u32,
    pub max_webauthn_devices: u32,
    pub session_timeout_minutes: u32,
    pub remember_device_days: u32,
    pub updated_at: i64,
    pub updated_by: Ulid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrustedDevice {
    pub id: Ulid,
    pub user_id: Ulid,
    pub org_id: u64,
    pub device_fingerprint: String,
    pub device_name: String,
    pub trusted_until: i64,
    pub created_at: i64,
    pub last_seen_at: i64,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
}

// SMS verification for 2FA setup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmsVerification {
    pub id: Ulid,
    pub user_id: Ulid,
    pub org_id: u64,
    pub phone_number: String,
    pub verification_code: String, // Hashed code
    pub verified: bool,
    pub expires_at: i64,
    pub created_at: i64,
    pub verified_at: Option<i64>,
    pub attempts: u32,
    pub max_attempts: u32,
}

// Credential-based Authentication Models

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityCredential {
    pub id: Ulid,
    pub user_id: Ulid,
    pub org_id: u64,
    pub credential_type: CredentialType,
    pub credential_data: String, // Encrypted/encoded credential data
    pub issuer: String,
    pub subject: String,
    pub public_key: Option<Vec<u8>>,
    pub certificate_chain: Option<Vec<u8>>,
    pub attestation_data: Option<Vec<u8>>,
    pub trust_level: CredentialTrustLevel,
    pub verified: bool,
    pub created_at: i64,
    pub expires_at: Option<i64>,
    pub last_used_at: Option<i64>,
    pub revoked_at: Option<i64>,
    pub revoked_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum CredentialType {
    X509Certificate,
    SoftwareAttestation,
    HardwareAttestation,
    BiometricCredential,
    SmartCard,
    PlatformCredential,
    DeviceCredential,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum CredentialTrustLevel {
    Untrusted,
    SoftwareVerified,
    HardwareVerified,
    CertifiedHardware,
    TrustedPlatform,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialVerificationChallenge {
    pub id: Ulid,
    pub credential_id: Ulid,
    pub user_id: Ulid,
    pub org_id: u64,
    pub challenge_data: Vec<u8>,
    pub nonce: String,
    pub created_at: i64,
    pub expires_at: i64,
    pub verified_at: Option<i64>,
    pub signature: Option<Vec<u8>>,
    pub verification_result: Option<VerificationResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationResult {
    pub success: bool,
    pub trust_score: f64,
    pub attestation_verified: bool,
    pub certificate_chain_valid: bool,
    pub revocation_checked: bool,
    pub details: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardwareAttestationData {
    pub attestation_type: AttestationType,
    pub attestation_certificate: Vec<u8>,
    pub attestation_signature: Vec<u8>,
    pub client_data_hash: Vec<u8>,
    pub authenticator_data: Vec<u8>,
    pub counter: u32,
    pub aaguid: Option<Vec<u8>>,
    pub hardware_key_handle: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AttestationType {
    None,
    Basic,
    SelfAttestation,
    AttestationCA,
    ECDAA,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionCredentialContext {
    pub session_id: Ulid,
    pub primary_credential: Option<Ulid>,
    pub secondary_credentials: Vec<Ulid>,
    pub hardware_attested: bool,
    pub biometric_verified: bool,
    pub device_bound: bool,
    pub trust_score: f64,
    pub verification_timestamp: i64,
    pub context_data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityAssertionRecord {
    pub id: Ulid,
    pub user_id: Ulid,
    pub org_id: u64,
    pub session_id: Ulid,
    pub credential_ids: Vec<Ulid>,
    pub assertion_type: AssertionType,
    pub challenge_response: Vec<u8>,
    pub client_data: Vec<u8>,
    pub authenticator_data: Option<Vec<u8>>,
    pub signature: Vec<u8>,
    pub verified: bool,
    pub trust_score: f64,
    pub created_at: i64,
    pub verified_at: Option<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AssertionType {
    Authentication,
    Authorization,
    Transaction,
    HighValue,
}

impl Default for TotpAlgorithm {
    fn default() -> Self {
        Self::Sha1
    }
}

impl Default for OrgTwoFactorPolicy {
    fn default() -> Self {
        Self {
            org_id: 0,
            required: false,
            grace_period_days: 30,
            allowed_methods: vec![
                ChallengeType::Totp,
                ChallengeType::Sms,
                ChallengeType::BackupCode,
            ],
            require_backup_codes: true,
            max_totp_devices: 3,
            max_webauthn_devices: 5,
            session_timeout_minutes: 480, // 8 hours
            remember_device_days: 30,
            updated_at: chrono::Utc::now().timestamp(),
            updated_by: Ulid::new(),
        }
    }
}

impl Default for CredentialTrustLevel {
    fn default() -> Self {
        Self::Untrusted
    }
}

impl Default for AttestationType {
    fn default() -> Self {
        Self::None
    }
}
