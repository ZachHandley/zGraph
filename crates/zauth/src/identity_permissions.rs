use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use zcore_storage::Store;

use zpermissions::{
    PermissionService, Principal, ResourceKind, Crud, CrudAction, PermissionError
};

use crate::{
    AuthSession,
    models::{CredentialTrustLevel, SessionCredentialContext},
};

/// Enhanced permission service that considers identity credentials and trust levels
#[derive(Clone)]
pub struct IdentityAwarePermissionService<'store> {
    permission_service: PermissionService<'store>,
    store: &'store Store,
}

impl<'store> IdentityAwarePermissionService<'store> {
    pub fn new(store: &'store Store) -> Self {
        Self {
            permission_service: PermissionService::new(store),
            store,
        }
    }

    /// Check permissions with identity awareness
    pub fn ensure_identity_aware(
        &self,
        session: &AuthSession,
        resource: ResourceKind,
        required: Crud,
    ) -> Result<(), IdentityPermissionError> {
        // First check basic permissions
        let principals = self.get_session_principals(session);

        match self.permission_service.ensure(session.org_id, &principals, resource, required) {
            Ok(_) => {
                // Basic permissions OK, now check identity requirements
                self.check_identity_requirements(session, resource, required)
            }
            Err(permission_error) => {
                // Check if identity credentials can elevate permissions
                if self.can_identity_elevate_permissions(session, resource, required)? {
                    self.check_identity_requirements(session, resource, required)
                } else {
                    Err(IdentityPermissionError::InsufficientPermissions(permission_error))
                }
            }
        }
    }

    /// Get effective permissions with identity awareness
    pub fn effective_identity_permissions(
        &self,
        session: &AuthSession,
    ) -> Result<IdentityAwarePermissionMap> {
        let principals = self.get_session_principals(session);
        let base_permissions = self.permission_service.effective_map(session.org_id, &principals)?;

        let mut enhanced_permissions = BTreeMap::new();

        for (resource, actions) in base_permissions {
            let enhanced_actions = self.enhance_actions_with_identity(session, resource, actions)?;
            enhanced_permissions.insert(resource, enhanced_actions);
        }

        Ok(IdentityAwarePermissionMap {
            base_permissions: enhanced_permissions,
            trust_level: self.get_session_trust_level(session),
            trust_score: session.trust_score,
            hardware_attested: session.hardware_attested,
            biometric_verified: session.biometric_verified,
            identity_requirements: self.get_resource_identity_requirements(),
        })
    }

    /// Check if session meets identity requirements for resource access
    fn check_identity_requirements(
        &self,
        session: &AuthSession,
        resource: ResourceKind,
        required: Crud,
    ) -> Result<(), IdentityPermissionError> {
        let requirements = self.get_identity_requirements(resource, required);

        // Check minimum trust score
        if session.trust_score < requirements.min_trust_score {
            return Err(IdentityPermissionError::InsufficientTrustScore {
                required: requirements.min_trust_score,
                actual: session.trust_score,
                resource,
            });
        }

        // Check hardware attestation requirement
        if requirements.require_hardware_attestation && !session.hardware_attested {
            return Err(IdentityPermissionError::HardwareAttestationRequired { resource });
        }

        // Check biometric requirement
        if requirements.require_biometric && !session.biometric_verified {
            return Err(IdentityPermissionError::BiometricVerificationRequired { resource });
        }

        // Check credential-specific requirements
        if let Some(context) = &session.credential_context {
            if requirements.require_device_bound && !context.device_bound {
                return Err(IdentityPermissionError::DeviceBindingRequired { resource });
            }

            if let Some(min_trust_level) = requirements.min_credential_trust_level {
                let session_trust_level = self.get_credential_trust_level(context);
                if session_trust_level < min_trust_level {
                    return Err(IdentityPermissionError::InsufficientCredentialTrustLevel {
                        required: min_trust_level,
                        actual: session_trust_level,
                        resource,
                    });
                }
            }
        } else if requirements.require_credentials {
            return Err(IdentityPermissionError::CredentialsRequired { resource });
        }

        Ok(())
    }

    /// Check if identity credentials can elevate permissions beyond basic RBAC
    fn can_identity_elevate_permissions(
        &self,
        session: &AuthSession,
        resource: ResourceKind,
        _required: Crud,
    ) -> Result<bool> {
        // High trust sessions can get elevated permissions for certain resources
        let elevation_rules = self.get_permission_elevation_rules();

        if let Some(rule) = elevation_rules.get(&resource) {
            // Check if session meets elevation criteria
            let meets_trust_threshold = session.trust_score >= rule.min_trust_score;
            let meets_attestation = !rule.require_hardware_attestation || session.hardware_attested;
            let meets_biometric = !rule.require_biometric || session.biometric_verified;

            Ok(meets_trust_threshold && meets_attestation && meets_biometric)
        } else {
            Ok(false)
        }
    }

    /// Get session principals including identity-derived ones
    fn get_session_principals(&self, session: &AuthSession) -> Vec<Principal> {
        let mut principals = Vec::new();

        // Add user principal if available
        if let Some(user_id) = session.user_id {
            principals.push(Principal::User(user_id.to_string()));
        }

        // Add role principals
        for role in &session.roles {
            principals.push(Principal::Role(role.clone()));
        }

        // Add label principals
        for label in &session.labels {
            principals.push(Principal::Label(label.clone()));
        }

        // Add identity-derived principals based on trust level
        if session.trust_score >= 0.9 {
            principals.push(Principal::Label("high_trust".to_string()));
        }

        if session.hardware_attested {
            principals.push(Principal::Label("hardware_attested".to_string()));
        }

        if session.biometric_verified {
            principals.push(Principal::Label("biometric_verified".to_string()));
        }

        // Add credential-based principals
        if let Some(context) = &session.credential_context {
            if context.device_bound {
                principals.push(Principal::Label("device_bound".to_string()));
            }

            // Add trust level based principal
            match self.get_credential_trust_level(context) {
                CredentialTrustLevel::TrustedPlatform => {
                    principals.push(Principal::Label("trusted_platform".to_string()));
                }
                CredentialTrustLevel::CertifiedHardware => {
                    principals.push(Principal::Label("certified_hardware".to_string()));
                }
                CredentialTrustLevel::HardwareVerified => {
                    principals.push(Principal::Label("hardware_verified".to_string()));
                }
                _ => {}
            }
        }

        principals
    }

    /// Get the highest credential trust level in the session
    fn get_credential_trust_level(&self, context: &SessionCredentialContext) -> CredentialTrustLevel {
        // This would need to look up the actual credentials
        // For now, we'll use a placeholder based on session properties
        if context.trust_score >= 0.95 && context.hardware_attested {
            CredentialTrustLevel::TrustedPlatform
        } else if context.hardware_attested {
            CredentialTrustLevel::HardwareVerified
        } else {
            CredentialTrustLevel::SoftwareVerified
        }
    }

    /// Get session trust level for display
    fn get_session_trust_level(&self, session: &AuthSession) -> SessionTrustLevel {
        match session.trust_score {
            s if s >= 0.95 => SessionTrustLevel::Maximum,
            s if s >= 0.85 => SessionTrustLevel::High,
            s if s >= 0.70 => SessionTrustLevel::Medium,
            s if s >= 0.50 => SessionTrustLevel::Low,
            _ => SessionTrustLevel::Minimal,
        }
    }

    /// Enhance base permissions with identity-aware capabilities
    fn enhance_actions_with_identity(
        &self,
        session: &AuthSession,
        resource: ResourceKind,
        base_actions: Vec<CrudAction>,
    ) -> Result<EnhancedPermissionActions> {
        let requirements = self.get_identity_requirements(resource, Crud::from_actions(&base_actions));
        let can_elevate = self.can_identity_elevate_permissions(session, resource, Crud::from_actions(&base_actions))?;

        Ok(EnhancedPermissionActions {
            base_actions,
            elevated_available: can_elevate,
            identity_requirements: requirements,
            current_trust_score: session.trust_score,
        })
    }

    /// Get identity requirements for resource/action combination
    fn get_identity_requirements(&self, resource: ResourceKind, _required: Crud) -> IdentityRequirements {
        match resource {
            ResourceKind::Permissions => IdentityRequirements {
                min_trust_score: 0.9,
                require_hardware_attestation: true,
                require_biometric: false,
                require_credentials: true,
                require_device_bound: false,
                min_credential_trust_level: Some(CredentialTrustLevel::HardwareVerified),
            },
            ResourceKind::Jobs => IdentityRequirements {
                min_trust_score: 0.7,
                require_hardware_attestation: false,
                require_biometric: false,
                require_credentials: false,
                require_device_bound: false,
                min_credential_trust_level: None,
            },
            ResourceKind::Files => IdentityRequirements {
                min_trust_score: 0.6,
                require_hardware_attestation: false,
                require_biometric: false,
                require_credentials: false,
                require_device_bound: false,
                min_credential_trust_level: None,
            },
            _ => IdentityRequirements {
                min_trust_score: 0.5,
                require_hardware_attestation: false,
                require_biometric: false,
                require_credentials: false,
                require_device_bound: false,
                min_credential_trust_level: None,
            },
        }
    }

    /// Get resource-level identity requirements mapping
    fn get_resource_identity_requirements(&self) -> BTreeMap<ResourceKind, IdentityRequirements> {
        ResourceKind::ALL.iter().map(|&resource| {
            (resource, self.get_identity_requirements(resource, Crud::READ))
        }).collect()
    }

    /// Get permission elevation rules
    fn get_permission_elevation_rules(&self) -> BTreeMap<ResourceKind, ElevationRule> {
        let mut rules = BTreeMap::new();

        // High trust users can get elevated permissions on collections and tables
        rules.insert(ResourceKind::Collections, ElevationRule {
            min_trust_score: 0.85,
            require_hardware_attestation: true,
            require_biometric: false,
        });

        rules.insert(ResourceKind::Tables, ElevationRule {
            min_trust_score: 0.85,
            require_hardware_attestation: true,
            require_biometric: false,
        });

        // Only maximum trust for permissions elevation
        rules.insert(ResourceKind::Permissions, ElevationRule {
            min_trust_score: 0.95,
            require_hardware_attestation: true,
            require_biometric: true,
        });

        rules
    }
}

// Types and structures

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityRequirements {
    pub min_trust_score: f64,
    pub require_hardware_attestation: bool,
    pub require_biometric: bool,
    pub require_credentials: bool,
    pub require_device_bound: bool,
    pub min_credential_trust_level: Option<CredentialTrustLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElevationRule {
    pub min_trust_score: f64,
    pub require_hardware_attestation: bool,
    pub require_biometric: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct IdentityAwarePermissionMap {
    pub base_permissions: BTreeMap<ResourceKind, EnhancedPermissionActions>,
    pub trust_level: SessionTrustLevel,
    pub trust_score: f64,
    pub hardware_attested: bool,
    pub biometric_verified: bool,
    pub identity_requirements: BTreeMap<ResourceKind, IdentityRequirements>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EnhancedPermissionActions {
    pub base_actions: Vec<CrudAction>,
    pub elevated_available: bool,
    pub identity_requirements: IdentityRequirements,
    pub current_trust_score: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SessionTrustLevel {
    Minimal,
    Low,
    Medium,
    High,
    Maximum,
}

#[derive(Debug, thiserror::Error)]
pub enum IdentityPermissionError {
    #[error("Insufficient permissions: {0}")]
    InsufficientPermissions(#[from] PermissionError),

    #[error("Insufficient trust score for {resource:?}: required {required}, actual {actual}")]
    InsufficientTrustScore {
        required: f64,
        actual: f64,
        resource: ResourceKind,
    },

    #[error("Hardware attestation required for {resource:?}")]
    HardwareAttestationRequired { resource: ResourceKind },

    #[error("Biometric verification required for {resource:?}")]
    BiometricVerificationRequired { resource: ResourceKind },

    #[error("Device binding required for {resource:?}")]
    DeviceBindingRequired { resource: ResourceKind },

    #[error("Credentials required for {resource:?}")]
    CredentialsRequired { resource: ResourceKind },

    #[error("Insufficient credential trust level for {resource:?}: required {required:?}, actual {actual:?}")]
    InsufficientCredentialTrustLevel {
        required: CredentialTrustLevel,
        actual: CredentialTrustLevel,
        resource: ResourceKind,
    },

    #[error("Storage error: {0}")]
    Storage(#[from] anyhow::Error),
}

impl IdentityRequirements {
    /// Check if requirements are satisfied by session
    pub fn satisfied_by(&self, session: &AuthSession) -> bool {
        session.trust_score >= self.min_trust_score &&
        (!self.require_hardware_attestation || session.hardware_attested) &&
        (!self.require_biometric || session.biometric_verified) &&
        (!self.require_credentials || session.credential_context.is_some()) &&
        (!self.require_device_bound || session.credential_context.as_ref()
            .map(|c| c.device_bound).unwrap_or(false))
    }
}