use super::*;
use anyhow::Result;
use rand_core::{OsRng, RngCore};
use sha2::{Sha256, Digest};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;


/// Hardware Security Module integration for biometric systems
pub struct BiometricHSMIntegration {
    hsm_connector: Arc<dyn HSMConnector>,
    key_manager: HSMKeyManager,
    secure_operations: HSMSecureOperations,
    attestation_service: HSMAttestationService,
}

/// Trait for HSM connectivity and operations
pub trait HSMConnector: Send + Sync {
    /// Initialize connection to HSM
    fn initialize(&self) -> Result<HSMSession>;

    /// Execute secure operation within HSM
    fn execute_secure_operation(
        &self,
        session: &HSMSession,
        operation: &SecureOperation,
    ) -> Result<OperationResult>;

    /// Get HSM capabilities and status
    fn get_hsm_info(&self) -> Result<HSMInfo>;

    /// Generate secure random data within HSM
    fn generate_random(&self, session: &HSMSession, length: usize) -> Result<Vec<u8>>;

    /// Perform hardware attestation
    fn create_attestation(&self, session: &HSMSession, data: &[u8]) -> Result<AttestationProof>;
}

#[derive(Debug, Clone)]
pub struct HSMSession {
    pub session_id: Ulid,
    pub hsm_handle: u64,
    pub authentication_token: Vec<u8>,
    pub capabilities: Vec<HSMCapability>,
    pub created_at: u64,
    pub expires_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecureOperation {
    pub operation_type: OperationType,
    pub input_data: Vec<u8>,
    pub parameters: Vec<(String, Vec<u8>)>,
    pub requires_attestation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OperationType {
    BiometricTemplateProtection,
    KeyDerivation,
    SecureMatching,
    TemplateEncryption,
    SignatureGeneration,
    KeyAgreement,
    RandomGeneration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationResult {
    pub operation_id: Ulid,
    pub result_data: Vec<u8>,
    pub attestation: Option<AttestationProof>,
    pub performance_metrics: OperationMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationMetrics {
    pub execution_time_ms: u64,
    pub memory_usage_kb: u32,
    pub cpu_cycles: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HSMInfo {
    pub vendor: String,
    pub model: String,
    pub firmware_version: String,
    pub security_level: HSMSecurityLevel,
    pub capabilities: Vec<HSMCapability>,
    pub status: HSMStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum HSMCapability {
    BiometricProcessing,
    CryptographicOperations,
    SecureKeyStorage,
    RandomNumberGeneration,
    DigitalSignatures,
    KeyAgreement,
    Attestation,
    TamperResistance,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum HSMSecurityLevel {
    FIPS140Level1,
    FIPS140Level2,
    FIPS140Level3,
    FIPS140Level4,
    CommonCriteriaEAL4Plus,
    CommonCriteriaEAL5Plus,
    CommonCriteriaEAL6Plus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum HSMStatus {
    Operational,
    Maintenance,
    Error,
    Tampered,
    Offline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttestationProof {
    pub attestation_type: AttestationType,
    pub hsm_identity: Vec<u8>,
    pub operation_hash: Vec<u8>,
    pub timestamp: u64,
    pub signature: Vec<u8>,
    pub certificate_chain: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AttestationType {
    OperationAttestation,
    KeyGeneration,
    BiometricProcessing,
    SecureBootstrap,
}

/// HSM-based key management
pub struct HSMKeyManager {
    key_handles: HashMap<Ulid, HSMKeyHandle>,
    key_policies: HashMap<Ulid, HSMKeyPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HSMKeyHandle {
    pub key_id: Ulid,
    pub hsm_key_handle: u64,
    pub key_type: HSMKeyType,
    pub key_usage: Vec<HSMKeyUsage>,
    pub created_in_hsm: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum HSMKeyType {
    BiometricDerivedKey,
    TemplateProtectionKey,
    SigningKey,
    EncryptionKey,
    MasterKey,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum HSMKeyUsage {
    BiometricMatching,
    TemplateEncryption,
    SignatureGeneration,
    KeyDerivation,
    DataEncryption,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HSMKeyPolicy {
    pub requires_authentication: bool,
    pub usage_limit: Option<u32>,
    pub expiration: Option<u64>,
    pub allowed_operations: Vec<OperationType>,
    pub attestation_required: bool,
}

/// HSM-based secure operations
pub struct HSMSecureOperations {
    operation_cache: Arc<Mutex<HashMap<Ulid, CachedOperation>>>,
}

#[derive(Debug, Clone)]
struct CachedOperation {
    operation_type: OperationType,
    result: OperationResult,
    cached_at: u64,
    ttl_seconds: u64,
}

/// HSM attestation service
pub struct HSMAttestationService {
    attestation_keys: HashMap<String, Vec<u8>>,
    trust_anchors: Vec<TrustAnchor>,
}

#[derive(Debug, Clone)]
struct TrustAnchor {
    issuer: String,
    public_key: Vec<u8>,
    valid_from: u64,
    valid_to: u64,
}

impl BiometricHSMIntegration {
    pub fn new(hsm_connector: Arc<dyn HSMConnector>) -> Result<Self> {
        Ok(Self {
            hsm_connector,
            key_manager: HSMKeyManager::new(),
            secure_operations: HSMSecureOperations::new(),
            attestation_service: HSMAttestationService::new(),
        })
    }

    /// Initialize HSM for biometric operations
    pub fn initialize_hsm(&self) -> Result<HSMSession> {
        let session = self.hsm_connector.initialize()?;

        // Verify HSM capabilities
        let hsm_info = self.hsm_connector.get_hsm_info()?;
        self.verify_hsm_capabilities(&hsm_info)?;

        // Perform initial attestation
        let attestation_data = b"BIOMETRIC_HSM_INIT";
        let _attestation = self.hsm_connector.create_attestation(&session, attestation_data)?;

        Ok(session)
    }

    /// Protect biometric template using HSM
    pub fn hsm_protect_template(
        &mut self,
        session: &HSMSession,
        template: &BiometricTemplate,
        protection_method: HSMProtectionMethod,
    ) -> Result<HSMProtectedTemplate> {
        let operation = SecureOperation {
            operation_type: OperationType::BiometricTemplateProtection,
            input_data: bincode::serialize(template)?,
            parameters: vec![
                ("protection_method".to_string(), bincode::serialize(&protection_method)?),
                ("user_id".to_string(), template.user_id.to_string().as_bytes().to_vec()),
            ],
            requires_attestation: true,
        };

        let result = self.hsm_connector.execute_secure_operation(session, &operation)?;

        // Parse the protected template from HSM result
        let protected_template = HSMProtectedTemplate {
            template_id: Ulid::new(),
            original_template_id: template.template_id,
            user_id: template.user_id,
            modality: template.modality.clone(),
            hsm_protected_data: result.result_data,
            protection_method,
            hsm_key_handle: self.generate_hsm_key_handle()?,
            attestation: result.attestation,
            created_at: current_timestamp(),
        };

        Ok(protected_template)
    }

    /// Perform secure matching using HSM
    pub fn hsm_secure_matching(
        &self,
        session: &HSMSession,
        query_template: &HSMProtectedTemplate,
        stored_template: &HSMProtectedTemplate,
    ) -> Result<HSMMatchingResult> {
        if query_template.modality != stored_template.modality {
            return Ok(HSMMatchingResult {
                is_match: false,
                confidence_score: 0.0,
                hsm_operation_id: Ulid::new(),
                attestation: None,
                matching_metadata: HashMap::new(),
            });
        }

        let operation = SecureOperation {
            operation_type: OperationType::SecureMatching,
            input_data: [&query_template.hsm_protected_data[..], &stored_template.hsm_protected_data[..]].concat(),
            parameters: vec![
                ("modality".to_string(), format!("{:?}", query_template.modality).as_bytes().to_vec()),
                ("query_key".to_string(), query_template.hsm_key_handle.to_le_bytes().to_vec()),
                ("stored_key".to_string(), stored_template.hsm_key_handle.to_le_bytes().to_vec()),
            ],
            requires_attestation: true,
        };

        let result = self.hsm_connector.execute_secure_operation(session, &operation)?;

        // Parse matching result
        let matching_result = self.parse_hsm_matching_result(&result)?;

        Ok(HSMMatchingResult {
            is_match: matching_result.is_match,
            confidence_score: matching_result.confidence_score,
            hsm_operation_id: result.operation_id,
            attestation: result.attestation,
            matching_metadata: matching_result.metadata,
        })
    }

    /// Derive cryptographic key using HSM
    pub fn hsm_derive_biometric_key(
        &self,
        session: &HSMSession,
        protected_template: &HSMProtectedTemplate,
        key_context: &KeyDerivationContext,
    ) -> Result<HSMBiometricKey> {
        let operation = SecureOperation {
            operation_type: OperationType::KeyDerivation,
            input_data: protected_template.hsm_protected_data.clone(),
            parameters: vec![
                ("key_length".to_string(), key_context.key_length.to_le_bytes().to_vec()),
                ("key_purpose".to_string(), format!("{:?}", key_context.key_purpose).as_bytes().to_vec()),
                ("salt".to_string(), key_context.salt.clone()),
                ("iterations".to_string(), key_context.derivation_parameters.iteration_count.to_le_bytes().to_vec()),
            ],
            requires_attestation: true,
        };

        let result = self.hsm_connector.execute_secure_operation(session, &operation)?;

        let hsm_key_handle = self.generate_hsm_key_handle()?;

        Ok(HSMBiometricKey {
            key_id: Ulid::new(),
            hsm_key_handle,
            template_id: protected_template.original_template_id,
            key_purpose: key_context.key_purpose.clone(),
            key_strength: key_context.key_length,
            derivation_method: "hsm_secure_derivation".to_string(),
            attestation: result.attestation,
            created_at: current_timestamp(),
        })
    }

    /// Generate attestation for biometric operation
    pub fn generate_operation_attestation(
        &self,
        session: &HSMSession,
        operation_data: &[u8],
        operation_type: OperationType,
    ) -> Result<AttestationProof> {
        let mut hasher = Sha256::new();
        hasher.update(operation_data);
        hasher.update(&format!("{:?}", operation_type).as_bytes());
        hasher.update(&current_timestamp().to_le_bytes());
        let operation_hash = hasher.finalize().to_vec();

        self.hsm_connector.create_attestation(session, &operation_hash)
    }

    fn verify_hsm_capabilities(&self, hsm_info: &HSMInfo) -> Result<()> {
        let required_capabilities = vec![
            HSMCapability::BiometricProcessing,
            HSMCapability::CryptographicOperations,
            HSMCapability::SecureKeyStorage,
            HSMCapability::Attestation,
        ];

        for required_cap in required_capabilities {
            if !hsm_info.capabilities.contains(&required_cap) {
                return Err(BiometricError::HsmFailure(
                    format!("HSM missing required capability: {:?}", required_cap)
                ).into());
            }
        }

        // Check security level
        match hsm_info.security_level {
            HSMSecurityLevel::FIPS140Level3 |
            HSMSecurityLevel::FIPS140Level4 |
            HSMSecurityLevel::CommonCriteriaEAL4Plus |
            HSMSecurityLevel::CommonCriteriaEAL5Plus |
            HSMSecurityLevel::CommonCriteriaEAL6Plus => Ok(()),
            _ => Err(BiometricError::HsmFailure(
                "HSM security level insufficient for biometric operations".to_string()
            ).into()),
        }
    }

    fn generate_hsm_key_handle(&self) -> Result<u64> {
        let mut handle_bytes = [0u8; 8];
        OsRng.fill_bytes(&mut handle_bytes);
        Ok(u64::from_le_bytes(handle_bytes))
    }

    fn parse_hsm_matching_result(&self, result: &OperationResult) -> Result<ParsedMatchingResult> {
        // Parse HSM-specific result format
        if result.result_data.len() < 9 { // Minimum: 1 byte match flag + 8 bytes confidence
            return Err(BiometricError::HsmFailure("Invalid HSM matching result format".to_string()).into());
        }

        let is_match = result.result_data[0] != 0;
        let confidence_bytes = &result.result_data[1..9];
        let confidence_score = f64::from_le_bytes([
            confidence_bytes[0], confidence_bytes[1], confidence_bytes[2], confidence_bytes[3],
            confidence_bytes[4], confidence_bytes[5], confidence_bytes[6], confidence_bytes[7],
        ]);

        let mut metadata = HashMap::new();
        if result.result_data.len() > 9 {
            // Parse additional metadata from remaining bytes
            metadata.insert("execution_time".to_string(), result.performance_metrics.execution_time_ms.to_string());
            metadata.insert("memory_usage".to_string(), result.performance_metrics.memory_usage_kb.to_string());
        }

        Ok(ParsedMatchingResult {
            is_match,
            confidence_score,
            metadata,
        })
    }
}

struct ParsedMatchingResult {
    is_match: bool,
    confidence_score: f64,
    metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum HSMProtectionMethod {
    HardwareEncryption,
    SecureTemplate,
    TamperResistantStorage,
    BiometricVault,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HSMProtectedTemplate {
    pub template_id: Ulid,
    pub original_template_id: Ulid,
    pub user_id: Ulid,
    pub modality: BiometricModality,
    pub hsm_protected_data: Vec<u8>,
    pub protection_method: HSMProtectionMethod,
    pub hsm_key_handle: u64,
    pub attestation: Option<AttestationProof>,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HSMMatchingResult {
    pub is_match: bool,
    pub confidence_score: f64,
    pub hsm_operation_id: Ulid,
    pub attestation: Option<AttestationProof>,
    pub matching_metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HSMBiometricKey {
    pub key_id: Ulid,
    pub hsm_key_handle: u64,
    pub template_id: Ulid,
    pub key_purpose: KeyPurpose,
    pub key_strength: u32,
    pub derivation_method: String,
    pub attestation: Option<AttestationProof>,
    pub created_at: u64,
}

impl HSMKeyManager {
    pub fn new() -> Self {
        Self {
            key_handles: HashMap::new(),
            key_policies: HashMap::new(),
        }
    }

    pub fn register_key(
        &mut self,
        key_id: Ulid,
        hsm_key_handle: u64,
        key_type: HSMKeyType,
        key_usage: Vec<HSMKeyUsage>,
        policy: HSMKeyPolicy,
    ) -> Result<()> {
        let handle = HSMKeyHandle {
            key_id,
            hsm_key_handle,
            key_type,
            key_usage,
            created_in_hsm: true,
        };

        self.key_handles.insert(key_id, handle);
        self.key_policies.insert(key_id, policy);

        Ok(())
    }

    pub fn get_key_handle(&self, key_id: &Ulid) -> Option<&HSMKeyHandle> {
        self.key_handles.get(key_id)
    }

    pub fn verify_key_usage(&self, key_id: &Ulid, operation: &OperationType) -> Result<bool> {
        let policy = self.key_policies.get(key_id)
            .ok_or_else(|| BiometricError::HsmFailure("Key policy not found".to_string()))?;

        Ok(policy.allowed_operations.contains(operation))
    }

    pub fn revoke_key(&mut self, key_id: &Ulid) -> Result<()> {
        self.key_handles.remove(key_id);
        self.key_policies.remove(key_id);
        Ok(())
    }
}

impl HSMSecureOperations {
    pub fn new() -> Self {
        Self {
            operation_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn cache_operation_result(&self, operation_type: OperationType, result: OperationResult) -> Result<()> {
        let cached_op = CachedOperation {
            operation_type,
            result,
            cached_at: current_timestamp(),
            ttl_seconds: 300, // 5 minute cache
        };

        let mut cache = self.operation_cache.lock().unwrap();
        cache.insert(Ulid::new(), cached_op);

        // Clean expired entries
        self.cleanup_expired_cache(&mut cache);

        Ok(())
    }

    pub fn get_cached_result(&self, operation_type: &OperationType) -> Option<OperationResult> {
        let cache = self.operation_cache.lock().unwrap();
        let current_time = current_timestamp();

        for cached_op in cache.values() {
            if cached_op.operation_type == *operation_type &&
               (current_time - cached_op.cached_at) < cached_op.ttl_seconds {
                return Some(cached_op.result.clone());
            }
        }

        None
    }

    fn cleanup_expired_cache(&self, cache: &mut HashMap<Ulid, CachedOperation>) {
        let current_time = current_timestamp();
        cache.retain(|_, cached_op| {
            (current_time - cached_op.cached_at) < cached_op.ttl_seconds
        });
    }
}

impl HSMAttestationService {
    pub fn new() -> Self {
        Self {
            attestation_keys: HashMap::new(),
            trust_anchors: Vec::new(),
        }
    }

    pub fn verify_attestation(&self, attestation: &AttestationProof) -> Result<bool> {
        // Verify attestation signature using appropriate trust anchor
        let trust_anchor = self.find_trust_anchor(&attestation.hsm_identity)?;

        // Verify timestamp is within acceptable range
        let current_time = current_timestamp();
        let attestation_age = current_time - attestation.timestamp;

        if attestation_age > 300 { // 5 minute maximum age
            return Ok(false);
        }

        // Verify signature (simplified - in practice would use proper cryptographic verification)
        let expected_signature = self.compute_expected_signature(attestation, &trust_anchor.public_key)?;
        Ok(expected_signature == attestation.signature)
    }

    pub fn add_trust_anchor(&mut self, issuer: String, public_key: Vec<u8>, valid_from: u64, valid_to: u64) {
        let trust_anchor = TrustAnchor {
            issuer,
            public_key,
            valid_from,
            valid_to,
        };

        self.trust_anchors.push(trust_anchor);
    }

    fn find_trust_anchor(&self, hsm_identity: &[u8]) -> Result<&TrustAnchor> {
        // Find matching trust anchor based on HSM identity
        for anchor in &self.trust_anchors {
            if anchor.public_key[..16] == hsm_identity[..16.min(hsm_identity.len())] {
                let current_time = current_timestamp();
                if current_time >= anchor.valid_from && current_time <= anchor.valid_to {
                    return Ok(anchor);
                }
            }
        }

        Err(BiometricError::HsmFailure("No valid trust anchor found".to_string()).into())
    }

    fn compute_expected_signature(&self, attestation: &AttestationProof, public_key: &[u8]) -> Result<Vec<u8>> {
        // Simplified signature computation - in practice would use proper ECDSA/RSA
        let mut hasher = Sha256::new();
        hasher.update(&attestation.operation_hash);
        hasher.update(&attestation.timestamp.to_le_bytes());
        hasher.update(&attestation.hsm_identity);
        hasher.update(public_key);

        Ok(hasher.finalize().to_vec())
    }
}

// Mock HSM connector for testing/simulation
pub struct MockHSMConnector {
    session_counter: Arc<Mutex<u64>>,
    operations_log: Arc<Mutex<Vec<SecureOperation>>>,
}

impl MockHSMConnector {
    pub fn new() -> Self {
        Self {
            session_counter: Arc::new(Mutex::new(0)),
            operations_log: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl HSMConnector for MockHSMConnector {
    fn initialize(&self) -> Result<HSMSession> {
        let mut counter = self.session_counter.lock().unwrap();
        *counter += 1;

        let mut auth_token = vec![0u8; 32];
        OsRng.fill_bytes(&mut auth_token);

        let current_time = current_timestamp();

        Ok(HSMSession {
            session_id: Ulid::new(),
            hsm_handle: *counter,
            authentication_token: auth_token,
            capabilities: vec![
                HSMCapability::BiometricProcessing,
                HSMCapability::CryptographicOperations,
                HSMCapability::SecureKeyStorage,
                HSMCapability::Attestation,
            ],
            created_at: current_time,
            expires_at: current_time + 3600, // 1 hour
        })
    }

    fn execute_secure_operation(
        &self,
        _session: &HSMSession,
        operation: &SecureOperation,
    ) -> Result<OperationResult> {
        // Log the operation
        {
            let mut log = self.operations_log.lock().unwrap();
            log.push(operation.clone());
        }

        // Simulate operation based on type
        let result_data = match operation.operation_type {
            OperationType::BiometricTemplateProtection => {
                // Simulate template protection by encrypting input data
                let mut protected_data = operation.input_data.clone();
                for byte in &mut protected_data {
                    *byte = byte.wrapping_add(127); // Simple encryption
                }
                protected_data
            },
            OperationType::SecureMatching => {
                // Simulate matching by comparing input data segments
                if operation.input_data.len() >= 16 {
                    let (query_data, stored_data) = operation.input_data.split_at(operation.input_data.len() / 2);
                    let similarity = self.compute_mock_similarity(query_data, stored_data);

                    let is_match = similarity > 0.8;
                    let mut result = vec![if is_match { 1u8 } else { 0u8 }];
                    result.extend_from_slice(&similarity.to_le_bytes());
                    result
                } else {
                    vec![0u8; 9] // No match
                }
            },
            OperationType::KeyDerivation => {
                // Simulate key derivation
                let mut derived_key = vec![0u8; 32];
                let mut hasher = Sha256::new();
                hasher.update(&operation.input_data);
                hasher.update(b"MOCK_HSM_KDF");
                derived_key.copy_from_slice(&hasher.finalize());
                derived_key
            },
            _ => {
                // Default simulation
                operation.input_data.clone()
            }
        };

        let attestation = if operation.requires_attestation {
            Some(self.create_mock_attestation(&operation.input_data)?)
        } else {
            None
        };

        Ok(OperationResult {
            operation_id: Ulid::new(),
            result_data,
            attestation,
            performance_metrics: OperationMetrics {
                execution_time_ms: 5, // Simulate 5ms execution
                memory_usage_kb: 64,
                cpu_cycles: 10000,
            },
        })
    }

    fn get_hsm_info(&self) -> Result<HSMInfo> {
        Ok(HSMInfo {
            vendor: "MockHSM Corp".to_string(),
            model: "MockHSM-2025".to_string(),
            firmware_version: "1.0.0".to_string(),
            security_level: HSMSecurityLevel::FIPS140Level3,
            capabilities: vec![
                HSMCapability::BiometricProcessing,
                HSMCapability::CryptographicOperations,
                HSMCapability::SecureKeyStorage,
                HSMCapability::RandomNumberGeneration,
                HSMCapability::Attestation,
                HSMCapability::TamperResistance,
            ],
            status: HSMStatus::Operational,
        })
    }

    fn generate_random(&self, _session: &HSMSession, length: usize) -> Result<Vec<u8>> {
        let mut random_data = vec![0u8; length];
        OsRng.fill_bytes(&mut random_data);
        Ok(random_data)
    }

    fn create_attestation(&self, _session: &HSMSession, data: &[u8]) -> Result<AttestationProof> {
        self.create_mock_attestation(data)
    }
}

impl MockHSMConnector {
    fn compute_mock_similarity(&self, data1: &[u8], data2: &[u8]) -> f64 {
        if data1.len() != data2.len() {
            return 0.0;
        }

        let mut matches = 0;
        for (a, b) in data1.iter().zip(data2.iter()) {
            let diff = (*a as i16 - *b as i16).abs();
            if diff <= 10 { // Allow small differences
                matches += 1;
            }
        }

        matches as f64 / data1.len() as f64
    }

    fn create_mock_attestation(&self, data: &[u8]) -> Result<AttestationProof> {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let operation_hash = hasher.finalize().to_vec();

        let hsm_identity = vec![0xAB, 0xCD, 0xEF, 0x12, 0x34, 0x56, 0x78, 0x90]; // Mock HSM ID

        // Create mock signature
        let mut sig_hasher = Sha256::new();
        sig_hasher.update(&operation_hash);
        sig_hasher.update(&hsm_identity);
        sig_hasher.update(&current_timestamp().to_le_bytes());
        sig_hasher.update(b"MOCK_HSM_SIGNATURE");
        let signature = sig_hasher.finalize().to_vec();

        // Mock certificate chain
        let certificate_chain = vec![
            vec![0x30, 0x82, 0x01, 0x22], // Mock certificate DER header
        ];

        Ok(AttestationProof {
            attestation_type: AttestationType::OperationAttestation,
            hsm_identity,
            operation_hash,
            timestamp: current_timestamp(),
            signature,
            certificate_chain,
        })
    }
}

/// Hardware-based liveness detection using HSM
pub struct HSMBasedLivenessDetection {
    hsm_integration: BiometricHSMIntegration,
    challenge_store: HashMap<Ulid, HSMChallenge>,
}

#[derive(Debug, Clone)]
struct HSMChallenge {
    challenge_id: Ulid,
    hsm_nonce: Vec<u8>,
    created_at: u64,
    attestation: AttestationProof,
}

impl HSMBasedLivenessDetection {
    pub fn new(hsm_integration: BiometricHSMIntegration) -> Self {
        Self {
            hsm_integration,
            challenge_store: HashMap::new(),
        }
    }

    /// Generate hardware-backed liveness challenge
    pub fn generate_hw_liveness_challenge(&mut self, session: &HSMSession) -> Result<BiometricChallenge> {
        // Generate secure random nonce in HSM
        let hsm_nonce = self.hsm_integration.hsm_connector.generate_random(session, 32)?;

        let challenge_id = Ulid::new();
        let current_time = current_timestamp();

        // Create attestation for the challenge
        let attestation = self.hsm_integration.generate_operation_attestation(
            session,
            &hsm_nonce,
            OperationType::BiometricTemplateProtection,
        )?;

        // Store HSM challenge
        let hsm_challenge = HSMChallenge {
            challenge_id,
            hsm_nonce: hsm_nonce.clone(),
            created_at: current_time,
            attestation,
        };
        self.challenge_store.insert(challenge_id, hsm_challenge);

        // Create standard biometric challenge with HSM-generated data
        let mut nonce = [0u8; 32];
        nonce.copy_from_slice(&hsm_nonce);

        Ok(BiometricChallenge {
            challenge_id,
            modality: BiometricModality::Face, // Default, can be changed
            challenge_data: hsm_nonce,
            created_at: current_time,
            expires_at: current_time + 300, // 5 minutes
            nonce,
            anti_replay_token: format!("hsm-{}", challenge_id),
        })
    }

    /// Verify hardware-backed liveness response
    pub fn verify_hw_liveness_response(
        &self,
        session: &HSMSession,
        response: &BiometricResponse,
    ) -> Result<bool> {
        let hsm_challenge = self.challenge_store.get(&response.challenge_id)
            .ok_or_else(|| BiometricError::ChallengeExpired)?;

        // Verify challenge hasn't expired
        if response.timestamp > hsm_challenge.created_at + 300 {
            return Err(BiometricError::ChallengeExpired.into());
        }

        // Use HSM to verify the response
        let operation = SecureOperation {
            operation_type: OperationType::BiometricTemplateProtection,
            input_data: [&response.biometric_data[..], &hsm_challenge.hsm_nonce[..]].concat(),
            parameters: vec![
                ("challenge_id".to_string(), response.challenge_id.to_string().as_bytes().to_vec()),
                ("response_timestamp".to_string(), response.timestamp.to_le_bytes().to_vec()),
            ],
            requires_attestation: true,
        };

        let result = self.hsm_integration.hsm_connector.execute_secure_operation(session, &operation)?;

        // HSM returns verification result
        Ok(!result.result_data.is_empty() && result.result_data[0] != 0)
    }
}

/// HSM-based secure biometric vault
pub struct HSMBiometricVault {
    hsm_integration: BiometricHSMIntegration,
    vault_keys: HashMap<Ulid, HSMVaultKey>,
}

#[derive(Debug, Clone)]
struct HSMVaultKey {
    key_id: Ulid,
    hsm_handle: u64,
    access_policy: VaultAccessPolicy,
}

#[derive(Debug, Clone)]
pub struct VaultAccessPolicy {
    required_authentication_level: u8,
    allowed_operations: Vec<VaultOperation>,
    usage_limits: HashMap<VaultOperation, u32>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
enum VaultOperation {
    Store,
    Retrieve,
    Match,
    Delete,
}

impl HSMBiometricVault {
    pub fn new(hsm_integration: BiometricHSMIntegration) -> Self {
        Self {
            hsm_integration,
            vault_keys: HashMap::new(),
        }
    }

    /// Securely store biometric template in HSM vault
    pub fn secure_store_template(
        &mut self,
        session: &HSMSession,
        template: &BiometricTemplate,
        access_policy: VaultAccessPolicy,
    ) -> Result<HSMProtectedTemplate> {
        // Create vault key for this template
        let vault_key_id = Ulid::new();
        let hsm_handle = self.generate_vault_key_handle()?;

        let vault_key = HSMVaultKey {
            key_id: vault_key_id,
            hsm_handle,
            access_policy,
        };

        // Store template using HSM protection
        let protected_template = self.hsm_integration.hsm_protect_template(
            session,
            template,
            HSMProtectionMethod::BiometricVault,
        )?;

        self.vault_keys.insert(vault_key_id, vault_key);

        Ok(protected_template)
    }

    /// Securely retrieve template from HSM vault
    pub fn secure_retrieve_template(
        &self,
        session: &HSMSession,
        template_id: &Ulid,
        access_credentials: &[u8],
    ) -> Result<Option<HSMProtectedTemplate>> {
        // Verify access credentials using HSM
        let access_operation = SecureOperation {
            operation_type: OperationType::BiometricTemplateProtection,
            input_data: access_credentials.to_vec(),
            parameters: vec![
                ("operation".to_string(), b"VAULT_ACCESS".to_vec()),
                ("template_id".to_string(), template_id.to_string().as_bytes().to_vec()),
            ],
            requires_attestation: true,
        };

        let access_result = self.hsm_integration.hsm_connector.execute_secure_operation(session, &access_operation)?;

        if access_result.result_data.is_empty() || access_result.result_data[0] == 0 {
            return Ok(None); // Access denied
        }

        // Access granted - retrieve would happen here
        // For this mock implementation, we return a placeholder
        Ok(None)
    }

    fn generate_vault_key_handle(&self) -> Result<u64> {
        let mut handle_bytes = [0u8; 8];
        OsRng.fill_bytes(&mut handle_bytes);
        Ok(u64::from_le_bytes(handle_bytes))
    }
}