use super::*;
use anyhow::Result;
use std::collections::HashMap;

/// Multimodal biometric system for enhanced security and accuracy
pub struct MultimodalBiometricSystem {
    template_generator: CancelableTemplateGenerator,
    liveness_detector: LivenessDetector,
    matching_engine: BiometricMatchingEngine,
    fusion_engine: MultimodalFusionEngine,
    decision_engine: MultimodalDecisionEngine,
    performance_monitor: MultimodalPerformanceMonitor,
}

/// Advanced fusion engine for multimodal biometrics
pub struct MultimodalFusionEngine {
    fusion_strategies: HashMap<String, Box<dyn MultimodalFusionStrategy>>,
    quality_assessor: QualityBasedFusion,
    adaptive_weighting: AdaptiveWeighting,
}

/// Trait for multimodal fusion strategies
pub trait MultimodalFusionStrategy: Send + Sync {
    /// Fuse multiple biometric modalities at the template level
    fn fuse_templates(
        &self,
        templates: &[BiometricTemplate],
        fusion_context: &FusionContext,
    ) -> Result<MultimodalTemplate>;

    /// Fuse scores from multiple modalities
    fn fuse_scores(
        &self,
        modality_scores: &[ModalityScore],
        fusion_context: &FusionContext,
    ) -> Result<FusedMultimodalScore>;

    /// Get the name of this fusion strategy
    fn strategy_name(&self) -> &str;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FusionContext {
    pub user_id: Ulid,
    pub security_level: SecurityLevel,
    pub environment_factors: EnvironmentFactors,
    pub user_preferences: UserPreferences,
    pub quality_requirements: QualityRequirements,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentFactors {
    pub lighting_conditions: f64,     // 0.0 (poor) to 1.0 (excellent)
    pub noise_level: f64,            // 0.0 (quiet) to 1.0 (noisy)
    pub device_stability: f64,       // 0.0 (unstable) to 1.0 (stable)
    pub network_quality: f64,        // 0.0 (poor) to 1.0 (excellent)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserPreferences {
    pub preferred_modalities: Vec<BiometricModality>,
    pub fallback_modalities: Vec<BiometricModality>,
    pub max_enrollment_time: u64,    // Maximum time for enrollment in seconds
    pub privacy_level: PrivacyLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultimodalTemplate {
    pub template_id: Ulid,
    pub user_id: Ulid,
    pub org_id: u64,
    pub modality_templates: Vec<BiometricTemplate>,
    pub fusion_metadata: FusionMetadata,
    pub quality_metrics: MultimodalQualityMetrics,
    pub created_at: u64,
    pub updated_at: u64,
    pub version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FusionMetadata {
    pub fusion_strategy: String,
    pub modality_weights: Vec<f64>,
    pub correlation_matrix: Vec<Vec<f64>>,
    pub redundancy_scores: Vec<f64>,
    pub distinctiveness_scores: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultimodalQualityMetrics {
    pub overall_quality: f64,
    pub modality_qualities: Vec<QualityMetrics>,
    pub cross_modal_consistency: f64,
    pub template_stability: f64,
    pub discriminability: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModalityScore {
    pub modality: BiometricModality,
    pub similarity_score: f64,
    pub confidence: f64,
    pub quality_impact: f64,
    pub environmental_impact: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FusedMultimodalScore {
    pub final_score: f64,
    pub confidence: f64,
    pub individual_contributions: Vec<ModalityContribution>,
    pub fusion_strategy: String,
    pub quality_assessment: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModalityContribution {
    pub modality: BiometricModality,
    pub contribution_weight: f64,
    pub original_score: f64,
    pub quality_adjusted_score: f64,
}

/// Quality-based fusion that adapts based on biometric quality
pub struct QualityBasedFusion {
    quality_thresholds: HashMap<BiometricModality, f64>,
    quality_weights: HashMap<BiometricModality, f64>,
}

/// Adaptive weighting system that learns from user patterns
pub struct AdaptiveWeighting {
    user_profiles: HashMap<Ulid, UserBiometricProfile>,
    learning_rate: f64,
    adaptation_window: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserBiometricProfile {
    pub user_id: Ulid,
    pub modality_preferences: HashMap<BiometricModality, ModalityPreference>,
    pub authentication_patterns: AuthenticationPatterns,
    pub quality_history: QualityHistory,
    pub last_updated: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModalityPreference {
    pub success_rate: f64,
    pub average_quality: f64,
    pub average_confidence: f64,
    pub failure_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticationPatterns {
    pub time_of_day_patterns: Vec<(u8, f64)>, // (hour, success_rate)
    pub device_patterns: HashMap<String, f64>, // device_id -> success_rate
    pub environment_patterns: HashMap<String, f64>, // environment -> success_rate
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityHistory {
    pub quality_samples: Vec<(u64, f64)>, // (timestamp, quality)
    pub quality_trends: HashMap<BiometricModality, f64>,
    pub stability_metrics: HashMap<BiometricModality, f64>,
}

/// Decision engine for multimodal biometric systems
pub struct MultimodalDecisionEngine {
    decision_policies: HashMap<String, MultimodalDecisionPolicy>,
    risk_assessor: RiskAssessmentEngine,
    adaptive_thresholds: AdaptiveThresholdSystem,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultimodalDecisionPolicy {
    pub policy_name: String,
    pub required_modalities: Vec<BiometricModality>,
    pub optional_modalities: Vec<BiometricModality>,
    pub minimum_modality_count: u32,
    pub score_combination_method: ScoreCombinationMethod,
    pub threshold_strategy: ThresholdStrategy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ScoreCombinationMethod {
    WeightedAverage,
    Product,
    Maximum,
    Minimum,
    Majority,
    AdaptiveCombo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ThresholdStrategy {
    Fixed(f64),
    Adaptive(AdaptiveThresholdConfig),
    RiskBased(RiskBasedThresholdConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveThresholdConfig {
    pub base_threshold: f64,
    pub quality_adjustment_factor: f64,
    pub user_history_factor: f64,
    pub environmental_factor: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskBasedThresholdConfig {
    pub low_risk_threshold: f64,
    pub medium_risk_threshold: f64,
    pub high_risk_threshold: f64,
    pub risk_factors: Vec<RiskFactor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskFactor {
    pub factor_name: String,
    pub weight: f64,
    pub threshold: f64,
}

/// Risk assessment engine for multimodal systems
pub struct RiskAssessmentEngine {
    risk_models: HashMap<String, Box<dyn RiskModel>>,
    threat_intelligence: ThreatIntelligenceService,
}

/// Trait for risk assessment models
pub trait RiskModel: Send + Sync {
    /// Assess risk level for an authentication attempt
    fn assess_risk(
        &self,
        context: &AuthenticationContext,
        biometric_data: &MultimodalBiometricData,
    ) -> Result<RiskAssessment>;

    /// Update risk model based on feedback
    fn update_model(&mut self, feedback: &AuthenticationFeedback) -> Result<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticationContext {
    pub user_id: Ulid,
    pub device_id: String,
    pub location: LocationInfo,
    pub timestamp: u64,
    pub session_context: SessionContext,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationInfo {
    pub country: String,
    pub city: String,
    pub ip_address: String,
    pub is_vpn: bool,
    pub risk_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionContext {
    pub session_duration: u64,
    pub previous_authentications: Vec<PreviousAuthentication>,
    pub device_trust_level: f64,
    pub behavioral_patterns: BehavioralPatterns,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreviousAuthentication {
    pub timestamp: u64,
    pub success: bool,
    pub modalities_used: Vec<BiometricModality>,
    pub confidence_scores: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BehavioralPatterns {
    pub typing_patterns: Option<TypingPattern>,
    pub mouse_patterns: Option<MousePattern>,
    pub touch_patterns: Option<TouchPattern>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypingPattern {
    pub keystroke_dynamics: Vec<f64>,
    pub typing_speed: f64,
    pub rhythm_patterns: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MousePattern {
    pub movement_velocity: f64,
    pub click_patterns: Vec<f64>,
    pub scroll_patterns: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TouchPattern {
    pub pressure_patterns: Vec<f64>,
    pub swipe_patterns: Vec<f64>,
    pub gesture_patterns: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultimodalBiometricData {
    pub templates: Vec<BiometricTemplate>,
    pub quality_metrics: Vec<QualityMetrics>,
    pub liveness_proofs: Vec<LivenessProof>,
    pub collection_metadata: CollectionMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionMetadata {
    pub collection_timestamp: u64,
    pub device_info: DeviceInfo,
    pub environmental_conditions: EnvironmentFactors,
    pub collection_quality: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub device_type: String,
    pub operating_system: String,
    pub browser: String,
    pub camera_info: Option<CameraInfo>,
    pub microphone_info: Option<MicrophoneInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CameraInfo {
    pub resolution: String,
    pub fps: u32,
    pub auto_focus: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MicrophoneInfo {
    pub sample_rate: u32,
    pub channels: u32,
    pub noise_cancellation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskAssessment {
    pub risk_level: RiskLevel,
    pub risk_score: f64,
    pub contributing_factors: Vec<RiskContributor>,
    pub recommended_actions: Vec<RecommendedAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RiskLevel {
    VeryLow,
    Low,
    Medium,
    High,
    VeryHigh,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskContributor {
    pub factor_name: String,
    pub contribution: f64,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RecommendedAction {
    Allow,
    RequireAdditionalAuthentication,
    RequireStepUpAuthentication,
    RequireManualReview,
    Deny,
    TemporaryBlock,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticationFeedback {
    pub authentication_id: Ulid,
    pub actual_outcome: bool, // true if legitimate, false if fraudulent
    pub confidence_in_feedback: f64,
    pub additional_context: HashMap<String, String>,
}

/// Performance monitoring for multimodal systems
pub struct MultimodalPerformanceMonitor {
    performance_metrics: HashMap<String, MultimodalPerformanceMetrics>,
    user_satisfaction: HashMap<Ulid, UserSatisfactionMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultimodalPerformanceMetrics {
    pub modality_performance: HashMap<BiometricModality, ModalityPerformance>,
    pub fusion_performance: FusionPerformance,
    pub overall_system_performance: SystemPerformance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModalityPerformance {
    pub false_accept_rate: f64,
    pub false_reject_rate: f64,
    pub equal_error_rate: f64,
    pub average_enrollment_time: f64,
    pub average_authentication_time: f64,
    pub failure_to_enroll_rate: f64,
    pub failure_to_acquire_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FusionPerformance {
    pub improvement_over_best_single: f64,
    pub robustness_to_failure: f64,
    pub computational_overhead: f64,
    pub fusion_accuracy: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemPerformance {
    pub throughput: f64,          // authentications per second
    pub latency_p50: f64,         // 50th percentile latency in ms
    pub latency_p95: f64,         // 95th percentile latency in ms
    pub latency_p99: f64,         // 99th percentile latency in ms
    pub resource_utilization: ResourceUtilization,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUtilization {
    pub cpu_usage: f64,           // Percentage
    pub memory_usage: f64,        // MB
    pub storage_usage: f64,       // MB
    pub network_bandwidth: f64,   // Mbps
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserSatisfactionMetrics {
    pub user_id: Ulid,
    pub satisfaction_score: f64,  // 1.0 to 5.0
    pub ease_of_use_score: f64,
    pub perceived_security_score: f64,
    pub authentication_time_satisfaction: f64,
    pub failure_frustration_score: f64,
}

/// Threat intelligence service for advanced security
pub struct ThreatIntelligenceService {
    threat_feeds: Vec<ThreatFeed>,
    attack_patterns: HashMap<String, AttackPattern>,
    ip_reputation: HashMap<String, IPReputation>,
}

#[derive(Debug, Clone)]
pub struct ThreatFeed {
    pub source: String,
    pub last_updated: u64,
    pub threat_indicators: Vec<ThreatIndicator>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreatIndicator {
    pub indicator_type: IndicatorType,
    pub value: String,
    pub confidence: f64,
    pub severity: ThreatSeverity,
    pub first_seen: u64,
    pub last_seen: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndicatorType {
    IPAddress,
    Domain,
    UserAgent,
    Fingerprint,
    BehaviorPattern,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ThreatSeverity {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttackPattern {
    pub pattern_name: String,
    pub description: String,
    pub indicators: Vec<String>,
    pub detection_rules: Vec<DetectionRule>,
    pub mitigation_strategies: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectionRule {
    pub rule_name: String,
    pub condition: String,
    pub threshold: f64,
    pub action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IPReputation {
    pub ip_address: String,
    pub reputation_score: f64,  // -100 (malicious) to +100 (trusted)
    pub categories: Vec<String>,
    pub first_seen: u64,
    pub last_updated: u64,
}

impl MultimodalBiometricSystem {
    pub fn new() -> Result<Self> {
        Ok(Self {
            template_generator: CancelableTemplateGenerator::new(),
            liveness_detector: LivenessDetector::new(),
            matching_engine: BiometricMatchingEngine::new(),
            fusion_engine: MultimodalFusionEngine::new()?,
            decision_engine: MultimodalDecisionEngine::new()?,
            performance_monitor: MultimodalPerformanceMonitor::new(),
        })
    }

    /// Enroll a user with multiple biometric modalities
    pub fn enroll_multimodal_user(
        &mut self,
        user_id: Ulid,
        org_id: u64,
        biometric_data: Vec<(BiometricModality, Vec<u8>)>,
        fusion_context: FusionContext,
    ) -> Result<MultimodalTemplate> {
        // Generate individual biometric templates
        let mut templates = Vec::new();
        let mut quality_metrics = Vec::new();

        for (modality, raw_data) in biometric_data {
            // Assess quality first
            let quality_assessor = QualityAssessor::new();
            let quality = quality_assessor.assess_quality(&raw_data, &modality)?;

            // Check if quality meets minimum requirements
            if quality.overall_quality < fusion_context.quality_requirements.minimum_quality {
                tracing::warn!("Low quality sample for modality {:?}: {}", modality, quality.overall_quality);
                continue; // Skip low quality samples
            }

            // Generate cancelable template
            let template = self.template_generator.generate_template(
                user_id,
                org_id,
                modality.clone(),
                &raw_data,
                "biohash", // Default transform
            )?;

            templates.push(template);
            quality_metrics.push(quality);
        }

        if templates.is_empty() {
            return Err(BiometricError::TemplateGeneration(
                "No quality biometric samples provided".to_string()
            ).into());
        }

        // Fuse templates using the fusion engine
        let multimodal_template = self.fusion_engine.create_multimodal_template(
            templates,
            quality_metrics,
            fusion_context,
        )?;

        // Monitor enrollment performance
        self.performance_monitor.record_enrollment(&multimodal_template);

        Ok(multimodal_template)
    }

    /// Authenticate using multimodal biometrics
    pub fn authenticate_multimodal(
        &mut self,
        query_data: Vec<(BiometricModality, Vec<u8>)>,
        stored_template: &MultimodalTemplate,
        fusion_context: FusionContext,
    ) -> Result<MultimodalAuthenticationResult> {
        let start_time = std::time::Instant::now();

        // Generate liveness challenges if required
        let mut liveness_results = Vec::new();
        if fusion_context.quality_requirements.require_liveness {
            for (modality, data) in &query_data {
                let challenge = self.liveness_detector.create_challenge(modality, 60)?;
                let response = BiometricResponse {
                    challenge_id: challenge.challenge_id,
                    biometric_data: data.clone(),
                    liveness_proof: LivenessProof {
                        proof_type: LivenessType::PassiveDetection,
                        proof_data: vec![],
                        confidence_score: 0.9, // Simulated
                        metadata: vec![],
                    },
                    timestamp: current_timestamp(),
                    device_attestation: None,
                    anti_replay_token: challenge.anti_replay_token.clone(),
                };

                let liveness_proof = self.liveness_detector.verify_liveness(&challenge, &response)?;
                liveness_results.push(liveness_proof);
            }
        }

        // Generate query templates
        let mut query_templates = Vec::new();
        for (modality, raw_data) in query_data {
            let template = self.template_generator.generate_template(
                stored_template.user_id,
                stored_template.org_id,
                modality,
                &raw_data,
                "biohash",
            )?;
            query_templates.push(template);
        }

        // Perform multimodal matching
        let matching_results = self.match_multimodal_templates(
            &query_templates,
            stored_template,
            &fusion_context,
        )?;

        // Assess risk
        let auth_context = AuthenticationContext {
            user_id: stored_template.user_id,
            device_id: "unknown".to_string(), // Would be provided in real implementation
            location: LocationInfo {
                country: "Unknown".to_string(),
                city: "Unknown".to_string(),
                ip_address: "0.0.0.0".to_string(),
                is_vpn: false,
                risk_score: 0.5,
            },
            timestamp: current_timestamp(),
            session_context: SessionContext {
                session_duration: 0,
                previous_authentications: vec![],
                device_trust_level: 0.8,
                behavioral_patterns: BehavioralPatterns {
                    typing_patterns: None,
                    mouse_patterns: None,
                    touch_patterns: None,
                },
            },
        };

        let biometric_data = MultimodalBiometricData {
            templates: query_templates,
            quality_metrics: vec![], // Would be computed
            liveness_proofs: liveness_results.clone(),
            collection_metadata: CollectionMetadata {
                collection_timestamp: current_timestamp(),
                device_info: DeviceInfo {
                    device_type: "Unknown".to_string(),
                    operating_system: "Unknown".to_string(),
                    browser: "Unknown".to_string(),
                    camera_info: None,
                    microphone_info: None,
                },
                environmental_conditions: fusion_context.environment_factors.clone(),
                collection_quality: 0.8,
            },
        };

        let risk_assessment = self.decision_engine.assess_authentication_risk(
            &auth_context,
            &biometric_data,
        )?;

        // Make final decision
        let decision = self.decision_engine.make_multimodal_decision(
            &matching_results,
            &risk_assessment,
            &fusion_context,
        )?;

        let authentication_time = start_time.elapsed().as_millis() as f64;

        let result = MultimodalAuthenticationResult {
            is_authenticated: decision.is_authenticated,
            confidence_score: decision.confidence_score,
            modality_results: matching_results.modality_results,
            fusion_result: matching_results.fusion_result,
            risk_assessment,
            liveness_verified: !liveness_results.is_empty(),
            authentication_time_ms: authentication_time,
            decision_factors: decision.decision_factors,
        };

        // Record performance metrics
        self.performance_monitor.record_authentication(&result);

        Ok(result)
    }

    fn match_multimodal_templates(
        &mut self,
        query_templates: &[BiometricTemplate],
        stored_template: &MultimodalTemplate,
        fusion_context: &FusionContext,
    ) -> Result<MultimodalMatchingResult> {
        let mut modality_results = Vec::new();

        // Match each query template against corresponding stored templates
        for query_template in query_templates {
            // Find matching modality in stored template
            if let Some(stored_modality_template) = stored_template.modality_templates
                .iter()
                .find(|t| t.modality == query_template.modality) {

                let matching_result = self.matching_engine.match_single_template(
                    query_template,
                    stored_modality_template,
                    &fusion_context.security_level,
                )?;

                let modality_score = ModalityScore {
                    modality: query_template.modality.clone(),
                    similarity_score: matching_result.match_score,
                    confidence: matching_result.confidence_score,
                    quality_impact: 0.0, // Would be computed from quality metrics
                    environmental_impact: self.calculate_environmental_impact(
                        &query_template.modality,
                        &fusion_context.environment_factors,
                    ),
                };

                modality_results.push(modality_score);
            }
        }

        // Fuse the scores
        let fusion_result = self.fusion_engine.fuse_modality_scores(
            &modality_results,
            fusion_context,
        )?;

        Ok(MultimodalMatchingResult {
            modality_results,
            fusion_result,
            template_id: stored_template.template_id,
        })
    }

    fn calculate_environmental_impact(
        &self,
        modality: &BiometricModality,
        environment: &EnvironmentFactors,
    ) -> f64 {
        match modality {
            BiometricModality::Face => {
                // Face recognition is heavily impacted by lighting
                environment.lighting_conditions * 0.8 + environment.device_stability * 0.2
            },
            BiometricModality::Voice => {
                // Voice recognition is impacted by noise and device quality
                (1.0 - environment.noise_level) * 0.7 + environment.device_stability * 0.3
            },
            BiometricModality::Fingerprint => {
                // Fingerprint is less affected by environment but sensitive to device stability
                environment.device_stability * 0.6 + environment.lighting_conditions * 0.4
            },
            BiometricModality::Iris => {
                // Iris requires good lighting and stability
                environment.lighting_conditions * 0.5 + environment.device_stability * 0.5
            },
            _ => 0.8, // Default impact
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultimodalAuthenticationResult {
    pub is_authenticated: bool,
    pub confidence_score: f64,
    pub modality_results: Vec<ModalityScore>,
    pub fusion_result: FusedMultimodalScore,
    pub risk_assessment: RiskAssessment,
    pub liveness_verified: bool,
    pub authentication_time_ms: f64,
    pub decision_factors: Vec<(String, f64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultimodalMatchingResult {
    pub modality_results: Vec<ModalityScore>,
    pub fusion_result: FusedMultimodalScore,
    pub template_id: Ulid,
}

#[derive(Debug, Clone)]
pub struct MultimodalDecisionResult {
    pub is_authenticated: bool,
    pub confidence_score: f64,
    pub decision_factors: Vec<(String, f64)>,
}

// Implementation of fusion engine
impl MultimodalFusionEngine {
    pub fn new() -> Result<Self> {
        let mut fusion_strategies: HashMap<String, Box<dyn MultimodalFusionStrategy>> = HashMap::new();

        fusion_strategies.insert("quality_weighted".to_string(), Box::new(QualityWeightedFusion));
        fusion_strategies.insert("adaptive_fusion".to_string(), Box::new(AdaptiveFusion));
        fusion_strategies.insert("reliability_based".to_string(), Box::new(ReliabilityBasedFusion));

        Ok(Self {
            fusion_strategies,
            quality_assessor: QualityBasedFusion::new(),
            adaptive_weighting: AdaptiveWeighting::new(),
        })
    }

    pub fn create_multimodal_template(
        &mut self,
        templates: Vec<BiometricTemplate>,
        quality_metrics: Vec<QualityMetrics>,
        fusion_context: FusionContext,
    ) -> Result<MultimodalTemplate> {
        // Analyze cross-modal relationships
        let correlation_matrix = self.compute_correlation_matrix(&templates)?;
        let redundancy_scores = self.compute_redundancy_scores(&templates, &correlation_matrix)?;
        let distinctiveness_scores = self.compute_distinctiveness_scores(&templates)?;

        // Compute optimal modality weights
        let modality_weights = self.adaptive_weighting.compute_optimal_weights(
            &templates,
            &quality_metrics,
            &fusion_context,
        )?;

        let fusion_metadata = FusionMetadata {
            fusion_strategy: "quality_weighted".to_string(),
            modality_weights,
            correlation_matrix,
            redundancy_scores,
            distinctiveness_scores,
        };

        let multimodal_quality = self.compute_multimodal_quality(&quality_metrics, &templates)?;

        Ok(MultimodalTemplate {
            template_id: Ulid::new(),
            user_id: fusion_context.user_id,
            org_id: templates[0].org_id,
            modality_templates: templates,
            fusion_metadata,
            quality_metrics: multimodal_quality,
            created_at: current_timestamp(),
            updated_at: current_timestamp(),
            version: 1,
        })
    }

    pub fn fuse_modality_scores(
        &self,
        modality_scores: &[ModalityScore],
        fusion_context: &FusionContext,
    ) -> Result<FusedMultimodalScore> {
        let strategy = self.fusion_strategies.get("quality_weighted")
            .ok_or_else(|| BiometricError::MatchingFailed)?;

        strategy.fuse_scores(modality_scores, fusion_context)
    }

    fn compute_correlation_matrix(&self, templates: &[BiometricTemplate]) -> Result<Vec<Vec<f64>>> {
        let n = templates.len();
        let mut matrix = vec![vec![0.0; n]; n];

        for i in 0..n {
            for j in 0..n {
                if i == j {
                    matrix[i][j] = 1.0;
                } else {
                    matrix[i][j] = self.compute_template_correlation(
                        &templates[i],
                        &templates[j],
                    )?;
                }
            }
        }

        Ok(matrix)
    }

    fn compute_template_correlation(
        &self,
        template1: &BiometricTemplate,
        template2: &BiometricTemplate,
    ) -> Result<f64> {
        // Simplified correlation computation
        let data1 = &template1.protected_template;
        let data2 = &template2.protected_template;

        if data1.len() != data2.len() {
            return Ok(0.0);
        }

        let mean1 = data1.iter().map(|&x| x as f64).sum::<f64>() / data1.len() as f64;
        let mean2 = data2.iter().map(|&x| x as f64).sum::<f64>() / data2.len() as f64;

        let mut numerator = 0.0;
        let mut sum_sq1 = 0.0;
        let mut sum_sq2 = 0.0;

        for (a, b) in data1.iter().zip(data2.iter()) {
            let dev1 = *a as f64 - mean1;
            let dev2 = *b as f64 - mean2;
            numerator += dev1 * dev2;
            sum_sq1 += dev1.powi(2);
            sum_sq2 += dev2.powi(2);
        }

        let denominator = (sum_sq1 * sum_sq2).sqrt();
        if denominator == 0.0 {
            Ok(0.0)
        } else {
            Ok(numerator / denominator)
        }
    }

    fn compute_redundancy_scores(
        &self,
        templates: &[BiometricTemplate],
        correlation_matrix: &[Vec<f64>],
    ) -> Result<Vec<f64>> {
        let mut redundancy_scores = Vec::new();

        for i in 0..templates.len() {
            let mut total_correlation = 0.0;
            let mut count = 0;

            for j in 0..templates.len() {
                if i != j {
                    total_correlation += correlation_matrix[i][j].abs();
                    count += 1;
                }
            }

            let avg_correlation = if count > 0 { total_correlation / count as f64 } else { 0.0 };
            redundancy_scores.push(avg_correlation);
        }

        Ok(redundancy_scores)
    }

    fn compute_distinctiveness_scores(&self, templates: &[BiometricTemplate]) -> Result<Vec<f64>> {
        let mut distinctiveness_scores = Vec::new();

        for template in templates {
            // Simple distinctiveness based on entropy of template data
            let distinctiveness = self.compute_entropy(&template.protected_template);
            distinctiveness_scores.push(distinctiveness);
        }

        Ok(distinctiveness_scores)
    }

    fn compute_entropy(&self, data: &[u8]) -> f64 {
        let mut counts = [0u32; 256];
        for &byte in data {
            counts[byte as usize] += 1;
        }

        let mut entropy = 0.0;
        let total = data.len() as f64;

        for &count in &counts {
            if count > 0 {
                let p = count as f64 / total;
                entropy -= p * p.log2();
            }
        }

        entropy / 8.0 // Normalize to [0,1]
    }

    fn compute_multimodal_quality(
        &self,
        quality_metrics: &[QualityMetrics],
        templates: &[BiometricTemplate],
    ) -> Result<MultimodalQualityMetrics> {
        let overall_quality = quality_metrics.iter()
            .map(|q| q.overall_quality)
            .sum::<f64>() / quality_metrics.len() as f64;

        // Compute cross-modal consistency
        let cross_modal_consistency = self.compute_cross_modal_consistency(quality_metrics)?;

        // Compute template stability (simplified)
        let template_stability = self.compute_template_stability(templates)?;

        // Compute discriminability
        let discriminability = self.compute_discriminability(templates)?;

        Ok(MultimodalQualityMetrics {
            overall_quality,
            modality_qualities: quality_metrics.to_vec(),
            cross_modal_consistency,
            template_stability,
            discriminability,
        })
    }

    fn compute_cross_modal_consistency(&self, quality_metrics: &[QualityMetrics]) -> Result<f64> {
        if quality_metrics.len() < 2 {
            return Ok(1.0);
        }

        let mean_quality = quality_metrics.iter()
            .map(|q| q.overall_quality)
            .sum::<f64>() / quality_metrics.len() as f64;

        let variance = quality_metrics.iter()
            .map(|q| (q.overall_quality - mean_quality).powi(2))
            .sum::<f64>() / quality_metrics.len() as f64;

        // Higher consistency (lower variance) is better
        Ok(1.0 - variance.sqrt().min(1.0))
    }

    fn compute_template_stability(&self, templates: &[BiometricTemplate]) -> Result<f64> {
        // Simplified stability computation based on quality scores
        let stability_sum = templates.iter()
            .map(|t| t.quality_metrics.overall_quality)
            .sum::<f64>();

        Ok(stability_sum / templates.len() as f64)
    }

    fn compute_discriminability(&self, templates: &[BiometricTemplate]) -> Result<f64> {
        // Simplified discriminability based on template diversity
        if templates.len() < 2 {
            return Ok(1.0);
        }

        let mut total_distance = 0.0;
        let mut count = 0;

        for i in 0..templates.len() {
            for j in (i + 1)..templates.len() {
                let distance = self.compute_template_distance(&templates[i], &templates[j])?;
                total_distance += distance;
                count += 1;
            }
        }

        let avg_distance = if count > 0 { total_distance / count as f64 } else { 0.0 };
        Ok(avg_distance.min(1.0))
    }

    fn compute_template_distance(
        &self,
        template1: &BiometricTemplate,
        template2: &BiometricTemplate,
    ) -> Result<f64> {
        let data1 = &template1.protected_template;
        let data2 = &template2.protected_template;

        let min_len = data1.len().min(data2.len());
        let mut distance = 0.0;

        for i in 0..min_len {
            distance += (data1[i] as f64 - data2[i] as f64).abs();
        }

        Ok(distance / (min_len as f64 * 255.0))
    }
}

// Fusion strategy implementations
pub struct QualityWeightedFusion;
pub struct AdaptiveFusion;
pub struct ReliabilityBasedFusion;

impl MultimodalFusionStrategy for QualityWeightedFusion {
    fn fuse_templates(
        &self,
        _templates: &[BiometricTemplate],
        _fusion_context: &FusionContext,
    ) -> Result<MultimodalTemplate> {
        // This would be implemented for template-level fusion
        // For now, we'll return an error as this is primarily used for score fusion
        Err(BiometricError::MatchingFailed.into())
    }

    fn fuse_scores(
        &self,
        modality_scores: &[ModalityScore],
        _fusion_context: &FusionContext,
    ) -> Result<FusedMultimodalScore> {
        let total_weight = modality_scores.iter()
            .map(|s| s.confidence)
            .sum::<f64>();

        if total_weight == 0.0 {
            return Ok(FusedMultimodalScore {
                final_score: 0.0,
                confidence: 0.0,
                individual_contributions: Vec::new(),
                fusion_strategy: self.strategy_name().to_string(),
                quality_assessment: 0.0,
            });
        }

        let mut weighted_score = 0.0;
        let mut contributions = Vec::new();

        for score in modality_scores {
            let weight = score.confidence / total_weight;
            let quality_adjusted_score = score.similarity_score * score.quality_impact.max(0.1);
            weighted_score += quality_adjusted_score * weight;

            contributions.push(ModalityContribution {
                modality: score.modality.clone(),
                contribution_weight: weight,
                original_score: score.similarity_score,
                quality_adjusted_score,
            });
        }

        let confidence = modality_scores.iter()
            .map(|s| s.confidence)
            .sum::<f64>() / modality_scores.len() as f64;

        let quality_assessment = modality_scores.iter()
            .map(|s| s.quality_impact)
            .sum::<f64>() / modality_scores.len() as f64;

        Ok(FusedMultimodalScore {
            final_score: weighted_score,
            confidence,
            individual_contributions: contributions,
            fusion_strategy: self.strategy_name().to_string(),
            quality_assessment,
        })
    }

    fn strategy_name(&self) -> &str {
        "quality_weighted"
    }
}

impl MultimodalFusionStrategy for AdaptiveFusion {
    fn fuse_templates(
        &self,
        _templates: &[BiometricTemplate],
        _fusion_context: &FusionContext,
    ) -> Result<MultimodalTemplate> {
        Err(BiometricError::MatchingFailed.into())
    }

    fn fuse_scores(
        &self,
        modality_scores: &[ModalityScore],
        fusion_context: &FusionContext,
    ) -> Result<FusedMultimodalScore> {
        // Adaptive fusion based on environment and user preferences
        let mut adaptive_weights = Vec::new();

        for score in modality_scores {
            let mut weight = score.confidence;

            // Adjust weight based on environment
            weight *= score.environmental_impact;

            // Adjust based on user preferences
            if fusion_context.user_preferences.preferred_modalities.contains(&score.modality) {
                weight *= 1.2;
            }

            adaptive_weights.push(weight);
        }

        // Normalize weights
        let total_weight: f64 = adaptive_weights.iter().sum();
        if total_weight > 0.0 {
            for weight in &mut adaptive_weights {
                *weight /= total_weight;
            }
        }

        // Compute weighted score
        let final_score = modality_scores.iter()
            .zip(adaptive_weights.iter())
            .map(|(score, weight)| score.similarity_score * weight)
            .sum::<f64>();

        let confidence = modality_scores.iter()
            .map(|s| s.confidence)
            .sum::<f64>() / modality_scores.len() as f64;

        let contributions = modality_scores.iter()
            .zip(adaptive_weights.iter())
            .map(|(score, weight)| ModalityContribution {
                modality: score.modality.clone(),
                contribution_weight: *weight,
                original_score: score.similarity_score,
                quality_adjusted_score: score.similarity_score * score.quality_impact,
            })
            .collect();

        Ok(FusedMultimodalScore {
            final_score,
            confidence,
            individual_contributions: contributions,
            fusion_strategy: self.strategy_name().to_string(),
            quality_assessment: modality_scores.iter()
                .map(|s| s.quality_impact)
                .sum::<f64>() / modality_scores.len() as f64,
        })
    }

    fn strategy_name(&self) -> &str {
        "adaptive_fusion"
    }
}

impl MultimodalFusionStrategy for ReliabilityBasedFusion {
    fn fuse_templates(
        &self,
        _templates: &[BiometricTemplate],
        _fusion_context: &FusionContext,
    ) -> Result<MultimodalTemplate> {
        Err(BiometricError::MatchingFailed.into())
    }

    fn fuse_scores(
        &self,
        modality_scores: &[ModalityScore],
        _fusion_context: &FusionContext,
    ) -> Result<FusedMultimodalScore> {
        // Reliability-based fusion using modality-specific reliability weights
        let reliability_weights = self.get_reliability_weights();

        let mut weighted_scores = Vec::new();
        let mut total_reliability = 0.0;

        for score in modality_scores {
            let reliability = reliability_weights.get(&score.modality).unwrap_or(&0.5);
            let weighted_score = score.similarity_score * reliability * score.confidence;
            weighted_scores.push(weighted_score);
            total_reliability += reliability;
        }

        let final_score = if total_reliability > 0.0 {
            weighted_scores.iter().sum::<f64>() / total_reliability
        } else {
            0.0
        };

        let confidence = modality_scores.iter()
            .map(|s| s.confidence)
            .sum::<f64>() / modality_scores.len() as f64;

        let contributions = modality_scores.iter()
            .map(|score| {
                let reliability = *reliability_weights.get(&score.modality).unwrap_or(&0.5);
                ModalityContribution {
                    modality: score.modality.clone(),
                    contribution_weight: reliability,
                    original_score: score.similarity_score,
                    quality_adjusted_score: score.similarity_score * reliability,
                }
            })
            .collect();

        Ok(FusedMultimodalScore {
            final_score,
            confidence,
            individual_contributions: contributions,
            fusion_strategy: self.strategy_name().to_string(),
            quality_assessment: modality_scores.iter()
                .map(|s| s.quality_impact)
                .sum::<f64>() / modality_scores.len() as f64,
        })
    }

    fn strategy_name(&self) -> &str {
        "reliability_based"
    }
}

impl ReliabilityBasedFusion {
    fn get_reliability_weights(&self) -> HashMap<BiometricModality, f64> {
        let mut weights = HashMap::new();
        weights.insert(BiometricModality::Iris, 0.95);
        weights.insert(BiometricModality::Fingerprint, 0.85);
        weights.insert(BiometricModality::Face, 0.75);
        weights.insert(BiometricModality::Voice, 0.65);
        weights
    }
}

// Implementation of remaining components
impl QualityBasedFusion {
    pub fn new() -> Self {
        let mut quality_thresholds = HashMap::new();
        quality_thresholds.insert(BiometricModality::Face, 0.7);
        quality_thresholds.insert(BiometricModality::Fingerprint, 0.8);
        quality_thresholds.insert(BiometricModality::Voice, 0.6);
        quality_thresholds.insert(BiometricModality::Iris, 0.9);

        let mut quality_weights = HashMap::new();
        quality_weights.insert(BiometricModality::Face, 0.8);
        quality_weights.insert(BiometricModality::Fingerprint, 0.9);
        quality_weights.insert(BiometricModality::Voice, 0.7);
        quality_weights.insert(BiometricModality::Iris, 0.95);

        Self {
            quality_thresholds,
            quality_weights,
        }
    }
}

impl AdaptiveWeighting {
    pub fn new() -> Self {
        Self {
            user_profiles: HashMap::new(),
            learning_rate: 0.1,
            adaptation_window: 100,
        }
    }

    pub fn compute_optimal_weights(
        &mut self,
        templates: &[BiometricTemplate],
        quality_metrics: &[QualityMetrics],
        fusion_context: &FusionContext,
    ) -> Result<Vec<f64>> {
        let mut weights = Vec::new();

        // Get or create user profile
        let user_profile = self.user_profiles.entry(fusion_context.user_id)
            .or_insert_with(|| UserBiometricProfile {
                user_id: fusion_context.user_id,
                modality_preferences: HashMap::new(),
                authentication_patterns: AuthenticationPatterns {
                    time_of_day_patterns: vec![],
                    device_patterns: HashMap::new(),
                    environment_patterns: HashMap::new(),
                },
                quality_history: QualityHistory {
                    quality_samples: vec![],
                    quality_trends: HashMap::new(),
                    stability_metrics: HashMap::new(),
                },
                last_updated: current_timestamp(),
            });

        for (i, template) in templates.iter().enumerate() {
            let base_weight = quality_metrics[i].overall_quality;

            // Adjust based on user history
            let historical_adjustment = if let Some(pref) = user_profile.modality_preferences.get(&template.modality) {
                pref.success_rate * 0.2
            } else {
                0.0
            };

            let final_weight = (base_weight + historical_adjustment).clamp(0.1, 1.0);
            weights.push(final_weight);
        }

        // Normalize weights
        let total: f64 = weights.iter().sum();
        if total > 0.0 {
            for weight in &mut weights {
                *weight /= total;
            }
        }

        Ok(weights)
    }
}

impl MultimodalDecisionEngine {
    pub fn new() -> Result<Self> {
        Ok(Self {
            decision_policies: HashMap::new(),
            risk_assessor: RiskAssessmentEngine::new()?,
            adaptive_thresholds: AdaptiveThresholdSystem::new(),
        })
    }

    pub fn assess_authentication_risk(
        &self,
        context: &AuthenticationContext,
        biometric_data: &MultimodalBiometricData,
    ) -> Result<RiskAssessment> {
        self.risk_assessor.assess_risk(context, biometric_data)
    }

    pub fn make_multimodal_decision(
        &self,
        matching_result: &MultimodalMatchingResult,
        risk_assessment: &RiskAssessment,
        fusion_context: &FusionContext,
    ) -> Result<MultimodalDecisionResult> {
        // Combine biometric score with risk assessment
        let biometric_confidence = matching_result.fusion_result.confidence;
        let risk_factor = match risk_assessment.risk_level {
            RiskLevel::VeryLow => 1.0,
            RiskLevel::Low => 0.9,
            RiskLevel::Medium => 0.7,
            RiskLevel::High => 0.5,
            RiskLevel::VeryHigh => 0.2,
            RiskLevel::Critical => 0.0,
        };

        let adjusted_confidence = biometric_confidence * risk_factor;

        // Get adaptive threshold
        let threshold = self.adaptive_thresholds.get_threshold(
            &fusion_context.security_level,
            &risk_assessment.risk_level,
        );

        let is_authenticated = adjusted_confidence >= threshold;

        let decision_factors = vec![
            ("biometric_confidence".to_string(), biometric_confidence),
            ("risk_factor".to_string(), risk_factor),
            ("threshold".to_string(), threshold),
            ("adjusted_confidence".to_string(), adjusted_confidence),
        ];

        Ok(MultimodalDecisionResult {
            is_authenticated,
            confidence_score: adjusted_confidence,
            decision_factors,
        })
    }
}

impl RiskAssessmentEngine {
    pub fn new() -> Result<Self> {
        Ok(Self {
            risk_models: HashMap::new(),
            threat_intelligence: ThreatIntelligenceService::new(),
        })
    }

    pub fn assess_risk(
        &self,
        context: &AuthenticationContext,
        _biometric_data: &MultimodalBiometricData,
    ) -> Result<RiskAssessment> {
        let mut risk_score = 0.0;
        let mut contributing_factors = Vec::new();

        // Location-based risk
        let location_risk = context.location.risk_score;
        risk_score += location_risk * 0.3;
        contributing_factors.push(RiskContributor {
            factor_name: "location".to_string(),
            contribution: location_risk * 0.3,
            description: "Risk based on geographic location".to_string(),
        });

        // Device trust level
        let device_risk = 1.0 - context.session_context.device_trust_level;
        risk_score += device_risk * 0.4;
        contributing_factors.push(RiskContributor {
            factor_name: "device_trust".to_string(),
            contribution: device_risk * 0.4,
            description: "Risk based on device trust level".to_string(),
        });

        // VPN usage
        if context.location.is_vpn {
            risk_score += 0.2;
            contributing_factors.push(RiskContributor {
                factor_name: "vpn_usage".to_string(),
                contribution: 0.2,
                description: "Additional risk due to VPN usage".to_string(),
            });
        }

        // Time-based patterns
        let time_risk = self.assess_time_based_risk(context);
        risk_score += time_risk * 0.1;
        contributing_factors.push(RiskContributor {
            factor_name: "time_patterns".to_string(),
            contribution: time_risk * 0.1,
            description: "Risk based on unusual access time".to_string(),
        });

        let risk_level = match risk_score {
            x if x < 0.2 => RiskLevel::VeryLow,
            x if x < 0.4 => RiskLevel::Low,
            x if x < 0.6 => RiskLevel::Medium,
            x if x < 0.8 => RiskLevel::High,
            x if x < 1.0 => RiskLevel::VeryHigh,
            _ => RiskLevel::Critical,
        };

        let recommended_actions = match risk_level {
            RiskLevel::VeryLow | RiskLevel::Low => vec![RecommendedAction::Allow],
            RiskLevel::Medium => vec![RecommendedAction::RequireAdditionalAuthentication],
            RiskLevel::High => vec![RecommendedAction::RequireStepUpAuthentication],
            RiskLevel::VeryHigh => vec![RecommendedAction::RequireManualReview],
            RiskLevel::Critical => vec![RecommendedAction::Deny],
        };

        Ok(RiskAssessment {
            risk_level,
            risk_score,
            contributing_factors,
            recommended_actions,
        })
    }

    fn assess_time_based_risk(&self, context: &AuthenticationContext) -> f64 {
        // Simple time-based risk assessment
        // In practice, this would analyze historical patterns
        let current_hour = (context.timestamp / 3600) % 24;

        // Higher risk for unusual hours (late night/early morning)
        match current_hour {
            0..=5 => 0.3,   // Late night
            6..=8 => 0.1,   // Early morning
            9..=17 => 0.0,  // Business hours
            18..=22 => 0.1, // Evening
            _ => 0.2,       // Late evening
        }
    }
}

struct AdaptiveThresholdSystem;

impl AdaptiveThresholdSystem {
    pub fn new() -> Self {
        Self
    }

    pub fn get_threshold(&self, security_level: &SecurityLevel, risk_level: &RiskLevel) -> f64 {
        let base_threshold = match security_level {
            SecurityLevel::Low => 0.6,
            SecurityLevel::Medium => 0.7,
            SecurityLevel::High => 0.8,
            SecurityLevel::VeryHigh => 0.85,
        };

        let risk_adjustment = match risk_level {
            RiskLevel::VeryLow => -0.05,
            RiskLevel::Low => 0.0,
            RiskLevel::Medium => 0.05,
            RiskLevel::High => 0.1,
            RiskLevel::VeryHigh => 0.15,
            RiskLevel::Critical => 0.2,
        };

        (base_threshold as f64 + risk_adjustment).clamp(0.3_f64, 0.95_f64)
    }
}

impl ThreatIntelligenceService {
    pub fn new() -> Self {
        Self {
            threat_feeds: Vec::new(),
            attack_patterns: HashMap::new(),
            ip_reputation: HashMap::new(),
        }
    }
}

impl MultimodalPerformanceMonitor {
    pub fn new() -> Self {
        Self {
            performance_metrics: HashMap::new(),
            user_satisfaction: HashMap::new(),
        }
    }

    pub fn record_enrollment(&mut self, template: &MultimodalTemplate) {
        // Record enrollment metrics
        tracing::info!(
            "Multimodal enrollment completed for user {} with {} modalities",
            template.user_id,
            template.modality_templates.len()
        );
    }

    pub fn record_authentication(&mut self, result: &MultimodalAuthenticationResult) {
        // Record authentication metrics
        tracing::info!(
            "Multimodal authentication completed: {} (confidence: {:.3}, time: {:.1}ms)",
            if result.is_authenticated { "SUCCESS" } else { "FAILURE" },
            result.confidence_score,
            result.authentication_time_ms
        );
    }
}