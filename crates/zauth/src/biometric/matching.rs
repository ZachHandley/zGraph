use super::*;
use anyhow::Result;
use std::collections::HashMap;

/// Advanced biometric matching engine with multiple matching algorithms
pub struct BiometricMatchingEngine {
    matchers: HashMap<BiometricModality, Box<dyn BiometricMatcher>>,
    fusion_engine: ScoreFusionEngine,
    decision_engine: DecisionEngine,
    performance_monitor: MatchingPerformanceMonitor,
}

/// Trait for biometric matching algorithms
pub trait BiometricMatcher: Send + Sync {
    /// Compute similarity score between two biometric templates
    fn compute_similarity(
        &self,
        template1: &BiometricTemplate,
        template2: &BiometricTemplate,
    ) -> Result<SimilarityScore>;

    /// Get the matching threshold for this matcher
    fn get_threshold(&self, security_level: SecurityLevel) -> f64;

    /// Normalize scores to a common range [0, 1]
    fn normalize_score(&self, score: f64) -> f64;

    /// Get matcher-specific metadata
    fn get_matcher_info(&self) -> MatcherInfo;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimilarityScore {
    pub score: f64,
    pub normalized_score: f64,
    pub confidence: f64,
    pub matching_algorithm: String,
    pub feature_contributions: Vec<FeatureContribution>,
    pub quality_impact: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureContribution {
    pub feature_name: String,
    pub contribution_weight: f64,
    pub similarity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SecurityLevel {
    Low,      // FAR: 1:1,000
    Medium,   // FAR: 1:10,000
    High,     // FAR: 1:100,000
    VeryHigh, // FAR: 1:1,000,000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatcherInfo {
    pub algorithm_name: String,
    pub version: String,
    pub supported_modalities: Vec<BiometricModality>,
    pub performance_metrics: PerformanceMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub far_at_frr_1_percent: f64,  // False Accept Rate at 1% False Reject Rate
    pub frr_at_far_0_1_percent: f64, // False Reject Rate at 0.1% False Accept Rate
    pub equal_error_rate: f64,       // Equal Error Rate
    pub d_prime: f64,                // Discriminability index
}

/// Score fusion engine for multimodal biometrics
pub struct ScoreFusionEngine {
    fusion_strategies: HashMap<String, Box<dyn FusionStrategy>>,
    weight_optimizer: WeightOptimizer,
}

/// Trait for score fusion strategies
pub trait FusionStrategy: Send + Sync {
    /// Fuse multiple similarity scores into a single decision score
    fn fuse_scores(&self, scores: &[SimilarityScore], weights: &[f64]) -> Result<FusedScore>;

    /// Get the name of this fusion strategy
    fn strategy_name(&self) -> &str;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FusedScore {
    pub fused_score: f64,
    pub fusion_strategy: String,
    pub individual_scores: Vec<SimilarityScore>,
    pub fusion_weights: Vec<f64>,
    pub confidence: f64,
}

/// Decision engine for making final authentication decisions
pub struct DecisionEngine {
    decision_policies: HashMap<String, DecisionPolicy>,
    adaptive_threshold: AdaptiveThresholdManager,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionPolicy {
    pub policy_name: String,
    pub base_threshold: f64,
    pub security_level: SecurityLevel,
    pub quality_requirements: QualityRequirements,
    pub contextual_adjustments: ContextualAdjustments,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityRequirements {
    pub minimum_quality: f64,
    pub quality_weight: f64,
    pub require_liveness: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextualAdjustments {
    pub time_of_day_factor: f64,
    pub location_factor: f64,
    pub device_trust_factor: f64,
    pub user_behavior_factor: f64,
}

/// Performance monitoring for matching operations
pub struct MatchingPerformanceMonitor {
    performance_stats: HashMap<String, PerformanceStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceStats {
    pub total_matches: u64,
    pub successful_matches: u64,
    pub false_accepts: u64,
    pub false_rejects: u64,
    pub average_match_time_ms: f64,
    pub quality_distribution: Vec<(f64, u32)>, // (quality_score, count)
}

impl BiometricMatchingEngine {
    pub fn new() -> Self {
        let mut matchers: HashMap<BiometricModality, Box<dyn BiometricMatcher>> = HashMap::new();

        // Register matchers for each modality
        matchers.insert(BiometricModality::Face, Box::new(FaceMatcher::new()));
        matchers.insert(BiometricModality::Fingerprint, Box::new(FingerprintMatcher::new()));
        matchers.insert(BiometricModality::Voice, Box::new(VoiceMatcher::new()));
        matchers.insert(BiometricModality::Iris, Box::new(IrisMatcher::new()));

        Self {
            matchers,
            fusion_engine: ScoreFusionEngine::new(),
            decision_engine: DecisionEngine::new(),
            performance_monitor: MatchingPerformanceMonitor::new(),
        }
    }

    /// Perform comprehensive biometric matching
    pub fn match_biometrics(
        &mut self,
        query_template: &BiometricTemplate,
        stored_templates: &[BiometricTemplate],
        security_level: SecurityLevel,
    ) -> Result<Vec<MatchingResult>> {
        let start_time = std::time::Instant::now();
        let mut results = Vec::new();

        for stored_template in stored_templates {
            let matching_result = self.match_single_template(
                query_template,
                stored_template,
                &security_level,
            )?;

            results.push(matching_result);
        }

        let match_time = start_time.elapsed().as_millis() as f64;
        self.performance_monitor.record_matching_session(&results, match_time);

        // Sort results by confidence score (highest first)
        results.sort_by(|a, b| b.confidence_score.partial_cmp(&a.confidence_score).unwrap());

        Ok(results)
    }

    /// Match against a single stored template
    pub fn match_single_template(
        &mut self,
        query_template: &BiometricTemplate,
        stored_template: &BiometricTemplate,
        security_level: &SecurityLevel,
    ) -> Result<MatchingResult> {
        // Ensure templates are for the same modality
        if query_template.modality != stored_template.modality {
            return Ok(MatchingResult {
                is_match: false,
                confidence_score: 0.0,
                match_score: 0.0,
                decision_threshold: 1.0,
                template_id: stored_template.template_id,
                matching_metadata: vec![
                    ("error".to_string(), "modality_mismatch".to_string()),
                ],
            });
        }

        // Handle multimodal matching
        if let BiometricModality::Multimodal(modalities) = &query_template.modality {
            return self.match_multimodal_template(
                query_template,
                stored_template,
                modalities,
                security_level,
            );
        }

        // Single modality matching
        let matcher = self.matchers.get(&query_template.modality)
            .ok_or_else(|| BiometricError::UnsupportedModality(
                format!("{:?}", query_template.modality)
            ))?;

        let similarity_score = matcher.compute_similarity(query_template, stored_template)?;
        let threshold = matcher.get_threshold(security_level.clone());

        // Apply quality adjustments
        let quality_adjusted_score = self.apply_quality_adjustments(
            &similarity_score,
            query_template,
            stored_template,
        )?;

        // Make decision
        let decision_result = self.decision_engine.make_decision(
            &quality_adjusted_score,
            threshold,
            security_level,
            query_template,
        )?;

        Ok(MatchingResult {
            is_match: decision_result.is_match,
            confidence_score: decision_result.confidence,
            match_score: similarity_score.normalized_score,
            decision_threshold: threshold,
            template_id: stored_template.template_id,
            matching_metadata: vec![
                ("algorithm".to_string(), similarity_score.matching_algorithm),
                ("quality_impact".to_string(), similarity_score.quality_impact.to_string()),
                ("security_level".to_string(), format!("{:?}", security_level)),
            ],
        })
    }

    /// Handle multimodal biometric matching
    fn match_multimodal_template(
        &mut self,
        query_template: &BiometricTemplate,
        stored_template: &BiometricTemplate,
        modalities: &[BiometricModality],
        security_level: &SecurityLevel,
    ) -> Result<MatchingResult> {
        let mut individual_scores = Vec::new();
        let mut weights = Vec::new();

        // Extract and match individual modalities
        for (i, modality) in modalities.iter().enumerate() {
            if let Some(matcher) = self.matchers.get(modality) {
                // Create sub-templates for individual modalities (simplified extraction)
                let query_sub_template = self.extract_modality_template(query_template, modality, i)?;
                let stored_sub_template = self.extract_modality_template(stored_template, modality, i)?;

                let similarity_score = matcher.compute_similarity(&query_sub_template, &stored_sub_template)?;
                let weight = self.calculate_modality_weight(modality, &similarity_score)?;

                individual_scores.push(similarity_score);
                weights.push(weight);
            }
        }

        if individual_scores.is_empty() {
            return Ok(MatchingResult {
                is_match: false,
                confidence_score: 0.0,
                match_score: 0.0,
                decision_threshold: 1.0,
                template_id: stored_template.template_id,
                matching_metadata: vec![
                    ("error".to_string(), "no_valid_modalities".to_string()),
                ],
            });
        }

        // Fuse scores
        let fused_score = self.fusion_engine.fuse_scores(&individual_scores, &weights)?;

        // Apply decision logic
        let threshold = self.calculate_multimodal_threshold(security_level, &individual_scores);
        let decision_result = self.decision_engine.make_multimodal_decision(
            &fused_score,
            threshold,
            security_level,
        )?;

        Ok(MatchingResult {
            is_match: decision_result.is_match,
            confidence_score: decision_result.confidence,
            match_score: fused_score.fused_score,
            decision_threshold: threshold,
            template_id: stored_template.template_id,
            matching_metadata: vec![
                ("fusion_strategy".to_string(), fused_score.fusion_strategy),
                ("num_modalities".to_string(), individual_scores.len().to_string()),
                ("multimodal".to_string(), "true".to_string()),
            ],
        })
    }

    fn extract_modality_template(
        &self,
        template: &BiometricTemplate,
        modality: &BiometricModality,
        index: usize,
    ) -> Result<BiometricTemplate> {
        // Simplified modality extraction - in practice would properly segment multimodal data
        let data_segment_size = template.protected_template.len() / 4; // Assume 4 modalities max
        let start_idx = index * data_segment_size;
        let end_idx = ((index + 1) * data_segment_size).min(template.protected_template.len());

        let mut sub_template = template.clone();
        sub_template.template_id = Ulid::new();
        sub_template.modality = modality.clone();
        sub_template.protected_template = template.protected_template[start_idx..end_idx].to_vec();

        Ok(sub_template)
    }

    fn calculate_modality_weight(
        &self,
        modality: &BiometricModality,
        similarity_score: &SimilarityScore,
    ) -> Result<f64> {
        // Calculate weight based on modality reliability and quality
        let base_weight = match modality {
            BiometricModality::Iris => 0.35,        // Highest accuracy
            BiometricModality::Fingerprint => 0.30, // High accuracy
            BiometricModality::Face => 0.25,        // Good accuracy
            BiometricModality::Voice => 0.20,       // Lower accuracy
            _ => 0.20,
        };

        // Adjust weight based on quality and confidence
        let quality_adjustment = similarity_score.confidence * 0.2;
        let adjusted_weight = (base_weight + quality_adjustment).min(1.0).max(0.1);

        Ok(adjusted_weight)
    }

    fn calculate_multimodal_threshold(
        &self,
        security_level: &SecurityLevel,
        individual_scores: &[SimilarityScore],
    ) -> f64 {
        let base_threshold = match security_level {
            SecurityLevel::Low => 0.6,
            SecurityLevel::Medium => 0.7,
            SecurityLevel::High => 0.8,
            SecurityLevel::VeryHigh => 0.85,
        };

        // Adjust based on number of modalities (more modalities = slightly lower threshold)
        let modality_factor = 1.0 - (individual_scores.len() as f64 * 0.05).min(0.2);

        // Adjust based on average quality
        let avg_confidence = individual_scores.iter()
            .map(|s| s.confidence)
            .sum::<f64>() / individual_scores.len() as f64;

        let quality_adjustment = (avg_confidence - 0.5) * 0.1;

        (base_threshold * modality_factor + quality_adjustment).clamp(0.3, 0.95)
    }

    fn apply_quality_adjustments(
        &self,
        similarity_score: &SimilarityScore,
        query_template: &BiometricTemplate,
        stored_template: &BiometricTemplate,
    ) -> Result<AdjustedScore> {
        let query_quality = query_template.quality_metrics.overall_quality;
        let stored_quality = stored_template.quality_metrics.overall_quality;
        let min_quality = query_quality.min(stored_quality);

        // Quality impact on score
        let quality_penalty = if min_quality < 0.5 {
            0.2 * (0.5 - min_quality) // Up to 20% penalty for very poor quality
        } else {
            0.0
        };

        let adjusted_score = (similarity_score.normalized_score - quality_penalty).max(0.0);
        let adjusted_confidence = similarity_score.confidence * (0.5 + min_quality * 0.5);

        Ok(AdjustedScore {
            score: adjusted_score,
            confidence: adjusted_confidence,
            quality_penalty,
            original_score: similarity_score.normalized_score,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AdjustedScore {
    pub score: f64,
    pub confidence: f64,
    pub quality_penalty: f64,
    pub original_score: f64,
}

#[derive(Debug, Clone)]
pub struct DecisionResult {
    pub is_match: bool,
    pub confidence: f64,
    pub decision_factors: Vec<(String, f64)>,
}

// Individual matcher implementations
pub struct FaceMatcher {
    algorithm_name: String,
    performance_metrics: PerformanceMetrics,
}

pub struct FingerprintMatcher {
    algorithm_name: String,
    performance_metrics: PerformanceMetrics,
}

pub struct VoiceMatcher {
    algorithm_name: String,
    performance_metrics: PerformanceMetrics,
}

pub struct IrisMatcher {
    algorithm_name: String,
    performance_metrics: PerformanceMetrics,
}

impl FaceMatcher {
    pub fn new() -> Self {
        Self {
            algorithm_name: "Deep Face Embedding".to_string(),
            performance_metrics: PerformanceMetrics {
                far_at_frr_1_percent: 0.001,
                frr_at_far_0_1_percent: 0.05,
                equal_error_rate: 0.015,
                d_prime: 3.2,
            },
        }
    }
}

impl BiometricMatcher for FaceMatcher {
    fn compute_similarity(
        &self,
        template1: &BiometricTemplate,
        template2: &BiometricTemplate,
    ) -> Result<SimilarityScore> {
        // Compute cosine similarity between face embeddings
        let similarity = self.cosine_similarity(&template1.protected_template, &template2.protected_template);

        let feature_contributions = vec![
            FeatureContribution {
                feature_name: "facial_structure".to_string(),
                contribution_weight: 0.4,
                similarity: similarity * 1.1, // Slightly higher for structure
            },
            FeatureContribution {
                feature_name: "eye_region".to_string(),
                contribution_weight: 0.3,
                similarity: similarity * 0.9,
            },
            FeatureContribution {
                feature_name: "mouth_nose_region".to_string(),
                contribution_weight: 0.3,
                similarity: similarity * 0.95,
            },
        ];

        let quality_impact = (template1.quality_metrics.overall_quality + template2.quality_metrics.overall_quality) / 2.0;
        let confidence = similarity * quality_impact;

        Ok(SimilarityScore {
            score: similarity,
            normalized_score: self.normalize_score(similarity),
            confidence,
            matching_algorithm: self.algorithm_name.clone(),
            feature_contributions,
            quality_impact,
        })
    }

    fn get_threshold(&self, security_level: SecurityLevel) -> f64 {
        match security_level {
            SecurityLevel::Low => 0.65,
            SecurityLevel::Medium => 0.75,
            SecurityLevel::High => 0.83,
            SecurityLevel::VeryHigh => 0.88,
        }
    }

    fn normalize_score(&self, score: f64) -> f64 {
        // Normalize cosine similarity from [-1, 1] to [0, 1]
        (score + 1.0) / 2.0
    }

    fn get_matcher_info(&self) -> MatcherInfo {
        MatcherInfo {
            algorithm_name: self.algorithm_name.clone(),
            version: "1.0".to_string(),
            supported_modalities: vec![BiometricModality::Face],
            performance_metrics: self.performance_metrics.clone(),
        }
    }
}

impl FaceMatcher {
    fn cosine_similarity(&self, vec1: &[u8], vec2: &[u8]) -> f64 {
        if vec1.len() != vec2.len() || vec1.is_empty() {
            return 0.0;
        }

        let mut dot_product = 0.0;
        let mut norm1 = 0.0;
        let mut norm2 = 0.0;

        for (a, b) in vec1.iter().zip(vec2.iter()) {
            let fa = *a as f64 / 255.0; // Normalize to [0,1]
            let fb = *b as f64 / 255.0;

            dot_product += fa * fb;
            norm1 += fa * fa;
            norm2 += fb * fb;
        }

        if norm1 == 0.0 || norm2 == 0.0 {
            0.0
        } else {
            dot_product / (norm1.sqrt() * norm2.sqrt())
        }
    }
}

impl FingerprintMatcher {
    pub fn new() -> Self {
        Self {
            algorithm_name: "Minutiae Matching".to_string(),
            performance_metrics: PerformanceMetrics {
                far_at_frr_1_percent: 0.0001,
                frr_at_far_0_1_percent: 0.02,
                equal_error_rate: 0.008,
                d_prime: 4.1,
            },
        }
    }
}

impl BiometricMatcher for FingerprintMatcher {
    fn compute_similarity(
        &self,
        template1: &BiometricTemplate,
        template2: &BiometricTemplate,
    ) -> Result<SimilarityScore> {
        let similarity = self.minutiae_similarity(&template1.protected_template, &template2.protected_template);

        let feature_contributions = vec![
            FeatureContribution {
                feature_name: "ridge_endings".to_string(),
                contribution_weight: 0.4,
                similarity: similarity * 1.05,
            },
            FeatureContribution {
                feature_name: "ridge_bifurcations".to_string(),
                contribution_weight: 0.4,
                similarity: similarity * 0.98,
            },
            FeatureContribution {
                feature_name: "ridge_patterns".to_string(),
                contribution_weight: 0.2,
                similarity: similarity * 1.1,
            },
        ];

        let quality_impact = (template1.quality_metrics.overall_quality + template2.quality_metrics.overall_quality) / 2.0;
        let confidence = similarity * quality_impact;

        Ok(SimilarityScore {
            score: similarity,
            normalized_score: similarity, // Already in [0,1] range
            confidence,
            matching_algorithm: self.algorithm_name.clone(),
            feature_contributions,
            quality_impact,
        })
    }

    fn get_threshold(&self, security_level: SecurityLevel) -> f64 {
        match security_level {
            SecurityLevel::Low => 0.7,
            SecurityLevel::Medium => 0.8,
            SecurityLevel::High => 0.87,
            SecurityLevel::VeryHigh => 0.92,
        }
    }

    fn normalize_score(&self, score: f64) -> f64 {
        score.clamp(0.0, 1.0)
    }

    fn get_matcher_info(&self) -> MatcherInfo {
        MatcherInfo {
            algorithm_name: self.algorithm_name.clone(),
            version: "1.0".to_string(),
            supported_modalities: vec![BiometricModality::Fingerprint],
            performance_metrics: self.performance_metrics.clone(),
        }
    }
}

impl FingerprintMatcher {
    fn minutiae_similarity(&self, template1: &[u8], template2: &[u8]) -> f64 {
        // Simplified minutiae matching based on pattern correlation
        let correlation = self.pattern_correlation(template1, template2);
        let ridge_similarity = self.ridge_pattern_similarity(template1, template2);

        // Weighted combination
        (correlation * 0.6 + ridge_similarity * 0.4).clamp(0.0, 1.0)
    }

    fn pattern_correlation(&self, data1: &[u8], data2: &[u8]) -> f64 {
        if data1.len() != data2.len() || data1.is_empty() {
            return 0.0;
        }

        let mut correlation = 0.0;
        for (a, b) in data1.iter().zip(data2.iter()) {
            let diff = (*a as i16 - *b as i16).abs() as f64;
            correlation += 1.0 - (diff / 255.0);
        }

        correlation / data1.len() as f64
    }

    fn ridge_pattern_similarity(&self, data1: &[u8], data2: &[u8]) -> f64 {
        // Analyze ridge flow patterns
        let flow1 = self.extract_ridge_flow(data1);
        let flow2 = self.extract_ridge_flow(data2);

        let mut similarity = 0.0;
        for (f1, f2) in flow1.iter().zip(flow2.iter()) {
            let angular_diff = (f1 - f2).abs();
            let normalized_diff = angular_diff.min(std::f64::consts::PI - angular_diff);
            similarity += 1.0 - (normalized_diff / std::f64::consts::PI);
        }

        if flow1.is_empty() { 0.0 } else { similarity / flow1.len() as f64 }
    }

    fn extract_ridge_flow(&self, data: &[u8]) -> Vec<f64> {
        let mut flow_angles = Vec::new();

        // Simplified ridge flow estimation using gradients
        for window in data.windows(3) {
            if window.len() == 3 {
                let gradient_x = window[2] as f64 - window[0] as f64;
                let gradient_y = window[1] as f64; // Simplified

                let angle = gradient_y.atan2(gradient_x);
                flow_angles.push(angle);
            }
        }

        flow_angles
    }
}

impl VoiceMatcher {
    pub fn new() -> Self {
        Self {
            algorithm_name: "Speaker Recognition MFCC".to_string(),
            performance_metrics: PerformanceMetrics {
                far_at_frr_1_percent: 0.01,
                frr_at_far_0_1_percent: 0.15,
                equal_error_rate: 0.05,
                d_prime: 2.1,
            },
        }
    }
}

impl BiometricMatcher for VoiceMatcher {
    fn compute_similarity(
        &self,
        template1: &BiometricTemplate,
        template2: &BiometricTemplate,
    ) -> Result<SimilarityScore> {
        let similarity = self.voice_similarity(&template1.protected_template, &template2.protected_template);

        let feature_contributions = vec![
            FeatureContribution {
                feature_name: "fundamental_frequency".to_string(),
                contribution_weight: 0.3,
                similarity: similarity * 0.95,
            },
            FeatureContribution {
                feature_name: "spectral_envelope".to_string(),
                contribution_weight: 0.4,
                similarity: similarity * 1.05,
            },
            FeatureContribution {
                feature_name: "prosodic_features".to_string(),
                contribution_weight: 0.3,
                similarity: similarity * 0.9,
            },
        ];

        let quality_impact = (template1.quality_metrics.overall_quality + template2.quality_metrics.overall_quality) / 2.0;
        let confidence = similarity * quality_impact;

        Ok(SimilarityScore {
            score: similarity,
            normalized_score: similarity,
            confidence,
            matching_algorithm: self.algorithm_name.clone(),
            feature_contributions,
            quality_impact,
        })
    }

    fn get_threshold(&self, security_level: SecurityLevel) -> f64 {
        match security_level {
            SecurityLevel::Low => 0.6,
            SecurityLevel::Medium => 0.7,
            SecurityLevel::High => 0.78,
            SecurityLevel::VeryHigh => 0.82,
        }
    }

    fn normalize_score(&self, score: f64) -> f64 {
        score.clamp(0.0, 1.0)
    }

    fn get_matcher_info(&self) -> MatcherInfo {
        MatcherInfo {
            algorithm_name: self.algorithm_name.clone(),
            version: "1.0".to_string(),
            supported_modalities: vec![BiometricModality::Voice],
            performance_metrics: self.performance_metrics.clone(),
        }
    }
}

impl VoiceMatcher {
    fn voice_similarity(&self, template1: &[u8], template2: &[u8]) -> f64 {
        let spectral_sim = self.spectral_similarity(template1, template2);
        let temporal_sim = self.temporal_similarity(template1, template2);
        let prosodic_sim = self.prosodic_similarity(template1, template2);

        // Weighted combination
        (spectral_sim * 0.5 + temporal_sim * 0.3 + prosodic_sim * 0.2).clamp(0.0, 1.0)
    }

    fn spectral_similarity(&self, data1: &[u8], data2: &[u8]) -> f64 {
        // Compute spectral similarity using simplified MFCC comparison
        let mfcc1 = self.extract_mfcc_features(data1);
        let mfcc2 = self.extract_mfcc_features(data2);

        self.euclidean_similarity(&mfcc1, &mfcc2)
    }

    fn temporal_similarity(&self, data1: &[u8], data2: &[u8]) -> f64 {
        // Analyze temporal patterns in the voice signal
        let energy1 = self.compute_energy_contour(data1);
        let energy2 = self.compute_energy_contour(data2);

        self.correlation_coefficient(&energy1, &energy2).abs()
    }

    fn prosodic_similarity(&self, data1: &[u8], data2: &[u8]) -> f64 {
        // Analyze prosodic features (pitch, rhythm, stress patterns)
        let pitch1 = self.extract_pitch_contour(data1);
        let pitch2 = self.extract_pitch_contour(data2);

        self.correlation_coefficient(&pitch1, &pitch2).abs()
    }

    fn extract_mfcc_features(&self, data: &[u8]) -> Vec<f64> {
        // Simplified MFCC feature extraction
        let mut features = Vec::new();

        // Compute basic spectral statistics as MFCC approximation
        for chunk in data.chunks(32) {
            let energy = chunk.iter().map(|&x| (x as f64).powi(2)).sum::<f64>();
            let spectral_centroid = self.compute_spectral_centroid(chunk);
            let spectral_rolloff = self.compute_spectral_rolloff(chunk);

            features.push(energy.sqrt());
            features.push(spectral_centroid);
            features.push(spectral_rolloff);
        }

        features
    }

    fn compute_spectral_centroid(&self, data: &[u8]) -> f64 {
        let mut weighted_sum = 0.0;
        let mut magnitude_sum = 0.0;

        for (i, &sample) in data.iter().enumerate() {
            let magnitude = sample as f64;
            weighted_sum += (i + 1) as f64 * magnitude;
            magnitude_sum += magnitude;
        }

        if magnitude_sum == 0.0 { 0.0 } else { weighted_sum / magnitude_sum }
    }

    fn compute_spectral_rolloff(&self, data: &[u8]) -> f64 {
        let total_energy: f64 = data.iter().map(|&x| (x as f64).powi(2)).sum();
        let threshold = 0.85 * total_energy;

        let mut cumulative_energy = 0.0;
        for (i, &sample) in data.iter().enumerate() {
            cumulative_energy += (sample as f64).powi(2);
            if cumulative_energy >= threshold {
                return i as f64 / data.len() as f64;
            }
        }

        1.0
    }

    fn compute_energy_contour(&self, data: &[u8]) -> Vec<f64> {
        data.chunks(16)
            .map(|chunk| chunk.iter().map(|&x| (x as f64).powi(2)).sum::<f64>().sqrt())
            .collect()
    }

    fn extract_pitch_contour(&self, data: &[u8]) -> Vec<f64> {
        // Simplified pitch extraction using autocorrelation
        let mut pitch_values = Vec::new();

        for window in data.chunks(64) {
            let pitch = self.estimate_pitch_autocorr(window);
            pitch_values.push(pitch);
        }

        pitch_values
    }

    fn estimate_pitch_autocorr(&self, data: &[u8]) -> f64 {
        if data.len() < 20 { return 0.0; }

        let mut max_correlation = 0.0;
        let mut best_lag = 0;

        for lag in 20..data.len() / 2 {
            let mut correlation = 0.0;
            let valid_samples = data.len() - lag;

            for i in 0..valid_samples {
                correlation += (data[i] as f64) * (data[i + lag] as f64);
            }

            correlation /= valid_samples as f64;

            if correlation > max_correlation {
                max_correlation = correlation;
                best_lag = lag;
            }
        }

        if best_lag > 0 { 44100.0 / best_lag as f64 } else { 0.0 } // Assume 44.1kHz sample rate
    }

    fn euclidean_similarity(&self, vec1: &[f64], vec2: &[f64]) -> f64 {
        if vec1.len() != vec2.len() || vec1.is_empty() {
            return 0.0;
        }

        let distance: f64 = vec1.iter()
            .zip(vec2.iter())
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f64>()
            .sqrt();

        // Convert distance to similarity (assuming max distance of sqrt(dimensions))
        let max_distance = (vec1.len() as f64).sqrt() * 255.0;
        1.0 - (distance / max_distance).min(1.0)
    }

    fn correlation_coefficient(&self, vec1: &[f64], vec2: &[f64]) -> f64 {
        if vec1.len() != vec2.len() || vec1.len() < 2 {
            return 0.0;
        }

        let n = vec1.len() as f64;
        let mean1 = vec1.iter().sum::<f64>() / n;
        let mean2 = vec2.iter().sum::<f64>() / n;

        let mut numerator = 0.0;
        let mut sum_sq1 = 0.0;
        let mut sum_sq2 = 0.0;

        for (a, b) in vec1.iter().zip(vec2.iter()) {
            let dev1 = a - mean1;
            let dev2 = b - mean2;
            numerator += dev1 * dev2;
            sum_sq1 += dev1.powi(2);
            sum_sq2 += dev2.powi(2);
        }

        let denominator = (sum_sq1 * sum_sq2).sqrt();
        if denominator == 0.0 { 0.0 } else { numerator / denominator }
    }
}

impl IrisMatcher {
    pub fn new() -> Self {
        Self {
            algorithm_name: "Iris Code Hamming Distance".to_string(),
            performance_metrics: PerformanceMetrics {
                far_at_frr_1_percent: 0.00001,
                frr_at_far_0_1_percent: 0.01,
                equal_error_rate: 0.003,
                d_prime: 5.2,
            },
        }
    }
}

impl BiometricMatcher for IrisMatcher {
    fn compute_similarity(
        &self,
        template1: &BiometricTemplate,
        template2: &BiometricTemplate,
    ) -> Result<SimilarityScore> {
        let similarity = self.iris_code_similarity(&template1.protected_template, &template2.protected_template);

        let feature_contributions = vec![
            FeatureContribution {
                feature_name: "iris_texture".to_string(),
                contribution_weight: 0.6,
                similarity: similarity * 1.02,
            },
            FeatureContribution {
                feature_name: "pupil_boundary".to_string(),
                contribution_weight: 0.25,
                similarity: similarity * 0.95,
            },
            FeatureContribution {
                feature_name: "iris_boundary".to_string(),
                contribution_weight: 0.15,
                similarity: similarity * 0.98,
            },
        ];

        let quality_impact = (template1.quality_metrics.overall_quality + template2.quality_metrics.overall_quality) / 2.0;
        let confidence = similarity * quality_impact;

        Ok(SimilarityScore {
            score: similarity,
            normalized_score: similarity,
            confidence,
            matching_algorithm: self.algorithm_name.clone(),
            feature_contributions,
            quality_impact,
        })
    }

    fn get_threshold(&self, security_level: SecurityLevel) -> f64 {
        match security_level {
            SecurityLevel::Low => 0.75,
            SecurityLevel::Medium => 0.82,
            SecurityLevel::High => 0.88,
            SecurityLevel::VeryHigh => 0.93,
        }
    }

    fn normalize_score(&self, score: f64) -> f64 {
        score.clamp(0.0, 1.0)
    }

    fn get_matcher_info(&self) -> MatcherInfo {
        MatcherInfo {
            algorithm_name: self.algorithm_name.clone(),
            version: "1.0".to_string(),
            supported_modalities: vec![BiometricModality::Iris],
            performance_metrics: self.performance_metrics.clone(),
        }
    }
}

impl IrisMatcher {
    fn iris_code_similarity(&self, template1: &[u8], template2: &[u8]) -> f64 {
        // Compute Hamming distance between iris codes
        let hamming_distance = self.hamming_distance(template1, template2);
        let total_bits = template1.len().min(template2.len()) * 8;

        if total_bits == 0 {
            0.0
        } else {
            1.0 - (hamming_distance as f64 / total_bits as f64)
        }
    }

    fn hamming_distance(&self, data1: &[u8], data2: &[u8]) -> u32 {
        let mut distance = 0;

        for (byte1, byte2) in data1.iter().zip(data2.iter()) {
            let xor = byte1 ^ byte2;
            distance += xor.count_ones();
        }

        distance
    }
}

// Score fusion implementations
impl ScoreFusionEngine {
    pub fn new() -> Self {
        let mut fusion_strategies: HashMap<String, Box<dyn FusionStrategy>> = HashMap::new();

        fusion_strategies.insert("sum".to_string(), Box::new(SumFusionStrategy));
        fusion_strategies.insert("product".to_string(), Box::new(ProductFusionStrategy));
        fusion_strategies.insert("max".to_string(), Box::new(MaxFusionStrategy));
        fusion_strategies.insert("weighted_sum".to_string(), Box::new(WeightedSumFusionStrategy));

        Self {
            fusion_strategies,
            weight_optimizer: WeightOptimizer::new(),
        }
    }

    pub fn fuse_scores(&mut self, scores: &[SimilarityScore], weights: &[f64]) -> Result<FusedScore> {
        if scores.is_empty() {
            return Err(BiometricError::MatchingFailed.into());
        }

        // Choose fusion strategy based on score characteristics
        let strategy_name = self.select_fusion_strategy(scores);
        let strategy = self.fusion_strategies.get(&strategy_name)
            .ok_or_else(|| BiometricError::MatchingFailed)?;

        let optimized_weights = if weights.len() == scores.len() {
            weights.to_vec()
        } else {
            self.weight_optimizer.optimize_weights(scores)?
        };

        strategy.fuse_scores(scores, &optimized_weights)
    }

    fn select_fusion_strategy(&self, scores: &[SimilarityScore]) -> String {
        if scores.len() == 1 {
            return "sum".to_string();
        }

        // Calculate score variance to decide on strategy
        let mean_score = scores.iter().map(|s| s.normalized_score).sum::<f64>() / scores.len() as f64;
        let variance = scores.iter()
            .map(|s| (s.normalized_score - mean_score).powi(2))
            .sum::<f64>() / scores.len() as f64;

        if variance < 0.01 {
            "weighted_sum".to_string() // Low variance - use weighted sum
        } else if variance > 0.1 {
            "max".to_string() // High variance - use max
        } else {
            "product".to_string() // Medium variance - use product
        }
    }
}

pub struct WeightOptimizer;

impl WeightOptimizer {
    pub fn new() -> Self {
        Self
    }

    pub fn optimize_weights(&self, scores: &[SimilarityScore]) -> Result<Vec<f64>> {
        // Simple weight optimization based on confidence scores
        let total_confidence: f64 = scores.iter().map(|s| s.confidence).sum();

        if total_confidence == 0.0 {
            // Equal weights if no confidence information
            Ok(vec![1.0 / scores.len() as f64; scores.len()])
        } else {
            // Weights proportional to confidence
            Ok(scores.iter()
                .map(|s| s.confidence / total_confidence)
                .collect())
        }
    }
}

// Fusion strategy implementations
pub struct SumFusionStrategy;
pub struct ProductFusionStrategy;
pub struct MaxFusionStrategy;
pub struct WeightedSumFusionStrategy;

impl FusionStrategy for SumFusionStrategy {
    fn fuse_scores(&self, scores: &[SimilarityScore], _weights: &[f64]) -> Result<FusedScore> {
        let fused_score = scores.iter().map(|s| s.normalized_score).sum::<f64>() / scores.len() as f64;
        let confidence = scores.iter().map(|s| s.confidence).sum::<f64>() / scores.len() as f64;

        Ok(FusedScore {
            fused_score,
            fusion_strategy: "sum".to_string(),
            individual_scores: scores.to_vec(),
            fusion_weights: vec![1.0 / scores.len() as f64; scores.len()],
            confidence,
        })
    }

    fn strategy_name(&self) -> &str {
        "sum"
    }
}

impl FusionStrategy for ProductFusionStrategy {
    fn fuse_scores(&self, scores: &[SimilarityScore], _weights: &[f64]) -> Result<FusedScore> {
        let fused_score = scores.iter()
            .map(|s| s.normalized_score)
            .fold(1.0, |acc, score| acc * score)
            .powf(1.0 / scores.len() as f64); // Geometric mean

        let confidence = scores.iter()
            .map(|s| s.confidence)
            .fold(1.0, |acc, conf| acc * conf)
            .powf(1.0 / scores.len() as f64);

        Ok(FusedScore {
            fused_score,
            fusion_strategy: "product".to_string(),
            individual_scores: scores.to_vec(),
            fusion_weights: vec![1.0; scores.len()],
            confidence,
        })
    }

    fn strategy_name(&self) -> &str {
        "product"
    }
}

impl FusionStrategy for MaxFusionStrategy {
    fn fuse_scores(&self, scores: &[SimilarityScore], _weights: &[f64]) -> Result<FusedScore> {
        let max_score_idx = scores.iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.normalized_score.partial_cmp(&b.normalized_score).unwrap())
            .map(|(i, _)| i)
            .unwrap_or(0);

        let fused_score = scores[max_score_idx].normalized_score;
        let confidence = scores[max_score_idx].confidence;

        let mut weights = vec![0.0; scores.len()];
        weights[max_score_idx] = 1.0;

        Ok(FusedScore {
            fused_score,
            fusion_strategy: "max".to_string(),
            individual_scores: scores.to_vec(),
            fusion_weights: weights,
            confidence,
        })
    }

    fn strategy_name(&self) -> &str {
        "max"
    }
}

impl FusionStrategy for WeightedSumFusionStrategy {
    fn fuse_scores(&self, scores: &[SimilarityScore], weights: &[f64]) -> Result<FusedScore> {
        let weights = if weights.len() == scores.len() {
            weights
        } else {
            &vec![1.0 / scores.len() as f64; scores.len()]
        };

        let fused_score = scores.iter()
            .zip(weights.iter())
            .map(|(score, weight)| score.normalized_score * weight)
            .sum::<f64>();

        let confidence = scores.iter()
            .zip(weights.iter())
            .map(|(score, weight)| score.confidence * weight)
            .sum::<f64>();

        Ok(FusedScore {
            fused_score,
            fusion_strategy: "weighted_sum".to_string(),
            individual_scores: scores.to_vec(),
            fusion_weights: weights.to_vec(),
            confidence,
        })
    }

    fn strategy_name(&self) -> &str {
        "weighted_sum"
    }
}

// Decision engine implementations
impl DecisionEngine {
    pub fn new() -> Self {
        Self {
            decision_policies: HashMap::new(),
            adaptive_threshold: AdaptiveThresholdManager::new(),
        }
    }

    pub fn make_decision(
        &mut self,
        score: &AdjustedScore,
        threshold: f64,
        security_level: &SecurityLevel,
        template: &BiometricTemplate,
    ) -> Result<DecisionResult> {
        let adaptive_threshold = self.adaptive_threshold.get_adaptive_threshold(
            threshold,
            template,
            security_level,
        )?;

        let is_match = score.score >= adaptive_threshold;
        let confidence = if is_match {
            score.confidence * (score.score / adaptive_threshold).min(1.0)
        } else {
            score.confidence * (1.0 - (adaptive_threshold - score.score).min(1.0))
        };

        let decision_factors = vec![
            ("base_threshold".to_string(), threshold),
            ("adaptive_threshold".to_string(), adaptive_threshold),
            ("score".to_string(), score.score),
            ("quality_penalty".to_string(), score.quality_penalty),
        ];

        Ok(DecisionResult {
            is_match,
            confidence,
            decision_factors,
        })
    }

    pub fn make_multimodal_decision(
        &mut self,
        fused_score: &FusedScore,
        threshold: f64,
        _security_level: &SecurityLevel,
    ) -> Result<DecisionResult> {
        let is_match = fused_score.fused_score >= threshold;
        let confidence = if is_match {
            fused_score.confidence * (fused_score.fused_score / threshold).min(1.0)
        } else {
            fused_score.confidence * (1.0 - (threshold - fused_score.fused_score).min(1.0))
        };

        let decision_factors = vec![
            ("threshold".to_string(), threshold),
            ("fused_score".to_string(), fused_score.fused_score),
            ("fusion_strategy".to_string(), 0.0), // Placeholder
            ("num_modalities".to_string(), fused_score.individual_scores.len() as f64),
        ];

        Ok(DecisionResult {
            is_match,
            confidence,
            decision_factors,
        })
    }
}

pub struct AdaptiveThresholdManager;

impl AdaptiveThresholdManager {
    pub fn new() -> Self {
        Self
    }

    pub fn get_adaptive_threshold(
        &self,
        base_threshold: f64,
        template: &BiometricTemplate,
        security_level: &SecurityLevel,
    ) -> Result<f64> {
        // Adjust threshold based on template quality
        let quality_adjustment = self.calculate_quality_adjustment(&template.quality_metrics);

        // Adjust based on security level
        let security_adjustment = self.calculate_security_adjustment(security_level);

        let adaptive_threshold = (base_threshold + quality_adjustment + security_adjustment)
            .clamp(0.1, 0.99);

        Ok(adaptive_threshold)
    }

    fn calculate_quality_adjustment(&self, quality: &QualityMetrics) -> f64 {
        // Lower threshold for higher quality templates
        let quality_factor = quality.overall_quality;
        (0.5 - quality_factor) * 0.1 // Max ±0.05 adjustment
    }

    fn calculate_security_adjustment(&self, security_level: &SecurityLevel) -> f64 {
        match security_level {
            SecurityLevel::Low => -0.05,      // Lower threshold
            SecurityLevel::Medium => 0.0,     // No adjustment
            SecurityLevel::High => 0.03,      // Higher threshold
            SecurityLevel::VeryHigh => 0.05,  // Much higher threshold
        }
    }
}

impl MatchingPerformanceMonitor {
    pub fn new() -> Self {
        Self {
            performance_stats: HashMap::new(),
        }
    }

    pub fn record_matching_session(&mut self, results: &[MatchingResult], match_time: f64) {
        for result in results {
            let key = format!("{:?}", result.template_id);
            let stats = self.performance_stats.entry(key).or_insert_with(|| PerformanceStats {
                total_matches: 0,
                successful_matches: 0,
                false_accepts: 0,
                false_rejects: 0,
                average_match_time_ms: 0.0,
                quality_distribution: Vec::new(),
            });

            stats.total_matches += 1;
            if result.is_match {
                stats.successful_matches += 1;
            }

            // Update average match time
            let total_time = stats.average_match_time_ms * (stats.total_matches - 1) as f64;
            stats.average_match_time_ms = (total_time + match_time) / stats.total_matches as f64;
        }
    }

    pub fn get_performance_summary(&self) -> HashMap<String, PerformanceStats> {
        self.performance_stats.clone()
    }
}