use super::*;
use anyhow::Result;
use rand_core::{OsRng, RngCore};
use sha2::{Sha256, Digest};
use std::collections::HashMap;

/// Comprehensive liveness detection system with anti-spoofing measures
pub struct LivenessDetector {
    challenge_generators: HashMap<BiometricModality, Box<dyn LivenessChallengeGenerator>>,
    spoof_detectors: HashMap<BiometricModality, Box<dyn SpoofDetector>>,
    replay_detector: ReplayDetector,
}

/// Trait for generating liveness challenges
pub trait LivenessChallengeGenerator: Send + Sync {
    /// Generate a new liveness challenge for the given modality
    fn generate_challenge(&self, challenge_id: &Ulid, nonce: &[u8; 32]) -> Result<LivenessChallenge>;

    /// Verify that a response correctly addresses the challenge
    fn verify_challenge_response(
        &self,
        challenge: &LivenessChallenge,
        response: &BiometricResponse,
    ) -> Result<LivenessVerificationResult>;
}

/// Trait for spoof detection
pub trait SpoofDetector: Send + Sync {
    /// Analyze biometric data for spoofing indicators
    fn detect_spoof(&self, data: &[u8], metadata: &[(String, String)]) -> Result<SpoofAnalysis>;

    /// Get confidence threshold for spoof detection
    fn confidence_threshold(&self) -> f64;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivenessChallenge {
    pub challenge_type: LivenessType,
    pub challenge_data: Vec<u8>,
    pub expected_response_pattern: Vec<u8>,
    pub timing_requirements: TimingRequirements,
    pub spatial_requirements: Option<SpatialRequirements>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimingRequirements {
    pub min_duration_ms: u64,
    pub max_duration_ms: u64,
    pub sequence_timing: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpatialRequirements {
    pub required_positions: Vec<Position3D>,
    pub tolerance: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position3D {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivenessVerificationResult {
    pub is_live: bool,
    pub confidence_score: f64,
    pub challenge_completion_rate: f64,
    pub timing_accuracy: f64,
    pub spatial_accuracy: Option<f64>,
    pub anti_spoof_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpoofAnalysis {
    pub spoof_probability: f64,
    pub spoof_indicators: Vec<SpoofIndicator>,
    pub texture_analysis: TextureAnalysis,
    pub motion_analysis: Option<MotionAnalysis>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpoofIndicator {
    pub indicator_type: String,
    pub confidence: f64,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextureAnalysis {
    pub texture_score: f64,
    pub surface_properties: Vec<(String, f64)>,
    pub micro_texture_patterns: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MotionAnalysis {
    pub motion_consistency: f64,
    pub natural_movement_score: f64,
    pub involuntary_movements: Vec<f64>,
}

/// Face liveness challenge generator
pub struct FaceLivenessChallengeGenerator;

/// Voice liveness challenge generator
pub struct VoiceLivenessChallengeGenerator;

/// Fingerprint liveness challenge generator
pub struct FingerprintLivenessChallengeGenerator;

/// Iris liveness challenge generator
pub struct IrisLivenessChallengeGenerator;

impl LivenessDetector {
    pub fn new() -> Self {
        let mut challenge_generators: HashMap<BiometricModality, Box<dyn LivenessChallengeGenerator>> = HashMap::new();
        let mut spoof_detectors: HashMap<BiometricModality, Box<dyn SpoofDetector>> = HashMap::new();

        // Register challenge generators
        challenge_generators.insert(BiometricModality::Face, Box::new(FaceLivenessChallengeGenerator));
        challenge_generators.insert(BiometricModality::Voice, Box::new(VoiceLivenessChallengeGenerator));
        challenge_generators.insert(BiometricModality::Fingerprint, Box::new(FingerprintLivenessChallengeGenerator));
        challenge_generators.insert(BiometricModality::Iris, Box::new(IrisLivenessChallengeGenerator));

        // Register spoof detectors
        spoof_detectors.insert(BiometricModality::Face, Box::new(FaceSpoofDetector));
        spoof_detectors.insert(BiometricModality::Voice, Box::new(VoiceSpoofDetector));
        spoof_detectors.insert(BiometricModality::Fingerprint, Box::new(FingerprintSpoofDetector));
        spoof_detectors.insert(BiometricModality::Iris, Box::new(IrisSpoofDetector));

        Self {
            challenge_generators,
            spoof_detectors,
            replay_detector: ReplayDetector::new(),
        }
    }

    /// Create a comprehensive liveness challenge for a biometric modality
    pub fn create_challenge(&self, modality: &BiometricModality, timeout_secs: u64) -> Result<BiometricChallenge> {
        let mut challenge = BiometricChallenge::new(modality.clone(), timeout_secs)?;

        if let Some(generator) = self.challenge_generators.get(modality) {
            let liveness_challenge = generator.generate_challenge(&challenge.challenge_id, &challenge.nonce)?;

            // Embed liveness challenge into the biometric challenge
            let mut enhanced_data = challenge.challenge_data.clone();
            enhanced_data.extend_from_slice(&bincode::serialize(&liveness_challenge)?);
            challenge.challenge_data = enhanced_data;
        }

        Ok(challenge)
    }

    /// Verify liveness from a biometric response
    pub fn verify_liveness(
        &self,
        challenge: &BiometricChallenge,
        response: &BiometricResponse,
    ) -> Result<LivenessProof> {
        // Check for replay attacks
        self.replay_detector.check_replay(response)?;

        let modality = &challenge.modality;

        // Extract liveness challenge from challenge data
        let liveness_challenge = if challenge.challenge_data.len() > 32 {
            bincode::deserialize::<LivenessChallenge>(&challenge.challenge_data[32..])?
        } else {
            return Err(BiometricError::LivenessFailure("Invalid challenge format".to_string()).into());
        };

        // Verify liveness challenge response
        let verification_result = if let Some(generator) = self.challenge_generators.get(modality) {
            generator.verify_challenge_response(&liveness_challenge, response)?
        } else {
            return Err(BiometricError::UnsupportedModality(format!("{:?}", modality)).into());
        };

        // Perform spoof detection
        let spoof_analysis = if let Some(detector) = self.spoof_detectors.get(modality) {
            detector.detect_spoof(&response.biometric_data, &response.liveness_proof.metadata)?
        } else {
            SpoofAnalysis {
                spoof_probability: 0.0,
                spoof_indicators: Vec::new(),
                texture_analysis: TextureAnalysis {
                    texture_score: 1.0,
                    surface_properties: Vec::new(),
                    micro_texture_patterns: Vec::new(),
                },
                motion_analysis: None,
            }
        };

        // Calculate overall confidence score
        let confidence_score = self.calculate_confidence_score(&verification_result, &spoof_analysis)?;

        // Check if liveness requirements are met
        if !verification_result.is_live || spoof_analysis.spoof_probability > 0.3 || confidence_score < 0.7 {
            return Err(BiometricError::LivenessFailure(
                "Liveness detection failed - possible spoof attempt".to_string()
            ).into());
        }

        Ok(LivenessProof {
            proof_type: liveness_challenge.challenge_type,
            proof_data: bincode::serialize(&verification_result)?,
            confidence_score,
            metadata: vec![
                ("spoof_probability".to_string(), spoof_analysis.spoof_probability.to_string()),
                ("challenge_completion".to_string(), verification_result.challenge_completion_rate.to_string()),
                ("timing_accuracy".to_string(), verification_result.timing_accuracy.to_string()),
            ],
        })
    }

    fn calculate_confidence_score(
        &self,
        verification: &LivenessVerificationResult,
        spoof_analysis: &SpoofAnalysis,
    ) -> Result<f64> {
        let liveness_score = if verification.is_live { verification.confidence_score } else { 0.0 };
        let anti_spoof_score = 1.0 - spoof_analysis.spoof_probability;
        let challenge_score = verification.challenge_completion_rate;
        let timing_score = verification.timing_accuracy;

        // Weighted average of different components
        let confidence = (liveness_score * 0.3) +
                        (anti_spoof_score * 0.3) +
                        (challenge_score * 0.2) +
                        (timing_score * 0.2);

        Ok(confidence.min(1.0).max(0.0))
    }
}

impl LivenessChallengeGenerator for FaceLivenessChallengeGenerator {
    fn generate_challenge(&self, _challenge_id: &Ulid, nonce: &[u8; 32]) -> Result<LivenessChallenge> {
        let mut rng = OsRng;

        // Generate random head movement sequence
        let movements = vec![
            ("turn_left", 15.0),
            ("turn_right", 15.0),
            ("nod_up", 10.0),
            ("nod_down", 10.0),
            ("smile", 2.0),
        ];

        let mut challenge_sequence = Vec::new();
        let mut timing_sequence = Vec::new();

        // Select 2-3 random movements
        let num_movements = 2 + (rng.next_u32() % 2) as usize;
        for _i in 0..num_movements {
            let movement_idx = rng.next_u32() as usize % movements.len();
            let (movement, angle) = &movements[movement_idx];

            // Encode movement as bytes
            challenge_sequence.extend_from_slice(movement.as_bytes());
            challenge_sequence.push(0); // separator
            challenge_sequence.extend_from_slice(&(*angle as f32).to_le_bytes());

            // Add timing requirement (1-3 seconds per movement)
            timing_sequence.push(1000 + (rng.next_u32() % 2000) as u64);
        }

        // Generate spatial requirements
        let spatial_requirements = Some(SpatialRequirements {
            required_positions: vec![
                Position3D { x: 0.0, y: 0.0, z: 0.0 }, // center
                Position3D { x: -15.0, y: 0.0, z: 0.0 }, // left
                Position3D { x: 15.0, y: 0.0, z: 0.0 }, // right
            ],
            tolerance: 5.0, // degrees
        });

        Ok(LivenessChallenge {
            challenge_type: LivenessType::ActiveChallenge,
            challenge_data: challenge_sequence,
            expected_response_pattern: nonce.to_vec(), // Use nonce as expected pattern
            timing_requirements: TimingRequirements {
                min_duration_ms: timing_sequence.iter().sum::<u64>(),
                max_duration_ms: timing_sequence.iter().sum::<u64>() + 2000,
                sequence_timing: timing_sequence,
            },
            spatial_requirements,
        })
    }

    fn verify_challenge_response(
        &self,
        challenge: &LivenessChallenge,
        response: &BiometricResponse,
    ) -> Result<LivenessVerificationResult> {
        // Verify timing
        let expected_duration = challenge.timing_requirements.min_duration_ms;
        let max_duration = challenge.timing_requirements.max_duration_ms;

        // Simulate timing analysis (in real implementation, would parse response metadata)
        let actual_duration = response.timestamp.saturating_sub(response.timestamp - 3000); // Simulate
        let timing_accuracy = if actual_duration >= expected_duration && actual_duration <= max_duration {
            1.0
        } else {
            let diff = if actual_duration < expected_duration {
                expected_duration - actual_duration
            } else {
                actual_duration - max_duration
            };
            1.0 - (diff as f64 / expected_duration as f64).min(1.0)
        };

        // Verify spatial requirements (simplified)
        let spatial_accuracy = if let Some(_requirements) = &challenge.spatial_requirements {
            // In real implementation, would analyze head pose from biometric data
            0.9_f64 // Simulated accuracy
        } else {
            0.5_f64 // Default accuracy when no requirements
        };

        // Analyze challenge completion
        let challenge_completion_rate = self.analyze_challenge_completion(&challenge.challenge_data, &response.biometric_data)?;

        let is_live = timing_accuracy > 0.7 && challenge_completion_rate > 0.8;
        let confidence_score = (timing_accuracy + challenge_completion_rate) / 2.0;

        Ok(LivenessVerificationResult {
            is_live,
            confidence_score,
            challenge_completion_rate,
            timing_accuracy,
            spatial_accuracy: Some(spatial_accuracy),
            anti_spoof_score: 0.9, // Would be calculated from texture analysis
        })
    }
}

impl FaceLivenessChallengeGenerator {
    fn analyze_challenge_completion(&self, _challenge_data: &[u8], response_data: &[u8]) -> Result<f64> {
        // Simplified analysis - in reality would use computer vision
        // to analyze facial movements against challenge requirements

        if response_data.len() < 100 {
            return Ok(0.0);
        }

        // Simulate movement detection based on data variance
        let mut movement_score = 0.0;

        // Analyze data segments for movement patterns
        for chunk in response_data.chunks(response_data.len() / 4) {
            let variance = self.calculate_variance(chunk);
            movement_score += variance / 255.0;
        }

        Ok((movement_score / 4.0).min(1.0))
    }

    fn calculate_variance(&self, data: &[u8]) -> f64 {
        if data.is_empty() { return 0.0; }

        let mean = data.iter().map(|&x| x as f64).sum::<f64>() / data.len() as f64;
        let variance = data.iter()
            .map(|&x| (x as f64 - mean).powi(2))
            .sum::<f64>() / data.len() as f64;

        variance.sqrt()
    }
}

impl LivenessChallengeGenerator for VoiceLivenessChallengeGenerator {
    fn generate_challenge(&self, _challenge_id: &Ulid, nonce: &[u8; 32]) -> Result<LivenessChallenge> {
        let phrases = [
            b"Please say the following numbers in order",
            b"Repeat this phrase with proper emphasis x",
            b"Count from one to ten with natural pauses",
        ];

        let mut rng = OsRng;
        let phrase_idx = rng.next_u32() as usize % phrases.len();
        let mut challenge_data = phrases[phrase_idx].to_vec();

        // Add random numbers to speak
        for _ in 0..3 {
            let number = rng.next_u32() % 100;
            challenge_data.extend_from_slice(&number.to_le_bytes());
        }

        Ok(LivenessChallenge {
            challenge_type: LivenessType::ActiveChallenge,
            challenge_data,
            expected_response_pattern: nonce.to_vec(),
            timing_requirements: TimingRequirements {
                min_duration_ms: 3000,
                max_duration_ms: 10000,
                sequence_timing: vec![1000, 1000, 1000], // Per word timing
            },
            spatial_requirements: None,
        })
    }

    fn verify_challenge_response(
        &self,
        challenge: &LivenessChallenge,
        response: &BiometricResponse,
    ) -> Result<LivenessVerificationResult> {
        // Simplified voice verification
        let timing_accuracy = self.verify_speech_timing(challenge, response)?;
        let challenge_completion_rate = self.verify_phrase_completion(challenge, response)?;

        let is_live = timing_accuracy > 0.6 && challenge_completion_rate > 0.7;
        let confidence_score = (timing_accuracy + challenge_completion_rate) / 2.0;

        Ok(LivenessVerificationResult {
            is_live,
            confidence_score,
            challenge_completion_rate,
            timing_accuracy,
            spatial_accuracy: None,
            anti_spoof_score: 0.85,
        })
    }
}

impl VoiceLivenessChallengeGenerator {
    fn verify_speech_timing(&self, challenge: &LivenessChallenge, _response: &BiometricResponse) -> Result<f64> {
        // Simplified timing verification
        let expected_min = challenge.timing_requirements.min_duration_ms as f64;
        let expected_max = challenge.timing_requirements.max_duration_ms as f64;

        // Simulate actual duration analysis
        let simulated_duration = 4000.0;

        if simulated_duration >= expected_min && simulated_duration <= expected_max {
            Ok(1.0)
        } else {
            let diff = if simulated_duration < expected_min {
                expected_min - simulated_duration
            } else {
                simulated_duration - expected_max
            };
            Ok(1.0 - (diff / expected_min).min(1.0))
        }
    }

    fn verify_phrase_completion(&self, _challenge: &LivenessChallenge, response: &BiometricResponse) -> Result<f64> {
        // Simplified phrase verification based on audio characteristics
        if response.biometric_data.len() < 1000 {
            return Ok(0.0);
        }

        // Analyze audio energy patterns
        let energy_score = self.calculate_audio_energy(&response.biometric_data);
        Ok(energy_score)
    }

    fn calculate_audio_energy(&self, data: &[u8]) -> f64 {
        let rms = (data.iter()
            .map(|&x| (x as f64 - 128.0).powi(2))
            .sum::<f64>() / data.len() as f64)
            .sqrt();

        (rms / 128.0).min(1.0)
    }
}

impl LivenessChallengeGenerator for FingerprintLivenessChallengeGenerator {
    fn generate_challenge(&self, _challenge_id: &Ulid, nonce: &[u8; 32]) -> Result<LivenessChallenge> {
        let mut challenge_data = Vec::new();

        // Require multiple finger placements with different pressures
        let pressures = [0.3, 0.7, 0.5]; // Light, heavy, medium pressure
        for &pressure in &pressures {
            challenge_data.extend_from_slice(&(pressure as f32).to_le_bytes());
        }

        Ok(LivenessChallenge {
            challenge_type: LivenessType::ActiveChallenge,
            challenge_data,
            expected_response_pattern: nonce.to_vec(),
            timing_requirements: TimingRequirements {
                min_duration_ms: 2000,
                max_duration_ms: 8000,
                sequence_timing: vec![1000, 1000, 1000], // Per placement
            },
            spatial_requirements: None,
        })
    }

    fn verify_challenge_response(
        &self,
        challenge: &LivenessChallenge,
        response: &BiometricResponse,
    ) -> Result<LivenessVerificationResult> {
        let timing_accuracy = 0.9; // Simplified
        let challenge_completion_rate = self.verify_pressure_sequence(challenge, response)?;

        let is_live = timing_accuracy > 0.7 && challenge_completion_rate > 0.8;
        let confidence_score = (timing_accuracy + challenge_completion_rate) / 2.0;

        Ok(LivenessVerificationResult {
            is_live,
            confidence_score,
            challenge_completion_rate,
            timing_accuracy,
            spatial_accuracy: None,
            anti_spoof_score: 0.92,
        })
    }
}

impl FingerprintLivenessChallengeGenerator {
    fn verify_pressure_sequence(&self, _challenge: &LivenessChallenge, response: &BiometricResponse) -> Result<f64> {
        // Analyze pressure variations in fingerprint data
        let pressure_variance = self.calculate_pressure_variance(&response.biometric_data);
        Ok(pressure_variance)
    }

    fn calculate_pressure_variance(&self, data: &[u8]) -> f64 {
        if data.len() < 9 { return 0.0; }

        let segments = data.chunks(data.len() / 3);
        let segment_means: Vec<f64> = segments
            .map(|segment| segment.iter().map(|&x| x as f64).sum::<f64>() / segment.len() as f64)
            .collect();

        if segment_means.len() < 3 { return 0.0; }

        let overall_mean = segment_means.iter().sum::<f64>() / segment_means.len() as f64;
        let variance = segment_means.iter()
            .map(|&mean| (mean - overall_mean).powi(2))
            .sum::<f64>() / segment_means.len() as f64;

        (variance.sqrt() / 255.0).min(1.0)
    }
}

impl LivenessChallengeGenerator for IrisLivenessChallengeGenerator {
    fn generate_challenge(&self, _challenge_id: &Ulid, nonce: &[u8; 32]) -> Result<LivenessChallenge> {
        // Generate pupil dilation challenge by varying light conditions
        let mut challenge_data = Vec::new();
        challenge_data.extend_from_slice(b"pupil_response");
        challenge_data.extend_from_slice(&[255u8, 128u8, 64u8]); // Light intensity sequence

        Ok(LivenessChallenge {
            challenge_type: LivenessType::HardwareBacked,
            challenge_data,
            expected_response_pattern: nonce.to_vec(),
            timing_requirements: TimingRequirements {
                min_duration_ms: 3000,
                max_duration_ms: 6000,
                sequence_timing: vec![1000, 1000, 1000],
            },
            spatial_requirements: None,
        })
    }

    fn verify_challenge_response(
        &self,
        _challenge: &LivenessChallenge,
        response: &BiometricResponse,
    ) -> Result<LivenessVerificationResult> {
        let pupil_response_score = self.analyze_pupil_response(&response.biometric_data)?;

        let is_live = pupil_response_score > 0.8;
        let confidence_score = pupil_response_score;

        Ok(LivenessVerificationResult {
            is_live,
            confidence_score,
            challenge_completion_rate: pupil_response_score,
            timing_accuracy: 0.95,
            spatial_accuracy: None,
            anti_spoof_score: 0.94,
        })
    }
}

impl IrisLivenessChallengeGenerator {
    fn analyze_pupil_response(&self, data: &[u8]) -> Result<f64> {
        if data.len() < 300 { return Ok(0.0); }

        // Analyze three segments representing different light conditions
        let segment_size = data.len() / 3;
        let segments = [
            &data[0..segment_size],
            &data[segment_size..2*segment_size],
            &data[2*segment_size..],
        ];

        // Calculate average intensity for each segment (simulated pupil size)
        let intensities: Vec<f64> = segments.iter()
            .map(|segment| segment.iter().map(|&x| x as f64).sum::<f64>() / segment.len() as f64)
            .collect();

        // Good pupil response should show variation between segments
        let max_intensity = intensities.iter().fold(0.0f64, |a, &b| a.max(b));
        let min_intensity = intensities.iter().fold(255.0f64, |a, &b| a.min(b));
        let response_range = max_intensity - min_intensity;

        Ok((response_range / 255.0).min(1.0))
    }
}

// Spoof Detectors
pub struct FaceSpoofDetector;
pub struct VoiceSpoofDetector;
pub struct FingerprintSpoofDetector;
pub struct IrisSpoofDetector;

impl SpoofDetector for FaceSpoofDetector {
    fn detect_spoof(&self, data: &[u8], _metadata: &[(String, String)]) -> Result<SpoofAnalysis> {
        let texture_analysis = self.analyze_face_texture(data)?;
        let motion_analysis = self.analyze_face_motion(data)?;

        let spoof_probability = 1.0 - texture_analysis.texture_score.min(motion_analysis.motion_consistency);

        Ok(SpoofAnalysis {
            spoof_probability,
            spoof_indicators: self.generate_face_spoof_indicators(&texture_analysis, &motion_analysis),
            texture_analysis,
            motion_analysis: Some(motion_analysis),
        })
    }

    fn confidence_threshold(&self) -> f64 {
        0.7
    }
}

impl FaceSpoofDetector {
    fn analyze_face_texture(&self, data: &[u8]) -> Result<TextureAnalysis> {
        // Simplified texture analysis for face spoofing detection
        let texture_score = self.calculate_texture_complexity(data);

        let surface_properties = vec![
            ("skin_reflection".to_string(), self.analyze_skin_reflection(data)),
            ("micro_movements".to_string(), self.detect_micro_movements(data)),
        ];

        Ok(TextureAnalysis {
            texture_score,
            surface_properties,
            micro_texture_patterns: self.extract_texture_patterns(data),
        })
    }

    fn analyze_face_motion(&self, data: &[u8]) -> Result<MotionAnalysis> {
        Ok(MotionAnalysis {
            motion_consistency: self.calculate_motion_consistency(data),
            natural_movement_score: self.assess_natural_movement(data),
            involuntary_movements: self.detect_involuntary_movements(data),
        })
    }

    fn calculate_texture_complexity(&self, data: &[u8]) -> f64 {
        // Calculate local binary patterns or similar texture measure
        let mut complexity_score = 0.0;

        for window in data.windows(9) {
            let center = window[4];
            let mut pattern = 0u8;

            for (i, &pixel) in window.iter().enumerate() {
                if i != 4 && pixel >= center {
                    pattern |= 1 << i;
                }
            }

            // Count unique patterns (higher = more complex texture)
            complexity_score += (pattern.count_ones() as f64) / 8.0;
        }

        if data.len() < 9 { 0.0 } else { complexity_score / (data.len() - 8) as f64 }
    }

    fn analyze_skin_reflection(&self, data: &[u8]) -> f64 {
        // Analyze reflection patterns that might indicate screen/photo
        let mean = data.iter().map(|&x| x as f64).sum::<f64>() / data.len() as f64;
        let high_intensity_ratio = data.iter().filter(|&&x| x as f64 > mean * 1.2).count() as f64 / data.len() as f64;

        // Natural skin should have moderate reflection variation
        1.0 - (high_intensity_ratio - 0.1).abs().min(1.0)
    }

    fn detect_micro_movements(&self, data: &[u8]) -> f64 {
        // Detect subtle movements that indicate life
        let mut movement_score = 0.0;

        for window in data.windows(3) {
            let variation = (window[2] as i16 - window[0] as i16).abs() as f64;
            movement_score += variation / 255.0;
        }

        if data.len() < 3 { 0.0 } else { (movement_score / (data.len() - 2) as f64).min(1.0) }
    }

    fn extract_texture_patterns(&self, data: &[u8]) -> Vec<f64> {
        // Extract key texture pattern indicators
        let mut patterns = Vec::new();

        // Edge density
        let mut edge_count = 0;
        for window in data.windows(2) {
            if (window[1] as i16 - window[0] as i16).abs() > 10 {
                edge_count += 1;
            }
        }
        patterns.push(edge_count as f64 / (data.len() - 1) as f64);

        // Local variance
        for chunk in data.chunks(16) {
            let mean = chunk.iter().map(|&x| x as f64).sum::<f64>() / chunk.len() as f64;
            let variance = chunk.iter()
                .map(|&x| (x as f64 - mean).powi(2))
                .sum::<f64>() / chunk.len() as f64;
            patterns.push((variance.sqrt() / 255.0).min(1.0));
        }

        patterns
    }

    fn calculate_motion_consistency(&self, data: &[u8]) -> f64 {
        // Analyze consistency of motion patterns
        if data.len() < 6 { return 0.0; }

        let mut motion_vectors = Vec::new();
        for window in data.windows(3) {
            let motion = (window[2] as i16 - window[0] as i16) as f64;
            motion_vectors.push(motion);
        }

        let mean_motion = motion_vectors.iter().sum::<f64>() / motion_vectors.len() as f64;
        let motion_variance = motion_vectors.iter()
            .map(|&motion| (motion - mean_motion).powi(2))
            .sum::<f64>() / motion_vectors.len() as f64;

        // Consistent motion should have moderate variance
        let normalized_variance = (motion_variance.sqrt() / 255.0).min(1.0);
        1.0 - (normalized_variance - 0.1).abs().min(1.0)
    }

    fn assess_natural_movement(&self, data: &[u8]) -> f64 {
        // Natural movement should have some randomness but not be completely chaotic
        let entropy = self.calculate_entropy(data);

        // Ideal entropy is around 0.6-0.8 for natural movement
        if entropy >= 0.6 && entropy <= 0.8 {
            1.0
        } else {
            1.0 - ((entropy - 0.7).abs() * 5.0).min(1.0)
        }
    }

    fn detect_involuntary_movements(&self, data: &[u8]) -> Vec<f64> {
        // Detect small involuntary movements that indicate life
        let mut movements = Vec::new();

        // Detect micro-tremors
        let tremor_score = self.detect_tremor_patterns(data);
        movements.push(tremor_score);

        // Detect blink patterns (simplified)
        let blink_score = self.detect_blink_patterns(data);
        movements.push(blink_score);

        movements
    }

    fn calculate_entropy(&self, data: &[u8]) -> f64 {
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

    fn detect_tremor_patterns(&self, data: &[u8]) -> f64 {
        // Detect high-frequency small movements
        let mut tremor_score = 0.0;

        for window in data.windows(5) {
            let mut changes = 0;
            for i in 1..window.len() {
                if (window[i] as i16 - window[i-1] as i16).abs() <= 3 {
                    changes += 1;
                }
            }
            tremor_score += changes as f64 / 4.0;
        }

        if data.len() < 5 { 0.0 } else { (tremor_score / (data.len() - 4) as f64).min(1.0) }
    }

    fn detect_blink_patterns(&self, data: &[u8]) -> f64 {
        // Detect blink-like patterns (rapid intensity changes)
        let mut blink_score = 0.0;

        for window in data.windows(10) {
            let min_val = *window.iter().min().unwrap();
            let max_val = *window.iter().max().unwrap();

            if max_val - min_val > 50 {
                blink_score += 1.0;
            }
        }

        if data.len() < 10 { 0.0 } else { (blink_score / (data.len() - 9) as f64).min(1.0) }
    }

    fn generate_face_spoof_indicators(&self, texture: &TextureAnalysis, motion: &MotionAnalysis) -> Vec<SpoofIndicator> {
        let mut indicators = Vec::new();

        if texture.texture_score < 0.3 {
            indicators.push(SpoofIndicator {
                indicator_type: "low_texture_complexity".to_string(),
                confidence: 1.0 - texture.texture_score,
                description: "Unusually low texture complexity suggesting printed photo".to_string(),
            });
        }

        if motion.natural_movement_score < 0.4 {
            indicators.push(SpoofIndicator {
                indicator_type: "unnatural_movement".to_string(),
                confidence: 1.0 - motion.natural_movement_score,
                description: "Movement patterns inconsistent with live face".to_string(),
            });
        }

        if motion.involuntary_movements.iter().all(|&score| score < 0.2) {
            indicators.push(SpoofIndicator {
                indicator_type: "missing_involuntary_movement".to_string(),
                confidence: 0.8,
                description: "Lack of natural involuntary movements like micro-tremors".to_string(),
            });
        }

        indicators
    }
}

// Simplified implementations for other modalities
impl SpoofDetector for VoiceSpoofDetector {
    fn detect_spoof(&self, data: &[u8], _metadata: &[(String, String)]) -> Result<SpoofAnalysis> {
        let texture_analysis = TextureAnalysis {
            texture_score: self.analyze_voice_naturalness(data),
            surface_properties: vec![
                ("frequency_analysis".to_string(), self.analyze_frequency_spectrum(data)),
            ],
            micro_texture_patterns: Vec::new(),
        };

        Ok(SpoofAnalysis {
            spoof_probability: 1.0 - texture_analysis.texture_score,
            spoof_indicators: Vec::new(),
            texture_analysis,
            motion_analysis: None,
        })
    }

    fn confidence_threshold(&self) -> f64 { 0.75 }
}

impl VoiceSpoofDetector {
    fn analyze_voice_naturalness(&self, data: &[u8]) -> f64 {
        // Simplified voice naturalness analysis
        let entropy = self.calculate_audio_entropy(data);
        entropy.min(1.0)
    }

    fn analyze_frequency_spectrum(&self, data: &[u8]) -> f64 {
        // Simplified frequency analysis
        let mut high_freq_energy = 0.0;
        let mut total_energy = 0.0;

        for &sample in data {
            let energy = (sample as f64 - 128.0).powi(2);
            total_energy += energy;
            if sample > 150 || sample < 100 {
                high_freq_energy += energy;
            }
        }

        if total_energy == 0.0 { 0.0 } else { high_freq_energy / total_energy }
    }

    fn calculate_audio_entropy(&self, data: &[u8]) -> f64 {
        let mut counts = [0u32; 256];
        for &sample in data {
            counts[sample as usize] += 1;
        }

        let mut entropy = 0.0;
        let total = data.len() as f64;

        for &count in &counts {
            if count > 0 {
                let p = count as f64 / total;
                entropy -= p * p.log2();
            }
        }

        entropy / 8.0
    }
}

impl SpoofDetector for FingerprintSpoofDetector {
    fn detect_spoof(&self, data: &[u8], _metadata: &[(String, String)]) -> Result<SpoofAnalysis> {
        let texture_analysis = TextureAnalysis {
            texture_score: self.analyze_ridge_quality(data),
            surface_properties: vec![
                ("capacitance_variation".to_string(), self.analyze_capacitance_patterns(data)),
            ],
            micro_texture_patterns: Vec::new(),
        };

        Ok(SpoofAnalysis {
            spoof_probability: 1.0 - texture_analysis.texture_score,
            spoof_indicators: Vec::new(),
            texture_analysis,
            motion_analysis: None,
        })
    }

    fn confidence_threshold(&self) -> f64 { 0.8 }
}

impl FingerprintSpoofDetector {
    fn analyze_ridge_quality(&self, data: &[u8]) -> f64 {
        // Analyze fingerprint ridge patterns for authenticity
        let mut ridge_score = 0.0;

        // Look for periodic patterns characteristic of fingerprints
        for window in data.windows(8) {
            let mut pattern_score = 0.0;
            for i in 1..window.len() {
                let diff = (window[i] as i16 - window[i-1] as i16).abs();
                if diff > 5 && diff < 30 { // Typical ridge transition
                    pattern_score += 1.0;
                }
            }
            ridge_score += pattern_score / 7.0;
        }

        if data.len() < 8 { 0.0 } else { (ridge_score / (data.len() - 7) as f64).min(1.0) }
    }

    fn analyze_capacitance_patterns(&self, data: &[u8]) -> f64 {
        // Analyze patterns that would indicate live finger vs. artificial
        let variance = self.calculate_local_variance(data);
        variance
    }

    fn calculate_local_variance(&self, data: &[u8]) -> f64 {
        if data.len() < 16 { return 0.0; }

        let mut variance_sum = 0.0;
        let chunks = data.chunks(16);
        let chunk_count = chunks.len();

        for chunk in chunks {
            let mean = chunk.iter().map(|&x| x as f64).sum::<f64>() / chunk.len() as f64;
            let variance = chunk.iter()
                .map(|&x| (x as f64 - mean).powi(2))
                .sum::<f64>() / chunk.len() as f64;
            variance_sum += variance;
        }

        ((variance_sum / chunk_count as f64).sqrt() / 255.0).min(1.0)
    }
}

impl SpoofDetector for IrisSpoofDetector {
    fn detect_spoof(&self, data: &[u8], _metadata: &[(String, String)]) -> Result<SpoofAnalysis> {
        let texture_analysis = TextureAnalysis {
            texture_score: self.analyze_iris_texture(data),
            surface_properties: vec![
                ("pupil_response".to_string(), self.analyze_pupil_dynamics(data)),
            ],
            micro_texture_patterns: Vec::new(),
        };

        Ok(SpoofAnalysis {
            spoof_probability: 1.0 - texture_analysis.texture_score,
            spoof_indicators: Vec::new(),
            texture_analysis,
            motion_analysis: None,
        })
    }

    fn confidence_threshold(&self) -> f64 { 0.85 }
}

impl IrisSpoofDetector {
    fn analyze_iris_texture(&self, data: &[u8]) -> f64 {
        // Analyze iris texture patterns for authenticity
        let complexity = self.calculate_iris_complexity(data);
        complexity
    }

    fn analyze_pupil_dynamics(&self, data: &[u8]) -> f64 {
        // Analyze pupil response dynamics
        if data.len() < 100 { return 0.0; }

        let center_region = &data[data.len()/3..2*data.len()/3];
        let center_intensity = center_region.iter().map(|&x| x as f64).sum::<f64>() / center_region.len() as f64;

        let outer_regions = [&data[..data.len()/3], &data[2*data.len()/3..]];
        let outer_intensity = outer_regions.iter()
            .flat_map(|region| region.iter())
            .map(|&x| x as f64)
            .sum::<f64>() / (2 * data.len() / 3) as f64;

        // Good iris should have contrast between pupil and iris regions
        let contrast = (outer_intensity - center_intensity).abs() / 255.0;
        contrast.min(1.0)
    }

    fn calculate_iris_complexity(&self, data: &[u8]) -> f64 {
        // Calculate complexity of iris patterns
        let mut complexity = 0.0;

        for window in data.windows(8) {
            let mut transitions = 0;
            for i in 1..window.len() {
                if (window[i] as i16 - window[i-1] as i16).abs() > 10 {
                    transitions += 1;
                }
            }
            complexity += transitions as f64 / 7.0;
        }

        if data.len() < 8 { 0.0 } else { (complexity / (data.len() - 7) as f64).min(1.0) }
    }
}

/// Anti-replay attack detector
pub struct ReplayDetector {
    seen_responses: dashmap::DashMap<String, u64>,
}

impl ReplayDetector {
    pub fn new() -> Self {
        Self {
            seen_responses: dashmap::DashMap::new(),
        }
    }

    pub fn check_replay(&self, response: &BiometricResponse) -> Result<()> {
        let response_hash = self.calculate_response_hash(response)?;
        let current_time = current_timestamp();

        // Check if we've seen this exact response before
        if let Some(timestamp) = self.seen_responses.get(&response_hash) {
            if current_time - *timestamp < 60 { // 1 minute replay window
                return Err(BiometricError::ReplayAttack.into());
            }
        }

        // Store this response hash
        self.seen_responses.insert(response_hash, current_time);

        // Clean old entries periodically
        if current_time % 300 == 0 { // Every 5 minutes
            self.cleanup_old_entries(current_time);
        }

        Ok(())
    }

    fn calculate_response_hash(&self, response: &BiometricResponse) -> Result<String> {
        let mut hasher = Sha256::new();
        hasher.update(&response.biometric_data);
        hasher.update(&response.timestamp.to_le_bytes());
        hasher.update(&response.anti_replay_token.as_bytes());

        Ok(hex::encode(hasher.finalize()))
    }

    fn cleanup_old_entries(&self, current_time: u64) {
        self.seen_responses.retain(|_, &mut timestamp| {
            current_time - timestamp < 300 // Keep entries for 5 minutes
        });
    }
}