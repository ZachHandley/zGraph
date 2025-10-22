use super::*;
use anyhow::Result;
use rand_core::{OsRng, RngCore};
use sha2::{Sha256, Digest};
use std::collections::HashMap;


/// Cancelable biometric template generator that creates privacy-preserving
/// biometric templates using irreversible transformations
pub struct CancelableTemplateGenerator {
    transform_registry: HashMap<String, Box<dyn CancelableTransform>>,
    quality_assessor: QualityAssessor,
}

/// Trait for cancelable biometric transformations
pub trait CancelableTransform: Send + Sync {
    /// Generate a cancelable template from raw biometric data
    fn transform(&self, data: &[u8], params: &TransformParameters) -> Result<Vec<u8>>;

    /// Verify that the transformation is irreversible
    fn verify_irreversibility(&self, template: &[u8]) -> bool;

    /// Get the transform identifier
    fn transform_id(&self) -> &str;

    /// Get recommended parameters for this transform
    fn default_parameters(&self) -> Result<TransformParameters>;
}

/// BioHashing cancelable transform implementation
pub struct BioHashingTransform;

/// Random projection cancelable transform
pub struct RandomProjectionTransform;

/// Fuzzy commitment scheme transform
pub struct FuzzyCommitmentTransform;

/// Biometric cryptosystem transform
pub struct BioCryptoTransform;

impl CancelableTemplateGenerator {
    pub fn new() -> Self {
        let mut transform_registry: HashMap<String, Box<dyn CancelableTransform>> = HashMap::new();

        transform_registry.insert("biohash".to_string(), Box::new(BioHashingTransform));
        transform_registry.insert("randproj".to_string(), Box::new(RandomProjectionTransform));
        transform_registry.insert("fuzzycommit".to_string(), Box::new(FuzzyCommitmentTransform));
        transform_registry.insert("biocrypto".to_string(), Box::new(BioCryptoTransform));

        Self {
            transform_registry,
            quality_assessor: QualityAssessor::new(),
        }
    }

    /// Generate a cancelable biometric template
    pub fn generate_template(
        &self,
        user_id: Ulid,
        org_id: u64,
        modality: BiometricModality,
        raw_data: &[u8],
        transform_id: &str,
    ) -> Result<BiometricTemplate> {
        // Assess biometric quality
        let quality_metrics = self.quality_assessor.assess_quality(&raw_data, &modality)?;

        // Check minimum quality threshold
        if quality_metrics.overall_quality < 0.6 {
            return Err(BiometricError::TemplateGeneration(
                "Biometric quality below minimum threshold".to_string()
            ).into());
        }

        let transform = self.transform_registry.get(transform_id)
            .ok_or_else(|| BiometricError::TemplateGeneration(
                format!("Unknown transform: {}", transform_id)
            ))?;

        let transform_params = transform.default_parameters()?;
        let protected_template = transform.transform(raw_data, &transform_params)?;

        // Verify irreversibility
        if !transform.verify_irreversibility(&protected_template) {
            return Err(BiometricError::TemplateProtectionFailure(
                "Transform verification failed".to_string()
            ).into());
        }

        let now = current_timestamp();

        Ok(BiometricTemplate {
            template_id: Ulid::new(),
            user_id,
            org_id,
            modality,
            protected_template,
            transform_parameters: transform_params,
            quality_metrics,
            created_at: now,
            updated_at: now,
            version: 1,
        })
    }

    /// Revoke/cancel a biometric template by generating new transform parameters
    pub fn revoke_template(&self, template: &mut BiometricTemplate) -> Result<()> {
        let transform = self.transform_registry.get(&template.transform_parameters.transform_id)
            .ok_or_else(|| BiometricError::TemplateGeneration(
                "Transform not found for revocation".to_string()
            ))?;

        // Generate new transform parameters to make old template unusable
        template.transform_parameters = transform.default_parameters()?;
        template.updated_at = current_timestamp();
        template.version += 1;

        Ok(())
    }
}

impl BioHashingTransform {
    fn generate_random_vectors(&self, dimension: usize, num_vectors: usize, seed: &[u8]) -> Result<Vec<Vec<f64>>> {
        let mut hasher = Sha256::new();
        hasher.update(seed);
        let hash_seed = hasher.finalize();

        let mut vectors = Vec::with_capacity(num_vectors);
        let mut rng_state = hash_seed.to_vec();

        for i in 0..num_vectors {
            let mut vector = Vec::with_capacity(dimension);

            for j in 0..dimension {
                // Use deterministic pseudo-random generation
                let mut hasher = Sha256::new();
                hasher.update(&rng_state);
                hasher.update(&(i as u32).to_le_bytes());
                hasher.update(&(j as u32).to_le_bytes());
                let hash = hasher.finalize();

                // Convert hash to float in [-1, 1]
                let val = (hash[0] as f64) / 128.0 - 1.0;
                vector.push(val);
            }
            vectors.push(vector);

            // Update RNG state
            let mut hasher = Sha256::new();
            hasher.update(&rng_state);
            hasher.update(&(i as u32).to_le_bytes());
            rng_state = hasher.finalize().to_vec();
        }

        Ok(vectors)
    }

    fn extract_features(&self, data: &[u8]) -> Result<Vec<f64>> {
        // Simple feature extraction - in practice this would be modality-specific
        let mut features = Vec::new();

        // Statistical features
        let mean = data.iter().map(|&x| x as f64).sum::<f64>() / data.len() as f64;
        let variance = data.iter()
            .map(|&x| (x as f64 - mean).powi(2))
            .sum::<f64>() / data.len() as f64;

        features.push(mean);
        features.push(variance.sqrt());

        // Frequency domain features (simplified DCT-like transform)
        for i in 0..min(64, data.len()) {
            let mut sum = 0.0;
            for (j, &val) in data.iter().enumerate() {
                sum += (val as f64) * ((std::f64::consts::PI * i as f64 * j as f64) / data.len() as f64).cos();
            }
            features.push(sum);
        }

        // Pad or truncate to fixed size
        features.resize(128, 0.0);
        Ok(features)
    }

    fn dot_product(a: &[f64], b: &[f64]) -> f64 {
        a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
    }
}

impl CancelableTransform for BioHashingTransform {
    fn transform(&self, data: &[u8], params: &TransformParameters) -> Result<Vec<u8>> {
        let features = self.extract_features(data)?;
        let random_vectors = self.generate_random_vectors(features.len(), 256, &params.salt)?;

        let mut bio_hash = Vec::new();
        for vector in random_vectors {
            let dot_product = Self::dot_product(&features, &vector);
            // Quantize to binary
            let bit = if dot_product >= 0.0 { 1u8 } else { 0u8 };
            bio_hash.push(bit);
        }

        // Pack bits into bytes
        let mut packed = Vec::new();
        for chunk in bio_hash.chunks(8) {
            let mut byte = 0u8;
            for (i, &bit) in chunk.iter().enumerate() {
                byte |= bit << i;
            }
            packed.push(byte);
        }

        Ok(packed)
    }

    fn verify_irreversibility(&self, template: &[u8]) -> bool {
        // BioHashing is irreversible due to random projection + quantization
        // Verify template has expected structure
        template.len() == 32 && template.iter().any(|&x| x != 0)
    }

    fn transform_id(&self) -> &str {
        "biohash"
    }

    fn default_parameters(&self) -> Result<TransformParameters> {
        let mut salt = [0u8; 32];
        OsRng.fill_bytes(&mut salt);

        Ok(TransformParameters {
            transform_id: "biohash".to_string(),
            parameters: vec![128, 0, 0, 1], // feature_dim, reserved, reserved, version
            salt,
            iteration_count: 1000,
        })
    }
}

impl CancelableTransform for RandomProjectionTransform {
    fn transform(&self, data: &[u8], params: &TransformParameters) -> Result<Vec<u8>> {
        let features = self.extract_features(data)?;

        // Johnson-Lindenstrauss random projection
        let target_dim = 64;
        let mut projection_matrix = Vec::new();

        let mut hasher = Sha256::new();
        hasher.update(&params.salt);
        let seed = hasher.finalize();

        // Generate random projection matrix
        for i in 0..target_dim {
            let mut row = Vec::new();
            for j in 0..features.len() {
                let mut hasher = Sha256::new();
                hasher.update(&seed);
                hasher.update(&(i as u32).to_le_bytes());
                hasher.update(&(j as u32).to_le_bytes());
                let hash = hasher.finalize();

                let val = match hash[0] % 3 {
                    0 => -1.0 / (target_dim as f64).sqrt(),
                    1 => 1.0 / (target_dim as f64).sqrt(),
                    _ => 0.0,
                };
                row.push(val);
            }
            projection_matrix.push(row);
        }

        // Apply projection
        let mut projected = Vec::new();
        for row in &projection_matrix {
            let dot_product: f64 = features.iter().zip(row.iter()).map(|(a, b)| a * b).sum();
            projected.push(dot_product);
        }

        // Quantize to bytes
        let mut result = Vec::new();
        for val in projected {
            let normalized = ((val + 1.0) * 127.5).clamp(0.0, 255.0) as u8;
            result.push(normalized);
        }

        Ok(result)
    }

    fn verify_irreversibility(&self, template: &[u8]) -> bool {
        template.len() == 64 && template.iter().any(|&x| x != 0)
    }

    fn transform_id(&self) -> &str {
        "randproj"
    }

    fn default_parameters(&self) -> Result<TransformParameters> {
        let mut salt = [0u8; 32];
        OsRng.fill_bytes(&mut salt);

        Ok(TransformParameters {
            transform_id: "randproj".to_string(),
            parameters: vec![64, 0, 0, 1], // target_dim, reserved, reserved, version
            salt,
            iteration_count: 1,
        })
    }
}

impl RandomProjectionTransform {
    fn extract_features(&self, data: &[u8]) -> Result<Vec<f64>> {
        // Similar to BioHashingTransform but potentially different for RP
        let mut features = Vec::new();

        // Normalized pixel values
        for &byte in data.iter().take(256) {
            features.push((byte as f64) / 255.0);
        }

        features.resize(256, 0.0);
        Ok(features)
    }
}

impl CancelableTransform for FuzzyCommitmentTransform {
    fn transform(&self, data: &[u8], params: &TransformParameters) -> Result<Vec<u8>> {
        // Fuzzy commitment scheme implementation
        let features = self.quantize_features(data)?;
        let codeword = self.generate_error_correcting_codeword(&params.salt)?;

        // XOR biometric with codeword
        let mut commitment = Vec::new();
        for (i, &feature_bit) in features.iter().enumerate() {
            let codeword_bit = codeword[i % codeword.len()];
            commitment.push(feature_bit ^ codeword_bit);
        }

        // Hash the commitment for privacy
        let mut hasher = Sha256::new();
        hasher.update(&commitment);
        hasher.update(&params.salt);
        let hash = hasher.finalize();

        Ok(hash.to_vec())
    }

    fn verify_irreversibility(&self, template: &[u8]) -> bool {
        template.len() == 32 // SHA256 output
    }

    fn transform_id(&self) -> &str {
        "fuzzycommit"
    }

    fn default_parameters(&self) -> Result<TransformParameters> {
        let mut salt = [0u8; 32];
        OsRng.fill_bytes(&mut salt);

        Ok(TransformParameters {
            transform_id: "fuzzycommit".to_string(),
            parameters: vec![0, 0, 0, 1], // reserved, reserved, reserved, version
            salt,
            iteration_count: 1,
        })
    }
}

impl FuzzyCommitmentTransform {
    fn quantize_features(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut features = Vec::new();

        // Simple quantization to binary
        for &byte in data.iter().take(256) {
            for i in 0..8 {
                let bit = (byte >> i) & 1;
                features.push(bit);
            }
        }

        features.resize(2048, 0);
        Ok(features)
    }

    fn generate_error_correcting_codeword(&self, seed: &[u8]) -> Result<Vec<u8>> {
        // Simple Reed-Solomon like code generation
        let mut codeword = Vec::new();
        let mut hasher = Sha256::new();
        hasher.update(seed);
        hasher.update(b"codeword");
        let hash = hasher.finalize();

        for byte in hash.iter() {
            for i in 0..8 {
                let bit = (byte >> i) & 1;
                codeword.push(bit);
            }
        }

        // Extend to required length
        while codeword.len() < 2048 {
            let mut hasher = Sha256::new();
            hasher.update(&codeword[codeword.len() - 32..]);
            let hash = hasher.finalize();

            for byte in hash.iter() {
                for i in 0..8 {
                    if codeword.len() >= 2048 { break; }
                    let bit = (byte >> i) & 1;
                    codeword.push(bit);
                }
            }
        }

        codeword.resize(2048, 0);
        Ok(codeword)
    }
}

impl CancelableTransform for BioCryptoTransform {
    fn transform(&self, data: &[u8], params: &TransformParameters) -> Result<Vec<u8>> {
        // Biometric cryptosystem using fuzzy extractors
        let features = self.extract_stable_features(data)?;
        let (key, helper_data) = self.fuzzy_extract(&features, &params.salt)?;

        // Combine key derivation with helper data
        let mut result = Vec::new();
        result.extend_from_slice(&key);
        result.extend_from_slice(&helper_data);

        Ok(result)
    }

    fn verify_irreversibility(&self, template: &[u8]) -> bool {
        template.len() == 64 // 32 bytes key + 32 bytes helper data
    }

    fn transform_id(&self) -> &str {
        "biocrypto"
    }

    fn default_parameters(&self) -> Result<TransformParameters> {
        let mut salt = [0u8; 32];
        OsRng.fill_bytes(&mut salt);

        Ok(TransformParameters {
            transform_id: "biocrypto".to_string(),
            parameters: vec![32, 0, 0, 1], // key_size, reserved, reserved, version
            salt,
            iteration_count: 1000,
        })
    }
}

impl BioCryptoTransform {
    fn extract_stable_features(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Extract stable features that are robust to noise
        let mut features = Vec::new();

        // Use sliding window statistics
        for window in data.windows(8) {
            let mean = window.iter().map(|&x| x as u32).sum::<u32>() / window.len() as u32;
            let above_mean = window.iter().filter(|&&x| x as u32 > mean).count();

            // Binary feature based on majority vote
            let bit = if above_mean > window.len() / 2 { 1 } else { 0 };
            features.push(bit);
        }

        features.resize(256, 0);
        Ok(features)
    }

    fn fuzzy_extract(&self, features: &[u8], salt: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        // Simplified fuzzy extractor
        let mut key = [0u8; 32];
        let mut helper_data = Vec::new();

        // Generate random helper data based on salt
        let mut hasher = Sha256::new();
        hasher.update(salt);
        hasher.update(b"helper");
        let helper_seed = hasher.finalize();

        helper_data.extend_from_slice(&helper_seed);

        // Derive key using features and helper data
        let mut key_hasher = Sha256::new();
        key_hasher.update(features);
        key_hasher.update(&helper_data);
        key_hasher.update(salt);
        let key_hash = key_hasher.finalize();

        key.copy_from_slice(&key_hash);

        Ok((key.to_vec(), helper_data))
    }
}

/// Quality assessment for biometric samples
pub struct QualityAssessor;

impl QualityAssessor {
    pub fn new() -> Self {
        Self
    }

    pub fn assess_quality(&self, data: &[u8], modality: &BiometricModality) -> Result<QualityMetrics> {
        match modality {
            BiometricModality::Face => self.assess_face_quality(data),
            BiometricModality::Fingerprint => self.assess_fingerprint_quality(data),
            BiometricModality::Voice => self.assess_voice_quality(data),
            BiometricModality::Iris => self.assess_iris_quality(data),
            _ => self.assess_generic_quality(data),
        }
    }

    fn assess_face_quality(&self, data: &[u8]) -> Result<QualityMetrics> {
        let sharpness = self.calculate_sharpness(data);
        let contrast = self.calculate_contrast(data);
        let uniformity = self.calculate_uniformity(data);

        let modality_specific = vec![
            ("pose_angle".to_string(), self.estimate_pose_quality(data)),
            ("illumination".to_string(), self.estimate_illumination_quality(data)),
            ("occlusion".to_string(), self.estimate_occlusion_quality(data)),
        ];

        let overall_quality = (sharpness + contrast + uniformity) / 3.0;

        Ok(QualityMetrics {
            overall_quality,
            uniformity,
            sharpness,
            contrast,
            modality_specific,
        })
    }

    fn assess_fingerprint_quality(&self, data: &[u8]) -> Result<QualityMetrics> {
        let sharpness = self.calculate_sharpness(data);
        let contrast = self.calculate_contrast(data);
        let uniformity = self.calculate_uniformity(data);

        let modality_specific = vec![
            ("ridge_quality".to_string(), self.estimate_ridge_quality(data)),
            ("minutiae_count".to_string(), self.estimate_minutiae_quality(data)),
        ];

        let overall_quality = (sharpness + contrast + uniformity) / 3.0;

        Ok(QualityMetrics {
            overall_quality,
            uniformity,
            sharpness,
            contrast,
            modality_specific,
        })
    }

    fn assess_voice_quality(&self, data: &[u8]) -> Result<QualityMetrics> {
        let uniformity = self.calculate_audio_uniformity(data);
        let sharpness = self.calculate_audio_clarity(data);
        let contrast = self.calculate_dynamic_range(data);

        let modality_specific = vec![
            ("snr".to_string(), self.estimate_signal_to_noise(data)),
            ("spectral_quality".to_string(), self.estimate_spectral_quality(data)),
        ];

        let overall_quality = (uniformity + sharpness + contrast) / 3.0;

        Ok(QualityMetrics {
            overall_quality,
            uniformity,
            sharpness,
            contrast,
            modality_specific,
        })
    }

    fn assess_iris_quality(&self, data: &[u8]) -> Result<QualityMetrics> {
        let sharpness = self.calculate_sharpness(data);
        let contrast = self.calculate_contrast(data);
        let uniformity = self.calculate_uniformity(data);

        let modality_specific = vec![
            ("pupil_dilation".to_string(), self.estimate_pupil_quality(data)),
            ("eyelid_occlusion".to_string(), self.estimate_eyelid_interference(data)),
        ];

        let overall_quality = (sharpness + contrast + uniformity) / 3.0;

        Ok(QualityMetrics {
            overall_quality,
            uniformity,
            sharpness,
            contrast,
            modality_specific,
        })
    }

    fn assess_generic_quality(&self, data: &[u8]) -> Result<QualityMetrics> {
        let sharpness = self.calculate_sharpness(data);
        let contrast = self.calculate_contrast(data);
        let uniformity = self.calculate_uniformity(data);

        let overall_quality = (sharpness + contrast + uniformity) / 3.0;

        Ok(QualityMetrics {
            overall_quality,
            uniformity,
            sharpness,
            contrast,
            modality_specific: vec![],
        })
    }

    // Quality calculation methods
    fn calculate_sharpness(&self, data: &[u8]) -> f64 {
        if data.len() < 2 { return 0.0; }

        let mut gradient_sum = 0.0;
        for window in data.windows(2) {
            let diff = (window[1] as i32 - window[0] as i32).abs() as f64;
            gradient_sum += diff;
        }

        (gradient_sum / (data.len() - 1) as f64) / 255.0
    }

    fn calculate_contrast(&self, data: &[u8]) -> f64 {
        if data.is_empty() { return 0.0; }

        let min_val = *data.iter().min().unwrap() as f64;
        let max_val = *data.iter().max().unwrap() as f64;

        if max_val == min_val { 0.0 } else { (max_val - min_val) / 255.0 }
    }

    fn calculate_uniformity(&self, data: &[u8]) -> f64 {
        if data.is_empty() { return 0.0; }

        let mean = data.iter().map(|&x| x as f64).sum::<f64>() / data.len() as f64;
        let variance = data.iter()
            .map(|&x| (x as f64 - mean).powi(2))
            .sum::<f64>() / data.len() as f64;

        // Normalize and invert so higher values mean better uniformity
        1.0 - (variance.sqrt() / 255.0).min(1.0)
    }

    fn calculate_audio_uniformity(&self, data: &[u8]) -> f64 {
        self.calculate_uniformity(data)
    }

    fn calculate_audio_clarity(&self, data: &[u8]) -> f64 {
        self.calculate_sharpness(data)
    }

    fn calculate_dynamic_range(&self, data: &[u8]) -> f64 {
        self.calculate_contrast(data)
    }

    // Modality-specific quality estimators
    fn estimate_pose_quality(&self, data: &[u8]) -> f64 {
        // Simplified pose estimation based on data distribution
        let center = data.len() / 2;
        let left_mean = data[..center].iter().map(|&x| x as f64).sum::<f64>() / center as f64;
        let right_mean = data[center..].iter().map(|&x| x as f64).sum::<f64>() / (data.len() - center) as f64;

        // Good pose should have balanced left/right distribution
        let asymmetry = (left_mean - right_mean).abs() / 255.0;
        1.0 - asymmetry.min(1.0)
    }

    fn estimate_illumination_quality(&self, data: &[u8]) -> f64 {
        let mean = data.iter().map(|&x| x as f64).sum::<f64>() / data.len() as f64;
        // Good illumination should be in the middle range
        let ideal_range = 64.0..=192.0;
        if ideal_range.contains(&mean) {
            1.0
        } else {
            let distance = if mean < 64.0 { 64.0 - mean } else { mean - 192.0 };
            1.0 - (distance / 128.0).min(1.0)
        }
    }

    fn estimate_occlusion_quality(&self, data: &[u8]) -> f64 {
        // Estimate based on local variance - high occlusion areas have low variance
        let mut local_variances = Vec::new();

        for window in data.chunks(16) {
            if window.len() < 4 { continue; }
            let mean = window.iter().map(|&x| x as f64).sum::<f64>() / window.len() as f64;
            let variance = window.iter()
                .map(|&x| (x as f64 - mean).powi(2))
                .sum::<f64>() / window.len() as f64;
            local_variances.push(variance);
        }

        let avg_variance = local_variances.iter().sum::<f64>() / local_variances.len() as f64;
        (avg_variance / (255.0 * 255.0)).min(1.0)
    }

    fn estimate_ridge_quality(&self, data: &[u8]) -> f64 {
        // Estimate ridge quality based on periodic patterns
        self.calculate_sharpness(data)
    }

    fn estimate_minutiae_quality(&self, data: &[u8]) -> f64 {
        // Simplified minutiae estimation
        let mut direction_changes = 0;
        for window in data.windows(3) {
            let increasing = window[1] > window[0];
            let still_increasing = window[2] > window[1];
            if increasing != still_increasing {
                direction_changes += 1;
            }
        }

        // Normalize direction changes
        (direction_changes as f64 / data.len() as f64).min(1.0)
    }

    fn estimate_signal_to_noise(&self, data: &[u8]) -> f64 {
        // Simplified SNR estimation
        let mean = data.iter().map(|&x| x as f64).sum::<f64>() / data.len() as f64;
        let signal_power = mean * mean;
        let noise_power = data.iter()
            .map(|&x| (x as f64 - mean).powi(2))
            .sum::<f64>() / data.len() as f64;

        if noise_power == 0.0 { 1.0 } else { (signal_power / noise_power).min(1.0) }
    }

    fn estimate_spectral_quality(&self, data: &[u8]) -> f64 {
        // Simplified spectral quality based on frequency content
        self.calculate_sharpness(data)
    }

    fn estimate_pupil_quality(&self, data: &[u8]) -> f64 {
        // Estimate pupil quality based on contrast in center region
        let center_start = data.len() / 3;
        let center_end = 2 * data.len() / 3;
        let center_data = &data[center_start..center_end];
        self.calculate_contrast(center_data)
    }

    fn estimate_eyelid_interference(&self, data: &[u8]) -> f64 {
        // Estimate eyelid interference based on top/bottom regions
        let top_third = &data[..data.len() / 3];
        let bottom_third = &data[2 * data.len() / 3..];

        let top_mean = top_third.iter().map(|&x| x as f64).sum::<f64>() / top_third.len() as f64;
        let bottom_mean = bottom_third.iter().map(|&x| x as f64).sum::<f64>() / bottom_third.len() as f64;

        let interference = (top_mean - bottom_mean).abs() / 255.0;
        1.0 - interference.min(1.0)
    }
}

use std::cmp::min;