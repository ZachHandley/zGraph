//! Provider-specific implementations and utilities

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::config::{EmailProviderType, SmsProviderType};

/// Provider capability information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderCapabilities {
    pub provider_type: ProviderType,
    pub supports_html: bool,
    pub supports_attachments: bool,
    pub supports_tracking: bool,
    pub supports_templates: bool,
    pub supports_scheduling: bool,
    pub max_recipients: Option<u32>,
    pub rate_limits: Option<RateLimits>,
}

/// Provider type (unified for email and SMS)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ProviderType {
    Email(EmailProviderType),
    Sms(SmsProviderType),
}

/// Rate limit information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimits {
    pub requests_per_second: Option<f64>,
    pub requests_per_minute: Option<u32>,
    pub requests_per_hour: Option<u32>,
    pub requests_per_day: Option<u32>,
}

/// Provider health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderHealth {
    pub provider_type: ProviderType,
    pub is_available: bool,
    pub last_success: Option<i64>,
    pub last_failure: Option<i64>,
    pub error_rate: f64,
    pub average_response_time: Option<u64>, // milliseconds
}

/// Provider statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderStats {
    pub provider_type: ProviderType,
    pub total_sent: u64,
    pub total_delivered: u64,
    pub total_failed: u64,
    pub delivery_rate: f64,
    pub average_delivery_time: Option<u64>, // seconds
}

impl ProviderCapabilities {
    /// Get capabilities for SendGrid
    pub fn sendgrid() -> Self {
        Self {
            provider_type: ProviderType::Email(EmailProviderType::SendGrid),
            supports_html: true,
            supports_attachments: true,
            supports_tracking: true,
            supports_templates: true,
            supports_scheduling: true,
            max_recipients: Some(1000),
            rate_limits: Some(RateLimits {
                requests_per_second: Some(10.0),
                requests_per_minute: Some(600),
                requests_per_hour: Some(3600),
                requests_per_day: None,
            }),
        }
    }

    /// Get capabilities for AWS SES
    pub fn aws_ses() -> Self {
        Self {
            provider_type: ProviderType::Email(EmailProviderType::AwsSes),
            supports_html: true,
            supports_attachments: true,
            supports_tracking: false,
            supports_templates: true,
            supports_scheduling: false,
            max_recipients: Some(50),
            rate_limits: Some(RateLimits {
                requests_per_second: Some(14.0), // Default sending rate
                requests_per_minute: None,
                requests_per_hour: None,
                requests_per_day: Some(200), // Default daily quota
            }),
        }
    }

    /// Get capabilities for Mailgun
    pub fn mailgun() -> Self {
        Self {
            provider_type: ProviderType::Email(EmailProviderType::Mailgun),
            supports_html: true,
            supports_attachments: true,
            supports_tracking: true,
            supports_templates: true,
            supports_scheduling: true,
            max_recipients: Some(1000),
            rate_limits: Some(RateLimits {
                requests_per_second: Some(10.0),
                requests_per_minute: Some(300),
                requests_per_hour: Some(10000),
                requests_per_day: None,
            }),
        }
    }

    /// Get capabilities for SMTP
    pub fn smtp() -> Self {
        Self {
            provider_type: ProviderType::Email(EmailProviderType::Smtp),
            supports_html: true,
            supports_attachments: true,
            supports_tracking: false,
            supports_templates: false,
            supports_scheduling: false,
            max_recipients: Some(1),
            rate_limits: None, // Depends on SMTP server
        }
    }

    /// Get capabilities for Twilio
    pub fn twilio() -> Self {
        Self {
            provider_type: ProviderType::Sms(SmsProviderType::Twilio),
            supports_html: false,
            supports_attachments: false,
            supports_tracking: true,
            supports_templates: false,
            supports_scheduling: false,
            max_recipients: Some(1),
            rate_limits: Some(RateLimits {
                requests_per_second: Some(1.0),
                requests_per_minute: Some(60),
                requests_per_hour: None,
                requests_per_day: None,
            }),
        }
    }

    /// Get capabilities for AWS SNS
    pub fn aws_sns() -> Self {
        Self {
            provider_type: ProviderType::Sms(SmsProviderType::AwsSns),
            supports_html: false,
            supports_attachments: false,
            supports_tracking: false,
            supports_templates: false,
            supports_scheduling: false,
            max_recipients: Some(1),
            rate_limits: Some(RateLimits {
                requests_per_second: Some(1.0),
                requests_per_minute: None,
                requests_per_hour: None,
                requests_per_day: None,
            }),
        }
    }
}

/// Provider selection strategy
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ProviderSelectionStrategy {
    /// Use providers in priority order
    Priority,
    /// Round-robin between available providers
    RoundRobin,
    /// Load balance based on current queue size
    LoadBalanced,
    /// Use the fastest responding provider
    LowestLatency,
}

/// Provider manager for handling multiple providers and failover
pub struct ProviderManager {
    email_providers: Vec<ProviderCapabilities>,
    sms_providers: Vec<ProviderCapabilities>,
    selection_strategy: ProviderSelectionStrategy,
}

impl ProviderManager {
    /// Create a new provider manager
    pub fn new(strategy: ProviderSelectionStrategy) -> Self {
        Self {
            email_providers: Vec::new(),
            sms_providers: Vec::new(),
            selection_strategy: strategy,
        }
    }

    /// Register an email provider
    pub fn register_email_provider(&mut self, capabilities: ProviderCapabilities) {
        if matches!(capabilities.provider_type, ProviderType::Email(_)) {
            self.email_providers.push(capabilities);
        }
    }

    /// Register an SMS provider
    pub fn register_sms_provider(&mut self, capabilities: ProviderCapabilities) {
        if matches!(capabilities.provider_type, ProviderType::Sms(_)) {
            self.sms_providers.push(capabilities);
        }
    }

    /// Get the best email provider based on strategy
    pub fn select_email_provider(&self) -> Option<&ProviderCapabilities> {
        if self.email_providers.is_empty() {
            return None;
        }

        match self.selection_strategy {
            ProviderSelectionStrategy::Priority => {
                // Return first available provider (assumes sorted by priority)
                self.email_providers.first()
            }
            ProviderSelectionStrategy::RoundRobin => {
                // TODO: Implement round-robin logic
                self.email_providers.first()
            }
            ProviderSelectionStrategy::LoadBalanced => {
                // TODO: Implement load balancing logic
                self.email_providers.first()
            }
            ProviderSelectionStrategy::LowestLatency => {
                // TODO: Implement latency-based selection
                self.email_providers.first()
            }
        }
    }

    /// Get the best SMS provider based on strategy
    pub fn select_sms_provider(&self) -> Option<&ProviderCapabilities> {
        if self.sms_providers.is_empty() {
            return None;
        }

        match self.selection_strategy {
            ProviderSelectionStrategy::Priority => {
                self.sms_providers.first()
            }
            ProviderSelectionStrategy::RoundRobin => {
                // TODO: Implement round-robin logic
                self.sms_providers.first()
            }
            ProviderSelectionStrategy::LoadBalanced => {
                // TODO: Implement load balancing logic
                self.sms_providers.first()
            }
            ProviderSelectionStrategy::LowestLatency => {
                // TODO: Implement latency-based selection
                self.sms_providers.first()
            }
        }
    }

    /// Check if a provider supports a specific feature
    pub fn provider_supports_feature(&self, provider_type: &ProviderType, feature: &str) -> bool {
        let capabilities = match provider_type {
            ProviderType::Email(_) => {
                self.email_providers.iter().find(|p| &p.provider_type == provider_type)
            }
            ProviderType::Sms(_) => {
                self.sms_providers.iter().find(|p| &p.provider_type == provider_type)
            }
        };

        if let Some(cap) = capabilities {
            match feature {
                "html" => cap.supports_html,
                "attachments" => cap.supports_attachments,
                "tracking" => cap.supports_tracking,
                "templates" => cap.supports_templates,
                "scheduling" => cap.supports_scheduling,
                _ => false,
            }
        } else {
            false
        }
    }
}

/// Utility functions for provider validation
pub mod validation {
    use super::*;

    /// Validate email address format
    pub fn is_valid_email(email: &str) -> bool {
        // Basic email validation - for production use a proper email validation library
        email.contains('@') && email.contains('.') && email.len() > 5
    }

    /// Validate phone number format
    pub fn is_valid_phone(phone: &str) -> bool {
        // Basic phone validation - for production use a proper phone validation library
        let cleaned = phone.chars().filter(|c| c.is_ascii_digit()).collect::<String>();
        cleaned.len() >= 10 && cleaned.len() <= 15
    }

    /// Normalize phone number to E.164 format
    pub fn normalize_phone(phone: &str) -> Result<String> {
        let cleaned = phone.chars().filter(|c| c.is_ascii_digit()).collect::<String>();

        if cleaned.len() < 10 {
            return Err(anyhow::anyhow!("Phone number too short"));
        }

        // Add country code if missing (assumes US +1)
        if cleaned.len() == 10 {
            Ok(format!("+1{}", cleaned))
        } else if cleaned.len() == 11 && cleaned.starts_with('1') {
            Ok(format!("+{}", cleaned))
        } else {
            Ok(format!("+{}", cleaned))
        }
    }

    /// Check if email domain is blacklisted
    pub fn is_blacklisted_domain(email: &str) -> bool {
        let blacklisted_domains = [
            "example.com",
            "test.com",
            "invalid.com",
            "mailinator.com",
            "10minutemail.com",
        ];

        if let Some(domain) = email.split('@').nth(1) {
            blacklisted_domains.contains(&domain.to_lowercase().as_str())
        } else {
            false
        }
    }
}