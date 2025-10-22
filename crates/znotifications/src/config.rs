//! Configuration structures for notification providers

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Main notification configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationConfig {
    pub email: EmailConfig,
    pub sms: SmsConfig,
    pub rate_limiting: bool,
    pub webhook_security: WebhookSecurity,
}

/// Email provider configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailConfig {
    pub default_provider: EmailProviderType,
    pub providers: HashMap<EmailProviderType, EmailProviderConfig>,
    pub fallback_enabled: bool,
    pub retry_config: RetryConfig,
}

/// SMS provider configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmsConfig {
    pub default_provider: SmsProviderType,
    pub providers: HashMap<SmsProviderType, SmsProviderConfig>,
    pub fallback_enabled: bool,
    pub retry_config: RetryConfig,
}

/// Email provider types
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmailProviderType {
    SendGrid,
    AwsSes,
    Mailgun,
    Smtp,
}

/// SMS provider types
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum SmsProviderType {
    Twilio,
    AwsSns,
}

/// Email provider configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailProviderConfig {
    pub provider_type: EmailProviderType,
    pub credentials: EmailCredentials,
    pub settings: EmailSettings,
    pub enabled: bool,
    pub priority: u32, // Lower number = higher priority for fallbacks
}

/// SMS provider configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmsProviderConfig {
    pub provider_type: SmsProviderType,
    pub credentials: SmsCredentials,
    pub settings: SmsSettings,
    pub enabled: bool,
    pub priority: u32,
}

/// Email provider credentials
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum EmailCredentials {
    SendGrid {
        api_key: String,
    },
    AwsSes {
        access_key_id: String,
        secret_access_key: String,
        region: String,
    },
    Mailgun {
        api_key: String,
        domain: String,
        base_url: Option<String>, // For EU endpoint
    },
    Smtp {
        host: String,
        port: u16,
        username: String,
        password: String,
        use_tls: bool,
        use_starttls: bool,
    },
}

/// SMS provider credentials
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SmsCredentials {
    Twilio {
        account_sid: String,
        auth_token: String,
    },
    AwsSns {
        access_key_id: String,
        secret_access_key: String,
        region: String,
    },
}

/// Email provider settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailSettings {
    pub default_from_email: Option<String>,
    pub default_from_name: Option<String>,
    pub default_reply_to: Option<String>,
    pub track_opens: bool,
    pub track_clicks: bool,
    pub batch_size: Option<u32>,
    pub rate_limit_per_second: Option<f64>,
}

/// SMS provider settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmsSettings {
    pub default_from_number: Option<String>,
    pub delivery_receipt: bool,
    pub rate_limit_per_second: Option<f64>,
}

/// Retry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_multiplier: f64,
    pub retry_on_rate_limit: bool,
}

/// Webhook security configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookSecurity {
    pub verify_signatures: bool,
    pub allowed_ips: Vec<String>,
    pub timeout_seconds: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay_ms: 1000,
            max_delay_ms: 60000,
            backoff_multiplier: 2.0,
            retry_on_rate_limit: true,
        }
    }
}

impl Default for EmailSettings {
    fn default() -> Self {
        Self {
            default_from_email: None,
            default_from_name: None,
            default_reply_to: None,
            track_opens: false,
            track_clicks: false,
            batch_size: Some(100),
            rate_limit_per_second: Some(10.0),
        }
    }
}

impl Default for SmsSettings {
    fn default() -> Self {
        Self {
            default_from_number: None,
            delivery_receipt: true,
            rate_limit_per_second: Some(1.0),
        }
    }
}

impl Default for WebhookSecurity {
    fn default() -> Self {
        Self {
            verify_signatures: true,
            allowed_ips: Vec::new(),
            timeout_seconds: 30,
        }
    }
}

impl NotificationConfig {
    /// Create a new notification config from environment variables
    pub fn from_env() -> anyhow::Result<Self> {
        let email_config = EmailConfig::from_env()?;
        let sms_config = SmsConfig::from_env()?;

        Ok(Self {
            email: email_config,
            sms: sms_config,
            rate_limiting: std::env::var("NOTIFICATION_RATE_LIMITING")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            webhook_security: WebhookSecurity::default(),
        })
    }
}

impl EmailConfig {
    /// Create email config from environment variables
    pub fn from_env() -> anyhow::Result<Self> {
        let mut providers = HashMap::new();

        // SendGrid configuration
        if let Ok(api_key) = std::env::var("SENDGRID_API_KEY") {
            providers.insert(
                EmailProviderType::SendGrid,
                EmailProviderConfig {
                    provider_type: EmailProviderType::SendGrid,
                    credentials: EmailCredentials::SendGrid { api_key },
                    settings: EmailSettings::default(),
                    enabled: true,
                    priority: 1,
                },
            );
        }

        // AWS SES configuration
        if let (Ok(access_key), Ok(secret_key), Ok(region)) = (
            std::env::var("AWS_ACCESS_KEY_ID"),
            std::env::var("AWS_SECRET_ACCESS_KEY"),
            std::env::var("AWS_REGION"),
        ) {
            providers.insert(
                EmailProviderType::AwsSes,
                EmailProviderConfig {
                    provider_type: EmailProviderType::AwsSes,
                    credentials: EmailCredentials::AwsSes {
                        access_key_id: access_key,
                        secret_access_key: secret_key,
                        region,
                    },
                    settings: EmailSettings::default(),
                    enabled: true,
                    priority: 2,
                },
            );
        }

        // Mailgun configuration
        if let (Ok(api_key), Ok(domain)) = (
            std::env::var("MAILGUN_API_KEY"),
            std::env::var("MAILGUN_DOMAIN"),
        ) {
            providers.insert(
                EmailProviderType::Mailgun,
                EmailProviderConfig {
                    provider_type: EmailProviderType::Mailgun,
                    credentials: EmailCredentials::Mailgun {
                        api_key,
                        domain,
                        base_url: std::env::var("MAILGUN_BASE_URL").ok(),
                    },
                    settings: EmailSettings::default(),
                    enabled: true,
                    priority: 3,
                },
            );
        }

        // SMTP configuration
        if let (Ok(host), Ok(username), Ok(password)) = (
            std::env::var("SMTP_HOST"),
            std::env::var("SMTP_USERNAME"),
            std::env::var("SMTP_PASSWORD"),
        ) {
            let port = std::env::var("SMTP_PORT")
                .unwrap_or_else(|_| "587".to_string())
                .parse()
                .unwrap_or(587);
            let use_tls = std::env::var("SMTP_USE_TLS")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true);
            let use_starttls = std::env::var("SMTP_USE_STARTTLS")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true);

            providers.insert(
                EmailProviderType::Smtp,
                EmailProviderConfig {
                    provider_type: EmailProviderType::Smtp,
                    credentials: EmailCredentials::Smtp {
                        host,
                        port,
                        username,
                        password,
                        use_tls,
                        use_starttls,
                    },
                    settings: EmailSettings::default(),
                    enabled: true,
                    priority: 4,
                },
            );
        }

        if providers.is_empty() {
            return Err(anyhow::anyhow!("No email providers configured"));
        }

        // Determine default provider (highest priority = lowest number)
        let default_provider = providers
            .values()
            .min_by_key(|config| config.priority)
            .map(|config| config.provider_type)
            .unwrap();

        Ok(Self {
            default_provider,
            providers,
            fallback_enabled: true,
            retry_config: RetryConfig::default(),
        })
    }
}

impl SmsConfig {
    /// Create SMS config from environment variables
    pub fn from_env() -> anyhow::Result<Self> {
        let mut providers = HashMap::new();

        // Twilio configuration
        if let (Ok(account_sid), Ok(auth_token)) = (
            std::env::var("TWILIO_ACCOUNT_SID"),
            std::env::var("TWILIO_AUTH_TOKEN"),
        ) {
            providers.insert(
                SmsProviderType::Twilio,
                SmsProviderConfig {
                    provider_type: SmsProviderType::Twilio,
                    credentials: SmsCredentials::Twilio {
                        account_sid,
                        auth_token,
                    },
                    settings: SmsSettings::default(),
                    enabled: true,
                    priority: 1,
                },
            );
        }

        // AWS SNS configuration
        if let (Ok(access_key), Ok(secret_key), Ok(region)) = (
            std::env::var("AWS_ACCESS_KEY_ID"),
            std::env::var("AWS_SECRET_ACCESS_KEY"),
            std::env::var("AWS_REGION"),
        ) {
            providers.insert(
                SmsProviderType::AwsSns,
                SmsProviderConfig {
                    provider_type: SmsProviderType::AwsSns,
                    credentials: SmsCredentials::AwsSns {
                        access_key_id: access_key,
                        secret_access_key: secret_key,
                        region,
                    },
                    settings: SmsSettings::default(),
                    enabled: true,
                    priority: 2,
                },
            );
        }

        if providers.is_empty() {
            return Err(anyhow::anyhow!("No SMS providers configured"));
        }

        // Determine default provider
        let default_provider = providers
            .values()
            .min_by_key(|config| config.priority)
            .map(|config| config.provider_type)
            .unwrap();

        Ok(Self {
            default_provider,
            providers,
            fallback_enabled: true,
            retry_config: RetryConfig::default(),
        })
    }
}