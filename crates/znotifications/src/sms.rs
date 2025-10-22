//! SMS service implementation with multiple provider support

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::config::{SmsConfig, SmsCredentials, SmsProviderConfig, SmsProviderType, RetryConfig};
use crate::{SmsOptions, NotificationError};

/// SMS service that manages multiple providers and handles failover
#[derive(Clone)]
pub struct SmsService {
    providers: HashMap<SmsProviderType, Arc<dyn SmsProvider>>,
    default_provider: SmsProviderType,
    fallback_enabled: bool,
    retry_config: RetryConfig,
    http_client: Client,
}

impl SmsService {
    /// Create a new SMS service with the given configuration
    pub fn new(config: SmsConfig) -> Result<Self> {
        let mut providers: HashMap<SmsProviderType, Arc<dyn SmsProvider>> = HashMap::new();
        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;

        for (provider_type, provider_config) in config.providers {
            if !provider_config.enabled {
                continue;
            }

            let provider: Arc<dyn SmsProvider> = match provider_type {
                SmsProviderType::Twilio => {
                    Arc::new(TwilioProvider::new(provider_config, http_client.clone())?)
                }
                SmsProviderType::AwsSns => {
                    Arc::new(AwsSnsProvider::new(provider_config, http_client.clone())?)
                }
            };

            providers.insert(provider_type, provider);
        }

        if providers.is_empty() {
            return Err(anyhow::anyhow!("No SMS providers available"));
        }

        Ok(Self {
            providers,
            default_provider: config.default_provider,
            fallback_enabled: config.fallback_enabled,
            retry_config: config.retry_config,
            http_client,
        })
    }

    /// Send an SMS using the configured providers with fallback support
    pub async fn send_sms(
        &self,
        to: &str,
        message: &str,
        options: &SmsOptions,
    ) -> Result<String> {
        let providers_to_try = if self.fallback_enabled {
            let mut ordered: Vec<_> = self.providers.keys().copied().collect();
            ordered.sort_by_key(|&provider_type| {
                if provider_type == self.default_provider {
                    0
                } else {
                    self.providers.get(&provider_type)
                        .map(|p| p.priority())
                        .unwrap_or(u32::MAX)
                }
            });
            ordered
        } else {
            vec![self.default_provider]
        };

        let mut last_error = None;

        for provider_type in providers_to_try {
            if let Some(provider) = self.providers.get(&provider_type) {
                match self.send_with_retry(provider.as_ref(), to, message, options).await {
                    Ok(message_id) => {
                        info!("SMS sent successfully via {:?}: {}", provider_type, message_id);
                        return Ok(message_id);
                    }
                    Err(e) => {
                        warn!("SMS failed via {:?}: {}", provider_type, e);
                        last_error = Some(e);
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!("No providers available")
        }))
    }

    async fn send_with_retry(
        &self,
        provider: &dyn SmsProvider,
        to: &str,
        message: &str,
        options: &SmsOptions,
    ) -> Result<String> {
        let mut last_error = None;
        let mut delay_ms = self.retry_config.initial_delay_ms;

        for attempt in 0..=self.retry_config.max_retries {
            match provider.send_sms(to, message, options).await {
                Ok(message_id) => return Ok(message_id),
                Err(e) => {
                    last_error = Some(e);

                    if attempt < self.retry_config.max_retries {
                        sleep(Duration::from_millis(delay_ms)).await;
                        delay_ms = (delay_ms as f64 * self.retry_config.backoff_multiplier) as u64;
                        delay_ms = delay_ms.min(self.retry_config.max_delay_ms);
                    }
                }
            }
        }

        Err(last_error.unwrap())
    }
}

/// Trait for SMS providers
#[async_trait]
pub trait SmsProvider: Send + Sync {
    async fn send_sms(
        &self,
        to: &str,
        message: &str,
        options: &SmsOptions,
    ) -> Result<String>;

    fn provider_type(&self) -> SmsProviderType;
    fn priority(&self) -> u32;
    fn is_available(&self) -> bool;
}

/// Twilio SMS provider
pub struct TwilioProvider {
    account_sid: String,
    auth_token: String,
    config: SmsProviderConfig,
    client: Client,
}

impl TwilioProvider {
    pub fn new(config: SmsProviderConfig, client: Client) -> Result<Self> {
        let (account_sid, auth_token) = match &config.credentials {
            SmsCredentials::Twilio { account_sid, auth_token } => {
                (account_sid.clone(), auth_token.clone())
            }
            _ => return Err(anyhow::anyhow!("Invalid credentials for Twilio")),
        };

        Ok(Self {
            account_sid,
            auth_token,
            config,
            client,
        })
    }
}

#[async_trait]
impl SmsProvider for TwilioProvider {
    async fn send_sms(
        &self,
        to: &str,
        message: &str,
        options: &SmsOptions,
    ) -> Result<String> {
        let from = options.from_number.as_deref()
            .or(self.config.settings.default_from_number.as_deref())
            .ok_or_else(|| anyhow::anyhow!("No from number configured"))?;

        let mut form_data = vec![
            ("From", from),
            ("To", to),
            ("Body", message),
        ];

        if options.delivery_receipt || self.config.settings.delivery_receipt {
            // TODO: Add webhook URL for delivery receipts
            form_data.push(("StatusCallback", ""));
        }

        let url = format!(
            "https://api.twilio.com/2010-04-01/Accounts/{}/Messages.json",
            self.account_sid
        );

        let response = self.client
            .post(&url)
            .basic_auth(&self.account_sid, Some(&self.auth_token))
            .form(&form_data)
            .send()
            .await?;

        if response.status().is_success() {
            let response_data: serde_json::Value = response.json().await?;
            let message_id = response_data["sid"].as_str().unwrap_or("unknown").to_string();
            Ok(message_id)
        } else {
            let error_text = response.text().await?;
            Err(NotificationError::SmsProvider(format!("Twilio error: {}", error_text)).into())
        }
    }

    fn provider_type(&self) -> SmsProviderType {
        SmsProviderType::Twilio
    }

    fn priority(&self) -> u32 {
        self.config.priority
    }

    fn is_available(&self) -> bool {
        self.config.enabled
    }
}

/// AWS SNS SMS provider
pub struct AwsSnsProvider {
    sns_client: Option<aws_sdk_sns::Client>,
    config: SmsProviderConfig,
    region: String,
}

impl AwsSnsProvider {
    pub fn new(config: SmsProviderConfig, _client: Client) -> Result<Self> {
        let region = match &config.credentials {
            SmsCredentials::AwsSns { region, .. } => region.clone(),
            _ => return Err(anyhow::anyhow!("Invalid credentials for AWS SNS")),
        };

        Ok(Self {
            sns_client: None, // Will be initialized lazily
            config,
            region,
        })
    }

    async fn get_sns_client(&self) -> Result<aws_sdk_sns::Client> {
        let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(self.region.clone()))
            .load()
            .await;

        Ok(aws_sdk_sns::Client::new(&aws_config))
    }
}

#[async_trait]
impl SmsProvider for AwsSnsProvider {
    async fn send_sms(
        &self,
        to: &str,
        message: &str,
        options: &SmsOptions,
    ) -> Result<String> {
        let sns_client = self.get_sns_client().await?;

        // Normalize phone number to E.164 format if needed
        let phone_number = if to.starts_with('+') {
            to.to_string()
        } else {
            // Assume US number if no country code
            format!("+1{}", to.chars().filter(|c| c.is_ascii_digit()).collect::<String>())
        };

        // Set SMS attributes for delivery
        let mut attributes = std::collections::HashMap::new();

        // Set SMS type (Promotional or Transactional)
        attributes.insert(
            "AWS.SNS.SMS.SMSType".to_string(),
            aws_sdk_sns::types::MessageAttributeValue::builder()
                .data_type("String")
                .string_value("Transactional") // Most notifications are transactional
                .build()?,
        );

        // Set sender ID if configured
        if let Some(from_number) = &options.from_number {
            attributes.insert(
                "AWS.SNS.SMS.SenderID".to_string(),
                aws_sdk_sns::types::MessageAttributeValue::builder()
                    .data_type("String")
                    .string_value(from_number)
                    .build()?,
            );
        }

        // Set max price to prevent unexpected charges (in USD)
        attributes.insert(
            "AWS.SNS.SMS.MaxPrice".to_string(),
            aws_sdk_sns::types::MessageAttributeValue::builder()
                .data_type("Number")
                .string_value("0.50") // $0.50 max per SMS
                .build()?,
        );

        // Send the SMS
        let result = sns_client
            .publish()
            .phone_number(&phone_number)
            .message(message)
            .set_message_attributes(Some(attributes))
            .send()
            .await?;

        let message_id = result.message_id()
            .unwrap_or("unknown")
            .to_string();

        Ok(message_id)
    }

    fn provider_type(&self) -> SmsProviderType {
        SmsProviderType::AwsSns
    }

    fn priority(&self) -> u32 {
        self.config.priority
    }

    fn is_available(&self) -> bool {
        self.config.enabled
    }
}



/// Twilio webhook response for delivery status
#[derive(Debug, Deserialize)]
pub struct TwilioWebhook {
    #[serde(rename = "MessageSid")]
    pub message_sid: String,
    #[serde(rename = "MessageStatus")]
    pub message_status: String,
    #[serde(rename = "To")]
    pub to: String,
    #[serde(rename = "From")]
    pub from: String,
    #[serde(rename = "ErrorCode")]
    pub error_code: Option<String>,
    #[serde(rename = "ErrorMessage")]
    pub error_message: Option<String>,
}