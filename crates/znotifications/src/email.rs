//! Email service implementation with multiple provider support

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use lettre::{
    message::{header::ContentType, Mailbox, Message},
    transport::smtp::{
        authentication::Credentials,
        SmtpTransport,
        client::Tls,
    },
    Transport,
};
use reqwest::Client;
use serde_json::json;
use tokio::time::sleep;
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::{EmailConfig, EmailCredentials, EmailProviderConfig, EmailProviderType, RetryConfig};
use crate::{EmailOptions, NotificationError};

/// Email service that manages multiple providers and handles failover
#[derive(Clone)]
pub struct EmailService {
    providers: HashMap<EmailProviderType, Arc<dyn EmailProvider>>,
    default_provider: EmailProviderType,
    fallback_enabled: bool,
    retry_config: RetryConfig,
    http_client: Client,
}

impl EmailService {
    /// Create a new email service with the given configuration
    pub fn new(config: EmailConfig) -> Result<Self> {
        let mut providers: HashMap<EmailProviderType, Arc<dyn EmailProvider>> = HashMap::new();
        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;

        for (provider_type, provider_config) in config.providers {
            if !provider_config.enabled {
                continue;
            }

            let provider: Arc<dyn EmailProvider> = match provider_type {
                EmailProviderType::SendGrid => {
                    Arc::new(SendGridProvider::new(provider_config, http_client.clone())?)
                }
                EmailProviderType::AwsSes => {
                    Arc::new(AwsSesProvider::new(provider_config, http_client.clone())?)
                }
                EmailProviderType::Mailgun => {
                    Arc::new(MailgunProvider::new(provider_config, http_client.clone())?)
                }
                EmailProviderType::Smtp => {
                    Arc::new(SmtpProvider::new(provider_config)?)
                }
            };

            providers.insert(provider_type, provider);
        }

        if providers.is_empty() {
            return Err(anyhow::anyhow!("No email providers available"));
        }

        Ok(Self {
            providers,
            default_provider: config.default_provider,
            fallback_enabled: config.fallback_enabled,
            retry_config: config.retry_config,
            http_client,
        })
    }

    /// Send an email using the configured providers with fallback support
    pub async fn send_email(
        &self,
        subject: &str,
        html_body: &str,
        text_body: Option<&str>,
        to: &str,
        options: &EmailOptions,
    ) -> Result<String> {
        let providers_to_try = if self.fallback_enabled {
            // Try default provider first, then fallbacks in priority order
            let mut ordered: Vec<_> = self.providers.keys().copied().collect();
            ordered.sort_by_key(|&provider_type| {
                if provider_type == self.default_provider {
                    0 // Highest priority
                } else {
                    // Use provider config priority for others
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
                match self.send_with_retry(
                    provider.as_ref(),
                    subject,
                    html_body,
                    text_body,
                    to,
                    options,
                ).await {
                    Ok(message_id) => {
                        info!("Email sent successfully via {:?}: {}", provider_type, message_id);
                        return Ok(message_id);
                    }
                    Err(e) => {
                        warn!("Email failed via {:?}: {}", provider_type, e);
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
        provider: &dyn EmailProvider,
        subject: &str,
        html_body: &str,
        text_body: Option<&str>,
        to: &str,
        options: &EmailOptions,
    ) -> Result<String> {
        let mut last_error = None;
        let mut delay_ms = self.retry_config.initial_delay_ms;

        for attempt in 0..=self.retry_config.max_retries {
            match provider.send_email(subject, html_body, text_body, to, options).await {
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

/// Trait for email providers
#[async_trait]
pub trait EmailProvider: Send + Sync {
    async fn send_email(
        &self,
        subject: &str,
        html_body: &str,
        text_body: Option<&str>,
        to: &str,
        options: &EmailOptions,
    ) -> Result<String>;

    fn provider_type(&self) -> EmailProviderType;
    fn priority(&self) -> u32;
    fn is_available(&self) -> bool;
}

/// SendGrid email provider
pub struct SendGridProvider {
    api_key: String,
    config: EmailProviderConfig,
    client: Client,
}

impl SendGridProvider {
    pub fn new(config: EmailProviderConfig, client: Client) -> Result<Self> {
        let api_key = match &config.credentials {
            EmailCredentials::SendGrid { api_key } => api_key.clone(),
            _ => return Err(anyhow::anyhow!("Invalid credentials for SendGrid")),
        };

        Ok(Self {
            api_key,
            config,
            client,
        })
    }
}

#[async_trait]
impl EmailProvider for SendGridProvider {
    async fn send_email(
        &self,
        subject: &str,
        html_body: &str,
        text_body: Option<&str>,
        to: &str,
        options: &EmailOptions,
    ) -> Result<String> {
        let from_email = options.from_email.as_deref()
            .or(self.config.settings.default_from_email.as_deref())
            .ok_or_else(|| anyhow::anyhow!("No from email configured"))?;

        let from_name = options.from_name.as_deref()
            .or(self.config.settings.default_from_name.as_deref());

        let mut content = vec![
            json!({
                "type": "text/html",
                "value": html_body
            })
        ];

        if let Some(text) = text_body {
            content.insert(0, json!({
                "type": "text/plain",
                "value": text
            }));
        }

        let payload = json!({
            "personalizations": [{
                "to": [{"email": to}],
                "subject": subject
            }],
            "from": {
                "email": from_email,
                "name": from_name
            },
            "content": content,
            "tracking_settings": {
                "click_tracking": {
                    "enable": options.track_clicks || self.config.settings.track_clicks
                },
                "open_tracking": {
                    "enable": options.track_opens || self.config.settings.track_opens
                }
            }
        });

        let response = self.client
            .post("https://api.sendgrid.com/v3/mail/send")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if response.status().is_success() {
            // SendGrid returns the message ID in the X-Message-Id header
            let message_id = response
                .headers()
                .get("X-Message-Id")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("unknown")
                .to_string();
            Ok(message_id)
        } else {
            let error_text = response.text().await?;
            Err(NotificationError::EmailProvider(format!("SendGrid error: {}", error_text)).into())
        }
    }

    fn provider_type(&self) -> EmailProviderType {
        EmailProviderType::SendGrid
    }

    fn priority(&self) -> u32 {
        self.config.priority
    }

    fn is_available(&self) -> bool {
        self.config.enabled
    }
}

/// AWS SES email provider
pub struct AwsSesProvider {
    ses_client: Option<aws_sdk_ses::Client>,
    config: EmailProviderConfig,
    region: String,
}

impl AwsSesProvider {
    pub fn new(config: EmailProviderConfig, _client: Client) -> Result<Self> {
        let region = match &config.credentials {
            EmailCredentials::AwsSes { region, .. } => region.clone(),
            _ => return Err(anyhow::anyhow!("Invalid credentials for AWS SES")),
        };

        Ok(Self {
            ses_client: None, // Will be initialized lazily
            config,
            region,
        })
    }

    async fn get_ses_client(&self) -> Result<aws_sdk_ses::Client> {
        let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(self.region.clone()))
            .load()
            .await;

        Ok(aws_sdk_ses::Client::new(&aws_config))
    }
}

#[async_trait]
impl EmailProvider for AwsSesProvider {
    async fn send_email(
        &self,
        subject: &str,
        html_body: &str,
        text_body: Option<&str>,
        to: &str,
        options: &EmailOptions,
    ) -> Result<String> {
        let ses_client = self.get_ses_client().await?;

        let from_email = options.from_email.as_deref()
            .or(self.config.settings.default_from_email.as_deref())
            .ok_or_else(|| anyhow::anyhow!("No from email configured"))?;

        let source = match &options.from_name {
            Some(name) => format!("{} <{}>", name, from_email),
            None => from_email.to_string(),
        };

        // Build destination
        let destination = aws_sdk_ses::types::Destination::builder()
            .to_addresses(to)
            .build();

        // Build message content
        let mut content_builder = aws_sdk_ses::types::Content::builder()
            .data(html_body)
            .charset("UTF-8");

        let html_content = content_builder.build()?;

        let mut body_builder = aws_sdk_ses::types::Body::builder()
            .html(html_content);

        // Add text content if provided
        if let Some(text) = text_body {
            let text_content = aws_sdk_ses::types::Content::builder()
                .data(text)
                .charset("UTF-8")
                .build()?;
            body_builder = body_builder.text(text_content);
        }

        let body = body_builder.build();

        // Build subject
        let subject_content = aws_sdk_ses::types::Content::builder()
            .data(subject)
            .charset("UTF-8")
            .build()?;

        // Build message
        let message = aws_sdk_ses::types::Message::builder()
            .subject(subject_content)
            .body(body)
            .build();

        // Add reply-to if specified
        let mut send_email_builder = ses_client
            .send_email()
            .source(&source)
            .destination(destination)
            .message(message);

        if let Some(reply_to) = &options.reply_to {
            send_email_builder = send_email_builder.reply_to_addresses(reply_to);
        }

        // Send the email
        let result = send_email_builder.send().await?;

        let message_id = result.message_id().to_string();

        Ok(message_id)
    }

    fn provider_type(&self) -> EmailProviderType {
        EmailProviderType::AwsSes
    }

    fn priority(&self) -> u32 {
        self.config.priority
    }

    fn is_available(&self) -> bool {
        self.config.enabled
    }
}

/// Mailgun email provider
pub struct MailgunProvider {
    api_key: String,
    domain: String,
    base_url: String,
    config: EmailProviderConfig,
    client: Client,
}

impl MailgunProvider {
    pub fn new(config: EmailProviderConfig, client: Client) -> Result<Self> {
        let (api_key, domain, base_url) = match &config.credentials {
            EmailCredentials::Mailgun { api_key, domain, base_url } => {
                let base_url = base_url.as_deref().unwrap_or("https://api.mailgun.net").to_string();
                (api_key.clone(), domain.clone(), base_url)
            }
            _ => return Err(anyhow::anyhow!("Invalid credentials for Mailgun")),
        };

        Ok(Self {
            api_key,
            domain,
            base_url,
            config,
            client,
        })
    }
}

#[async_trait]
impl EmailProvider for MailgunProvider {
    async fn send_email(
        &self,
        subject: &str,
        html_body: &str,
        text_body: Option<&str>,
        to: &str,
        options: &EmailOptions,
    ) -> Result<String> {
        let from_email = options.from_email.as_deref()
            .or(self.config.settings.default_from_email.as_deref())
            .ok_or_else(|| anyhow::anyhow!("No from email configured"))?;

        let from = match &options.from_name {
            Some(name) => format!("{} <{}>", name, from_email),
            None => from_email.to_string(),
        };

        let mut form_data = vec![
            ("from", from.as_str()),
            ("to", to),
            ("subject", subject),
            ("html", html_body),
        ];

        if let Some(text) = text_body {
            form_data.push(("text", text));
        }

        if options.track_opens || self.config.settings.track_opens {
            form_data.push(("o:tracking-opens", "yes"));
        }

        if options.track_clicks || self.config.settings.track_clicks {
            form_data.push(("o:tracking-clicks", "yes"));
        }

        let url = format!("{}/v3/{}/messages", self.base_url, self.domain);
        let response = self.client
            .post(&url)
            .basic_auth("api", Some(&self.api_key))
            .form(&form_data)
            .send()
            .await?;

        if response.status().is_success() {
            let response_data: serde_json::Value = response.json().await?;
            let message_id = response_data["id"].as_str().unwrap_or("unknown").to_string();
            Ok(message_id)
        } else {
            let error_text = response.text().await?;
            Err(NotificationError::EmailProvider(format!("Mailgun error: {}", error_text)).into())
        }
    }

    fn provider_type(&self) -> EmailProviderType {
        EmailProviderType::Mailgun
    }

    fn priority(&self) -> u32 {
        self.config.priority
    }

    fn is_available(&self) -> bool {
        self.config.enabled
    }
}

/// SMTP email provider
pub struct SmtpProvider {
    transport: SmtpTransport,
    config: EmailProviderConfig,
}

impl SmtpProvider {
    pub fn new(config: EmailProviderConfig) -> Result<Self> {
        let (host, port, username, password, use_tls, use_starttls) = match &config.credentials {
            EmailCredentials::Smtp { host, port, username, password, use_tls, use_starttls } => {
                (host.clone(), *port, username.clone(), password.clone(), *use_tls, *use_starttls)
            }
            _ => return Err(anyhow::anyhow!("Invalid credentials for SMTP")),
        };

        let mut builder = SmtpTransport::relay(&host)?;

        if use_tls {
            builder = builder.tls(Tls::Required(lettre::transport::smtp::client::TlsParameters::new(host.clone()).unwrap_or_else(|_| lettre::transport::smtp::client::TlsParameters::new_native(host.clone()).unwrap())));
        } else if use_starttls {
            builder = builder.tls(Tls::Opportunistic(lettre::transport::smtp::client::TlsParameters::new(host.clone()).unwrap_or_else(|_| lettre::transport::smtp::client::TlsParameters::new_native(host.clone()).unwrap())));
        } else {
            builder = builder.tls(Tls::None);
        }

        let transport = builder
            .credentials(Credentials::new(username, password))
            .port(port)
            .build();

        Ok(Self {
            transport,
            config,
        })
    }
}

#[async_trait]
impl EmailProvider for SmtpProvider {
    async fn send_email(
        &self,
        subject: &str,
        html_body: &str,
        text_body: Option<&str>,
        to: &str,
        options: &EmailOptions,
    ) -> Result<String> {
        let from_email = options.from_email.as_deref()
            .or(self.config.settings.default_from_email.as_deref())
            .ok_or_else(|| anyhow::anyhow!("No from email configured"))?;

        let from_mailbox: Mailbox = match &options.from_name {
            Some(name) => format!("{} <{}>", name, from_email).parse()?,
            None => from_email.parse()?,
        };

        let to_mailbox: Mailbox = to.parse()?;

        let mut message_builder = Message::builder()
            .from(from_mailbox)
            .to(to_mailbox)
            .subject(subject);

        if let Some(reply_to) = &options.reply_to {
            message_builder = message_builder.reply_to(reply_to.parse()?);
        }

        let message = if let Some(text) = text_body {
            message_builder
                .multipart(
                    lettre::message::MultiPart::alternative()
                        .singlepart(
                            lettre::message::SinglePart::builder()
                                .header(ContentType::TEXT_PLAIN)
                                .body(text.to_string())
                        )
                        .singlepart(
                            lettre::message::SinglePart::builder()
                                .header(ContentType::TEXT_HTML)
                                .body(html_body.to_string())
                        )
                )?
        } else {
            message_builder
                .header(ContentType::TEXT_HTML)
                .body(html_body.to_string())?
        };

        let response = self.transport.send(&message)?;
        // Generate a unique message ID for tracking
        let message_id = Uuid::new_v4().to_string();
        Ok(message_id)
    }

    fn provider_type(&self) -> EmailProviderType {
        EmailProviderType::Smtp
    }

    fn priority(&self) -> u32 {
        self.config.priority
    }

    fn is_available(&self) -> bool {
        self.config.enabled
    }
}

