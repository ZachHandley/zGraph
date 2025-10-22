//! # ZRUSTDB Notifications System
//!
//! Complete notification system for email and SMS delivery with multi-provider support,
//! template rendering, delivery tracking, and storage integration.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ulid::Ulid;
use zcore_storage::Store;

pub mod config;
pub mod email;
pub mod models;
pub mod providers;
pub mod repository;
pub mod sms;
pub mod templates;
pub mod tracking;

pub use config::*;
pub use email::EmailService;
pub use models::*;
pub use repository::NotificationRepository;
pub use sms::SmsService;
pub use templates::{TemplateEngine, TemplateContext, RenderedTemplate};

/// Main notification service that coordinates email and SMS delivery
#[derive(Clone)]
pub struct NotificationService {
    email_service: Arc<EmailService>,
    sms_service: Arc<SmsService>,
    template_engine: Arc<TemplateEngine>,
    repository: Arc<NotificationRepository>,
}

impl NotificationService {
    /// Create a new notification service with the given configuration
    pub fn new(config: NotificationConfig, store: &'static Store) -> Result<Self> {
        let email_service = Arc::new(EmailService::new(config.email)?);
        let sms_service = Arc::new(SmsService::new(config.sms)?);
        let template_engine = Arc::new(TemplateEngine::new()?);
        let repository = Arc::new(NotificationRepository::new(store));

        Ok(Self {
            email_service,
            sms_service,
            template_engine,
            repository,
        })
    }

    /// Send an email notification using a template
    pub async fn send_email_notification(
        &self,
        org_id: u64,
        template_id: &str,
        recipient: &str,
        context: TemplateContext,
        options: EmailOptions,
    ) -> Result<NotificationId> {
        // Check if recipient is suppressed
        if self.repository.is_suppressed(org_id, recipient)? {
            return Err(NotificationError::RecipientSuppressed(recipient.to_string()).into());
        }

        // Get the template
        let template = self.repository.get_template(org_id, template_id)?
            .ok_or_else(|| NotificationError::TemplateNotFound(template_id.to_string()))?;

        // Verify template is for email
        if template.notification_type != NotificationType::Email {
            return Err(NotificationError::InvalidTemplate(
                "Template is not for email".to_string()
            ).into());
        }

        // Render the template
        let rendered = self.template_engine.render(&template, &context)?;

        // Create notification record
        let notification_id = Ulid::new();
        let notification = NotificationRecord {
            id: notification_id,
            org_id,
            notification_type: NotificationType::Email,
            template_id: template_id.to_string(),
            recipient: recipient.to_string(),
            status: NotificationStatus::Pending,
            created_at: chrono::Utc::now().timestamp(),
            scheduled_at: options.scheduled_at,
            sent_at: None,
            delivered_at: None,
            failed_at: None,
            error_message: None,
            retry_count: 0,
            metadata: options.metadata.clone(),
        };

        // Store the notification
        self.repository.insert_notification(&notification)?;

        // Send the email
        match self.email_service.send_email(
            &rendered.subject,
            &rendered.html_body,
            rendered.text_body.as_deref(),
            recipient,
            &options,
        ).await {
            Ok(provider_id) => {
                // Update notification as sent
                self.repository.update_notification_sent(&notification_id, Some(provider_id))?;
                Ok(NotificationId(notification_id.to_string()))
            }
            Err(e) => {
                // Update notification as failed
                self.repository.update_notification_failed(&notification_id, &e.to_string())?;
                Err(e)
            }
        }
    }

    /// Send an SMS notification using a template
    pub async fn send_sms_notification(
        &self,
        org_id: u64,
        template_id: &str,
        phone_number: &str,
        context: TemplateContext,
        options: SmsOptions,
    ) -> Result<NotificationId> {
        // Check if recipient is suppressed
        if self.repository.is_suppressed(org_id, phone_number)? {
            return Err(NotificationError::RecipientSuppressed(phone_number.to_string()).into());
        }

        // Get the template
        let template = self.repository.get_template(org_id, template_id)?
            .ok_or_else(|| NotificationError::TemplateNotFound(template_id.to_string()))?;

        // Verify template is for SMS
        if template.notification_type != NotificationType::Sms {
            return Err(NotificationError::InvalidTemplate(
                "Template is not for SMS".to_string()
            ).into());
        }

        // Render the template
        let rendered = self.template_engine.render(&template, &context)?;

        // For SMS, use text body or subject as message
        let message = rendered.text_body.unwrap_or(rendered.subject);

        // Create notification record
        let notification_id = Ulid::new();
        let notification = NotificationRecord {
            id: notification_id,
            org_id,
            notification_type: NotificationType::Sms,
            template_id: template_id.to_string(),
            recipient: phone_number.to_string(),
            status: NotificationStatus::Pending,
            created_at: chrono::Utc::now().timestamp(),
            scheduled_at: options.scheduled_at,
            sent_at: None,
            delivered_at: None,
            failed_at: None,
            error_message: None,
            retry_count: 0,
            metadata: options.metadata.clone(),
        };

        // Store the notification
        self.repository.insert_notification(&notification)?;

        // Send the SMS
        match self.sms_service.send_sms(phone_number, &message, &options).await {
            Ok(provider_id) => {
                // Update notification as sent
                self.repository.update_notification_sent(&notification_id, Some(provider_id))?;
                Ok(NotificationId(notification_id.to_string()))
            }
            Err(e) => {
                // Update notification as failed
                self.repository.update_notification_failed(&notification_id, &e.to_string())?;
                Err(e)
            }
        }
    }

    /// Get notification by ID
    pub fn get_notification(&self, notification_id: &str) -> Result<Option<NotificationRecord>> {
        let id = Ulid::from_string(notification_id)?;
        self.repository.get_notification(&id)
    }

    /// List notifications for an organization
    pub fn list_notifications(
        &self,
        org_id: u64,
        filters: NotificationFilters,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<Vec<NotificationRecord>> {
        self.repository.list_notifications(org_id, filters, limit, offset)
    }

    /// Create or update a notification template
    pub fn upsert_template(&self, org_id: u64, template: &NotificationTemplate) -> Result<()> {
        self.repository.upsert_template(org_id, template)
    }

    /// Get a notification template
    pub fn get_template(&self, org_id: u64, template_id: &str) -> Result<Option<NotificationTemplate>> {
        self.repository.get_template(org_id, template_id)
    }

    /// List templates for an organization
    pub fn list_templates(&self, org_id: u64) -> Result<Vec<NotificationTemplate>> {
        self.repository.list_templates(org_id)
    }

    /// Delete a notification template
    pub fn delete_template(&self, org_id: u64, template_id: &str) -> Result<()> {
        self.repository.delete_template(org_id, template_id)
    }
}

/// Filters for listing notifications
#[derive(Debug, Clone, Default)]
pub struct NotificationFilters {
    pub notification_type: Option<NotificationType>,
    pub status: Option<NotificationStatus>,
    pub template_id: Option<String>,
    pub recipient: Option<String>,
    pub date_from: Option<i64>,
    pub date_to: Option<i64>,
}

/// Custom error types for the notification system
#[derive(Debug, thiserror::Error)]
pub enum NotificationError {
    #[error("Template not found: {0}")]
    TemplateNotFound(String),

    #[error("Invalid template: {0}")]
    InvalidTemplate(String),

    #[error("Email provider error: {0}")]
    EmailProvider(String),

    #[error("SMS provider error: {0}")]
    SmsProvider(String),

    #[error("Recipient suppressed: {0}")]
    RecipientSuppressed(String),

    #[error("Rate limit exceeded")]
    RateLimitExceeded,

    #[error("Configuration error: {0}")]
    Configuration(String),
}

/// Unique identifier for a notification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationId(pub String);

/// Email priority levels
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum EmailPriority {
    Low,
    Normal,
    High,
    Critical,
}

/// Options for email delivery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailOptions {
    pub from_name: Option<String>,
    pub from_email: Option<String>,
    pub reply_to: Option<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub priority: EmailPriority,
    pub scheduled_at: Option<i64>,
    pub track_opens: bool,
    pub track_clicks: bool,
    pub tags: Vec<String>,
    pub metadata: serde_json::Value,
}

impl Default for EmailOptions {
    fn default() -> Self {
        Self {
            from_name: None,
            from_email: None,
            reply_to: None,
            cc: Vec::new(),
            bcc: Vec::new(),
            priority: EmailPriority::Normal,
            scheduled_at: None,
            track_opens: false,
            track_clicks: false,
            tags: Vec::new(),
            metadata: serde_json::Value::Null,
        }
    }
}

/// Options for SMS delivery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmsOptions {
    pub from_number: Option<String>,
    pub scheduled_at: Option<i64>,
    pub delivery_receipt: bool,
    pub tags: Vec<String>,
    pub metadata: serde_json::Value,
}

impl Default for SmsOptions {
    fn default() -> Self {
        Self {
            from_number: None,
            scheduled_at: None,
            delivery_receipt: false,
            tags: Vec::new(),
            metadata: serde_json::Value::Null,
        }
    }
}