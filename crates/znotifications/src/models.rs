//! Data models for the notification system

use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Notification record stored in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationRecord {
    pub id: Ulid,
    pub org_id: u64,
    pub notification_type: NotificationType,
    pub template_id: String,
    pub recipient: String,
    pub status: NotificationStatus,
    pub created_at: i64,
    pub scheduled_at: Option<i64>,
    pub sent_at: Option<i64>,
    pub delivered_at: Option<i64>,
    pub failed_at: Option<i64>,
    pub error_message: Option<String>,
    pub retry_count: u32,
    pub metadata: serde_json::Value,
}

/// Type of notification
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NotificationType {
    Email,
    Sms,
}

/// Status of a notification
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NotificationStatus {
    Pending,
    Sent,
    Delivered,
    Failed,
    Bounced,
    Complained,
    Unsubscribed,
}

/// Notification template definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationTemplate {
    pub id: String,
    pub org_id: u64,
    pub name: String,
    pub description: Option<String>,
    pub notification_type: NotificationType,
    pub subject_template: String,
    pub html_body_template: Option<String>,
    pub text_body_template: Option<String>,
    pub variables: Vec<TemplateVariable>,
    pub active: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Template variable definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateVariable {
    pub name: String,
    pub variable_type: VariableType,
    pub description: Option<String>,
    pub required: bool,
    pub default_value: Option<String>,
}

/// Type of template variable
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum VariableType {
    String,
    Number,
    Boolean,
    Date,
    Array,
    Object,
}

/// Organization notification preferences
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrgNotificationPreferences {
    pub org_id: u64,
    pub email_enabled: bool,
    pub sms_enabled: bool,
    pub default_from_email: Option<String>,
    pub default_from_name: Option<String>,
    pub default_reply_to: Option<String>,
    pub default_from_number: Option<String>,
    pub rate_limits: RateLimits,
    pub bounce_handling: BounceHandling,
    pub unsubscribe_handling: UnsubscribeHandling,
    pub updated_at: i64,
}

/// Rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimits {
    pub emails_per_hour: Option<u32>,
    pub emails_per_day: Option<u32>,
    pub sms_per_hour: Option<u32>,
    pub sms_per_day: Option<u32>,
    pub per_recipient_per_hour: Option<u32>,
    pub per_recipient_per_day: Option<u32>,
}

/// Bounce handling configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BounceHandling {
    pub auto_suppress_hard_bounces: bool,
    pub max_soft_bounces: u32,
    pub suppress_after_complaints: u32,
    pub webhook_url: Option<String>,
}

/// Unsubscribe handling configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsubscribeHandling {
    pub auto_process_unsubscribes: bool,
    pub include_unsubscribe_header: bool,
    pub unsubscribe_url: Option<String>,
    pub webhook_url: Option<String>,
}

/// Delivery tracking record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryRecord {
    pub id: Ulid,
    pub notification_id: Ulid,
    pub provider_id: String,
    pub event_type: DeliveryEventType,
    pub timestamp: i64,
    pub raw_data: serde_json::Value,
    pub processed: bool,
}

/// Type of delivery event
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DeliveryEventType {
    Sent,
    Delivered,
    Bounced,
    Complained,
    Opened,
    Clicked,
    Unsubscribed,
    Failed,
}

/// Suppression list entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuppressionEntry {
    pub id: Ulid,
    pub org_id: u64,
    pub recipient: String,
    pub suppression_type: SuppressionType,
    pub reason: String,
    pub created_at: i64,
    pub expires_at: Option<i64>,
    pub active: bool,
}

/// Type of suppression
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SuppressionType {
    Bounce,
    Complaint,
    Unsubscribe,
    Manual,
}

/// Delivery statistics summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryStats {
    pub org_id: u64,
    pub date: String, // YYYY-MM-DD
    pub emails_sent: u64,
    pub emails_delivered: u64,
    pub emails_bounced: u64,
    pub emails_complained: u64,
    pub emails_opened: u64,
    pub emails_clicked: u64,
    pub sms_sent: u64,
    pub sms_delivered: u64,
    pub sms_failed: u64,
    pub updated_at: i64,
}

impl Default for RateLimits {
    fn default() -> Self {
        Self {
            emails_per_hour: Some(1000),
            emails_per_day: Some(10000),
            sms_per_hour: Some(100),
            sms_per_day: Some(1000),
            per_recipient_per_hour: Some(10),
            per_recipient_per_day: Some(50),
        }
    }
}

impl Default for BounceHandling {
    fn default() -> Self {
        Self {
            auto_suppress_hard_bounces: true,
            max_soft_bounces: 5,
            suppress_after_complaints: 1,
            webhook_url: None,
        }
    }
}

impl Default for UnsubscribeHandling {
    fn default() -> Self {
        Self {
            auto_process_unsubscribes: true,
            include_unsubscribe_header: true,
            unsubscribe_url: None,
            webhook_url: None,
        }
    }
}

impl Default for OrgNotificationPreferences {
    fn default() -> Self {
        Self {
            org_id: 0,
            email_enabled: true,
            sms_enabled: true,
            default_from_email: None,
            default_from_name: None,
            default_reply_to: None,
            default_from_number: None,
            rate_limits: RateLimits::default(),
            bounce_handling: BounceHandling::default(),
            unsubscribe_handling: UnsubscribeHandling::default(),
            updated_at: chrono::Utc::now().timestamp(),
        }
    }
}