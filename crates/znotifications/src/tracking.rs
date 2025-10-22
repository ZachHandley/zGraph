//! Delivery tracking and analytics for notifications

use anyhow::Result;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::models::{DeliveryEventType, DeliveryRecord, DeliveryStats, NotificationStatus};
use crate::repository::NotificationRepository;

/// Service for tracking notification delivery and generating analytics
pub struct DeliveryTracker {
    repository: NotificationRepository,
}

impl DeliveryTracker {
    /// Create a new delivery tracker
    pub fn new(repository: NotificationRepository) -> Self {
        Self { repository }
    }

    /// Record a delivery event
    pub fn record_event(
        &self,
        notification_id: Ulid,
        event_type: DeliveryEventType,
        provider_id: String,
        raw_data: serde_json::Value,
    ) -> Result<()> {
        let event = DeliveryRecord {
            id: Ulid::new(),
            notification_id,
            provider_id,
            event_type,
            timestamp: chrono::Utc::now().timestamp(),
            raw_data,
            processed: false,
        };

        self.repository.insert_delivery_event(&event)?;

        // Update notification status based on event type
        self.update_notification_from_event(&notification_id, event_type)?;

        Ok(())
    }

    /// Update notification status based on delivery event
    fn update_notification_from_event(
        &self,
        notification_id: &Ulid,
        event_type: DeliveryEventType,
    ) -> Result<()> {
        if let Some(mut notification) = self.repository.get_notification(notification_id)? {
            match event_type {
                DeliveryEventType::Delivered => {
                    notification.status = NotificationStatus::Delivered;
                    notification.delivered_at = Some(chrono::Utc::now().timestamp());
                }
                DeliveryEventType::Bounced => {
                    notification.status = NotificationStatus::Bounced;
                    notification.failed_at = Some(chrono::Utc::now().timestamp());
                }
                DeliveryEventType::Complained => {
                    notification.status = NotificationStatus::Complained;
                }
                DeliveryEventType::Unsubscribed => {
                    notification.status = NotificationStatus::Unsubscribed;
                }
                DeliveryEventType::Failed => {
                    notification.status = NotificationStatus::Failed;
                    notification.failed_at = Some(chrono::Utc::now().timestamp());
                }
                _ => {
                    // Don't update status for opened/clicked events
                    return Ok(());
                }
            }

            // TODO: Update notification in repository
            // self.repository.update_notification(&notification)?;
        }

        Ok(())
    }

    /// Generate delivery statistics for a date range
    pub fn generate_stats(
        &self,
        org_id: u64,
        date_from: i64,
        date_to: i64,
    ) -> Result<Vec<DeliveryStats>> {
        // TODO: Implement statistics aggregation
        // This would query the delivery events and aggregate by date
        Ok(Vec::new())
    }

    /// Get delivery rate for an organization
    pub fn get_delivery_rate(&self, org_id: u64, days: u32) -> Result<DeliveryRate> {
        // TODO: Implement delivery rate calculation
        Ok(DeliveryRate {
            total_sent: 0,
            total_delivered: 0,
            delivery_rate: 0.0,
            bounce_rate: 0.0,
            complaint_rate: 0.0,
        })
    }
}

/// Delivery status enumeration
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DeliveryStatus {
    Pending,
    Sent,
    Delivered,
    Bounced,
    Failed,
    Complained,
    Opened,
    Clicked,
    Unsubscribed,
}

/// Delivery event for webhook processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryEvent {
    pub notification_id: Ulid,
    pub event_type: DeliveryEventType,
    pub timestamp: i64,
    pub provider_id: String,
    pub recipient: String,
    pub reason: Option<String>,
    pub metadata: serde_json::Value,
}

/// Delivery rate metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryRate {
    pub total_sent: u64,
    pub total_delivered: u64,
    pub delivery_rate: f64,
    pub bounce_rate: f64,
    pub complaint_rate: f64,
}

impl DeliveryRate {
    /// Calculate rates from raw counts
    pub fn calculate(
        sent: u64,
        delivered: u64,
        bounced: u64,
        complained: u64,
    ) -> Self {
        let delivery_rate = if sent > 0 {
            delivered as f64 / sent as f64 * 100.0
        } else {
            0.0
        };

        let bounce_rate = if sent > 0 {
            bounced as f64 / sent as f64 * 100.0
        } else {
            0.0
        };

        let complaint_rate = if sent > 0 {
            complained as f64 / sent as f64 * 100.0
        } else {
            0.0
        };

        Self {
            total_sent: sent,
            total_delivered: delivered,
            delivery_rate,
            bounce_rate,
            complaint_rate,
        }
    }
}

/// Webhook payload parsers for different providers
pub mod webhook_parsers {
    use super::*;
    use serde_json::Value;

    /// Parse SendGrid webhook payload
    pub fn parse_sendgrid_webhook(payload: Value) -> Result<Vec<DeliveryEvent>> {
        let events = payload.as_array()
            .ok_or_else(|| anyhow::anyhow!("SendGrid payload must be an array"))?;

        let mut delivery_events = Vec::new();

        for event in events {
            if let Some(event_type) = event.get("event").and_then(|v| v.as_str()) {
                let delivery_event_type = match event_type {
                    "delivered" => DeliveryEventType::Delivered,
                    "bounce" | "blocked" => DeliveryEventType::Bounced,
                    "spamreport" => DeliveryEventType::Complained,
                    "unsubscribe" => DeliveryEventType::Unsubscribed,
                    "open" => DeliveryEventType::Opened,
                    "click" => DeliveryEventType::Clicked,
                    "dropped" | "deferred" => DeliveryEventType::Failed,
                    _ => continue,
                };

                // Extract notification ID from custom headers or message ID
                // This requires setting up the message ID mapping during send
                if let Some(message_id) = event.get("sg_message_id").and_then(|v| v.as_str()) {
                    // Parse notification ID from message ID
                    // Implementation depends on how we encode the notification ID
                    if let Ok(notification_id) = Ulid::from_string(message_id) {
                        delivery_events.push(DeliveryEvent {
                            notification_id,
                            event_type: delivery_event_type,
                            timestamp: event.get("timestamp")
                                .and_then(|v| v.as_i64())
                                .unwrap_or_else(|| chrono::Utc::now().timestamp()),
                            provider_id: message_id.to_string(),
                            recipient: event.get("email")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            reason: event.get("reason").and_then(|v| v.as_str()).map(String::from),
                            metadata: event.clone(),
                        });
                    }
                }
            }
        }

        Ok(delivery_events)
    }

    /// Parse Twilio webhook payload
    pub fn parse_twilio_webhook(payload: Value) -> Result<Vec<DeliveryEvent>> {
        let mut delivery_events = Vec::new();

        if let Some(status) = payload.get("MessageStatus").and_then(|v| v.as_str()) {
            let event_type = match status {
                "delivered" => DeliveryEventType::Delivered,
                "failed" | "undelivered" => DeliveryEventType::Failed,
                _ => return Ok(delivery_events),
            };

            if let Some(message_sid) = payload.get("MessageSid").and_then(|v| v.as_str()) {
                // Extract notification ID from message SID mapping
                // This requires maintaining a mapping table
                delivery_events.push(DeliveryEvent {
                    notification_id: Ulid::new(), // TODO: Lookup from mapping
                    event_type,
                    timestamp: chrono::Utc::now().timestamp(),
                    provider_id: message_sid.to_string(),
                    recipient: payload.get("To")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    reason: payload.get("ErrorCode").and_then(|v| v.as_str()).map(String::from),
                    metadata: payload,
                });
            }
        }

        Ok(delivery_events)
    }
}