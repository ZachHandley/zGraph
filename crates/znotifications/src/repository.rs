//! Database repository for notification data

use anyhow::Result;
use ulid::Ulid;
use zcore_storage::Store;

use crate::models::*;
use crate::NotificationFilters;

// Collection names
const NOTIFICATIONS_COLLECTION: &str = "notifications";
const TEMPLATES_COLLECTION: &str = "templates";
const ORG_PREFERENCES_COLLECTION: &str = "org_preferences";
const DELIVERY_EVENTS_COLLECTION: &str = "delivery_events";
const SUPPRESSIONS_COLLECTION: &str = "suppressions";
const DELIVERY_STATS_COLLECTION: &str = "delivery_stats";

/// Repository for notification data operations
pub struct NotificationRepository {
    store: &'static Store,
}

impl NotificationRepository {
    /// Create a new notification repository
    pub fn new(store: &'static Store) -> Self {
        Self { store }
    }

    /// Insert a new notification record
    pub fn insert_notification(&self, notification: &NotificationRecord) -> Result<()> {
        let key = notification_key(&notification.id);
        let value = bincode::serialize(notification)?;

        let mut write_txn = self.store.begin_write()?;
        write_txn.set(NOTIFICATIONS_COLLECTION, key, value);
        write_txn.commit(self.store)?;
        Ok(())
    }

    /// Get a notification by ID
    pub fn get_notification(&self, id: &Ulid) -> Result<Option<NotificationRecord>> {
        let key = notification_key(id);
        let read_txn = self.store.begin_read()?;

        if let Some(value) = read_txn.get(self.store, NOTIFICATIONS_COLLECTION, &key)? {
            let notification: NotificationRecord = bincode::deserialize(&value)?;
            Ok(Some(notification))
        } else {
            Ok(None)
        }
    }

    /// Update notification status to sent
    pub fn update_notification_sent(&self, id: &Ulid, provider_id: Option<String>) -> Result<()> {
        let key = notification_key(id);
        let mut write_txn = self.store.begin_write()?;

        if let Some(existing_data) = write_txn.get(self.store, NOTIFICATIONS_COLLECTION, &key)? {
            let mut notification: NotificationRecord = bincode::deserialize(&existing_data)?;
            notification.status = NotificationStatus::Sent;
            notification.sent_at = Some(chrono::Utc::now().timestamp());

            if let Some(provider_id) = provider_id {
                notification.metadata = serde_json::json!({
                    "provider_id": provider_id
                });
            }

            let serialized = bincode::serialize(&notification)?;
            write_txn.set(NOTIFICATIONS_COLLECTION, key, serialized);
        }

        write_txn.commit(self.store)?;
        Ok(())
    }

    /// Update notification status to failed
    pub fn update_notification_failed(&self, id: &Ulid, error_message: &str) -> Result<()> {
        let key = notification_key(id);
        let mut write_txn = self.store.begin_write()?;

        if let Some(existing_data) = write_txn.get(self.store, NOTIFICATIONS_COLLECTION, &key)? {
            let mut notification: NotificationRecord = bincode::deserialize(&existing_data)?;
            notification.status = NotificationStatus::Failed;
            notification.failed_at = Some(chrono::Utc::now().timestamp());
            notification.error_message = Some(error_message.to_string());
            notification.retry_count += 1;

            let serialized = bincode::serialize(&notification)?;
            write_txn.set(NOTIFICATIONS_COLLECTION, key, serialized);
        }

        write_txn.commit(self.store)?;
        Ok(())
    }

    /// List notifications for an organization with filters
    pub fn list_notifications(
        &self,
        org_id: u64,
        filters: NotificationFilters,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<Vec<NotificationRecord>> {
        let read_txn = self.store.begin_read()?;

        // Get all notifications with prefix scan
        let prefix = format!("notification:");
        let results = read_txn.scan_prefix(self.store, NOTIFICATIONS_COLLECTION, prefix.as_bytes())?;
        let mut notifications = Vec::new();

        let skip = offset.unwrap_or(0) as usize;
        let take = limit.unwrap_or(100) as usize;
        let mut count = 0;

        for (_, value) in results {
            let notification: NotificationRecord = bincode::deserialize(&value)?;

            // Filter by org_id
            if notification.org_id != org_id {
                continue;
            }

            // Apply filters
            if let Some(notification_type) = filters.notification_type {
                if notification.notification_type != notification_type {
                    continue;
                }
            }

            if let Some(status) = filters.status {
                if notification.status != status {
                    continue;
                }
            }

            if let Some(ref template_id) = filters.template_id {
                if notification.template_id != *template_id {
                    continue;
                }
            }

            if let Some(ref recipient) = filters.recipient {
                if notification.recipient != *recipient {
                    continue;
                }
            }

            if let Some(date_from) = filters.date_from {
                if notification.created_at < date_from {
                    continue;
                }
            }

            if let Some(date_to) = filters.date_to {
                if notification.created_at > date_to {
                    continue;
                }
            }

            // Skip and take logic
            if count < skip {
                count += 1;
                continue;
            }

            if notifications.len() >= take {
                break;
            }

            notifications.push(notification);
            count += 1;
        }

        Ok(notifications)
    }

    /// Insert or update a notification template
    pub fn upsert_template(&self, org_id: u64, template: &NotificationTemplate) -> Result<()> {
        let key = template_key(org_id, &template.id);
        let value = bincode::serialize(template)?;

        let mut write_txn = self.store.begin_write()?;
        write_txn.set(TEMPLATES_COLLECTION, key, value);
        write_txn.commit(self.store)?;
        Ok(())
    }

    /// Get a notification template
    pub fn get_template(&self, org_id: u64, template_id: &str) -> Result<Option<NotificationTemplate>> {
        let key = template_key(org_id, template_id);
        let read_txn = self.store.begin_read()?;

        if let Some(value) = read_txn.get(self.store, TEMPLATES_COLLECTION, &key)? {
            let template: NotificationTemplate = bincode::deserialize(&value)?;
            Ok(Some(template))
        } else {
            Ok(None)
        }
    }

    /// Delete a notification template
    pub fn delete_template(&self, org_id: u64, template_id: &str) -> Result<()> {
        let key = template_key(org_id, template_id);
        let mut write_txn = self.store.begin_write()?;
        write_txn.delete(TEMPLATES_COLLECTION, key);
        write_txn.commit(self.store)?;
        Ok(())
    }

    /// List templates for an organization
    pub fn list_templates(&self, org_id: u64) -> Result<Vec<NotificationTemplate>> {
        let read_txn = self.store.begin_read()?;
        let org_prefix = org_id.to_be_bytes();
        let results = read_txn.scan_prefix(self.store, TEMPLATES_COLLECTION, &org_prefix)?;

        let mut templates = Vec::new();
        for (_, value) in results {
            let template: NotificationTemplate = bincode::deserialize(&value)?;
            templates.push(template);
        }

        Ok(templates)
    }

    /// Get organization notification preferences
    pub fn get_org_preferences(&self, org_id: u64) -> Result<Option<OrgNotificationPreferences>> {
        let key = org_id.to_be_bytes().to_vec();
        let read_txn = self.store.begin_read()?;

        if let Some(value) = read_txn.get(self.store, ORG_PREFERENCES_COLLECTION, &key)? {
            let preferences: OrgNotificationPreferences = bincode::deserialize(&value)?;
            Ok(Some(preferences))
        } else {
            Ok(None)
        }
    }

    /// Update organization notification preferences
    pub fn update_org_preferences(&self, org_id: u64, preferences: &OrgNotificationPreferences) -> Result<()> {
        let key = org_id.to_be_bytes().to_vec();
        let value = bincode::serialize(preferences)?;

        let mut write_txn = self.store.begin_write()?;
        write_txn.set(ORG_PREFERENCES_COLLECTION, key, value);
        write_txn.commit(self.store)?;
        Ok(())
    }

    /// Insert a delivery event
    pub fn insert_delivery_event(&self, event: &DeliveryRecord) -> Result<()> {
        let key = delivery_event_key(&event.id);
        let value = bincode::serialize(event)?;

        let mut write_txn = self.store.begin_write()?;
        write_txn.set(DELIVERY_EVENTS_COLLECTION, key, value);
        write_txn.commit(self.store)?;
        Ok(())
    }

    /// Insert a suppression entry
    pub fn insert_suppression(&self, suppression: &SuppressionEntry) -> Result<()> {
        let key = suppression_key(&suppression.id);
        let value = bincode::serialize(suppression)?;

        let mut write_txn = self.store.begin_write()?;
        write_txn.set(SUPPRESSIONS_COLLECTION, key, value);
        write_txn.commit(self.store)?;
        Ok(())
    }

    /// Check if a recipient is suppressed
    pub fn is_suppressed(&self, org_id: u64, recipient: &str) -> Result<bool> {
        let read_txn = self.store.begin_read()?;
        let results = read_txn.scan_prefix(self.store, SUPPRESSIONS_COLLECTION, b"")?;

        for (_, value) in results {
            let suppression: SuppressionEntry = bincode::deserialize(&value)?;

            if suppression.org_id == org_id
                && suppression.recipient == recipient
                && suppression.active
            {
                // Check if suppression has expired
                if let Some(expires_at) = suppression.expires_at {
                    if chrono::Utc::now().timestamp() > expires_at {
                        continue;
                    }
                }
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Update delivery statistics
    pub fn update_delivery_stats(&self, stats: &DeliveryStats) -> Result<()> {
        let key = format!("{}:{}", stats.org_id, stats.date);
        let value = bincode::serialize(stats)?;

        let mut write_txn = self.store.begin_write()?;
        write_txn.set(DELIVERY_STATS_COLLECTION, key.as_bytes().to_vec(), value);
        write_txn.commit(self.store)?;
        Ok(())
    }
}

/// Generate notification key for storage
fn notification_key(id: &Ulid) -> Vec<u8> {
    id.to_bytes().to_vec()
}

/// Generate template key for storage
fn template_key(org_id: u64, template_id: &str) -> Vec<u8> {
    let mut key = Vec::new();
    key.extend_from_slice(&org_id.to_be_bytes());
    key.extend_from_slice(template_id.as_bytes());
    key
}

/// Generate delivery event key for storage
fn delivery_event_key(id: &Ulid) -> Vec<u8> {
    id.to_bytes().to_vec()
}

/// Generate suppression key for storage
fn suppression_key(id: &Ulid) -> Vec<u8> {
    id.to_bytes().to_vec()
}