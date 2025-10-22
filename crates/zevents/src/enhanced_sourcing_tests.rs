//! Enhanced Event Sourcing Tests for Replay and Snapshots

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use uuid::Uuid;

use crate::{
    Event, EventType, EventData, EventStore, EventStream, Snapshot,
    EventFilter, EventMetadata,
};

/// Test aggregate for event sourcing scenarios
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct BankAccount {
    account_id: Uuid,
    owner_name: String,
    balance: i64, // in cents
    is_active: bool,
    transaction_count: u32,
    last_transaction_time: Option<i64>,
}

impl BankAccount {
    fn new(account_id: Uuid, owner_name: String, initial_balance: i64) -> Self {
        Self {
            account_id,
            owner_name,
            balance: initial_balance,
            is_active: true,
            transaction_count: 0,
            last_transaction_time: None,
        }
    }

    fn apply_event(&mut self, event: &Event) -> Result<()> {
        match &event.data {
            EventData::Custom(data) => {
                match data.get("event_type").and_then(|v| v.as_str()) {
                    Some("account_created") => {
                        // Already handled in constructor
                    }
                    Some("deposit") => {
                        let amount = data.get("amount")
                            .and_then(|v| v.as_i64())
                            .ok_or_else(|| anyhow::anyhow!("Invalid deposit amount"))?;
                        self.balance += amount;
                        self.transaction_count += 1;
                        self.last_transaction_time = Some(event.timestamp.timestamp());
                    }
                    Some("withdrawal") => {
                        let amount = data.get("amount")
                            .and_then(|v| v.as_i64())
                            .ok_or_else(|| anyhow::anyhow!("Invalid withdrawal amount"))?;
                        if self.balance >= amount {
                            self.balance -= amount;
                            self.transaction_count += 1;
                            self.last_transaction_time = Some(event.timestamp.timestamp());
                        } else {
                            return Err(anyhow::anyhow!("Insufficient funds"));
                        }
                    }
                    Some("account_frozen") => {
                        self.is_active = false;
                    }
                    Some("account_unfrozen") => {
                        self.is_active = true;
                    }
                    _ => {
                        return Err(anyhow::anyhow!("Unknown event type"));
                    }
                }
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported event data type"));
            }
        }
        Ok(())
    }
}

/// Aggregate repository for event sourcing
struct BankAccountRepository {
    event_store: Arc<EventStore>,
}

impl BankAccountRepository {
    fn new(event_store: Arc<EventStore>) -> Self {
        Self { event_store }
    }

    async fn save_events(&self, account_id: Uuid, events: Vec<Event>, expected_version: u32) -> Result<()> {
        let stream_id = format!("account-{}", account_id);

        for (i, event) in events.iter().enumerate() {
            self.event_store.append_event(
                &stream_id,
                event.clone(),
                expected_version + i as u32,
            ).await?;
        }

        Ok(())
    }

    async fn load_account(&self, account_id: Uuid) -> Result<Option<BankAccount>> {
        let stream_id = format!("account-{}", account_id);
        let events = self.event_store.get_events(&stream_id, 0, None).await?;

        if events.is_empty() {
            return Ok(None);
        }

        // Find account creation event
        let creation_event = events.iter()
            .find(|e| {
                if let EventData::Custom(data) = &e.data {
                    data.get("event_type").and_then(|v| v.as_str()) == Some("account_created")
                } else {
                    false
                }
            })
            .ok_or_else(|| anyhow::anyhow!("No account creation event found"))?;

        let initial_data = if let EventData::Custom(data) = &creation_event.data {
            data
        } else {
            return Err(anyhow::anyhow!("Invalid creation event data"));
        };

        let owner_name = initial_data.get("owner_name")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown")
            .to_string();

        let initial_balance = initial_data.get("initial_balance")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let mut account = BankAccount::new(account_id, owner_name, initial_balance);

        // Apply all events except creation
        for event in events.iter().skip(1) {
            account.apply_event(event)?;
        }

        Ok(Some(account))
    }

    async fn load_account_at_version(&self, account_id: Uuid, version: u32) -> Result<Option<BankAccount>> {
        let stream_id = format!("account-{}", account_id);
        let events = self.event_store.get_events(&stream_id, 0, Some(version + 1)).await?;

        if events.is_empty() {
            return Ok(None);
        }

        // Same logic as load_account but only up to specified version
        let creation_event = events.iter()
            .find(|e| {
                if let EventData::Custom(data) = &e.data {
                    data.get("event_type").and_then(|v| v.as_str()) == Some("account_created")
                } else {
                    false
                }
            })
            .ok_or_else(|| anyhow::anyhow!("No account creation event found"))?;

        let initial_data = if let EventData::Custom(data) = &creation_event.data {
            data
        } else {
            return Err(anyhow::anyhow!("Invalid creation event data"));
        };

        let owner_name = initial_data.get("owner_name")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown")
            .to_string();

        let initial_balance = initial_data.get("initial_balance")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let mut account = BankAccount::new(account_id, owner_name, initial_balance);

        // Apply events up to the specified version
        for event in events.iter().skip(1).take(version as usize) {
            account.apply_event(event)?;
        }

        Ok(Some(account))
    }

    async fn save_snapshot(&self, account_id: Uuid, account: &BankAccount, version: u32) -> Result<()> {
        let stream_id = format!("account-{}", account_id);
        let snapshot_data = serde_json::to_value(account)?;

        let snapshot = Snapshot {
            stream_id: stream_id.clone(),
            version,
            data: snapshot_data,
            created_at: chrono::Utc::now(),
        };

        self.event_store.save_snapshot(&stream_id, snapshot).await?;
        Ok(())
    }

    async fn load_from_snapshot(&self, account_id: Uuid) -> Result<Option<(BankAccount, u32)>> {
        let stream_id = format!("account-{}", account_id);

        if let Some(snapshot) = self.event_store.get_latest_snapshot(&stream_id).await? {
            let account: BankAccount = serde_json::from_value(snapshot.data)?;

            // Load events after the snapshot
            let events = self.event_store.get_events(&stream_id, snapshot.version + 1, None).await?;

            let mut current_account = account;
            let mut current_version = snapshot.version;

            // Apply events after snapshot
            for event in events {
                current_account.apply_event(&event)?;
                current_version += 1;
            }

            Ok(Some((current_account, current_version)))
        } else {
            Ok(None)
        }
    }
}

fn create_account_event(account_id: Uuid, owner_name: &str, initial_balance: i64) -> Event {
    let mut event_data = serde_json::Map::new();
    event_data.insert("event_type".to_string(), serde_json::Value::String("account_created".to_string()));
    event_data.insert("account_id".to_string(), serde_json::Value::String(account_id.to_string()));
    event_data.insert("owner_name".to_string(), serde_json::Value::String(owner_name.to_string()));
    event_data.insert("initial_balance".to_string(), serde_json::Value::Number(serde_json::Number::from(initial_balance)));

    Event::new(
        EventType::Custom("account_created".to_string()),
        EventData::Custom(serde_json::Value::Object(event_data)),
        1,
    )
}

fn create_deposit_event(account_id: Uuid, amount: i64) -> Event {
    let mut event_data = serde_json::Map::new();
    event_data.insert("event_type".to_string(), serde_json::Value::String("deposit".to_string()));
    event_data.insert("account_id".to_string(), serde_json::Value::String(account_id.to_string()));
    event_data.insert("amount".to_string(), serde_json::Value::Number(serde_json::Number::from(amount)));

    Event::new(
        EventType::Custom("deposit".to_string()),
        EventData::Custom(serde_json::Value::Object(event_data)),
        1,
    )
}

fn create_withdrawal_event(account_id: Uuid, amount: i64) -> Event {
    let mut event_data = serde_json::Map::new();
    event_data.insert("event_type".to_string(), serde_json::Value::String("withdrawal".to_string()));
    event_data.insert("account_id".to_string(), serde_json::Value::String(account_id.to_string()));
    event_data.insert("amount".to_string(), serde_json::Value::Number(serde_json::Number::from(amount)));

    Event::new(
        EventType::Custom("withdrawal".to_string()),
        EventData::Custom(serde_json::Value::Object(event_data)),
        1,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use zcore_storage::Store;

    #[tokio::test]
    async fn test_event_stream_replay() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path)?);
        let event_store = Arc::new(EventStore::new(storage));
        let repository = BankAccountRepository::new(event_store.clone());

        let account_id = Uuid::new_v4();
        let owner_name = "Alice Johnson";
        let initial_balance = 100000; // $1000.00

        // Create account
        let creation_event = create_account_event(account_id, owner_name, initial_balance);
        repository.save_events(account_id, vec![creation_event], 0).await?;

        // Perform several transactions
        let transactions = vec![
            create_deposit_event(account_id, 50000),   // +$500
            create_withdrawal_event(account_id, 25000), // -$250
            create_deposit_event(account_id, 75000),   // +$750
            create_withdrawal_event(account_id, 30000), // -$300
        ];

        repository.save_events(account_id, transactions, 1).await?;

        // Load the current state
        let account = repository.load_account(account_id).await?
            .ok_or_else(|| anyhow::anyhow!("Account not found"))?;

        // Verify final state
        assert_eq!(account.account_id, account_id);
        assert_eq!(account.owner_name, owner_name);
        assert_eq!(account.balance, 170000); // $1000 + $500 - $250 + $750 - $300 = $1700
        assert_eq!(account.transaction_count, 4);
        assert!(account.is_active);

        // Test replay at different points in time
        let account_v1 = repository.load_account_at_version(account_id, 1).await?
            .ok_or_else(|| anyhow::anyhow!("Account not found at version 1"))?;
        assert_eq!(account_v1.balance, 150000); // $1000 + $500 = $1500

        let account_v2 = repository.load_account_at_version(account_id, 2).await?
            .ok_or_else(|| anyhow::anyhow!("Account not found at version 2"))?;
        assert_eq!(account_v2.balance, 125000); // $1500 - $250 = $1250

        let account_v3 = repository.load_account_at_version(account_id, 3).await?
            .ok_or_else(|| anyhow::anyhow!("Account not found at version 3"))?;
        assert_eq!(account_v3.balance, 200000); // $1250 + $750 = $2000

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_and_restore() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path)?);
        let event_store = Arc::new(EventStore::new(storage));
        let repository = BankAccountRepository::new(event_store.clone());

        let account_id = Uuid::new_v4();
        let owner_name = "Bob Smith";
        let initial_balance = 200000; // $2000.00

        // Create account and perform many transactions
        let creation_event = create_account_event(account_id, owner_name, initial_balance);
        repository.save_events(account_id, vec![creation_event], 0).await?;

        // Create 100 small transactions
        let mut events = Vec::new();
        for i in 1..=100 {
            if i % 2 == 0 {
                events.push(create_deposit_event(account_id, 1000)); // +$10
            } else {
                events.push(create_withdrawal_event(account_id, 500)); // -$5
            }
        }

        repository.save_events(account_id, events, 1).await?;

        // Load the account to get current state
        let account = repository.load_account(account_id).await?
            .ok_or_else(|| anyhow::anyhow!("Account not found"))?;

        // Save snapshot at version 50
        repository.save_snapshot(account_id, &account, 100).await?;

        // Add more events after snapshot
        let post_snapshot_events = vec![
            create_deposit_event(account_id, 100000), // +$1000
            create_withdrawal_event(account_id, 50000), // -$500
        ];

        repository.save_events(account_id, post_snapshot_events, 101).await?;

        // Load from snapshot and verify
        let (restored_account, version) = repository.load_from_snapshot(account_id).await?
            .ok_or_else(|| anyhow::anyhow!("Could not load from snapshot"))?;

        // Load without snapshot for comparison
        let full_replay_account = repository.load_account(account_id).await?
            .ok_or_else(|| anyhow::anyhow!("Account not found"))?;

        // Both should be identical
        assert_eq!(restored_account.balance, full_replay_account.balance);
        assert_eq!(restored_account.transaction_count, full_replay_account.transaction_count);
        assert_eq!(restored_account.is_active, full_replay_account.is_active);

        // Expected balance: $2000 + (50 * $10) + (50 * -$5) + $1000 - $500 = $2750
        assert_eq!(restored_account.balance, 275000);
        assert_eq!(restored_account.transaction_count, 102); // 100 + 2 post-snapshot

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_event_ordering() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path)?);
        let event_store = Arc::new(EventStore::new(storage));
        let repository = BankAccountRepository::new(event_store.clone());

        let account_id = Uuid::new_v4();
        let owner_name = "Concurrent Test";
        let initial_balance = 100000; // $1000.00

        // Create account
        let creation_event = create_account_event(account_id, owner_name, initial_balance);
        repository.save_events(account_id, vec![creation_event], 0).await?;

        // Simulate concurrent transactions
        let mut tasks = Vec::new();
        let num_concurrent_transactions = 50;

        for i in 0..num_concurrent_transactions {
            let repo = repository.clone();
            let account_id = account_id;

            let task = tokio::spawn(async move {
                // Alternating deposits and withdrawals
                if i % 2 == 0 {
                    let event = create_deposit_event(account_id, 1000);
                    repo.save_events(account_id, vec![event], i + 1).await
                } else {
                    let event = create_withdrawal_event(account_id, 500);
                    repo.save_events(account_id, vec![event], i + 1).await
                }
            });

            tasks.push(task);
        }

        // Wait for all transactions to complete
        for task in tasks {
            task.await.unwrap()?;
        }

        // Verify final state
        let account = repository.load_account(account_id).await?
            .ok_or_else(|| anyhow::anyhow!("Account not found"))?;

        // Expected: 25 deposits of $10 = +$250, 25 withdrawals of $5 = -$125
        // Final: $1000 + $250 - $125 = $1125
        assert_eq!(account.balance, 112500);
        assert_eq!(account.transaction_count, num_concurrent_transactions);

        Ok(())
    }

    #[tokio::test]
    async fn test_event_filtering_and_querying() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path)?);
        let event_store = Arc::new(EventStore::new(storage));

        // Create multiple account streams
        let account1 = Uuid::new_v4();
        let account2 = Uuid::new_v4();

        // Account 1 events
        let stream1 = format!("account-{}", account1);
        event_store.append_event(&stream1, create_account_event(account1, "Account 1", 100000), 0).await?;
        event_store.append_event(&stream1, create_deposit_event(account1, 50000), 1).await?;
        event_store.append_event(&stream1, create_withdrawal_event(account1, 25000), 2).await?;

        // Account 2 events
        let stream2 = format!("account-{}", account2);
        event_store.append_event(&stream2, create_account_event(account2, "Account 2", 200000), 0).await?;
        event_store.append_event(&stream2, create_deposit_event(account2, 30000), 1).await?;

        // Test filtering by event type
        let all_events = event_store.get_all_events(None, None).await?;
        assert_eq!(all_events.len(), 5);

        // Filter for deposit events only
        let deposit_filter = EventFilter::new()
            .event_type(EventType::Custom("deposit".to_string()));

        let deposit_events = event_store.get_filtered_events(deposit_filter).await?;
        assert_eq!(deposit_events.len(), 2);

        // Verify all are deposit events
        for event in deposit_events {
            if let EventData::Custom(data) = &event.data {
                assert_eq!(
                    data.get("event_type").and_then(|v| v.as_str()),
                    Some("deposit")
                );
            }
        }

        // Test stream-specific queries
        let account1_events = event_store.get_events(&stream1, 0, None).await?;
        assert_eq!(account1_events.len(), 3);

        let account2_events = event_store.get_events(&stream2, 0, None).await?;
        assert_eq!(account2_events.len(), 2);

        // Test range queries
        let account1_partial = event_store.get_events(&stream1, 1, Some(2)).await?;
        assert_eq!(account1_partial.len(), 1); // Only the deposit event

        Ok(())
    }

    #[tokio::test]
    async fn test_event_store_consistency() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path)?);
        let event_store = Arc::new(EventStore::new(storage));

        let account_id = Uuid::new_v4();
        let stream_id = format!("account-{}", account_id);

        // Test append ordering
        let events = vec![
            create_account_event(account_id, "Test Account", 100000),
            create_deposit_event(account_id, 50000),
            create_withdrawal_event(account_id, 25000),
            create_deposit_event(account_id, 75000),
        ];

        // Append events one by one to ensure ordering
        for (i, event) in events.iter().enumerate() {
            event_store.append_event(&stream_id, event.clone(), i as u32).await?;
        }

        // Retrieve events and verify order
        let retrieved_events = event_store.get_events(&stream_id, 0, None).await?;
        assert_eq!(retrieved_events.len(), 4);

        // Verify events are in the correct order
        for (i, event) in retrieved_events.iter().enumerate() {
            if let EventData::Custom(data) = &event.data {
                let expected_type = match i {
                    0 => "account_created",
                    1 => "deposit",
                    2 => "withdrawal",
                    3 => "deposit",
                    _ => panic!("Unexpected event index"),
                };

                assert_eq!(
                    data.get("event_type").and_then(|v| v.as_str()),
                    Some(expected_type),
                    "Event {} should be {}", i, expected_type
                );
            }
        }

        // Test version consistency
        let stream = event_store.get_stream(&stream_id).await?
            .ok_or_else(|| anyhow::anyhow!("Stream not found"))?;

        assert_eq!(stream.version, 3); // 0-based, so 4 events = version 3
        assert_eq!(stream.event_count, 4);

        Ok(())
    }
}