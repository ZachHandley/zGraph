//! Enhanced Trigger System Tests for Complex Conditions and Chaining

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::{json, Value};
use tokio::sync::{mpsc, RwLock};
use tokio::time::{sleep, timeout};

use crate::{
    Event, EventType, EventData, EventHandler, HandlerResult,
    TriggerCondition, TriggerConfig, TriggerManager,
};

/// Test event data for complex trigger scenarios
#[derive(Debug, Clone)]
struct OrderEvent {
    order_id: String,
    customer_id: String,
    amount: f64,
    status: String,
    items: Vec<OrderItem>,
    shipping_address: Address,
    payment_method: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
struct OrderItem {
    product_id: String,
    quantity: u32,
    price: f64,
    category: String,
}

#[derive(Debug, Clone)]
struct Address {
    country: String,
    state: String,
    city: String,
    zip_code: String,
}

/// Test handler that tracks trigger executions
#[derive(Debug, Clone)]
struct TriggerTestHandler {
    handler_id: String,
    execution_count: Arc<AtomicU32>,
    executed_events: Arc<RwLock<Vec<Event>>>,
    execution_log: Arc<RwLock<Vec<TriggerExecution>>>,
}

#[derive(Debug, Clone)]
struct TriggerExecution {
    trigger_id: String,
    event_id: String,
    timestamp: chrono::DateTime<chrono::Utc>,
    context: HashMap<String, Value>,
}

impl TriggerTestHandler {
    fn new(handler_id: String) -> Self {
        Self {
            handler_id,
            execution_count: Arc::new(AtomicU32::new(0)),
            executed_events: Arc::new(RwLock::new(Vec::new())),
            execution_log: Arc::new(RwLock::new(Vec::new())),
        }
    }

    async fn get_execution_count(&self) -> u32 {
        self.execution_count.load(Ordering::Acquire)
    }

    async fn get_executed_events(&self) -> Vec<Event> {
        self.executed_events.read().await.clone()
    }

    async fn get_execution_log(&self) -> Vec<TriggerExecution> {
        self.execution_log.read().await.clone()
    }

    async fn record_execution(&self, trigger_id: String, event: &Event, context: HashMap<String, Value>) {
        let execution = TriggerExecution {
            trigger_id,
            event_id: event.id.to_string(),
            timestamp: chrono::Utc::now(),
            context,
        };

        self.execution_log.write().await.push(execution);
        self.executed_events.write().await.push(event.clone());
        self.execution_count.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl EventHandler for TriggerTestHandler {
    async fn handle(&self, event: &Event) -> HandlerResult {
        // Simulate trigger execution
        let mut context = HashMap::new();
        context.insert("handler_id".to_string(), json!(self.handler_id));
        context.insert("processed_at".to_string(), json!(chrono::Utc::now().to_rfc3339()));

        self.record_execution("test_trigger".to_string(), event, context).await;

        println!("Trigger handler {} executed for event {}", self.handler_id, event.id);
        HandlerResult::Success
    }

    fn interested_in(&self, _event_type: &EventType) -> bool {
        true
    }
}

/// Create test order event
fn create_order_event(
    order_id: &str,
    customer_id: &str,
    amount: f64,
    status: &str,
    country: &str,
    payment_method: &str,
    items: Vec<OrderItem>,
) -> Event {
    let order_data = json!({
        "order_id": order_id,
        "customer_id": customer_id,
        "amount": amount,
        "status": status,
        "items": items.iter().map(|item| json!({
            "product_id": item.product_id,
            "quantity": item.quantity,
            "price": item.price,
            "category": item.category
        })).collect::<Vec<_>>(),
        "shipping_address": {
            "country": country,
            "state": "CA",
            "city": "San Francisco",
            "zip_code": "94102"
        },
        "payment_method": payment_method,
        "created_at": chrono::Utc::now().to_rfc3339()
    });

    Event::new(
        EventType::Custom("order_event".to_string()),
        EventData::Custom(order_data),
        1,
    )
}

/// Create test user event
fn create_user_event(user_id: &str, action: &str, metadata: Value) -> Event {
    let user_data = json!({
        "user_id": user_id,
        "action": action,
        "metadata": metadata,
        "timestamp": chrono::Utc::now().to_rfc3339()
    });

    Event::new(
        EventType::Custom("user_event".to_string()),
        EventData::Custom(user_data),
        1,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use zcore_storage::Store;

    #[tokio::test]
    async fn test_complex_condition_evaluation() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path)?);

        let trigger_manager = Arc::new(TriggerManager::new(storage));

        // Create complex trigger condition for high-value international orders
        let high_value_international_condition = TriggerCondition::And(vec![
            TriggerCondition::FieldGreaterThan {
                field: "amount".to_string(),
                value: json!(1000.0),
            },
            TriggerCondition::FieldNotEquals {
                field: "shipping_address.country".to_string(),
                value: json!("US"),
            },
            TriggerCondition::FieldEquals {
                field: "status".to_string(),
                value: json!("confirmed"),
            },
            TriggerCondition::Or(vec![
                TriggerCondition::FieldEquals {
                    field: "payment_method".to_string(),
                    value: json!("wire_transfer"),
                },
                TriggerCondition::FieldEquals {
                    field: "payment_method".to_string(),
                    value: json!("crypto"),
                },
            ]),
        ]);

        let handler = Arc::new(TriggerTestHandler::new("high_value_intl".to_string()));

        let trigger_config = TriggerConfig {
            id: uuid::Uuid::new_v4(),
            name: "High Value International Orders".to_string(),
            description: Some("Trigger for high-value international orders with special payment methods".to_string()),
            condition: high_value_international_condition,
            handler: handler.clone(),
            enabled: true,
            cooldown_ms: 0,
            max_executions_per_hour: None,
            metadata: HashMap::new(),
        };

        trigger_manager.register_trigger(trigger_config).await?;

        // Test events
        let items = vec![
            OrderItem {
                product_id: "prod_1".to_string(),
                quantity: 2,
                price: 500.0,
                category: "electronics".to_string(),
            },
            OrderItem {
                product_id: "prod_2".to_string(),
                quantity: 1,
                price: 200.0,
                category: "accessories".to_string(),
            },
        ];

        // This should trigger (meets all conditions)
        let triggering_event = create_order_event(
            "order_123",
            "customer_456",
            1200.0,
            "confirmed",
            "UK",
            "wire_transfer",
            items.clone(),
        );

        // This should NOT trigger (amount too low)
        let non_triggering_event1 = create_order_event(
            "order_124",
            "customer_457",
            800.0,
            "confirmed",
            "UK",
            "wire_transfer",
            items.clone(),
        );

        // This should NOT trigger (US address)
        let non_triggering_event2 = create_order_event(
            "order_125",
            "customer_458",
            1500.0,
            "confirmed",
            "US",
            "wire_transfer",
            items.clone(),
        );

        // This should NOT trigger (wrong payment method)
        let non_triggering_event3 = create_order_event(
            "order_126",
            "customer_459",
            1300.0,
            "confirmed",
            "DE",
            "credit_card",
            items.clone(),
        );

        // Process events
        trigger_manager.process_event(&triggering_event).await?;
        trigger_manager.process_event(&non_triggering_event1).await?;
        trigger_manager.process_event(&non_triggering_event2).await?;
        trigger_manager.process_event(&non_triggering_event3).await?;

        sleep(Duration::from_millis(100)).await;

        // Verify only the triggering event caused execution
        let execution_count = handler.get_execution_count().await;
        assert_eq!(execution_count, 1, "Only one event should have triggered the condition");

        let executed_events = handler.get_executed_events().await;
        assert_eq!(executed_events.len(), 1);
        assert_eq!(executed_events[0].id, triggering_event.id);

        Ok(())
    }

    #[tokio::test]
    async fn test_trigger_chaining() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path)?);

        let trigger_manager = Arc::new(TriggerManager::new(storage));

        // Create chained triggers: order_created -> inventory_check -> shipping_notification
        let order_handler = Arc::new(TriggerTestHandler::new("order_processor".to_string()));
        let inventory_handler = Arc::new(TriggerTestHandler::new("inventory_checker".to_string()));
        let shipping_handler = Arc::new(TriggerTestHandler::new("shipping_notifier".to_string()));

        // Trigger 1: Process any order event
        let order_trigger = TriggerConfig {
            id: uuid::Uuid::new_v4(),
            name: "Order Processor".to_string(),
            description: Some("Processes any new order".to_string()),
            condition: TriggerCondition::EventType(EventType::Custom("order_event".to_string())),
            handler: order_handler.clone(),
            enabled: true,
            cooldown_ms: 0,
            max_executions_per_hour: None,
            metadata: HashMap::new(),
        };

        // Trigger 2: Check inventory for confirmed orders
        let inventory_trigger = TriggerConfig {
            id: uuid::Uuid::new_v4(),
            name: "Inventory Checker".to_string(),
            description: Some("Checks inventory for confirmed orders".to_string()),
            condition: TriggerCondition::And(vec![
                TriggerCondition::EventType(EventType::Custom("order_event".to_string())),
                TriggerCondition::FieldEquals {
                    field: "status".to_string(),
                    value: json!("confirmed"),
                },
            ]),
            handler: inventory_handler.clone(),
            enabled: true,
            cooldown_ms: 0,
            max_executions_per_hour: None,
            metadata: HashMap::new(),
        };

        // Trigger 3: Send shipping notification for high-value confirmed orders
        let shipping_trigger = TriggerConfig {
            id: uuid::Uuid::new_v4(),
            name: "Shipping Notifier".to_string(),
            description: Some("Sends shipping notifications for high-value orders".to_string()),
            condition: TriggerCondition::And(vec![
                TriggerCondition::EventType(EventType::Custom("order_event".to_string())),
                TriggerCondition::FieldEquals {
                    field: "status".to_string(),
                    value: json!("confirmed"),
                },
                TriggerCondition::FieldGreaterThan {
                    field: "amount".to_string(),
                    value: json!(500.0),
                },
            ]),
            handler: shipping_handler.clone(),
            enabled: true,
            cooldown_ms: 0,
            max_executions_per_hour: None,
            metadata: HashMap::new(),
        };

        // Register triggers
        trigger_manager.register_trigger(order_trigger).await?;
        trigger_manager.register_trigger(inventory_trigger).await?;
        trigger_manager.register_trigger(shipping_trigger).await?;

        // Test with different order events
        let items = vec![
            OrderItem {
                product_id: "prod_1".to_string(),
                quantity: 1,
                price: 600.0,
                category: "electronics".to_string(),
            },
        ];

        // High-value confirmed order (should trigger all 3)
        let high_value_confirmed = create_order_event(
            "order_001",
            "customer_001",
            600.0,
            "confirmed",
            "US",
            "credit_card",
            items.clone(),
        );

        // Low-value confirmed order (should trigger 1 and 2, but not 3)
        let low_value_confirmed = create_order_event(
            "order_002",
            "customer_002",
            300.0,
            "confirmed",
            "US",
            "credit_card",
            items.clone(),
        );

        // High-value pending order (should trigger only 1)
        let high_value_pending = create_order_event(
            "order_003",
            "customer_003",
            800.0,
            "pending",
            "US",
            "credit_card",
            items.clone(),
        );

        // Process events
        trigger_manager.process_event(&high_value_confirmed).await?;
        trigger_manager.process_event(&low_value_confirmed).await?;
        trigger_manager.process_event(&high_value_pending).await?;

        sleep(Duration::from_millis(100)).await;

        // Verify trigger execution counts
        assert_eq!(order_handler.get_execution_count().await, 3, "Order handler should process all events");
        assert_eq!(inventory_handler.get_execution_count().await, 2, "Inventory handler should process confirmed orders");
        assert_eq!(shipping_handler.get_execution_count().await, 1, "Shipping handler should process high-value confirmed orders");

        Ok(())
    }

    #[tokio::test]
    async fn test_frequency_based_triggers() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path)?);

        let trigger_manager = Arc::new(TriggerManager::new(storage));

        // Create trigger with frequency-based condition
        let handler = Arc::new(TriggerTestHandler::new("frequency_trigger".to_string()));

        let frequency_trigger = TriggerConfig {
            id: uuid::Uuid::new_v4(),
            name: "Frequent User Activity".to_string(),
            description: Some("Triggers when user has high activity frequency".to_string()),
            condition: TriggerCondition::And(vec![
                TriggerCondition::EventType(EventType::Custom("user_event".to_string())),
                TriggerCondition::Frequency {
                    field: "user_id".to_string(),
                    count: 5,
                    window_seconds: 60,
                },
            ]),
            handler: handler.clone(),
            enabled: true,
            cooldown_ms: 1000, // 1 second cooldown
            max_executions_per_hour: Some(10),
            metadata: HashMap::new(),
        };

        trigger_manager.register_trigger(frequency_trigger).await?;

        let user_id = "user_123";

        // Send events rapidly (should trigger after 5th event)
        for i in 1..=7 {
            let event = create_user_event(
                user_id,
                "page_view",
                json!({ "page": format!("page_{}", i), "session": "session_123" }),
            );

            trigger_manager.process_event(&event).await?;
            sleep(Duration::from_millis(100)).await;
        }

        sleep(Duration::from_millis(200)).await;

        // Should trigger once after the 5th event
        let execution_count = handler.get_execution_count().await;
        assert!(execution_count > 0, "Frequency trigger should have executed");

        // Test with different user (should not trigger)
        for i in 1..=3 {
            let event = create_user_event(
                "user_456",
                "page_view",
                json!({ "page": format!("page_{}", i) }),
            );

            trigger_manager.process_event(&event).await?;
        }

        sleep(Duration::from_millis(100)).await;

        // Count should not increase for different user
        let new_count = handler.get_execution_count().await;
        assert_eq!(new_count, execution_count, "Different user should not trigger frequency condition");

        Ok(())
    }

    #[tokio::test]
    async fn test_rate_limiting_and_cooldown() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path)?);

        let trigger_manager = Arc::new(TriggerManager::new(storage));

        let handler = Arc::new(TriggerTestHandler::new("rate_limited".to_string()));

        // Create trigger with rate limiting
        let rate_limited_trigger = TriggerConfig {
            id: uuid::Uuid::new_v4(),
            name: "Rate Limited Trigger".to_string(),
            description: Some("Trigger with cooldown and rate limiting".to_string()),
            condition: TriggerCondition::EventType(EventType::Custom("user_event".to_string())),
            handler: handler.clone(),
            enabled: true,
            cooldown_ms: 500, // 500ms cooldown
            max_executions_per_hour: Some(3), // Max 3 executions per hour
            metadata: HashMap::new(),
        };

        trigger_manager.register_trigger(rate_limited_trigger).await?;

        // Send events rapidly
        for i in 1..=10 {
            let event = create_user_event(
                "user_rate_test",
                "action",
                json!({ "iteration": i }),
            );

            trigger_manager.process_event(&event).await?;
            sleep(Duration::from_millis(100)).await; // Faster than cooldown
        }

        sleep(Duration::from_millis(200)).await;

        let execution_count = handler.get_execution_count().await;
        println!("Executions after rapid events: {}", execution_count);

        // Should be limited by rate limiting
        assert!(execution_count <= 3, "Should be limited by max executions per hour");

        // Test cooldown by waiting and sending more events
        sleep(Duration::from_millis(600)).await; // Wait for cooldown

        for i in 11..=12 {
            let event = create_user_event(
                "user_rate_test",
                "action",
                json!({ "iteration": i }),
            );

            trigger_manager.process_event(&event).await?;
            sleep(Duration::from_millis(600)).await; // Respect cooldown
        }

        sleep(Duration::from_millis(200)).await;

        let final_count = handler.get_execution_count().await;
        println!("Final execution count: {}", final_count);

        // Should have processed more events after respecting cooldown
        assert!(final_count > execution_count, "Should process more events after cooldown");
        assert!(final_count <= 3, "Should still respect hourly rate limit");

        Ok(())
    }

    #[tokio::test]
    async fn test_nested_condition_evaluation() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path)?);

        let trigger_manager = Arc::new(TriggerManager::new(storage));

        let handler = Arc::new(TriggerTestHandler::new("nested_conditions".to_string()));

        // Create deeply nested condition
        let nested_condition = TriggerCondition::And(vec![
            TriggerCondition::EventType(EventType::Custom("order_event".to_string())),
            TriggerCondition::Or(vec![
                TriggerCondition::And(vec![
                    TriggerCondition::FieldEquals {
                        field: "shipping_address.country".to_string(),
                        value: json!("US"),
                    },
                    TriggerCondition::FieldGreaterThan {
                        field: "amount".to_string(),
                        value: json!(100.0),
                    },
                ]),
                TriggerCondition::And(vec![
                    TriggerCondition::FieldEquals {
                        field: "shipping_address.country".to_string(),
                        value: json!("CA"),
                    },
                    TriggerCondition::FieldGreaterThan {
                        field: "amount".to_string(),
                        value: json!(150.0),
                    },
                ]),
                TriggerCondition::And(vec![
                    TriggerCondition::FieldNotIn {
                        field: "shipping_address.country".to_string(),
                        values: vec![json!("US"), json!("CA")],
                    },
                    TriggerCondition::FieldGreaterThan {
                        field: "amount".to_string(),
                        value: json!(200.0),
                    },
                ]),
            ]),
        ]);

        let nested_trigger = TriggerConfig {
            id: uuid::Uuid::new_v4(),
            name: "Nested Condition Trigger".to_string(),
            description: Some("Complex nested condition evaluation".to_string()),
            condition: nested_condition,
            handler: handler.clone(),
            enabled: true,
            cooldown_ms: 0,
            max_executions_per_hour: None,
            metadata: HashMap::new(),
        };

        trigger_manager.register_trigger(nested_trigger).await?;

        let items = vec![
            OrderItem {
                product_id: "prod_1".to_string(),
                quantity: 1,
                price: 120.0,
                category: "test".to_string(),
            },
        ];

        // Test cases for different conditions
        let test_cases = vec![
            // US orders > $100 (should trigger)
            ("US", 120.0, true),
            ("US", 80.0, false),
            // CA orders > $150 (should trigger)
            ("CA", 160.0, true),
            ("CA", 140.0, false),
            // Other countries > $200 (should trigger)
            ("UK", 250.0, true),
            ("DE", 180.0, false),
        ];

        let mut expected_triggers = 0;

        for (i, (country, amount, should_trigger)) in test_cases.iter().enumerate() {
            let event = create_order_event(
                &format!("order_{}", i),
                &format!("customer_{}", i),
                *amount,
                "confirmed",
                country,
                "credit_card",
                items.clone(),
            );

            trigger_manager.process_event(&event).await?;

            if *should_trigger {
                expected_triggers += 1;
            }
        }

        sleep(Duration::from_millis(200)).await;

        let execution_count = handler.get_execution_count().await;
        assert_eq!(
            execution_count, expected_triggers,
            "Should trigger exactly {} times for nested conditions",
            expected_triggers
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_trigger_performance_under_load() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path)?);

        let trigger_manager = Arc::new(TriggerManager::new(storage));

        // Create multiple triggers with different conditions
        let mut handlers = Vec::new();

        for i in 0..10 {
            let handler = Arc::new(TriggerTestHandler::new(format!("handler_{}", i)));
            handlers.push(handler.clone());

            let condition = match i % 3 {
                0 => TriggerCondition::FieldGreaterThan {
                    field: "amount".to_string(),
                    value: json!(100.0 * i as f64),
                },
                1 => TriggerCondition::FieldEquals {
                    field: "status".to_string(),
                    value: json!(format!("status_{}", i)),
                },
                _ => TriggerCondition::And(vec![
                    TriggerCondition::EventType(EventType::Custom("order_event".to_string())),
                    TriggerCondition::FieldContains {
                        field: "customer_id".to_string(),
                        value: json!(format!("customer_{}", i)),
                    },
                ]),
            };

            let trigger_config = TriggerConfig {
                id: uuid::Uuid::new_v4(),
                name: format!("Performance Trigger {}", i),
                description: Some(format!("Trigger {} for performance testing", i)),
                condition,
                handler,
                enabled: true,
                cooldown_ms: 0,
                max_executions_per_hour: None,
                metadata: HashMap::new(),
            };

            trigger_manager.register_trigger(trigger_config).await?;
        }

        // Generate load
        let num_events = 1000;
        let start_time = Instant::now();

        let mut tasks = Vec::new();

        for i in 0..num_events {
            let manager = trigger_manager.clone();

            let task = tokio::spawn(async move {
                let items = vec![
                    OrderItem {
                        product_id: format!("prod_{}", i),
                        quantity: 1,
                        price: (i % 100) as f64 + 50.0,
                        category: "test".to_string(),
                    },
                ];

                let event = create_order_event(
                    &format!("order_{}", i),
                    &format!("customer_{}", i % 10),
                    (i % 1000) as f64 + 100.0,
                    &format!("status_{}", i % 10),
                    "US",
                    "credit_card",
                    items,
                );

                manager.process_event(&event).await
            });

            tasks.push(task);
        }

        // Wait for all events to be processed
        for task in tasks {
            task.await.unwrap()?;
        }

        let processing_time = start_time.elapsed();

        sleep(Duration::from_millis(100)).await;

        // Check results
        let mut total_executions = 0;
        for handler in &handlers {
            total_executions += handler.get_execution_count().await;
        }

        println!(
            "Processed {} events in {:?} with {} total trigger executions",
            num_events, processing_time, total_executions
        );

        // Performance assertions
        assert!(processing_time < Duration::from_secs(5), "Should process events quickly");
        assert!(total_executions > 0, "Should have triggered some conditions");

        let events_per_sec = num_events as f64 / processing_time.as_secs_f64();
        println!("Event processing rate: {:.2} events/sec", events_per_sec);
        assert!(events_per_sec > 100.0, "Should achieve good throughput");

        Ok(())
    }
}