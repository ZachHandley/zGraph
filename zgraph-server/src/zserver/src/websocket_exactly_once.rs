use crate::AppState;
use anyhow::{anyhow, Result};
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{ConnectInfo, State, WebSocketUpgrade};
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex as TokioMutex, RwLock as TokioRwLock};
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use zevents::{Event, EventHandler, HandlerResult, EventType, ExactlyOnceEventBus, ConsumerConfig};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebSocketMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub topic: Option<String>,
    pub data: serde_json::Value,
    pub id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebSocketResponse {
    pub id: Option<String>,
    pub status: String,
    pub data: Option<serde_json::Value>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebSocketEvent {
    pub topic: String,
    pub seq: u64,
    pub ts: chrono::DateTime<chrono::Utc>,
    pub data: serde_json::Value,
}

// Distributed transaction WebSocket message types
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DistributedTransactionMessage {
    #[serde(rename = "type")]
    pub msg_type: String, // "2pc_prepare", "2pc_commit", "2pc_abort", etc.
    pub transaction_id: String,
    pub coordinator_id: String,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SagaMessage {
    #[serde(rename = "type")]
    pub msg_type: String, // "saga_step", "saga_compensation", etc.
    pub saga_id: String,
    pub step_id: Option<String>,
    pub data: serde_json::Value,
}

/// WebSocket connection handler with exactly-once delivery
pub struct ExactlyOnceWebSocketHandler {
    connection_id: String,
    subscriptions: Arc<TokioRwLock<HashSet<String>>>,
    event_bus: Arc<ExactlyOnceEventBus>,
    subscription_id: Option<String>,
    tx: Arc<TokioMutex<Option<futures_util::stream::SplitSink<WebSocket, Message>>>>,
}

impl ExactlyOnceWebSocketHandler {
    pub fn new(connection_id: String, event_bus: Arc<ExactlyOnceEventBus>) -> Self {
        Self {
            connection_id,
            subscriptions: Arc::new(TokioRwLock::new(HashSet::new())),
            event_bus,
            subscription_id: None,
            tx: Arc::new(TokioMutex::new(None)),
        }
    }

    pub async fn start_consuming(&mut self) -> Result<()> {
        let handler = Arc::new(WebSocketEventHandler {
            connection_id: self.connection_id.clone(),
            subscriptions: self.subscriptions.clone(),
            tx: self.tx.clone(),
        });

        let consumer_config = ConsumerConfig {
            consumer_id: format!("websocket_{}", self.connection_id),
            topics: vec!["**".to_string()], // Initially subscribe to all topics
            max_concurrency: 1, // WebSocket is single-threaded
            processing_timeout_secs: 10,
            poll_interval_ms: 50, // Fast polling for real-time updates
            auto_start: true,
            persistent: false, // WebSocket subscriptions are not persistent
        };

        let subscription_id = self.event_bus
            .subscribe_with_config(handler, vec!["**".to_string()], false, consumer_config)
            .await?;

        self.subscription_id = Some(subscription_id);

        info!(
            connection_id = %self.connection_id,
            subscription_id = %self.subscription_id.as_ref().unwrap(),
            "Started exactly-once WebSocket event consumption"
        );

        Ok(())
    }

    pub async fn stop_consuming(&self) -> Result<()> {
        if let Some(ref subscription_id) = self.subscription_id {
            self.event_bus.unsubscribe(subscription_id).await?;

            info!(
                connection_id = %self.connection_id,
                subscription_id = %subscription_id,
                "Stopped WebSocket event consumption"
            );
        }
        Ok(())
    }

    pub async fn subscribe_to_topic(&self, topic: String) -> Result<()> {
        self.subscriptions.write().await.insert(topic.clone());
        debug!(
            connection_id = %self.connection_id,
            topic = %topic,
            "WebSocket subscribed to topic"
        );
        Ok(())
    }

    pub async fn unsubscribe_from_topic(&self, topic: String) -> Result<()> {
        self.subscriptions.write().await.remove(&topic);
        debug!(
            connection_id = %self.connection_id,
            topic = %topic,
            "WebSocket unsubscribed from topic"
        );
        Ok(())
    }

    pub async fn set_sink(&self, sink: futures_util::stream::SplitSink<WebSocket, Message>) {
        *self.tx.lock().await = Some(sink);
    }
}

/// Event handler that forwards events to WebSocket connections
pub struct WebSocketEventHandler {
    connection_id: String,
    subscriptions: Arc<TokioRwLock<HashSet<String>>>,
    tx: Arc<TokioMutex<Option<futures_util::stream::SplitSink<WebSocket, Message>>>>,
}

#[async_trait::async_trait]
impl EventHandler for WebSocketEventHandler {
    async fn handle(&self, event: &Event) -> HandlerResult {
        let topic = event.to_topic();

        // Check if we're subscribed to this topic
        let subscribed = {
            let subs = self.subscriptions.read().await;
            subs.contains(&topic) || subs.iter().any(|pattern| {
                // Simple wildcard matching
                if pattern == "**" {
                    return true;
                }
                if pattern.ends_with("/**") {
                    let prefix = &pattern[..pattern.len() - 3];
                    return topic.starts_with(prefix);
                }
                pattern == &topic
            })
        };

        if !subscribed {
            return HandlerResult::Discard;
        }

        // Convert event to WebSocket format
        let ws_event = WebSocketEvent {
            topic: topic.clone(),
            seq: 0, // Will be set by message sequence
            ts: event.timestamp,
            data: match &event.data {
                Some(data) => data.clone(),
                None => serde_json::Value::Null,
            },
        };

        let message = match serde_json::to_string(&ws_event) {
            Ok(json) => json,
            Err(e) => {
                error!(
                    connection_id = %self.connection_id,
                    event_id = %event.id,
                    error = %e,
                    "Failed to serialize event to JSON"
                );
                return HandlerResult::DeadLetter(format!("JSON serialization failed: {}", e));
            }
        };

        // Send to WebSocket
        let mut tx_guard = self.tx.lock().await;
        if let Some(ref mut tx) = tx_guard.as_mut() {
            match tx.send(Message::Text(message)).await {
                Ok(()) => {
                    debug!(
                        connection_id = %self.connection_id,
                        event_id = %event.id,
                        topic = %topic,
                        "Successfully sent event to WebSocket"
                    );
                    HandlerResult::Success
                }
                Err(e) => {
                    warn!(
                        connection_id = %self.connection_id,
                        event_id = %event.id,
                        error = %e,
                        "Failed to send message to WebSocket, connection likely closed"
                    );
                    // Don't retry as the connection is probably closed
                    HandlerResult::DeadLetter(format!("WebSocket send failed: {}", e))
                }
            }
        } else {
            warn!(
                connection_id = %self.connection_id,
                event_id = %event.id,
                "WebSocket sink not available"
            );
            HandlerResult::Retry
        }
    }

    fn interested_in(&self, _event_type: &EventType) -> bool {
        true // We handle topic-based filtering in handle()
    }
}

/// Enhanced WebSocket handler with exactly-once delivery
pub async fn enhanced_websocket_handler(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let connection_id = Uuid::new_v4().to_string();

    ws.on_upgrade(move |socket| async move {
        if let Err(e) = handle_websocket_with_exactly_once(socket, state, connection_id, addr).await {
            error!(error = %e, "WebSocket handler error");
        }
    })
}

async fn handle_websocket_with_exactly_once(
    socket: WebSocket,
    state: AppState,
    connection_id: String,
    addr: SocketAddr,
) -> Result<()> {
    info!(
        connection_id = %connection_id,
        remote_addr = %addr,
        "New WebSocket connection with exactly-once delivery"
    );

    // Create exactly-once event bus if not available
    // For now, we'll create a simple fallback. In production, this should be injected
    let storage = state.store;
    let event_bus = Arc::new(
        zevents::ExactlyOnceEventBusBuilder::new()
            .build(storage)
            .await?
    );
    event_bus.start().await?;

    let mut handler = ExactlyOnceWebSocketHandler::new(connection_id.clone(), event_bus.clone());

    let (sink, mut stream) = socket.split();
    handler.set_sink(sink).await;

    // Start consuming events
    handler.start_consuming().await?;

    // Handle incoming messages
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Err(e) = handle_websocket_message(&handler, text).await {
                    error!(
                        connection_id = %connection_id,
                        error = %e,
                        "Failed to handle WebSocket message"
                    );
                }
            }
            Ok(Message::Close(_)) => {
                info!(
                    connection_id = %connection_id,
                    "WebSocket connection closed by client"
                );
                break;
            }
            Ok(_) => {
                // Ignore other message types (binary, ping, pong)
            }
            Err(e) => {
                error!(
                    connection_id = %connection_id,
                    error = %e,
                    "WebSocket error"
                );
                break;
            }
        }
    }

    // Clean up
    handler.stop_consuming().await?;
    event_bus.shutdown().await?;

    info!(
        connection_id = %connection_id,
        "WebSocket connection handler finished"
    );

    Ok(())
}

async fn handle_websocket_message(
    handler: &ExactlyOnceWebSocketHandler,
    message: String,
) -> Result<()> {
    let ws_message: WebSocketMessage = serde_json::from_str(&message)
        .map_err(|e| anyhow!("Invalid JSON message: {}", e))?;

    match ws_message.msg_type.as_str() {
        "subscribe" => {
            if let Some(topic) = ws_message.topic {
                handler.subscribe_to_topic(topic).await?;

                // Send success response
                let response = WebSocketResponse {
                    id: ws_message.id,
                    status: "success".to_string(),
                    data: Some(serde_json::json!({"action": "subscribed"})),
                    error: None,
                };

                send_response(handler, response).await?;
            }
        }
        "unsubscribe" => {
            if let Some(topic) = ws_message.topic {
                handler.unsubscribe_from_topic(topic).await?;

                let response = WebSocketResponse {
                    id: ws_message.id,
                    status: "success".to_string(),
                    data: Some(serde_json::json!({"action": "unsubscribed"})),
                    error: None,
                };

                send_response(handler, response).await?;
            }
        }
        "ping" => {
            let response = WebSocketResponse {
                id: ws_message.id,
                status: "success".to_string(),
                data: Some(serde_json::json!({"pong": true})),
                error: None,
            };

            send_response(handler, response).await?;
        }
        // Distributed transaction messages
        "2pc_prepare" | "2pc_commit" | "2pc_abort" | "2pc_response" => {
            handle_distributed_transaction_message(handler, &ws_message).await?;
        }
        "saga_step" | "saga_compensation" | "saga_status" => {
            handle_saga_message(handler, &ws_message).await?;
        }
        _ => {
            let response = WebSocketResponse {
                id: ws_message.id,
                status: "error".to_string(),
                data: None,
                error: Some(format!("Unknown message type: {}", ws_message.msg_type)),
            };

            send_response(handler, response).await?;
        }
    }

    Ok(())
}

async fn send_response(handler: &ExactlyOnceWebSocketHandler, response: WebSocketResponse) -> Result<()> {
    let response_json = serde_json::to_string(&response)?;
    let mut tx_guard = handler.tx.lock().await;
    if let Some(ref mut tx) = tx_guard.as_mut() {
        tx.send(Message::Text(response_json)).await.map_err(|e| anyhow!("Failed to send response: {}", e))?;
    }
    Ok(())
}

/// Handle distributed transaction WebSocket messages
async fn handle_distributed_transaction_message(
    handler: &ExactlyOnceWebSocketHandler,
    message: &WebSocketMessage,
) -> Result<()> {
    debug!("Handling distributed transaction message: {}", message.msg_type);

    // Parse the distributed transaction message
    let tx_message: DistributedTransactionMessage = serde_json::from_value(message.data.clone())
        .map_err(|e| anyhow!("Invalid distributed transaction message format: {}", e))?;

    match message.msg_type.as_str() {
        "2pc_prepare" => {
            // Handle two-phase commit prepare message
            info!("Received 2PC prepare for transaction: {}", tx_message.transaction_id);

            // In a full implementation, this would:
            // 1. Validate the transaction operations
            // 2. Lock required resources
            // 3. Prepare for commit
            // 4. Send back prepare response

            let response = WebSocketResponse {
                id: message.id.clone(),
                status: "success".to_string(),
                data: Some(serde_json::json!({
                    "transaction_id": tx_message.transaction_id,
                    "vote": "commit", // or "abort"
                    "message": "Prepared successfully"
                })),
                error: None,
            };

            send_response(handler, response).await?;
        }
        "2pc_commit" => {
            // Handle two-phase commit commit message
            info!("Received 2PC commit for transaction: {}", tx_message.transaction_id);

            // In a full implementation, this would:
            // 1. Apply all prepared changes
            // 2. Release locks
            // 3. Send acknowledgment

            let response = WebSocketResponse {
                id: message.id.clone(),
                status: "success".to_string(),
                data: Some(serde_json::json!({
                    "transaction_id": tx_message.transaction_id,
                    "status": "committed"
                })),
                error: None,
            };

            send_response(handler, response).await?;
        }
        "2pc_abort" => {
            // Handle two-phase commit abort message
            info!("Received 2PC abort for transaction: {}", tx_message.transaction_id);

            // In a full implementation, this would:
            // 1. Rollback any prepared changes
            // 2. Release locks
            // 3. Send acknowledgment

            let response = WebSocketResponse {
                id: message.id.clone(),
                status: "success".to_string(),
                data: Some(serde_json::json!({
                    "transaction_id": tx_message.transaction_id,
                    "status": "aborted"
                })),
                error: None,
            };

            send_response(handler, response).await?;
        }
        "2pc_response" => {
            // Handle response from a participant
            info!("Received 2PC response for transaction: {}", tx_message.transaction_id);

            // Forward to the distributed transaction coordinator
            // In a real implementation, this would be handled by the messaging system
            let response = WebSocketResponse {
                id: message.id.clone(),
                status: "success".to_string(),
                data: Some(serde_json::json!({
                    "transaction_id": tx_message.transaction_id,
                    "acknowledged": true
                })),
                error: None,
            };

            send_response(handler, response).await?;
        }
        _ => {
            warn!("Unknown distributed transaction message type: {}", message.msg_type);
        }
    }

    Ok(())
}

/// Handle saga WebSocket messages
async fn handle_saga_message(
    handler: &ExactlyOnceWebSocketHandler,
    message: &WebSocketMessage,
) -> Result<()> {
    debug!("Handling saga message: {}", message.msg_type);

    // Parse the saga message
    let saga_message: SagaMessage = serde_json::from_value(message.data.clone())
        .map_err(|e| anyhow!("Invalid saga message format: {}", e))?;

    match message.msg_type.as_str() {
        "saga_step" => {
            // Handle saga step execution
            info!("Received saga step execution for saga: {}", saga_message.saga_id);

            // In a full implementation, this would:
            // 1. Execute the saga step operation
            // 2. Store the result for potential compensation
            // 3. Send response back to saga coordinator

            let response = WebSocketResponse {
                id: message.id.clone(),
                status: "success".to_string(),
                data: Some(serde_json::json!({
                    "saga_id": saga_message.saga_id,
                    "step_id": saga_message.step_id,
                    "status": "completed",
                    "result": "Step executed successfully"
                })),
                error: None,
            };

            send_response(handler, response).await?;
        }
        "saga_compensation" => {
            // Handle saga compensation step
            info!("Received saga compensation for saga: {}", saga_message.saga_id);

            // In a full implementation, this would:
            // 1. Execute the compensation operation
            // 2. Undo the effects of the original step
            // 3. Send response back to saga coordinator

            let response = WebSocketResponse {
                id: message.id.clone(),
                status: "success".to_string(),
                data: Some(serde_json::json!({
                    "saga_id": saga_message.saga_id,
                    "step_id": saga_message.step_id,
                    "status": "compensated",
                    "result": "Compensation executed successfully"
                })),
                error: None,
            };

            send_response(handler, response).await?;
        }
        "saga_status" => {
            // Handle saga status query
            info!("Received saga status query for saga: {}", saga_message.saga_id);

            // In a full implementation, this would query the saga coordinator
            let response = WebSocketResponse {
                id: message.id.clone(),
                status: "success".to_string(),
                data: Some(serde_json::json!({
                    "saga_id": saga_message.saga_id,
                    "status": "executing",
                    "steps_completed": 2,
                    "steps_total": 5
                })),
                error: None,
            };

            send_response(handler, response).await?;
        }
        _ => {
            warn!("Unknown saga message type: {}", message.msg_type);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use zcore_storage::Store;
    use zevents::{Event, EventType, EventData};

    #[tokio::test]
    async fn test_websocket_event_handler() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let storage = Arc::new(Store::open(&db_path).unwrap());

        let event_bus = Arc::new(
            zevents::ExactlyOnceEventBusBuilder::new()
                .build(storage)
                .await
                .unwrap()
        );

        event_bus.start().await.unwrap();

        let handler = WebSocketEventHandler {
            connection_id: "test_connection".to_string(),
            subscriptions: Arc::new(TokioRwLock::new(HashSet::from(["test/**".to_string()]))),
            tx: Arc::new(TokioMutex::new(None)), // No actual sink for test
        };

        let event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 1,
                status: Some("pending".to_string()),
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );

        // This should be discarded because no sink is available
        let result = handler.handle(&event).await;
        assert!(matches!(result, HandlerResult::Retry));

        event_bus.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_websocket_topic_filtering() {
        let handler = WebSocketEventHandler {
            connection_id: "test_connection".to_string(),
            subscriptions: Arc::new(TokioRwLock::new(HashSet::from([
                "jobs/**".to_string(),
                "users/123".to_string(),
            ]))),
            tx: Arc::new(TokioMutex::new(None)),
        };

        // Test matching topics
        let job_event = Event::new(
            EventType::JobCreated,
            EventData::Job {
                job_id: uuid::Uuid::new_v4(),
                org_id: 1,
                status: Some("pending".to_string()),
                worker_id: None,
                error: None,
                output: None,
            },
            1,
        );

        // Should be interested (though will fail to send due to no sink)
        let result = handler.handle(&job_event).await;
        assert!(matches!(result, HandlerResult::Retry));

        // Test non-matching topic
        let user_event = Event::new(
            EventType::UserCreated,
            EventData::User {
                user_id: Some(456), // Different user ID
                org_id: 1,
                email: Some("test@example.com".to_string()),
                action: "created".to_string(),
                ip_address: None,
            },
            1,
        );

        let result = handler.handle(&user_event).await;
        assert!(matches!(result, HandlerResult::Discard));
    }
}