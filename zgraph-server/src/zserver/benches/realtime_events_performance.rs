use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::task::JoinSet;
use tokio::time::sleep;
use futures_util::{SinkExt, StreamExt};
use axum::extract::ws::{Message, WebSocket};
use serde_json::{json, Value};
use zserver::{AppState, WebSocketMetrics};
use zcore_storage::Storage;
use zcore_catalog::CatalogManager;
use zauth::AuthService;
use zbrain::JobQueue;
use zworker::LocalWorker;
use zpermissions::PermissionService;

/// Mock WebRTC peer connection for benchmarking
struct MockWebRTCPeer {
    id: String,
    data_channel_sender: mpsc::Sender<Value>,
    data_channel_receiver: mpsc::Receiver<Value>,
    metrics: WebSocketMetrics,
}

impl MockWebRTCPeer {
    fn new(id: String) -> Self {
        let (sender, receiver) = mpsc::channel(256);
        Self {
            id,
            data_channel_sender: sender,
            data_channel_receiver: receiver,
            metrics: WebSocketMetrics::default(),
        }
    }

    async fn send_event(&self, event: Value) -> Result<(), mpsc::error::SendError<Value>> {
        self.data_channel_sender.send(event).await
    }

    async fn receive_event(&mut self) -> Option<Value> {
        self.data_channel_receiver.recv().await
    }
}

/// Setup benchmark environment with real-time event infrastructure
async fn setup_realtime_env() -> (Arc<AppState>, Arc<broadcast::Sender<Value>>) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let storage = Arc::new(Storage::new(temp_dir.path().join("rt_storage")).await.expect("Storage creation failed"));
    let catalog = Arc::new(CatalogManager::new(storage.clone()).await.expect("Catalog creation failed"));
    let auth_service = Arc::new(AuthService::new(storage.clone()).await.expect("Auth service creation failed"));
    let job_queue = Arc::new(JobQueue::new(storage.clone()).await.expect("Job queue creation failed"));
    let worker = Arc::new(LocalWorker::new(temp_dir.path().join("worker")).expect("Worker creation failed"));
    let permission_service = Arc::new(PermissionService::new(storage.clone()).await.expect("Permission service creation failed"));

    let websocket_metrics = WebSocketMetrics::default();
    let websocket_senders = Arc::new(RwLock::new(HashMap::new()));

    let app_state = Arc::new(AppState {
        auth_service,
        storage,
        catalog_manager: catalog,
        job_queue,
        worker,
        permission_service,
        websocket_metrics,
        websocket_senders,
    });

    let (event_sender, _) = broadcast::channel(10000);
    let event_sender = Arc::new(event_sender);

    (app_state, event_sender)
}

/// Mock WebSocket connection for benchmarking
struct MockWebSocket {
    sender: mpsc::Sender<Message>,
    receiver: mpsc::Receiver<Message>,
    metrics: WebSocketMetrics,
}

impl MockWebSocket {
    fn new() -> Self {
        let (sender, receiver) = mpsc::channel(256);
        Self {
            sender,
            receiver,
            metrics: WebSocketMetrics::default(),
        }
    }

    async fn send(&self, message: Message) -> Result<(), mpsc::error::SendError<Message>> {
        self.sender.send(message).await
    }

    async fn recv(&mut self) -> Option<Message> {
        self.receiver.recv().await
    }
}

/// Benchmark event delivery latency from database change to client notification
fn bench_event_delivery_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (app_state, event_sender) = rt.block_on(setup_realtime_env());

    let mut group = c.benchmark_group("Event Delivery Latency");

    group.bench_function("Single Client - Database Change to WebSocket", |b| {
        b.to_async(&rt).iter(|| async {
            let mut websocket = MockWebSocket::new();
            let start_time = Instant::now();

            // Simulate database change event
            let db_event = json!({
                "type": "table_update",
                "table": "users",
                "operation": "insert",
                "data": {"id": 1, "name": "Test User"},
                "timestamp": chrono::Utc::now()
            });

            // Send event through broadcast channel
            let _ = event_sender.send(db_event.clone());

            // Simulate WebSocket message creation and delivery
            let ws_message = Message::Text(db_event.to_string());
            websocket.send(ws_message).await.expect("Failed to send WebSocket message");

            let delivery_time = start_time.elapsed();
            black_box(delivery_time)
        })
    });

    group.bench_function("Multiple Clients - Fan-out Delivery", |b| {
        b.to_async(&rt).iter(|| async {
            let client_count = 100;
            let mut websockets: Vec<MockWebSocket> = (0..client_count).map(|_| MockWebSocket::new()).collect();

            let start_time = Instant::now();

            let db_event = json!({
                "type": "table_update",
                "table": "products",
                "operation": "update",
                "data": {"id": 42, "price": 99.99},
                "timestamp": chrono::Utc::now()
            });

            // Simulate fan-out to all connected clients
            let mut tasks = JoinSet::new();
            for ws in websockets.iter() {
                let event = db_event.clone();
                let sender = ws.sender.clone();
                tasks.spawn(async move {
                    let ws_message = Message::Text(event.to_string());
                    sender.send(ws_message).await
                });
            }

            // Wait for all deliveries to complete
            while let Some(result) = tasks.join_next().await {
                result.expect("Task failed").expect("WebSocket send failed");
            }

            let fanout_time = start_time.elapsed();
            black_box(fanout_time)
        })
    });

    group.finish();
}

/// Benchmark WebSocket vs WebRTC performance for event streaming
fn bench_websocket_vs_webrtc(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("WebSocket vs WebRTC");

    // WebSocket event streaming benchmark
    group.bench_function("WebSocket Event Streaming", |b| {
        b.to_async(&rt).iter(|| async {
            let mut websocket = MockWebSocket::new();
            let event_count = 1000;

            let start_time = Instant::now();

            for i in 0..event_count {
                let event = json!({
                    "type": "stream_event",
                    "sequence": i,
                    "data": format!("Event data {}", i),
                    "timestamp": chrono::Utc::now()
                });

                let ws_message = Message::Text(event.to_string());
                websocket.send(ws_message).await.expect("Failed to send WebSocket message");
            }

            let streaming_time = start_time.elapsed();
            black_box(streaming_time)
        })
    });

    // WebRTC data channel streaming benchmark
    group.bench_function("WebRTC DataChannel Streaming", |b| {
        b.to_async(&rt).iter(|| async {
            let mut webrtc_peer = MockWebRTCPeer::new("peer-1".to_string());
            let event_count = 1000;

            let start_time = Instant::now();

            for i in 0..event_count {
                let event = json!({
                    "type": "stream_event",
                    "sequence": i,
                    "data": format!("Event data {}", i),
                    "timestamp": chrono::Utc::now()
                });

                webrtc_peer.send_event(event).await.expect("Failed to send WebRTC event");
            }

            let streaming_time = start_time.elapsed();
            black_box(streaming_time)
        })
    });

    // Compare latency characteristics
    for &message_size in &[128, 512, 1024, 4096] {
        group.bench_function(BenchmarkId::new("WebSocket Latency", message_size), |b| {
            b.to_async(&rt).iter(|| async {
                let mut websocket = MockWebSocket::new();
                let large_data = "x".repeat(message_size);

                let start_time = Instant::now();

                let event = json!({
                    "type": "large_event",
                    "data": large_data,
                    "timestamp": chrono::Utc::now()
                });

                let ws_message = Message::Text(event.to_string());
                websocket.send(ws_message).await.expect("Failed to send WebSocket message");

                let latency = start_time.elapsed();
                black_box(latency)
            })
        });

        group.bench_function(BenchmarkId::new("WebRTC Latency", message_size), |b| {
            b.to_async(&rt).iter(|| async {
                let webrtc_peer = MockWebRTCPeer::new("peer-1".to_string());
                let large_data = "x".repeat(message_size);

                let start_time = Instant::now();

                let event = json!({
                    "type": "large_event",
                    "data": large_data,
                    "timestamp": chrono::Utc::now()
                });

                webrtc_peer.send_event(event).await.expect("Failed to send WebRTC event");

                let latency = start_time.elapsed();
                black_box(latency)
            })
        });
    }

    group.finish();
}

/// Benchmark event batching efficiency under different loads
fn bench_event_batching(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (app_state, event_sender) = rt.block_on(setup_realtime_env());

    let mut group = c.benchmark_group("Event Batching");

    // Test different batch sizes
    for &batch_size in &[1, 10, 50, 100, 500] {
        group.bench_function(BenchmarkId::new("Batch Processing", batch_size), |b| {
            b.to_async(&rt).iter(|| async {
                let mut websockets: Vec<MockWebSocket> = (0..10).map(|_| MockWebSocket::new()).collect();
                let events_per_batch = batch_size;

                let start_time = Instant::now();

                // Generate batch of events
                let mut events = Vec::new();
                for i in 0..events_per_batch {
                    events.push(json!({
                        "type": "batch_event",
                        "sequence": i,
                        "data": format!("Batch event {}", i),
                        "timestamp": chrono::Utc::now()
                    }));
                }

                // Send batch to all connected clients
                let mut tasks = JoinSet::new();
                for ws in websockets.iter() {
                    let events_batch = events.clone();
                    let sender = ws.sender.clone();

                    tasks.spawn(async move {
                        let batch_message = json!({
                            "type": "event_batch",
                            "events": events_batch,
                            "count": events_batch.len()
                        });

                        let ws_message = Message::Text(batch_message.to_string());
                        sender.send(ws_message).await
                    });
                }

                // Wait for all batch deliveries
                while let Some(result) = tasks.join_next().await {
                    result.expect("Task failed").expect("Batch send failed");
                }

                let batch_time = start_time.elapsed();
                black_box(batch_time)
            })
        });
    }

    // Test different client loads
    for &client_count in &[1, 10, 50, 100, 500] {
        group.bench_function(BenchmarkId::new("Client Load", client_count), |b| {
            b.to_async(&rt).iter(|| async {
                let mut websockets: Vec<MockWebSocket> = (0..client_count).map(|_| MockWebSocket::new()).collect();

                let start_time = Instant::now();

                let event = json!({
                    "type": "load_test_event",
                    "data": "Test event for load testing",
                    "timestamp": chrono::Utc::now()
                });

                // Send event to all clients concurrently
                let mut tasks = JoinSet::new();
                for ws in websockets.iter() {
                    let event = event.clone();
                    let sender = ws.sender.clone();

                    tasks.spawn(async move {
                        let ws_message = Message::Text(event.to_string());
                        sender.send(ws_message).await
                    });
                }

                while let Some(result) = tasks.join_next().await {
                    result.expect("Task failed").expect("Event send failed");
                }

                let load_time = start_time.elapsed();
                black_box(load_time)
            })
        });
    }

    group.finish();
}

/// Benchmark cache invalidation performance impact
fn bench_cache_invalidation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (app_state, event_sender) = rt.block_on(setup_realtime_env());

    let mut group = c.benchmark_group("Cache Invalidation");

    group.bench_function("Cache Miss After Invalidation", |b| {
        b.to_async(&rt).iter(|| async {
            // Simulate cache population
            let cache_key = "user_data_123";
            let cached_data = json!({
                "id": 123,
                "name": "Test User",
                "email": "test@example.com",
                "last_updated": chrono::Utc::now()
            });

            let start_time = Instant::now();

            // Simulate cache invalidation event
            let invalidation_event = json!({
                "type": "cache_invalidation",
                "key": cache_key,
                "reason": "data_update",
                "timestamp": chrono::Utc::now()
            });

            // Send invalidation event
            let _ = event_sender.send(invalidation_event);

            // Simulate cache miss and data reload
            sleep(Duration::from_micros(100)).await; // Simulate cache lookup time
            let reload_time = start_time.elapsed();

            black_box(reload_time)
        })
    });

    group.bench_function("Bulk Cache Invalidation", |b| {
        b.to_async(&rt).iter(|| async {
            let cache_keys = (0..1000).map(|i| format!("cache_key_{}", i)).collect::<Vec<_>>();

            let start_time = Instant::now();

            // Send bulk invalidation event
            let bulk_invalidation = json!({
                "type": "bulk_cache_invalidation",
                "keys": cache_keys,
                "reason": "schema_change",
                "timestamp": chrono::Utc::now()
            });

            let _ = event_sender.send(bulk_invalidation);

            let invalidation_time = start_time.elapsed();
            black_box(invalidation_time)
        })
    });

    group.finish();
}

/// Benchmark cross-system event propagation latency
fn bench_cross_system_propagation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (app_state, event_sender) = rt.block_on(setup_realtime_env());

    let mut group = c.benchmark_group("Cross-System Propagation");

    group.bench_function("Database to WebSocket Propagation", |b| {
        b.to_async(&rt).iter(|| async {
            let mut websocket = MockWebSocket::new();

            let start_time = Instant::now();

            // Simulate database event
            let db_event = json!({
                "type": "database_event",
                "table": "orders",
                "operation": "insert",
                "data": {"id": 123, "total": 99.99, "status": "pending"},
                "source": "database",
                "timestamp": chrono::Utc::now()
            });

            // Propagate through event system
            let _ = event_sender.send(db_event.clone());

            // Convert to WebSocket message
            let ws_message = Message::Text(db_event.to_string());
            websocket.send(ws_message).await.expect("Failed to send WebSocket message");

            let propagation_time = start_time.elapsed();
            black_box(propagation_time)
        })
    });

    group.bench_function("Job Queue to WebSocket Propagation", |b| {
        b.to_async(&rt).iter(|| async {
            let mut websocket = MockWebSocket::new();

            let start_time = Instant::now();

            // Simulate job queue event
            let job_event = json!({
                "type": "job_status_change",
                "job_id": "job_12345",
                "status": "completed",
                "result": {"success": true, "output": "Job completed successfully"},
                "source": "job_queue",
                "timestamp": chrono::Utc::now()
            });

            // Propagate through event system
            let _ = event_sender.send(job_event.clone());

            // Convert to WebSocket message
            let ws_message = Message::Text(job_event.to_string());
            websocket.send(ws_message).await.expect("Failed to send WebSocket message");

            let propagation_time = start_time.elapsed();
            black_box(propagation_time)
        })
    });

    group.bench_function("Multi-System Event Chain", |b| {
        b.to_async(&rt).iter(|| async {
            let mut websockets: Vec<MockWebSocket> = (0..5).map(|_| MockWebSocket::new()).collect();

            let start_time = Instant::now();

            // Simulate complex event chain: Database -> Job Queue -> Cache -> WebSocket
            let chain_events = vec![
                json!({
                    "type": "database_update",
                    "table": "users",
                    "operation": "update",
                    "data": {"id": 1, "last_login": chrono::Utc::now()},
                    "stage": "database"
                }),
                json!({
                    "type": "job_triggered",
                    "job_type": "user_analytics_update",
                    "trigger_data": {"user_id": 1},
                    "stage": "job_queue"
                }),
                json!({
                    "type": "cache_update",
                    "keys": ["user_1_profile", "user_1_analytics"],
                    "stage": "cache"
                }),
                json!({
                    "type": "client_notification",
                    "user_id": 1,
                    "message": "Profile updated",
                    "stage": "websocket"
                })
            ];

            // Send each event in the chain
            for event in chain_events {
                let _ = event_sender.send(event.clone());

                // Simulate propagation to WebSocket clients
                for ws in &websockets {
                    let ws_message = Message::Text(event.to_string());
                    ws.send(ws_message).await.expect("Failed to send WebSocket message");
                }
            }

            let chain_time = start_time.elapsed();
            black_box(chain_time)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_event_delivery_latency,
    bench_websocket_vs_webrtc,
    bench_event_batching,
    bench_cache_invalidation,
    bench_cross_system_propagation
);
criterion_main!(benches);