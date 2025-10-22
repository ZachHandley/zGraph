# ZServer - ZRUSTDB HTTP/WebSocket Server

The main HTTP/WebSocket server component for ZRUSTDB, providing REST APIs, streaming ingest, real-time communication, and web console serving.

## Overview

ZServer is a high-performance Rust server built with the Axum framework that provides:

- **REST APIs** for SQL execution, vector search, and data management
- **WebSocket support** for real-time updates and job monitoring
- **WebRTC functionality** for real-time communication and collaboration
- **Streaming data ingest** with SIMD-accelerated JSON processing
- **Authentication & authorization** with JWT tokens and permissions
- **Job orchestration** with queue management and artifact storage
- **Web console serving** for the Leptos-based frontend

## Current Status

⚠️ **COMPILATION ISSUES**: The zserver crate currently fails to compile due to missing workspace dependencies and integration problems.

### Known Issues
- Missing `axum` workspace dependency (not defined in workspace Cargo.toml)
- Dependencies on several crates that may not exist or have compilation issues
- Mixed storage backend usage (some components using fjall, others referencing redb)
- Multiple commented-out dependencies indicating integration challenges

## Architecture

### Core Components

- **HTTP Server**: Axum-based REST API server with middleware support
- **WebSocket Handler**: Real-time communication with topic subscriptions
- **WebRTC Engine**: Multi-participant video/audio with data channels
- **Stream Processor**: NDJSON/JSON streaming with compression support
- **Authentication Layer**: JWT-based auth with session management
- **Permission System**: Role-based access control for all resources
- **Job Queue**: Background task execution with artifact management
- **File Storage**: Content-addressed file storage with deduplication

### Technology Stack

- **Web Framework**: Axum with Tower middleware
- **Real-time**: WebSockets + WebRTC for live communication
- **Authentication**: JWT tokens with Argon2 password hashing
- **Storage**: Integration with zcore-storage (fjall LSM-tree) and zcore-catalog
- **Vector Operations**: SIMD-optimized kernels for distance calculations
- **SQL Engine**: zexec-engine for query execution with vector support
- **Job Orchestration**: zbrain queue with zworker local execution
- **Streaming**: simd-json for high-performance JSON processing

### Storage Backend Analysis

The system has evolved from using redb to fjall as the primary storage backend:

**Current State (fjall-based):**
- `zcore-storage` now uses fjall LSM-tree for high-performance write operations
- Provides MVCC (Multi-Version Concurrency Control) for unlimited concurrent writers
- Better suited for high-throughput write scenarios
- LSM-tree architecture offers excellent write performance and compression

**Legacy References (redb):**
- Some documentation and comments still reference redb
- Certain components may have mixed storage backend references
- Workspace still includes redb dependency for compatibility

**Migration Status:**
- The storage layer has been migrated to fjall for better performance
- Some integration points may still need updating to reflect this change
- Documentation needs to be updated to reflect the fjall-based architecture

## Features

### REST API Endpoints

#### Authentication (`/v1/auth`)
- `POST /v1/auth/signup` - User registration
- `POST /v1/auth/login` - User authentication with environment selection
- `POST /v1/auth/refresh` - Token refresh
- `POST /v1/auth/switch-environment` - Environment switching
- `POST /v1/auth/logout` - Session termination
- `GET /v1/whoami` - Current user information

#### SQL & Vector Operations (`/v1`)
- `POST /v1/sql` - Execute SQL queries with vector support
- `POST /v1/knn` - Exact k-nearest neighbor search
- `POST /v1/indexes/hnsw/search` - HNSW vector index search
- `POST /v1/indexes/hnsw/build` - Create HNSW vector index
- `GET /v1/indexes/hnsw` - List available vector indexes

#### Collections (`/v1/collections`)
- `GET /v1/collections` - List collections with projections
- `POST /v1/collections` - Create collection with optional projection
- `GET /v1/collections/{name}/projection` - Get collection projection
- `PUT /v1/collections/{name}/projection` - Update collection projection
- `POST /v1/collections/{name}/ingest` - Stream data into collection

#### Job Management (`/v1/jobs`)
- `POST /v1/jobs` - Submit new job for execution
- `GET /v1/jobs` - List jobs with filtering
- `GET /v1/jobs/{id}` - Get job details and status
- `DELETE /v1/jobs/{id}` - Cancel running job
- `GET /v1/jobs/{id}/logs` - Retrieve job logs (paginated)
- `GET /v1/jobs/{id}/artifacts` - List job artifacts
- `GET /v1/jobs/{id}/artifacts/{name}` - Download job artifact

#### File Storage (`/v1/files`)
- `POST /v1/files` - Upload file (returns SHA256 hash)
- `GET /v1/files/{sha}` - Download file by content hash

#### Permissions (`/v1/permissions`)
- `GET /v1/permissions` - List permission records
- `POST /v1/permissions` - Create permission record
- `GET /v1/permissions/{id}` - Get specific permission
- `PUT /v1/permissions/{id}` - Update permission record
- `DELETE /v1/permissions/{id}` - Delete permission record

### WebRTC API (`/v2/rtc`)

#### Room Management
- `POST /v2/rtc/rooms` - Create WebRTC room
- `GET /v2/rtc/rooms` - List active rooms
- `GET /v2/rtc/rooms/{id}` - Get room details
- `DELETE /v2/rtc/rooms/{id}` - Delete room
- `POST /v2/rtc/rooms/{id}/join` - Join room as participant
- `POST /v2/rtc/rooms/{id}/leave` - Leave room
- `GET /v2/rtc/rooms/{id}/participants` - List room participants

#### Session Management
- `GET /v2/rtc/sessions` - List WebRTC sessions
- `GET /v2/rtc/sessions/{id}` - Get session details
- `PUT /v2/rtc/sessions/{id}` - Update session configuration

#### Data Subscriptions
- `POST /v2/rtc/subscriptions` - Create data subscription
- `GET /v2/rtc/subscriptions` - List active subscriptions
- `GET /v2/rtc/subscriptions/{id}` - Get subscription details
- `PUT /v2/rtc/subscriptions/{id}` - Update subscription
- `DELETE /v2/rtc/subscriptions/{id}` - Remove subscription

#### Function Triggers
- `POST /v2/rtc/triggers` - Create function trigger
- `GET /v2/rtc/triggers` - List registered triggers
- `GET /v2/rtc/triggers/{id}` - Get trigger details
- `PUT /v2/rtc/triggers/{id}` - Update trigger configuration
- `DELETE /v2/rtc/triggers/{id}` - Remove trigger
- `POST /v2/rtc/triggers/{id}/execute` - Execute trigger manually
- `GET /v2/rtc/triggers/{id}/history` - Get trigger execution history

### WebSocket Communication (`/ws`)

Real-time communication with topic-based subscriptions:

#### Subscription Topics
- `jobs/{id}` - Job status updates and logs
- `collections/{name}` - Collection change notifications
- `permissions` - Permission updates
- `system` - System-wide notifications

#### Message Format
```json
{
  "topic": "jobs/123",
  "type": "log",
  "data": {
    "timestamp": "2025-01-15T10:30:00Z",
    "level": "info",
    "message": "Job started successfully"
  }
}
```

## Usage Examples

### Basic SQL Query
```bash
curl -X POST http://localhost:8080/v1/sql \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users LIMIT 10"}'
```

### Vector Search
```bash
curl -X POST http://localhost:8080/v1/knn \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "metric": "cosine",
    "query": [0.1, 0.2, 0.3, 0.4],
    "vectors": [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]],
    "top_k": 5
  }'
```

### Streaming Data Ingest
```bash
# NDJSON streaming
curl -X POST http://localhost:8080/v1/collections/events/ingest \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/x-ndjson" \
  --data-binary @events.ndjson

# JSON array streaming
curl -X POST http://localhost:8080/v1/collections/events/ingest \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '[{"id": 1, "name": "event1"}, {"id": 2, "name": "event2"}]'
```

### WebSocket Connection (JavaScript)
```javascript
const ws = new WebSocket('ws://localhost:8080/ws');

// Subscribe to job updates
ws.send(JSON.stringify({
  action: 'subscribe',
  topic: 'jobs/123'
}));

// Handle messages
ws.onmessage = (event) => {
  const message = JSON.parse(event.data);
  console.log(`${message.topic}: ${message.data.message}`);
};
```

### Job Submission
```bash
curl -X POST http://localhost:8080/v1/jobs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "command": ["python", "script.py"],
    "timeout_secs": 300,
    "env": {"PYTHONPATH": "/app"},
    "metadata": {"project": "analytics"}
  }'
```

## Configuration

### Environment Variables

#### Server Configuration
- `ZDB_ADDR` - Server bind address (default: `127.0.0.1:8080`)
- `ZDB_DATA` - Data directory root (default: `./data`)

#### Authentication
- `ZJWT_SECRET` - JWT signing secret
- `ZJWT_REFRESH_SECRET` - Refresh token secret
- `ZJWT_ACCESS_TTL_SECS` - Access token TTL (default: 900)
- `ZJWT_REFRESH_TTL_SECS` - Refresh token TTL (default: 604800)
- `ZAUTH_DEV_ORG_ID` - Development organization ID
- `ZAUTH_DEV_SECRET` - Development token secret

#### WebRTC Configuration
- `ZWEBRTC_ICE_SERVERS` - JSON array of ICE server configurations
  ```json
  [
    {
      "urls": ["stun:stun.l.google.com:19302"]
    },
    {
      "urls": ["turn:turn.example.com:3478"],
      "username": "user",
      "credential": "pass"
    }
  ]
  ```

#### Worker Configuration
- `ZWORKER_MODE` - Worker execution mode (`auto`|`local`|`docker`)

#### Organization Settings
- `ZDEFAULT_ORG_ID` - Default organization ID for requests

#### Observability
- `ZOBS_METRICS_ENABLED` - Enable metrics collection (default: true)
- `ZOBS_METRICS_ADDR` - Metrics server address (default: `127.0.0.1:9090`)
- `ZOBS_HEALTH_ENABLED` - Enable health checks (default: true)
- `ZOBS_HEALTH_ADDR` - Health check address (default: `127.0.0.1:8080`)
- `ZOBS_SERVICE_NAME` - Service name for observability (default: `zrustdb`)
- `ZOBS_ENVIRONMENT` - Environment name (default: `development`)

## Performance Features

### SIMD Optimizations
- **Vector Operations**: Uses zvec-kernels for SIMD-accelerated distance calculations
- **JSON Processing**: simd-json for high-performance parsing
- **CPU Feature Detection**: Automatic runtime optimization selection

### Streaming Processing
- **Zero-Copy**: Minimal data copying during stream processing
- **Bounded Memory**: Configurable memory limits for large uploads
- **Compression**: Gzip support for compressed data streams
- **Batch Processing**: Efficient bulk operations with error tracking

### Connection Management
- **Connection Pooling**: Efficient database connection reuse
- **WebSocket Scaling**: Support for thousands of concurrent WebSocket connections
- **Resource Limits**: Configurable limits for rooms, participants, and subscriptions

## Security

### Authentication & Authorization
- **JWT Tokens**: Secure token-based authentication
- **Permission System**: Granular CRUD permissions per resource type
- **Session Management**: Automatic token refresh and session validation
- **Environment Isolation**: Multi-environment support with data isolation

### Data Protection
- **Input Validation**: Comprehensive request validation and sanitization
- **SQL Injection Prevention**: Parameterized queries throughout
- **Content Addressing**: SHA256-based file integrity verification
- **Error Handling**: Security-conscious error messages

### WebRTC Security
- **ICE Server Validation**: Configurable and validated ICE servers
- **Room Access Control**: Permission-based room access
- **Participant Limits**: Configurable limits to prevent abuse
- **Session Timeout**: Automatic cleanup of stale sessions

## Development

### Compilation Status
⚠️ **The crate currently fails to compile** due to workspace dependency issues.

### Current Issues
```bash
# This will fail with missing workspace dependencies
cargo check --package zserver

# Error: failed to parse manifest at `/home/zach/github/ZDB/crates/zserver/Cargo.toml`
# Caused by: error inheriting `axum` from workspace root manifest's `workspace.dependencies.axum`
# Caused by: `dependency.axum` was not found in `workspace.dependencies`
```

### Required Fixes
1. **Workspace Dependencies**: Add missing `axum` dependency to workspace Cargo.toml
2. **Crate Members**: Ensure all referenced crates exist in workspace members
3. **Storage Backend**: Update all references to reflect fjall instead of redb
4. **Integration**: Fix integration between zserver and dependent crates

### Testing (When Fixed)
```bash
# Unit tests (when compilation is fixed)
cargo test --package zserver

# Integration tests
cargo test --test auth_integ
cargo test --test sql_api_integration
cargo test --test webrtc_test

# With logging
RUST_LOG=debug cargo test --package zserver -- --nocapture
```

## Integration

### Frontend Integration
The server serves the Leptos-based web console at `/console` with:
- SPA routing support with index.html fallback
- Static asset serving from `web/zconsole/dist`
- API integration for all management functions

### Database Integration
- **Storage**: Uses zcore-storage for persistent data
- **Catalog**: Integrates zcore-catalog for metadata management
- **Collections**: Supports zcollections for NoSQL operations
- **Vector Search**: Uses zann-hnsw for vector indexing

### Job System Integration
- **Queue**: Uses zbrain for job queue management
- **Execution**: Integrates zworker for local job execution
- **Artifacts**: Content-addressed artifact storage
- **Monitoring**: Real-time job status and log streaming

## Architecture Diagrams

### Request Flow
```
Client → Authentication → Permission Check → Handler → Storage → Response
  ↓                                                        ↓
WebSocket ← Real-time Updates ← Event System ← Background Jobs
```

### WebRTC Flow
```
Client A ─── Signaling ──→ ZServer ←── Signaling ─── Client B
    │                         │                         │
    └─── WebRTC P2P Connection ─────────────────────────┘
```

### Data Ingest Flow
```
Stream → Compression → JSON Parser → Batch → Projection → Storage
          (gzip)       (simd-json)   (1000)    (SQL)      (redb)
```

## License

MIT OR Apache-2.0