# zserver CLAUDE.md

## Purpose
HTTP/WebSocket server providing REST APIs, streaming ingest, real-time updates, and web console serving for ZRUSTDB.

## Narrative Summary
The zserver crate implements the main HTTP server for ZRUSTDB using Axum framework. It provides comprehensive REST APIs for SQL execution, vector search, collection management, job orchestration, and file storage. The server includes streaming NDJSON/JSON ingestion with SIMD acceleration, WebSocket support for real-time updates, permission-based access control, and serves the Leptos-based web console. The implementation emphasizes performance through streaming processing and efficient memory management.

## Key Files
- `src/lib.rs:1-1815` - Complete HTTP server implementation
- `src/lib.rs:503-552` - Router configuration and endpoint mapping
- `src/lib.rs:554-676` - Server startup and initialization
- `src/lib.rs:843-1033` - WebSocket handler with topic subscriptions
- `src/lib.rs:1107-1253` - Streaming collection ingest
- `src/lib.rs:713-754` - SQL execution endpoint

## Core Server Components
### Application State (153-203)
- `AppState` - Shared server state with auth, storage, catalogs
- Contains job queue, WebSocket metrics, permission service
- Cloneable state for Axum handler sharing

### Router Configuration (503-552)
- Authentication endpoints (`/v1/auth/*`)
- SQL and vector search APIs (`/v1/sql`, `/v1/knn`)
- Collection and table management
- Job orchestration endpoints
- File storage and artifact management
- Permission management APIs
- WebSocket upgrade endpoint

## Authentication Integration
### Session Management
- `AuthSession` extractor for protected routes
- JWT token validation with automatic refresh
- Role-based permission checking
- Development token support with production protection

### Permission Enforcement (221-247)
- `ensure_permission()` - Resource-level access control
- CRUD permissions per resource type
- Principal-based authorization (User, Role, Label)
- HTTP status code mapping for permission errors

## REST API Endpoints
### SQL Execution (713-754)
- `POST /v1/sql` - Execute SQL queries with permission checking
- Query type detection for appropriate permissions
- Integration with zexec-engine for query execution
- Error handling with detailed logging

### Vector Operations
- `POST /v1/knn` - Exact KNN search using SIMD kernels
- `POST /v1/indexes/hnsw/search` - HNSW index search
- `POST /v1/indexes/hnsw/build` - Index creation
- `GET /v1/indexes/hnsw` - List available indexes

### Collection Management
- `GET /v1/collections` - List collections with projections
- `POST /v1/collections` - Create collection with projection
- `GET/PUT /v1/collections/{name}/projection` - Manage projections
- `POST /v1/collections/{name}/ingest` - Streaming data ingest

## Streaming Ingest Implementation (1107-1253)
### Content Type Detection
- `application/x-ndjson` - Line-by-line NDJSON streaming
- `application/json` - JSON array streaming
- Optional gzip compression support
- Automatic format detection and processing

### NDJSON Processing
- Line-by-line streaming with `LinesCodec`
- SIMD-JSON parsing for high performance
- Batch processing with error tracking
- Memory-efficient processing of large datasets

### JSON Array Processing
- Streaming JSON deserializer
- Iterator-based processing
- Support for both single objects and arrays
- Graceful error handling and reporting

## WebSocket Implementation (843-1033)
### Topic Subscriptions
- Subscribe/unsubscribe to specific topics
- Job-specific subscriptions (`jobs/{id}`)
- Real-time log and state updates
- Metadata notifications for connection health

### Message Handling
- Producer task for event broadcasting
- Sender task for WebSocket message delivery
- Queue management with backpressure handling
- Graceful connection cleanup

### Performance Features
- Bounded message queues (256 message capacity)
- Dropped message tracking and reporting
- Efficient subscription management
- Concurrent connection handling

## File Storage (1301-1354)
### Content-Addressed Storage
- SHA256-based file identification
- Org-scoped file organization
- Deduplication through content addressing
- Integrity verification on upload

### Upload/Download
- `POST /v1/files` - Upload with SHA256 generation
- `GET /v1/files/{sha}` - Download by content hash
- Proper HTTP headers for downloads
- Permission-based access control

## Job Orchestration APIs (1357-1668)
### Job Management
- `POST /v1/jobs` - Submit jobs with specifications
- `GET /v1/jobs` - List jobs with org filtering
- `GET /v1/jobs/{id}` - Get job details and status
- `DELETE /v1/jobs/{id}` - Cancel running jobs

### Job Monitoring
- `GET /v1/jobs/{id}/logs` - Paginated log retrieval
- `GET /v1/jobs/{id}/artifacts` - List job artifacts
- `GET /v1/jobs/{id}/artifacts/{name}` - Download artifacts
- WebSocket streaming for real-time updates

## Web Console Serving
- Static file serving for Leptos SPA at `/console`
- Built artifacts served from `web/zconsole/dist`
- Index.html fallback for SPA routing
- Production-ready static asset serving

## Server Configuration
### Environment Variables
- Authentication configuration (JWT secrets, dev tokens)
- Database paths and storage configuration
- Worker mode selection (local/docker)
- Organization and environment settings

#### WebRTC Configuration
- `ZWEBRTC_ICE_SERVERS` - JSON array of ICE server configurations for WebRTC connections
  - Format: `[{"urls": ["stun:server:port"], "username": "optional", "credential": "optional"}]`
  - Defaults to Google STUN servers if not specified
  - Example: `ZWEBRTC_ICE_SERVERS='[{"urls":["stun:stun.l.google.com:19302"]},{"urls":["turn:turn.example.com:3478"],"username":"user","credential":"pass"}]'`

### Database Initialization (617-641)
- Automatic table creation on startup
- Migration-safe initialization
- Storage layer preparation
- Default environment and permission setup

## Performance Optimizations
### SIMD Integration
- Uses zvec-kernels for vector operations
- Automatic CPU feature detection
- Optimized batch processing
- Memory-efficient distance calculations

### Streaming Processing
- Zero-copy where possible
- Bounded memory usage for large uploads
- Efficient JSON parsing with simd-json
- Async processing for concurrent operations

## Error Handling
- Comprehensive error responses with appropriate status codes
- Detailed server-side logging
- Security-conscious error messages (no information leakage)
- Graceful degradation for non-critical failures

## Testing Coverage
- WebSocket metrics functionality
- Permission enforcement logic
- Hash join optimization algorithms
- Selection vector operations
- Content-addressed file storage

## Integration Points
- Uses zauth for authentication and session management
- Integrates zexec-engine for SQL execution
- Connects to zann-hnsw for vector indexing
- Uses zcollections for NoSQL operations
- Manages zbrain job queue and zworker execution
- Serves web console built with Leptos framework
