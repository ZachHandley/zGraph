# zGraph — Unified Multi-Query-Language Database System

## 🚀 Enhanced Vision

**zGraph** is a revolutionary unified database system that seamlessly integrates **four powerful query languages** — **SQL, Cypher, SPARQL, and GraphQL** — with advanced vector capabilities, knowledge graph semantics, and traditional relational data management. Built on Fjall's robust LSM-tree storage foundation and engineered in Rust for maximum performance and safety.

> **One Database, Four Languages, Infinite Possibilities** — Query your data using the right language for the job, all backed by a single, consistent storage engine with unified security and transaction management.

## 🌟 Core Features

### 🔤 Multi-Query-Language Support
- **SQL** - Traditional relational queries with JOINs, aggregations, and window functions
- **Cypher** - Graph pattern matching and intuitive traversal queries
- **SPARQL** - Semantic web and RDF triple pattern queries
- **GraphQL** - Flexible API queries with real-time subscriptions
- **Hybrid Queries** - Seamlessly combine multiple languages in single operations

### 🧠 Advanced Knowledge Graph Engine
- **Triplet-based storage** with RDF-style semantics (subject-predicate-object)
- **Graph traversal algorithms** (BFS, DFS, Dijkstra's shortest path, PageRank)
- **Ontology support** for schema definitions and type systems
- **Graph visualization** and analytics capabilities

### 📊 High-Performance Relational Database
- **Fjall-backed relational tables** with flexible row values (scalars, JSON, vectors)
- **Full SQL support** with comprehensive query optimization via DataFusion
- **ACID transactions** with MVCC for data consistency
- **Schema migrations** and versioning support

### 🔍 Next-Generation Vector Store
- **Multi-dimensional vector support** - Different dimensionalities in same tables (image embeddings, text embeddings, audio embeddings)
- **Dynamic vector column regeneration** - Regenerate vector columns when embedding sizes change
- **Flexible vector schema** - Multiple vector columns with different dimensions for different modalities
- **Vector migration tools** - Seamless migration when updating embedding models or dimensionalities
- **SIMD-optimized vector operations** with runtime AVX2/SSE4.1/scalar dispatch
- **HNSW indexing** for approximate nearest neighbor search
- **Multiple distance metrics** (L2, Cosine, Dot Product, Jaccard)
- **Hybrid queries** combining vector similarity with structured filters

### 🔗 Rich Relations & Graph Analytics
- **Cross-table relations** with optional attributes
- **Cypher query support** for graph pattern matching
- **Edge types and properties** for rich relationship modeling
- **Graph algorithms** for analytics and recommendations

### 🛡️ Enterprise-Grade Security
- **Role-based access control (RBAC)** with fine-grained permissions
- **Row-level security** for multi-tenant applications
- **Column-level access control** for data privacy
- **Audit logging** for compliance requirements

## 💡 Unique Value Proposition

### What Makes zGraph Revolutionary?

1. **Query Language Freedom** - Use the right query language for your specific use case without database switching
2. **Unified Storage Engine** - Single source of truth with consistent performance and security across all query types
3. **Multi-Modal Vector Support** - Handle text, image, audio, and video embeddings in the same database with different dimensionalities
4. **Zero-Compromise Architecture** - No trade-offs between relational, graph, and vector capabilities
5. **Rust-Powered Performance** - Memory safety + zero-cost abstractions + fearless concurrency
6. **Production-Ready** - Built from the ground up for enterprise workloads with comprehensive testing

### Competitive Advantages

- **vs PostgreSQL + pgvector**: Native multi-language support + superior vector flexibility
- **vs Neo4j**: Built-in vector search + SQL compatibility + unified storage
- **vs Elasticsearch**: Graph capabilities + SQL support + true ACID transactions
- **vs Dedicated Vector DBs**: Relational + graph + semantic query capabilities in one system

## 🏗️ Architecture Highlights

### Core Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                    Query Language Layer                     │
├─────────────┬─────────────┬─────────────┬─────────────────┤
│    SQL      │   Cypher    │   SPARQL    │    GraphQL      │
│  (DataFusion) │  (Native)   │ (Spargebra) │ (Async-GraphQL) │
└─────────────┴─────────────┴─────────────┴─────────────────┘
┌─────────────────────────────────────────────────────────────┐
│                    Unified Query Engine                     │
│            (Cross-Language Optimization & Planning)         │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│                 Storage & Security Layer                    │
├─────────────┬─────────────┬─────────────┬─────────────────┤
│    MVCC     │     RBAC    │   Vectors   │   Transactions  │
└─────────────┴─────────────┴─────────────┴─────────────────┘
┌─────────────────────────────────────────────────────────────┐
│                    Fjall Storage Engine                     │
│              (LSM-Tree + Persistent Storage)                │
└─────────────────────────────────────────────────────────────┘
```

### Modular Design
- **zvec-kernels** - SIMD-optimized vector operations and kernels
- **zcore-storage** - Fjall-based storage layer with MVCC
- **zcore-catalog** - Schema and metadata management
- **zann-hnsw** - Approximate nearest neighbor indexing
- **zfluent-sql** - SQL query engine and optimization
- **zsql-parser** - SQL parsing and analysis
- **zpermissions** - Role-based access control system

### Best-in-Class Rust Ecosystem
- **DataFusion** for SQL query optimization
- **Fjall** for high-performance LSM-tree storage
- **Async-GraphQL** for type-safe GraphQL APIs
- **Spargebra** for SPARQL query processing
- **Custom SIMD kernels** for vector operations

## 🗺️ Implementation Roadmap

### 12-Week Comprehensive Development Plan

#### **Phase 1: Foundation (Weeks 1-3)**
- ✅ Project structure and core dependencies
- ✅ Fjall storage layer integration
- ✅ MVCC transaction system
- ✅ Basic SQL parsing and execution

#### **Phase 2: Multi-Language Support (Weeks 4-6)**
- 🔄 Cypher query language implementation
- 🔄 SPARQL semantic query support
- 🔄 GraphQL schema and resolvers
- 🔄 Cross-language query optimization

#### **Phase 3: Advanced Features (Weeks 7-9)**
- 📋 Vector store with HNSW indexing
- 📋 Multi-dimensional vector support
- 📋 Hybrid query capabilities
- 📋 Knowledge graph algorithms

#### **Phase 4: Production Readiness (Weeks 10-12)**
- 📋 Security and permissions system
- 📋 Performance optimization and benchmarking
- 📋 CLI tools and client libraries
- 📋 Documentation and examples

*See [PROJECT_TODO.md](PROJECT_TODO.md) for detailed task breakdown and technical specifications.*

## 🎯 Performance Targets

### Query Performance Benchmarks
- **SQL Queries**: <100ms for simple queries, <1s for complex joins
- **Cypher Queries**: <50ms for pattern matching, <500ms for traversals
- **Vector Search**: <10ms for 10K vectors, <100ms for 1M vectors
- **SPARQL Queries**: <200ms for standard patterns
- **GraphQL Queries**: <100ms for typical queries

### Scalability Metrics
- **Concurrent Queries**: 1,000+ concurrent queries
- **QPS**: 10,000+ queries per second
- **Triple Ingestion**: 50,000+ triples per second
- **Vector Ingestion**: 10,000+ vectors per second

### Storage Performance
- **Read Latency**: <1ms (p95), <5ms (p99)
- **Write Latency**: <2ms (p95), <10ms (p99)
- **Storage Efficiency**: 60-80% compression with columnar layouts

## 🔧 Advanced Vector Capabilities

### Multi-Dimensional Vector Support

zGraph revolutionizes vector storage by supporting **different dimensionalities within the same table**, enabling true multi-modal AI applications:

```sql
-- Create table with multiple vector types
CREATE TABLE content_items (
  id UUID PRIMARY KEY,
  title TEXT,
  content TEXT,

  -- Text embeddings (384 dimensions)
  text_embedding VECTOR(384),

  -- Image embeddings (1536 dimensions)
  image_embedding VECTOR(1536),

  -- Audio embeddings (512 dimensions)
  audio_embedding VECTOR(512),

  -- Combined multi-modal embedding (2048 dimensions)
  multimodal_embedding VECTOR(2048)
);
```

### Dynamic Vector Column Regeneration

When embedding models are updated, zGraph can **seamlessly regenerate vector columns** without downtime:

```sql
-- Regenerate text embeddings with new model
ALTER TABLE content_items
REGENERATE COLUMN text_embedding
USING 'sentence-transformers/all-MiniLM-L6-v2';

-- Vector column is automatically updated with new dimensionalities
```

### Flexible Vector Schema Evolution

Migrate between embedding models and dimensionalities with zero data loss:

```sql
-- Migrate from 384-dim to 768-dim embeddings
ALTER TABLE content_items
MIGRATE text_embedding TO VECTOR(768)
USING 'sentence-transformers/all-mpnet-base-v2';

-- Migration is handled automatically with proper indexing updates
```

### Vector Migration Tools

Comprehensive tooling for embedding model updates:

```bash
# CLI tool for bulk vector migration
zgraph migrate-vectors \
  --table content_items \
  --column text_embedding \
  --from-model "old-model-name" \
  --to-model "new-model-name" \
  --batch-size 1000

# Vector column analysis and optimization
zgraph analyze-vectors \
  --table content_items \
  --optimize-indexes \
  --suggest-dimensions
```

## 🚀 Getting Started

### Prerequisites
- Rust 1.70+ (recommended: latest stable)
- Git
- 8GB+ RAM for development (16GB+ recommended for production workloads)

### Installation

```bash
# Clone the repository
git clone https://github.com/zgraph/zgraph.git
cd zgraph

# Build the project
cargo build --release

# Run tests to verify installation
cargo test --workspace

# Start the zGraph server
cargo run --bin zgraph-server
```

### Feature Flags

zGraph uses feature flags for modular functionality:

```bash
# Full installation with all features
cargo build --release --features "full"

# Minimal installation (SQL + basic storage)
cargo build --release --features "sql,storage"

# Graph database features
cargo build --release --features "sql,storage,graph"

# Vector database features
cargo build --release --features "sql,storage,vectors"

# Semantic web features
cargo build --release --features "sql,storage,sparql"

# GraphQL API features
cargo build --release --features "sql,storage,graphql"

# Production-ready installation
cargo build --release --features "full,production"
```

### Quick Start Examples

#### SQL Queries
```bash
# Start SQL shell
cargo run --bin zgraph-sql

# Create tables and insert data
CREATE TABLE users (
  id UUID PRIMARY KEY,
  name TEXT,
  email TEXT,
  profile_vector VECTOR(768)
);

INSERT INTO users (id, name, email, profile_vector)
VALUES (
  gen_random_uuid(),
  'Alice Johnson',
  'alice@example.com',
  '[0.1, 0.2, 0.3, ...]' -- 768-dimensional vector
);
```

#### Graph Queries
```bash
# Start Cypher shell
cargo run --bin zgraph-cypher

# Create nodes and relationships
CREATE (alice:User {name: 'Alice', email: 'alice@example.com'})
CREATE (bob:User {name: 'Bob', email: 'bob@example.com'})
CREATE (alice)-[:FRIENDS_WITH]->(bob);

-- Find friends of friends
MATCH (user:User {name: 'Alice'})-[:FRIENDS_WITH]->(friend)-[:FRIENDS_WITH]->(fof)
WHERE user <> fof
RETURN DISTINCT fof.name;
```

#### Vector Search
```bash
# API-based vector search
curl -X POST http://localhost:8080/api/v1/vectors/search \
  -H "Content-Type: application/json" \
  -d '{
    "vector": [0.1, 0.2, 0.3, 0.4, 0.5],
    "metric": "cosine",
    "k": 10,
    "table": "users",
    "column": "profile_vector"
  }'
```

## 🎯 Use Cases

### Knowledge Management & Semantic Search
- **Enterprise knowledge graphs** with document relationships
- **Semantic search** across multiple content types
- **Expert discovery** based on skill similarity

### Recommendation Engines
- **Content recommendations** using hybrid graph-vector algorithms
- **Collaborative filtering** with social graph analysis
- **Personalization** using multi-modal embeddings

### Fraud Detection & Security
- **Transaction pattern analysis** with graph traversal
- **Anomaly detection** using vector similarity
- **Identity verification** with relationship mapping

### Bioinformatics & Research
- **Protein interaction networks** with semantic relationships
- **Gene expression analysis** with vector embeddings
- **Research collaboration graphs** with citation networks

### Content Management & Media
- **Multi-modal content analysis** (text, images, audio, video)
- **Content recommendation** with semantic understanding
- **Rights management** with complex relationship modeling

### IoT & Time-Series Data
- **Device relationship mapping** with graph topology
- **Anomaly detection** using time-series vectors
- **Predictive maintenance** with pattern analysis

## 🤝 Contributing

We welcome contributions! See our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Setup
```bash
# Clone and setup development environment
git clone https://github.com/zgraph/zgraph.git
cd zgraph

# Install development dependencies
cargo install cargo-watch cargo-nextest

# Run tests with nextest for better performance
cargo nextest run --workspace

# Run development server with hot reload
cargo watch -x 'run --bin zgraph-server'

# Run benchmarks
cargo bench --workspace
```

### Architecture Documentation
- [Storage Layer Design](docs/storage.md)
- [Query Engine Architecture](docs/query-engine.md)
- [Vector Operations Guide](docs/vectors.md)
- [Security Model](docs/security.md)

## 📄 License

Licensed under either [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.

## 🙏 Acknowledgments

- **Fjall** for the exceptional LSM-tree storage engine
- **DataFusion** for world-class SQL query optimization
- **Rust community** for inspiring performance and safety standards
- **Apache Arrow** for efficient columnar memory format

---

**zGraph** — The future of unified data querying is here. 🚀

*One database. Four query languages. Unlimited possibilities.*