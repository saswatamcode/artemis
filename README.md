# Artemis

An experimental distributed tracing database built with Apache Arrow and Parquet.

> **⚠️ WARNING: EXPERIMENTAL - NOT PRODUCTION READY**
>
> This project is a proof-of-concept and is **NOT** suitable for production use. It lacks production-grade features.
> This is primarily an experimental project for exploring distributed tracing storage architectures.
>
> _Note: Portions of this codebase were developed with AI assistance._

## Features

- **OTLP Ingestion**: Native OpenTelemetry Protocol (OTLP) support via gRPC
- **Dual Query APIs**: Jaeger-compatible and Tempo-compatible HTTP APIs
- **Columnar Storage**: Arrow for hot data (L0), Parquet for cold data (L1+)
- **Leveled Compaction**: Multi-level compaction with automatic block merging
- **Write-Ahead Log**: Durable writes with checkpointing and crash recovery
- **Efficient Indexing**: Fast lookups by trace ID, span ID, and tags

## Quick Start

```bash
# Build
make build

# Run server
./bin/artemis

# Or with custom configuration
./bin/artemis \
  --wal-dir=./data/wal \
  --blocks-dir=./data/blocks \
  --otlp-addr=:4317 \
  --api-addr=:16686 \
  --tempo-addr=:3200
```

## Architecture

Artemis uses a tiered storage architecture:

1. **WAL**: All writes are first written to a write-ahead log for durability
2. **Head Block** (L0): In-memory Arrow columnar storage with indexes
3. **Persisted Blocks** (L0): Arrow IPC files on disk
4. **Compacted Blocks** (L1+): Parquet files with optimized compression

### Compaction Levels

- **L0**: Arrow IPC format (fast writes, good for queries)
- **L1**: Parquet format (compressed, optimized for cold storage)
- **L2**: Larger Parquet blocks (long-term retention)

Blocks are automatically compacted as they age, similar to LSM-tree databases.

## APIs

### OTLP Receiver
```bash
# Send traces via OTLP gRPC
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
```

### Jaeger API
```bash
# Search traces
curl "http://localhost:16686/api/traces?service=my-service"

# Get trace by ID
curl "http://localhost:16686/api/traces/{traceID}"

# List services
curl "http://localhost:16686/api/services"
```

### Tempo API
```bash
# Search with TraceQL
curl "http://localhost:3200/api/search?q={service.name=my-service}"

# Get trace by ID (OTLP format)
curl "http://localhost:3200/api/traces/{traceID}"

# List tags
curl "http://localhost:3200/api/v2/search/tags"
```
