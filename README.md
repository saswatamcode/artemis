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
- **Flight SQL API**: Apache Arrow Flight SQL for efficient SQL queries over spans
- **Dual Query APIs**: Jaeger-compatible and Tempo-compatible HTTP APIs
- **Columnar Storage**: Arrow for hot data (L0), Parquet for cold data (L1+)
- **Leveled Compaction**: Multi-level compaction with automatic block merging
- **Write-Ahead Log**: Durable writes with checkpointing and crash recovery
- **Efficient Indexing**: Fast lookups by trace ID, span ID, and tags

## Quick Start

```bash
# Build (creates both artemis and artemis-query)
make build

# Run server
./bin/artemis

# Or with custom configuration
./bin/artemis \
  --wal-dir=./data/wal \
  --blocks-dir=./data/blocks \
  --otlp-addr=:4317 \
  --api-addr=:16686 \
  --tempo-addr=:3200 \
  --flight-addr=:8815
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

### Use Telemetrygen
```bash
telemetrygen traces --otlp-endpoint localhost:4317 --otlp-insecure --traces 10 --service "my-test-service" --child-spans 5
```

This generates 10 traces with 5 child spans each over 10 seconds.

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

### Flight SQL API

Artemis exposes trace data via Apache Arrow Flight SQL, enabling SQL queries with native Arrow format results.

#### Schema

The `spans` table exposes the following columns:

| Column          | Type      | Nullable | Description                    |
|----------------|-----------|----------|--------------------------------|
| trace_id       | String    | No       | Trace identifier               |
| span_id        | String    | No       | Span identifier                |
| parent_span_id | String    | Yes      | Parent span identifier         |
| name           | String    | No       | Span name/operation            |
| start_time     | Int64     | No       | Start timestamp (nanoseconds)  |
| end_time       | Int64     | No       | End timestamp (nanoseconds)    |
| duration       | Int64     | No       | Duration (nanoseconds)         |
| service_name   | String    | No       | Service name                   |
| tags           | Map<String, String> | Yes | Span attributes/tags    |

#### Supported SQL Features

- **SELECT**: Column projection (`*` or specific columns)
- **WHERE**: Filtering with `=`, `!=`, `LIKE`, `>=`, `<=`
- **ORDER BY**: Sorting (ASC/DESC)
- **LIMIT**: Result limiting
- **Time ranges**: Filter by `start_time` and `end_time`

#### Using artemis-query CLI Tool

The `artemis-query` tool provides a simple way to query your telemetrygen data:

```bash
# Terminal 3: Query the data
./bin/artemis-query -query "SELECT * FROM spans WHERE service_name = 'my-test-service' LIMIT 10"
```

```bash
# Count all spans
./bin/artemis-query -query "SELECT * FROM spans"

# Find slowest operations
./bin/artemis-query -query "SELECT name, service_name, duration FROM spans ORDER BY duration DESC LIMIT 10"

# Filter by service
./bin/artemis-query -query "SELECT trace_id, name, duration FROM spans WHERE service_name = 'my-test-service' LIMIT 20"

# Find fast operations (< 100ms)
./bin/artemis-query -query "SELECT name, duration FROM spans WHERE duration < 100000000 ORDER BY duration ASC LIMIT 10"

# Get root spans only
./bin/artemis-query -query "SELECT trace_id, span_id, name FROM spans WHERE parent_span_id = '' LIMIT 10"
```

#### we may can add

- **PromQL Translation**: Query with PromQL syntax, translated to SQL
- **JOIN Support**: Join traces with service metadata
- **Aggregations**: COUNT, AVG, SUM, GROUP BY
- **Prepared Statements**: Parameter binding for performance
- **Virtual Tables**: Pre-aggregated views (trace_summary, service_stats)
