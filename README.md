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

## Usage

Artemis usage:
```
Artemis is a high-performance trace storage and query backend.

Usage:
  artemis [flags]
  artemis [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  help        Help about any command
  version     Print version information

Flags:
      --api-addr string                      HTTP API (Jaeger) address (default ":16686")
      --block-compaction-interval duration   How often to run block compaction (default 5m0s)
      --blocks-dir string                    Directory for persisted blocks (default "./data/blocks")
      --checkpoint-interval duration         How often to create WAL checkpoints (default 1m0s)
      --checkpoint-threshold int             Create checkpoint after N segments (default 5)
      --compact-interval duration            How often to flush pending data to Arrow batches (default 10s)
      --enable-compaction                    Enable automatic block compaction (default true)
      --enable-retention                     Enable automatic retention cleanup
  -h, --help                                 help for artemis
      --log.format string                    Output format of log messages. One of: [logfmt, json] (default "logfmt")
      --log.level string                     Only log messages with the given severity or above. One of: [debug, info, warn, error] (default "info")
      --max-block-duration duration          Maximum time range per block (default 2h0m0s)
      --max-block-spans int                  Maximum spans per block (default 1000000)
      --min-block-age-l0 duration            Minimum age before compacting L0 blocks (default 10m0s)
      --min-block-age-l1 duration            Minimum age before compacting L1 blocks (default 2h0m0s)
      --min-blocks-l0 int                    Minimum L0 blocks to trigger compaction (default 2)
      --min-blocks-l1 int                    Minimum L1 blocks to trigger compaction (default 2)
      --otlp-addr string                     OTLP gRPC receiver address (default ":4317")
      --retention-period duration            Delete blocks older than this (0 = no retention)
      --tempo-addr string                    Tempo API address (default ":3200")
      --wal-dir string                       Directory for WAL segments (default "./data/wal")
      --wal-segment-size int                 WAL segment size in bytes (default 128MB) (default 134217728)

Use "artemis [command] --help" for more information about a command.
```

artemistool usage
```
artemistool is a command-line interface for querying an Artemis trace server via its various APIs.

Usage:
  artemistool [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  help        Help about any command
  jaeger      Query Artemis using Jaeger API
  sql         Query Artemis using SQL API
  tempo       Query Artemis using Tempo API

Flags:
  -h, --help               help for artemistool
      --jaeger-port int    Jaeger API port (default 16686)
      --query-url string   Artemis server query endpoint URL (default "http://localhost")
      --sql-port int       SQL API port (default 5433)
      --tempo-port int     Tempo API port (default 3200)

Use "artemistool [command] --help" for more information about a command.
```

## Quick Start

### Standard Build (without SQL support)

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
  --jaeger-addr=:16686 \
  --tempo-addr=:3200
```

### DuckDB Build (with SQL query support)

```bash
# Build with DuckDB support (requires CGO)
make build-duckdb

# Run server
./bin/artemis-duckdb

# The SQL API will be available on port 5433 by default
./bin/artemis-duckdb --sqlapi-addr=:5433
```

### Build artemistool CLI

```bash
# Build the query tool
make build-tool

# Query using Jaeger API
./bin/artemistool jaeger search --service my-service --limit 10

# Query using SQL (requires DuckDB build)
./bin/artemistool sql --query "SELECT service_name, COUNT(*) as count FROM spans GROUP BY service_name"
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
