# Default Dockerfile - lightweight Alpine build without CGO
# This produces a smaller binary (~31MB) and container image
#
# Build with: docker build -t artemis:latest .
#
# Features:
# - Full OTLP trace ingestion
# - Jaeger and Tempo query APIs
# - Arrow IPC and Parquet storage
# - Block compaction and WAL
# - Web UI for querying metrics and exploring traces
#
# SQL querying is NOT available (returns "not supported" error)

# Stage 1: Build UI
FROM node:22-alpine AS ui-builder

WORKDIR /build/ui

# Copy package files
COPY ui/package*.json ./

# Install dependencies
RUN npm ci

# Copy UI source
COPY ui/ ./

# Build UI
RUN npm run build

# Stage 2: Build Go binary
FROM golang:1.25-alpine AS builder

ARG TARGETOS
ARG TARGETARCH

# Build variables
ARG VERSION
ARG REVISION
ARG BRANCH
ARG BUILDUSER
ARG BUILDDATE

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Copy built UI from ui-builder stage
COPY --from=ui-builder /build/ui/dist ./pkg/ui/dist

# Build without CGO (no DuckDB support) but with embedded UI
# This produces a smaller binary and doesn't require glibc
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
  go build -a -tags builtinassets -o artemis \
  -ldflags="-s -w \
  -X github.com/prometheus/common/version.Version=${VERSION} \
  -X github.com/prometheus/common/version.Revision=${REVISION} \
  -X github.com/prometheus/common/version.Branch=${BRANCH} \
  -X github.com/prometheus/common/version.BuildUser=${BUILDUSER} \
  -X github.com/prometheus/common/version.BuildDate=${BUILDDATE}" \
  cmd/artemis/main.go

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/artemis .

# Create data directories
RUN mkdir -p /data/wal /data/blocks

# Expose ports
# 4317 - OTLP gRPC
# 8080 - Query API and Web UI
# 16686 - HTTP API (Jaeger-compatible)
# 3200 - Tempo API
# 5433 - DuckDB SQL API (not available)
EXPOSE 4317 8080 16686 3200 5433

# Run server
# Pass flags as arguments to docker run, e.g.:
#   docker run artemis:latest -log.level=debug -retention-period=30d
ENTRYPOINT ["/app/artemis"]
