FROM golang:1.25-bookworm AS builder

ARG TARGETOS
ARG TARGETARCH

# Build variables
ARG VERSION
ARG REVISION
ARG BRANCH
ARG BUILDUSER
ARG BUILDDATE

WORKDIR /build

# Install build dependencies for CGO (gcc, g++, etc.)
# DuckDB requires glibc, which is available in Debian-based images
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

RUN CGO_ENABLED=1 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
  go build -a -o artemis \
  -ldflags="-s -w \
  -X github.com/prometheus/common/version.Version=${VERSION} \
  -X github.com/prometheus/common/version.Revision=${REVISION} \
  -X github.com/prometheus/common/version.Branch=${BRANCH} \
  -X github.com/prometheus/common/version.BuildUser=${BUILDUSER} \
  -X github.com/prometheus/common/version.BuildDate=${BUILDDATE}" \
  cmd/artemis/main.go

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libc6 \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/artemis .

# Create data directories
RUN mkdir -p /data/wal /data/blocks

# Expose ports
# 4317 - OTLP gRPC
# 16686 - HTTP API (Jaeger-compatible)
# 3200 - Tempo API
EXPOSE 4317 16686 3200

# Run server
# Pass flags as arguments to docker run, e.g.:
#   docker run artemis:latest -log.level=debug -retention-period=30d
ENTRYPOINT ["/app/artemis"]
