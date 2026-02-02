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

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
  go build -a -o artemis \
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
# 16686 - HTTP API (Jaeger-compatible)
# 3200 - Tempo API
EXPOSE 4317 16686 3200

# Run server
# Pass flags as arguments to docker run, e.g.:
#   docker run artemis:latest -log.level=debug -retention-period=30d
ENTRYPOINT ["/app/artemis"]
