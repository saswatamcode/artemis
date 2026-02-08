package e2e

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/efficientgo/e2e"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// buildArtemisImage builds the Artemis Docker image from the Dockerfile
func buildArtemisImage() error {
	cmd := exec.Command("docker", "build", "--load", "-t", "artemis:latest", "..")
	cmd.Dir = "."
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// createArtemis creates the Artemis runnable
func createArtemis(env e2e.Environment) e2e.Runnable {
	ports := map[string]int{
		"otlp":  4317,  // OTLP gRPC
		"http":  16686, // HTTP API (Jaeger-compatible)
		"tempo": 3200,  // Tempo API
	}

	f := env.Runnable("artemis").WithPorts(ports).Future()

	// Create data directory in current working directory for easy access
	// This makes it easy to inspect WAL and blocks while the test is running
	cwd, err := os.Getwd()
	if err != nil {
		return e2e.NewFailedRunnable("artemis", fmt.Errorf("failed to get working directory: %w", err))
	}

	// Store data in ./e2e-data relative to the e2e directory
	dataDir := filepath.Join(cwd, "e2e-data")

	// // Clean up old data from previous runs
	if err := os.RemoveAll(dataDir); err != nil && !os.IsNotExist(err) {
		return e2e.NewFailedRunnable("artemis", fmt.Errorf("failed to clean old data dir: %w", err))
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return e2e.NewFailedRunnable("artemis", fmt.Errorf("failed to create data dir: %w", err))
	}

	fmt.Printf("\n📁 Artemis data directory: %s\n", dataDir)
	fmt.Printf("   View WAL segments:     ls -lh %s/wal/\n", dataDir)
	fmt.Printf("   View persisted blocks: ls -lh %s/blocks/\n\n", dataDir)

	// Check if demo mode is enabled for aggressive multi-level compaction
	opts := e2e.StartOptions{
		Image: "artemis:latest",
		Volumes: []string{
			// Mount to /app/data since that's where artemis writes (WORKDIR is /app)
			dataDir + ":/app/data",
		},
		EnvVars: map[string]string{
			"DEBUG": "1", // Enable debug logging to see database operations
		},
		Readiness: e2e.NewHTTPReadinessProbe(
			"http",
			"/health",
			200,
			200,
		),
	}

	// Override with aggressive settings for demo mode
	if os.Getenv("ARTEMIS_DEMO_MODE") == "1" {
		fmt.Println("🔥 DEMO MODE ENABLED - Aggressive multi-level compaction")
		fmt.Println("   This demonstrates the full database lifecycle:")
		fmt.Println("   • WAL segments rotate every ~64KB (vs 128MB default)")
		fmt.Println("   • Head → L0 Arrow IPC (every ~50 spans or 30s)")
		fmt.Println("   • L0 → L1 → L2 → L3 Parquet (every 8s, 10s min age)")
		fmt.Println("   • WAL checkpoints (every 10s, threshold 1 segment)")
		fmt.Println("   Watch: ls -lh e2e-data/blocks/")
		fmt.Println("   Watch: ls -lh e2e-data/wal/")
		fmt.Println()

		opts.Command = e2e.NewCommand(
			"--wal-segment-size=65536", // 64KB WAL segments (vs 128MB default)
			"--compact-interval=5s",
			"--checkpoint-interval=10s",
			"--checkpoint-threshold=1", // Checkpoint after just 1 segment
			"--block-compaction-interval=8s",
			"--max-block-duration=30s",
			"--max-block-spans=50",
			"--min-block-age-l0=10s", // L0 blocks can compact after 10s
			"--min-blocks-l0=2",      // Need 2 L0 blocks
			"--min-block-age-l1=20s", // L1 blocks can compact after 20s
			"--min-blocks-l1=2",      // Need 2 L1 blocks
			"--log.level=info",
		)
	}

	return f.Init(opts)
}

// TestArtemisE2E is a comprehensive end-to-end test that:
// 1. Spins up Artemis instance
// 2. Sends OTLP traces via gRPC
// 3. Queries them back using both Jaeger and Tempo APIs
// 4. Validates the received data matches what was sent
//
// Run with: go test -v ./e2e -run TestArtemisE2E
func TestArtemisE2E(t *testing.T) {
	fmt.Println("=== Building Artemis Docker image...")
	testutil.Ok(t, buildArtemisImage())

	fmt.Println("=== Creating e2e environment...")
	env, err := e2e.New()
	testutil.Ok(t, err)
	t.Cleanup(env.Close)

	fmt.Println("=== Starting Artemis...")
	artemis := createArtemis(env)
	testutil.Ok(t, e2e.StartAndWaitReady(artemis))
	fmt.Printf("✓ Artemis ready - OTLP: %s, Jaeger API: %s, Tempo API: %s\n",
		artemis.Endpoint("otlp"), artemis.Endpoint("http"), artemis.Endpoint("tempo"))

	// Give Artemis a moment to fully initialize
	time.Sleep(2 * time.Second)

	fmt.Println("\n=== Testing OTLP trace ingestion and query APIs...")

	// Test Case 1: Simple single-service trace
	t.Run("SingleServiceTrace", func(t *testing.T) {
		testSingleServiceTrace(t, artemis)
	})

	// Test Case 2: Multi-service distributed trace
	t.Run("MultiServiceTrace", func(t *testing.T) {
		testMultiServiceTrace(t, artemis)
	})

	// Test Case 3: Trace with errors
	t.Run("ErrorTrace", func(t *testing.T) {
		testErrorTrace(t, artemis)
	})

	// Test Case 4: Complex nested trace
	t.Run("NestedTrace", func(t *testing.T) {
		testNestedTrace(t, artemis)
	})

	// Test Case 5: Query by service and tags
	t.Run("QueryByServiceAndTags", func(t *testing.T) {
		testQueryByServiceAndTags(t, artemis)
	})

	// Note: SQL queries test removed - requires DuckDB build tag and API endpoint
	// SQL querying is an experimental feature that needs to be explicitly enabled

	fmt.Println("\n=== ✅ All E2E tests passed! ===")
}

// testSingleServiceTrace tests a simple trace with a single service
func testSingleServiceTrace(t *testing.T, artemis e2e.Runnable) {
	fmt.Println("\n--- Test: Single Service Trace ---")

	// Generate a unique trace ID
	traceID := generateTraceID()
	spanID1 := generateSpanID()
	spanID2 := generateSpanID()

	// Create OTLP trace with 2 spans
	now := time.Now()
	trace := &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{Key: "service.name", Value: stringValue("api-server")},
						{Key: "service.version", Value: stringValue("1.0.0")},
						{Key: "deployment.environment", Value: stringValue("production")},
					},
				},
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Scope: &commonv1.InstrumentationScope{
							Name:    "test-instrumentation",
							Version: "1.0.0",
						},
						Spans: []*tracev1.Span{
							{
								TraceId:           traceID,
								SpanId:            spanID1,
								Name:              "GET /api/users",
								Kind:              tracev1.Span_SPAN_KIND_SERVER,
								StartTimeUnixNano: uint64(now.UnixNano()),
								EndTimeUnixNano:   uint64(now.Add(100 * time.Millisecond).UnixNano()),
								Attributes: []*commonv1.KeyValue{
									{Key: "http.method", Value: stringValue("GET")},
									{Key: "http.route", Value: stringValue("/api/users")},
									{Key: "http.status_code", Value: intValue(200)},
								},
							},
							{
								TraceId:           traceID,
								SpanId:            spanID2,
								ParentSpanId:      spanID1,
								Name:              "database query",
								Kind:              tracev1.Span_SPAN_KIND_CLIENT,
								StartTimeUnixNano: uint64(now.Add(10 * time.Millisecond).UnixNano()),
								EndTimeUnixNano:   uint64(now.Add(80 * time.Millisecond).UnixNano()),
								Attributes: []*commonv1.KeyValue{
									{Key: "db.system", Value: stringValue("postgresql")},
									{Key: "db.statement", Value: stringValue("SELECT * FROM users")},
								},
							},
						},
					},
				},
			},
		},
	}

	// Send trace via OTLP gRPC
	fmt.Println("  Sending trace via OTLP gRPC...")
	testutil.Ok(t, sendOTLPTrace(artemis.Endpoint("otlp"), trace))
	fmt.Printf("  ✓ Sent trace %s with 2 spans\n", hex.EncodeToString(traceID))

	// Wait for ingestion
	time.Sleep(2 * time.Second)

	// Query via Jaeger API
	fmt.Println("  Querying via Jaeger API...")
	jaegerTrace := queryJaegerTrace(t, artemis, traceID)
	testutil.Assert(t, jaegerTrace != nil, "Jaeger trace should not be nil")
	testutil.Equals(t, hex.EncodeToString(traceID), jaegerTrace.TraceID)
	testutil.Equals(t, 2, len(jaegerTrace.Spans), "should have 2 spans")

	// Validate Jaeger trace structure (spans may be in any order)
	spanNames := make(map[string]bool)
	for _, span := range jaegerTrace.Spans {
		validateJaegerSpan(t, span, hex.EncodeToString(traceID), span.OperationName)
		spanNames[span.OperationName] = true
	}
	testutil.Assert(t, spanNames["GET /api/users"], "should have 'GET /api/users' span")
	testutil.Assert(t, spanNames["database query"], "should have 'database query' span")

	// Verify parent-child relationship
	foundParent := false
	for _, span := range jaegerTrace.Spans {
		if len(span.References) > 0 {
			testutil.Equals(t, "CHILD_OF", span.References[0].RefType)
			foundParent = true
		}
	}
	testutil.Assert(t, foundParent, "should have parent-child relationship")
	fmt.Println("  ✓ Jaeger API validation passed")

	// Query via Tempo API
	fmt.Println("  Querying via Tempo API...")
	tempoTrace := queryTempoTrace(t, artemis, traceID)
	testutil.Assert(t, tempoTrace != nil, "Tempo trace should not be nil")
	fmt.Println("  ✓ Tempo API validation passed")

	// Verify service discovery
	services := queryServices(t, artemis)
	testutil.Assert(t, contains(services, "api-server"), "should discover api-server service")
	fmt.Println("  ✓ Service discovery working")

	fmt.Println("  ✅ Single service trace test passed!")
}

// testMultiServiceTrace tests a distributed trace across multiple services
func testMultiServiceTrace(t *testing.T, artemis e2e.Runnable) {
	fmt.Println("\n--- Test: Multi-Service Distributed Trace ---")

	traceID := generateTraceID()
	frontendSpanID := generateSpanID()
	apiSpanID := generateSpanID()
	dbSpanID := generateSpanID()

	now := time.Now()

	// Frontend service
	frontendTrace := &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{Key: "service.name", Value: stringValue("frontend")},
					},
				},
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Spans: []*tracev1.Span{
							{
								TraceId:           traceID,
								SpanId:            frontendSpanID,
								Name:              "HTTP GET /checkout",
								Kind:              tracev1.Span_SPAN_KIND_SERVER,
								StartTimeUnixNano: uint64(now.UnixNano()),
								EndTimeUnixNano:   uint64(now.Add(200 * time.Millisecond).UnixNano()),
								Attributes: []*commonv1.KeyValue{
									{Key: "http.method", Value: stringValue("GET")},
									{Key: "component", Value: stringValue("frontend")},
								},
							},
						},
					},
				},
			},
		},
	}

	// API service
	apiTrace := &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{Key: "service.name", Value: stringValue("payment-api")},
					},
				},
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Spans: []*tracev1.Span{
							{
								TraceId:           traceID,
								SpanId:            apiSpanID,
								ParentSpanId:      frontendSpanID,
								Name:              "process payment",
								Kind:              tracev1.Span_SPAN_KIND_SERVER,
								StartTimeUnixNano: uint64(now.Add(10 * time.Millisecond).UnixNano()),
								EndTimeUnixNano:   uint64(now.Add(150 * time.Millisecond).UnixNano()),
								Attributes: []*commonv1.KeyValue{
									{Key: "payment.amount", Value: stringValue("99.99")},
									{Key: "payment.currency", Value: stringValue("USD")},
								},
							},
						},
					},
				},
			},
		},
	}

	// Database service
	dbTrace := &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{Key: "service.name", Value: stringValue("postgres")},
					},
				},
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Spans: []*tracev1.Span{
							{
								TraceId:           traceID,
								SpanId:            dbSpanID,
								ParentSpanId:      apiSpanID,
								Name:              "INSERT INTO payments",
								Kind:              tracev1.Span_SPAN_KIND_CLIENT,
								StartTimeUnixNano: uint64(now.Add(20 * time.Millisecond).UnixNano()),
								EndTimeUnixNano:   uint64(now.Add(140 * time.Millisecond).UnixNano()),
								Attributes: []*commonv1.KeyValue{
									{Key: "db.system", Value: stringValue("postgresql")},
									{Key: "db.operation", Value: stringValue("INSERT")},
								},
							},
						},
					},
				},
			},
		},
	}

	// Send all traces
	fmt.Println("  Sending multi-service trace...")
	testutil.Ok(t, sendOTLPTrace(artemis.Endpoint("otlp"), frontendTrace))
	testutil.Ok(t, sendOTLPTrace(artemis.Endpoint("otlp"), apiTrace))
	testutil.Ok(t, sendOTLPTrace(artemis.Endpoint("otlp"), dbTrace))
	fmt.Printf("  ✓ Sent trace %s across 3 services\n", hex.EncodeToString(traceID))

	time.Sleep(2 * time.Second)

	// Query and validate
	jaegerTrace := queryJaegerTrace(t, artemis, traceID)
	testutil.Assert(t, jaegerTrace != nil, "trace should exist")
	testutil.Equals(t, 3, len(jaegerTrace.Spans), "should have 3 spans")
	testutil.Equals(t, 3, len(jaegerTrace.Processes), "should have 3 processes/services")

	// Verify all services are present
	services := make(map[string]bool)
	for _, proc := range jaegerTrace.Processes {
		services[proc.ServiceName] = true
	}
	testutil.Assert(t, services["frontend"], "should have frontend service")
	testutil.Assert(t, services["payment-api"], "should have payment-api service")
	testutil.Assert(t, services["postgres"], "should have postgres service")

	fmt.Println("  ✅ Multi-service trace test passed!")
}

// testErrorTrace tests a trace with error status
func testErrorTrace(t *testing.T, artemis e2e.Runnable) {
	fmt.Println("\n--- Test: Error Trace ---")

	traceID := generateTraceID()
	spanID := generateSpanID()

	now := time.Now()
	trace := &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{Key: "service.name", Value: stringValue("error-service")},
					},
				},
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Spans: []*tracev1.Span{
							{
								TraceId:           traceID,
								SpanId:            spanID,
								Name:              "failing operation",
								Kind:              tracev1.Span_SPAN_KIND_INTERNAL,
								StartTimeUnixNano: uint64(now.UnixNano()),
								EndTimeUnixNano:   uint64(now.Add(50 * time.Millisecond).UnixNano()),
								Status: &tracev1.Status{
									Code:    tracev1.Status_STATUS_CODE_ERROR,
									Message: "internal server error",
								},
								Attributes: []*commonv1.KeyValue{
									{Key: "error", Value: boolValue(true)},
									{Key: "error.type", Value: stringValue("InternalError")},
								},
							},
						},
					},
				},
			},
		},
	}

	fmt.Println("  Sending error trace...")
	testutil.Ok(t, sendOTLPTrace(artemis.Endpoint("otlp"), trace))
	fmt.Printf("  ✓ Sent error trace %s\n", hex.EncodeToString(traceID))

	time.Sleep(2 * time.Second)

	// Query and validate error is preserved
	jaegerTrace := queryJaegerTrace(t, artemis, traceID)
	testutil.Assert(t, jaegerTrace != nil, "trace should exist")
	testutil.Equals(t, 1, len(jaegerTrace.Spans), "should have 1 span")

	// Check for error tag
	foundError := false
	for _, tag := range jaegerTrace.Spans[0].Tags {
		if tag.Key == "error" && tag.Value == true {
			foundError = true
			break
		}
	}
	testutil.Assert(t, foundError, "should have error tag")

	fmt.Println("  ✅ Error trace test passed!")
}

// testNestedTrace tests a deeply nested trace
func testNestedTrace(t *testing.T, artemis e2e.Runnable) {
	fmt.Println("\n--- Test: Nested Trace ---")

	traceID := generateTraceID()
	spanIDs := make([][]byte, 5)
	for i := range spanIDs {
		spanIDs[i] = generateSpanID()
	}

	now := time.Now()
	spans := make([]*tracev1.Span, 5)

	// Create nested chain: span0 -> span1 -> span2 -> span3 -> span4
	for i := range 5 {
		var parentSpanID []byte
		if i > 0 {
			parentSpanID = spanIDs[i-1]
		}

		spans[i] = &tracev1.Span{
			TraceId:           traceID,
			SpanId:            spanIDs[i],
			ParentSpanId:      parentSpanID,
			Name:              fmt.Sprintf("operation-level-%d", i),
			Kind:              tracev1.Span_SPAN_KIND_INTERNAL,
			StartTimeUnixNano: uint64(now.Add(time.Duration(i*10) * time.Millisecond).UnixNano()),
			EndTimeUnixNano:   uint64(now.Add(time.Duration(100-i*5) * time.Millisecond).UnixNano()),
			Attributes: []*commonv1.KeyValue{
				{Key: "level", Value: intValue(int64(i))},
			},
		}
	}

	trace := &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{Key: "service.name", Value: stringValue("nested-service")},
					},
				},
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Spans: spans,
					},
				},
			},
		},
	}

	fmt.Println("  Sending nested trace with 5 levels...")
	testutil.Ok(t, sendOTLPTrace(artemis.Endpoint("otlp"), trace))
	fmt.Printf("  ✓ Sent nested trace %s\n", hex.EncodeToString(traceID))

	time.Sleep(2 * time.Second)

	// Query and validate structure
	jaegerTrace := queryJaegerTrace(t, artemis, traceID)
	testutil.Assert(t, jaegerTrace != nil, "trace should exist")
	testutil.Equals(t, 5, len(jaegerTrace.Spans), "should have 5 spans")

	// Verify nesting (each span except root should have parent)
	parentCount := 0
	for _, span := range jaegerTrace.Spans {
		if len(span.References) > 0 {
			parentCount++
		}
	}
	testutil.Equals(t, 4, parentCount, "should have 4 child spans")

	fmt.Println("  ✅ Nested trace test passed!")
}

// testQueryByServiceAndTags tests querying by service name and tags
func testQueryByServiceAndTags(t *testing.T, artemis e2e.Runnable) {
	fmt.Println("\n--- Test: Query by Service and Tags ---")

	// Send multiple traces with different services and tags
	traces := []struct {
		service string
		env     string
		method  string
	}{
		{"web-server", "production", "GET"},
		{"web-server", "staging", "POST"},
		{"auth-service", "production", "GET"},
	}

	var traceIDs [][]byte
	for _, tc := range traces {
		traceID := generateTraceID()
		traceIDs = append(traceIDs, traceID)

		now := time.Now()
		trace := &collectortracev1.ExportTraceServiceRequest{
			ResourceSpans: []*tracev1.ResourceSpans{
				{
					Resource: &resourcev1.Resource{
						Attributes: []*commonv1.KeyValue{
							{Key: "service.name", Value: stringValue(tc.service)},
							{Key: "deployment.environment", Value: stringValue(tc.env)},
						},
					},
					ScopeSpans: []*tracev1.ScopeSpans{
						{
							Spans: []*tracev1.Span{
								{
									TraceId:           traceID,
									SpanId:            generateSpanID(),
									Name:              "test operation",
									Kind:              tracev1.Span_SPAN_KIND_SERVER,
									StartTimeUnixNano: uint64(now.UnixNano()),
									EndTimeUnixNano:   uint64(now.Add(10 * time.Millisecond).UnixNano()),
									Attributes: []*commonv1.KeyValue{
										{Key: "http.method", Value: stringValue(tc.method)},
									},
								},
							},
						},
					},
				},
			},
		}
		testutil.Ok(t, sendOTLPTrace(artemis.Endpoint("otlp"), trace))
	}

	fmt.Println("  Sent 3 traces with different services and tags")
	time.Sleep(2 * time.Second)

	// Query for web-server service
	httpClient := &http.Client{Timeout: 10 * time.Second}
	servicesURL := fmt.Sprintf("http://%s/api/services", artemis.Endpoint("http"))
	resp, err := httpClient.Get(servicesURL)
	testutil.Ok(t, err)
	defer resp.Body.Close()

	var servicesResp struct {
		Data []string `json:"data"`
	}
	testutil.Ok(t, json.NewDecoder(resp.Body).Decode(&servicesResp))

	testutil.Assert(t, contains(servicesResp.Data, "web-server"), "should have web-server")
	testutil.Assert(t, contains(servicesResp.Data, "auth-service"), "should have auth-service")

	fmt.Println("  ✅ Query by service test passed!")
}

// NOTE: SQL query tests removed from e2e suite
// SQL querying is an experimental feature that requires:
// 1. Building with -tags duckdb (requires CGO)
// 2. API endpoint implementation for /api/query/sql
// 3. Only works with persisted blocks, not in-memory head block
//
// The SQL querier implementation exists in pkg/query/sql_querier_duckdb.go
// but is not exposed via HTTP API by default.
//
// To test SQL queries manually:
// 1. Build artemis with: go build -tags duckdb
// 2. Implement HTTP endpoint for SQL queries
// 3. Ensure data is persisted to blocks (wait for compaction)

// testSQLQueries tests SQL query functionality using DuckDB (DISABLED)
func testSQLQueries(t *testing.T, artemis e2e.Runnable) {
	t.Skip("SQL queries test disabled - requires DuckDB build tag and API endpoint")
	fmt.Println("\n--- Test: SQL Queries ---")

	// Send test traces with varied characteristics for SQL testing
	// Note: SQL querier only queries persisted blocks, not the in-memory head
	// We need to send enough spans to trigger a block flush or wait for compaction
	fmt.Println("  Sending test traces for SQL queries...")

	traces := []struct {
		service  string
		spanName string
		duration time.Duration
		count    int
	}{
		{"sql-test-api", "GET /users", 100 * time.Millisecond, 15},
		{"sql-test-api", "POST /users", 200 * time.Millisecond, 15},
		{"sql-test-db", "SELECT users", 50 * time.Millisecond, 15},
		{"sql-test-cache", "GET cache", 10 * time.Millisecond, 15},
	}

	totalSpans := 0
	for _, tc := range traces {
		for i := 0; i < tc.count; i++ {
			traceID := generateTraceID()
			now := time.Now()
			trace := &collectortracev1.ExportTraceServiceRequest{
				ResourceSpans: []*tracev1.ResourceSpans{
					{
						Resource: &resourcev1.Resource{
							Attributes: []*commonv1.KeyValue{
								{Key: "service.name", Value: stringValue(tc.service)},
							},
						},
						ScopeSpans: []*tracev1.ScopeSpans{
							{
								Spans: []*tracev1.Span{
									{
										TraceId:           traceID,
										SpanId:            generateSpanID(),
										Name:              tc.spanName,
										Kind:              tracev1.Span_SPAN_KIND_SERVER,
										StartTimeUnixNano: uint64(now.UnixNano()),
										EndTimeUnixNano:   uint64(now.Add(tc.duration).UnixNano()),
										Attributes: []*commonv1.KeyValue{
											{Key: "test.group", Value: stringValue("sql-test")},
										},
									},
								},
							},
						},
					},
				},
			}
			testutil.Ok(t, sendOTLPTrace(artemis.Endpoint("otlp"), trace))
			totalSpans++
		}
	}

	fmt.Printf("  ✓ Sent %d spans across %d services\n", totalSpans, len(traces))

	// Wait longer for spans to be flushed to L0 blocks (compaction happens periodically)
	// Default settings: compact-interval=1m, max-block-spans=10000, max-block-duration=1h
	// We need to wait for at least one compaction cycle
	fmt.Println("  Waiting for spans to be persisted to blocks...")
	time.Sleep(65 * time.Second)

	httpClient := &http.Client{Timeout: 10 * time.Second}

	// Test Case 1: COUNT all spans
	fmt.Println("  Testing COUNT query...")
	countResult := executeSQLQuery(t, artemis, httpClient, "SELECT COUNT(*) as total FROM spans")
	testutil.Assert(t, countResult.Success, "count query should succeed")
	testutil.Assert(t, len(countResult.Rows) > 0, "count query should return rows")
	testutil.Assert(t, countResult.RowCount > 0, "should have row count")
	fmt.Println("  ✓ COUNT query passed")

	// Test Case 2: GROUP BY service_name
	fmt.Println("  Testing GROUP BY service_name...")
	groupResult := executeSQLQuery(t, artemis, httpClient,
		"SELECT service_name, COUNT(*) as span_count FROM spans WHERE service_name LIKE 'sql-test-%' GROUP BY service_name ORDER BY service_name")
	testutil.Assert(t, groupResult.Success, "group by query should succeed")
	testutil.Assert(t, len(groupResult.Rows) >= 3, "should have at least 3 service groups")
	testutil.Assert(t, groupResult.Columns != nil, "should have column info")
	testutil.Assert(t, contains(groupResult.Columns, "service_name"), "should have service_name column")
	testutil.Assert(t, contains(groupResult.Columns, "span_count"), "should have span_count column")
	fmt.Println("  ✓ GROUP BY query passed")

	// Test Case 3: Average duration calculation
	fmt.Println("  Testing AVG duration query...")
	avgResult := executeSQLQuery(t, artemis, httpClient,
		"SELECT service_name, AVG(duration) as avg_duration FROM spans WHERE service_name LIKE 'sql-test-%' GROUP BY service_name")
	testutil.Assert(t, avgResult.Success, "avg duration query should succeed")
	testutil.Assert(t, len(avgResult.Rows) >= 3, "should have service averages")
	fmt.Println("  ✓ AVG duration query passed")

	// Test Case 4: Full span selection (should return traces)
	fmt.Println("  Testing full span SELECT...")
	spanResult := executeSQLQuery(t, artemis, httpClient,
		"SELECT * FROM spans WHERE service_name = 'sql-test-cache' LIMIT 5")
	testutil.Assert(t, spanResult.Success, "span select should succeed")
	testutil.Assert(t, len(spanResult.Traces) > 0, "should return traces for full span query")
	testutil.Assert(t, spanResult.RowCount > 0, "should have spans")
	testutil.Assert(t, spanResult.RowCount <= 5, "should respect LIMIT")

	// Validate trace structure
	for _, trace := range spanResult.Traces {
		testutil.Assert(t, trace.TraceID != "", "trace should have trace ID")
		testutil.Assert(t, trace.SpanID != "", "trace should have span ID")
		testutil.Assert(t, trace.Name != "", "trace should have name")
		testutil.Assert(t, trace.ServiceName == "sql-test-cache", "should match service filter")
	}
	fmt.Println("  ✓ Full span SELECT passed")

	// Test Case 5: Time-based filtering with span projection
	fmt.Println("  Testing time-based filtering...")
	timeResult := executeSQLQuery(t, artemis, httpClient,
		"SELECT service_name, name, duration FROM spans WHERE service_name = 'sql-test-api' AND duration > 100000000")
	testutil.Assert(t, timeResult.Success, "time filter query should succeed")
	testutil.Assert(t, len(timeResult.Rows) > 0, "should find spans with duration > 100ms")
	fmt.Println("  ✓ Time-based filtering passed")

	// Test Case 6: Complex analytical query
	fmt.Println("  Testing complex analytical query...")
	analyticsResult := executeSQLQuery(t, artemis, httpClient, `
		SELECT
			service_name,
			COUNT(*) as total_spans,
			MIN(duration) as min_duration,
			MAX(duration) as max_duration,
			AVG(duration) as avg_duration
		FROM spans
		WHERE service_name LIKE 'sql-test-%'
		GROUP BY service_name
		ORDER BY total_spans DESC
	`)
	testutil.Assert(t, analyticsResult.Success, "analytics query should succeed")
	testutil.Assert(t, len(analyticsResult.Rows) >= 3, "should have analytics for services")
	testutil.Assert(t, len(analyticsResult.Columns) == 5, "should have 5 columns")
	fmt.Println("  ✓ Complex analytical query passed")

	fmt.Println("  ✅ All SQL query tests passed!")
}

// executeSQLQuery executes a SQL query against the Artemis API and returns the result
func executeSQLQuery(t *testing.T, artemis e2e.Runnable, httpClient *http.Client, query string) *SQLQueryResponse {
	t.Helper()

	url := fmt.Sprintf("http://%s/api/query/sql", artemis.Endpoint("http"))

	requestBody := map[string]string{
		"query": query,
	}
	requestJSON, err := json.Marshal(requestBody)
	testutil.Ok(t, err)

	// Make the POST request with the JSON body
	resp, err := httpClient.Post(url, "application/json", bytes.NewReader(requestJSON))
	testutil.Ok(t, err)
	defer resp.Body.Close()

	// Read response body
	bodyBytes, err := io.ReadAll(resp.Body)
	testutil.Ok(t, err)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("SQL query API returned status %d: %s\nQuery: %s", resp.StatusCode, string(bodyBytes), query)
	}

	var result SQLQueryResponse
	testutil.Ok(t, json.Unmarshal(bodyBytes, &result))

	// Debug output
	if !result.Success && result.Error != "" {
		t.Logf("SQL query failed: %s\nQuery: %s", result.Error, query)
	}

	return &result
}

// Helper functions

func sendOTLPTrace(endpoint string, trace *collectortracev1.ExportTraceServiceRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to OTLP endpoint: %w", err)
	}
	defer conn.Close()

	client := collectortracev1.NewTraceServiceClient(conn)
	_, err = client.Export(ctx, trace)
	return err
}

func queryJaegerTrace(t *testing.T, artemis e2e.Runnable, traceID []byte) *JaegerTrace {
	t.Helper()

	traceIDHex := hex.EncodeToString(traceID)
	url := fmt.Sprintf("http://%s/api/traces/%s", artemis.Endpoint("http"), traceIDHex)

	httpClient := &http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Get(url)
	testutil.Ok(t, err)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Jaeger API returned status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Data []JaegerTrace `json:"data"`
	}
	testutil.Ok(t, json.NewDecoder(resp.Body).Decode(&result))

	if len(result.Data) == 0 {
		return nil
	}
	return &result.Data[0]
}

func queryTempoTrace(t *testing.T, artemis e2e.Runnable, traceID []byte) []byte {
	t.Helper()

	traceIDHex := hex.EncodeToString(traceID)
	url := fmt.Sprintf("http://%s/api/traces/%s", artemis.Endpoint("tempo"), traceIDHex)

	httpClient := &http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Get(url)
	testutil.Ok(t, err)
	defer resp.Body.Close()

	testutil.Equals(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	testutil.Ok(t, err)
	return body
}

func queryServices(t *testing.T, artemis e2e.Runnable) []string {
	t.Helper()

	url := fmt.Sprintf("http://%s/api/services", artemis.Endpoint("http"))
	httpClient := &http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Get(url)
	testutil.Ok(t, err)
	defer resp.Body.Close()

	var result struct {
		Data []string `json:"data"`
	}
	testutil.Ok(t, json.NewDecoder(resp.Body).Decode(&result))
	return result.Data
}

func validateJaegerSpan(t *testing.T, span JaegerSpan, expectedTraceID, expectedName string) {
	t.Helper()
	testutil.Equals(t, expectedTraceID, span.TraceID)
	testutil.Equals(t, expectedName, span.OperationName)
	testutil.Assert(t, span.StartTime > 0, "start time should be set")
	testutil.Assert(t, span.Duration > 0, "duration should be set")
}

var idCounter uint64

func generateTraceID() []byte {
	traceID := make([]byte, 16)
	now := time.Now().UnixNano()
	idCounter++
	for i := range 8 {
		traceID[i] = byte(now >> (i * 8))
	}
	for i := 8; i < 16; i++ {
		traceID[i] = byte((now + int64(idCounter)) >> ((i - 8) * 8))
	}
	return traceID
}

func generateSpanID() []byte {
	spanID := make([]byte, 8)
	now := time.Now().UnixNano()
	idCounter++
	for i := range 8 {
		spanID[i] = byte((now + int64(idCounter)) >> (i * 8))
	}
	return spanID
}

func stringValue(s string) *commonv1.AnyValue {
	return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: s}}
}

func intValue(i int64) *commonv1.AnyValue {
	return &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: i}}
}

func boolValue(b bool) *commonv1.AnyValue {
	return &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: b}}
}

func contains(slice []string, item string) bool {
	return slices.Contains(slice, item)
}

// JaegerTrace represents the Jaeger API trace response structure
type JaegerTrace struct {
	TraceID   string             `json:"traceID"`
	Spans     []JaegerSpan       `json:"spans"`
	Processes map[string]Process `json:"processes"`
}

// JaegerSpan represents a span in Jaeger format
type JaegerSpan struct {
	TraceID       string      `json:"traceID"`
	SpanID        string      `json:"spanID"`
	OperationName string      `json:"operationName"`
	References    []Reference `json:"references"`
	StartTime     int64       `json:"startTime"`
	Duration      int64       `json:"duration"`
	Tags          []Tag       `json:"tags"`
	ProcessID     string      `json:"processID"`
}

// Reference represents a span reference
type Reference struct {
	RefType string `json:"refType"`
	TraceID string `json:"traceID"`
	SpanID  string `json:"spanID"`
}

// Tag represents a span tag
type Tag struct {
	Key   string `json:"key"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}

// Process represents service information
type Process struct {
	ServiceName string `json:"serviceName"`
	Tags        []Tag  `json:"tags"`
}

// SQLQueryResponse represents the response from the SQL query API
type SQLQueryResponse struct {
	Success  bool                     `json:"success"`
	Columns  []string                 `json:"columns"`
	RowCount int                      `json:"row_count"`
	Traces   []SQLTrace               `json:"traces,omitempty"`
	Rows     []map[string]interface{} `json:"rows,omitempty"`
	Error    string                   `json:"error,omitempty"`
}

// SQLTrace represents a trace/span returned from SQL queries
type SQLTrace struct {
	TraceID      string            `json:"trace_id"`
	SpanID       string            `json:"span_id"`
	ParentSpanID string            `json:"parent_span_id,omitempty"`
	Name         string            `json:"name"`
	StartTime    int64             `json:"start_time"`
	EndTime      int64             `json:"end_time"`
	Duration     int64             `json:"duration"`
	ServiceName  string            `json:"service_name"`
	Tags         map[string]string `json:"tags,omitempty"`
}
