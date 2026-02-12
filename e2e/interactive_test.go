package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/efficientgo/e2e"
	e2einteractive "github.com/efficientgo/e2e/interactive"
)

// TestArtemisTracingStack is an interactive e2e test that demonstrates the complete tracing pipeline:
// Telemetrygen + Prometheus (with tracing) -> OTEL Collector -> Artemis -> Grafana
//
// Run with: go test -v ./e2e -run TestArtemisTracingStack -timeout 99m
//
// The test will:
// 1. Build and start all services (Artemis, OTEL Collector, Prometheus, Telemetrygen, Grafana)
// 2. Generate traces continuously from both telemetrygen and Prometheus queries
// 3. Validate the pipeline is working
// 4. Open Grafana in your browser
// 5. Keep running until you hit the endpoint or press Ctrl+C
func TestArtemisTracingStack(t *testing.T) {
	// t.Skip("This is an interactive test, comment this line before running")

	fmt.Println("=== Building Artemis Docker image...")
	testutil.Ok(t, buildArtemisImage())

	fmt.Println("=== Creating e2e environment...")
	env, err := e2e.New()
	testutil.Ok(t, err)
	t.Cleanup(env.Close)

	fmt.Println("=== Starting OTEL Collector...")
	otelCollector := createOtelCollector(env)
	testutil.Ok(t, e2e.StartAndWaitReady(otelCollector))
	fmt.Printf("✓ OTEL Collector ready at %s\n", otelCollector.Endpoint("otlp-grpc"))

	fmt.Println("=== Starting Artemis...")
	artemis := createArtemis(env)
	testutil.Ok(t, e2e.StartAndWaitReady(artemis))
	fmt.Printf("✓ Artemis ready - OTLP: %s, Jaeger API: %s, Tempo API: %s\n",
		artemis.Endpoint("otlp"), artemis.Endpoint("http"), artemis.Endpoint("tempo"))

	fmt.Println("=== Starting Prometheus with tracing...")
	prometheus := createPrometheus(env)
	testutil.Ok(t, e2e.StartAndWaitReady(prometheus))
	fmt.Printf("✓ Prometheus ready at %s\n", prometheus.Endpoint("http"))

	fmt.Println("=== Starting telemetrygen for continuous trace generation...")
	telemetrygen := createTelemetryGen(env)
	testutil.Ok(t, telemetrygen.Start())
	fmt.Println("✓ Telemetrygen running - generating traces continuously (5 traces/sec)")

	fmt.Println("=== Starting Grafana with Artemis datasource...")
	grafana := createGrafana(env)
	testutil.Ok(t, e2e.StartAndWaitReady(grafana))
	fmt.Printf("✓ Grafana ready at %s\n", grafana.Endpoint("http"))

	fmt.Println("\n=== Starting query load generator...")
	// Start generating Prometheus queries to create traces
	stopQueryGen := make(chan struct{})
	t.Cleanup(func() { close(stopQueryGen) })
	go generatePrometheusQueries(prometheus, stopQueryGen)

	fmt.Println("=== Waiting for traces to be generated and ingested...")
	time.Sleep(15 * time.Second) // Give more time for queries to execute and traces to flow

	fmt.Println("=== Validating tracing pipeline...")

	// Check Artemis health
	artemisHealth := fmt.Sprintf("http://%s/health", artemis.Endpoint("http"))
	resp, err := http.Get(artemisHealth)
	testutil.Ok(t, err)
	defer resp.Body.Close()
	testutil.Equals(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("✓ Artemis health: %s\n", string(body))

	// Query for services
	servicesURL := fmt.Sprintf("http://%s/api/services", artemis.Endpoint("http"))
	resp, err = http.Get(servicesURL)
	testutil.Ok(t, err)
	defer resp.Body.Close()
	testutil.Equals(t, 200, resp.StatusCode)

	var servicesResp struct {
		Data []string `json:"data"`
	}
	body, _ = io.ReadAll(resp.Body)
	testutil.Ok(t, json.Unmarshal(body, &servicesResp))
	fmt.Printf("✓ Discovered %d service(s): %v\n", len(servicesResp.Data), servicesResp.Data)

	// Query for traces if we have services
	if len(servicesResp.Data) > 0 {
		tracesURL := fmt.Sprintf("http://%s/api/traces?service=%s&limit=5", artemis.Endpoint("http"), servicesResp.Data[0])
		resp, err = http.Get(tracesURL)
		testutil.Ok(t, err)
		defer resp.Body.Close()

		var tracesResp struct {
			Data  []any `json:"data"`
			Total int   `json:"total"`
		}
		body, _ = io.ReadAll(resp.Body)
		testutil.Ok(t, json.Unmarshal(body, &tracesResp))
		fmt.Printf("✓ Found %d trace(s) for service '%s'\n", tracesResp.Total, servicesResp.Data[0])
	}

	// Verify Grafana is accessible
	grafanaHealth := fmt.Sprintf("http://%s/api/health", grafana.Endpoint("http"))
	resp, err = http.Get(grafanaHealth)
	testutil.Ok(t, err)
	defer resp.Body.Close()
	testutil.Equals(t, 200, resp.StatusCode)
	fmt.Println("✓ Grafana health check passed")

	// Test Tempo API endpoints
	tempoEcho := fmt.Sprintf("http://%s/api/echo", artemis.Endpoint("tempo"))
	resp, err = http.Get(tempoEcho)
	testutil.Ok(t, err)
	defer resp.Body.Close()
	testutil.Equals(t, 200, resp.StatusCode)
	echoBody, _ := io.ReadAll(resp.Body)
	testutil.Equals(t, "echo", string(echoBody))
	fmt.Println("✓ Tempo API health check passed")

	// Test Tempo search (v1)
	tempoSearch := fmt.Sprintf("http://%s/api/search", artemis.Endpoint("tempo"))
	resp, err = http.Get(tempoSearch)
	testutil.Ok(t, err)
	defer resp.Body.Close()
	testutil.Equals(t, 200, resp.StatusCode)
	fmt.Println("✓ Tempo v1 search endpoint working")

	// Test Tempo v2 search
	tempoSearchV2 := fmt.Sprintf("http://%s/api/v2/search", artemis.Endpoint("tempo"))
	resp, err = http.Get(tempoSearchV2)
	testutil.Ok(t, err)
	defer resp.Body.Close()
	testutil.Equals(t, 200, resp.StatusCode)
	fmt.Println("✓ Tempo v2 search endpoint working")

	// Test Tempo v2 tag values (the endpoint Grafana uses)
	tempoTagValuesV2 := fmt.Sprintf("http://%s/api/v2/search/tag/name/values", artemis.Endpoint("tempo"))
	resp, err = http.Get(tempoTagValuesV2)
	testutil.Ok(t, err)
	defer resp.Body.Close()
	testutil.Equals(t, 200, resp.StatusCode)
	fmt.Println("✓ Tempo v2 tag values endpoint working")

	fmt.Println("\n=== 🎉 Tracing stack is ready! ===")
	fmt.Println("\nServices:")
	fmt.Printf("  • Grafana:       http://%s (login: admin/admin)\n", grafana.Endpoint("http"))
	fmt.Printf("  • Prometheus:    http://%s\n", prometheus.Endpoint("http"))
	fmt.Printf("  • Artemis (Jaeger): http://%s\n", artemis.Endpoint("http"))
	fmt.Printf("  • Artemis (Tempo):  http://%s\n", artemis.Endpoint("tempo"))

	// Get the data directory path
	cwd, _ := os.Getwd()
	dataDir := filepath.Join(cwd, "e2e-data")

	fmt.Println("\n📁 Database Files (inspect in real-time):")
	fmt.Printf("  • Data directory: %s\n", dataDir)
	fmt.Printf("  • View WAL segments:     ls -lh %s/wal/\n", dataDir)
	fmt.Printf("  • View persisted blocks: ls -lh %s/blocks/\n", dataDir)
	fmt.Printf("  • Watch logs:            docker logs -f artemis\n")
	fmt.Println("\nTrace Generators:")
	fmt.Println("  • Telemetrygen: 5 traces/sec with 5 child spans each")
	fmt.Println("  • Prometheus Query Generator: Complex PromQL range queries every 2s")
	fmt.Println("    - Queries include: rate(), histogram_quantile(), topk(), subqueries")
	fmt.Println("    - Generating traces from Prometheus's query engine")
	fmt.Println("\nIn Grafana:")
	fmt.Println("  1. Navigate to Explore")
	fmt.Println("  2. Select 'Artemis (Jaeger)' or 'Artemis (Tempo)' datasource")
	fmt.Println("  3. Search for traces from 'prometheus' or 'telemetrygen-test-service' services")
	fmt.Println("  4. Observe query execution traces (Prometheus) vs synthetic traces (telemetrygen)")
	fmt.Println("  5. Compare Jaeger vs Tempo UI/UX")
	fmt.Println()

	// Open Grafana and Prometheus in browser
	grafanaURL := fmt.Sprintf("http://%s", grafana.Endpoint("http"))
	prometheusURL := fmt.Sprintf("http://%s", prometheus.Endpoint("http"))

	fmt.Println("=== Opening Grafana in your browser...")
	testutil.Ok(t, e2einteractive.OpenInBrowser(grafanaURL))

	fmt.Println("=== Opening Prometheus in your browser...")
	testutil.Ok(t, e2einteractive.OpenInBrowser(prometheusURL))

	fmt.Println("\n=== Stack is running! ===")
	fmt.Println("Visit the endpoint displayed below or press Ctrl+C to stop the test.")
	testutil.Ok(t, e2einteractive.RunUntilEndpointHit())

	fmt.Println("\n=== Shutting down services... ===")
}

// createOtelCollector creates the OTEL Collector runnable
func createOtelCollector(env e2e.Environment) e2e.Runnable {
	ports := map[string]int{
		"otlp-grpc": 4317,
		"otlp-http": 4318,
		"metrics":   8888,
	}

	f := env.Runnable("otel-collector").WithPorts(ports).Future()

	// Copy OTEL Collector config to shared directory
	configPath := filepath.Join(f.Dir(), "otel-collector.yml")
	configContent, err := os.ReadFile("configs/otel-collector.yml")
	if err != nil {
		return e2e.NewFailedRunnable("otel-collector", fmt.Errorf("failed to read otel config: %w", err))
	}
	if err := os.WriteFile(configPath, configContent, 0644); err != nil {
		return e2e.NewFailedRunnable("otel-collector", fmt.Errorf("failed to write otel config: %w", err))
	}

	return f.Init(e2e.StartOptions{
		Image: "otel/opentelemetry-collector:0.95.0",
		Command: e2e.NewCommand(
			"--config=/etc/otel-collector.yml",
		),
		Volumes: []string{
			configPath + ":/etc/otel-collector.yml:ro",
		},
		Readiness: e2e.NewTCPReadinessProbe("otlp-grpc"),
	})
}

// createPrometheus creates the Prometheus runnable with tracing enabled
func createPrometheus(env e2e.Environment) e2e.Runnable {
	ports := map[string]int{
		"http": 9090,
	}

	f := env.Runnable("prometheus").WithPorts(ports).Future()

	// Copy Prometheus config to shared directory
	configContent, err := os.ReadFile("configs/prometheus.yml")
	if err != nil {
		return e2e.NewFailedRunnable("prometheus", fmt.Errorf("failed to read prometheus config: %w", err))
	}

	configPath := filepath.Join(f.Dir(), "prometheus.yml")
	if err := os.WriteFile(configPath, configContent, 0644); err != nil {
		return e2e.NewFailedRunnable("prometheus", fmt.Errorf("failed to write prometheus config: %w", err))
	}

	return f.Init(e2e.StartOptions{
		Image: "prom/prometheus:v2.48.1",
		Command: e2e.NewCommand(
			"--config.file=/etc/prometheus/prometheus.yml",
			"--storage.tsdb.path=/prometheus",
			"--web.listen-address=:9090",
			"--log.level=info",
		),
		Volumes: []string{
			configPath + ":/etc/prometheus/prometheus.yml:ro",
		},
		Readiness: e2e.NewHTTPReadinessProbe(
			"http",
			"/-/ready",
			200,
			200,
		),
	})
}

// generatePrometheusQueries executes complex range queries against Prometheus periodically
// This generates traces from Prometheus's internal metrics processing
func generatePrometheusQueries(prometheus e2e.Runnable, stopCh <-chan struct{}) {
	promEndpoint := prometheus.Endpoint("http")
	client := &http.Client{Timeout: 30 * time.Second}

	// Complex PromQL queries that will generate interesting traces
	queries := []string{
		// Rate calculations over different time windows
		`rate(prometheus_http_requests_total[5m])`,
		`rate(prometheus_tsdb_head_samples_appended_total[1m])`,
		`rate(prometheus_rule_evaluations_total[2m])`,

		// Aggregations with grouping
		`sum by (job, handler) (rate(prometheus_http_requests_total[5m]))`,
		`avg by (instance) (rate(prometheus_tsdb_head_samples_appended_total[1m]))`,
		`max by (rule_group) (prometheus_rule_group_last_duration_seconds)`,

		// Histogram quantiles (complex calculations)
		`histogram_quantile(0.95, rate(prometheus_http_request_duration_seconds_bucket[5m]))`,
		`histogram_quantile(0.99, sum by (le, handler) (rate(prometheus_http_request_duration_seconds_bucket[5m])))`,

		// Multiple aggregations
		`sum(rate(prometheus_tsdb_head_samples_appended_total[5m])) / sum(rate(prometheus_tsdb_head_chunks_created_total[5m]))`,

		// Topk queries
		`topk(5, rate(prometheus_http_requests_total[5m]))`,
		`bottomk(3, prometheus_tsdb_storage_blocks_bytes)`,

		// Range vector operations
		`increase(prometheus_http_requests_total[10m])`,
		`delta(prometheus_tsdb_head_series[5m])`,
		`deriv(prometheus_tsdb_head_samples_appended_total[5m])`,

		// Binary operations across metrics
		`rate(prometheus_http_requests_total[5m]) > 0.1`,
		`(prometheus_tsdb_head_series / prometheus_tsdb_symbol_table_size_bytes) * 1024`,

		// Subqueries (very complex, generates deep traces)
		`max_over_time(rate(prometheus_http_requests_total[5m])[10m:1m])`,
		`avg_over_time(prometheus_tsdb_head_series[15m:30s])`,

		// Multiple time series operations
		`absent(up{job="nonexistent"})`,
		`count(prometheus_http_requests_total) by (job)`,
		`count_values("status", prometheus_http_requests_total)`,
	}

	ticker := time.NewTicker(2 * time.Second) // Query every 2 seconds
	defer ticker.Stop()

	queryCount := 0
	fmt.Println("Query generator started - executing complex range queries...")

	for {
		select {
		case <-stopCh:
			fmt.Printf("Query generator stopped after %d queries\n", queryCount)
			return
		case <-ticker.C:
			// Pick a query (rotate through them)
			query := queries[queryCount%len(queries)]
			queryCount++

			// Execute range query with proper URL encoding
			queryURL := fmt.Sprintf("http://%s/api/v1/query_range?query=%s&start=%d&end=%d&step=15s",
				promEndpoint,
				url.QueryEscape(query),
				time.Now().Add(-5*time.Minute).Unix(),
				time.Now().Unix(),
			)

			resp, err := client.Get(queryURL)
			if err != nil {
				fmt.Printf("⚠ Query %d failed: %v\n", queryCount, err)
				continue
			}
			resp.Body.Close()

			if queryCount%10 == 0 {
				queryPreview := query
				if len(query) > 60 {
					queryPreview = query[:60] + "..."
				}
				fmt.Printf("✓ Executed %d queries (latest: %s)\n", queryCount, queryPreview)
			}
		}
	}
}

// createGrafana creates the Grafana runnable with Artemis datasource provisioned
func createGrafana(env e2e.Environment) e2e.Runnable {
	ports := map[string]int{
		"http": 3000,
	}

	f := env.Runnable("grafana").WithPorts(ports).Future()

	// Create provisioning directory structure
	provisioningDir := filepath.Join(f.Dir(), "provisioning", "datasources")
	if err := os.MkdirAll(provisioningDir, 0755); err != nil {
		return e2e.NewFailedRunnable("grafana", fmt.Errorf("failed to create provisioning dir: %w", err))
	}

	// Copy datasource config
	configContent, err := os.ReadFile("configs/grafana-datasource.yml")
	if err != nil {
		return e2e.NewFailedRunnable("grafana", fmt.Errorf("failed to read grafana datasource config: %w", err))
	}

	datasourcePath := filepath.Join(provisioningDir, "datasource.yml")
	if err := os.WriteFile(datasourcePath, configContent, 0644); err != nil {
		return e2e.NewFailedRunnable("grafana", fmt.Errorf("failed to write grafana datasource config: %w", err))
	}

	return f.Init(e2e.StartOptions{
		Image: "grafana/grafana:10.2.3",
		EnvVars: map[string]string{
			"GF_AUTH_ANONYMOUS_ENABLED":  "true",
			"GF_AUTH_ANONYMOUS_ORG_ROLE": "Admin",
			"GF_AUTH_DISABLE_LOGIN_FORM": "false",
			"GF_SECURITY_ADMIN_PASSWORD": "admin",
			// Install Infinity plugin for SQL queries
			"GF_INSTALL_PLUGINS": "yesoreyeram-infinity-datasource",
		},
		Volumes: []string{
			filepath.Join(f.Dir(), "provisioning") + ":/etc/grafana/provisioning:ro",
		},
		Readiness: e2e.NewHTTPReadinessProbe(
			"http",
			"/api/health",
			200,
			200,
		),
	})
}

// createTelemetryGen creates a telemetrygen runnable for continuous trace generation
func createTelemetryGen(env e2e.Environment) e2e.Runnable {
	f := env.Runnable("telemetrygen").Future()

	// telemetrygen will generate traces continuously with varying patterns
	// Using --rate to control traces per second and --duration for how long to run
	return f.Init(e2e.StartOptions{
		Image: "telemetrygen:latest",
		Command: e2e.NewCommand(
			"traces",
			"--otlp-endpoint=otel-collector:4317",
			"--otlp-insecure",
			"--rate=5",       // Generate 5 traces per second
			"--duration=inf", // Run indefinitely (0 means forever)
			"--service=telemetrygen-test-service",
			"--child-spans=5",
			"--status-code=2",
			"--span-duration=123ms",
			"--span-events=3",
			"--span-links=3",
		),
		// No readiness probe needed since this is a trace generator
		// It doesn't expose any ports, it just sends traces
	})
}
