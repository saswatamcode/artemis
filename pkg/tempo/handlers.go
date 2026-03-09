package tempo

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/saswatamcode/artemis/pkg/query"
	"github.com/saswatamcode/artemis/pkg/reduced_promql/engine"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/tracedb"

	"github.com/prometheus/common/version"
	coltracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// Server provides Tempo-compatible HTTP API for trace queries
type Server struct {
	db          *tracedb.DB
	queryEngine *engine.Engine
	mux         *http.ServeMux
	logger      *slog.Logger
	srv         *http.Server
}

// NewServer creates a new Tempo API server
func NewServer(db *tracedb.DB, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	// Create query engine once for reuse across all queries
	// Pass a function that gets blocks dynamically (always queries latest blocks)
	isolation := db.GetIsolation()
	queryEngine := engine.NewEngine(db.GetBlocks, isolation)

	s := &Server{
		db:          db,
		queryEngine: queryEngine,
		mux:         http.NewServeMux(),
		logger:      logger,
	}

	s.registerRoutes()
	return s
}

// registerRoutes sets up HTTP routes for Tempo API
func (s *Server) registerRoutes() {
	// Tempo API v1 endpoints
	s.mux.HandleFunc("/api/search", s.handleSearch)
	s.mux.HandleFunc("/api/traces/", s.handleGetTrace)
	s.mux.HandleFunc("/api/search/tags", s.handleSearchTags)
	s.mux.HandleFunc("/api/search/tag/", s.handleSearchTagValues)
	s.mux.HandleFunc("/api/echo", s.handleEcho)

	s.mux.HandleFunc("/api/status/buildinfo", s.handleBuildInfo)

	// Metrics endpoints (Prometheus-compatible)
	s.mux.HandleFunc("/api/metrics/query_range", s.handleMetricsQueryRange)

	// Prometheus-compatible metrics endpoint (for Grafana)
	s.mux.HandleFunc("/api/v1/query_range", s.handleMetricsQueryRange)
	s.mux.HandleFunc("/api/v1/query", s.handleMetricsQuery)

	// Tempo API v2 endpoints (used by Grafana)
	s.mux.HandleFunc("/api/v2/search/tags", s.handleSearchTags)
	s.mux.HandleFunc("/api/v2/search/tag/", s.handleSearchTagValuesV2)
	s.mux.HandleFunc("/api/v2/search", s.handleSearchV2)
}

// ServeHTTP implements http.Handler
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	s.mux.ServeHTTP(w, r)
}

// handleSearch searches for traces using reduced PromQL selectors
// GET /api/search?q={query}&start={start}&end={end}&limit={limit}
//
// Query format: Reduced PromQL SELECTORS ONLY (not functions/aggregations)
// Examples:
//   - {service_name="api"}
//   - {service_name="api", status_code="200"}
//   - {trace_id="abc123"}
//
// For metric queries (rate, histogram_quantile, etc), use /api/metrics/query_range
func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	queryStr := q.Get("q")
	limit := 20
	if limitStr := q.Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}

	start := time.Now().Add(-1 * time.Hour)
	end := time.Now()

	if startStr := q.Get("start"); startStr != "" {
		if ts, err := strconv.ParseInt(startStr, 10, 64); err == nil {
			start = time.Unix(ts, 0)
		}
	}

	if endStr := q.Get("end"); endStr != "" {
		if ts, err := strconv.ParseInt(endStr, 10, 64); err == nil {
			end = time.Unix(ts, 0)
		}
	}

	s.logger.Info("Processing trace search query",
		slog.String("query", queryStr),
		slog.Time("start", start),
		slog.Time("end", end),
		slog.Int("limit", limit))

	// If no query provided, default to selecting all spans
	if queryStr == "" {
		queryStr = "{}"
	}

	// Execute query using reduced PromQL engine
	// (engine automatically queries latest blocks on each execution)
	opts := &engine.QueryOptions{
		StartTime:   start,
		EndTime:     end,
		Context:     r.Context(),
		UseSnapshot: true,        // Enable MVCC snapshot isolation
		Limit:       limit * 100, // Request more spans to ensure we get enough traces
	}

	result, err := s.queryEngine.Execute(queryStr, opts)
	if err != nil {
		s.logger.Error("Query execution failed",
			slog.String("query", queryStr),
			slog.String("error", err.Error()))
		http.Error(w, "query execution failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.logger.Debug("Query executed successfully",
		slog.String("query", queryStr),
		slog.String("result_type", string(result.Type)),
		slog.Int("result_count", getResultCount(result)),
		slog.String("stats", result.Stats.String()))

	// Auto-route based on result type
	// Grafana's Tempo datasource sends all queries to /api/search, so we need to handle both
	switch result.Type {
	case engine.ResultTypeSpans:
		// Traditional trace search
		s.handleSpansResult(w, result, limit)

	case engine.ResultTypeVector, engine.ResultTypeMatrix, engine.ResultTypeScalar:
		// Metric query - return Prometheus format
		// This happens when Grafana sends rate(), histogram_quantile(), etc. to /api/search
		s.handleMetricResult(w, result)

	default:
		http.Error(w, "unsupported result type", http.StatusInternalServerError)
	}
}

// handleGetTrace retrieves a trace by ID in OTLP format
// GET /api/traces/{traceID}
func (s *Server) handleGetTrace(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/traces/")
	traceID := strings.TrimSuffix(path, "/")

	if traceID == "" {
		http.Error(w, "trace ID required", http.StatusBadRequest)
		return
	}

	s.logger.Debug("Retrieving trace by ID",
		slog.String("trace_id", traceID))

	matcher, err := query.NewMatcher(query.MatchEqual, "trace_id", traceID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	querier := s.db.GetQuerier()
	result, err := querier.Select(matcher)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(result.Spans) == 0 {
		http.Error(w, "trace not found", http.StatusNotFound)
		return
	}

	// Links are already populated on spans by GetSpansBatch() during query
	// No need to load them separately - they were fetched in parallel with spans
	// This optimization saves 100-150ms per trace query by avoiding N+1 queries

	resourceSpansList := ConvertSpansToOTLP(result.Spans)
	if len(resourceSpansList) == 0 {
		http.Error(w, "failed to convert trace", http.StatusInternalServerError)
		return
	}

	traceRequest := &coltracev1.ExportTraceServiceRequest{
		ResourceSpans: resourceSpansList,
	}
	data, err := proto.Marshal(traceRequest)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/protobuf")
	w.Write(data)
}

// handleSearchTags returns all available tag names
// GET /api/search/tags
func (s *Server) handleSearchTags(w http.ResponseWriter, r *http.Request) {
	// Query recent spans to discover tags
	timeRange := query.NewTimeRange(time.Now().Add(-24*time.Hour), time.Now())
	querier := s.db.GetQuerier()
	result, err := querier.SelectWithTimeRange(timeRange)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tagNames := make(map[string]bool)
	for _, sp := range result.Spans {
		for k := range sp.Tags {
			tagNames[k] = true
		}
	}

	tags := make([]string, 0, len(tagNames))
	for tag := range tagNames {
		tags = append(tags, tag)
	}

	response := TagsResponse{
		TagNames: tags,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleSearchTagValues returns all values for a specific tag
// GET /api/search/tag/{tagName}/values
func (s *Server) handleSearchTagValues(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/search/tag/")
	tagName := strings.TrimSuffix(path, "/values")

	if tagName == "" {
		http.Error(w, "tag name required", http.StatusBadRequest)
		return
	}

	s.logger.Debug("Retrieving tag values",
		slog.String("tag_name", tagName))

	// Query recent spans
	timeRange := query.NewTimeRange(time.Now().Add(-24*time.Hour), time.Now())
	querier := s.db.GetQuerier()
	result, err := querier.SelectWithTimeRange(timeRange)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	valueSet := make(map[string]bool)
	for _, sp := range result.Spans {
		if val, ok := sp.Tags[tagName]; ok {
			valueSet[val] = true
		}
	}

	values := make([]TagValue, 0, len(valueSet))
	for val := range valueSet {
		values = append(values, TagValue{
			Type:  "string",
			Value: val,
		})
	}

	response := TagValuesResponse{
		TagValues: values,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleEcho is a health check endpoint
// GET /api/echo
func (s *Server) handleEcho(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("echo"))
}

// handleBuildInfo returns build information about the Artemis/Tempo server
// GET /api/status/buildinfo
// This is used by Grafana to discover server capabilities
func (s *Server) handleBuildInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"version":   version.Version,
		"revision":  version.Revision,
		"branch":    version.Branch,
		"buildDate": version.BuildDate,
		"buildUser": version.BuildUser,
		"goVersion": version.GoVersion,
		"platform":  runtime.GOOS + "/" + runtime.GOARCH,
	})
}

// handleSearchV2 is the v2 version of search (same as v1)
// GET /api/v2/search
func (s *Server) handleSearchV2(w http.ResponseWriter, r *http.Request) {
	s.handleSearch(w, r)
}

// handleSearchTagValuesV2 returns all values for a specific tag (v2 API)
// GET /api/v2/search/tag/{tagName}/values
func (s *Server) handleSearchTagValuesV2(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v2/search/tag/")
	tagName := strings.TrimSuffix(path, "/values")

	if tagName == "" {
		http.Error(w, "tag name required", http.StatusBadRequest)
		return
	}

	// Normalize tag name - Tempo uses .service.name, we store as service.name
	// Also handle resource. and span. prefixes
	normalizedTag := normalizeTagName(tagName)

	s.logger.Debug("Retrieving tag values (v2)",
		slog.String("tag_name", tagName),
		slog.String("normalized_tag", normalizedTag))

	timeRange := query.NewTimeRange(time.Now().Add(-24*time.Hour), time.Now())
	querier := s.db.GetQuerier()
	result, err := querier.SelectWithTimeRange(timeRange)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	valueSet := make(map[string]bool)
	for _, sp := range result.Spans {
		if val, ok := sp.Tags[normalizedTag]; ok {
			valueSet[val] = true
		}
	}

	values := make([]TagValue, 0, len(valueSet))
	for val := range valueSet {
		values = append(values, TagValue{
			Type:  "string",
			Value: val,
		})
	}

	response := TagValuesResponse{
		TagValues: values,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// normalizeTagName normalizes Tempo/TraceQL tag names to internal format
// .service.name -> service.name
// resource.service.name -> service.name
// span.http.method -> http.method
func normalizeTagName(tagName string) string {
	// Remove leading dot (Tempo intrinsic fields)
	tagName = strings.TrimPrefix(tagName, ".")

	// Remove resource. prefix
	tagName = strings.TrimPrefix(tagName, "resource.")

	// Remove span. prefix
	tagName = strings.TrimPrefix(tagName, "span.")

	return tagName
}

// handleSpansResult handles span-type query results (trace search)
func (s *Server) handleSpansResult(w http.ResponseWriter, result *engine.QueryResult, limit int) {
	// Group spans by trace ID
	traceSpans := make(map[string][]*span.Span)
	for _, sp := range result.Spans {
		traceSpans[sp.TraceID] = append(traceSpans[sp.TraceID], sp)
	}

	// Convert to search metadata format
	traces := ConvertSpansToSearchMetadata(traceSpans)

	// Apply limit to traces (not spans)
	if len(traces) > limit {
		traces = traces[:limit]
	}

	response := SearchResponse{
		Traces: traces,
		Metrics: SearchMetrics{
			InspectedTraces: len(traceSpans),
			InspectedBlocks: int(result.Stats.BlocksScanned),
			TotalBlocks:     int(result.Stats.BlocksScanned),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleMetricResult handles metric-type query results (Prometheus-compatible)
func (s *Server) handleMetricResult(w http.ResponseWriter, result *engine.QueryResult) {
	// Convert to Prometheus-compatible JSON format
	response := result.ToPrometheusJSON()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleMetricsQueryRange executes metric queries and returns Prometheus-compatible time-series
// GET /api/metrics/query_range?q={query}&start={start}&end={end}&step={step}
//
// Query format: Reduced PromQL (metric functions and aggregations)
// Examples:
//   - rate({service_name="api"}[5m])
//   - histogram_quantile(0.95, {service_name="api"})
//   - heatmap({service_name="api"})
//   - sum by (service_name) ({job="app"})
//
// Parameters:
//   - q: The reduced PromQL query (required)
//   - start: Start time (unix epoch seconds/nanos or RFC3339)
//   - end: End time (unix epoch seconds/nanos or RFC3339)
//   - since: Relative time range (e.g., "15m", "1h") - alternative to start/end
//   - step: Time series granularity (e.g., "15s", "1m") - optional
//   - exemplars: Max number of exemplars (optional)
func (s *Server) handleMetricsQueryRange(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	queryStr := q.Get("q")
	if queryStr == "" {
		http.Error(w, "query parameter 'q' is required", http.StatusBadRequest)
		return
	}

	// Parse time range
	var start, end time.Time
	var err error

	// Handle 'since' parameter (relative time)
	if sinceStr := q.Get("since"); sinceStr != "" {
		duration, err := time.ParseDuration(sinceStr)
		if err != nil {
			http.Error(w, "invalid 'since' duration: "+err.Error(), http.StatusBadRequest)
			return
		}
		end = time.Now()
		start = end.Add(-duration)
	} else {
		// Handle absolute start/end times
		start, end, err = parseTimeRange(q.Get("start"), q.Get("end"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Parse optional step parameter
	// TODO: Use step for downsampling/aggregation
	step := q.Get("step")
	if step == "" {
		// Default step based on time range
		step = calculateDefaultStep(start, end)
	}

	// Parse optional exemplars parameter
	exemplars := 0
	if exemplarsStr := q.Get("exemplars"); exemplarsStr != "" {
		if e, err := strconv.Atoi(exemplarsStr); err == nil {
			exemplars = e
		}
	}

	s.logger.Info("Processing metrics query",
		slog.String("query", queryStr),
		slog.Time("start", start),
		slog.Time("end", end),
		slog.String("step", step),
		slog.Int("exemplars", exemplars))

	// Execute query using reduced PromQL engine
	// (engine automatically queries latest blocks on each execution)
	opts := &engine.QueryOptions{
		StartTime:   start,
		EndTime:     end,
		Context:     r.Context(),
		UseSnapshot: true, // Enable MVCC snapshot isolation
		Limit:       0,    // No limit for metrics queries
	}

	result, err := s.queryEngine.Execute(queryStr, opts)
	if err != nil {
		s.logger.Error("Metrics query execution failed",
			slog.String("query", queryStr),
			slog.String("error", err.Error()))
		http.Error(w, "query execution failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.logger.Debug("Metrics query executed successfully",
		slog.String("query", queryStr),
		slog.String("result_type", string(result.Type)),
		slog.String("stats", result.Stats.String()))

	// Metrics endpoint should only return metrics (not spans)
	if result.Type == engine.ResultTypeSpans {
		http.Error(w, "span queries should use /api/search endpoint", http.StatusBadRequest)
		return
	}

	s.handleMetricResult(w, result)
}

// parseTimeRange parses start and end time parameters
// Supports:
//   - Unix epoch seconds (10 digits)
//   - Unix epoch nanoseconds (19 digits)
//   - RFC3339 strings
func parseTimeRange(startStr, endStr string) (time.Time, time.Time, error) {
	var start, end time.Time

	// Default to last hour if not specified
	if startStr == "" && endStr == "" {
		end = time.Now()
		start = end.Add(-1 * time.Hour)
		return start, end, nil
	}

	// Parse start time
	if startStr != "" {
		var err error
		start, err = parseTimestamp(startStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid start time: %w", err)
		}
	} else {
		start = time.Now().Add(-1 * time.Hour)
	}

	// Parse end time
	if endStr != "" {
		var err error
		end, err = parseTimestamp(endStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid end time: %w", err)
		}
	} else {
		end = time.Now()
	}

	return start, end, nil
}

// parseTimestamp parses a timestamp in various formats
func parseTimestamp(s string) (time.Time, error) {
	// Try parsing as integer (unix timestamp)
	if ts, err := strconv.ParseInt(s, 10, 64); err == nil {
		// Determine if seconds or nanoseconds based on magnitude
		if ts < 10000000000 {
			// Seconds (10 digits)
			return time.Unix(ts, 0), nil
		}
		// Nanoseconds (19 digits)
		return time.Unix(0, ts), nil
	}

	// Try parsing as RFC3339
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}

	// Try parsing as RFC3339Nano
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	return time.Time{}, fmt.Errorf("invalid timestamp format: %s", s)
}

// calculateDefaultStep calculates a reasonable default step based on time range
func calculateDefaultStep(start, end time.Time) string {
	duration := end.Sub(start)

	switch {
	case duration <= 5*time.Minute:
		return "5s"
	case duration <= 30*time.Minute:
		return "15s"
	case duration <= 2*time.Hour:
		return "30s"
	case duration <= 6*time.Hour:
		return "1m"
	case duration <= 24*time.Hour:
		return "5m"
	case duration <= 7*24*time.Hour:
		return "15m"
	default:
		return "1h"
	}
}

// handleMetricsQuery executes instant metric queries (single point in time)
// GET/POST /api/v1/query?query={query}&time={time}
//
// This is the Prometheus instant query endpoint, used by Grafana for single-value queries.
// For range queries, use /api/v1/query_range
func (s *Server) handleMetricsQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	queryStr := q.Get("query")
	if queryStr == "" {
		queryStr = q.Get("q") // Also support 'q' parameter
	}
	if queryStr == "" {
		http.Error(w, "query parameter 'query' or 'q' is required", http.StatusBadRequest)
		return
	}

	// Parse evaluation time (default to now)
	evalTime := time.Now()
	if timeStr := q.Get("time"); timeStr != "" {
		if t, err := parseTimestamp(timeStr); err == nil {
			evalTime = t
		}
	}

	// For instant queries, use a narrow time range around the evaluation time
	// This matches Prometheus behavior
	start := evalTime.Add(-5 * time.Minute)
	end := evalTime

	s.logger.Info("Processing instant metrics query",
		slog.String("query", queryStr),
		slog.Time("time", evalTime))

	// Execute using the same engine as range queries
	opts := &engine.QueryOptions{
		StartTime:   start,
		EndTime:     end,
		Context:     r.Context(),
		UseSnapshot: true,
		Limit:       0,
	}

	result, err := s.queryEngine.Execute(queryStr, opts)
	if err != nil {
		s.logger.Error("Instant query execution failed",
			slog.String("query", queryStr),
			slog.String("error", err.Error()))
		http.Error(w, "query execution failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.logger.Debug("Instant query executed successfully",
		slog.String("query", queryStr),
		slog.String("result_type", string(result.Type)),
		slog.String("stats", result.Stats.String()))

	// Instant queries should return metrics (not spans)
	if result.Type == engine.ResultTypeSpans {
		http.Error(w, "span queries should use /api/search endpoint", http.StatusBadRequest)
		return
	}

	s.handleMetricResult(w, result)
}

// getResultCount returns the count of results based on result type
func getResultCount(result *engine.QueryResult) int {
	switch result.Type {
	case engine.ResultTypeSpans:
		return len(result.Spans)
	case engine.ResultTypeVector:
		return len(result.Vector)
	case engine.ResultTypeMatrix:
		return len(result.Matrix)
	case engine.ResultTypeScalar:
		if result.Scalar != nil {
			return 1
		}
		return 0
	default:
		return 0
	}
}

// Start starts the HTTP server
func (s *Server) Start(addr string) error {
	s.srv = &http.Server{
		Addr:    addr,
		Handler: s,
	}
	s.logger.Info("Tempo API server starting",
		slog.String("addr", addr))
	return s.srv.ListenAndServe()
}

// Shutdown gracefully shuts down the HTTP server
func (s *Server) Shutdown() error {
	if s.srv == nil {
		return nil
	}
	s.logger.Info("Tempo API server shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}
