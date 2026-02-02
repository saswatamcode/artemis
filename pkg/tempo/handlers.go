package tempo

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/saswatamcode/artemis/pkg/query"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/tracedb"

	"github.com/prometheus/common/version"
	coltracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// Server provides Tempo-compatible HTTP API for trace queries
type Server struct {
	db     *tracedb.DB
	mux    *http.ServeMux
	logger *slog.Logger
	srv    *http.Server
}

// NewServer creates a new Tempo API server
func NewServer(db *tracedb.DB, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	s := &Server{
		db:     db,
		mux:    http.NewServeMux(),
		logger: logger,
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

// handleSearch searches for traces
// GET /api/search?q={query}&start={start}&end={end}&limit={limit}
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

	s.logger.Info("Processing search query",
		slog.String("query", queryStr),
		slog.Time("start", start),
		slog.Time("end", end),
		slog.Int("limit", limit))

	var matchers []*query.Matcher
	if queryStr != "" {
		// Try to parse as TraceQL query (e.g., {resource.service.name=prometheus})
		// If it fails, treat it as a simple service name
		parsedMatchers := parseTraceQL(queryStr)
		if len(parsedMatchers) > 0 {
			matchers = parsedMatchers
		} else {
			// Fallback: treat as simple service name
			m, _ := query.NewMatcher(query.MatchEqual, "service.name", queryStr)
			matchers = append(matchers, m)
		}
	}

	// Query database
	timeRange := query.NewTimeRange(start, end)
	querier := s.db.GetQuerier()
	result, err := querier.SelectWithTimeRange(timeRange, matchers...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	blockCount := len(s.db.GetBlocks())

	traceSpans := make(map[string][]*span.Span)
	for _, sp := range result.Spans {
		traceSpans[sp.TraceID] = append(traceSpans[sp.TraceID], sp)
	}

	traces := ConvertSpansToSearchMetadata(traceSpans)

	if len(traces) > limit {
		traces = traces[:limit]
	}

	response := SearchResponse{
		Traces: traces,
		Metrics: SearchMetrics{
			InspectedTraces: len(traceSpans),
			InspectedBlocks: blockCount + 1, // +1 for head
			TotalBlocks:     blockCount + 1,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
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

// parseTraceQL parses a basic TraceQL query and returns matchers
// Supports queries like: {resource.service.name=prometheus}
func parseTraceQL(queryStr string) []*query.Matcher {
	var matchers []*query.Matcher

	// Remove outer braces if present
	queryStr = strings.TrimSpace(queryStr)
	if strings.HasPrefix(queryStr, "{") && strings.HasSuffix(queryStr, "}") {
		queryStr = queryStr[1 : len(queryStr)-1]
	}

	// Split by && for multiple conditions (basic support)
	conditions := strings.SplitSeq(queryStr, "&&")

	for condition := range conditions {
		condition = strings.TrimSpace(condition)

		// Parse key=value or key="value"
		var key, value string
		if idx := strings.Index(condition, "="); idx > 0 {
			key = strings.TrimSpace(condition[:idx])
			value = strings.TrimSpace(condition[idx+1:])

			// Remove quotes from value
			value = strings.Trim(value, "\"'")

			// Map TraceQL field names to internal tag names
			// resource.service.name -> service.name
			// span.http.method -> http.method
			// .name -> name
			key = strings.TrimPrefix(key, ".")
			key = strings.TrimPrefix(key, "resource.")
			key = strings.TrimPrefix(key, "span.")

			if m, err := query.NewMatcher(query.MatchEqual, key, value); err == nil {
				matchers = append(matchers, m)
			}
		}
	}

	return matchers
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
