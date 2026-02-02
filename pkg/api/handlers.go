package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/saswatamcode/artemis/pkg/query"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/tracedb"
)

// Server provides Jaeger-compatible HTTP API for trace queries
type Server struct {
	db     *tracedb.DB
	mux    *http.ServeMux
	logger *slog.Logger
	srv    *http.Server
}

// NewServer creates a new API server
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

// registerRoutes sets up HTTP routes
func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/api/traces/", s.handleTraces)
	s.mux.HandleFunc("/api/traces", s.handleTraces) // Handle exact match too
	s.mux.HandleFunc("/api/services/", s.handleServices)
	s.mux.HandleFunc("/api/services", s.handleServices) // Handle exact match too
	s.mux.HandleFunc("/health", s.handleHealth)
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

// handleTraces routes between trace lookup and search
func (s *Server) handleTraces(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/traces")
	path = strings.TrimPrefix(path, "/")

	if path != "" && !strings.Contains(path, "/") {
		// Single trace lookup: /api/traces/{traceID}
		s.handleGetTrace(w, r, path)
	} else {
		// Trace search: /api/traces or /api/traces/
		s.handleSearchTraces(w, r)
	}
}

// handleServices routes between service list and operations
func (s *Server) handleServices(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/services")
	path = strings.TrimPrefix(path, "/")

	if before, ok := strings.CutSuffix(path, "/operations"); ok {
		// Get operations: /api/services/{service}/operations
		service := before
		s.handleGetOperations(w, r, service)
	} else if path != "" {
		// If there's a service name but no /operations suffix, treat as operations request
		// (some clients might call /api/services/{service} expecting operations)
		s.handleGetOperations(w, r, path)
	} else {
		// Get services: /api/services or /api/services/
		s.handleGetServices(w, r)
	}
}

// handleGetTrace retrieves a specific trace by ID
// GET /api/traces/{traceID}
func (s *Server) handleGetTrace(w http.ResponseWriter, r *http.Request, traceID string) {
	if traceID == "" {
		http.Error(w, "trace ID required", http.StatusBadRequest)
		return
	}

	s.logger.Debug("Looking up trace by ID", slog.String("trace_id", traceID))

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

	jaegerTrace := ConvertTraceToJaeger(result.Spans)

	response := struct {
		Data   []Trace `json:"data"`
		Total  int     `json:"total"`
		Limit  int     `json:"limit"`
		Offset int     `json:"offset"`
		Errors []any   `json:"errors"`
	}{
		Data:   []Trace{*jaegerTrace},
		Total:  1,
		Limit:  1,
		Offset: 0,
		Errors: nil,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleSearchTraces searches for traces based on criteria
// GET /api/traces?service=foo&operation=bar&start=...&end=...&limit=20
func (s *Server) handleSearchTraces(w http.ResponseWriter, r *http.Request) {
	params := parseTraceQueryParams(r)

	s.logger.Debug("Received search request",
		slog.String("service", params.Service),
		slog.String("operation", params.Operation),
		slog.Time("start", params.StartTime),
		slog.Time("end", params.EndTime),
		slog.Any("tags", params.Tags))

	var matchers []*query.Matcher

	if params.Service != "" {
		m, _ := query.NewMatcher(query.MatchEqual, "service.name", params.Service)
		matchers = append(matchers, m)
	}

	if params.Operation != "" {
		m, _ := query.NewMatcher(query.MatchEqual, "name", params.Operation)
		matchers = append(matchers, m)
		s.logger.Debug("Filtering by operation", slog.String("operation", params.Operation))
	}

	for k, v := range params.Tags {
		m, _ := query.NewMatcher(query.MatchEqual, k, v)
		matchers = append(matchers, m)
	}

	var timeRange *query.TimeRange
	if !params.StartTime.IsZero() && !params.EndTime.IsZero() {
		timeRange = query.NewTimeRange(params.StartTime, params.EndTime)
	} else {
		// Default: last 1 hour
		timeRange = query.NewTimeRange(time.Now().Add(-1*time.Hour), time.Now())
	}

	querier := s.db.GetQuerier()
	result, err := querier.SelectWithTimeRange(timeRange, matchers...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.logger.Debug("Query completed",
		slog.Int("span_count", len(result.Spans)),
		slog.Time("time_range_start", timeRange.Start),
		slog.Time("time_range_end", timeRange.End))

	filteredSpans := filterByDuration(result.Spans, params.MinDuration, params.MaxDuration)

	traceMap := make(map[string][]*span.Span)
	for _, sp := range filteredSpans {
		traceMap[sp.TraceID] = append(traceMap[sp.TraceID], sp)
	}

	traces := make([]Trace, 0, len(traceMap))
	for _, spans := range traceMap {
		if jaegerTrace := ConvertTraceToJaeger(spans); jaegerTrace != nil {
			traces = append(traces, *jaegerTrace)
		}
	}

	limit := params.Limit
	if limit == 0 {
		limit = 20
	}
	if len(traces) > limit {
		traces = traces[:limit]
	}

	response := TraceQueryResponse{
		Data:   traces,
		Total:  len(traces),
		Limit:  limit,
		Offset: 0,
		Errors: nil,
	}

	s.logger.Debug("Returning search results",
		slog.Int("trace_count", len(traces)),
		slog.Int("span_group_count", len(traceMap)),
		slog.Int("filtered_span_count", len(filteredSpans)))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleGetServices returns list of services
// GET /api/services
func (s *Server) handleGetServices(w http.ResponseWriter, _ *http.Request) {
	timeRange := query.NewTimeRange(time.Now().Add(-24*time.Hour), time.Now())

	querier := s.db.GetQuerier()
	result, err := querier.SelectWithTimeRange(timeRange)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	serviceSet := make(map[string]bool)
	for _, sp := range result.Spans {
		serviceSet[sp.ServiceName] = true
	}

	services := make([]string, 0, len(serviceSet))
	for service := range serviceSet {
		services = append(services, service)
	}

	response := ServicesResponse{
		Data: services,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleGetOperations returns list of operations for a service
// GET /api/services/{service}/operations
func (s *Server) handleGetOperations(w http.ResponseWriter, _ *http.Request, service string) {
	if service == "" {
		http.Error(w, "service required", http.StatusBadRequest)
		return
	}

	matcher, _ := query.NewMatcher(query.MatchEqual, "service.name", service)

	timeRange := query.NewTimeRange(time.Now().Add(-24*time.Hour), time.Now())

	querier := s.db.GetQuerier()
	result, err := querier.SelectWithTimeRange(timeRange, matcher)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	operationSet := make(map[string]bool)
	for _, sp := range result.Spans {
		operationSet[sp.Name] = true
	}

	operations := make([]string, 0, len(operationSet))
	for name := range operationSet {
		operations = append(operations, name)
	}

	response := OperationsResponse{
		Data: operations,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleHealth returns server health status
// GET /health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	stats := s.db.Stats()

	health := struct {
		Status string `json:"status"`
		Spans  int64  `json:"spans"`
	}{
		Status: "ok",
		Spans:  stats.TotalSpans,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

// parseTraceQueryParams parses query parameters for trace search
func parseTraceQueryParams(r *http.Request) TraceQueryParams {
	q := r.URL.Query()

	params := TraceQueryParams{
		Service:   q.Get("service"),
		Operation: q.Get("operation"),
		Tags:      make(map[string]string),
	}

	for k, v := range q {
		if strings.HasPrefix(k, "tag.") && len(v) > 0 {
			tagKey := strings.TrimPrefix(k, "tag.")
			params.Tags[tagKey] = v[0]
		}
	}

	// Timestamps are expected in Unix seconds
	if start := q.Get("start"); start != "" {
		if ts, err := strconv.ParseInt(start, 10, 64); err == nil {
			params.StartTime = time.Unix(ts, 0)
		}
	}

	if end := q.Get("end"); end != "" {
		if ts, err := strconv.ParseInt(end, 10, 64); err == nil {
			params.EndTime = time.Unix(ts, 0)
		}
	}

	// Parse durations
	if minDur := q.Get("minDuration"); minDur != "" {
		if d, err := time.ParseDuration(minDur); err == nil {
			params.MinDuration = d
		} else if micros, err := strconv.ParseInt(minDur, 10, 64); err == nil {
			params.MinDuration = time.Duration(micros) * time.Microsecond
		}
	}

	if maxDur := q.Get("maxDuration"); maxDur != "" {
		if d, err := time.ParseDuration(maxDur); err == nil {
			params.MaxDuration = d
		} else if micros, err := strconv.ParseInt(maxDur, 10, 64); err == nil {
			params.MaxDuration = time.Duration(micros) * time.Microsecond
		}
	}

	if limit := q.Get("limit"); limit != "" {
		if l, err := strconv.Atoi(limit); err == nil {
			params.Limit = l
		}
	}

	if lookback := q.Get("lookback"); lookback != "" {
		if days, err := strconv.Atoi(lookback); err == nil {
			params.LookbackDays = days
			if params.StartTime.IsZero() {
				params.StartTime = time.Now().Add(-time.Duration(days) * 24 * time.Hour)
				params.EndTime = time.Now()
			}
		}
	}

	return params
}

// filterByDuration filters spans by duration range
func filterByDuration(spans []*span.Span, minDur, maxDur time.Duration) []*span.Span {
	if minDur == 0 && maxDur == 0 {
		return spans
	}

	filtered := make([]*span.Span, 0, len(spans))
	for _, sp := range spans {
		duration := time.Duration(sp.Duration)

		if minDur > 0 && duration < minDur {
			continue
		}

		if maxDur > 0 && duration > maxDur {
			continue
		}

		filtered = append(filtered, sp)
	}

	return filtered
}

// Start starts the HTTP server
func (s *Server) Start(addr string) error {
	s.srv = &http.Server{
		Addr:    addr,
		Handler: s,
	}
	s.logger.Info("API server starting", slog.String("addr", addr))
	return s.srv.ListenAndServe()
}

// Shutdown gracefully shuts down the HTTP server
func (s *Server) Shutdown() error {
	if s.srv == nil {
		return nil
	}
	s.logger.Info("API server shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}
