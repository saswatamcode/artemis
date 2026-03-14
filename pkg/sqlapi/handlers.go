package sqlapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/saswatamcode/artemis/pkg/metrics"
	"github.com/saswatamcode/artemis/pkg/query"
	"github.com/saswatamcode/artemis/pkg/tracedb"
)

// Server provides SQL query API for trace queries
type Server struct {
	db         *tracedb.DB
	mux        *http.ServeMux
	logger     *slog.Logger
	srv        *http.Server
	dbMetrics  *metrics.DatabaseMetrics
	apiMetrics *metrics.APIMetrics
}

// NewServer creates a new SQL API server
func NewServer(db *tracedb.DB, logger *slog.Logger, dbMetrics *metrics.DatabaseMetrics, apiMetrics *metrics.APIMetrics) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	s := &Server{
		db:         db,
		mux:        http.NewServeMux(),
		logger:     logger,
		dbMetrics:  dbMetrics,
		apiMetrics: apiMetrics,
	}

	s.registerRoutes()
	return s
}

// registerRoutes sets up HTTP routes
func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/api/query", s.handleSQLQuery)
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

	// Wrap with metrics middleware
	handler := http.Handler(s.mux)
	if s.apiMetrics != nil {
		handler = metrics.HTTPMiddleware("sqlapi", s.apiMetrics)(handler)
	}
	handler.ServeHTTP(w, r)
}

// handleSQLQuery executes a SQL query against the trace database
// POST /api/query
// Body: {"query": "SELECT * FROM spans WHERE service_name = 'my-service' LIMIT 10"}
func (s *Server) handleSQLQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SQLQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Query == "" {
		http.Error(w, "query is required", http.StatusBadRequest)
		return
	}

	s.logger.Debug("Received SQL query", slog.String("query", req.Query))

	// Create SQL querier
	sqlQuerier, err := query.NewSQLQuerier()
	if err != nil {
		s.logger.Error("Failed to create SQL querier", slog.String("error", err.Error()))
		http.Error(w, "failed to create SQL querier: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer sqlQuerier.Close()

	// Load blocks from database
	blocks := s.db.GetBlocksForQuery()

	// For now, we only query persisted blocks (not the head block)
	// TODO: Add support for querying the head block as well
	if err := sqlQuerier.LoadBlocks(nil, blocks); err != nil {
		s.logger.Error("Failed to load blocks", slog.String("error", err.Error()))
		http.Error(w, "failed to load blocks: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Execute SQL query
	result, err := sqlQuerier.SelectSQL(req.Query)
	if err != nil {
		s.logger.Error("SQL query failed", slog.String("error", err.Error()))
		http.Error(w, "query failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.logger.Debug("SQL query completed",
		slog.Int("row_count", result.RowCount()),
		slog.Bool("is_span_result", result.IsSpanResult()))

	// Build response
	response := SQLQueryResponse{
		Columns: result.Columns,
		Success: true,
	}

	if result.IsSpanResult() {
		// Convert spans to generic rows for display
		rows := make([]map[string]interface{}, len(result.Spans))
		for i, sp := range result.Spans {
			rows[i] = map[string]interface{}{
				"trace_id":       sp.TraceID,
				"span_id":        sp.SpanID,
				"parent_span_id": sp.ParentSpanID,
				"name":           sp.Name,
				"start_time":     sp.StartTime.UnixNano(),
				"end_time":       sp.EndTime.UnixNano(),
				"duration":       sp.Duration,
				"service_name":   sp.ServiceName,
				"tags":           sp.Tags,
			}
		}
		response.Rows = rows
		response.RowCount = len(rows)
	} else {
		response.Rows = result.Rows
		response.RowCount = len(result.Rows)
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

// Start starts the HTTP server
func (s *Server) Start(addr string) error {
	s.srv = &http.Server{
		Addr:    addr,
		Handler: s,
	}
	s.logger.Info("SQL API server starting", slog.String("addr", addr))
	return s.srv.ListenAndServe()
}

// Shutdown gracefully shuts down the HTTP server
func (s *Server) Shutdown() error {
	if s.srv == nil {
		return nil
	}
	s.logger.Info("SQL API server shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}
