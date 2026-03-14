package queryapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/saswatamcode/artemis/pkg/metrics"
	"github.com/saswatamcode/artemis/pkg/reduced_promql/engine"
	"github.com/saswatamcode/artemis/pkg/tracedb"
	"github.com/saswatamcode/artemis/pkg/ui"
)

// Server provides Query API for metadata discovery, metric queries with exemplars, and trace retrieval
type Server struct {
	db          *tracedb.DB
	queryEngine *engine.Engine
	mux         *http.ServeMux
	logger      *slog.Logger
	srv         *http.Server
	dbMetrics   *metrics.DatabaseMetrics
	apiMetrics  *metrics.APIMetrics
}

// NewServer creates a new Query API server
func NewServer(db *tracedb.DB, logger *slog.Logger, dbMetrics *metrics.DatabaseMetrics, apiMetrics *metrics.APIMetrics) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	// Create query engine once for reuse across all queries
	isolation := db.GetIsolation()
	queryEngine := engine.NewEngine(db.GetBlocks, isolation)

	s := &Server{
		db:          db,
		queryEngine: queryEngine,
		mux:         http.NewServeMux(),
		logger:      logger,
		dbMetrics:   dbMetrics,
		apiMetrics:  apiMetrics,
	}

	s.registerRoutes()
	return s
}

// registerRoutes sets up HTTP routes for Query API
func (s *Server) registerRoutes() {
	// Metadata endpoints
	s.mux.HandleFunc("/api/v1/metadata/attribute_keys", s.handleMetadataAttributeKeys)
	s.mux.HandleFunc("/api/v1/metadata/attribute_values", s.handleMetadataAttributeValues)

	// Query range with optional exemplars
	s.mux.HandleFunc("/api/v1/query_range", s.handleQueryRange)

	// Trace detail retrieval
	s.mux.HandleFunc("/api/v1/query/trace", s.handleQueryTrace)

	// Health check
	s.mux.HandleFunc("/api/v1/health", s.handleHealth)

	// UI routes - serve embedded React app
	s.registerUIRoutes()
}

// registerUIRoutes sets up routes for serving the React UI
func (s *Server) registerUIRoutes() {
	// Serve index.html for root and React router paths
	s.mux.HandleFunc("/", s.serveIndex)
	s.mux.HandleFunc("/trace/", s.serveIndex) // React router path for trace details

	// Serve static assets (JS, CSS, images, etc.)
	s.mux.HandleFunc("/assets/", s.serveStaticAssets)
	s.mux.HandleFunc("/vite.svg", s.serveStaticFile)
}

// serveIndex serves the React app's index.html
func (s *Server) serveIndex(w http.ResponseWriter, r *http.Request) {
	// Only serve index.html for exact root or React router paths
	if r.URL.Path != "/" && !strings.HasPrefix(r.URL.Path, "/trace/") {
		http.NotFound(w, r)
		return
	}

	indexFile, err := ui.Assets.Open("index.html")
	if err != nil {
		s.logger.Error("Failed to open index.html", slog.String("error", err.Error()))
		http.Error(w, "UI not available", http.StatusNotFound)
		return
	}
	defer indexFile.Close()

	// Read file content
	content, err := io.ReadAll(indexFile)
	if err != nil {
		s.logger.Error("Failed to read index.html", slog.String("error", err.Error()))
		http.Error(w, "Failed to read UI", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write(content)
}

// serveStaticAssets serves static assets (JS, CSS, etc.)
func (s *Server) serveStaticAssets(w http.ResponseWriter, r *http.Request) {
	// Strip leading slash and serve from assets directory
	assetPath := strings.TrimPrefix(r.URL.Path, "/")

	file, err := ui.Assets.Open(assetPath)
	if err != nil {
		s.logger.Debug("Asset not found", slog.String("path", assetPath))
		http.NotFound(w, r)
		return
	}
	defer file.Close()

	// Get file info for size
	stat, err := file.Stat()
	if err != nil {
		http.Error(w, "Failed to stat file", http.StatusInternalServerError)
		return
	}

	// Set content type based on file extension
	ext := path.Ext(assetPath)
	contentType := getContentType(ext)
	w.Header().Set("Content-Type", contentType)

	// Set cache headers for assets
	w.Header().Set("Cache-Control", "public, max-age=31536000") // 1 year

	http.ServeContent(w, r, assetPath, stat.ModTime(), file.(io.ReadSeeker))
}

// serveStaticFile serves individual static files (like favicon)
func (s *Server) serveStaticFile(w http.ResponseWriter, r *http.Request) {
	fileName := strings.TrimPrefix(r.URL.Path, "/")

	file, err := ui.Assets.Open(fileName)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		http.Error(w, "Failed to stat file", http.StatusInternalServerError)
		return
	}

	ext := path.Ext(fileName)
	contentType := getContentType(ext)
	w.Header().Set("Content-Type", contentType)

	http.ServeContent(w, r, fileName, stat.ModTime(), file.(io.ReadSeeker))
}

// getContentType returns the MIME type based on file extension
func getContentType(ext string) string {
	switch ext {
	case ".html":
		return "text/html; charset=utf-8"
	case ".js":
		return "application/javascript; charset=utf-8"
	case ".css":
		return "text/css; charset=utf-8"
	case ".json":
		return "application/json; charset=utf-8"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".svg":
		return "image/svg+xml"
	case ".ico":
		return "image/x-icon"
	case ".woff":
		return "font/woff"
	case ".woff2":
		return "font/woff2"
	case ".ttf":
		return "font/ttf"
	default:
		return "application/octet-stream"
	}
}

// ServeHTTP implements http.Handler
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// CORS headers (match tempo/handlers.go pattern)
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
		handler = metrics.HTTPMiddleware("queryapi", s.apiMetrics)(handler)
	}
	handler.ServeHTTP(w, r)
}

// Start starts the HTTP server
func (s *Server) Start(addr string) error {
	s.srv = &http.Server{
		Addr:    addr,
		Handler: s,
	}
	s.logger.Info("Starting Query API server", slog.String("addr", addr))
	return s.srv.ListenAndServe()
}

// Shutdown gracefully shuts down the HTTP server
func (s *Server) Shutdown() error {
	if s.srv != nil {
		s.logger.Info("Query API server shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.srv.Shutdown(ctx)
	}
	return nil
}

// handleHealth is a health check endpoint
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, map[string]string{
		"status": "healthy",
	})
}
