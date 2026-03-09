package queryapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/tracedb"
)

func TestHealthEndpoint(t *testing.T) {
	// Create temporary database
	cfg := &tracedb.Config{
		WALDir:             t.TempDir(),
		WALSegmentSize:     1024 * 1024,
		CompactInterval:    1 * time.Second,
		CheckpointInterval: 5 * time.Second,
		CheckpointThreshold: 3,
		BlockConfig: &block.Config{
			Dir:              t.TempDir(),
			MaxBlockDuration: 2 * time.Hour,
			MaxBlockSpans:    1000,
		},
	}

	db, err := tracedb.New(cfg)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create Query API server
	server := NewServer(db, nil)

	// Test health endpoint
	req := httptest.NewRequest("GET", "/api/v1/health", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response["status"] != "healthy" {
		t.Errorf("Expected status 'healthy', got '%s'", response["status"])
	}
}

func TestMetadataAttributeKeysEndpoint(t *testing.T) {
	// Create temporary database
	cfg := &tracedb.Config{
		WALDir:             t.TempDir(),
		WALSegmentSize:     1024 * 1024,
		CompactInterval:    1 * time.Second,
		CheckpointInterval: 5 * time.Second,
		CheckpointThreshold: 3,
		BlockConfig: &block.Config{
			Dir:              t.TempDir(),
			MaxBlockDuration: 2 * time.Hour,
			MaxBlockSpans:    1000,
		},
	}

	db, err := tracedb.New(cfg)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create Query API server
	server := NewServer(db, nil)

	// Test metadata attribute_keys endpoint (should return empty list for empty database)
	req := httptest.NewRequest("GET", "/api/v1/metadata/attribute_keys", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response MetadataAttributeKeysResponse
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response.Status != "success" {
		t.Errorf("Expected status 'success', got '%s'", response.Status)
	}

	// Empty database should have 0 attribute keys
	if response.Data.Count != 0 {
		t.Errorf("Expected count 0, got %d", response.Data.Count)
	}
}

func TestMetadataAttributeValuesEndpoint(t *testing.T) {
	// Create temporary database
	cfg := &tracedb.Config{
		WALDir:             t.TempDir(),
		WALSegmentSize:     1024 * 1024,
		CompactInterval:    1 * time.Second,
		CheckpointInterval: 5 * time.Second,
		CheckpointThreshold: 3,
		BlockConfig: &block.Config{
			Dir:              t.TempDir(),
			MaxBlockDuration: 2 * time.Hour,
			MaxBlockSpans:    1000,
		},
	}

	db, err := tracedb.New(cfg)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create Query API server
	server := NewServer(db, nil)

	// Test metadata attribute_values endpoint without key parameter (should error)
	req := httptest.NewRequest("GET", "/api/v1/metadata/attribute_values", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}

	// Test with key parameter
	req = httptest.NewRequest("GET", "/api/v1/metadata/attribute_values?key=http.method", nil)
	w = httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response MetadataAttributeValuesResponse
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response.Status != "success" {
		t.Errorf("Expected status 'success', got '%s'", response.Status)
	}

	if response.Data.AttributeKey != "http.method" {
		t.Errorf("Expected key 'http.method', got '%s'", response.Data.AttributeKey)
	}
}

func TestQueryRangeEndpoint(t *testing.T) {
	// Create temporary database
	cfg := &tracedb.Config{
		WALDir:             t.TempDir(),
		WALSegmentSize:     1024 * 1024,
		CompactInterval:    1 * time.Second,
		CheckpointInterval: 5 * time.Second,
		CheckpointThreshold: 3,
		BlockConfig: &block.Config{
			Dir:              t.TempDir(),
			MaxBlockDuration: 2 * time.Hour,
			MaxBlockSpans:    1000,
		},
	}

	db, err := tracedb.New(cfg)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create Query API server
	server := NewServer(db, nil)

	// Test query_range endpoint without query parameter (should error)
	req := httptest.NewRequest("GET", "/api/v1/query_range", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestQueryTraceEndpoint(t *testing.T) {
	// Create temporary database
	cfg := &tracedb.Config{
		WALDir:             t.TempDir(),
		WALSegmentSize:     1024 * 1024,
		CompactInterval:    1 * time.Second,
		CheckpointInterval: 5 * time.Second,
		CheckpointThreshold: 3,
		BlockConfig: &block.Config{
			Dir:              t.TempDir(),
			MaxBlockDuration: 2 * time.Hour,
			MaxBlockSpans:    1000,
		},
	}

	db, err := tracedb.New(cfg)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create Query API server
	server := NewServer(db, nil)

	// Test query/trace endpoint without traceID parameter (should error)
	req := httptest.NewRequest("GET", "/api/v1/query/trace", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestCORSHeaders(t *testing.T) {
	// Create temporary database
	cfg := &tracedb.Config{
		WALDir:             t.TempDir(),
		WALSegmentSize:     1024 * 1024,
		CompactInterval:    1 * time.Second,
		CheckpointInterval: 5 * time.Second,
		CheckpointThreshold: 3,
		BlockConfig: &block.Config{
			Dir:              t.TempDir(),
			MaxBlockDuration: 2 * time.Hour,
			MaxBlockSpans:    1000,
		},
	}

	db, err := tracedb.New(cfg)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create Query API server
	server := NewServer(db, nil)

	// Test OPTIONS request (CORS preflight)
	req := httptest.NewRequest("OPTIONS", "/api/v1/health", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 for OPTIONS, got %d", w.Code)
	}

	// Check CORS headers
	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("Expected CORS origin header")
	}
}
