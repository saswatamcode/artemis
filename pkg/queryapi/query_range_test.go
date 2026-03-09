package queryapi

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/reduced_promql/engine"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

func TestQueryRangeWithExemplars(t *testing.T) {
	// Setup: Create in-memory storage and populate with test data
	arrowStorage := storage.NewArrowStorage()
	linkStorage := storage.NewArrowLinkStorage()
	isolation := storage.NewIsolationCoordinator()

	// Create test spans over a 10-minute window with consistent rate
	now := time.Now()
	baseTime := now.Add(-10 * time.Minute)

	// Create 10 spans per minute for 10 minutes = 100 spans total
	spansPerMin := 10
	durationMins := 10

	for min := 0; min < durationMins; min++ {
		for i := 0; i < spansPerMin; i++ {
			// Spread spans evenly across the minute
			offsetSeconds := (60 / spansPerMin) * i
			spanTime := baseTime.Add(time.Duration(min)*time.Minute + time.Duration(offsetSeconds)*time.Second)

			sp := &span.Span{
				TraceID:     fmt.Sprintf("trace-%d-%d", min, i),
				SpanID:      fmt.Sprintf("span-%d-%d", min, i),
				Name:        "testOp",
				ServiceName: "testService",
				StartTime:   spanTime,
				EndTime:     spanTime.Add(100 * time.Millisecond),
				Duration:    int64(100 * time.Millisecond),
				Tags: map[string]string{
					"op":   "testOp",
					"lang": "go",
				},
			}

			if err := arrowStorage.AddSpan(sp); err != nil {
				t.Fatalf("Failed to add span: %v", err)
			}
		}
	}

	// Flush the builder to create record batches
	if err := arrowStorage.Flush(); err != nil {
		t.Fatalf("Failed to flush storage: %v", err)
	}

	t.Logf("Created %d test spans over %d minutes", spansPerMin*durationMins, durationMins)

	// Wrap in HeadBlock
	headBlock := block.NewHeadBlock(arrowStorage, linkStorage)

	// Create engine
	blockGetter := func() []block.Block {
		return []block.Block{headBlock}
	}
	queryEngine := engine.NewEngine(blockGetter, isolation)

	// Create server
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	server := &Server{
		queryEngine: queryEngine,
		logger:      logger,
	}

	// Test: Execute rate query with exemplars
	t.Run("RateQueryWithExemplars", func(t *testing.T) {
		// Query: rate({service_name="testService"}[5m]) over 10 minutes with 1m step
		// Using service_name since it's a top-level field that's reliably indexed
		// This should produce ~10 data points
		req := httptest.NewRequest(
			"GET",
			fmt.Sprintf("/api/v1/query_range?query=rate({service_name=\"testService\"}[5m])&start=%d&end=%d&step=1m&exemplars=3&exemplar_strategy=slowest",
				baseTime.Unix(),
				now.Unix(),
			),
			nil,
		)
		w := httptest.NewRecorder()

		server.handleQueryRange(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d. Body: %s", w.Code, w.Body.String())
		}

		// Parse response
		var response map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatalf("Failed to parse response: %v", err)
		}

		// Check status
		if status, ok := response["status"].(string); !ok || status != "success" {
			t.Errorf("Expected status 'success', got %v", response["status"])
		}

		data, ok := response["data"].(map[string]interface{})
		if !ok {
			t.Fatalf("Expected data object, got %v", response["data"])
		}

		// Check result type
		resultType, ok := data["resultType"].(string)
		if !ok || resultType != "matrix" {
			t.Errorf("Expected resultType 'matrix', got %v", data["resultType"])
		}

		// Check results
		results, ok := data["result"].([]interface{})
		if !ok {
			t.Fatalf("Expected result array, got %v", data["result"])
		}

		if len(results) == 0 {
			t.Fatal("Expected at least one series, got none")
		}

		// Check first series
		series, ok := results[0].(map[string]interface{})
		if !ok {
			t.Fatalf("Expected series object, got %v", results[0])
		}

		// Check values (data points)
		values, ok := series["values"].([]interface{})
		if !ok {
			t.Fatalf("Expected values array, got %v", series["values"])
		}

		if len(values) < 5 {
			t.Errorf("Expected at least 5 data points, got %d", len(values))
		}

		t.Logf("Got %d data points in time series", len(values))

		// Log sample values
		for i := 0; i < min(3, len(values)); i++ {
			val := values[i].([]interface{})
			t.Logf("  Point %d: timestamp=%v, rate=%v", i, val[0], val[1])
		}

		// Check exemplars
		exemplars, ok := series["exemplars"].([]interface{})
		if !ok {
			t.Fatalf("Expected exemplars array, got %v. Series: %+v", series["exemplars"], series)
		}

		if len(exemplars) == 0 {
			t.Error("Expected at least one exemplar, got none")
		}

		t.Logf("Got %d exemplars total", len(exemplars))

		// Verify exemplar structure
		for i, ex := range exemplars {
			if i >= 3 {
				break
			}
			exMap, ok := ex.(map[string]interface{})
			if !ok {
				t.Errorf("Exemplar %d: expected object, got %v", i, ex)
				continue
			}

			// Check required fields
			if _, ok := exMap["timestamp"]; !ok {
				t.Errorf("Exemplar %d: missing timestamp", i)
			}
			if _, ok := exMap["traceID"]; !ok {
				t.Errorf("Exemplar %d: missing traceID", i)
			}
			if _, ok := exMap["duration"]; !ok {
				t.Errorf("Exemplar %d: missing duration", i)
			}

			t.Logf("  Exemplar %d: traceID=%v, timestamp=%v, duration=%v",
				i, exMap["traceID"], exMap["timestamp"], exMap["duration"])
		}

		// Ideally we should have exemplars spread across the time range
		// With ~10 data points and 3 exemplars per step, we should have up to 30 exemplars
		// But since we're bucketing, we might have fewer
		if len(exemplars) < len(values) {
			t.Logf("Note: Got %d exemplars for %d data points (expected similar counts)", len(exemplars), len(values))
		}
	})

	// Test: Query multiple times to verify deterministic exemplars
	t.Run("DeterministicExemplars", func(t *testing.T) {
		// Execute the same query 5 times
		var allResults []map[string]interface{}

		for i := 0; i < 5; i++ {
			req := httptest.NewRequest(
				"GET",
				fmt.Sprintf("/api/v1/query_range?query=rate({service_name=\"testService\"}[5m])&start=%d&end=%d&step=1m&exemplars=3&exemplar_strategy=slowest",
					baseTime.Unix(),
					now.Unix(),
				),
				nil,
			)
			w := httptest.NewRecorder()

			server.handleQueryRange(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("Run %d: Expected status 200, got %d", i, w.Code)
			}

			var response map[string]interface{}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatalf("Run %d: Failed to parse response: %v", i, err)
			}

			allResults = append(allResults, response)
		}

		// Compare all results - they should be identical (except stats.executionTime)
		for i := 1; i < len(allResults); i++ {
			// Remove stats before comparison (execution time varies)
			data0 := allResults[0]["data"].(map[string]interface{})
			data1 := allResults[i]["data"].(map[string]interface{})

			// Remove stats
			delete(data0, "stats")
			delete(data1, "stats")

			result1JSON, _ := json.MarshalIndent(allResults[0], "", "  ")
			result2JSON, _ := json.MarshalIndent(allResults[i], "", "  ")

			if string(result1JSON) != string(result2JSON) {
				t.Errorf("Run %d differs from run 0", i)
				t.Logf("Run 0:\n%s", string(result1JSON[:min(2000, len(result1JSON))]))
				t.Logf("Run %d:\n%s", i, string(result2JSON[:min(2000, len(result2JSON))]))
			}
		}

		t.Logf("✓ All 5 query executions returned identical results (deterministic)")
	})
}

// Helper function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
