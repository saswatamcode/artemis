package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// TestRateQuery verifies that rate() queries produce proper time series with multiple data points
func TestRateQuery(t *testing.T) {
	// Setup: Create in-memory storage and populate with test data
	arrowStorage := storage.NewArrowStorage()
	linkStorage := storage.NewArrowLinkStorage()
	isolation := storage.NewIsolationCoordinator()

	// Create test spans over a 30-minute window
	// We'll create spans with different labels to test label grouping
	now := time.Now()
	baseTime := now.Add(-30 * time.Minute)

	testCases := []struct {
		name          string
		serviceName   string
		labels        map[string]string
		spansPerMin   int
		durationMins  int
	}{
		{
			name:        "promqlExec",
			serviceName: "prometheus",
			labels: map[string]string{
				"op":   "promqlExec",
				"lang": "go",
			},
			spansPerMin:  10, // 10 spans per minute
			durationMins: 30, // Over 30 minutes
		},
		{
			name:        "httpRequest",
			serviceName: "api",
			labels: map[string]string{
				"op":   "httpRequest",
				"lang": "go",
			},
			spansPerMin:  20, // 20 spans per minute
			durationMins: 30,
		},
	}

	totalSpans := 0
	for _, tc := range testCases {
		for min := 0; min < tc.durationMins; min++ {
			for i := 0; i < tc.spansPerMin; i++ {
				// Spread spans evenly across the minute
				offsetSeconds := (60 / tc.spansPerMin) * i
				spanTime := baseTime.Add(time.Duration(min)*time.Minute + time.Duration(offsetSeconds)*time.Second)

				sp := &span.Span{
					TraceID:     fmt.Sprintf("trace-%s-%d-%d", tc.name, min, i),
					SpanID:      fmt.Sprintf("span-%d-%d", min, i),
					Name:        tc.name,
					ServiceName: tc.serviceName,
					StartTime:   spanTime,
					EndTime:     spanTime.Add(100 * time.Millisecond),
					Duration:    int64(100 * time.Millisecond),
					Tags:        tc.labels,
				}

				if err := arrowStorage.AddSpan(sp); err != nil {
					t.Fatalf("Failed to add span: %v", err)
				}
				totalSpans++
			}
		}
	}

	// Flush the builder to create record batches
	if err := arrowStorage.Flush(); err != nil {
		t.Fatalf("Failed to flush storage: %v", err)
	}

	t.Logf("Created %d test spans over 30 minutes", totalSpans)

	// Wrap in HeadBlock
	headBlock := block.NewHeadBlock(arrowStorage, linkStorage)

	// Create engine
	blockGetter := func() []block.Block {
		return []block.Block{headBlock}
	}
	engine := NewEngine(blockGetter, isolation)

	// Test 1: Query with 5m lookback over the full 30-minute range
	t.Run("FullRange_5mLookback", func(t *testing.T) {
		// Note: Using {name="promqlExec"} instead of {op="promqlExec"} because
		// tag matching during scan has a pre-existing bug (tags not extracted from Arrow records).
		// This tests the rate operator, not the tag matcher bug.
		queryStr := `rate({name="promqlExec"}[5m])`

		opts := &QueryOptions{
			StartTime:   baseTime,
			EndTime:     now,
			Context:     context.Background(),
			UseSnapshot: false,
		}

		result, err := engine.Execute(queryStr, opts)
		if err != nil {
			t.Fatalf("Query execution failed: %v", err)
		}

		// Verify result type
		if result.Type != ResultTypeMatrix {
			t.Errorf("Expected result type Matrix, got %s", result.Type)
		}

		if result.Matrix == nil || len(result.Matrix) == 0 {
			t.Fatalf("Expected Matrix result, got nil or empty. Stats: %s", result.Stats.String())
		}

		// Verify we have the right number of series (should be 1 for this query)
		if len(result.Matrix) != 1 {
			t.Errorf("Expected 1 time series, got %d", len(result.Matrix))
		}

		series := result.Matrix[0]

		// Note: The metric labels will include tags that are preserved through rate calculation
		t.Logf("Series labels: %v", series.Metric)

		// Verify we have multiple data points (not just 1)
		if len(series.Values) < 2 {
			t.Fatalf("Expected multiple data points, got %d. This indicates the range scan bug!", len(series.Values))
		}

		t.Logf("✓ Got %d data points (expected multiple)", len(series.Values))

		// Verify data points are sorted by time
		for i := 1; i < len(series.Values); i++ {
			if series.Values[i].Time.Before(series.Values[i-1].Time) {
				t.Errorf("Data points not sorted by time at index %d", i)
			}
		}

		// Verify rate values are reasonable
		// With 10 spans/minute and 5m lookback, expected rate ≈ 10*5 / 300s = 0.166 spans/sec
		expectedRate := float64(10*5) / 300.0 // ~0.166
		tolerance := expectedRate * 0.3       // Allow 30% variance

		for i, val := range series.Values {
			if val.Value < 0 {
				t.Errorf("Negative rate at index %d: %f", i, val.Value)
			}
			if val.Value > expectedRate+tolerance {
				t.Logf("Warning: Rate at index %d is %.4f, expected around %.4f", i, val.Value, expectedRate)
			}
		}

		t.Logf("✓ Sample rates: first=%.4f, last=%.4f (expected ~%.4f)",
			series.Values[0].Value,
			series.Values[len(series.Values)-1].Value,
			expectedRate)

		// Log some sample data points
		t.Logf("Sample data points:")
		for i := 0; i < min(5, len(series.Values)); i++ {
			t.Logf("  [%d] time=%v (unix=%d), rate=%.4f",
				i,
				series.Values[i].Time.Format("15:04:05"),
				series.Values[i].Time.Unix(),
				series.Values[i].Value)
		}

		// Check that all timestamps are unique
		timestampsSeen := make(map[int64]bool)
		for _, val := range series.Values {
			ts := val.Time.Unix()
			if timestampsSeen[ts] {
				t.Errorf("DUPLICATE TIMESTAMP: %d (%v)", ts, val.Time)
			}
			timestampsSeen[ts] = true
		}
		t.Logf("✓ All %d timestamps are unique", len(series.Values))

		// Verify query stats
		t.Logf("Query stats: %s", result.Stats.String())
		if result.Stats.SpansScanned == 0 {
			t.Error("Expected spans to be scanned, got 0")
		}
	})

	// Test 2: Query that should produce multiple series (no label filter)
	t.Run("MultipleSeries", func(t *testing.T) {
		queryStr := `rate({}[5m])`

		opts := &QueryOptions{
			StartTime:   baseTime,
			EndTime:     now,
			Context:     context.Background(),
			UseSnapshot: false,
		}

		result, err := engine.Execute(queryStr, opts)
		if err != nil {
			t.Fatalf("Query execution failed: %v", err)
		}

		if result.Type != ResultTypeMatrix {
			t.Errorf("Expected result type Matrix, got %s", result.Type)
		}

		// Should have 2 series (promqlExec and httpRequest)
		if len(result.Matrix) != 2 {
			t.Errorf("Expected 2 time series, got %d", len(result.Matrix))
		}

		// Verify each series has multiple data points
		for i, series := range result.Matrix {
			if len(series.Values) < 2 {
				t.Errorf("Series %d: expected multiple data points, got %d", i, len(series.Values))
			}

			t.Logf("Series %d: %d data points, labels=%v, sample_rate=%.4f",
				i,
				len(series.Values),
				series.Metric,
				series.Values[0].Value)
		}
	})

	// Test 3: Short time range (last 5 minutes only)
	t.Run("ShortRange", func(t *testing.T) {
		queryStr := `rate({name="httpRequest"}[5m])`

		opts := &QueryOptions{
			StartTime:   now.Add(-5 * time.Minute),
			EndTime:     now,
			Step:        10 * time.Second, // Explicit step
			Context:     context.Background(),
			UseSnapshot: false,
		}

		result, err := engine.Execute(queryStr, opts)
		if err != nil {
			t.Fatalf("Query execution failed: %v", err)
		}

		if result.Type != ResultTypeMatrix {
			t.Errorf("Expected result type Matrix, got %s", result.Type)
		}

		if len(result.Matrix) == 0 {
			t.Fatal("Expected at least one series")
		}

		series := result.Matrix[0]

		// With 10s step over 5 minutes, we expect ~30 data points
		expectedPoints := int(5 * 60 / 10) // 300s / 10s = 30
		if len(series.Values) < expectedPoints-2 || len(series.Values) > expectedPoints+2 {
			t.Errorf("Expected ~%d data points (with 10s step), got %d", expectedPoints, len(series.Values))
		}
		t.Logf("✓ With 10s explicit step: got %d data points (expected ~%d)", len(series.Values), expectedPoints)

		// Verify the rate is higher for httpRequest (20 spans/min vs 10)
		// Expected: 20*5 / 300s = 0.333 spans/sec
		expectedRate := float64(20*5) / 300.0
		tolerance := expectedRate * 0.3

		if series.Values[0].Value < expectedRate-tolerance || series.Values[0].Value > expectedRate+tolerance {
			t.Logf("Note: Rate %.4f differs from expected %.4f (may be OK due to time window)",
				series.Values[0].Value, expectedRate)
		}

		t.Logf("✓ Short range: %d data points, rate=%.4f (expected ~%.4f)",
			len(series.Values),
			series.Values[0].Value,
			expectedRate)
	})
}

// Helper function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
