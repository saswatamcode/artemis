package queryapi

import (
	"fmt"
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/span"
)

// TestExemplarLookbackWindows verifies that exemplars for rate() queries
// use sliding lookback windows that match the rate operator's evaluation logic.
func TestExemplarLookbackWindows(t *testing.T) {
	// Create test spans at specific times to verify lookback behavior
	baseTime := time.Unix(1000, 0)

	testSpans := []*span.Span{
		// Span at T+0s (will be in lookback for 5m, 6m, 7m, etc.)
		{
			TraceID:     "trace1",
			SpanID:      "span1",
			Name:        "test",
			ServiceName: "api",
			StartTime:   baseTime,
			EndTime:     baseTime.Add(100 * time.Millisecond),
			Duration:    100_000_000,
			Tags:        map[string]string{"env": "prod"},
		},
		// Span at T+2m (will be in lookback for 7m, 8m, etc., but NOT 5m or 6m)
		{
			TraceID:     "trace2",
			SpanID:      "span2",
			Name:        "test",
			ServiceName: "api",
			StartTime:   baseTime.Add(2 * time.Minute),
			EndTime:     baseTime.Add(2*time.Minute + 200*time.Millisecond),
			Duration:    200_000_000,
			Tags:        map[string]string{"env": "prod"},
		},
		// Span at T+6m (will be in lookback for 11m, 12m, etc., but NOT earlier steps)
		{
			TraceID:     "trace3",
			SpanID:      "span3",
			Name:        "test",
			ServiceName: "api",
			StartTime:   baseTime.Add(6 * time.Minute),
			EndTime:     baseTime.Add(6*time.Minute + 300*time.Millisecond),
			Duration:    300_000_000,
			Tags:        map[string]string{"env": "prod"},
		},
	}

	// Test: rate() query with 5m lookback
	// With [start, end) semantics (inclusive start, exclusive end):
	// Step at T+5m, window [T+0m, T+5m) → includes span1 (at T+0), span2 (at T+2m)
	// Step at T+6m, window [T+1m, T+6m) → includes span2 (at T+2m) only
	// Step at T+7m, window [T+2m, T+7m) → includes span2 (at T+2m), span3 (at T+6m)
	// Step at T+11m, window [T+6m, T+11m) → includes span3 (at T+6m) only

	t.Run("LookbackWindowMatchesRateOperator", func(t *testing.T) {
		start := baseTime.Add(5 * time.Minute)
		end := baseTime.Add(12 * time.Minute)
		step := 1 * time.Minute
		rangeDuration := 5 * time.Minute

		// Fetch exemplars using lookback strategy
		exemplarMap, err := (&Server{}).fetchExemplarsWithLookback(
			testSpans, start, end, step, rangeDuration, 10, "slowest",
		)
		if err != nil {
			t.Fatalf("fetchExemplarsWithLookback failed: %v", err)
		}

		// Verify step at T+5m gets span1 and span2 (from [T+0m, T+5m))
		// Both span1 (at T+0) and span2 (at T+2m) are within this window
		exemplarsAt5m := exemplarMap[start.Unix()]
		if len(exemplarsAt5m) != 2 {
			t.Errorf("Step at T+5m: expected 2 exemplars, got %d", len(exemplarsAt5m))
		} else {
			hasSpan1 := exemplarsAt5m[0].SpanID == "span1" || exemplarsAt5m[1].SpanID == "span1"
			hasSpan2 := exemplarsAt5m[0].SpanID == "span2" || exemplarsAt5m[1].SpanID == "span2"
			if !hasSpan1 || !hasSpan2 {
				t.Errorf("Step at T+5m: expected span1 and span2, got %v", exemplarsAt5m)
			}
		}

		// Verify step at T+6m gets span2 only (from [T+1m, T+6m))
		// span2 is at T+2m (included)
		// span3 is at T+6m (excluded - at boundary)
		exemplarsAt6m := exemplarMap[start.Add(1*time.Minute).Unix()]
		if len(exemplarsAt6m) != 1 {
			t.Errorf("Step at T+6m: expected 1 exemplar, got %d", len(exemplarsAt6m))
		} else if exemplarsAt6m[0].SpanID != "span2" {
			t.Errorf("Step at T+6m: expected span2, got %s", exemplarsAt6m[0].SpanID)
		}

		// Verify step at T+7m gets span2 and span3 (from [T+2m, T+7m])
		// span1 is at T+0, which is outside this window
		exemplarsAt7m := exemplarMap[start.Add(2*time.Minute).Unix()]
		if len(exemplarsAt7m) != 2 {
			t.Errorf("Step at T+7m: expected 2 exemplars, got %d", len(exemplarsAt7m))
		} else {
			// Should have span2 and span3, but NOT span1
			hasSpan2 := exemplarsAt7m[0].SpanID == "span2" || exemplarsAt7m[1].SpanID == "span2"
			hasSpan3 := exemplarsAt7m[0].SpanID == "span3" || exemplarsAt7m[1].SpanID == "span3"
			if !hasSpan2 || !hasSpan3 {
				t.Errorf("Step at T+7m: expected span2 and span3, got %v", exemplarsAt7m)
			}
			// Verify span1 is NOT included
			for _, ex := range exemplarsAt7m {
				if ex.SpanID == "span1" {
					t.Errorf("Step at T+7m: should NOT include span1 (outside lookback window)")
				}
			}
		}

		// Verify step at T+11m gets span3 only (from [T+6m, T+11m])
		exemplarsAt11m := exemplarMap[start.Add(6*time.Minute).Unix()]
		if len(exemplarsAt11m) != 1 {
			t.Errorf("Step at T+11m: expected 1 exemplar, got %d", len(exemplarsAt11m))
		} else if exemplarsAt11m[0].SpanID != "span3" {
			t.Errorf("Step at T+11m: expected span3, got %s", exemplarsAt11m[0].SpanID)
		}
	})

	t.Run("DirectBucketingForSelectors", func(t *testing.T) {
		// For non-range queries like {name="test"}, use direct bucketing
		// Span at T+0s → bucket 0
		// Span at T+2m → bucket 2
		// Span at T+6m → bucket 6

		start := baseTime
		end := baseTime.Add(10 * time.Minute)
		step := 1 * time.Minute

		// Fetch exemplars using bucketing strategy
		exemplarMap, err := (&Server{}).fetchExemplarsWithBucketing(
			testSpans, start, end, step, 10, "slowest",
		)
		if err != nil {
			t.Fatalf("fetchExemplarsWithBucketing failed: %v", err)
		}

		// Verify span1 is in bucket at T+0m
		exemplarsAt0m := exemplarMap[start.Unix()]
		if len(exemplarsAt0m) != 1 || exemplarsAt0m[0].SpanID != "span1" {
			t.Errorf("Bucket at T+0m: expected span1, got %v", exemplarsAt0m)
		}

		// Verify span2 is in bucket at T+2m
		exemplarsAt2m := exemplarMap[start.Add(2*time.Minute).Unix()]
		if len(exemplarsAt2m) != 1 || exemplarsAt2m[0].SpanID != "span2" {
			t.Errorf("Bucket at T+2m: expected span2, got %v", exemplarsAt2m)
		}

		// Verify span3 is in bucket at T+6m
		exemplarsAt6m := exemplarMap[start.Add(6*time.Minute).Unix()]
		if len(exemplarsAt6m) != 1 || exemplarsAt6m[0].SpanID != "span3" {
			t.Errorf("Bucket at T+6m: expected span3, got %v", exemplarsAt6m)
		}

		// Verify empty buckets have no exemplars
		exemplarsAt1m := exemplarMap[start.Add(1*time.Minute).Unix()]
		if len(exemplarsAt1m) != 0 {
			t.Errorf("Bucket at T+1m: expected 0 exemplars, got %d", len(exemplarsAt1m))
		}
	})
}

// TestExemplarStrategySelection verifies that exemplar selection strategies work correctly.
func TestExemplarStrategySelection(t *testing.T) {
	baseTime := time.Unix(2000, 0)

	// Create spans with different durations
	testSpans := []*span.Span{
		{SpanID: "fast", Duration: 10_000_000, StartTime: baseTime, EndTime: baseTime.Add(10 * time.Millisecond)},
		{SpanID: "medium", Duration: 100_000_000, StartTime: baseTime, EndTime: baseTime.Add(100 * time.Millisecond)},
		{SpanID: "slow", Duration: 1_000_000_000, StartTime: baseTime, EndTime: baseTime.Add(1 * time.Second)},
	}

	t.Run("SlowestStrategy", func(t *testing.T) {
		exemplars := selectExemplars(testSpans, 1, "slowest")
		if len(exemplars) != 1 {
			t.Fatalf("Expected 1 exemplar, got %d", len(exemplars))
		}
		if exemplars[0].SpanID != "slow" {
			t.Errorf("Expected slowest span, got %s", exemplars[0].SpanID)
		}
	})

	t.Run("FastestStrategy", func(t *testing.T) {
		exemplars := selectExemplars(testSpans, 1, "fastest")
		if len(exemplars) != 1 {
			t.Fatalf("Expected 1 exemplar, got %d", len(exemplars))
		}
		if exemplars[0].SpanID != "fast" {
			t.Errorf("Expected fastest span, got %s", exemplars[0].SpanID)
		}
	})

	t.Run("MultipleExemplars", func(t *testing.T) {
		exemplars := selectExemplars(testSpans, 2, "slowest")
		if len(exemplars) != 2 {
			t.Fatalf("Expected 2 exemplars, got %d", len(exemplars))
		}
		// Should get slow and medium (sorted by duration descending)
		if exemplars[0].SpanID != "slow" || exemplars[1].SpanID != "medium" {
			t.Errorf("Expected slow,medium, got %s,%s", exemplars[0].SpanID, exemplars[1].SpanID)
		}
	})
}

// TestExtractSelectorAndRange verifies query parsing for exemplar support.
func TestExtractSelectorAndRange(t *testing.T) {
	tests := []struct {
		query             string
		expectedSelector  string
		expectedRange     time.Duration
		expectedHasRange  bool
		expectError       bool
	}{
		{
			query:             `rate({name="foo"}[5m])`,
			expectedSelector:  `{name="foo"}`,
			expectedRange:     5 * time.Minute,
			expectedHasRange:  true,
		},
		{
			query:             `{name="bar"}`,
			expectedSelector:  `{name="bar"}`,
			expectedRange:     0,
			expectedHasRange:  false,
		},
		{
			query:             `sum by (service_name) (rate({env="prod"}[1h]))`,
			expectedSelector:  `{env="prod"}`,
			expectedRange:     1 * time.Hour,
			expectedHasRange:  true,
		},
		{
			query:             `heatmap({status="ok"})`,
			expectedSelector:  `{status="ok"}`,
			expectedRange:     0,
			expectedHasRange:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			selector, rangeDur, hasRange, err := extractSelectorAndRange(tt.query)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if selector != tt.expectedSelector {
				t.Errorf("Selector: expected %q, got %q", tt.expectedSelector, selector)
			}

			if rangeDur != tt.expectedRange {
				t.Errorf("Range: expected %v, got %v", tt.expectedRange, rangeDur)
			}

			if hasRange != tt.expectedHasRange {
				t.Errorf("HasRange: expected %v, got %v", tt.expectedHasRange, hasRange)
			}
		})
	}
}

// BenchmarkExemplarLookback benchmarks the lookback window strategy.
func BenchmarkExemplarLookback(b *testing.B) {
	baseTime := time.Unix(3000, 0)

	// Create 1000 test spans spread over 1 hour
	spans := make([]*span.Span, 1000)
	for i := 0; i < 1000; i++ {
		spans[i] = &span.Span{
			TraceID:     "bench",
			SpanID:      "span",
			StartTime:   baseTime.Add(time.Duration(i) * 3600 * time.Millisecond), // Spread over 1 hour
			EndTime:     baseTime.Add(time.Duration(i)*3600*time.Millisecond + 100*time.Millisecond),
			Duration:    100_000_000,
		}
	}

	start := baseTime.Add(5 * time.Minute)
	end := baseTime.Add(1 * time.Hour)
	step := 1 * time.Minute
	rangeDuration := 5 * time.Minute

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := (&Server{}).fetchExemplarsWithLookback(
			spans, start, end, step, rangeDuration, 5, "slowest",
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkExemplarBucketing benchmarks the direct bucketing strategy.
func BenchmarkExemplarBucketing(b *testing.B) {
	baseTime := time.Unix(3000, 0)

	// Create 1000 test spans
	spans := make([]*span.Span, 1000)
	for i := 0; i < 1000; i++ {
		spans[i] = &span.Span{
			TraceID:     "bench",
			SpanID:      "span",
			StartTime:   baseTime.Add(time.Duration(i) * 3600 * time.Millisecond),
			EndTime:     baseTime.Add(time.Duration(i)*3600*time.Millisecond + 100*time.Millisecond),
			Duration:    100_000_000,
		}
	}

	start := baseTime
	end := baseTime.Add(1 * time.Hour)
	step := 1 * time.Minute

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := (&Server{}).fetchExemplarsWithBucketing(
			spans, start, end, step, 5, "slowest",
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// TestHeatmapExemplarFiltering verifies that heatmap series only get exemplars
// with durations matching their duration bucket range.
func TestHeatmapExemplarFiltering(t *testing.T) {
	server := &Server{}

	// Create exemplars with different durations
	// Bucket 20: [2^20, 2^21) = [1048576, 2097152) ns ≈ [1ms, 2ms)
	// Bucket 26: [2^26, 2^27) = [67108864, 134217728) ns ≈ [67ms, 134ms)
	// Bucket 30: [2^30, 2^31) = [1073741824, 2147483648) ns ≈ [1s, 2s)

	timestamp := int64(1000)
	exemplarMap := map[int64][]Exemplar{
		timestamp: {
			{TraceID: "trace1", SpanID: "span1", Duration: 1_500_000, Timestamp: timestamp},    // 1.5ms → bucket 20
			{TraceID: "trace2", SpanID: "span2", Duration: 100_000_000, Timestamp: timestamp},  // 100ms → bucket 26
			{TraceID: "trace3", SpanID: "span3", Duration: 1_500_000_000, Timestamp: timestamp}, // 1.5s → bucket 30
			{TraceID: "trace4", SpanID: "span4", Duration: 1_800_000, Timestamp: timestamp},    // 1.8ms → bucket 20
		},
	}

	// Simulate a heatmap response with 3 duration buckets
	data := map[string]interface{}{
		"result": []map[string]interface{}{
			{
				"metric": map[string]interface{}{
					"duration_bucket": "20",
					"duration_range":  "1ms-2ms",
				},
				"values": [][]interface{}{
					{timestamp, "2.0"}, // 2 spans in this bucket
				},
			},
			{
				"metric": map[string]interface{}{
					"duration_bucket": "26",
					"duration_range":  "67ms-134ms",
				},
				"values": [][]interface{}{
					{timestamp, "1.0"}, // 1 span in this bucket
				},
			},
			{
				"metric": map[string]interface{}{
					"duration_bucket": "30",
					"duration_range":  "1s-2s",
				},
				"values": [][]interface{}{
					{timestamp, "1.0"}, // 1 span in this bucket
				},
			},
		},
	}

	// Attach exemplars
	server.attachExemplarsToResponse(data, exemplarMap)

	result := data["result"].([]map[string]interface{})

	// Verify bucket 20 (1ms-2ms) only gets span1 and span4
	bucket20 := result[0]
	exemplars20, ok := bucket20["exemplars"].([]map[string]interface{})
	if !ok {
		t.Fatalf("Bucket 20: expected exemplars, got none")
	}
	if len(exemplars20) != 2 {
		t.Errorf("Bucket 20: expected 2 exemplars, got %d", len(exemplars20))
	}
	for _, ex := range exemplars20 {
		spanID := ex["spanID"].(string)
		if spanID != "span1" && spanID != "span4" {
			t.Errorf("Bucket 20: unexpected exemplar %s (should only have span1 and span4)", spanID)
		}
	}

	// Verify bucket 26 (67ms-134ms) only gets span2
	bucket26 := result[1]
	exemplars26, ok := bucket26["exemplars"].([]map[string]interface{})
	if !ok {
		t.Fatalf("Bucket 26: expected exemplars, got none")
	}
	if len(exemplars26) != 1 {
		t.Errorf("Bucket 26: expected 1 exemplar, got %d", len(exemplars26))
	}
	if exemplars26[0]["spanID"] != "span2" {
		t.Errorf("Bucket 26: expected span2, got %s", exemplars26[0]["spanID"])
	}

	// Verify bucket 30 (1s-2s) only gets span3
	bucket30 := result[2]
	exemplars30, ok := bucket30["exemplars"].([]map[string]interface{})
	if !ok {
		t.Fatalf("Bucket 30: expected exemplars, got none")
	}
	if len(exemplars30) != 1 {
		t.Errorf("Bucket 30: expected 1 exemplar, got %d", len(exemplars30))
	}
	if exemplars30[0]["spanID"] != "span3" {
		t.Errorf("Bucket 30: expected span3, got %s", exemplars30[0]["spanID"])
	}
}

// TestGetDurationRangeForBucket verifies bucket range calculations.
func TestGetDurationRangeForBucket(t *testing.T) {
	tests := []struct {
		bucket      int32
		expectedMin int64
		expectedMax int64
	}{
		{0, 1, 2},                          // [1ns, 2ns)
		{10, 1024, 2048},                   // [1024ns, 2048ns)
		{20, 1048576, 2097152},             // [~1ms, ~2ms)
		{26, 67108864, 134217728},          // [~67ms, ~134ms)
		{30, 1073741824, 2147483648},       // [~1s, ~2s)
		{62, 4611686018427387904, 9223372036854775807}, // max bucket
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("bucket_%d", tt.bucket), func(t *testing.T) {
			min, max := getDurationRangeForBucket(tt.bucket)
			if min != tt.expectedMin {
				t.Errorf("Min: expected %d, got %d", tt.expectedMin, min)
			}
			if max != tt.expectedMax {
				t.Errorf("Max: expected %d, got %d", tt.expectedMax, max)
			}
		})
	}
}

// mockQueryEngine for testing extractSelectorAndRange without full engine
type mockQueryEngine struct{}

func (m *mockQueryEngine) Execute(query string, opts interface{}) (interface{}, error) {
	return nil, nil
}

func (m *mockQueryEngine) ExecuteAsync(query string, opts interface{}) (<-chan interface{}, <-chan error) {
	return nil, nil
}
