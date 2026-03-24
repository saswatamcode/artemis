package queryapi

import (
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/span"
)

// TestBucketBoundaryConditions tests that spans at exact bucket boundaries
// are handled consistently across all bucketing functions.
func TestBucketBoundaryConditions(t *testing.T) {
	// Test setup: 3 buckets of 1 minute each
	start := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	end := start.Add(3 * time.Minute)
	step := 1 * time.Minute

	// Create spans at exact bucket boundaries
	spans := []*span.Span{
		// At start (10:00:00) - should be in first bucket
		{SpanID: "1", StartTime: start, Name: "test", ServiceName: "svc"},

		// At first boundary (10:01:00) - should be in second bucket
		{SpanID: "2", StartTime: start.Add(1 * time.Minute), Name: "test", ServiceName: "svc"},

		// At second boundary (10:02:00) - should be in third bucket
		{SpanID: "3", StartTime: start.Add(2 * time.Minute), Name: "test", ServiceName: "svc"},

		// At end (10:03:00) - should be EXCLUDED (exclusive end)
		{SpanID: "4", StartTime: end, Name: "test", ServiceName: "svc"},

		// Just before first boundary (10:00:59.999999999) - should be in first bucket
		{SpanID: "5", StartTime: start.Add(1*time.Minute - 1), Name: "test", ServiceName: "svc"},

		// Just after start (10:00:00.000000001) - should be in first bucket
		{SpanID: "6", StartTime: start.Add(1), Name: "test", ServiceName: "svc"},
	}

	// Test convertSpansToTimeSeries
	t.Run("convertSpansToTimeSeries", func(t *testing.T) {
		matrix := convertSpansToTimeSeries(spans, start, end, step)

		if len(matrix) != 1 {
			t.Fatalf("expected 1 series, got %d", len(matrix))
		}

		series := matrix[0]

		// Should have 3 buckets (start, start+1m, start+2m)
		// Bucket 1 (10:00): spans 1, 5, 6 = 3 spans
		// Bucket 2 (10:01): span 2 = 1 span
		// Bucket 3 (10:02): span 3 = 1 span
		// Span 4 (at end) should be excluded

		if len(series.Values) != 3 {
			t.Fatalf("expected 3 buckets, got %d", len(series.Values))
		}

		// Check bucket 1
		if series.Values[0].Value != 3 {
			t.Errorf("bucket 1 (10:00): expected 3 spans, got %f", series.Values[0].Value)
		}

		// Check bucket 2
		if series.Values[1].Value != 1 {
			t.Errorf("bucket 2 (10:01): expected 1 span, got %f", series.Values[1].Value)
		}

		// Check bucket 3
		if series.Values[2].Value != 1 {
			t.Errorf("bucket 3 (10:02): expected 1 span, got %f", series.Values[2].Value)
		}
	})

	// Test fetchExemplarsWithBucketing
	t.Run("fetchExemplarsWithBucketing", func(t *testing.T) {
		srv := &Server{}
		exemplarMap, err := srv.fetchExemplarsWithBucketing(spans, start, end, step, 10, "slowest")
		if err != nil {
			t.Fatalf("fetchExemplarsWithBucketing failed: %v", err)
		}

		// Should have 3 buckets
		if len(exemplarMap) != 3 {
			t.Errorf("expected 3 buckets, got %d", len(exemplarMap))
		}

		// Check each bucket has the right number of exemplars
		bucket1 := start.Unix()
		bucket2 := start.Add(1 * time.Minute).Unix()
		bucket3 := start.Add(2 * time.Minute).Unix()

		if len(exemplarMap[bucket1]) != 3 {
			t.Errorf("bucket 1: expected 3 exemplars, got %d", len(exemplarMap[bucket1]))
		}

		if len(exemplarMap[bucket2]) != 1 {
			t.Errorf("bucket 2: expected 1 exemplar, got %d", len(exemplarMap[bucket2]))
		}

		if len(exemplarMap[bucket3]) != 1 {
			t.Errorf("bucket 3: expected 1 exemplar, got %d", len(exemplarMap[bucket3]))
		}
	})
}

// TestLookbackWindowBoundaries tests that lookback windows use [start, end) semantics
func TestLookbackWindowBoundaries(t *testing.T) {
	start := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	end := start.Add(3 * time.Minute)
	step := 1 * time.Minute
	lookback := 1 * time.Minute

	// Create spans at specific times
	spans := []*span.Span{
		// At 10:00:00 - should be in window for step 10:01
		{SpanID: "1", TraceID: "trace1", StartTime: start, Name: "test", ServiceName: "svc", Duration: 100},

		// At 10:01:00 - should be in window for step 10:02 (NOT step 10:01 - exclusive end)
		{SpanID: "2", TraceID: "trace2", StartTime: start.Add(1 * time.Minute), Name: "test", ServiceName: "svc", Duration: 200},

		// At 10:01:59.999999999 - should be in window for step 10:02
		{SpanID: "3", TraceID: "trace3", StartTime: start.Add(2*time.Minute - 1), Name: "test", ServiceName: "svc", Duration: 150},
	}

	srv := &Server{}
	exemplarMap, err := srv.fetchExemplarsWithLookback(spans, start, end, step, lookback, 10, "slowest")
	if err != nil {
		t.Fatalf("fetchExemplarsWithLookback failed: %v", err)
	}

	// Step 10:00 with lookback [09:59, 10:00) - should have 0 spans (span 1 is at 10:00, excluded)
	bucket1 := start.Unix()
	if len(exemplarMap[bucket1]) != 0 {
		t.Errorf("step 10:00: expected 0 exemplars, got %d (span at exact end should be excluded)", len(exemplarMap[bucket1]))
	}

	// Step 10:01 with lookback [10:00, 10:01) - should have 1 span (span 1)
	bucket2 := start.Add(1 * time.Minute).Unix()
	if len(exemplarMap[bucket2]) != 1 {
		t.Errorf("step 10:01: expected 1 exemplar, got %d", len(exemplarMap[bucket2]))
	} else if exemplarMap[bucket2][0].SpanID != "1" {
		t.Errorf("step 10:01: expected span 1, got span %s", exemplarMap[bucket2][0].SpanID)
	}

	// Step 10:02 with lookback [10:01, 10:02) - should have 2 spans (spans 2 and 3)
	bucket3 := start.Add(2 * time.Minute).Unix()
	if len(exemplarMap[bucket3]) != 2 {
		t.Errorf("step 10:02: expected 2 exemplars, got %d", len(exemplarMap[bucket3]))
	}
}

// TestNoDoubleCounting ensures spans are never counted in multiple buckets
func TestNoDoubleCounting(t *testing.T) {
	start := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	end := start.Add(10 * time.Minute)
	step := 1 * time.Minute

	// Create a span at each minute boundary
	var spans []*span.Span
	for i := 0; i < 10; i++ {
		spans = append(spans, &span.Span{
			SpanID:      string(rune('A' + i)),
			StartTime:   start.Add(time.Duration(i) * time.Minute),
			Name:        "test",
			ServiceName: "svc",
		})
	}

	matrix := convertSpansToTimeSeries(spans, start, end, step)

	if len(matrix) != 1 {
		t.Fatalf("expected 1 series, got %d", len(matrix))
	}

	// Count total spans across all buckets
	totalSpans := 0
	for _, value := range matrix[0].Values {
		totalSpans += int(value.Value)
	}

	// Should have exactly 10 spans total (no double counting)
	// Spans are created at 10:00, 10:01, ..., 10:09 (10 total)
	// Range is [10:00, 10:10), so all 10 should be included
	expectedSpans := 10
	if totalSpans != expectedSpans {
		t.Errorf("expected %d total spans across all buckets, got %d (double counting detected)", expectedSpans, totalSpans)
	}
}

// TestNanosecondPrecision ensures nanosecond-level timestamps are handled correctly
func TestNanosecondPrecision(t *testing.T) {
	start := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Second)
	step := 1 * time.Second

	spans := []*span.Span{
		// Exactly at boundary
		{SpanID: "1", StartTime: start, Name: "test", ServiceName: "svc"},

		// 1 nanosecond after boundary
		{SpanID: "2", StartTime: start.Add(1), Name: "test", ServiceName: "svc"},

		// 1 nanosecond before next boundary
		{SpanID: "3", StartTime: start.Add(1*time.Second - 1), Name: "test", ServiceName: "svc"},

		// Exactly at next boundary
		{SpanID: "4", StartTime: start.Add(1 * time.Second), Name: "test", ServiceName: "svc"},
	}

	matrix := convertSpansToTimeSeries(spans, start, end, step)

	if len(matrix) != 1 {
		t.Fatalf("expected 1 series, got %d", len(matrix))
	}

	series := matrix[0]

	// Should have 2 buckets
	if len(series.Values) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(series.Values))
	}

	// First bucket [10:00:00.000000000, 10:00:01.000000000): spans 1, 2, 3
	if series.Values[0].Value != 3 {
		t.Errorf("first bucket: expected 3 spans, got %f", series.Values[0].Value)
	}

	// Second bucket [10:00:01.000000000, 10:00:02.000000000): span 4
	if series.Values[1].Value != 1 {
		t.Errorf("second bucket: expected 1 span, got %f", series.Values[1].Value)
	}
}

// TestEndBoundaryExclusion explicitly tests that spans at 'end' are excluded
func TestEndBoundaryExclusion(t *testing.T) {
	start := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	end := start.Add(3 * time.Minute)
	step := 1 * time.Minute

	spans := []*span.Span{
		// These should be included
		{SpanID: "1", StartTime: start, Name: "test", ServiceName: "svc"},
		{SpanID: "2", StartTime: start.Add(1 * time.Minute), Name: "test", ServiceName: "svc"},
		{SpanID: "3", StartTime: start.Add(2 * time.Minute), Name: "test", ServiceName: "svc"},
		// This should be EXCLUDED (exactly at end)
		{SpanID: "4", StartTime: end, Name: "test", ServiceName: "svc"},
		// This should also be EXCLUDED (after end)
		{SpanID: "5", StartTime: end.Add(1 * time.Minute), Name: "test", ServiceName: "svc"},
	}

	matrix := convertSpansToTimeSeries(spans, start, end, step)

	if len(matrix) != 1 {
		t.Fatalf("expected 1 series, got %d", len(matrix))
	}

	// Count total spans
	totalSpans := 0
	for _, value := range matrix[0].Values {
		totalSpans += int(value.Value)
	}

	// Should have exactly 3 spans (spans 1, 2, 3)
	// Spans 4 and 5 should be excluded
	if totalSpans != 3 {
		t.Errorf("expected 3 spans (excluding those at/after end), got %d", totalSpans)
	}
}

// TestEdgeCaseEmptyBuckets tests handling of time ranges with no spans
func TestEdgeCaseEmptyBuckets(t *testing.T) {
	start := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	end := start.Add(5 * time.Minute)
	step := 1 * time.Minute

	// Create spans only in some buckets (sparse data)
	spans := []*span.Span{
		{SpanID: "1", StartTime: start, Name: "test", ServiceName: "svc"},
		{SpanID: "2", StartTime: start.Add(3 * time.Minute), Name: "test", ServiceName: "svc"},
	}

	matrix := convertSpansToTimeSeries(spans, start, end, step)

	if len(matrix) != 1 {
		t.Fatalf("expected 1 series, got %d", len(matrix))
	}

	// Should only have values for non-empty buckets
	// Bucket 0 (10:00): 1 span
	// Bucket 3 (10:03): 1 span
	if len(matrix[0].Values) != 2 {
		t.Errorf("expected 2 non-empty buckets, got %d", len(matrix[0].Values))
	}
}
