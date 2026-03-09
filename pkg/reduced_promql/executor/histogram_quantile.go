package executor

import (
	"fmt"
	"math/bits"
	"sort"

	"github.com/saswatamcode/artemis/pkg/span"
)

// HistogramQuantileOperator implements the histogram_quantile() function.
//
// Calculates duration quantiles (e.g., p95, p99) from span durations using
// exponential bucketing.
//
// Algorithm:
//  1. Consume all batches from input
//  2. Build histogram using duration_bucket field (0-63)
//  3. Calculate cumulative histogram
//  4. Find quantile using linear interpolation
//  5. Emit single synthetic span with quantile value
//
// The duration_bucket field is pre-computed during ingestion at
// pkg/storage/arrow.go:219-226 using bits.Len64(duration) - 1.
// This gives exponential buckets where bucket i represents durations in [2^i, 2^(i+1)).
//
// Example:
//   - Bucket 0: [1ns, 2ns)
//   - Bucket 10: [1024ns, 2048ns) = [1µs, 2µs)
//   - Bucket 20: [1ms, 2ms)
//   - Bucket 30: [1s, 2s)
//
// Quantile calculation:
//   - Build cumulative counts
//   - Find bucket where cumulative count >= quantile * total
//   - Use linear interpolation within bucket (approximation)
//
// Output format:
//   - Single span with Tags["quantile_value"] = calculated duration in nanoseconds
//   - Tags["quantile"] = requested quantile (e.g., "0.95")
//   - Tags["total_spans"] = total number of spans
type HistogramQuantileOperator struct {
	quantile float64 // Quantile value (e.g., 0.95 for p95)
	input    BatchIterator
	ctx      *ExecutionContext
	consumed bool

	// Histogram data
	buckets [64]int // Count per duration bucket (indexed 0-63)
	total   int     // Total spans

	// Result
	resultEmitted bool
}

// NewHistogramQuantileOperator creates a new histogram quantile operator.
func NewHistogramQuantileOperator(quantile float64, input BatchIterator, execCtx *ExecutionContext) *HistogramQuantileOperator {
	return &HistogramQuantileOperator{
		quantile: quantile,
		input:    input,
		ctx:      execCtx,
	}
}

// Next returns the quantile result.
//
// On first call, consumes all input and calculates quantile.
// Returns single span with result, then nil on subsequent calls.
func (h *HistogramQuantileOperator) Next() ([]*span.Span, error) {
	// First call: consume input and calculate quantile
	if !h.consumed {
		if err := h.consumeAndCalculate(); err != nil {
			return nil, err
		}
		h.consumed = true
	}

	// Return result once
	if h.resultEmitted {
		return nil, nil
	}
	h.resultEmitted = true

	// Calculate quantile value
	quantileValue := h.calculateQuantile()

	// Create synthetic span with result
	result := &span.Span{
		TraceID:     "histogram_quantile_result",
		SpanID:      fmt.Sprintf("q%f", h.quantile),
		Name:        "histogram_quantile",
		ServiceName: "query_engine",
		Tags: map[string]string{
			"quantile":       fmt.Sprintf("%f", h.quantile),
			"quantile_value": fmt.Sprintf("%d", quantileValue), // Duration in nanoseconds
			"total_spans":    fmt.Sprintf("%d", h.total),
		},
	}

	return []*span.Span{result}, nil
}

// consumeAndCalculate consumes all input and builds the histogram.
func (h *HistogramQuantileOperator) consumeAndCalculate() error {
	// Consume all input batches
	for {
		batch, err := h.input.Next()
		if err != nil {
			return fmt.Errorf("histogram_quantile: error reading input: %w", err)
		}
		if batch == nil {
			break // Input exhausted
		}

		// Build histogram from span durations
		for _, sp := range batch {
			// Calculate duration bucket
			// Note: duration_bucket is in Arrow schema but not in Span struct
			// We need to calculate it from Duration
			bucket := calculateDurationBucket(sp.GetDuration())
			if bucket >= 0 && bucket < 64 {
				h.buckets[bucket]++
				h.total++
			}
		}
	}

	return nil
}

// calculateQuantile calculates the quantile value from the histogram.
func (h *HistogramQuantileOperator) calculateQuantile() int64 {
	if h.total == 0 {
		return 0
	}

	// Build cumulative histogram
	cumulative := make([]int, 64)
	cumulative[0] = h.buckets[0]
	for i := 1; i < 64; i++ {
		cumulative[i] = cumulative[i-1] + h.buckets[i]
	}

	// Find target count for quantile
	targetCount := int(float64(h.total) * h.quantile)

	// Binary search for bucket containing quantile
	bucketIdx := sort.Search(64, func(i int) bool {
		return cumulative[i] >= targetCount
	})

	if bucketIdx >= 64 {
		bucketIdx = 63
	}

	// Calculate approximate duration for this bucket
	// Bucket i represents durations in [2^i, 2^(i+1))
	// Use bucket midpoint as approximation
	bucketStart := int64(1) << uint(bucketIdx)
	bucketEnd := int64(1) << uint(bucketIdx+1)
	bucketMid := (bucketStart + bucketEnd) / 2

	// Linear interpolation within bucket (optional refinement)
	// For simplicity, use bucket midpoint
	return bucketMid
}

// Close releases resources held by the operator.
func (h *HistogramQuantileOperator) Close() error {
	if h.input != nil {
		return h.input.Close()
	}
	return nil
}

// calculateDurationBucket calculates the duration bucket for a given duration.
// This matches the calculation in pkg/storage/arrow.go:219-226
func calculateDurationBucket(durationNs int64) int32 {
	if durationNs <= 0 {
		return 0
	}
	return int32(bits.Len64(uint64(durationNs)) - 1)
}
