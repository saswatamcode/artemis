package executor

import (
	"fmt"
	"sort"

	"github.com/saswatamcode/artemis/pkg/span"
)

// HeatmapOperator implements the heatmap() function.
//
// Generates a 2D histogram (heatmap) of time x duration, useful for visualizing
// latency distribution over time.
//
// Algorithm:
//  1. Consume all batches from input (must be VectorSelector only, enforced by grammar)
//  2. Build 2D histogram: time buckets (using bucket1s) x duration buckets (using duration_bucket)
//  3. Emit results as synthetic spans (one per non-empty cell)
//
// Axes:
//   - X-axis (time): 1-second buckets via bucket1s field
//   - Y-axis (duration): Exponential buckets via duration_bucket field (0-63)
//
// Output format:
//   - One span per non-empty heatmap cell
//   - Tags["time_bucket"] = bucket1s timestamp
//   - Tags["duration_bucket"] = duration bucket index (0-63)
//   - Tags["count"] = number of spans in this cell
//   - Tags["duration_range"] = human-readable duration range
//
// Example output:
//
//	[
//	  {time_bucket: 1000000000, duration_bucket: 20, count: 10, duration_range: "1ms-2ms"},
//	  {time_bucket: 1000000000, duration_bucket: 25, count: 5, duration_range: "32ms-64ms"},
//	  ...
//	]
//
// Visualization:
//
//	The output can be rendered as a heatmap with:
//	- X-axis: time_bucket (temporal progression)
//	- Y-axis: duration_bucket (latency distribution)
//	- Color intensity: count (span frequency)
type HeatmapOperator struct {
	input    BatchIterator
	ctx      *ExecutionContext
	consumed bool

	// 2D histogram: time bucket → duration bucket → count
	heatmap map[int64]map[int32]int

	// Result state
	resultBatch []*span.Span
	resultIdx   int
}

// NewHeatmapOperator creates a new heatmap operator.
func NewHeatmapOperator(input BatchIterator, execCtx *ExecutionContext) *HeatmapOperator {
	return &HeatmapOperator{
		input:   input,
		ctx:     execCtx,
		heatmap: make(map[int64]map[int32]int),
	}
}

// Next returns the next batch of heatmap results.
//
// On first call, consumes all input and builds heatmap.
// Returns results as synthetic spans representing heatmap cells.
func (h *HeatmapOperator) Next() ([]*span.Span, error) {
	// First call: consume input and build heatmap
	if !h.consumed {
		if err := h.consumeAndBuild(); err != nil {
			return nil, err
		}
		h.consumed = true
	}

	// Return results (single batch)
	if h.resultIdx >= len(h.resultBatch) {
		return nil, nil // Exhausted
	}

	// Return all results at once
	results := h.resultBatch[h.resultIdx:]
	h.resultIdx = len(h.resultBatch)
	return results, nil
}

// consumeAndBuild consumes all input and builds the heatmap.
func (h *HeatmapOperator) consumeAndBuild() error {
	// Consume all input batches
	for {
		batch, err := h.input.Next()
		if err != nil {
			return fmt.Errorf("heatmap: error reading input: %w", err)
		}
		if batch == nil {
			break // Input exhausted
		}

		// Build 2D histogram
		for _, sp := range batch {
			// Calculate time bucket (bucket1s)
			timeBucket := calculateBucket1s(sp.StartTime.UnixNano())

			// Calculate duration bucket
			durationBucket := calculateDurationBucket(sp.GetDuration())

			// Increment count for this cell
			if h.heatmap[timeBucket] == nil {
				h.heatmap[timeBucket] = make(map[int32]int)
			}
			h.heatmap[timeBucket][durationBucket]++
		}
	}

	// Convert heatmap to result spans
	h.resultBatch = make([]*span.Span, 0, len(h.heatmap)*10) // Estimate ~10 duration buckets per time bucket

	// Sort time buckets for consistent output
	timeBuckets := make([]int64, 0, len(h.heatmap))
	for tb := range h.heatmap {
		timeBuckets = append(timeBuckets, tb)
	}
	sort.Slice(timeBuckets, func(i, j int) bool {
		return timeBuckets[i] < timeBuckets[j]
	})

	// Generate result spans
	for _, timeBucket := range timeBuckets {
		durationBuckets := h.heatmap[timeBucket]

		// Sort duration buckets for consistent output
		durBuckets := make([]int32, 0, len(durationBuckets))
		for db := range durationBuckets {
			durBuckets = append(durBuckets, db)
		}
		sort.Slice(durBuckets, func(i, j int) bool {
			return durBuckets[i] < durBuckets[j]
		})

		for _, durationBucket := range durBuckets {
			count := durationBuckets[durationBucket]

			// Create synthetic span for this heatmap cell
			syntheticSpan := &span.Span{
				TraceID:     "heatmap_result",
				SpanID:      fmt.Sprintf("t%d_d%d", timeBucket, durationBucket),
				Name:        "heatmap_cell",
				ServiceName: "query_engine",
				Tags: map[string]string{
					"time_bucket":     fmt.Sprintf("%d", timeBucket),
					"duration_bucket": fmt.Sprintf("%d", durationBucket),
					"count":           fmt.Sprintf("%d", count),
					"duration_range":  formatDurationRange(durationBucket),
				},
			}

			h.resultBatch = append(h.resultBatch, syntheticSpan)
		}
	}

	return nil
}

// Close releases resources held by the operator.
func (h *HeatmapOperator) Close() error {
	if h.input != nil {
		return h.input.Close()
	}
	return nil
}

// formatDurationRange returns a human-readable duration range for a bucket.
func formatDurationRange(bucket int32) string {
	if bucket < 0 || bucket >= 64 {
		return "invalid"
	}

	bucketStart := int64(1) << uint(bucket)
	bucketEnd := int64(1) << uint(bucket+1)

	return fmt.Sprintf("%s-%s",
		formatDuration(bucketStart),
		formatDuration(bucketEnd))
}

// formatDuration formats a duration in nanoseconds to a human-readable string.
func formatDuration(ns int64) string {
	switch {
	case ns < 1000:
		return fmt.Sprintf("%dns", ns)
	case ns < 1000000:
		return fmt.Sprintf("%dµs", ns/1000)
	case ns < 1000000000:
		return fmt.Sprintf("%dms", ns/1000000)
	default:
		return fmt.Sprintf("%ds", ns/1000000000)
	}
}
