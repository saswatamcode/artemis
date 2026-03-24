package executor

import (
	"fmt"
	"strings"
	"time"

	"github.com/saswatamcode/artemis/pkg/span"
)

// RateOperator implements the rate() function - calculates rate of change over time.
//
// Algorithm (Prometheus-compatible):
//  1. Consume all input spans and index them by timestamp and labels
//  2. Iterate over the query time range in steps
//  3. For each step timestamp, look back by the range duration (e.g., 5m)
//  4. Count spans in that lookback window, grouped by labels
//  5. Calculate rate = count / range_duration_seconds
//  6. Emit one data point per step per label combination
//
// Example:
//   Query: rate({name="promqlExec"}[5m]) from 10:00 to 10:10, step 1m
//   - At 10:00: look back to 09:55, count spans in [09:55, 10:00], rate = count/300
//   - At 10:01: look back to 09:56, count spans in [09:56, 10:01], rate = count/300
//   - At 10:02: look back to 09:57, count spans in [09:57, 10:02], rate = count/300
//   - ... and so on
//
// This produces a time series with one data point per step, showing how the rate
// changes over time.
//
// Output format:
//   - One synthetic span per (step, label combination)
//   - Span.Tags["rate"] = calculated rate value
//   - Span.Tags["bucket_time"] = step timestamp
//   - Original span labels are preserved
type RateOperator struct {
	input    BatchIterator
	ctx      *ExecutionContext
	lookback time.Duration // Range duration (e.g., 5m)
	consumed bool

	// All input spans indexed for lookback queries
	allSpans []*span.Span

	// Result state
	resultBatch []*span.Span
	resultIdx   int
}

// NewRateOperator creates a new rate operator.
func NewRateOperator(input BatchIterator, lookback time.Duration, execCtx *ExecutionContext) *RateOperator {
	return &RateOperator{
		input:    input,
		ctx:      execCtx,
		lookback: lookback,
	}
}

// Next returns the next batch of rate results.
//
// On first call, consumes all input and calculates rates.
// Subsequent calls return nil (single batch output).
func (r *RateOperator) Next() ([]*span.Span, error) {
	// First call: consume all input and calculate rates
	if !r.consumed {
		if err := r.consumeAndCalculate(); err != nil {
			return nil, err
		}
		r.consumed = true
	}

	// Return results (single batch)
	if r.resultIdx >= len(r.resultBatch) {
		return nil, nil // Exhausted
	}

	// Return all results at once
	results := r.resultBatch[r.resultIdx:]
	r.resultIdx = len(r.resultBatch)
	return results, nil
}

// consumeAndCalculate consumes all input batches and calculates rates with lookback windows.
func (r *RateOperator) consumeAndCalculate() error {
	// 1. Consume all input spans
	for {
		batch, err := r.input.Next()
		if err != nil {
			return fmt.Errorf("rate: error reading input: %w", err)
		}
		if batch == nil {
			break // Input exhausted
		}
		r.allSpans = append(r.allSpans, batch...)
	}

	// 2. Determine step interval
	// Use step from query options if provided, otherwise calculate a sensible default
	step := r.ctx.Step
	if step == 0 {
		// Fallback: calculate step (default to 15 seconds, or 1/20th of range, whichever is larger)
		queryDuration := r.ctx.EndTime.Sub(r.ctx.StartTime)
		step = 15 * time.Second
		if queryDuration/20 > step {
			step = queryDuration / 20
		}
		if step > r.lookback/4 {
			step = r.lookback / 4 // Don't let step be too large relative to lookback
		}
	}

	// 3. Iterate over time range in steps
	r.resultBatch = make([]*span.Span, 0)

	for stepTime := r.ctx.StartTime; stepTime.Before(r.ctx.EndTime) || stepTime.Equal(r.ctx.EndTime); stepTime = stepTime.Add(step) {
		// Calculate lookback window: [stepTime - lookback, stepTime)
		// Boundary semantics: inclusive start, exclusive end
		// This prevents double-counting spans at exact step boundaries
		windowStart := stepTime.Add(-r.lookback)
		windowEnd := stepTime

		// 4. Group spans in this window by labels and count them
		seriesCounts := make(map[string]int)
		seriesLabels := make(map[string]map[string]string)

		for _, sp := range r.allSpans {
			// Check if span falls within the lookback window [windowStart, windowEnd)
			// Inclusive start, exclusive end - matches Prometheus convention
			if sp.StartTime.Before(windowStart) || !sp.StartTime.Before(windowEnd) {
				continue
			}

			// Extract metric labels from span tags (filter out internal/span-specific labels)
			labels := make(map[string]string)
			for k, v := range sp.Tags {
				if isMetricLabel(k) {
					labels[k] = v
				}
			}

			// Add top-level span fields for proper series grouping
			// These are critical for distinguishing different spans
			labels["__name"] = sp.Name
			labels["service_name"] = sp.ServiceName

			// Create a stable key for this label set
			labelKey := makeLabelsKey(labels)

			// Count this span
			seriesCounts[labelKey]++

			// Store labels for this series (if not already stored)
			if _, exists := seriesLabels[labelKey]; !exists {
				seriesLabels[labelKey] = labels
			}
		}

		// 5. Calculate rate for each series at this step
		for labelKey, count := range seriesCounts {
			// Rate = count / lookback_duration_in_seconds
			rate := float64(count) / r.lookback.Seconds()

			// Create synthetic span to hold rate value
			tags := make(map[string]string)
			for k, v := range seriesLabels[labelKey] {
				tags[k] = v
			}
			tags["rate"] = fmt.Sprintf("%f", rate)
			tags["bucket_time"] = fmt.Sprintf("%d", stepTime.UnixNano())

			syntheticSpan := &span.Span{
				TraceID:     "rate_result",
				SpanID:      fmt.Sprintf("%s_step_%d", labelKey, stepTime.Unix()),
				Name:        "rate",
				ServiceName: "query_engine",
				Tags:        tags,
			}

			r.resultBatch = append(r.resultBatch, syntheticSpan)
		}
	}

	return nil
}

// isMetricLabel determines if a span tag should be included as a metric label.
// This filters out only span/trace identifiers that change per span and would
// cause over-fragmentation of time series.
//
// Excluded labels:
//   - parent_span_id: changes per span
//   - trace_id: changes per trace
//   - span_id: changes per span
//
// All other labels are included for grouping, including OpenTelemetry metadata
// like scope.*, process.*, telemetry.sdk.*, etc.
func isMetricLabel(key string) bool {
	// Only exclude span-specific identifiers
	if key == "parent_span_id" || key == "trace_id" || key == "span_id" {
		return false
	}

	// Include everything else
	return true
}

// makeLabelsKey creates a stable key from a label set for grouping
func makeLabelsKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}

	// Sort keys for consistent ordering
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}

	// Simple bubble sort (small maps, ~10 labels max)
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	// Build key using strings.Builder for efficiency
	var result strings.Builder
	for _, k := range keys {
		result.WriteString(k)
		result.WriteString("=")
		result.WriteString(labels[k])
		result.WriteString(",")
	}
	return result.String()
}

// Close releases resources held by the rate operator.
func (r *RateOperator) Close() error {
	if r.input != nil {
		return r.input.Close()
	}
	return nil
}

// calculateBucket1s calculates the 1-second bucket for a given timestamp.
// This matches the calculation in pkg/storage/arrow.go:214-217
func calculateBucket1s(unixNano int64) int64 {
	const sec = int64(1_000_000_000)
	return unixNano - (unixNano % sec)
}
