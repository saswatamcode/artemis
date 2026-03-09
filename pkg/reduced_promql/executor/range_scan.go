package executor

import (
	"time"

	"github.com/saswatamcode/artemis/pkg/span"
)

// RangeScanOperator implements MatrixSelector - adds time window filtering to VectorSelector.
//
// A matrix selector like {service_name="api"}[5m] returns all spans within a lookback window
// from the query time. This operator wraps a ScanOperator and applies additional time filtering.
//
// Time filtering:
//   - Base query time range: [ctx.StartTime, ctx.EndTime] (from ExecutionContext)
//   - Lookback window: [queryTime - lookback, queryTime]
//   - Effective range: intersection of the two
//
// Example:
//
//	Query: rate({service_name="api"}[5m]) at 10:00
//	- Lookback: [9:55, 10:00]
//	- Scans spans with startTime in [9:55, 10:00]
//
// Implementation:
//   - Wraps a ScanOperator (delegates block scanning)
//   - Filters each batch by time range
//   - Reuses buffer pool for filtered results
type RangeScanOperator struct {
	input    BatchIterator // Underlying vector scan
	lookback time.Duration // Lookback window (e.g., 5m)
	ctx      *ExecutionContext

	// Computed time range for filtering
	rangeStart time.Time
	rangeEnd   time.Time
}

// NewRangeScanOperator creates a new range scan operator.
//
// For range queries (like rate()), we need to scan data across the entire query
// duration PLUS the lookback before the start time. This ensures we have enough
// data to calculate rates at each step.
//
// Example: Query from 10:00-11:00 with 5m lookback
// - Scans: 09:55 (10:00 - 5m) to 11:00
// - This allows rate calculation at 10:00 (looks back to 09:55)
//   and at every step until 11:00
func NewRangeScanOperator(input BatchIterator, lookback time.Duration, execCtx *ExecutionContext) *RangeScanOperator {
	// Calculate effective time range
	// For range queries, we need data from (StartTime - lookback) to EndTime
	rangeEnd := execCtx.EndTime
	rangeStart := execCtx.StartTime.Add(-lookback)

	// Don't go before epoch
	if rangeStart.Before(time.Unix(0, 0)) {
		rangeStart = time.Unix(0, 0)
	}

	return &RangeScanOperator{
		input:      input,
		lookback:   lookback,
		ctx:        execCtx,
		rangeStart: rangeStart,
		rangeEnd:   rangeEnd,
	}
}

// Next returns the next filtered batch of spans within the time range.
func (r *RangeScanOperator) Next() ([]*span.Span, error) {
	// Get next batch from input
	inputBatch, err := r.input.Next()
	if err != nil {
		return nil, err
	}
	if inputBatch == nil {
		// Input exhausted
		return nil, nil
	}

	// Filter by time range
	// Get output buffer from pool
	output := r.ctx.GetSpanBatch()

	for _, sp := range inputBatch {
		// Check if span falls within the lookback window
		// A span is included if it starts within the range
		if !sp.StartTime.Before(r.rangeStart) && !sp.StartTime.After(r.rangeEnd) {
			*output = append(*output, sp)
		}
	}

	// If no spans matched, release buffer and try next batch
	if len(*output) == 0 {
		r.ctx.ReleaseSpanBatch(output)
		return r.Next()
	}

	return *output, nil
}

// Close releases resources held by the range scan operator.
func (r *RangeScanOperator) Close() error {
	if r.input != nil {
		return r.input.Close()
	}
	return nil
}
