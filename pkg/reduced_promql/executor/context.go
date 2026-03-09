package executor

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// ExecutionContext holds resources and metadata for query execution.
// It is shared across all operators in a query execution tree.
//
// Key responsibilities:
//   - Provides access to buffer pools via IsolationCoordinator
//   - Tracks query time range for filtering
//   - Provides access to blocks for scanning
//   - Supports query cancellation via context.Context
//   - Collects execution statistics
type ExecutionContext struct {
	// Query time range
	// All spans must satisfy: StartTime >= QueryStartTime && EndTime <= QueryEndTime
	StartTime time.Time
	EndTime   time.Time

	// Step interval for range queries (e.g., rate())
	// Used by rate operator to determine evaluation points
	Step time.Duration

	// Resource management
	// IsolationCoordinator provides buffer pooling and MVCC snapshot isolation
	Isolation *storage.IsolationCoordinator

	// Blocks to query
	// For ScanOperator to read span data from storage
	Blocks []block.Block

	// Cancellation support
	// Query execution checks this context for cancellation
	// Set via context.WithCancel or context.WithTimeout
	Ctx context.Context

	// MVCC snapshot sequence
	// If non-zero, operators should filter spans using isolation.IsVisible(spanID, snapshot)
	// This provides snapshot isolation for concurrent queries and writes
	Snapshot uint64

	// Execution statistics (thread-safe via atomic operations)
	spansScanned  atomic.Int64 // Total spans scanned from blocks
	blocksScanned atomic.Int64 // Total blocks scanned
}

// GetSpanBatch returns a span buffer from the pool.
// The returned buffer has capacity for ~1000 spans and length 0.
//
// Usage:
//
//	buf := ctx.GetSpanBatch()
//	*buf = append(*buf, span1, span2, ...)
//	// ... process batch ...
//	ctx.ReleaseSpanBatch(buf)
func (ctx *ExecutionContext) GetSpanBatch() *[]*span.Span {
	return ctx.Isolation.GetSpanBuffer()
}

// ReleaseSpanBatch returns a span buffer to the pool.
// The buffer is automatically cleared (set to length 0) before being returned.
//
// It is safe to call this with nil (no-op).
func (ctx *ExecutionContext) ReleaseSpanBatch(buf *[]*span.Span) {
	if buf == nil {
		return
	}
	ctx.Isolation.ReleaseSpanBuffer(buf)
}

// RecordSpansScanned increments the spans scanned counter.
// This is called by leaf operators (ScanOperator) to track query cost.
func (ctx *ExecutionContext) RecordSpansScanned(count int64) {
	ctx.spansScanned.Add(count)
}

// RecordBlockScanned increments the blocks scanned counter.
// This is called by ScanOperator each time it queries a block.
func (ctx *ExecutionContext) RecordBlockScanned() {
	ctx.blocksScanned.Add(1)
}

// SpansScanned returns the total number of spans scanned so far.
func (ctx *ExecutionContext) SpansScanned() int64 {
	return ctx.spansScanned.Load()
}

// BlocksScanned returns the total number of blocks scanned so far.
func (ctx *ExecutionContext) BlocksScanned() int64 {
	return ctx.blocksScanned.Load()
}

// IsCancelled returns true if the query has been cancelled.
// Operators should check this periodically to support early termination.
func (ctx *ExecutionContext) IsCancelled() bool {
	select {
	case <-ctx.Ctx.Done():
		return true
	default:
		return false
	}
}

// Err returns the cancellation error if cancelled, nil otherwise.
func (ctx *ExecutionContext) Err() error {
	return ctx.Ctx.Err()
}
