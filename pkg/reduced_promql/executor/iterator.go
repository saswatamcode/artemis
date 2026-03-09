package executor

import "github.com/saswatamcode/artemis/pkg/span"

// BatchIterator is the core interface for volcano-style query execution.
// Each operator implements this interface, processing batches of spans.
//
// Operators form a tree where each node pulls data from its children:
//   - Leaf operators (ScanOperator) read from blocks
//   - Intermediate operators (FilterOperator, RateOperator) transform batches
//   - Root operator returns final results
//
// Batch type: []*span.Span (slice of span pointers)
// Typical batch size: 1000 spans (matching IsolationCoordinator buffer pool)
type BatchIterator interface {
	// Next returns the next batch of spans.
	// Returns (nil, nil) when the iterator is exhausted (no more data).
	// Returns (nil, error) on error.
	// Returns ([]*span.Span, nil) on success.
	//
	// The returned slice should be from the buffer pool and will be released
	// by the caller when done processing.
	Next() ([]*span.Span, error)

	// Close releases resources held by this iterator.
	// This includes:
	//   - Returning buffers to the pool
	//   - Closing child iterators
	//   - Canceling goroutines
	//
	// Close must be idempotent (safe to call multiple times).
	// Close is typically called via defer after creating the iterator.
	Close() error
}
