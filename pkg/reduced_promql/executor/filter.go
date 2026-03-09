package executor

import (
	"github.com/saswatamcode/artemis/pkg/query"
	"github.com/saswatamcode/artemis/pkg/span"
)

// FilterOperator applies matchers to filter spans from its input.
//
// This is a pass-through operator that receives batches from a child iterator,
// filters each batch using query.Matchers, and returns the filtered results.
//
// Use cases:
//   - Apply filters that couldn't be pushed down to the scan operator
//   - Add additional filtering on top of partial scans
//   - Post-processing filter after aggregations or functions
//
// Memory management:
//   - Gets output buffer from pool for each batch
//   - Releases input buffer back to pool after filtering
//   - This prevents memory buildup when filtering is selective
type FilterOperator struct {
	input    BatchIterator
	matchers query.Matchers
	ctx      *ExecutionContext
}

// NewFilterOperator creates a new filter operator.
func NewFilterOperator(input BatchIterator, matchers query.Matchers, execCtx *ExecutionContext) *FilterOperator {
	return &FilterOperator{
		input:    input,
		matchers: matchers,
		ctx:      execCtx,
	}
}

// Next returns the next filtered batch of spans.
func (f *FilterOperator) Next() ([]*span.Span, error) {
	// Get next batch from input
	inputBatch, err := f.input.Next()
	if err != nil {
		return nil, err
	}
	if inputBatch == nil {
		// Input exhausted
		return nil, nil
	}

	// Special case: no matchers, pass through
	if len(f.matchers) == 0 {
		return inputBatch, nil
	}

	// Get output buffer from pool
	output := f.ctx.GetSpanBatch()

	// Filter spans using matchers
	for _, sp := range inputBatch {
		if f.matchers.Matches(sp) {
			*output = append(*output, sp)
		}
	}

	// If no spans matched, release output buffer and try next batch
	// This prevents returning empty batches in the middle of execution
	if len(*output) == 0 {
		f.ctx.ReleaseSpanBatch(output)
		// Recursively get next batch (tail call optimization would help here)
		return f.Next()
	}

	// Note: We keep inputBatch allocated since it's part of the original scan results
	// The scan operator owns those buffers and will release them when closed
	// We only manage the output buffer we allocated

	return *output, nil
}

// Close releases resources held by the filter operator.
func (f *FilterOperator) Close() error {
	// Close the input iterator
	if f.input != nil {
		return f.input.Close()
	}
	return nil
}
