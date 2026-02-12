package query

import (
	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/span"
)

// Querier provides an interface for querying spans from storage
// This allows different implementations and makes testing easier
type Querier interface {
	// Select queries spans using matchers
	// Returns all spans that match the given matchers
	// Events field is not populated (nil/empty)
	Select(matchers ...*Matcher) (*SelectResult, error)

	// SelectWithTimeRange queries spans using matchers within a time range
	// Time range filtering provides significant performance benefits by skipping irrelevant blocks
	// Events field is not populated (nil/empty)
	SelectWithTimeRange(timeRange *TimeRange, matchers ...*Matcher) (*SelectResult, error)

	// SelectWithOptions queries spans with custom options
	// Use QueryOptions.IncludeEvents to populate the Events field
	SelectWithOptions(opts *QueryOptions, matchers ...*Matcher) (*SelectResult, error)

	// SelectWithTimeRangeAndOptions queries spans with time range and custom options
	SelectWithTimeRangeAndOptions(timeRange *TimeRange, opts *QueryOptions, matchers ...*Matcher) (*SelectResult, error)
}

// EventQuerier provides methods for querying span events separately
type EventQuerier interface {
	// GetEventsForSpan retrieves all events for a specific span
	GetEventsForSpan(spanID string) ([]span.SpanEvent, error)

	// GetEventsForTrace retrieves all events for all spans in a trace
	// Returns a map of spanID -> events
	GetEventsForTrace(traceID string) (map[string][]span.SpanEvent, error)
}

// BlockQuerier queries spans across all blocks uniformly through the Block interface
// Treats HeadBlock, ArrowBlock (L0), and ParquetBlock (L1+) uniformly
// Uses FanoutQuerier internally to query head and persisted blocks in parallel
type BlockQuerier struct {
	fanoutQuerier *FanoutQuerier
}

// NewBlockQuerier creates a new querier that queries across head and persisted blocks
// head can be nil if there's no in-memory data
// blocks can be empty if there are no persisted blocks yet
//
// To create from a block manager:
//
//	querier := NewBlockQuerier(manager.GetHeadAsBlock(), manager.GetBlocks())
//
// The querier uses a FanoutQuerier internally to query persisted blocks and head block in parallel
func NewBlockQuerier(head block.Block, blocks []block.Block) *BlockQuerier {
	// Build list of queriers: persisted blocks first, then head
	// Order matters for deduplication: persisted blocks are authoritative sources,
	// while head block may contain spans that were just flushed (duplicates).
	// FanoutQuerier queries all queriers in parallel, then mergeResults() deduplicates
	// by span ID, keeping the first occurrence (from persisted blocks).
	//
	// NOTE: This doesn't prevent missing spans if flush happens during query,
	// but that's handled by queryMu.RLock() in QueryWithLock() which prevents
	// flushes during the entire query operation.
	queriers := make([]Querier, 0, 2)

	// Add persisted blocks querier if we have blocks
	if len(blocks) > 0 {
		queriers = append(queriers, NewPersistedBlockQuerier(blocks))
	}

	// Add head block querier if we have a head block
	if head != nil {
		queriers = append(queriers, NewHeadBlockQuerier(head))
	}

	return &BlockQuerier{
		fanoutQuerier: NewFanoutQuerier(queriers...),
	}
}

// Select queries spans across all blocks using matchers
// Queries persisted blocks and head block in parallel using FanoutQuerier
func (q *BlockQuerier) Select(matchers ...*Matcher) (*SelectResult, error) {
	return q.fanoutQuerier.Select(matchers...)
}

// SelectWithTimeRange queries spans across all blocks within a specific time range
// Time range filtering provides significant performance benefits by skipping irrelevant blocks
// Queries persisted blocks and head block in parallel using FanoutQuerier
func (q *BlockQuerier) SelectWithTimeRange(timeRange *TimeRange, matchers ...*Matcher) (*SelectResult, error) {
	return q.fanoutQuerier.SelectWithTimeRange(timeRange, matchers...)
}

// SelectWithOptions queries spans across all blocks with custom options
// Use QueryOptions.IncludeEvents to populate the Events field on returned spans
// Queries persisted blocks and head block in parallel using FanoutQuerier
func (q *BlockQuerier) SelectWithOptions(opts *QueryOptions, matchers ...*Matcher) (*SelectResult, error) {
	return q.fanoutQuerier.SelectWithOptions(opts, matchers...)
}

// SelectWithTimeRangeAndOptions queries spans across all blocks with time range and custom options
// Use QueryOptions.IncludeEvents to populate the Events field on returned spans
// Queries persisted blocks and head block in parallel using FanoutQuerier
func (q *BlockQuerier) SelectWithTimeRangeAndOptions(timeRange *TimeRange, opts *QueryOptions, matchers ...*Matcher) (*SelectResult, error) {
	return q.fanoutQuerier.SelectWithTimeRangeAndOptions(timeRange, opts, matchers...)
}
