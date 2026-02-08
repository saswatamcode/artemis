package query

import (
	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/span"
)

// HeadBlockQuerier queries the in-memory head block
// This is a specialized querier that only queries the head block
type HeadBlockQuerier struct {
	head block.Block // HeadBlock wrapper
}

// NewHeadBlockQuerier creates a new head block querier
func NewHeadBlockQuerier(head block.Block) *HeadBlockQuerier {
	return &HeadBlockQuerier{
		head: head,
	}
}

// Select queries spans from the head block using matchers
func (q *HeadBlockQuerier) Select(matchers ...*Matcher) (*SelectResult, error) {
	return q.SelectWithTimeRange(nil, matchers...)
}

// SelectWithTimeRange queries spans from the head block within a time range
func (q *HeadBlockQuerier) SelectWithTimeRange(timeRange *TimeRange, matchers ...*Matcher) (*SelectResult, error) {
	ms := Matchers(matchers)
	result := &SelectResult{
		Spans: make([]*span.Span, 0),
	}

	// If no head block, return empty result
	if q.head == nil {
		return result, nil
	}

	// Check if time range overlaps with head block
	if timeRange != nil && !timeRangeOverlapsBlock(q.head, timeRange) {
		return result, nil
	}

	// Safe type assertion - head block must be a HeadBlock type
	headBlock, ok := q.head.(*block.HeadBlock)
	if !ok {
		// This should never happen if the querier is constructed properly
		// but we handle it defensively to avoid panics
		return result, nil
	}

	// Query the head block
	spans := queryHeadBlock(headBlock, ms, timeRange)
	result.Spans = append(result.Spans, spans...)

	return result, nil
}
