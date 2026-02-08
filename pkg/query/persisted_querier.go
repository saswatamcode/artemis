package query

import (
	"sync"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/span"
)

// PersistedBlockQuerier queries individual persisted blocks (Arrow or Parquet)
// This querier can query blocks in parallel for better performance
type PersistedBlockQuerier struct {
	blocks []block.Block
}

// NewPersistedBlockQuerier creates a new persisted block querier
func NewPersistedBlockQuerier(blocks []block.Block) *PersistedBlockQuerier {
	return &PersistedBlockQuerier{
		blocks: blocks,
	}
}

// Select queries spans from persisted blocks using matchers
func (q *PersistedBlockQuerier) Select(matchers ...*Matcher) (*SelectResult, error) {
	return q.SelectWithTimeRange(nil, matchers...)
}

// SelectWithTimeRange queries spans from persisted blocks within a time range
// This method queries blocks in parallel for better performance
func (q *PersistedBlockQuerier) SelectWithTimeRange(timeRange *TimeRange, matchers ...*Matcher) (*SelectResult, error) {
	ms := Matchers(matchers)

	if len(q.blocks) == 0 {
		return &SelectResult{Spans: make([]*span.Span, 0)}, nil
	}

	// Query blocks in parallel
	type blockResult struct {
		spans []*span.Span
		err   error
	}

	resultCh := make(chan blockResult, len(q.blocks))
	var wg sync.WaitGroup

	for _, blk := range q.blocks {
		// Skip blocks that don't overlap with time range
		if timeRange != nil {
			meta := blk.Meta()
			if !timeRange.Overlaps(meta.MinTime, meta.MaxTime) {
				continue
			}
		}

		wg.Add(1)
		go func(b block.Block) {
			defer wg.Done()
			spans := q.queryBlock(b, ms, timeRange)
			resultCh <- blockResult{spans: spans, err: nil}
		}(blk)
	}

	// Wait for all queries to complete
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect results
	result := &SelectResult{
		Spans: make([]*span.Span, 0),
	}

	for res := range resultCh {
		if res.err == nil {
			result.Spans = append(result.Spans, res.spans...)
		}
	}

	return result, nil
}

// queryBlock queries a single block (Arrow or Parquet)
func (q *PersistedBlockQuerier) queryBlock(blk block.Block, ms Matchers, timeRange *TimeRange) []*span.Span {
	// Try Parquet block first
	if pb, ok := blk.(*block.ParquetBlock); ok {
		return queryParquetBlock(pb, ms, timeRange)
	}

	// Try Arrow block
	if ab, ok := blk.(*block.ArrowBlock); ok {
		return queryArrowBlock(ab, ms, timeRange)
	}

	// Unknown block type
	return make([]*span.Span, 0)
}
