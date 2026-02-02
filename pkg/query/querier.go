package query

import (
	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// Querier provides an interface for querying spans from storage
// This allows different implementations and makes testing easier
type Querier interface {
	// Select queries spans using matchers
	// Returns all spans that match the given matchers
	Select(matchers ...*Matcher) (*SelectResult, error)

	// SelectWithTimeRange queries spans using matchers within a time range
	// Time range filtering provides significant performance benefits by skipping irrelevant blocks
	SelectWithTimeRange(timeRange *TimeRange, matchers ...*Matcher) (*SelectResult, error)
}

// BlockQuerier queries spans across both head block (Arrow) and persisted blocks (Arrow L0 and Parquet L1+)
// This is the standard implementation used in production
//
// TODO: Split this into a Block and Head querier so that we can keep select functions cleaner
type BlockQuerier struct {
	head   *storage.ArrowStorage
	blocks []block.Block
}

// NewBlockQuerier creates a new querier that queries across head and persisted blocks
// head can be nil if there's no in-memory data
// blocks can be empty if there are no persisted blocks yet
func NewBlockQuerier(head *storage.ArrowStorage, blocks []block.Block) *BlockQuerier {
	return &BlockQuerier{
		head:   head,
		blocks: blocks,
	}
}

// Select queries spans across all blocks using matchers
// Queries persisted blocks first, then head block (to handle concurrent flushes correctly)
func (q *BlockQuerier) Select(matchers ...*Matcher) (*SelectResult, error) {
	return SelectFromBlocks(q.head, q.blocks, matchers...)
}

// SelectWithTimeRange queries spans across all blocks within a specific time range
// Time range filtering provides significant performance benefits by skipping irrelevant blocks
func (q *BlockQuerier) SelectWithTimeRange(timeRange *TimeRange, matchers ...*Matcher) (*SelectResult, error) {
	return SelectFromBlocksWithTimeRange(q.head, q.blocks, timeRange, matchers...)
}
