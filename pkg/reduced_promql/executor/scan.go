package executor

import (
	"sync"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/query"
	"github.com/saswatamcode/artemis/pkg/span"
)

// ScanOperator implements VectorSelector - scans blocks with label matchers.
//
// This is a leaf operator that reads span data from blocks using the existing
// queryBlockWithTimeRange infrastructure. It implements parallel block scanning
// using goroutines and channels.
//
// Architecture:
//   - On first Next() call, spawns one goroutine per block
//   - Each goroutine queries its block and sends results to resultCh
//   - Next() reads from resultCh and returns batches sequentially
//   - Errors are propagated immediately via errCh
//
// Integration points:
//   - Uses query.Matcher from pkg/query/matcher.go
//   - Queries blocks via Block interface (HeadBlock, ArrowBlock, ParquetBlock)
//   - Integrates with existing query execution path
type ScanOperator struct {
	blocks   []block.Block
	matchers query.Matchers
	ctx      *ExecutionContext

	// Parallel execution state
	resultCh   chan []*span.Span
	errCh      chan error
	wg         sync.WaitGroup
	started    bool
	done       bool
	closeOnce  sync.Once // Ensure channels are only closed once
	resultOnce sync.Once // Ensure resultCh is only closed once

	// Current batch being returned
	current []*span.Span
}

// NewScanOperator creates a new scan operator.
func NewScanOperator(blocks []block.Block, matchers query.Matchers, execCtx *ExecutionContext) *ScanOperator {
	return &ScanOperator{
		blocks:   blocks,
		matchers: matchers,
		ctx:      execCtx,
		resultCh: make(chan []*span.Span, len(blocks)), // Buffer one result per block
		errCh:    make(chan error, len(blocks)),
	}
}

// Next returns the next batch of spans from the scan.
func (s *ScanOperator) Next() ([]*span.Span, error) {
	// Start parallel block scanning on first call
	if !s.started {
		s.start()
		s.started = true
	}

	// Check for cancellation
	if s.ctx.IsCancelled() {
		return nil, s.ctx.Err()
	}

	// Check if already exhausted
	if s.done {
		return nil, nil
	}

	// Try to receive next batch
	select {
	case <-s.ctx.Ctx.Done():
		// Query cancelled
		return nil, s.ctx.Err()

	case err := <-s.errCh:
		// Error from one of the block scanners
		return nil, err

	case batch, ok := <-s.resultCh:
		if !ok {
			// Channel closed, all blocks scanned
			s.done = true
			return nil, nil
		}
		return batch, nil
	}
}

// Close releases resources held by the scan operator.
func (s *ScanOperator) Close() error {
	// Wait for all goroutines to finish
	s.wg.Wait()

	// Close errCh only (resultCh is closed by coordinator goroutine)
	// Use closeOnce to ensure we only close once even if Close() is called multiple times
	s.closeOnce.Do(func() {
		if s.started {
			close(s.errCh)
		}
	})

	return nil
}

// start spawns goroutines to query each block in parallel.
func (s *ScanOperator) start() {
	// Special case: no blocks to scan
	if len(s.blocks) == 0 {
		s.done = true
		return
	}

	// Spawn one goroutine per block
	for _, blk := range s.blocks {
		s.wg.Add(1)
		go s.scanBlock(blk)
	}

	// Spawn coordinator goroutine to close resultCh when all blocks are done
	go func() {
		s.wg.Wait()
		// Use resultOnce to ensure we only close once (protects against double-close panics)
		s.resultOnce.Do(func() {
			close(s.resultCh)
		})
	}()
}

// scanBlock queries a single block and sends results to resultCh.
func (s *ScanOperator) scanBlock(blk block.Block) {
	defer s.wg.Done()

	// Check for cancellation before starting expensive operation
	if s.ctx.IsCancelled() {
		return
	}

	// Record that we're scanning this block
	s.ctx.RecordBlockScanned()

	// Build time range for this query
	timeRange := &query.TimeRange{
		Start: s.ctx.StartTime,
		End:   s.ctx.EndTime,
	}

	// Query the block using the existing infrastructure
	// This delegates to queryBlockWithTimeRange which handles:
	//   - HeadBlock: Direct memory access to ArrowStorage
	//   - ArrowBlock: Load Arrow IPC file, indexed lookups
	//   - ParquetBlock: Page-level Parquet reads with OffsetIndex
	spans := queryBlockWithTimeRange(blk, s.matchers, timeRange)

	// Record spans scanned
	s.ctx.RecordSpansScanned(int64(len(spans)))

	// Filter by MVCC snapshot if needed
	if s.ctx.Snapshot > 0 {
		filtered := s.ctx.GetSpanBatch()
		for _, sp := range spans {
			if s.ctx.Isolation.IsVisible(sp.SpanID, s.ctx.Snapshot) {
				*filtered = append(*filtered, sp)
			}
		}
		spans = *filtered
	}

	// Send results if we have any
	if len(spans) > 0 {
		select {
		case s.resultCh <- spans:
			// Sent successfully
		case <-s.ctx.Ctx.Done():
			// Query cancelled, stop sending
			return
		}
	}
}

// queryBlockWithTimeRange uses the optimized query implementation from pkg/query.
// This uses indexes and efficient scan paths instead of full table scans.
func queryBlockWithTimeRange(blk block.Block, ms query.Matchers, timeRange *query.TimeRange) []*span.Span {
	// Delegate to the optimized implementation in pkg/query
	// This uses:
	//   - Index lookups for trace_id, span_id
	//   - Attribute-first query for tags
	//   - Efficient block scanning
	return query.QueryBlockWithTimeRange(blk, ms, timeRange)
}
