package tracedb

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/query"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// TestDB_WALSegmentRaceCondition tests that WAL segment tracking is correct
// even when segment rotation happens during WriteSpan
func TestDB_WALSegmentRaceCondition(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:          filepath.Join(tmpDir, "wal"),
		CompactInterval: 0,
		BlockConfig: &block.Config{
			Dir:              filepath.Join(tmpDir, "blocks"),
			MaxBlockDuration: 1 * time.Hour,
			MaxBlockSpans:    100,
		},
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// Write many spans with large data to trigger WAL segment rotation
	// Each span should be tracked in the correct segment
	const numSpans = 1000
	for i := range numSpans {
		sp := &span.Span{
			TraceID:     fmt.Sprintf("%032x", i),
			SpanID:      fmt.Sprintf("%016x", i+1),
			Name:        "operation-with-very-long-name-to-increase-size-and-trigger-rotation",
			StartTime:   time.Now().Add(time.Duration(i) * time.Millisecond),
			EndTime:     time.Now().Add(time.Duration(i+1) * time.Millisecond),
			ServiceName: "service-with-long-name-for-increased-size",
			Tags: map[string]string{
				"tag1": "value1-very-long-value-to-increase-record-size-significantly",
				"tag2": "value2-very-long-value-to-increase-record-size-significantly",
				"tag3": "value3-very-long-value-to-increase-record-size-significantly",
			},
		}

		if err := db.WriteSpan(sp); err != nil {
			t.Fatalf("WriteSpan(%d) error = %v", i, err)
		}
	}

	// Flush and check WAL segment range
	db.Flush()
	minWAL, maxWAL := db.storage.GetWALSegmentRange()

	t.Logf("WAL segment range: %d - %d", minWAL, maxWAL)

	// Verify segments are tracked
	if minWAL == -1 {
		t.Error("MinWALSegment should be set after writing spans")
	}
	if maxWAL == -1 {
		t.Error("MaxWALSegment should be set after writing spans")
	}
	if maxWAL < minWAL {
		t.Errorf("MaxWALSegment (%d) should be >= MinWALSegment (%d)", maxWAL, minWAL)
	}

	// Flush to block and verify block has correct WAL segment range
	if err := db.flushHeadBlock(); err != nil {
		t.Fatalf("flushHeadBlock() error = %v", err)
	}

	blocks := db.GetBlocks()
	if len(blocks) == 0 {
		t.Fatal("Expected at least 1 block after flush")
	}

	blockMeta := blocks[0].Meta()
	if blockMeta.MinWALSegment < 0 {
		t.Errorf("Block MinWALSegment = %d, should be >= 0", blockMeta.MinWALSegment)
	}
	if blockMeta.MaxWALSegment < blockMeta.MinWALSegment {
		t.Errorf("Block MaxWALSegment (%d) should be >= MinWALSegment (%d)",
			blockMeta.MaxWALSegment, blockMeta.MinWALSegment)
	}

	t.Logf("Block WAL segment range: %d - %d", blockMeta.MinWALSegment, blockMeta.MaxWALSegment)
}

// TestDB_QueryFlushRaceCondition tests that queries are properly synchronized
// with head block flushes to prevent missing or duplicate spans
func TestDB_QueryFlushRaceCondition(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:          filepath.Join(tmpDir, "wal"),
		CompactInterval: 0,
		BlockConfig: &block.Config{
			Dir:              filepath.Join(tmpDir, "blocks"),
			MaxBlockDuration: 1 * time.Hour,
			MaxBlockSpans:    50, // Low threshold for frequent flushes
		},
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// Write initial spans
	const numSpans = 100
	for i := range numSpans {
		sp := &span.Span{
			TraceID:     "00000000000000000000000000000001",
			SpanID:      fmt.Sprintf("%016x", i+1),
			Name:        "operation",
			StartTime:   time.Now().Add(time.Duration(i) * time.Millisecond),
			EndTime:     time.Now().Add(time.Duration(i+1) * time.Millisecond),
			ServiceName: "test-service",
			Tags: map[string]string{
				"index": fmt.Sprintf("%d", i),
			},
		}
		if err := db.WriteSpan(sp); err != nil {
			t.Fatalf("WriteSpan() error = %v", err)
		}
	}
	db.Flush()

	// Run concurrent queries and flushes
	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	// Query goroutines (using QueryWithLock)
	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range 10 {
				matcher, _ := query.NewMatcher(query.MatchEqual, "trace_id", "00000000000000000000000000000001")
				var result *query.SelectResult
				err := db.QueryWithLock(func(head *storage.ArrowStorage, blocks []block.Block) error {
					var queryErr error
					result, queryErr = query.SelectFromBlocks(block.NewHeadBlock(head), blocks, matcher)
					return queryErr
				})
				if err != nil {
					errCh <- fmt.Errorf("query goroutine %d iteration %d: %w", id, j, err)
					return
				}

				// Should get all spans that have been written
				if len(result.Spans) != numSpans {
					errCh <- fmt.Errorf("query goroutine %d iteration %d: got %d spans, want %d",
						id, j, len(result.Spans), numSpans)
					return
				}
			}
		}(i)
	}

	// Flush goroutine
	wg.Go(func() {
		for range 5 {
			time.Sleep(10 * time.Millisecond)
			if err := db.flushHeadBlock(); err != nil {
				errCh <- fmt.Errorf("flush error: %w", err)
				return
			}
		}
	})

	// Wait for all goroutines
	wg.Wait()
	close(errCh)

	// Check for errors
	for err := range errCh {
		t.Error(err)
	}

	// Final verification - all spans should be queryable
	matcher, _ := query.NewMatcher(query.MatchEqual, "trace_id", "00000000000000000000000000000001")
	var finalResult *query.SelectResult
	err = db.QueryWithLock(func(head *storage.ArrowStorage, blocks []block.Block) error {
		var queryErr error
		finalResult, queryErr = query.SelectFromBlocks(block.NewHeadBlock(head), blocks, matcher)
		return queryErr
	})
	if err != nil {
		t.Fatalf("Final query error: %v", err)
	}

	if len(finalResult.Spans) != numSpans {
		t.Errorf("Final query: got %d spans, want %d", len(finalResult.Spans), numSpans)
	}

	t.Logf("Race test completed: %d spans across %d blocks", len(finalResult.Spans), len(db.GetBlocks()))
}

// TestDB_ConcurrentWritesAndQueries tests concurrent writes and queries
// to ensure data consistency
func TestDB_ConcurrentWritesAndQueries(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:          filepath.Join(tmpDir, "wal"),
		CompactInterval: 0,
		BlockConfig: &block.Config{
			Dir:              filepath.Join(tmpDir, "blocks"),
			MaxBlockDuration: 1 * time.Hour,
			MaxBlockSpans:    100,
		},
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	const numWriters = 5
	const numReaders = 5
	const spansPerWriter = 20

	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	// Writer goroutines
	for writerID := range numWriters {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range spansPerWriter {
				sp := &span.Span{
					TraceID:     fmt.Sprintf("%032x", id),
					SpanID:      fmt.Sprintf("%016x", id*1000+i+1),
					Name:        "concurrent-write",
					StartTime:   time.Now(),
					EndTime:     time.Now().Add(time.Millisecond),
					ServiceName: "concurrent-service",
				}
				if err := db.WriteSpan(sp); err != nil {
					errCh <- fmt.Errorf("writer %d: %w", id, err)
					return
				}
				time.Sleep(time.Millisecond) // Small delay
			}
		}(writerID)
	}

	// Reader goroutines
	for readerID := range numReaders {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range 20 {
				matcher, _ := query.NewMatcher(query.MatchEqual, "service.name", "concurrent-service")
				var result *query.SelectResult
				err := db.QueryWithLock(func(head *storage.ArrowStorage, blocks []block.Block) error {
					var queryErr error
					result, queryErr = query.SelectFromBlocks(block.NewHeadBlock(head), blocks, matcher)
					return queryErr
				})
				if err != nil {
					errCh <- fmt.Errorf("reader %d: %w", id, err)
					return
				}
				// Should get at least some spans (exact count varies due to concurrency)
				t.Logf("Reader %d iteration %d: found %d spans", id, i, len(result.Spans))
				time.Sleep(5 * time.Millisecond)
			}
		}(readerID)
	}

	// Wait for all operations
	wg.Wait()
	close(errCh)

	// Check for errors
	for err := range errCh {
		t.Error(err)
	}

	// Final verification - should have all written spans
	db.Flush()
	expectedSpans := numWriters * spansPerWriter

	matcher, _ := query.NewMatcher(query.MatchEqual, "service.name", "concurrent-service")
	var finalResult *query.SelectResult
	err = db.QueryWithLock(func(head *storage.ArrowStorage, blocks []block.Block) error {
		var queryErr error
		finalResult, queryErr = query.SelectFromBlocks(block.NewHeadBlock(head), blocks, matcher)
		return queryErr
	})
	if err != nil {
		t.Fatalf("Final query error: %v", err)
	}

	if len(finalResult.Spans) != expectedSpans {
		t.Errorf("Final count: got %d spans, want %d", len(finalResult.Spans), expectedSpans)
	}

	t.Logf("Concurrent test completed: %d writers × %d spans/writer = %d total spans",
		numWriters, spansPerWriter, len(finalResult.Spans))
}

// TestDB_CheckpointWithHeadData tests that checkpoint correctly handles
// the case where head block has unpersisted data
func TestDB_CheckpointWithHeadData(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:              filepath.Join(tmpDir, "wal"),
		CompactInterval:     0,
		CheckpointInterval:  1 * time.Hour,
		CheckpointThreshold: 1,
		BlockConfig: &block.Config{
			Dir:              filepath.Join(tmpDir, "blocks"),
			MaxBlockDuration: 1 * time.Hour,
			MaxBlockSpans:    10,
		},
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// Scenario: Flush first block (segment 0)
	for i := range 10 {
		sp := &span.Span{
			TraceID:     "00000000000000000000000000000001",
			SpanID:      fmt.Sprintf("%016x", i+1),
			Name:        "operation",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		db.WriteSpan(sp)
	}
	db.Flush()
	db.flushHeadBlock()

	// Write to head but DON'T flush (stays in segment 0)
	for i := range 5 {
		sp := &span.Span{
			TraceID:     "00000000000000000000000000000001",
			SpanID:      fmt.Sprintf("%016x", i+11),
			Name:        "operation",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		db.WriteSpan(sp)
	}

	// Get head's WAL range
	headMin, headMax := db.storage.GetWALSegmentRange()
	t.Logf("Head WAL range: %d-%d", headMin, headMax)

	// Try to checkpoint - should NOT delete segment 0 (head still needs it)
	maxPersisted := db.findMaxPersistedWALSegment()
	t.Logf("Max persisted WAL segment: %d", maxPersisted)

	// With head at segment 0, we can't delete segment 0
	if headMin == 0 && maxPersisted >= 0 {
		t.Errorf("With head at segment 0, maxPersisted should be -1 (can't delete), got %d", maxPersisted)
	}

	// Close and reopen - should recover all 15 spans
	db.Close()

	db2, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer db2.Close()

	stats := db2.Stats()
	if stats.TotalSpans != 15 {
		t.Errorf("After recovery: got %d spans, want 15", stats.TotalSpans)
	}

	t.Logf("Successfully recovered %d spans (%d in blocks + %d in head)",
		stats.TotalSpans, db2.BlockStats().PersistedSpans, stats.TotalSpans-db2.BlockStats().PersistedSpans)
}
