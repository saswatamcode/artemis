package query

import (
	"fmt"
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// simpleWAL is a simple WAL implementation for testing (returns segment 0 always)
type simpleWAL struct{}

func (m *simpleWAL) WriteSpan(s *span.Span) (int, error) {
	return 0, nil // Return segment 0
}

func (m *simpleWAL) WriteLink(link *span.SpanLink) (int, error) {
	return 0, nil // Return segment 0
}

// TestHeadBlockQueryVisibility tests that spans in head block are queryable
// This test reproduces the issue where head block spans aren't visible via GetQuerier()
func TestHeadBlockQueryVisibility(t *testing.T) {
	fmt.Println("\n=== TestHeadBlockQueryVisibility ===")

	// Create storage with isolation coordinator (simulating real DB setup)
	arrowStorage := storage.NewArrowStorage()
	linkStorage := storage.NewArrowLinkStorage()
	isolationCoord := storage.NewIsolationCoordinator()

	// Configure transactional dependencies (like tracedb.NewDB does)
	wal := &simpleWAL{}
	arrowStorage.SetTransactionDependencies(isolationCoord, linkStorage, wal)

	// Add test spans using transactional appender (like WriteSpans does)
	testSpans := []*span.Span{
		{
			TraceID:     "00000000000000010000000000000001",
			SpanID:      "0000000000000001",
			Name:        "test-span-1",
			ServiceName: "test-service",
			StartTime:   time.Now().Add(-1 * time.Minute),
			EndTime:     time.Now(),
			Tags:        map[string]string{"env": "test"},
		},
		{
			TraceID:     "00000000000000020000000000000002",
			SpanID:      "0000000000000002",
			Name:        "test-span-2",
			ServiceName: "test-service",
			StartTime:   time.Now().Add(-2 * time.Minute),
			EndTime:     time.Now(),
			Tags:        map[string]string{"env": "test"},
		},
	}

	// Use transactional appender (simulating WriteSpans)
	fmt.Println("\n--- Writing spans via transaction ---")
	appender := arrowStorage.BeginTransaction()
	for _, sp := range testSpans {
		if err := appender.AddSpan(sp); err != nil {
			t.Fatalf("Failed to add span to transaction: %v", err)
		}
	}
	if err := appender.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Flush to ensure spans are in records
	arrowStorage.Flush()

	// Verify spans are in storage
	rowCount := arrowStorage.RowCount()
	fmt.Printf("\nStorage row count: %d\n", rowCount)
	if rowCount != int64(len(testSpans)) {
		t.Fatalf("Expected %d spans in storage, got %d", len(testSpans), rowCount)
	}

	// Create HeadBlock (like GetQuerier does)
	fmt.Println("\n--- Creating HeadBlock ---")
	headBlock := block.NewHeadBlock(arrowStorage, linkStorage)

	// Check isolation coordinator
	ic := headBlock.IsolationCoordinator()
	fmt.Printf("HeadBlock isolation coordinator: %v\n", ic != nil)
	if ic != nil {
		snapshot := ic.BeginQuery()
		fmt.Printf("Current snapshot sequence: %d\n", snapshot)
	}

	// Query via HeadBlockQuerier (simulating GetQuerier flow)
	fmt.Println("\n--- Querying via HeadBlockQuerier ---")
	querier := NewHeadBlockQuerier(headBlock)

	// Query without matchers (should return all spans)
	result, err := querier.Select()
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	fmt.Printf("\nQuery returned %d spans (expected %d)\n", len(result.Spans), len(testSpans))
	for i, sp := range result.Spans {
		fmt.Printf("  [%d] SpanID=%s Name=%s\n", i, sp.SpanID, sp.Name)
	}

	// Verify results
	if len(result.Spans) != len(testSpans) {
		t.Errorf("Expected %d spans, got %d", len(testSpans), len(result.Spans))

		// Debug: Try direct read from storage
		fmt.Println("\n--- Debug: Direct read from storage ---")
		records := arrowStorage.GetRecords()
		fmt.Printf("Records count: %d\n", len(records))
		for i, record := range records {
			fmt.Printf("  Record[%d]: %d rows\n", i, record.NumRows())
		}
	}
}

// TestHeadBlockQueryVisibilityNonTransactional tests with non-transactional writes
func TestHeadBlockQueryVisibilityNonTransactional(t *testing.T) {
	fmt.Println("\n=== TestHeadBlockQueryVisibilityNonTransactional ===")

	// Create storage WITHOUT setting transaction dependencies
	arrowStorage := storage.NewArrowStorage()
	linkStorage := storage.NewArrowLinkStorage()

	// Add test spans using non-transactional AddSpan (like WAL replay)
	testSpans := []*span.Span{
		{
			TraceID:     "00000000000000030000000000000003",
			SpanID:      "0000000000000003",
			Name:        "wal-replay-span-1",
			ServiceName: "test-service",
			StartTime:   time.Now().Add(-1 * time.Minute),
			EndTime:     time.Now(),
			Tags:        map[string]string{"source": "wal"},
		},
	}

	fmt.Println("\n--- Writing spans via AddSpan (non-transactional) ---")
	for _, sp := range testSpans {
		if err := arrowStorage.AddSpan(sp); err != nil {
			t.Fatalf("Failed to add span: %v", err)
		}
	}

	arrowStorage.Flush()

	fmt.Printf("Storage row count: %d\n", arrowStorage.RowCount())

	// Create HeadBlock
	fmt.Println("\n--- Creating HeadBlock ---")
	headBlock := block.NewHeadBlock(arrowStorage, linkStorage)

	ic := headBlock.IsolationCoordinator()
	fmt.Printf("HeadBlock isolation coordinator: %v\n", ic != nil)

	// Query
	fmt.Println("\n--- Querying ---")
	querier := NewHeadBlockQuerier(headBlock)
	result, err := querier.Select()
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	fmt.Printf("Query returned %d spans (expected %d)\n", len(result.Spans), len(testSpans))

	if len(result.Spans) != len(testSpans) {
		t.Errorf("Expected %d spans, got %d", len(testSpans), len(result.Spans))
	}
}

// TestHeadBlockQueryRaceCondition tests for race between write and query
func TestHeadBlockQueryRaceCondition(t *testing.T) {
	fmt.Println("\n=== TestHeadBlockQueryRaceCondition ===")

	// Create storage with isolation coordinator
	arrowStorage := storage.NewArrowStorage()
	linkStorage := storage.NewArrowLinkStorage()
	isolationCoord := storage.NewIsolationCoordinator()
	wal := &simpleWAL{}
	arrowStorage.SetTransactionDependencies(isolationCoord, linkStorage, wal)

	// Write first batch
	fmt.Println("\n--- Writing first batch ---")
	appender1 := arrowStorage.BeginTransaction()
	span1 := &span.Span{
		TraceID:     "00000000000000040000000000000004",
		SpanID:      "0000000000000004",
		Name:        "first-batch",
		ServiceName: "test-service",
		StartTime:   time.Now().Add(-1 * time.Minute),
		EndTime:     time.Now(),
		Tags:        map[string]string{"batch": "1"},
	}
	appender1.AddSpan(span1)
	if err := appender1.Commit(); err != nil {
		t.Fatalf("Failed to commit first batch: %v", err)
	}
	arrowStorage.Flush()

	// Query - should see first batch
	fmt.Println("\n--- Query after first batch ---")
	headBlock1 := block.NewHeadBlock(arrowStorage, linkStorage)
	querier1 := NewHeadBlockQuerier(headBlock1)
	result1, _ := querier1.Select()
	fmt.Printf("Query 1 returned %d spans\n", len(result1.Spans))

	// Write second batch
	fmt.Println("\n--- Writing second batch ---")
	appender2 := arrowStorage.BeginTransaction()
	span2 := &span.Span{
		TraceID:     "00000000000000050000000000000005",
		SpanID:      "0000000000000005",
		Name:        "second-batch",
		ServiceName: "test-service",
		StartTime:   time.Now().Add(-1 * time.Minute),
		EndTime:     time.Now(),
		Tags:        map[string]string{"batch": "2"},
	}
	appender2.AddSpan(span2)
	if err := appender2.Commit(); err != nil {
		t.Fatalf("Failed to commit second batch: %v", err)
	}
	arrowStorage.Flush()

	// Query again - should see BOTH batches
	fmt.Println("\n--- Query after second batch ---")
	headBlock2 := block.NewHeadBlock(arrowStorage, linkStorage)
	querier2 := NewHeadBlockQuerier(headBlock2)
	result2, _ := querier2.Select()
	fmt.Printf("Query 2 returned %d spans\n", len(result2.Spans))

	if len(result2.Spans) != 2 {
		t.Errorf("Expected 2 spans after second batch, got %d", len(result2.Spans))

		// Debug
		fmt.Printf("\nDEBUG: Storage row count: %d\n", arrowStorage.RowCount())
		snapshot := isolationCoord.BeginQuery()
		fmt.Printf("DEBUG: Current snapshot: %d\n", snapshot)
	}
}

// TestHeadBlockQueryViaBlockQuerier tests the full GetQuerier() flow
func TestHeadBlockQueryViaBlockQuerier(t *testing.T) {
	fmt.Println("\n=== TestHeadBlockQueryViaBlockQuerier ===")

	// Create storage (simulating DB setup)
	arrowStorage := storage.NewArrowStorage()
	linkStorage := storage.NewArrowLinkStorage()
	isolationCoord := storage.NewIsolationCoordinator()
	wal := &simpleWAL{}
	arrowStorage.SetTransactionDependencies(isolationCoord, linkStorage, wal)

	// Write spans
	fmt.Println("\n--- Writing spans ---")
	appender := arrowStorage.BeginTransaction()
	for i := 1; i <= 3; i++ {
		sp := &span.Span{
			TraceID:     fmt.Sprintf("0000000000000%03d0000000000000%03d", i, i),
			SpanID:      fmt.Sprintf("0000000000000%03d", i),
			Name:        fmt.Sprintf("span-%d", i),
			ServiceName: "test-service",
			StartTime:   time.Now().Add(-1 * time.Minute),
			EndTime:     time.Now(),
			Tags:        map[string]string{"index": fmt.Sprintf("%d", i)},
		}
		appender.AddSpan(sp)
	}
	if err := appender.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Flush (like GetQuerier does)
	fmt.Println("\n--- Flushing storage ---")
	arrowStorage.Flush()
	fmt.Printf("Storage row count: %d\n", arrowStorage.RowCount())

	// Create BlockQuerier (like GetQuerier does)
	fmt.Println("\n--- Creating BlockQuerier ---")
	headBlock := block.NewHeadBlock(arrowStorage, linkStorage)
	var blocks []block.Block // No persisted blocks
	querier := NewBlockQuerier(headBlock, blocks)

	// Query
	fmt.Println("\n--- Querying ---")
	result, err := querier.Select()
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	fmt.Printf("Query returned %d spans (expected 3)\n", len(result.Spans))
	for i, sp := range result.Spans {
		fmt.Printf("  [%d] SpanID=%s Name=%s\n", i, sp.SpanID, sp.Name)
	}

	if len(result.Spans) != 3 {
		t.Errorf("Expected 3 spans, got %d", len(result.Spans))
	}
}
