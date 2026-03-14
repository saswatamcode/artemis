package tracedb

import (
	"fmt"
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/span"
)

// TestGetAllBlocksIncludesHeadBlock verifies that GetAllBlocks returns head block
func TestGetAllBlocksIncludesHeadBlock(t *testing.T) {
	fmt.Println("\n=== TestGetAllBlocksIncludesHeadBlock ===")

	dir := t.TempDir()
	cfg := &Config{
		WALDir: dir + "/wal",
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Write spans using WriteSpans (transactional)
	fmt.Println("\n--- Writing spans ---")
	testSpans := []*span.Span{
		{
			TraceID:     "00000000000000010000000000000001",
			SpanID:      "0000000000000001",
			Name:        "head-block-span",
			ServiceName: "test-service",
			StartTime:   time.Now().Add(-1 * time.Minute),
			EndTime:     time.Now(),
			Tags:        map[string]string{"location": "head"},
		},
	}

	if err := db.WriteSpans(testSpans); err != nil {
		t.Fatalf("Failed to write spans: %v", err)
	}

	// Verify spans are in storage
	rowCount := db.storage.RowCount()
	fmt.Printf("Head block row count: %d\n", rowCount)

	// Test GetBlocks (should return empty since no compaction yet)
	fmt.Println("\n--- Testing GetBlocks (persisted only) ---")
	persistedBlocks := db.GetBlocks()
	fmt.Printf("GetBlocks returned %d blocks\n", len(persistedBlocks))

	// Test GetAllBlocks (should include head block)
	fmt.Println("\n--- Testing GetAllBlocks (includes head) ---")
	allBlocks := db.GetAllBlocks()
	fmt.Printf("GetAllBlocks returned %d blocks\n", len(allBlocks))

	if len(allBlocks) == 0 {
		t.Fatal("GetAllBlocks should return at least head block")
	}

	// Verify last block is head block
	lastBlock := allBlocks[len(allBlocks)-1]
	fmt.Printf("Last block dir: '%s'\n", lastBlock.Dir())
	fmt.Printf("Last block meta: span_count=%d\n", lastBlock.Meta().SpanCount)

	if lastBlock.Dir() != "" {
		t.Errorf("Expected head block (empty dir), got '%s'", lastBlock.Dir())
	}

	// Verify head block contains our span
	meta := lastBlock.Meta()
	fmt.Printf("Head block span count: %d\n", meta.SpanCount)

	if meta.SpanCount != int64(len(testSpans)) {
		t.Errorf("Expected %d spans in head block, got %d", len(testSpans), meta.SpanCount)
	}
}

// TestGetAllBlocksQueryability verifies spans in head block are queryable
func TestGetAllBlocksQueryability(t *testing.T) {
	fmt.Println("\n=== TestGetAllBlocksQueryability ===")

	dir := t.TempDir()
	cfg := &Config{
		WALDir: dir + "/wal",
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Write test spans
	testSpans := []*span.Span{
		{
			TraceID:     "00000000000000020000000000000002",
			SpanID:      "0000000000000002",
			Name:        "queryable-span",
			ServiceName: "test-service",
			StartTime:   time.Now().Add(-1 * time.Minute),
			EndTime:     time.Now(),
			Tags:        map[string]string{"test": "value"},
		},
	}

	if err := db.WriteSpans(testSpans); err != nil {
		t.Fatalf("Failed to write spans: %v", err)
	}

	fmt.Printf("\nHead block row count: %d\n", db.storage.RowCount())

	// Query via GetAllBlocks
	fmt.Println("\n--- Querying via GetAllBlocks ---")
	allBlocks := db.GetAllBlocks()

	foundSpans := 0
	for i, blk := range allBlocks {
		meta := blk.Meta()
		fmt.Printf("Block[%d]: dir=%s, spans=%d\n", i, blk.Dir(), meta.SpanCount)

		// Try to query this block
		spans, err := blk.ReadAll()
		if err != nil {
			t.Errorf("Failed to read block %d: %v", i, err)
			continue
		}

		fmt.Printf("  ReadAll returned %d spans\n", len(spans))
		for _, sp := range spans {
			fmt.Printf("    - %s: %s\n", sp.SpanID, sp.Name)
			foundSpans++
		}
	}

	if foundSpans != len(testSpans) {
		t.Errorf("Expected to find %d spans, found %d", len(testSpans), foundSpans)
	}
}
