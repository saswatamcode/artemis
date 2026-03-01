package block

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

func TestAttributesIntegration(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "test-block")

	// Create test spans with tags
	now := time.Now()
	spans := []*span.Span{
		{
			TraceID:     "00000000000000010000000000000000",
			SpanID:      "0000000000000001",
			Name:        "test1",
			StartTime:   now,
			EndTime:     now.Add(time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "svc1",
			Tags: map[string]string{
				"http.method": "GET",
				"http.url":    "/api/test",
			},
		},
		{
			TraceID:     "00000000000000010000000000000000",
			SpanID:      "0000000000000002",
			Name:        "test2",
			StartTime:   now,
			EndTime:     now.Add(time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "svc2",
			Tags: map[string]string{
				"db.type": "postgres",
			},
		},
	}

	// Create index
	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	for _, sp := range spans {
		arrowStorage.AddSpan(sp)
	}

	meta := &BlockMeta{
		ULID:       ulid.Make(),
		MinTime:    now.UnixNano(),
		MaxTime:    now.Add(time.Millisecond).UnixNano(),
		SpanCount:  int64(len(spans)),
		Version:    1,
		CreatedAt:  now,
		Compaction: &CompactionMeta{Level: 1},
	}

	// Write block
	t.Log("Writing parquet block...")
	err := WriteParquetBlock(blockDir, meta, spans, arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("WriteParquetBlock failed: %v", err)
	}

	// Check if attributes.parquet was created
	attrsPath := filepath.Join(blockDir, "attributes.parquet")
	if _, err := os.Stat(attrsPath); os.IsNotExist(err) {
		t.Fatalf("attributes.parquet was not created!")
	}
	t.Log("✓ attributes.parquet exists")

	// Read back the block
	t.Log("Reading parquet block...")
	pb, err := NewParquetBlock(blockDir)
	if err != nil {
		t.Fatalf("NewParquetBlock failed: %v", err)
	}
	defer pb.Close()

	// Test ReadAll
	t.Log("Testing ReadAll...")
	allSpans, err := pb.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(allSpans) != len(spans) {
		t.Fatalf("ReadAll returned %d spans, want %d", len(allSpans), len(spans))
	}

	// Verify tags on first span
	span0 := allSpans[0]
	t.Logf("Span 0 tags: %v", span0.Tags)
	if len(span0.Tags) != 2 {
		t.Errorf("Span 0: got %d tags, want 2. Tags: %v", len(span0.Tags), span0.Tags)
	}
	if span0.Tags["http.method"] != "GET" {
		t.Errorf("Span 0: http.method = %q, want GET", span0.Tags["http.method"])
	}
	if span0.Tags["http.url"] != "/api/test" {
		t.Errorf("Span 0: http.url = %q, want /api/test", span0.Tags["http.url"])
	}

	// Verify tags on second span
	span1 := allSpans[1]
	t.Logf("Span 1 tags: %v", span1.Tags)
	if len(span1.Tags) != 1 {
		t.Errorf("Span 1: got %d tags, want 1. Tags: %v", len(span1.Tags), span1.Tags)
	}
	if span1.Tags["db.type"] != "postgres" {
		t.Errorf("Span 1: db.type = %q, want postgres", span1.Tags["db.type"])
	}

	// Test GetSpanByID
	t.Log("Testing GetSpanByID...")
	retrieved, err := pb.GetSpanByID("0000000000000001")
	if err != nil {
		t.Fatalf("GetSpanByID failed: %v", err)
	}
	t.Logf("Retrieved span tags: %v", retrieved.Tags)
	if len(retrieved.Tags) != 2 {
		t.Errorf("GetSpanByID: got %d tags, want 2. Tags: %v", len(retrieved.Tags), retrieved.Tags)
	}

	// Test GetSpansBatch
	t.Log("Testing GetSpansBatch...")
	batchSpans, err := pb.GetSpansBatch([]string{"0000000000000001", "0000000000000002"})
	if err != nil {
		t.Fatalf("GetSpansBatch failed: %v", err)
	}
	if len(batchSpans) != 2 {
		t.Fatalf("GetSpansBatch returned %d spans, want 2", len(batchSpans))
	}
	for i, sp := range batchSpans {
		t.Logf("Batch span %d tags: %v", i, sp.Tags)
		if len(sp.Tags) == 0 {
			t.Errorf("Batch span %d has no tags!", i)
		}
	}

	t.Log("✓ All tests passed!")
}
