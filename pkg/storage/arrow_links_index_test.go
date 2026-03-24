package storage

import (
	"testing"

	"github.com/saswatamcode/artemis/pkg/span"
)

// TestLinkIndex verifies that the link index provides O(1) lookups
func TestLinkIndex_BasicLookup(t *testing.T) {
	storage := NewArrowLinkStorage()
	defer storage.Release()

	// Add some test links
	links := []*span.SpanLink{
		{SpanID: "span1", LinkedTraceID: "trace1", LinkedSpanID: "linked1", Attributes: map[string]string{"key": "value1"}},
		{SpanID: "span1", LinkedTraceID: "trace2", LinkedSpanID: "linked2", Attributes: map[string]string{"key": "value2"}},
		{SpanID: "span2", LinkedTraceID: "trace3", LinkedSpanID: "linked3", Attributes: map[string]string{"key": "value3"}},
	}

	for _, l := range links {
		if err := storage.AddLink(l); err != nil {
			t.Fatalf("Failed to add link: %v", err)
		}
	}

	// Flush to create records (links are in builder until flushed)
	if err := storage.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Test indexed lookup for span1 (should return 2 links)
	result, err := storage.GetLinksBySpanID("span1")
	if err != nil {
		t.Fatalf("GetLinksBySpanID failed: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("Expected 2 links for span1, got %d", len(result))
	}

	// Test indexed lookup for span2 (should return 1 link)
	result, err = storage.GetLinksBySpanID("span2")
	if err != nil {
		t.Fatalf("GetLinksBySpanID failed: %v", err)
	}

	if len(result) != 1 {
		t.Errorf("Expected 1 link for span2, got %d", len(result))
	}

	// Test lookup for non-existent span (should return empty)
	result, err = storage.GetLinksBySpanID("nonexistent")
	if err != nil {
		t.Fatalf("GetLinksBySpanID failed: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("Expected 0 links for nonexistent span, got %d", len(result))
	}
}

// TestLinkIndex_BatchLookup verifies batch lookups work correctly
func TestLinkIndex_BatchLookup(t *testing.T) {
	storage := NewArrowLinkStorage()
	defer storage.Release()

	// Add test links
	links := []*span.SpanLink{
		{SpanID: "span1", LinkedTraceID: "trace1", LinkedSpanID: "linked1"},
		{SpanID: "span2", LinkedTraceID: "trace2", LinkedSpanID: "linked2"},
		{SpanID: "span3", LinkedTraceID: "trace3", LinkedSpanID: "linked3"},
	}

	for _, l := range links {
		if err := storage.AddLink(l); err != nil {
			t.Fatalf("Failed to add link: %v", err)
		}
	}

	// Flush to create records
	if err := storage.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Batch lookup
	result, err := storage.GetLinksBatch([]string{"span1", "span3", "nonexistent"})
	if err != nil {
		t.Fatalf("GetLinksBatch failed: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("Expected 2 entries in result map, got %d", len(result))
	}

	if _, found := result["span1"]; !found {
		t.Error("Expected span1 in results")
	}

	if _, found := result["span3"]; !found {
		t.Error("Expected span3 in results")
	}

	if _, found := result["nonexistent"]; found {
		t.Error("Did not expect nonexistent span in results")
	}
}

// TestLinkIndex_AcrossRecordBatches verifies index works across multiple record batches
func TestLinkIndex_AcrossRecordBatches(t *testing.T) {
	storage := NewArrowLinkStorage()
	defer storage.Release()

	// Add more than linkBatchSize (1024) links to force multiple record batches
	numLinks := 2500
	for i := 0; i < numLinks; i++ {
		link := &span.SpanLink{
			SpanID:        "test-span",
			LinkedTraceID:  "trace",
			LinkedSpanID:  "linked",
		}
		if err := storage.AddLink(link); err != nil {
			t.Fatalf("Failed to add link %d: %v", i, err)
		}
	}

	// Flush remaining links in builder to records
	if err := storage.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Verify all links are indexed and retrievable
	result, err := storage.GetLinksBySpanID("test-span")
	if err != nil {
		t.Fatalf("GetLinksBySpanID failed: %v", err)
	}

	if len(result) != numLinks {
		t.Errorf("Expected %d links, got %d", numLinks, len(result))
	}

	// Verify multiple record batches were created
	if storage.RecordCount() <= 1 {
		t.Errorf("Expected multiple record batches, got %d", storage.RecordCount())
	}
}
