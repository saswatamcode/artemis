package storage

import (
	"fmt"
	"testing"

	"github.com/saswatamcode/artemis/pkg/span"
)

// BenchmarkGetLinksBySpanID_WithIndex benchmarks the new indexed link lookup
func BenchmarkGetLinksBySpanID_WithIndex(b *testing.B) {
	storage := NewArrowLinkStorage()
	defer storage.Release()

	// Add 10,000 links across 100 different spans
	numSpans := 100
	linksPerSpan := 100
	targetSpan := "target-span"

	// Add links for various spans
	for i := 0; i < numSpans; i++ {
		spanID := fmt.Sprintf("span-%d", i)
		for j := 0; j < linksPerSpan; j++ {
			link := &span.SpanLink{
				SpanID:        spanID,
				LinkedTraceID: fmt.Sprintf("trace-%d", j),
				LinkedSpanID:  fmt.Sprintf("linked-%d", j),
			}
			storage.AddLink(link)
		}
	}

	// Add links for target span
	for j := 0; j < linksPerSpan; j++ {
		link := &span.SpanLink{
			SpanID:        targetSpan,
			LinkedTraceID: fmt.Sprintf("trace-%d", j),
			LinkedSpanID:  fmt.Sprintf("linked-%d", j),
		}
		storage.AddLink(link)
	}

	// Flush to create records
	storage.Flush()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		links, err := storage.GetLinksBySpanID(targetSpan)
		if err != nil {
			b.Fatal(err)
		}
		if len(links) != linksPerSpan {
			b.Fatalf("Expected %d links, got %d", linksPerSpan, len(links))
		}
	}
}

// BenchmarkGetLinksBatch_WithIndex benchmarks batch link lookup with index
func BenchmarkGetLinksBatch_WithIndex(b *testing.B) {
	storage := NewArrowLinkStorage()
	defer storage.Release()

	// Add 10,000 links across 100 different spans
	numSpans := 100
	linksPerSpan := 100

	for i := 0; i < numSpans; i++ {
		spanID := fmt.Sprintf("span-%d", i)
		for j := 0; j < linksPerSpan; j++ {
			link := &span.SpanLink{
				SpanID:        spanID,
				LinkedTraceID: fmt.Sprintf("trace-%d", j),
				LinkedSpanID:  fmt.Sprintf("linked-%d", j),
			}
			storage.AddLink(link)
		}
	}

	storage.Flush()

	// Query for 10 spans
	querySpans := make([]string, 10)
	for i := 0; i < 10; i++ {
		querySpans[i] = fmt.Sprintf("span-%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		results, err := storage.GetLinksBatch(querySpans)
		if err != nil {
			b.Fatal(err)
		}
		if len(results) != 10 {
			b.Fatalf("Expected 10 results, got %d", len(results))
		}
	}
}

// BenchmarkAddLink benchmarks link insertion with index building
func BenchmarkAddLink(b *testing.B) {
	storage := NewArrowLinkStorage()
	defer storage.Release()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		link := &span.SpanLink{
			SpanID:        fmt.Sprintf("span-%d", i%100),
			LinkedTraceID: fmt.Sprintf("trace-%d", i),
			LinkedSpanID:  fmt.Sprintf("linked-%d", i),
			Attributes:    map[string]string{"key": "value"},
		}
		storage.AddLink(link)
	}
}

// BenchmarkAddLinks_Batch benchmarks batch link insertion
func BenchmarkAddLinks_Batch(b *testing.B) {
	storage := NewArrowLinkStorage()
	defer storage.Release()

	// Create batch of 100 links
	links := make([]*span.SpanLink, 100)
	for i := 0; i < 100; i++ {
		links[i] = &span.SpanLink{
			SpanID:        fmt.Sprintf("span-%d", i),
			LinkedTraceID: fmt.Sprintf("trace-%d", i),
			LinkedSpanID:  fmt.Sprintf("linked-%d", i),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		storage.AddLinks(links)
		if i%1000 == 0 {
			storage.Reset()  // Reset periodically to prevent memory growth
		}
	}
}

// BenchmarkLinkIndex_MultipleRecordBatches benchmarks index performance across multiple record batches
func BenchmarkLinkIndex_MultipleRecordBatches(b *testing.B) {
	storage := NewArrowLinkStorage()
	defer storage.Release()

	// Add 5000 links to create multiple record batches (batch size = 1024)
	targetSpan := "target-span"
	for i := 0; i < 5000; i++ {
		spanID := targetSpan
		if i%10 != 0 {
			spanID = fmt.Sprintf("span-%d", i)
		}

		link := &span.SpanLink{
			SpanID:        spanID,
			LinkedTraceID: fmt.Sprintf("trace-%d", i),
			LinkedSpanID:  fmt.Sprintf("linked-%d", i),
		}
		storage.AddLink(link)
	}

	storage.Flush()

	b.Logf("Record count: %d", storage.RecordCount())
	b.Logf("Row count: %d", storage.RowCount())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		links, err := storage.GetLinksBySpanID(targetSpan)
		if err != nil {
			b.Fatal(err)
		}
		// Should have ~500 links for target span (every 10th link)
		if len(links) < 400 || len(links) > 600 {
			b.Fatalf("Expected ~500 links, got %d", len(links))
		}
	}
}
