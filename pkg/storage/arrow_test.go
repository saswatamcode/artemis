package storage

import (
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/span"
)

func TestArrowStorage_AddSpan(t *testing.T) {
	storage := NewArrowStorage()
	defer storage.Release()

	sp := &span.Span{
		TraceID:     "trace-1",
		SpanID:      "span-1",
		Name:        "test-operation",
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(time.Millisecond),
		ServiceName: "test-service",
		Tags: map[string]string{
			"key1": "value1",
		},
	}

	err := storage.AddSpan(sp)
	if err != nil {
		t.Fatalf("AddSpan() error = %v", err)
	}

	if storage.RowCount() != 1 {
		t.Errorf("RowCount() = %d, want 1", storage.RowCount())
	}
}

func TestArrowStorage_AddMultipleSpans(t *testing.T) {
	storage := NewArrowStorage()
	defer storage.Release()

	for i := range 100 {
		sp := &span.Span{
			TraceID:     "trace-1",
			SpanID:      "span-" + string(rune(i)),
			Name:        "operation",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		if err := storage.AddSpan(sp); err != nil {
			t.Fatalf("AddSpan() error = %v", err)
		}
	}

	if storage.RowCount() != 100 {
		t.Errorf("RowCount() = %d, want 100", storage.RowCount())
	}
}

func TestArrowStorage_Flush(t *testing.T) {
	storage := NewArrowStorage()
	defer storage.Release()

	// Add a few spans (less than batch size)
	for i := range 5 {
		sp := &span.Span{
			SpanID:      "span-" + string(rune(i)),
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		storage.AddSpan(sp)
	}

	// Should have 0 records before flush (not enough for a batch)
	if storage.RecordCount() != 0 {
		t.Errorf("RecordCount() before flush = %d, want 0", storage.RecordCount())
	}

	// Flush
	err := storage.Flush()
	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Should have 1 record after flush
	if storage.RecordCount() != 1 {
		t.Errorf("RecordCount() after flush = %d, want 1", storage.RecordCount())
	}
}

func TestArrowStorage_BatchCreation(t *testing.T) {
	storage := NewArrowStorage()
	defer storage.Release()

	// Add exactly batch size spans
	for i := range batchSize {
		sp := &span.Span{
			SpanID:      "span-" + string(rune(i)),
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		storage.AddSpan(sp)
	}

	// Should automatically create a record
	if storage.RecordCount() != 1 {
		t.Errorf("RecordCount() = %d, want 1", storage.RecordCount())
	}

	// Add one more span
	sp := &span.Span{
		SpanID:      "span-extra",
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(time.Millisecond),
		ServiceName: "service",
	}
	storage.AddSpan(sp)

	// Should still be 1 record (new one not flushed yet)
	if storage.RecordCount() != 1 {
		t.Errorf("RecordCount() after extra span = %d, want 1", storage.RecordCount())
	}

	// Flush to create second record
	storage.Flush()
	if storage.RecordCount() != 2 {
		t.Errorf("RecordCount() after second flush = %d, want 2", storage.RecordCount())
	}
}

func TestArrowStorage_GetSpanByID(t *testing.T) {
	storage := NewArrowStorage()
	defer storage.Release()

	originalSpan := &span.Span{
		TraceID:      "trace-123",
		SpanID:       "span-456",
		ParentSpanID: "span-000",
		Name:         "http.request",
		StartTime:    time.Unix(0, 1000000),
		EndTime:      time.Unix(0, 2000000),
		Duration:     1000000,
		ServiceName:  "api-gateway",
		Tags: map[string]string{
			"http.method": "GET",
			"http.status": "200",
		},
	}

	storage.AddSpan(originalSpan)
	storage.Flush()

	// Retrieve the span
	retrievedSpan, err := storage.GetSpanByID("span-456")
	if err != nil {
		t.Fatalf("GetSpanByID() error = %v", err)
	}

	// Verify fields
	if retrievedSpan.TraceID != originalSpan.TraceID {
		t.Errorf("TraceID = %s, want %s", retrievedSpan.TraceID, originalSpan.TraceID)
	}
	if retrievedSpan.SpanID != originalSpan.SpanID {
		t.Errorf("SpanID = %s, want %s", retrievedSpan.SpanID, originalSpan.SpanID)
	}
	if retrievedSpan.ParentSpanID != originalSpan.ParentSpanID {
		t.Errorf("ParentSpanID = %s, want %s", retrievedSpan.ParentSpanID, originalSpan.ParentSpanID)
	}
	if retrievedSpan.Name != originalSpan.Name {
		t.Errorf("Name = %s, want %s", retrievedSpan.Name, originalSpan.Name)
	}
	if retrievedSpan.ServiceName != originalSpan.ServiceName {
		t.Errorf("ServiceName = %s, want %s", retrievedSpan.ServiceName, originalSpan.ServiceName)
	}
	if len(retrievedSpan.Tags) != len(originalSpan.Tags) {
		t.Errorf("Tags length = %d, want %d", len(retrievedSpan.Tags), len(originalSpan.Tags))
	}
	if retrievedSpan.Tags["http.method"] != "GET" {
		t.Errorf("Tags[http.method] = %s, want GET", retrievedSpan.Tags["http.method"])
	}
}

func TestArrowStorage_GetSpanByID_NotFound(t *testing.T) {
	storage := NewArrowStorage()
	defer storage.Release()

	_, err := storage.GetSpanByID("nonexistent")
	if err == nil {
		t.Error("GetSpanByID() should return error for nonexistent span")
	}
}

func TestArrowStorage_EmptyParentSpanID(t *testing.T) {
	storage := NewArrowStorage()
	defer storage.Release()

	sp := &span.Span{
		SpanID:       "span-root",
		ParentSpanID: "", // No parent (root span)
		Name:         "root",
		StartTime:    time.Now(),
		EndTime:      time.Now().Add(time.Millisecond),
		ServiceName:  "service",
	}

	storage.AddSpan(sp)
	storage.Flush()

	retrieved, err := storage.GetSpanByID("span-root")
	if err != nil {
		t.Fatalf("GetSpanByID() error = %v", err)
	}

	if retrieved.ParentSpanID != "" {
		t.Errorf("ParentSpanID = %s, want empty string", retrieved.ParentSpanID)
	}
}

func TestArrowStorage_EmptyTags(t *testing.T) {
	storage := NewArrowStorage()
	defer storage.Release()

	sp := &span.Span{
		SpanID:      "span-no-tags",
		Name:        "operation",
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(time.Millisecond),
		ServiceName: "service",
		Tags:        nil, // No tags
	}

	storage.AddSpan(sp)
	storage.Flush()

	retrieved, err := storage.GetSpanByID("span-no-tags")
	if err != nil {
		t.Fatalf("GetSpanByID() error = %v", err)
	}

	if len(retrieved.Tags) > 0 {
		t.Errorf("Tags should be nil or empty, got %v", retrieved.Tags)
	}
}

func TestArrowStorage_GetTimeRange(t *testing.T) {
	storage := NewArrowStorage()
	defer storage.Release()

	// Initially empty
	minTime, maxTime := storage.GetTimeRange()
	if minTime != 0 || maxTime != 0 {
		t.Errorf("Empty storage time range = [%d, %d], want [0, 0]", minTime, maxTime)
	}

	// Add spans with known times
	startTime1 := time.Unix(0, 1000000000) // 1 second
	startTime2 := time.Unix(0, 2000000000) // 2 seconds

	sp1 := &span.Span{
		SpanID:      "span-1",
		StartTime:   startTime1,
		EndTime:     startTime1.Add(time.Millisecond),
		ServiceName: "service",
	}
	sp2 := &span.Span{
		SpanID:      "span-2",
		StartTime:   startTime2,
		EndTime:     startTime2.Add(time.Millisecond),
		ServiceName: "service",
	}

	storage.AddSpan(sp1)
	storage.AddSpan(sp2)

	minTime, maxTime = storage.GetTimeRange()
	if minTime != startTime1.UnixNano() {
		t.Errorf("MinTime = %d, want %d", minTime, startTime1.UnixNano())
	}
	// MaxTime should be the end time of the last span
	expectedMaxTime := startTime2.Add(time.Millisecond).UnixNano()
	if maxTime != expectedMaxTime {
		t.Errorf("MaxTime = %d, want %d", maxTime, expectedMaxTime)
	}
}

func TestArrowStorage_Reset(t *testing.T) {
	storage := NewArrowStorage()
	defer storage.Release()

	// Add some spans
	for i := range 10 {
		sp := &span.Span{
			SpanID:      "span-" + string(rune(i)),
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		storage.AddSpan(sp)
	}
	storage.Flush()

	// Verify we have data
	if storage.RowCount() == 0 {
		t.Error("Storage should have spans before reset")
	}

	// Reset
	storage.Reset()

	// Verify everything is cleared
	if storage.RowCount() != 0 {
		t.Errorf("RowCount() after reset = %d, want 0", storage.RowCount())
	}
	if storage.RecordCount() != 0 {
		t.Errorf("RecordCount() after reset = %d, want 0", storage.RecordCount())
	}

	minTime, maxTime := storage.GetTimeRange()
	if minTime != 0 || maxTime != 0 {
		t.Errorf("Time range after reset = [%d, %d], want [0, 0]", minTime, maxTime)
	}
}

func TestArrowStorage_GetIndex(t *testing.T) {
	storage := NewArrowStorage()
	defer storage.Release()

	idx := storage.GetIndex()
	if idx == nil {
		t.Error("GetIndex() should not return nil")
	}

	// Add a span and verify index is updated
	sp := &span.Span{
		SpanID:      "span-indexed",
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(time.Millisecond),
		ServiceName: "service",
		Tags: map[string]string{
			"env": "prod",
		},
	}
	storage.AddSpan(sp)

	// Verify index has the span
	_, ok := idx.LookupSpanID("span-indexed")
	if !ok {
		t.Error("Index should contain the added span")
	}

	// Verify tag index
	spans := idx.LookupByTag("env", "prod")
	if len(spans) != 1 {
		t.Errorf("Tag index should have 1 span, got %d", len(spans))
	}
}

func TestArrowStorage_Schema(t *testing.T) {
	storage := NewArrowStorage()
	defer storage.Release()

	schema := storage.Schema()
	if schema == nil {
		t.Error("Schema() should not return nil")
	}

	// Verify schema has expected fields
	if schema.NumFields() != 9 {
		t.Errorf("Schema has %d fields, want 9", schema.NumFields())
	}

	// Check field names
	expectedFields := []string{
		"trace_id", "span_id", "parent_span_id", "name",
		"start_time", "end_time", "duration", "service_name", "tags",
	}

	for i, expected := range expectedFields {
		if schema.Field(i).Name != expected {
			t.Errorf("Field %d name = %s, want %s", i, schema.Field(i).Name, expected)
		}
	}
}

func TestArrowStorage_PrintStats(t *testing.T) {
	storage := NewArrowStorage()
	defer storage.Release()

	stats := storage.PrintStats()
	if stats == "" {
		t.Error("PrintStats() should not return empty string")
	}

	// Add spans and check stats update
	for i := range 5 {
		sp := &span.Span{
			SpanID:      "span-" + string(rune(i)),
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		storage.AddSpan(sp)
	}

	stats = storage.PrintStats()
	if stats == "" {
		t.Error("PrintStats() should not return empty string after adding spans")
	}
}

func BenchmarkArrowStorage_AddSpan(b *testing.B) {
	storage := NewArrowStorage()
	defer storage.Release()

	sp := &span.Span{
		TraceID:     "trace-1",
		SpanID:      "span-1",
		Name:        "operation",
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(time.Millisecond),
		ServiceName: "service",
		Tags: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}

	for i := 0; b.Loop(); i++ {
		storage.AddSpan(sp)
		if i%batchSize == 0 {
			storage.Flush()
		}
	}
}

func BenchmarkArrowStorage_GetSpanByID(b *testing.B) {
	storage := NewArrowStorage()
	defer storage.Release()

	// Add 1000 spans
	for i := range 1000 {
		sp := &span.Span{
			SpanID:      "span-" + string(rune(i)),
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		storage.AddSpan(sp)
	}
	storage.Flush()

	for b.Loop() {
		storage.GetSpanByID("span-500")
	}
}

func BenchmarkArrowStorage_AddSpans_Bulk(b *testing.B) {
	storage := NewArrowStorage()
	defer storage.Release()

	// Create a batch of spans
	batchSize := 100
	spans := make([]*span.Span, batchSize)
	for i := range batchSize {
		spans[i] = &span.Span{
			TraceID:     "trace-1",
			SpanID:      "span-" + string(rune(i)),
			Name:        "operation",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
			Tags: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		}
	}

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		storage.AddSpans(spans)
		if i%10 == 0 {
			storage.Flush()
		}
	}
}

func BenchmarkArrowStorage_AddSpan_vs_AddSpans(b *testing.B) {
	b.Run("AddSpan_Individual", func(b *testing.B) {
		storage := NewArrowStorage()
		defer storage.Release()

		sp := &span.Span{
			TraceID:     "trace-1",
			SpanID:      "span-1",
			Name:        "operation",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
			Tags: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			storage.AddSpan(sp)
			if i%batchSize == 0 {
				storage.Flush()
			}
		}
	})

	b.Run("AddSpans_Bulk_100", func(b *testing.B) {
		storage := NewArrowStorage()
		defer storage.Release()

		batchSize := 100
		spans := make([]*span.Span, batchSize)
		for i := range batchSize {
			spans[i] = &span.Span{
				TraceID:     "trace-1",
				SpanID:      "span-" + string(rune(i)),
				Name:        "operation",
				StartTime:   time.Now(),
				EndTime:     time.Now().Add(time.Millisecond),
				ServiceName: "service",
				Tags: map[string]string{
					"key1": "value1",
					"key2": "value2",
				},
			}
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			storage.AddSpans(spans)
			if i%10 == 0 {
				storage.Flush()
			}
		}
	})
}
