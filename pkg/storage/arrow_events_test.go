package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/span"
)

func TestArrowEventStorage_AddAndQuery(t *testing.T) {
	storage := NewArrowEventStorage()
	defer storage.Release()

	evt := &span.SpanEvent{
		SpanID:    "span-1",
		Name:      "cache.hit",
		Timestamp: time.Now(),
		Attributes: map[string]string{
			"cache.key": "user:123",
			"ttl":       "3600",
		},
	}

	err := storage.AddEvent(evt)
	if err != nil {
		t.Fatalf("AddEvent() error = %v", err)
	}

	// Flush to materialize record
	if err := storage.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Query by span ID
	events, err := storage.GetEventsBySpanID("span-1")
	if err != nil {
		t.Fatalf("GetEventsBySpanID() error = %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("GetEventsBySpanID() returned %d events, want 1", len(events))
	}

	if events[0].Name != "cache.hit" {
		t.Errorf("Event name = %s, want cache.hit", events[0].Name)
	}

	if events[0].Attributes["cache.key"] != "user:123" {
		t.Errorf("Event attribute cache.key = %s, want user:123", events[0].Attributes["cache.key"])
	}
}

func TestArrowEventStorage_AddMultipleEvents(t *testing.T) {
	storage := NewArrowEventStorage()
	defer storage.Release()

	// Add multiple events for same span
	for i := range 5 {
		evt := &span.SpanEvent{
			SpanID:    "span-1",
			Name:      fmt.Sprintf("event-%d", i),
			Timestamp: time.Now().Add(time.Duration(i) * time.Millisecond),
			Attributes: map[string]string{
				"index": fmt.Sprintf("%d", i),
			},
		}
		if err := storage.AddEvent(evt); err != nil {
			t.Fatalf("AddEvent() error = %v", err)
		}
	}

	// Add events for different span
	for i := range 3 {
		evt := &span.SpanEvent{
			SpanID:    "span-2",
			Name:      fmt.Sprintf("event-%d", i),
			Timestamp: time.Now(),
			Attributes: map[string]string{
				"span": "2",
			},
		}
		if err := storage.AddEvent(evt); err != nil {
			t.Fatalf("AddEvent() error = %v", err)
		}
	}

	if err := storage.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Query span-1 events
	events1, err := storage.GetEventsBySpanID("span-1")
	if err != nil {
		t.Fatalf("GetEventsBySpanID(span-1) error = %v", err)
	}

	if len(events1) != 5 {
		t.Errorf("span-1 has %d events, want 5", len(events1))
	}

	// Query span-2 events
	events2, err := storage.GetEventsBySpanID("span-2")
	if err != nil {
		t.Fatalf("GetEventsBySpanID(span-2) error = %v", err)
	}

	if len(events2) != 3 {
		t.Errorf("span-2 has %d events, want 3", len(events2))
	}

	// Query non-existent span
	events3, err := storage.GetEventsBySpanID("span-999")
	if err != nil {
		t.Fatalf("GetEventsBySpanID(span-999) error = %v", err)
	}

	if len(events3) != 0 {
		t.Errorf("span-999 has %d events, want 0", len(events3))
	}
}

func TestArrowEventStorage_AddEventsBatch(t *testing.T) {
	storage := NewArrowEventStorage()
	defer storage.Release()

	// Create batch of events
	events := make([]*span.SpanEvent, 100)
	for i := range 100 {
		events[i] = &span.SpanEvent{
			SpanID:    fmt.Sprintf("span-%d", i%10), // 10 different spans
			Name:      fmt.Sprintf("event-%d", i),
			Timestamp: time.Now(),
			Attributes: map[string]string{
				"batch": "true",
			},
		}
	}

	err := storage.AddEvents(events)
	if err != nil {
		t.Fatalf("AddEvents() error = %v", err)
	}

	if err := storage.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Verify row count
	if storage.RowCount() != 100 {
		t.Errorf("RowCount() = %d, want 100", storage.RowCount())
	}

	// Each span should have 10 events
	for i := range 10 {
		spanID := fmt.Sprintf("span-%d", i)
		events, err := storage.GetEventsBySpanID(spanID)
		if err != nil {
			t.Fatalf("GetEventsBySpanID(%s) error = %v", spanID, err)
		}
		if len(events) != 10 {
			t.Errorf("%s has %d events, want 10", spanID, len(events))
		}
	}
}

func TestArrowEventStorage_Reset(t *testing.T) {
	storage := NewArrowEventStorage()
	defer storage.Release()

	// Add some events
	for i := range 10 {
		evt := &span.SpanEvent{
			SpanID:    "span-1",
			Name:      fmt.Sprintf("event-%d", i),
			Timestamp: time.Now(),
		}
		storage.AddEvent(evt)
	}

	storage.Flush()

	if storage.RowCount() != 10 {
		t.Errorf("Before reset: RowCount() = %d, want 10", storage.RowCount())
	}

	// Reset storage
	storage.Reset()

	if storage.RowCount() != 0 {
		t.Errorf("After reset: RowCount() = %d, want 0", storage.RowCount())
	}

	if storage.RecordCount() != 0 {
		t.Errorf("After reset: RecordCount() = %d, want 0", storage.RecordCount())
	}

	// Should be able to add new events after reset
	evt := &span.SpanEvent{
		SpanID:    "span-2",
		Name:      "new-event",
		Timestamp: time.Now(),
	}
	if err := storage.AddEvent(evt); err != nil {
		t.Fatalf("AddEvent() after reset error = %v", err)
	}

	storage.Flush()

	if storage.RowCount() != 1 {
		t.Errorf("After reset and add: RowCount() = %d, want 1", storage.RowCount())
	}
}

func TestArrowEventStorage_FlushMultipleTimes(t *testing.T) {
	storage := NewArrowEventStorage()
	defer storage.Release()

	// Add and flush in batches
	for batch := range 3 {
		for i := range 10 {
			evt := &span.SpanEvent{
				SpanID:    "span-1",
				Name:      fmt.Sprintf("batch-%d-event-%d", batch, i),
				Timestamp: time.Now(),
			}
			storage.AddEvent(evt)
		}

		if err := storage.Flush(); err != nil {
			t.Fatalf("Flush() batch %d error = %v", batch, err)
		}
	}

	// Should have 3 record batches
	if storage.RecordCount() != 3 {
		t.Errorf("RecordCount() = %d, want 3", storage.RecordCount())
	}

	// Should have 30 total events
	if storage.RowCount() != 30 {
		t.Errorf("RowCount() = %d, want 30", storage.RowCount())
	}

	// All events should be queryable
	events, err := storage.GetEventsBySpanID("span-1")
	if err != nil {
		t.Fatalf("GetEventsBySpanID() error = %v", err)
	}

	if len(events) != 30 {
		t.Errorf("GetEventsBySpanID() returned %d events, want 30", len(events))
	}
}

func TestArrowEventStorage_EmptyAttributes(t *testing.T) {
	storage := NewArrowEventStorage()
	defer storage.Release()

	// Event with no attributes
	evt := &span.SpanEvent{
		SpanID:     "span-1",
		Name:       "simple-event",
		Timestamp:  time.Now(),
		Attributes: nil,
	}

	err := storage.AddEvent(evt)
	if err != nil {
		t.Fatalf("AddEvent() with nil attributes error = %v", err)
	}

	storage.Flush()

	events, err := storage.GetEventsBySpanID("span-1")
	if err != nil {
		t.Fatalf("GetEventsBySpanID() error = %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("GetEventsBySpanID() returned %d events, want 1", len(events))
	}

	if events[0].Attributes == nil {
		events[0].Attributes = make(map[string]string)
	}

	if len(events[0].Attributes) != 0 {
		t.Errorf("Event has %d attributes, want 0", len(events[0].Attributes))
	}
}

func TestArrowEventStorage_GetRecordsAndSchema(t *testing.T) {
	storage := NewArrowEventStorage()
	defer storage.Release()

	// Add some events
	for i := range 5 {
		evt := &span.SpanEvent{
			SpanID:    "span-1",
			Name:      fmt.Sprintf("event-%d", i),
			Timestamp: time.Now(),
		}
		storage.AddEvent(evt)
	}

	storage.Flush()

	// Get records
	records := storage.GetRecords()
	if len(records) != 1 {
		t.Errorf("GetRecords() returned %d records, want 1", len(records))
	}

	if records[0].NumRows() != 5 {
		t.Errorf("Record has %d rows, want 5", records[0].NumRows())
	}

	// Get schema
	schema := storage.Schema()
	if schema == nil {
		t.Fatal("Schema() returned nil")
	}

	// Verify schema has expected fields
	expectedFields := []string{"span_id", "name", "timestamp", "attributes"}
	if schema.NumFields() != len(expectedFields) {
		t.Errorf("Schema has %d fields, want %d", schema.NumFields(), len(expectedFields))
	}
}

func BenchmarkArrowEventStorage_AddEvent(b *testing.B) {
	storage := NewArrowEventStorage()
	defer storage.Release()

	evt := &span.SpanEvent{
		SpanID:    "span-1",
		Name:      "cache.hit",
		Timestamp: time.Now(),
		Attributes: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}

	b.ResetTimer()
	for b.Loop() {
		storage.AddEvent(evt)
	}
}

func BenchmarkArrowEventStorage_AddEvents_Batch100(b *testing.B) {
	storage := NewArrowEventStorage()
	defer storage.Release()

	events := make([]*span.SpanEvent, 100)
	for i := range 100 {
		events[i] = &span.SpanEvent{
			SpanID:    "span-1",
			Name:      fmt.Sprintf("event-%d", i),
			Timestamp: time.Now(),
			Attributes: map[string]string{
				"key": "value",
			},
		}
	}

	b.ResetTimer()
	for b.Loop() {
		storage.AddEvents(events)
	}
}

func BenchmarkArrowEventStorage_GetEventsBySpanID(b *testing.B) {
	storage := NewArrowEventStorage()
	defer storage.Release()

	// Populate with events
	for i := range 1000 {
		evt := &span.SpanEvent{
			SpanID:    fmt.Sprintf("span-%d", i%100), // 100 different spans
			Name:      fmt.Sprintf("event-%d", i),
			Timestamp: time.Now(),
			Attributes: map[string]string{
				"index": fmt.Sprintf("%d", i),
			},
		}
		storage.AddEvent(evt)
	}
	storage.Flush()

	b.ResetTimer()
	for b.Loop() {
		storage.GetEventsBySpanID("span-50")
	}
}
