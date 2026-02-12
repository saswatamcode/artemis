package wal

import (
	"fmt"
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/span"
)

func TestWAL_WriteAndReadEvents(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWAL(tmpDir, nil)
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}
	defer w.Close()

	// Write a span event
	evt := &span.SpanEvent{
		SpanID:    "span-1",
		Name:      "cache.hit",
		Timestamp: time.Now(),
		Attributes: map[string]string{
			"cache.key": "user:123",
			"ttl":       "3600",
		},
	}

	_, err = w.WriteEvent(evt)
	if err != nil {
		t.Fatalf("WriteEvent() error = %v", err)
	}

	// Close and reopen to read
	w.Close()

	reader := NewReader(tmpDir, nil)
	events := []*span.SpanEvent{}
	err = reader.ReadAllWithEvents(
		func(s *span.Span) error {
			t.Error("Unexpected span in event-only WAL")
			return nil
		},
		func(e *span.SpanEvent) error {
			events = append(events, e)
			return nil
		},
	)

	if err != nil {
		t.Fatalf("ReadAllWithEvents() error = %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("ReadAllWithEvents() returned %d events, want 1", len(events))
	}

	if events[0].SpanID != "span-1" {
		t.Errorf("Event SpanID = %s, want span-1", events[0].SpanID)
	}

	if events[0].Name != "cache.hit" {
		t.Errorf("Event Name = %s, want cache.hit", events[0].Name)
	}

	if events[0].Attributes["cache.key"] != "user:123" {
		t.Errorf("Event attribute cache.key = %s, want user:123", events[0].Attributes["cache.key"])
	}
}

func TestWAL_WriteMultipleEvents(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWAL(tmpDir, nil)
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}

	// Write 100 events
	for i := range 100 {
		evt := &span.SpanEvent{
			SpanID:    fmt.Sprintf("span-%d", i%10), // 10 different spans
			Name:      fmt.Sprintf("event-%d", i),
			Timestamp: time.Now(),
			Attributes: map[string]string{
				"index": fmt.Sprintf("%d", i),
			},
		}
		if _, err := w.WriteEvent(evt); err != nil {
			t.Fatalf("WriteEvent() error = %v", err)
		}
	}

	w.Close()

	// Read back all events
	reader := NewReader(tmpDir, nil)
	count := 0
	err = reader.ReadAllWithEvents(
		nil,
		func(e *span.SpanEvent) error {
			count++
			return nil
		},
	)

	if err != nil {
		t.Fatalf("ReadAllWithEvents() error = %v", err)
	}

	if count != 100 {
		t.Errorf("Replayed %d events, want 100", count)
	}
}

func TestWAL_WriteEventsBatch(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWAL(tmpDir, nil)
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}

	// Create batch of events
	events := make([]*span.SpanEvent, 50)
	for i := range 50 {
		events[i] = &span.SpanEvent{
			SpanID:    "span-1",
			Name:      fmt.Sprintf("event-%d", i),
			Timestamp: time.Now(),
			Attributes: map[string]string{
				"batch": "true",
				"index": fmt.Sprintf("%d", i),
			},
		}
	}

	// Write batch
	_, err = w.WriteEvents(events)
	if err != nil {
		t.Fatalf("WriteEvents() error = %v", err)
	}

	w.Close()

	// Read back
	reader := NewReader(tmpDir, nil)
	count := 0
	err = reader.ReadAllWithEvents(
		nil,
		func(e *span.SpanEvent) error {
			count++
			if e.Attributes["batch"] != "true" {
				t.Errorf("Event missing batch attribute")
			}
			return nil
		},
	)

	if err != nil {
		t.Fatalf("ReadAllWithEvents() error = %v", err)
	}

	if count != 50 {
		t.Errorf("Replayed %d events, want 50", count)
	}
}

func TestWAL_MixedSpansAndEvents(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWAL(tmpDir, nil)
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}

	// Write interleaved spans and events
	for i := range 10 {
		// Write a span
		sp := &span.Span{
			TraceID:     "trace-1",
			SpanID:      fmt.Sprintf("span-%d", i),
			Name:        "operation",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		if _, err := w.WriteSpan(sp); err != nil {
			t.Fatalf("WriteSpan() error = %v", err)
		}

		// Write events for this span
		for j := range 3 {
			evt := &span.SpanEvent{
				SpanID:    fmt.Sprintf("span-%d", i),
				Name:      fmt.Sprintf("event-%d", j),
				Timestamp: time.Now(),
				Attributes: map[string]string{
					"span_index":  fmt.Sprintf("%d", i),
					"event_index": fmt.Sprintf("%d", j),
				},
			}
			if _, err := w.WriteEvent(evt); err != nil {
				t.Fatalf("WriteEvent() error = %v", err)
			}
		}
	}

	w.Close()

	// Read back both spans and events
	reader := NewReader(tmpDir, nil)
	spanCount := 0
	eventCount := 0
	eventsBySpan := make(map[string]int)

	err = reader.ReadAllWithEvents(
		func(s *span.Span) error {
			spanCount++
			return nil
		},
		func(e *span.SpanEvent) error {
			eventCount++
			eventsBySpan[e.SpanID]++
			return nil
		},
	)

	if err != nil {
		t.Fatalf("ReadAllWithEvents() error = %v", err)
	}

	if spanCount != 10 {
		t.Errorf("Replayed %d spans, want 10", spanCount)
	}

	if eventCount != 30 {
		t.Errorf("Replayed %d events, want 30 (10 spans × 3 events)", eventCount)
	}

	// Verify each span has exactly 3 events
	for i := range 10 {
		spanID := fmt.Sprintf("span-%d", i)
		if eventsBySpan[spanID] != 3 {
			t.Errorf("Span %s has %d events, want 3", spanID, eventsBySpan[spanID])
		}
	}
}

func TestWAL_EventSegmentRotation(t *testing.T) {
	tmpDir := t.TempDir()

	// Create WAL with small segment size to force rotation
	w, err := NewWALWithSegmentSize(tmpDir, 1024, nil) // 1KB segments
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}

	// Write many events to trigger rotation
	for i := range 500 {
		evt := &span.SpanEvent{
			SpanID:    fmt.Sprintf("span-%d", i),
			Name:      "event-with-long-name-to-increase-size",
			Timestamp: time.Now(),
			Attributes: map[string]string{
				"key1": "value-with-long-content-to-increase-size",
				"key2": "another-value-with-long-content",
				"key3": "yet-another-long-value",
			},
		}
		if _, err := w.WriteEvent(evt); err != nil {
			t.Fatalf("WriteEvent() error = %v", err)
		}
	}

	segmentIndex := w.SegmentIndex()
	w.Close()

	// Should have created multiple segments
	if segmentIndex == 0 {
		t.Error("Expected segment rotation but stayed on segment 0")
	}

	t.Logf("Created %d segments for events", segmentIndex+1)

	// Verify all events are readable
	reader := NewReader(tmpDir, nil)
	count := 0
	err = reader.ReadAllWithEvents(
		nil,
		func(e *span.SpanEvent) error {
			count++
			return nil
		},
	)

	if err != nil {
		t.Fatalf("ReadAllWithEvents() error = %v", err)
	}

	if count != 500 {
		t.Errorf("Replayed %d events after rotation, want 500", count)
	}
}

func TestWAL_EmptyEventsArray(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWAL(tmpDir, nil)
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}

	// Write empty events array - should return current segment and not error
	segmentIndex, err := w.WriteEvents([]*span.SpanEvent{})
	if err != nil {
		t.Errorf("WriteEvents([]) error = %v, want nil", err)
	}

	if segmentIndex != 0 {
		t.Logf("WriteEvents([]) returned segment index %d", segmentIndex)
	}

	w.Close()
}

func BenchmarkWAL_WriteEvent(b *testing.B) {
	tmpDir := b.TempDir()

	w, err := NewWAL(tmpDir, nil)
	if err != nil {
		b.Fatalf("NewWAL() error = %v", err)
	}
	defer w.Close()

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
		w.WriteEvent(evt) // Ignore errors for benchmark
	}
}

func BenchmarkWAL_WriteEvents_Batch10(b *testing.B) {
	tmpDir := b.TempDir()

	w, err := NewWAL(tmpDir, nil)
	if err != nil {
		b.Fatalf("NewWAL() error = %v", err)
	}
	defer w.Close()

	events := make([]*span.SpanEvent, 10)
	for i := range 10 {
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
		w.WriteEvents(events) // Ignore errors for benchmark
	}
}
