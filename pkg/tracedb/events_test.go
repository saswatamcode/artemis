package tracedb

import (
	"fmt"
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/query"
	"github.com/saswatamcode/artemis/pkg/span"
)

func TestDB_WriteAndReadEvents(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WALDir = t.TempDir()

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// Write a span first
	sp := &span.Span{
		TraceID:     "trace-1",
		SpanID:      "span-1",
		Name:        "operation",
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(time.Millisecond),
		ServiceName: "service",
	}

	if err := db.WriteSpan(sp); err != nil {
		t.Fatalf("WriteSpan() error = %v", err)
	}

	// Write events for the span
	evt := &span.SpanEvent{
		SpanID:    "span-1",
		Name:      "cache.hit",
		Timestamp: time.Now(),
		Attributes: map[string]string{
			"cache.key": "user:123",
			"ttl":       "3600",
		},
	}

	if err := db.WriteEvent(evt); err != nil {
		t.Fatalf("WriteEvent() error = %v", err)
	}

	// Read events back
	events, err := db.GetEventsForSpan("span-1")
	if err != nil {
		t.Fatalf("GetEventsForSpan() error = %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("GetEventsForSpan() returned %d events, want 1", len(events))
	}

	if events[0].Name != "cache.hit" {
		t.Errorf("Event name = %s, want cache.hit", events[0].Name)
	}

	if events[0].Attributes["cache.key"] != "user:123" {
		t.Errorf("Event attribute cache.key = %s, want user:123", events[0].Attributes["cache.key"])
	}
}

func TestDB_WriteEventsBatch(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WALDir = t.TempDir()

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// Write a span
	sp := &span.Span{
		TraceID:     "trace-1",
		SpanID:      "span-1",
		Name:        "operation",
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(time.Millisecond),
		ServiceName: "service",
	}

	if err := db.WriteSpan(sp); err != nil {
		t.Fatalf("WriteSpan() error = %v", err)
	}

	// Write batch of events
	events := make([]*span.SpanEvent, 10)
	for i := range 10 {
		events[i] = &span.SpanEvent{
			SpanID:    "span-1",
			Name:      fmt.Sprintf("event-%d", i),
			Timestamp: time.Now().Add(time.Duration(i) * time.Millisecond),
			Attributes: map[string]string{
				"index": fmt.Sprintf("%d", i),
			},
		}
	}

	if err := db.WriteEvents(events); err != nil {
		t.Fatalf("WriteEvents() error = %v", err)
	}

	// Read events back
	readEvents, err := db.GetEventsForSpan("span-1")
	if err != nil {
		t.Fatalf("GetEventsForSpan() error = %v", err)
	}

	if len(readEvents) != 10 {
		t.Errorf("GetEventsForSpan() returned %d events, want 10", len(readEvents))
	}
}

func TestDB_GetEventsForTrace(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WALDir = t.TempDir()

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// Write multiple spans in same trace
	for i := range 5 {
		sp := &span.Span{
			TraceID:     "trace-1",
			SpanID:      fmt.Sprintf("span-%d", i),
			Name:        "operation",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		if err := db.WriteSpan(sp); err != nil {
			t.Fatalf("WriteSpan() error = %v", err)
		}

		// Write events for each span
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
			if err := db.WriteEvent(evt); err != nil {
				t.Fatalf("WriteEvent() error = %v", err)
			}
		}
	}

	// Get all events for the trace
	eventsMap, err := db.GetEventsForTrace("trace-1")
	if err != nil {
		t.Fatalf("GetEventsForTrace() error = %v", err)
	}

	// Should have events for 5 spans
	if len(eventsMap) != 5 {
		t.Errorf("GetEventsForTrace() returned events for %d spans, want 5", len(eventsMap))
	}

	// Each span should have 3 events
	for i := range 5 {
		spanID := fmt.Sprintf("span-%d", i)
		events, exists := eventsMap[spanID]
		if !exists {
			t.Errorf("No events found for %s", spanID)
			continue
		}
		if len(events) != 3 {
			t.Errorf("%s has %d events, want 3", spanID, len(events))
		}
	}
}

func TestDB_QuerySpansWithEvents(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WALDir = t.TempDir()

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// Write spans with events
	for i := range 3 {
		sp := &span.Span{
			TraceID:     "trace-1",
			SpanID:      fmt.Sprintf("span-%d", i),
			Name:        "operation",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		if err := db.WriteSpan(sp); err != nil {
			t.Fatalf("WriteSpan() error = %v", err)
		}

		// Write events
		for j := range 2 {
			evt := &span.SpanEvent{
				SpanID:    fmt.Sprintf("span-%d", i),
				Name:      fmt.Sprintf("event-%d", j),
				Timestamp: time.Now(),
			}
			if err := db.WriteEvent(evt); err != nil {
				t.Fatalf("WriteEvent() error = %v", err)
			}
		}
	}

	// Query spans without events
	matcher, _ := query.NewMatcher(query.MatchEqual, "trace_id", "trace-1")
	spansWithoutEvents, err := db.QuerySpansWithEvents(false, matcher)
	if err != nil {
		t.Fatalf("QuerySpansWithEvents(false) error = %v", err)
	}

	if len(spansWithoutEvents) != 3 {
		t.Errorf("QuerySpansWithEvents(false) returned %d spans, want 3", len(spansWithoutEvents))
	}

	// Events should be nil
	for _, sp := range spansWithoutEvents {
		if sp.Events != nil && len(sp.Events) > 0 {
			t.Errorf("Span %s has %d events when includeEvents=false, want 0", sp.SpanID, len(sp.Events))
		}
	}

	// Query spans with events
	spansWithEvents, err := db.QuerySpansWithEvents(true, matcher)
	if err != nil {
		t.Fatalf("QuerySpansWithEvents(true) error = %v", err)
	}

	if len(spansWithEvents) != 3 {
		t.Errorf("QuerySpansWithEvents(true) returned %d spans, want 3", len(spansWithEvents))
	}

	// Events should be populated
	for _, sp := range spansWithEvents {
		if len(sp.Events) != 2 {
			t.Errorf("Span %s has %d events, want 2", sp.SpanID, len(sp.Events))
		}
	}
}

func TestDB_EventsWithPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.WALDir = tmpDir + "/wal"
	cfg.BlockConfig = &block.Config{
		Dir:              tmpDir + "/blocks",
		MaxBlockDuration: 1 * time.Hour,
		MaxBlockSpans:    100,
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Write spans and events
	for i := range 5 {
		sp := &span.Span{
			TraceID:     "trace-1",
			SpanID:      fmt.Sprintf("span-%d", i),
			Name:        "operation",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		if err := db.WriteSpan(sp); err != nil {
			t.Fatalf("WriteSpan() error = %v", err)
		}

		// Write events
		evt := &span.SpanEvent{
			SpanID:    fmt.Sprintf("span-%d", i),
			Name:      "test-event",
			Timestamp: time.Now(),
			Attributes: map[string]string{
				"persistent": "true",
			},
		}
		if err := db.WriteEvent(evt); err != nil {
			t.Fatalf("WriteEvent() error = %v", err)
		}
	}

	// Flush to disk
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Force head block flush
	if err := db.flushHeadBlock(); err != nil {
		t.Fatalf("flushHeadBlock() error = %v", err)
	}

	// Close and reopen database
	db.Close()

	db2, err := New(cfg)
	if err != nil {
		t.Fatalf("New() after restart error = %v", err)
	}
	defer db2.Close()

	// Events should be recoverable
	// Note: Events may appear twice - once from WAL replay and once from persisted block
	// This is expected until checkpoint cleanup runs (same as spans)
	for i := range 5 {
		spanID := fmt.Sprintf("span-%d", i)
		events, err := db2.GetEventsForSpan(spanID)
		if err != nil {
			t.Fatalf("GetEventsForSpan(%s) after restart error = %v", spanID, err)
		}

		// Should have at least 1 event (may have 2 before checkpoint: WAL + persisted)
		if len(events) < 1 {
			t.Errorf("After restart, span %s has %d events, want at least 1", spanID, len(events))
		}

		// Verify event attributes are preserved
		foundPersistent := false
		for _, evt := range events {
			if evt.Attributes["persistent"] == "true" {
				foundPersistent = true
				break
			}
		}
		if !foundPersistent {
			t.Errorf("Event attribute 'persistent' lost after restart")
		}
	}
}

func TestDB_EventsAcrossMultipleSpans(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WALDir = t.TempDir()

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// Write 100 spans, each with random number of events
	spanEventCount := make(map[string]int)
	for i := range 100 {
		sp := &span.Span{
			TraceID:     fmt.Sprintf("trace-%d", i%10), // 10 different traces
			SpanID:      fmt.Sprintf("span-%d", i),
			Name:        "operation",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		if err := db.WriteSpan(sp); err != nil {
			t.Fatalf("WriteSpan() error = %v", err)
		}

		// Random number of events (0-5)
		eventCount := i % 6
		spanEventCount[sp.SpanID] = eventCount

		for j := range eventCount {
			evt := &span.SpanEvent{
				SpanID:    sp.SpanID,
				Name:      fmt.Sprintf("event-%d", j),
				Timestamp: time.Now(),
			}
			if err := db.WriteEvent(evt); err != nil {
				t.Fatalf("WriteEvent() error = %v", err)
			}
		}
	}

	// Verify each span has correct number of events
	for spanID, expectedCount := range spanEventCount {
		events, err := db.GetEventsForSpan(spanID)
		if err != nil {
			t.Fatalf("GetEventsForSpan(%s) error = %v", spanID, err)
		}

		if len(events) != expectedCount {
			t.Errorf("Span %s has %d events, want %d", spanID, len(events), expectedCount)
		}
	}
}

func TestDB_EmptyEventsQuery(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WALDir = t.TempDir()

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// Write span without events
	sp := &span.Span{
		TraceID:     "trace-1",
		SpanID:      "span-1",
		Name:        "operation",
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(time.Millisecond),
		ServiceName: "service",
	}

	if err := db.WriteSpan(sp); err != nil {
		t.Fatalf("WriteSpan() error = %v", err)
	}

	// Query events for span without events
	events, err := db.GetEventsForSpan("span-1")
	if err != nil {
		t.Fatalf("GetEventsForSpan() error = %v", err)
	}

	if len(events) != 0 {
		t.Errorf("GetEventsForSpan() for span without events returned %d events, want 0", len(events))
	}

	// Query events for non-existent span
	events2, err := db.GetEventsForSpan("span-999")
	if err != nil {
		t.Fatalf("GetEventsForSpan(non-existent) error = %v", err)
	}

	if len(events2) != 0 {
		t.Errorf("GetEventsForSpan(non-existent) returned %d events, want 0", len(events2))
	}
}

func BenchmarkDB_WriteEvent(b *testing.B) {
	cfg := DefaultConfig()
	cfg.WALDir = b.TempDir()

	db, err := New(cfg)
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}
	defer db.Close()

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
		db.WriteEvent(evt)
	}
}

func BenchmarkDB_WriteEvents_Batch100(b *testing.B) {
	cfg := DefaultConfig()
	cfg.WALDir = b.TempDir()

	db, err := New(cfg)
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	events := make([]*span.SpanEvent, 100)
	for i := range 100 {
		events[i] = &span.SpanEvent{
			SpanID:    "span-1",
			Name:      fmt.Sprintf("event-%d", i),
			Timestamp: time.Now(),
			Attributes: map[string]string{
				"index": fmt.Sprintf("%d", i),
			},
		}
	}

	b.ResetTimer()
	for b.Loop() {
		db.WriteEvents(events)
	}
}

func BenchmarkDB_GetEventsForSpan(b *testing.B) {
	cfg := DefaultConfig()
	cfg.WALDir = b.TempDir()

	db, err := New(cfg)
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// Populate with events
	for i := range 100 {
		evt := &span.SpanEvent{
			SpanID:    "span-1",
			Name:      fmt.Sprintf("event-%d", i),
			Timestamp: time.Now(),
		}
		db.WriteEvent(evt)
	}

	db.Flush()

	b.ResetTimer()
	for b.Loop() {
		db.GetEventsForSpan("span-1")
	}
}
