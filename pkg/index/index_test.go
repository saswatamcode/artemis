package index

import (
	"sync"
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/span"
)

func TestSymbolTable_InternAndResolve(t *testing.T) {
	st := NewSymbolTable()

	// Test interning new strings
	id1 := st.Intern("hello")
	id2 := st.Intern("world")
	id3 := st.Intern("hello") // Duplicate

	if id1 == 0 {
		t.Error("ID should not be 0")
	}
	if id2 == 0 {
		t.Error("ID should not be 0")
	}
	if id1 == id2 {
		t.Error("Different strings should have different IDs")
	}
	if id1 != id3 {
		t.Error("Same string should have same ID")
	}

	// Test resolving IDs
	if st.Resolve(id1) != "hello" {
		t.Errorf("Resolve(%d) = %s, want hello", id1, st.Resolve(id1))
	}
	if st.Resolve(id2) != "world" {
		t.Errorf("Resolve(%d) = %s, want world", id2, st.Resolve(id2))
	}

	// Test invalid ID
	if st.Resolve(999) != "" {
		t.Error("Resolving invalid ID should return empty string")
	}
}

func TestSymbolTable_Lookup(t *testing.T) {
	st := NewSymbolTable()

	// Lookup non-existent string
	if id := st.Lookup("nonexistent"); id != 0 {
		t.Errorf("Lookup(nonexistent) = %d, want 0", id)
	}

	// Intern and lookup
	id1 := st.Intern("test")
	if id2 := st.Lookup("test"); id1 != id2 {
		t.Errorf("Lookup() = %d, want %d", id2, id1)
	}
}

func TestSymbolTable_Size(t *testing.T) {
	st := NewSymbolTable()

	if st.Size() != 0 {
		t.Errorf("Size() = %d, want 0", st.Size())
	}

	st.Intern("a")
	st.Intern("b")
	st.Intern("a") // Duplicate

	if st.Size() != 2 {
		t.Errorf("Size() = %d, want 2", st.Size())
	}
}

func TestSymbolTable_Serialization(t *testing.T) {
	st := NewSymbolTable()
	st.Intern("key1")
	st.Intern("key2")
	st.Intern("value1")

	// Serialize
	m := st.SerializeToMap()
	if len(m) != 3 {
		t.Errorf("Serialized map length = %d, want 3", len(m))
	}

	// Deserialize
	st2 := NewSymbolTableFromMap(m)
	if st2.Size() != st.Size() {
		t.Errorf("Deserialized size = %d, want %d", st2.Size(), st.Size())
	}

	// Verify IDs match
	id1 := st.Lookup("key1")
	id2 := st2.Lookup("key1")
	if id1 != id2 {
		t.Errorf("Deserialized ID = %d, want %d", id2, id1)
	}

	// Add new string to deserialized table
	newID := st2.Intern("new")
	if newID == 0 {
		t.Error("New ID should not be 0")
	}
}

func TestSymbolTable_Concurrency(t *testing.T) {
	st := NewSymbolTable()
	var wg sync.WaitGroup

	// Concurrent writes
	for range 10 {
		wg.Go(func() {
			for range 100 {
				st.Intern("concurrent-test")
			}
		})
	}

	wg.Wait()

	// Should only have one entry
	if st.Size() != 1 {
		t.Errorf("Size() = %d, want 1", st.Size())
	}
}

func TestIndex_AddSpanAndLookup(t *testing.T) {
	idx := NewIndex()

	sp := &span.Span{
		TraceID:     "trace-1",
		SpanID:      "span-1",
		Name:        "operation",
		ServiceName: "service",
		Tags: map[string]string{
			"env":     "prod",
			"version": "1.0",
		},
	}

	idx.AddSpan(sp, 0, 0, nil)

	// Lookup by span ID
	ref, ok := idx.LookupSpanID("span-1")
	if !ok {
		t.Error("LookupSpanID() should find span")
	}
	if ref.RecordIndex != 0 || ref.RowIndex != 0 {
		t.Errorf("SpanRef = {%d, %d}, want {0, 0}", ref.RecordIndex, ref.RowIndex)
	}

	// Lookup non-existent span
	_, ok = idx.LookupSpanID("nonexistent")
	if ok {
		t.Error("LookupSpanID() should not find nonexistent span")
	}
}

func TestIndex_LookupByTraceID(t *testing.T) {
	idx := NewIndex()

	// Add multiple spans for the same trace
	sp1 := &span.Span{
		TraceID: "trace-1",
		SpanID:  "span-1",
	}
	sp2 := &span.Span{
		TraceID: "trace-1",
		SpanID:  "span-2",
	}
	sp3 := &span.Span{
		TraceID: "trace-2",
		SpanID:  "span-3",
	}

	idx.AddSpan(sp1, 0, 0, nil)
	idx.AddSpan(sp2, 0, 1, nil)
	idx.AddSpan(sp3, 1, 0, nil)

	// Lookup by trace ID
	trace1Spans := idx.LookupByTraceID("trace-1")
	if len(trace1Spans) != 2 {
		t.Errorf("LookupByTraceID(trace-1) returned %d spans, want 2", len(trace1Spans))
	}

	// Verify span IDs
	spanIDs := make(map[string]bool)
	for _, spanID := range trace1Spans {
		spanIDs[spanID] = true
	}
	if !spanIDs["span-1"] || !spanIDs["span-2"] {
		t.Error("Should find both span-1 and span-2 for trace-1")
	}

	// Lookup another trace
	trace2Spans := idx.LookupByTraceID("trace-2")
	if len(trace2Spans) != 1 {
		t.Errorf("LookupByTraceID(trace-2) returned %d spans, want 1", len(trace2Spans))
	}
	if trace2Spans[0] != "span-3" {
		t.Errorf("LookupByTraceID(trace-2) returned %s, want span-3", trace2Spans[0])
	}

	// Lookup non-existent trace
	noneSpans := idx.LookupByTraceID("nonexistent")
	if len(noneSpans) != 0 {
		t.Error("LookupByTraceID() should return empty slice for non-existent trace")
	}
}

func TestIndex_TraceIDSerialization(t *testing.T) {
	idx := NewIndex()

	// Add spans with trace IDs
	sp1 := &span.Span{
		TraceID: "trace-1",
		SpanID:  "span-1",
		Tags:    map[string]string{"env": "prod"},
	}
	sp2 := &span.Span{
		TraceID: "trace-1",
		SpanID:  "span-2",
		Tags:    map[string]string{"env": "prod"},
	}

	idx.AddSpan(sp1, 0, 0, nil)
	idx.AddSpan(sp2, 0, 1, nil)

	// Serialize
	serialized := idx.Serialize()
	if len(serialized.TraceIndex) != 1 {
		t.Errorf("Serialized trace index length = %d, want 1", len(serialized.TraceIndex))
	}

	// Deserialize
	idx2 := NewIndexFromSerialized(serialized)

	// Verify trace lookup works
	spans := idx2.LookupByTraceID("trace-1")
	if len(spans) != 2 {
		t.Errorf("Trace lookup after deserialization returned %d spans, want 2", len(spans))
	}
}

func TestIndex_LookupByTag(t *testing.T) {
	idx := NewIndex()

	sp1 := &span.Span{
		SpanID: "span-1",
		Tags: map[string]string{
			"env": "prod",
		},
	}
	sp2 := &span.Span{
		SpanID: "span-2",
		Tags: map[string]string{
			"env": "prod",
		},
	}
	sp3 := &span.Span{
		SpanID: "span-3",
		Tags: map[string]string{
			"env": "dev",
		},
	}

	idx.AddSpan(sp1, 0, 0, nil)
	idx.AddSpan(sp2, 0, 1, nil)
	idx.AddSpan(sp3, 0, 2, nil)

	// Lookup by tag
	prodSpans := idx.LookupByTag("env", "prod")
	if len(prodSpans) != 2 {
		t.Errorf("LookupByTag(env, prod) returned %d spans, want 2", len(prodSpans))
	}

	devSpans := idx.LookupByTag("env", "dev")
	if len(devSpans) != 1 {
		t.Errorf("LookupByTag(env, dev) returned %d spans, want 1", len(devSpans))
	}

	// Lookup non-existent tag
	noneSpans := idx.LookupByTag("nonexistent", "value")
	if noneSpans != nil {
		t.Error("LookupByTag() should return nil for non-existent tag")
	}
}

func TestIndex_Stats(t *testing.T) {
	idx := NewIndex()

	// Empty index
	stats := idx.Stats()
	if stats.TotalSpans != 0 {
		t.Errorf("TotalSpans = %d, want 0", stats.TotalSpans)
	}

	// Add spans
	sp1 := &span.Span{
		SpanID: "span-1",
		Tags: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}
	sp2 := &span.Span{
		SpanID: "span-2",
		Tags: map[string]string{
			"key1": "value3",
		},
	}

	idx.AddSpan(sp1, 0, 0, nil)
	idx.AddSpan(sp2, 0, 1, nil)

	stats = idx.Stats()
	if stats.TotalSpans != 2 {
		t.Errorf("TotalSpans = %d, want 2", stats.TotalSpans)
	}
	if stats.UniqueTagKeys != 2 {
		t.Errorf("UniqueTagKeys = %d, want 2", stats.UniqueTagKeys)
	}
	if stats.UniqueTagValues != 3 {
		t.Errorf("UniqueTagValues = %d, want 3", stats.UniqueTagValues)
	}
}

func TestIndex_Clear(t *testing.T) {
	idx := NewIndex()

	sp := &span.Span{
		SpanID: "span-1",
		Tags: map[string]string{
			"key": "value",
		},
	}

	idx.AddSpan(sp, 0, 0, nil)

	stats := idx.Stats()
	if stats.TotalSpans == 0 {
		t.Error("Index should have spans before clear")
	}

	idx.Clear()

	stats = idx.Stats()
	if stats.TotalSpans != 0 {
		t.Errorf("TotalSpans after clear = %d, want 0", stats.TotalSpans)
	}

	// Verify span lookup fails
	_, ok := idx.LookupSpanID("span-1")
	if ok {
		t.Error("Should not find span after clear")
	}
}

func TestIndex_Serialization(t *testing.T) {
	idx := NewIndex()

	sp := &span.Span{
		SpanID: "span-1",
		Tags: map[string]string{
			"env": "prod",
		},
	}
	idx.AddSpan(sp, 5, 10, nil)

	// Serialize
	serialized := idx.Serialize()
	if len(serialized.SpanIndex) != 1 {
		t.Errorf("Serialized span index length = %d, want 1", len(serialized.SpanIndex))
	}

	// Deserialize
	idx2 := NewIndexFromSerialized(serialized)

	// Verify span lookup works
	ref, ok := idx2.LookupSpanID("span-1")
	if !ok {
		t.Error("Should find span after deserialization")
	}
	if ref.RecordIndex != 5 || ref.RowIndex != 10 {
		t.Errorf("SpanRef = {%d, %d}, want {5, 10}", ref.RecordIndex, ref.RowIndex)
	}

	// Verify tag lookup works
	spans := idx2.LookupByTag("env", "prod")
	if len(spans) != 1 {
		t.Errorf("Tag lookup after deserialization returned %d spans, want 1", len(spans))
	}
}

func TestIndex_MultipleReferences(t *testing.T) {
	idx := NewIndex()

	// Add spans at different locations
	for i := range 5 {
		sp := &span.Span{
			SpanID: "span-" + string(rune('A'+i)),
			Tags:   map[string]string{"batch": "1"},
		}
		idx.AddSpan(sp, i, i*10, nil)
	}

	// Verify each span has correct reference
	ref, ok := idx.LookupSpanID("span-C")
	if !ok {
		t.Error("Should find span-C")
	}
	if ref.RecordIndex != 2 || ref.RowIndex != 20 {
		t.Errorf("span-C ref = {%d, %d}, want {2, 20}", ref.RecordIndex, ref.RowIndex)
	}

	// Verify tag index
	spans := idx.LookupByTag("batch", "1")
	if len(spans) != 5 {
		t.Errorf("LookupByTag returned %d spans, want 5", len(spans))
	}
}

func BenchmarkIndex_AddSpan(b *testing.B) {
	idx := NewIndex()
	sp := &span.Span{
		TraceID:     "trace-1",
		SpanID:      "span-1",
		Name:        "operation",
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(time.Millisecond),
		ServiceName: "service",
		Tags: map[string]string{
			"env":     "prod",
			"version": "1.0",
			"region":  "us-east-1",
		},
	}

	for i := 0; b.Loop(); i++ {
		idx.AddSpan(sp, i, 0, nil)
	}
}

func BenchmarkIndex_LookupSpanID(b *testing.B) {
	idx := NewIndex()
	sp := &span.Span{
		SpanID: "span-1",
		Tags:   map[string]string{"key": "value"},
	}
	idx.AddSpan(sp, 0, 0, nil)

	for b.Loop() {
		idx.LookupSpanID("span-1")
	}
}

func BenchmarkIndex_LookupByTag(b *testing.B) {
	idx := NewIndex()
	for i := range 1000 {
		sp := &span.Span{
			SpanID: "span-" + string(rune(i)),
			Tags:   map[string]string{"env": "prod"},
		}
		idx.AddSpan(sp, i, 0, nil)
	}

	for b.Loop() {
		idx.LookupByTag("env", "prod")
	}
}
