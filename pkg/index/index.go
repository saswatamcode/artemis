package index

import (
	"fmt"
	"sync"

	"github.com/saswatamcode/artemis/pkg/span"
)

// SpanRef is a reference to a span in storage
type SpanRef struct {
	// Index of the Arrow record batch in Arrow, or the Row Group Index in Parquet
	RecordIndex int
	// Row within that Arrow record batch or Row within a Row Group in Parquet
	RowIndex int
}

// Index provides fast lookups for spans
type Index struct {
	mu sync.RWMutex

	// Symbol tables for tag compression
	tagKeys   *SymbolTable
	tagValues *SymbolTable

	// Span ID -> SpanRef
	spanIndex map[string]SpanRef

	// Trace ID -> list of span IDs
	traceIndex map[string][]string

	// Tag key-value pairs -> list of span IDs
	// Key format: "keyID:valueID"
	tagIndex map[string][]string

	// Inverted index: span ID -> list of tag key-value pairs
	spanTags map[string][]string
}

// NewIndex creates a new index
func NewIndex() *Index {
	return &Index{
		tagKeys:    NewSymbolTable(),
		tagValues:  NewSymbolTable(),
		spanIndex:  make(map[string]SpanRef),
		traceIndex: make(map[string][]string),
		tagIndex:   make(map[string][]string),
		spanTags:   make(map[string][]string),
	}
}

// AddSpan adds a span to the index
func (idx *Index) AddSpan(s *span.Span, recordIndex, rowIndex int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	ref := SpanRef{
		RecordIndex: recordIndex,
		RowIndex:    rowIndex,
	}

	idx.spanIndex[s.SpanID] = ref

	// Index by trace ID (critical for trace lookups!)
	idx.traceIndex[s.TraceID] = append(idx.traceIndex[s.TraceID], s.SpanID)

	// Index by tags
	var spanTagPairs []string
	for key, value := range s.Tags {
		keyID := idx.tagKeys.Intern(key)
		valueID := idx.tagValues.Intern(value)
		tagPair := fmt.Sprintf("%d:%d", keyID, valueID)

		// Add to tag index
		idx.tagIndex[tagPair] = append(idx.tagIndex[tagPair], s.SpanID)
		spanTagPairs = append(spanTagPairs, tagPair)
	}

	// Store inverted index
	if len(spanTagPairs) > 0 {
		idx.spanTags[s.SpanID] = spanTagPairs
	}
}

// LookupSpanID returns the storage reference for a span ID
func (idx *Index) LookupSpanID(spanID string) (SpanRef, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	ref, ok := idx.spanIndex[spanID]
	return ref, ok
}

// LookupByTraceID returns all span IDs for a given trace ID
func (idx *Index) LookupByTraceID(traceID string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.traceIndex[traceID]
}

// LookupByTag returns all span IDs matching a specific tag key-value pair
func (idx *Index) LookupByTag(key, value string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	keyID := idx.tagKeys.Lookup(key)
	if keyID == 0 {
		return nil
	}

	valueID := idx.tagValues.Lookup(value)
	if valueID == 0 {
		return nil
	}

	tagPair := fmt.Sprintf("%d:%d", keyID, valueID)
	return idx.tagIndex[tagPair]
}

// Stats returns index statistics
func (idx *Index) Stats() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return IndexStats{
		TotalSpans:      len(idx.spanIndex),
		UniqueTraces:    len(idx.traceIndex),
		UniqueTagKeys:   idx.tagKeys.Size(),
		UniqueTagValues: idx.tagValues.Size(),
		TagIndexSize:    len(idx.tagIndex),
	}
}

// IndexStats holds index statistics
type IndexStats struct {
	TotalSpans      int
	UniqueTraces    int
	UniqueTagKeys   int
	UniqueTagValues int
	TagIndexSize    int
}

// Clear clears the index (useful for rebuilding)
func (idx *Index) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.tagKeys = NewSymbolTable()
	idx.tagValues = NewSymbolTable()
	idx.spanIndex = make(map[string]SpanRef)
	idx.traceIndex = make(map[string][]string)
	idx.tagIndex = make(map[string][]string)
	idx.spanTags = make(map[string][]string)
}

// SerializedIndex is a JSON-serializable version of Index
type SerializedIndex struct {
	TagKeys    map[string]uint32   `json:"tag_keys"`
	TagValues  map[string]uint32   `json:"tag_values"`
	SpanIndex  map[string]SpanRef  `json:"span_index"`
	TraceIndex map[string][]string `json:"trace_index"`
	TagIndex   map[string][]string `json:"tag_index"`
	SpanTags   map[string][]string `json:"span_tags"`
}

// Serialize converts the index to a serializable format
func (idx *Index) Serialize() *SerializedIndex {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return &SerializedIndex{
		TagKeys:    idx.tagKeys.SerializeToMap(),
		TagValues:  idx.tagValues.SerializeToMap(),
		SpanIndex:  idx.spanIndex,
		TraceIndex: idx.traceIndex,
		TagIndex:   idx.tagIndex,
		SpanTags:   idx.spanTags,
	}
}

// NewIndexFromSerialized creates an index from serialized data
func NewIndexFromSerialized(s *SerializedIndex) *Index {
	return &Index{
		tagKeys:    NewSymbolTableFromMap(s.TagKeys),
		tagValues:  NewSymbolTableFromMap(s.TagValues),
		spanIndex:  s.SpanIndex,
		traceIndex: s.TraceIndex,
		tagIndex:   s.TagIndex,
		spanTags:   s.SpanTags,
	}
}
