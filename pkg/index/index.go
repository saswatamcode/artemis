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

// AttrRef is a reference to span attributes in attributes.parquet
type AttrRef struct {
	// Row group index in attributes.parquet
	RecordIndex int
	// Row index within row group
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

	// Span ID -> AttrRef (NEW: direct row references for attributes.parquet)
	attrIndex map[string]AttrRef

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
		attrIndex:  make(map[string]AttrRef),
		traceIndex: make(map[string][]string),
		tagIndex:   make(map[string][]string),
		spanTags:   make(map[string][]string),
	}
}

// AddSpan adds a span to the index
// attrRef is optional - if provided, enables fast attribute lookups
func (idx *Index) AddSpan(s *span.Span, recordIndex, rowIndex int, attrRef *AttrRef) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	ref := SpanRef{
		RecordIndex: recordIndex,
		RowIndex:    rowIndex,
	}

	idx.spanIndex[s.SpanID] = ref

	// Add attribute reference if provided
	if attrRef != nil {
		idx.attrIndex[s.SpanID] = *attrRef
	}

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

// LookupAttrRef returns the attribute reference for a span ID
func (idx *Index) LookupAttrRef(spanID string) (AttrRef, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	ref, ok := idx.attrIndex[spanID]
	return ref, ok
}

// LookupAttrRefsBatch returns attribute references for multiple span IDs
// Returns only the refs that exist in the index
func (idx *Index) LookupAttrRefsBatch(spanIDs []string) map[string]AttrRef {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := make(map[string]AttrRef, len(spanIDs))
	for _, spanID := range spanIDs {
		if ref, ok := idx.attrIndex[spanID]; ok {
			result[spanID] = ref
		}
	}
	return result
}

// LookupByTraceID returns all span IDs for a given trace ID
// Returns a defensive copy to prevent concurrent modification issues
func (idx *Index) LookupByTraceID(traceID string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := idx.traceIndex[traceID]
	if result == nil {
		return nil
	}
	// Return a copy to prevent concurrent modification
	copied := make([]string, len(result))
	copy(copied, result)
	return copied
}

// LookupByTag returns all span IDs matching a specific tag key-value pair
// Returns a defensive copy to prevent concurrent modification issues
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
	result := idx.tagIndex[tagPair]
	if result == nil {
		return nil
	}
	// Return a copy to prevent concurrent modification
	copied := make([]string, len(result))
	copy(copied, result)
	return copied
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
	idx.attrIndex = make(map[string]AttrRef)
	idx.traceIndex = make(map[string][]string)
	idx.tagIndex = make(map[string][]string)
	idx.spanTags = make(map[string][]string)
}

// ClearStorageRefs clears only storage references (spanIndex, attrIndex, traceIndex)
// Preserves tag index and symbol tables for query compatibility
// Used when converting Arrow block to Parquet block
func (idx *Index) ClearStorageRefs() {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.spanIndex = make(map[string]SpanRef)
	idx.attrIndex = make(map[string]AttrRef)
	idx.traceIndex = make(map[string][]string)
}

// AddSpanRef adds span and trace index entries without processing tags
// Used when tag index already exists from Arrow storage
func (idx *Index) AddSpanRef(spanID, traceID string, spanRef SpanRef, attrRef *AttrRef) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.spanIndex[spanID] = spanRef
	if attrRef != nil {
		idx.attrIndex[spanID] = *attrRef
	}
	idx.traceIndex[traceID] = append(idx.traceIndex[traceID], spanID)
}

// SerializedIndex is a JSON-serializable version of Index
type SerializedIndex struct {
	TagKeys    map[string]uint32   `json:"tag_keys"`
	TagValues  map[string]uint32   `json:"tag_values"`
	SpanIndex  map[string]SpanRef  `json:"span_index"`
	AttrIndex  map[string]AttrRef  `json:"attr_index,omitempty"` // Optional for backward compatibility
	TraceIndex map[string][]string `json:"trace_index"`
	TagIndex   map[string][]string `json:"tag_index"`
	SpanTags   map[string][]string `json:"span_tags"`
}

// Serialize converts the index to a serializable format
// Makes deep copies of all maps to prevent concurrent modification during serialization
func (idx *Index) Serialize() *SerializedIndex {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Deep copy spanIndex map
	spanIndexCopy := make(map[string]SpanRef, len(idx.spanIndex))
	for k, v := range idx.spanIndex {
		spanIndexCopy[k] = v
	}

	// Deep copy attrIndex map
	attrIndexCopy := make(map[string]AttrRef, len(idx.attrIndex))
	for k, v := range idx.attrIndex {
		attrIndexCopy[k] = v
	}

	// Deep copy traceIndex map (including slices)
	traceIndexCopy := make(map[string][]string, len(idx.traceIndex))
	for k, v := range idx.traceIndex {
		copied := make([]string, len(v))
		copy(copied, v)
		traceIndexCopy[k] = copied
	}

	// Deep copy tagIndex map (including slices)
	tagIndexCopy := make(map[string][]string, len(idx.tagIndex))
	for k, v := range idx.tagIndex {
		copied := make([]string, len(v))
		copy(copied, v)
		tagIndexCopy[k] = copied
	}

	// Deep copy spanTags map (including slices)
	spanTagsCopy := make(map[string][]string, len(idx.spanTags))
	for k, v := range idx.spanTags {
		copied := make([]string, len(v))
		copy(copied, v)
		spanTagsCopy[k] = copied
	}

	return &SerializedIndex{
		TagKeys:    idx.tagKeys.SerializeToMap(),
		TagValues:  idx.tagValues.SerializeToMap(),
		SpanIndex:  spanIndexCopy,
		AttrIndex:  attrIndexCopy,
		TraceIndex: traceIndexCopy,
		TagIndex:   tagIndexCopy,
		SpanTags:   spanTagsCopy,
	}
}

// NewIndexFromSerialized creates an index from serialized data
// Handles backward compatibility for indexes without attrIndex
func NewIndexFromSerialized(s *SerializedIndex) *Index {
	// Initialize attrIndex even if it's nil in serialized data (backward compatibility)
	attrIndex := s.AttrIndex
	if attrIndex == nil {
		attrIndex = make(map[string]AttrRef)
	}

	return &Index{
		tagKeys:    NewSymbolTableFromMap(s.TagKeys),
		tagValues:  NewSymbolTableFromMap(s.TagValues),
		spanIndex:  s.SpanIndex,
		attrIndex:  attrIndex,
		traceIndex: s.TraceIndex,
		tagIndex:   s.TagIndex,
		spanTags:   s.SpanTags,
	}
}
