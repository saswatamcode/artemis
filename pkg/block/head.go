package block

import (
	"fmt"

	"github.com/saswatamcode/artemis/pkg/index"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// HeadBlock wraps in-memory ArrowStorage to implement the Block interface
// This allows head block to be queried uniformly with persisted blocks
//
// Key differences from ArrowBlock (disk-based L0):
// - Mutable: Can add new spans via the underlying ArrowStorage
// - No directory: Purely in-memory, not backed by disk
// - Thread-safe: ArrowStorage has internal locking
// - Active ingestion: Used for real-time span ingestion
type HeadBlock struct {
	storage *storage.ArrowStorage
	meta    *BlockMeta
}

// NewHeadBlock creates a new head block wrapper around ArrowStorage
func NewHeadBlock(storage *storage.ArrowStorage) *HeadBlock {
	return &HeadBlock{
		storage: storage,
	}
}

// Meta returns the block metadata
// For head block, metadata is computed dynamically from storage state
// Uses a consistent snapshot to prevent metadata inconsistencies
func (hb *HeadBlock) Meta() *BlockMeta {
	// Get all metadata in a single atomic snapshot to prevent inconsistencies
	snapshot := hb.storage.GetMetadataSnapshot()

	return &BlockMeta{
		MinTime:       snapshot.MinTime,
		MaxTime:       snapshot.MaxTime,
		SpanCount:     snapshot.RowCount,
		Version:       1,
		Compaction:    nil, // Head block is always L0 (no compaction)
		MinWALSegment: snapshot.MinWALSegment,
		MaxWALSegment: snapshot.MaxWALSegment,
	}
}

// Index returns the block's index
func (hb *HeadBlock) Index() *index.Index {
	return hb.storage.GetIndex()
}

// HasIndex returns true if the block has an index loaded
// Head block always has an index (built in real-time)
func (hb *HeadBlock) HasIndex() bool {
	return true
}

// Dir returns the directory path of this block
// Head block has no directory (purely in-memory)
func (hb *HeadBlock) Dir() string {
	return "" // No directory for head block
}

// Close releases resources held by this block
// For head block, this does not release the storage itself
// (storage is managed separately by the block manager)
func (hb *HeadBlock) Close() error {
	// Head block storage is managed externally
	// We don't release it here
	return nil
}

// GetSpanByID retrieves a single span by ID using the index
func (hb *HeadBlock) GetSpanByID(spanID string) (*span.Span, error) {
	return hb.storage.GetSpanByID(spanID)
}

// GetSpansBatch efficiently retrieves multiple spans by ID
// For in-memory head block, this is fast since all data is in RAM
func (hb *HeadBlock) GetSpansBatch(spanIDs []string) ([]*span.Span, error) {
	idx := hb.storage.GetIndex()
	records := hb.storage.GetRecords()

	result := make([]*span.Span, 0, len(spanIDs))
	for _, spanID := range spanIDs {
		ref, ok := idx.LookupSpanID(spanID)
		if !ok {
			continue // Skip spans not found
		}

		if ref.RecordIndex >= len(records) {
			// FIX: Log warning for invalid record index to help detect index corruption
			// This makes debugging easier compared to silently skipping
			fmt.Printf("WARNING: HeadBlock index corruption - span %s has invalid record index %d (only %d records exist)\n",
				spanID, ref.RecordIndex, len(records))
			continue
		}

		sp, err := extractSpanFromArrowRecord(records[ref.RecordIndex], ref.RowIndex)
		if err != nil {
			continue
		}

		result = append(result, sp)
	}

	return result, nil
}

// ReadAll reads all spans from the head block
func (hb *HeadBlock) ReadAll() ([]*span.Span, error) {
	records := hb.storage.GetRecords()
	result := make([]*span.Span, 0, hb.storage.RowCount())

	for _, record := range records {
		for row := 0; row < int(record.NumRows()); row++ {
			sp, err := extractSpanFromArrowRecord(record, row)
			if err != nil {
				continue
			}
			result = append(result, sp)
		}
	}

	return result, nil
}

// GetTraceByID retrieves all spans for a given trace ID
func (hb *HeadBlock) GetTraceByID(traceID string) ([]*span.Span, error) {
	idx := hb.storage.GetIndex()
	spanIDs := idx.LookupByTraceID(traceID)
	return hb.GetSpansBatch(spanIDs)
}

// GetSpansByTag retrieves all spans that have a specific tag key-value pair
func (hb *HeadBlock) GetSpansByTag(tagKey, tagValue string) ([]*span.Span, error) {
	idx := hb.storage.GetIndex()
	spanIDs := idx.LookupByTag(tagKey, tagValue)
	return hb.GetSpansBatch(spanIDs)
}

// Storage returns the underlying ArrowStorage
// This is useful for write operations (AddSpan, etc.)
func (hb *HeadBlock) Storage() *storage.ArrowStorage {
	return hb.storage
}

// Flush forces creation of a record from current builder state
// This ensures all pending spans are visible for queries
func (hb *HeadBlock) Flush() error {
	return hb.storage.Flush()
}

// Note: HeadBlock intentionally does NOT implement write operations like AddSpan
// Those should be called directly on the underlying Storage() to make it clear
// that head block is special (mutable) vs other blocks (immutable)
