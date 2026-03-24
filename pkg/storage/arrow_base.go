package storage

import (
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// RecordBuilder is a generic interface for building Arrow records from items of type T.
// This allows ArrowStorageBase to work with both spans and links.
type RecordBuilder[T any] interface {
	// Append adds an item to the builder
	Append(item *T)

	// NewRecord builds and returns a new Arrow record, resetting the builder
	// Returns nil if no items have been appended
	NewRecord() arrow.RecordBatch

	// Release releases the builder resources
	Release()

	// CurrentRowCount returns the number of items in the current batch
	CurrentRowCount() int
}

// ArrowStorageBase provides generic Arrow-based columnar storage for any item type T.
// This eliminates code duplication between span storage and link storage.
//
// Type parameter T represents the item being stored (e.g., span.Span or span.SpanLink).
type ArrowStorageBase[T any] struct {
	mu       sync.RWMutex
	records  []arrow.RecordBatch
	schema   *arrow.Schema
	mem      memory.Allocator
	builder  RecordBuilder[T]
	rowCount int64

	// Batch size for this storage (typically 1024)
	batchSize int
}

// NewArrowStorageBase creates a new generic Arrow storage.
func NewArrowStorageBase[T any](
	schema *arrow.Schema,
	builder RecordBuilder[T],
	batchSize int,
) *ArrowStorageBase[T] {
	return &ArrowStorageBase[T]{
		records:   make([]arrow.RecordBatch, 0),
		schema:    schema,
		mem:       memory.NewGoAllocator(),
		builder:   builder,
		batchSize: batchSize,
	}
}

// AddItem adds a single item to the storage.
// This is the generic version used by both spans and links.
func (s *ArrowStorageBase[T]) AddItem(item *T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.addItemLocked(item)
}

// AddItems adds multiple items to the storage in a single lock acquisition.
// More efficient than calling AddItem repeatedly.
func (s *ArrowStorageBase[T]) AddItems(items []*T) {
	if len(items) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, item := range items {
		s.addItemLocked(item)
	}
}

// addItemLocked adds an item without acquiring the lock.
// MUST be called with s.mu held.
func (s *ArrowStorageBase[T]) addItemLocked(item *T) {
	s.builder.Append(item)
	s.rowCount++

	// Flush record batch if builder is full
	if s.builder.CurrentRowCount() >= s.batchSize {
		record := s.builder.NewRecord()
		if record != nil {
			s.records = append(s.records, record)
		}
	}
}

// Flush forces creation of a record from current builder state.
func (s *ArrowStorageBase[T]) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record := s.builder.NewRecord()
	if record != nil {
		s.records = append(s.records, record)
	}

	return nil
}

// GetRecords returns all Arrow records.
// Returns a defensive copy of the slice to prevent concurrent modification.
func (s *ArrowStorageBase[T]) GetRecords() []arrow.RecordBatch {
	s.mu.RLock()
	defer s.mu.RUnlock()

	copied := make([]arrow.RecordBatch, len(s.records))
	copy(copied, s.records)
	return copied
}

// RowCount returns the total number of items stored.
func (s *ArrowStorageBase[T]) RowCount() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.rowCount
}

// RecordCount returns the number of Arrow record batches.
func (s *ArrowStorageBase[T]) RecordCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.records)
}

// Release releases all resources.
func (s *ArrowStorageBase[T]) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, record := range s.records {
		record.Release()
	}
	s.builder.Release()
}

// Schema returns the Arrow schema.
func (s *ArrowStorageBase[T]) Schema() *arrow.Schema {
	return s.schema
}

// Reset clears all data in the storage.
// Accepts a factory function to create a new builder after reset.
func (s *ArrowStorageBase[T]) Reset(newBuilder func() RecordBuilder[T]) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.resetUnsafe(newBuilder)
}

// resetUnsafe clears all data without locking.
// MUST be called with s.mu held.
func (s *ArrowStorageBase[T]) resetUnsafe(newBuilder func() RecordBuilder[T]) {
	// Release existing records
	for _, record := range s.records {
		record.Release()
	}

	s.records = make([]arrow.RecordBatch, 0)
	s.rowCount = 0

	s.builder.Release()
	s.builder = newBuilder()
}

// PrintStats returns storage statistics as a string.
func (s *ArrowStorageBase[T]) PrintStats(typeName string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return fmt.Sprintf("%s: %d items across %d record batches", typeName, s.rowCount, len(s.records))
}

// GetRecordsUnsafe returns the records slice without locking or copying.
// MUST be called with external locking (s.mu held).
// Used internally for performance-critical paths.
func (s *ArrowStorageBase[T]) GetRecordsUnsafe() []arrow.RecordBatch {
	return s.records
}

// GetBuilderCurrentRowCount returns the current row count in the builder.
// MUST be called with s.mu held.
func (s *ArrowStorageBase[T]) GetBuilderCurrentRowCount() int {
	return s.builder.CurrentRowCount()
}

// AppendRecord appends a record to the storage.
// MUST be called with s.mu held.
func (s *ArrowStorageBase[T]) AppendRecord(record arrow.RecordBatch) {
	s.records = append(s.records, record)
}

// GetBuilder returns the builder for direct access.
// MUST be called with s.mu held.
func (s *ArrowStorageBase[T]) GetBuilder() RecordBuilder[T] {
	return s.builder
}

// IncrementRowCount increments the row count.
// MUST be called with s.mu held.
func (s *ArrowStorageBase[T]) IncrementRowCount() {
	s.rowCount++
}
