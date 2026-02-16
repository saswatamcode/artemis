package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/saswatamcode/artemis/pkg/span"
)

// ArrowEventStorage stores span events in-memory using Apache Arrow columnar format
type ArrowEventStorage struct {
	mu       sync.RWMutex
	records  []arrow.RecordBatch
	schema   *arrow.Schema
	mem      memory.Allocator
	builder  *EventRecordBuilder
	rowCount int64
}

// EventRecordBuilder builds Arrow records from span events
type EventRecordBuilder struct {
	mem             memory.Allocator
	schema          *arrow.Schema
	spanID          *array.Uint64Builder
	name            *array.StringBuilder
	timestamp       *array.Int64Builder
	attributes      *array.MapBuilder
	currentRowCount int
}

const eventBatchSize = 1024 // Number of events per record batch

// NewArrowEventStorage creates a new Arrow-based storage for events
func NewArrowEventStorage() *ArrowEventStorage {
	mem := memory.NewGoAllocator()
	schema := createEventSchema()

	return &ArrowEventStorage{
		records: make([]arrow.RecordBatch, 0),
		schema:  schema,
		mem:     mem,
		builder: NewEventRecordBuilder(mem, schema),
	}
}

// createEventSchema creates the Arrow schema for span events
func createEventSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "span_id", Type: arrow.PrimitiveTypes.Uint64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "attributes", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), Nullable: true},
		},
		nil,
	)
}

// NewEventRecordBuilder creates a new record builder for span events
func NewEventRecordBuilder(mem memory.Allocator, schema *arrow.Schema) *EventRecordBuilder {
	spanID := array.NewUint64Builder(mem)
	spanID.Reserve(eventBatchSize)

	name := array.NewStringBuilder(mem)
	name.Reserve(eventBatchSize)

	timestamp := array.NewInt64Builder(mem)
	timestamp.Reserve(eventBatchSize)

	attributes := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
	attributes.Reserve(eventBatchSize)

	return &EventRecordBuilder{
		mem:        mem,
		schema:     schema,
		spanID:     spanID,
		name:       name,
		timestamp:  timestamp,
		attributes: attributes,
	}
}

// Append adds a span event to the builder
func (b *EventRecordBuilder) Append(e *span.SpanEvent) {
	// Parse span ID
	spanIDVal, err := span.ParseSpanID(e.SpanID)
	if err != nil {
		spanIDVal = 0
	}
	b.spanID.Append(spanIDVal)

	b.name.Append(e.Name)
	b.timestamp.Append(e.Timestamp.UnixNano())

	if len(e.Attributes) > 0 {
		b.attributes.Append(true)
		keyBuilder := b.attributes.KeyBuilder().(*array.StringBuilder)
		valueBuilder := b.attributes.ItemBuilder().(*array.StringBuilder)

		for k, v := range e.Attributes {
			keyBuilder.Append(k)
			valueBuilder.Append(v)
		}
	} else {
		b.attributes.AppendNull()
	}

	b.currentRowCount++
}

// NewRecord builds and returns a new Arrow record, resetting the builder
func (b *EventRecordBuilder) NewRecord() arrow.RecordBatch {
	if b.currentRowCount == 0 {
		return nil
	}

	columns := []arrow.Array{
		b.spanID.NewUint64Array(),
		b.name.NewStringArray(),
		b.timestamp.NewInt64Array(),
		b.attributes.NewMapArray(),
	}

	record := array.NewRecord(b.schema, columns, int64(b.currentRowCount))
	b.currentRowCount = 0

	return record
}

// Release releases the builder resources
func (b *EventRecordBuilder) Release() {
	b.spanID.Release()
	b.name.Release()
	b.timestamp.Release()
	b.attributes.Release()
}

// AddEvent adds a span event to the storage
func (s *ArrowEventStorage) AddEvent(e *span.SpanEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.addEventLocked(e)
	return nil
}

// AddEvents adds multiple span events to the storage in a single lock acquisition
func (s *ArrowEventStorage) AddEvents(events []*span.SpanEvent) error {
	if len(events) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, e := range events {
		s.addEventLocked(e)
	}

	return nil
}

// addEventLocked adds an event to storage without acquiring the lock
// MUST be called with s.mu held
func (s *ArrowEventStorage) addEventLocked(e *span.SpanEvent) {
	s.builder.Append(e)
	s.rowCount++

	// Flush record batch if builder is full
	if s.builder.currentRowCount >= eventBatchSize {
		record := s.builder.NewRecord()
		if record != nil {
			s.records = append(s.records, record)
		}
	}
}

// Flush forces creation of a record from current builder state
func (s *ArrowEventStorage) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record := s.builder.NewRecord()
	if record != nil {
		s.records = append(s.records, record)
	}

	return nil
}

// GetRecords returns all Arrow records
func (s *ArrowEventStorage) GetRecords() []arrow.RecordBatch {
	s.mu.RLock()
	defer s.mu.RUnlock()

	copied := make([]arrow.RecordBatch, len(s.records))
	copy(copied, s.records)
	return copied
}

// RowCount returns the total number of events stored
func (s *ArrowEventStorage) RowCount() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.rowCount
}

// RecordCount returns the number of Arrow record batches
func (s *ArrowEventStorage) RecordCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.records)
}

// Release releases all resources
func (s *ArrowEventStorage) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, record := range s.records {
		record.Release()
	}
	s.builder.Release()
}

// PrintStats prints storage statistics
func (s *ArrowEventStorage) PrintStats() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return fmt.Sprintf("ArrowEventStorage: %d events across %d record batches", s.rowCount, len(s.records))
}

// Schema returns the Arrow schema
func (s *ArrowEventStorage) Schema() *arrow.Schema {
	return s.schema
}

// Reset clears all data in the event storage
func (s *ArrowEventStorage) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Release existing records
	for _, record := range s.records {
		record.Release()
	}

	s.records = make([]arrow.RecordBatch, 0)
	s.rowCount = 0

	s.builder.Release()
	s.builder = NewEventRecordBuilder(s.mem, s.schema)
}

// extractEvent extracts a span event from an Arrow record at the given row
func (s *ArrowEventStorage) extractEvent(record arrow.RecordBatch, rowIndex int) (*span.SpanEvent, error) {
	if rowIndex >= int(record.NumRows()) {
		return nil, fmt.Errorf("invalid row index %d", rowIndex)
	}

	e := &span.SpanEvent{}

	// Read span_id and format as hex string
	spanIDVal := record.Column(0).(*array.Uint64).Value(rowIndex)
	e.SpanID = fmt.Sprintf("%016x", spanIDVal)

	e.Name = record.Column(1).(*array.String).Value(rowIndex)
	e.Timestamp = time.Unix(0, record.Column(2).(*array.Int64).Value(rowIndex))

	attrsCol := record.Column(3).(*array.Map)
	if !attrsCol.IsNull(rowIndex) {
		e.Attributes = make(map[string]string)

		offset := attrsCol.Offsets()[rowIndex]
		nextOffset := attrsCol.Offsets()[rowIndex+1]

		keys := attrsCol.Keys().(*array.String)
		items := attrsCol.Items().(*array.String)

		for i := int(offset); i < int(nextOffset); i++ {
			key := keys.Value(i)
			value := items.Value(i)
			e.Attributes[key] = value
		}
	}

	return e, nil
}

// GetEventsBySpanID retrieves all events for a given span ID
func (s *ArrowEventStorage) GetEventsBySpanID(spanID string) ([]*span.SpanEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*span.SpanEvent, 0)

	for _, record := range s.records {
		for row := 0; row < int(record.NumRows()); row++ {
			e, err := s.extractEvent(record, row)
			if err != nil {
				continue
			}
			if e.SpanID == spanID {
				result = append(result, e)
			}
		}
	}

	return result, nil
}

// GetEventsBatch efficiently retrieves events for multiple span IDs
// Returns a map of spanID -> []SpanEvent
// Single pass through all event records instead of N passes
func (s *ArrowEventStorage) GetEventsBatch(spanIDs []string) (map[string][]*span.SpanEvent, error) {
	if len(spanIDs) == 0 {
		return nil, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Build set of span IDs we're looking for
	spanIDSet := make(map[string]struct{}, len(spanIDs))
	for _, sid := range spanIDs {
		spanIDSet[sid] = struct{}{}
	}

	// Single pass through all event records
	result := make(map[string][]*span.SpanEvent)

	for _, record := range s.records {
		for row := 0; row < int(record.NumRows()); row++ {
			e, err := s.extractEvent(record, row)
			if err != nil {
				continue
			}
			// Only collect events for span IDs we care about
			if _, found := spanIDSet[e.SpanID]; found {
				result[e.SpanID] = append(result[e.SpanID], e)
			}
		}
	}

	return result, nil
}
