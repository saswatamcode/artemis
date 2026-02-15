package storage

import (
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/saswatamcode/artemis/pkg/span"
)

// ArrowLinkStorage stores span links in-memory using Apache Arrow columnar format
type ArrowLinkStorage struct {
	mu       sync.RWMutex
	records  []arrow.RecordBatch
	schema   *arrow.Schema
	mem      memory.Allocator
	builder  *LinkRecordBuilder
	rowCount int64
}

// LinkRecordBuilder builds Arrow records from span links
type LinkRecordBuilder struct {
	mem             memory.Allocator
	schema          *arrow.Schema
	spanID          *array.Uint64Builder
	linkedTraceIDHi *array.Uint64Builder
	linkedTraceIDLo *array.Uint64Builder
	linkedSpanID    *array.Uint64Builder
	attributes      *array.MapBuilder
	currentRowCount int
}

const linkBatchSize = 1024 // Number of links per record batch

// NewArrowLinkStorage creates a new Arrow-based storage for links
func NewArrowLinkStorage() *ArrowLinkStorage {
	mem := memory.NewGoAllocator()
	schema := createLinkSchema()

	return &ArrowLinkStorage{
		records: make([]arrow.RecordBatch, 0),
		schema:  schema,
		mem:     mem,
		builder: NewLinkRecordBuilder(mem, schema),
	}
}

// createLinkSchema creates the Arrow schema for span links
func createLinkSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "span_id", Type: arrow.PrimitiveTypes.Uint64, Nullable: false},
			{Name: "linked_trace_id_hi", Type: arrow.PrimitiveTypes.Uint64, Nullable: false},
			{Name: "linked_trace_id_lo", Type: arrow.PrimitiveTypes.Uint64, Nullable: false},
			{Name: "linked_span_id", Type: arrow.PrimitiveTypes.Uint64, Nullable: false},
			{Name: "attributes", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), Nullable: true},
		},
		nil,
	)
}

// NewLinkRecordBuilder creates a new record builder for span links
func NewLinkRecordBuilder(mem memory.Allocator, schema *arrow.Schema) *LinkRecordBuilder {
	spanID := array.NewUint64Builder(mem)
	spanID.Reserve(linkBatchSize)

	linkedTraceIDHi := array.NewUint64Builder(mem)
	linkedTraceIDHi.Reserve(linkBatchSize)

	linkedTraceIDLo := array.NewUint64Builder(mem)
	linkedTraceIDLo.Reserve(linkBatchSize)

	linkedSpanID := array.NewUint64Builder(mem)
	linkedSpanID.Reserve(linkBatchSize)

	attributes := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
	attributes.Reserve(linkBatchSize)

	return &LinkRecordBuilder{
		mem:             mem,
		schema:          schema,
		spanID:          spanID,
		linkedTraceIDHi: linkedTraceIDHi,
		linkedTraceIDLo: linkedTraceIDLo,
		linkedSpanID:    linkedSpanID,
		attributes:      attributes,
	}
}

// Append adds a span link to the builder
func (b *LinkRecordBuilder) Append(l *span.SpanLink) {
	// Parse span ID
	spanIDVal, err := span.ParseSpanID(l.SpanID)
	if err != nil {
		spanIDVal = 0
	}
	b.spanID.Append(spanIDVal)

	// Parse linked trace ID into hi and lo parts
	linkedTraceIDHi, linkedTraceIDLo, err := span.ParseTraceID(l.LinkedTraceID)
	if err != nil {
		linkedTraceIDHi, linkedTraceIDLo = 0, 0
	}
	b.linkedTraceIDHi.Append(linkedTraceIDHi)
	b.linkedTraceIDLo.Append(linkedTraceIDLo)

	// Parse linked span ID
	linkedSpanIDVal, err := span.ParseSpanID(l.LinkedSpanID)
	if err != nil {
		linkedSpanIDVal = 0
	}
	b.linkedSpanID.Append(linkedSpanIDVal)

	if len(l.Attributes) > 0 {
		b.attributes.Append(true)
		keyBuilder := b.attributes.KeyBuilder().(*array.StringBuilder)
		valueBuilder := b.attributes.ItemBuilder().(*array.StringBuilder)

		for k, v := range l.Attributes {
			keyBuilder.Append(k)
			valueBuilder.Append(v)
		}
	} else {
		b.attributes.AppendNull()
	}

	b.currentRowCount++
}

// NewRecord builds and returns a new Arrow record, resetting the builder
func (b *LinkRecordBuilder) NewRecord() arrow.RecordBatch {
	if b.currentRowCount == 0 {
		return nil
	}

	columns := []arrow.Array{
		b.spanID.NewUint64Array(),
		b.linkedTraceIDHi.NewUint64Array(),
		b.linkedTraceIDLo.NewUint64Array(),
		b.linkedSpanID.NewUint64Array(),
		b.attributes.NewMapArray(),
	}

	record := array.NewRecord(b.schema, columns, int64(b.currentRowCount))
	b.currentRowCount = 0

	return record
}

// Release releases the builder resources
func (b *LinkRecordBuilder) Release() {
	b.spanID.Release()
	b.linkedTraceIDHi.Release()
	b.linkedTraceIDLo.Release()
	b.linkedSpanID.Release()
	b.attributes.Release()
}

// AddLink adds a span link to the storage
func (s *ArrowLinkStorage) AddLink(l *span.SpanLink) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.addLinkLocked(l)
	return nil
}

// AddLinks adds multiple span links to the storage in a single lock acquisition
func (s *ArrowLinkStorage) AddLinks(links []*span.SpanLink) error {
	if len(links) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, l := range links {
		s.addLinkLocked(l)
	}

	return nil
}

// addLinkLocked adds a link to storage without acquiring the lock
// MUST be called with s.mu held
func (s *ArrowLinkStorage) addLinkLocked(l *span.SpanLink) {
	s.builder.Append(l)
	s.rowCount++

	// Flush record batch if builder is full
	if s.builder.currentRowCount >= linkBatchSize {
		record := s.builder.NewRecord()
		if record != nil {
			s.records = append(s.records, record)
		}
	}
}

// Flush forces creation of a record from current builder state
func (s *ArrowLinkStorage) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record := s.builder.NewRecord()
	if record != nil {
		s.records = append(s.records, record)
	}

	return nil
}

// GetRecords returns all Arrow records
func (s *ArrowLinkStorage) GetRecords() []arrow.RecordBatch {
	s.mu.RLock()
	defer s.mu.RUnlock()

	copied := make([]arrow.RecordBatch, len(s.records))
	copy(copied, s.records)
	return copied
}

// RowCount returns the total number of links stored
func (s *ArrowLinkStorage) RowCount() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.rowCount
}

// RecordCount returns the number of Arrow record batches
func (s *ArrowLinkStorage) RecordCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.records)
}

// Release releases all resources
func (s *ArrowLinkStorage) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, record := range s.records {
		record.Release()
	}
	s.builder.Release()
}

// PrintStats prints storage statistics
func (s *ArrowLinkStorage) PrintStats() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return fmt.Sprintf("ArrowLinkStorage: %d links across %d record batches", s.rowCount, len(s.records))
}

// Schema returns the Arrow schema
func (s *ArrowLinkStorage) Schema() *arrow.Schema {
	return s.schema
}

// Reset clears all data in the link storage
func (s *ArrowLinkStorage) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Release existing records
	for _, record := range s.records {
		record.Release()
	}

	s.records = make([]arrow.RecordBatch, 0)
	s.rowCount = 0

	s.builder.Release()
	s.builder = NewLinkRecordBuilder(s.mem, s.schema)
}

// extractLink extracts a span link from an Arrow record at the given row
func (s *ArrowLinkStorage) extractLink(record arrow.RecordBatch, rowIndex int) (*span.SpanLink, error) {
	if rowIndex >= int(record.NumRows()) {
		return nil, fmt.Errorf("invalid row index %d", rowIndex)
	}

	l := &span.SpanLink{}

	// Read span_id and format as hex string
	spanIDVal := record.Column(0).(*array.Uint64).Value(rowIndex)
	l.SpanID = fmt.Sprintf("%016x", spanIDVal)

	// Read linked_trace_id_hi and linked_trace_id_lo and format as hex string
	linkedTraceIDHi := record.Column(1).(*array.Uint64).Value(rowIndex)
	linkedTraceIDLo := record.Column(2).(*array.Uint64).Value(rowIndex)
	l.LinkedTraceID = fmt.Sprintf("%016x%016x", linkedTraceIDHi, linkedTraceIDLo)

	// Read linked_span_id and format as hex string
	linkedSpanIDVal := record.Column(3).(*array.Uint64).Value(rowIndex)
	l.LinkedSpanID = fmt.Sprintf("%016x", linkedSpanIDVal)

	attrsCol := record.Column(4).(*array.Map)
	if !attrsCol.IsNull(rowIndex) {
		l.Attributes = make(map[string]string)

		offset := attrsCol.Offsets()[rowIndex]
		nextOffset := attrsCol.Offsets()[rowIndex+1]

		keys := attrsCol.Keys().(*array.String)
		items := attrsCol.Items().(*array.String)

		for i := int(offset); i < int(nextOffset); i++ {
			key := keys.Value(i)
			value := items.Value(i)
			l.Attributes[key] = value
		}
	}

	return l, nil
}

// GetLinksBySpanID retrieves all links for a given span ID
func (s *ArrowLinkStorage) GetLinksBySpanID(spanID string) ([]*span.SpanLink, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*span.SpanLink, 0)

	for _, record := range s.records {
		for row := 0; row < int(record.NumRows()); row++ {
			l, err := s.extractLink(record, row)
			if err != nil {
				continue
			}
			if l.SpanID == spanID {
				result = append(result, l)
			}
		}
	}

	return result, nil
}
