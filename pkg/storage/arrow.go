package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/saswatamcode/artemis/pkg/index"
	"github.com/saswatamcode/artemis/pkg/span"
)

// BlockManager interface for head block time tracking
type BlockManager interface {
	UpdateHeadTimeRange(minTime, maxTime int64)
}

// ArrowStorage stores spans in-memory using Apache Arrow columnar format
type ArrowStorage struct {
	mu            sync.RWMutex
	records       []arrow.Record
	schema        *arrow.Schema
	mem           memory.Allocator
	builder       *SpanRecordBuilder
	rowCount      int64
	idx           *index.Index
	blockManager  BlockManager // Optional block manager for time tracking
	minTime       int64        // Minimum timestamp in head block
	maxTime       int64        // Maximum timestamp in head block
	minWALSegment int          // Minimum WAL segment index in head block (-1 if not set)
	maxWALSegment int          // Maximum WAL segment index in head block (-1 if not set)
}

// SpanRecordBuilder builds Arrow records from spans
type SpanRecordBuilder struct {
	mem             memory.Allocator
	schema          *arrow.Schema
	traceID         *array.StringBuilder
	spanID          *array.StringBuilder
	parentSpanID    *array.StringBuilder
	name            *array.StringBuilder
	startTime       *array.Int64Builder
	endTime         *array.Int64Builder
	duration        *array.Int64Builder
	serviceName     *array.StringBuilder
	tags            *array.MapBuilder
	currentRowCount int
}

const batchSize = 1024 // Number of spans per record batch

// NewArrowStorage creates a new Arrow-based storage
func NewArrowStorage() *ArrowStorage {
	mem := memory.NewGoAllocator()
	schema := createSpanSchema()

	return &ArrowStorage{
		records:       make([]arrow.Record, 0),
		schema:        schema,
		mem:           mem,
		builder:       NewSpanRecordBuilder(mem, schema),
		idx:           index.NewIndex(),
		minWALSegment: -1,
		maxWALSegment: -1,
	}
}

// createSpanSchema creates the Arrow schema for spans
func createSpanSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "trace_id", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "span_id", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "parent_span_id", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "start_time", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "end_time", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "duration", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "service_name", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "tags", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), Nullable: true},
		},
		nil,
	)
}

// NewSpanRecordBuilder creates a new record builder for spans
func NewSpanRecordBuilder(mem memory.Allocator, schema *arrow.Schema) *SpanRecordBuilder {
	// Pre-allocate capacity for batch size to reduce allocations
	traceID := array.NewStringBuilder(mem)
	traceID.Reserve(batchSize)

	spanID := array.NewStringBuilder(mem)
	spanID.Reserve(batchSize)

	parentSpanID := array.NewStringBuilder(mem)
	parentSpanID.Reserve(batchSize)

	name := array.NewStringBuilder(mem)
	name.Reserve(batchSize)

	startTime := array.NewInt64Builder(mem)
	startTime.Reserve(batchSize)

	endTime := array.NewInt64Builder(mem)
	endTime.Reserve(batchSize)

	duration := array.NewInt64Builder(mem)
	duration.Reserve(batchSize)

	serviceName := array.NewStringBuilder(mem)
	serviceName.Reserve(batchSize)

	tags := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
	tags.Reserve(batchSize)

	return &SpanRecordBuilder{
		mem:          mem,
		schema:       schema,
		traceID:      traceID,
		spanID:       spanID,
		parentSpanID: parentSpanID,
		name:         name,
		startTime:    startTime,
		endTime:      endTime,
		duration:     duration,
		serviceName:  serviceName,
		tags:         tags,
	}
}

// Append adds a span to the builder
func (b *SpanRecordBuilder) Append(s *span.Span) {
	b.traceID.Append(s.TraceID)
	b.spanID.Append(s.SpanID)

	if s.ParentSpanID == "" {
		b.parentSpanID.AppendNull()
	} else {
		b.parentSpanID.Append(s.ParentSpanID)
	}

	b.name.Append(s.Name)
	b.startTime.Append(s.StartTime.UnixNano())
	b.endTime.Append(s.EndTime.UnixNano())
	b.duration.Append(s.GetDuration())
	b.serviceName.Append(s.ServiceName)

	if len(s.Tags) > 0 {
		b.tags.Append(true)
		keyBuilder := b.tags.KeyBuilder().(*array.StringBuilder)
		valueBuilder := b.tags.ItemBuilder().(*array.StringBuilder)

		for k, v := range s.Tags {
			keyBuilder.Append(k)
			valueBuilder.Append(v)
		}
	} else {
		b.tags.AppendNull()
	}

	b.currentRowCount++
}

// NewRecord builds and returns a new Arrow record, resetting the builder
func (b *SpanRecordBuilder) NewRecord() arrow.Record {
	if b.currentRowCount == 0 {
		return nil
	}

	columns := []arrow.Array{
		b.traceID.NewStringArray(),
		b.spanID.NewStringArray(),
		b.parentSpanID.NewStringArray(),
		b.name.NewStringArray(),
		b.startTime.NewInt64Array(),
		b.endTime.NewInt64Array(),
		b.duration.NewInt64Array(),
		b.serviceName.NewStringArray(),
		b.tags.NewMapArray(),
	}

	record := array.NewRecord(b.schema, columns, int64(b.currentRowCount))
	b.currentRowCount = 0

	return record
}

// Release releases the builder resources
func (b *SpanRecordBuilder) Release() {
	b.traceID.Release()
	b.spanID.Release()
	b.parentSpanID.Release()
	b.name.Release()
	b.startTime.Release()
	b.endTime.Release()
	b.duration.Release()
	b.serviceName.Release()
	b.tags.Release()
}

// AddSpan adds a span to the storage
func (s *ArrowStorage) AddSpan(sp *span.Span) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.addSpanLocked(sp)
	return nil
}

// AddSpans adds multiple spans to the storage in a single lock acquisition
// This is more efficient than calling AddSpan repeatedly for batch ingestion
func (s *ArrowStorage) AddSpans(spans []*span.Span) error {
	if len(spans) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	timeRangeChanged := false
	for _, sp := range spans {
		if s.addSpanLocked(sp) {
			timeRangeChanged = true
		}
	}

	// Notify block manager once after all spans are added
	if timeRangeChanged && s.blockManager != nil {
		s.blockManager.UpdateHeadTimeRange(s.minTime, s.maxTime)
	}

	return nil
}

// addSpanLocked adds a span to storage without acquiring the lock
// Returns true if the time range was updated
// MUST be called with s.mu held
func (s *ArrowStorage) addSpanLocked(sp *span.Span) bool {
	s.builder.Append(sp)
	s.rowCount++

	startTimeNano := sp.StartTime.UnixNano()
	endTimeNano := sp.EndTime.UnixNano()

	timeRangeChanged := false
	if s.minTime == 0 || startTimeNano < s.minTime {
		s.minTime = startTimeNano
		timeRangeChanged = true
	}
	if endTimeNano > s.maxTime {
		s.maxTime = endTimeNano
		timeRangeChanged = true
	}

	// CRITICAL: Flush record batch BEFORE indexing if builder is full
	// This ensures the index points to an actual record, not a pending one
	// Without this, queries would fail with "invalid record index" errors
	if s.builder.currentRowCount >= batchSize {
		record := s.builder.NewRecord()
		if record != nil {
			s.records = append(s.records, record)
		}
	}

	// Calculate which record and row this span is in
	// Since we flushed above if full, the span is either:
	// 1. In the last record (if we just flushed), or
	// 2. In the builder (which will become the next record)
	var currentRecordIndex int
	var currentRowInBuilder int

	if s.builder.currentRowCount == 0 {
		// We just flushed, span is the last row of the last record
		currentRecordIndex = len(s.records) - 1
		currentRowInBuilder = batchSize - 1
	} else {
		// Span is in the builder at the current position
		currentRecordIndex = len(s.records)
		currentRowInBuilder = s.builder.currentRowCount - 1
	}

	s.idx.AddSpan(sp, currentRecordIndex, currentRowInBuilder)
	return timeRangeChanged
}

// Flush forces creation of a record from current builder state
func (s *ArrowStorage) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record := s.builder.NewRecord()
	if record != nil {
		s.records = append(s.records, record)
	}

	return nil
}

// GetRecords returns all Arrow records
func (s *ArrowStorage) GetRecords() []arrow.Record {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.records
}

// RowCount returns the total number of spans stored
func (s *ArrowStorage) RowCount() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.rowCount
}

// RecordCount returns the number of Arrow record batches
func (s *ArrowStorage) RecordCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.records)
}

// Release releases all resources
func (s *ArrowStorage) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, record := range s.records {
		record.Release()
	}
	s.builder.Release()
}

// PrintStats prints storage statistics
func (s *ArrowStorage) PrintStats() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return fmt.Sprintf("ArrowStorage: %d spans across %d record batches", s.rowCount, len(s.records))
}

// GetSpanByID retrieves a span by its ID
func (s *ArrowStorage) GetSpanByID(spanID string) (*span.Span, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ref, ok := s.idx.LookupSpanID(spanID)
	if !ok {
		return nil, fmt.Errorf("span %s not found", spanID)
	}

	if ref.RecordIndex >= len(s.records) {
		return nil, fmt.Errorf("invalid record index %d", ref.RecordIndex)
	}

	record := s.records[ref.RecordIndex]
	return s.extractSpan(record, ref.RowIndex)
}

// extractSpan extracts a span from an Arrow record at the given row
func (s *ArrowStorage) extractSpan(record arrow.Record, rowIndex int) (*span.Span, error) {
	if rowIndex >= int(record.NumRows()) {
		return nil, fmt.Errorf("invalid row index %d", rowIndex)
	}

	sp := &span.Span{}

	sp.TraceID = record.Column(0).(*array.String).Value(rowIndex)

	sp.SpanID = record.Column(1).(*array.String).Value(rowIndex)

	parentCol := record.Column(2).(*array.String)
	if !parentCol.IsNull(rowIndex) {
		sp.ParentSpanID = parentCol.Value(rowIndex)
	}

	sp.Name = record.Column(3).(*array.String).Value(rowIndex)

	sp.StartTime = time.Unix(0, record.Column(4).(*array.Int64).Value(rowIndex))

	sp.EndTime = time.Unix(0, record.Column(5).(*array.Int64).Value(rowIndex))

	sp.Duration = record.Column(6).(*array.Int64).Value(rowIndex)

	sp.ServiceName = record.Column(7).(*array.String).Value(rowIndex)

	tagsCol := record.Column(8).(*array.Map)
	if !tagsCol.IsNull(rowIndex) {
		sp.Tags = make(map[string]string)

		// Get the offset for this map entry
		offset := tagsCol.Offsets()[rowIndex]
		nextOffset := tagsCol.Offsets()[rowIndex+1]

		keys := tagsCol.Keys().(*array.String)
		items := tagsCol.Items().(*array.String)

		for i := int(offset); i < int(nextOffset); i++ {
			key := keys.Value(i)
			value := items.Value(i)
			sp.Tags[key] = value
		}
	}

	return sp, nil
}

// GetIndex returns the index for querying
func (s *ArrowStorage) GetIndex() *index.Index {
	return s.idx
}

// Schema returns the Arrow schema
func (s *ArrowStorage) Schema() *arrow.Schema {
	return s.schema
}

// SetBlockManager sets the block manager for time tracking
func (s *ArrowStorage) SetBlockManager(bm BlockManager) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blockManager = bm
}

// GetTimeRange returns the min and max time of spans in this storage
func (s *ArrowStorage) GetTimeRange() (int64, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.minTime, s.maxTime
}

// UpdateWALSegment updates the WAL segment range for the head block
// Should be called when writing spans to track which WAL segments contributed
func (s *ArrowStorage) UpdateWALSegment(segmentIndex int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.minWALSegment == -1 || segmentIndex < s.minWALSegment {
		s.minWALSegment = segmentIndex
	}
	if segmentIndex > s.maxWALSegment {
		s.maxWALSegment = segmentIndex
	}
}

// GetWALSegmentRange returns the min and max WAL segment indices in this storage
// Returns (-1, -1) if no segments have been tracked yet
func (s *ArrowStorage) GetWALSegmentRange() (int, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.minWALSegment, s.maxWALSegment
}

// Reset clears all data in the head block (called after flushing to disk)
func (s *ArrowStorage) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Release existing records
	for _, record := range s.records {
		record.Release()
	}

	s.records = make([]arrow.Record, 0)
	s.rowCount = 0
	s.minTime = 0
	s.maxTime = 0
	s.minWALSegment = -1 // Reset WAL segment tracking
	s.maxWALSegment = -1 // Reset WAL segment tracking

	s.builder.Release()
	s.builder = NewSpanRecordBuilder(s.mem, s.schema)

	s.idx = index.NewIndex()
}
