package storage

import (
	"fmt"
	"math/bits"
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

// WAL interface for write-ahead logging
type WAL interface {
	WriteSpan(s *span.Span) (int, error)
	WriteLink(link *span.SpanLink) (int, error)
}

// ArrowStorage stores spans in-memory using Apache Arrow columnar format
type ArrowStorage struct {
	mu            sync.RWMutex
	records       []arrow.RecordBatch
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

	// Transaction dependencies (set via SetTransactionDependencies)
	isolation   *IsolationCoordinator
	linkStorage *ArrowLinkStorage
	wal         WAL
}

// SpanRecordBuilder builds Arrow records from spans
type SpanRecordBuilder struct {
	mem             memory.Allocator
	schema          *arrow.Schema
	traceIDHi       *array.Uint64Builder
	traceIDLo       *array.Uint64Builder
	spanID          *array.Uint64Builder
	parentSpanID    *array.Uint64Builder
	name            *array.StringBuilder
	startTime       *array.Int64Builder
	endTime         *array.Int64Builder
	duration        *array.Int64Builder
	serviceName     *array.StringBuilder
	tags            *array.MapBuilder
	bucket1s        *array.Int64Builder
	durationBucket  *array.Int32Builder
	currentRowCount int
}

const batchSize = 1024 // Number of spans per record batch

// NewArrowStorage creates a new Arrow-based storage
func NewArrowStorage() *ArrowStorage {
	mem := memory.NewGoAllocator()
	schema := createSpanSchema()

	return &ArrowStorage{
		records:       make([]arrow.RecordBatch, 0),
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
			{Name: "trace_id_hi", Type: arrow.PrimitiveTypes.Uint64, Nullable: false},
			{Name: "trace_id_lo", Type: arrow.PrimitiveTypes.Uint64, Nullable: false},
			{Name: "span_id", Type: arrow.PrimitiveTypes.Uint64, Nullable: false},
			{Name: "parent_span_id", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "start_time", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "end_time", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "duration", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "service_name", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "tags", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), Nullable: true},
			{Name: "bucket1s", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "duration_bucket", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		},
		nil,
	)
}

// NewSpanRecordBuilder creates a new record builder for spans
func NewSpanRecordBuilder(mem memory.Allocator, schema *arrow.Schema) *SpanRecordBuilder {
	// Pre-allocate capacity for batch size to reduce allocations
	traceIDHi := array.NewUint64Builder(mem)
	traceIDHi.Reserve(batchSize)

	traceIDLo := array.NewUint64Builder(mem)
	traceIDLo.Reserve(batchSize)

	spanID := array.NewUint64Builder(mem)
	spanID.Reserve(batchSize)

	parentSpanID := array.NewUint64Builder(mem)
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

	bucket1s := array.NewInt64Builder(mem)
	bucket1s.Reserve(batchSize)

	durationBucket := array.NewInt32Builder(mem)
	durationBucket.Reserve(batchSize)

	return &SpanRecordBuilder{
		mem:            mem,
		schema:         schema,
		traceIDHi:      traceIDHi,
		traceIDLo:      traceIDLo,
		spanID:         spanID,
		parentSpanID:   parentSpanID,
		name:           name,
		startTime:      startTime,
		endTime:        endTime,
		duration:       duration,
		serviceName:    serviceName,
		tags:           tags,
		bucket1s:       bucket1s,
		durationBucket: durationBucket,
	}
}

// Append adds a span to the builder
func (b *SpanRecordBuilder) Append(s *span.Span) {
	// Parse trace ID into hi and lo parts
	traceIDHi, traceIDLo, err := span.ParseTraceID(s.TraceID)
	if err != nil {
		// If parsing fails, use 0 values (should not happen with valid data)
		traceIDHi, traceIDLo = 0, 0
	}
	b.traceIDHi.Append(traceIDHi)
	b.traceIDLo.Append(traceIDLo)

	// Parse span ID
	spanIDVal, err := span.ParseSpanID(s.SpanID)
	if err != nil {
		spanIDVal = 0
	}
	b.spanID.Append(spanIDVal)

	// Parse parent span ID (or use 0 for null)
	if s.ParentSpanID == "" {
		b.parentSpanID.AppendNull()
	} else {
		parentSpanIDVal, err := span.ParseSpanID(s.ParentSpanID)
		if err != nil {
			parentSpanIDVal = 0
		}
		b.parentSpanID.Append(parentSpanIDVal)
	}

	b.name.Append(s.Name)
	startUnixNano := s.StartTime.UnixNano()
	b.startTime.Append(startUnixNano)
	b.endTime.Append(s.EndTime.UnixNano())
	durationNs := s.GetDuration()
	b.duration.Append(durationNs)
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

	// Calculate bucket1s: round down start time to the second
	const sec = int64(1_000_000_000)
	bucket := startUnixNano - (startUnixNano % sec)
	b.bucket1s.Append(bucket)

	// Calculate duration bucket using exponential bucketing
	var durationBucketVal int32
	if durationNs <= 0 {
		durationBucketVal = 0
	} else {
		durationBucketVal = int32(bits.Len64(uint64(durationNs)) - 1)
	}
	b.durationBucket.Append(durationBucketVal)

	b.currentRowCount++
}

// NewRecord builds and returns a new Arrow record, resetting the builder
func (b *SpanRecordBuilder) NewRecord() arrow.RecordBatch {
	if b.currentRowCount == 0 {
		return nil
	}

	columns := []arrow.Array{
		b.traceIDHi.NewUint64Array(),
		b.traceIDLo.NewUint64Array(),
		b.spanID.NewUint64Array(),
		b.parentSpanID.NewUint64Array(),
		b.name.NewStringArray(),
		b.startTime.NewInt64Array(),
		b.endTime.NewInt64Array(),
		b.duration.NewInt64Array(),
		b.serviceName.NewStringArray(),
		b.tags.NewMapArray(),
		b.bucket1s.NewInt64Array(),
		b.durationBucket.NewInt32Array(),
	}

	record := array.NewRecord(b.schema, columns, int64(b.currentRowCount))
	b.currentRowCount = 0

	return record
}

// Release releases the builder resources
func (b *SpanRecordBuilder) Release() {
	b.traceIDHi.Release()
	b.traceIDLo.Release()
	b.spanID.Release()
	b.parentSpanID.Release()
	b.name.Release()
	b.startTime.Release()
	b.endTime.Release()
	b.duration.Release()
	b.serviceName.Release()
	b.tags.Release()
	b.bucket1s.Release()
	b.durationBucket.Release()
}

// AddSpan adds a span to the storage
func (s *ArrowStorage) AddSpan(sp *span.Span) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	timeRangeChanged := s.addSpanLocked(sp)

	// Notify block manager if time range changed (consistency with AddSpans)
	if timeRangeChanged && s.blockManager != nil {
		s.blockManager.UpdateHeadTimeRange(s.minTime, s.maxTime)
	}

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

	return s.addSpansLocked(spans)
}

// addSpansLocked adds multiple spans without acquiring the lock.
// MUST be called with s.mu held.
// Used by the Appender interface for transactional ingestion.
func (s *ArrowStorage) addSpansLocked(spans []*span.Span) error {
	if len(spans) == 0 {
		return nil
	}

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
		lastRecord := s.records[currentRecordIndex]
		// Use actual NumRows instead of assuming batchSize
		// A partial batch flush (via Flush()) will have fewer than batchSize rows
		currentRowInBuilder = int(lastRecord.NumRows()) - 1
	} else {
		// Span is in the builder at the current position
		currentRecordIndex = len(s.records)
		currentRowInBuilder = s.builder.currentRowCount - 1
	}

	s.idx.AddSpan(sp, currentRecordIndex, currentRowInBuilder, nil) // No attrRef for in-memory storage
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
// Returns a defensive copy of the slice to prevent concurrent modification issues
func (s *ArrowStorage) GetRecords() []arrow.RecordBatch {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Return a copy of the slice (not the records themselves, as they're immutable)
	copied := make([]arrow.RecordBatch, len(s.records))
	copy(copied, s.records)
	return copied
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
		return nil, fmt.Errorf("span %s: invalid record index %d (have %d records)", spanID, ref.RecordIndex, len(s.records))
	}

	record := s.records[ref.RecordIndex]

	// Validate row index bounds before extraction
	if ref.RowIndex < 0 || ref.RowIndex >= int(record.NumRows()) {
		return nil, fmt.Errorf("span %s: invalid row index %d (record has %d rows)", spanID, ref.RowIndex, record.NumRows())
	}

	return s.extractSpan(record, ref.RowIndex)
}

// extractSpan extracts a span from an Arrow record at the given row
func (s *ArrowStorage) extractSpan(record arrow.RecordBatch, rowIndex int) (*span.Span, error) {
	if rowIndex >= int(record.NumRows()) {
		return nil, fmt.Errorf("invalid row index %d", rowIndex)
	}

	// Validate schema has expected number of columns
	// Note: We have 12 columns now (10 original + 2 new indexing fields)
	// but the new fields (bucket1s, duration_bucket) don't need to be extracted
	// into the Span struct as they're derived for indexing purposes
	expectedColumns := 12
	if record.NumCols() < int64(expectedColumns) {
		return nil, fmt.Errorf("invalid schema: expected at least %d columns, got %d", expectedColumns, record.NumCols())
	}

	sp := &span.Span{}

	// Read trace_id_hi and trace_id_lo and format as hex string
	traceIDHi := record.Column(0).(*array.Uint64).Value(rowIndex)
	traceIDLo := record.Column(1).(*array.Uint64).Value(rowIndex)
	sp.TraceID = fmt.Sprintf("%016x%016x", traceIDHi, traceIDLo)

	// Read span_id and format as hex string
	spanIDVal := record.Column(2).(*array.Uint64).Value(rowIndex)
	sp.SpanID = fmt.Sprintf("%016x", spanIDVal)

	// Read parent_span_id and format as hex string (or empty if null)
	parentCol := record.Column(3).(*array.Uint64)
	if !parentCol.IsNull(rowIndex) {
		parentSpanIDVal := parentCol.Value(rowIndex)
		sp.ParentSpanID = fmt.Sprintf("%016x", parentSpanIDVal)
	}

	sp.Name = record.Column(4).(*array.String).Value(rowIndex)

	sp.StartTime = time.Unix(0, record.Column(5).(*array.Int64).Value(rowIndex))

	sp.EndTime = time.Unix(0, record.Column(6).(*array.Int64).Value(rowIndex))

	sp.Duration = record.Column(7).(*array.Int64).Value(rowIndex)

	sp.ServiceName = record.Column(8).(*array.String).Value(rowIndex)

	tagsCol := record.Column(9).(*array.Map)
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

// GetIsolationCoordinator returns the isolation coordinator for MVCC snapshot isolation.
// Returns nil if no isolation coordinator is configured (non-transactional mode).
func (s *ArrowStorage) GetIsolationCoordinator() *IsolationCoordinator {
	return s.isolation
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

// SetTransactionDependencies configures the dependencies needed for transactional ingestion.
// Must be called before using BeginTransaction().
func (s *ArrowStorage) SetTransactionDependencies(
	isolation *IsolationCoordinator,
	linkStorage *ArrowLinkStorage,
	wal WAL,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isolation = isolation
	s.linkStorage = linkStorage
	s.wal = wal
}

// BeginTransaction creates a new transactional appender for ingesting spans.
// Each appender represents a single atomic transaction (typically one OTLP batch).
//
// Usage:
//
//	appender := storage.BeginTransaction()
//	for _, span := range otlpBatch {
//	    appender.AddSpan(span)
//	}
//	if err := appender.Commit(); err != nil {
//	    appender.Rollback()
//	}
func (s *ArrowStorage) BeginTransaction() Appender {
	// No locks needed - just creating a new appender
	// The appender will acquire locks when needed
	return NewArrowAppender(s.isolation, s, s.linkStorage, s.wal)
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

// MetadataSnapshot holds a consistent snapshot of storage metadata
type MetadataSnapshot struct {
	RowCount      int64
	RecordCount   int
	MinTime       int64
	MaxTime       int64
	MinWALSegment int
	MaxWALSegment int
}

// GetMetadataSnapshot returns a consistent snapshot of all metadata under a single lock
// This prevents inconsistencies that could occur from calling individual getters separately
func (s *ArrowStorage) GetMetadataSnapshot() MetadataSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return MetadataSnapshot{
		RowCount:      s.rowCount,
		RecordCount:   len(s.records),
		MinTime:       s.minTime,
		MaxTime:       s.maxTime,
		MinWALSegment: s.minWALSegment,
		MaxWALSegment: s.maxWALSegment,
	}
}

// Reset clears all data in the head block (called after flushing to disk)
func (s *ArrowStorage) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Release existing records
	for _, record := range s.records {
		record.Release()
	}

	s.records = make([]arrow.RecordBatch, 0)
	s.rowCount = 0
	s.minTime = 0
	s.maxTime = 0
	s.minWALSegment = -1 // Reset WAL segment tracking
	s.maxWALSegment = -1 // Reset WAL segment tracking

	s.builder.Release()
	s.builder = NewSpanRecordBuilder(s.mem, s.schema)

	s.idx = index.NewIndex()

	// Notify block manager that head block is now empty
	// Without this, block manager would still think head has the old time range
	if s.blockManager != nil {
		s.blockManager.UpdateHeadTimeRange(0, 0)
	}
}
