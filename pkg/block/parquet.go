package block

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"

	"github.com/saswatamcode/artemis/pkg/index"
	"github.com/saswatamcode/artemis/pkg/span"
)

const (
	parquetDataFilename = "spans.parquet"
)

// ParquetSpan is the Parquet-optimized representation of a span
// Using struct tags to configure Parquet encoding
// Compression is configured at the writer level, not per-field
type ParquetSpan struct {
	TraceIDHi      uint64            `parquet:"trace_id_hi"`
	TraceIDLo      uint64            `parquet:"trace_id_lo"`
	SpanID         uint64            `parquet:"span_id"`
	ParentSpanID   uint64            `parquet:"parent_span_id,optional"`
	Name           string            `parquet:"name,dict"`
	StartTime      int64             `parquet:"start_time,delta"` // nanoseconds since epoch
	EndTime        int64             `parquet:"end_time,delta"`   // nanoseconds since epoch
	Duration       int64             `parquet:"duration,delta"`   // nanoseconds
	ServiceName    string            `parquet:"service_name,dict"`
	Tags           map[string]string `parquet:"tags,optional" parquet-key:"dict" parquet-value:"dict"`
	Bucket1s       int64             `parquet:"bucket1s"`        // per-second bucket of start time
	DurationNs     int64             `parquet:"duration_ns"`     // duration in nanoseconds
	DurationBucket int32             `parquet:"duration_bucket"` // exponential duration bucket
}

// ParquetSpanMetadata is a metadata-only projection
// Useful for queries that only need span metadata without tags
type ParquetSpanMetadata struct {
	TraceIDHi      uint64 `parquet:"trace_id_hi"`
	TraceIDLo      uint64 `parquet:"trace_id_lo"`
	SpanID         uint64 `parquet:"span_id"`
	ParentSpanID   uint64 `parquet:"parent_span_id,optional"`
	Name           string `parquet:"name,dict"`
	StartTime      int64  `parquet:"start_time,delta"`
	EndTime        int64  `parquet:"end_time,delta"`
	Duration       int64  `parquet:"duration,delta"`
	ServiceName    string `parquet:"service_name,dict"`
	Bucket1s       int64  `parquet:"bucket1s"`
	DurationNs     int64  `parquet:"duration_ns"`
	DurationBucket int32  `parquet:"duration_bucket"`
}

// ParquetBlock represents a block stored as Parquet (compacted data)
type ParquetBlock struct {
	meta   *BlockMeta
	dir    string
	file   *parquet.File // Parquet file handle for page-level access
	osFile *os.File      // Underlying OS file (must be closed)
	index  *index.Index
}

// NewParquetBlock creates a new Parquet block from disk
func NewParquetBlock(dir string) (*ParquetBlock, error) {
	metaPath := filepath.Join(dir, metaFilename)
	metaData, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var meta BlockMeta
	if err := json.Unmarshal(metaData, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	pb := &ParquetBlock{
		meta: &meta,
		dir:  dir,
	}

	if err := pb.openParquet(); err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	if err := pb.loadIndex(); err != nil {
		slog.Default().Warn("failed to load index for parquet block",
			slog.String("block_dir", dir),
			slog.String("error", err.Error()))
	}

	return pb, nil
}

// openParquet opens the Parquet file for reading
func (pb *ParquetBlock) openParquet() error {
	dataPath := filepath.Join(pb.dir, parquetDataFilename)

	f, err := os.Open(dataPath)
	if err != nil {
		return fmt.Errorf("failed to open parquet file: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("failed to stat parquet file: %w", err)
	}

	file, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		f.Close()
		return fmt.Errorf("failed to open parquet file: %w", err)
	}

	pb.file = file
	pb.osFile = f // Store OS file handle so we can close it later

	return nil
}

// loadIndex loads the index from disk
func (pb *ParquetBlock) loadIndex() error {
	indexPath := filepath.Join(pb.dir, indexFilename)

	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		return fmt.Errorf("index file not found")
	}

	indexData, err := os.ReadFile(indexPath)
	if err != nil {
		return fmt.Errorf("failed to read index file: %w", err)
	}

	var serialized index.SerializedIndex
	if err := json.Unmarshal(indexData, &serialized); err != nil {
		return fmt.Errorf("failed to unmarshal index: %w", err)
	}

	pb.index = index.NewIndexFromSerialized(&serialized)
	return nil
}

// Meta returns the block metadata
func (pb *ParquetBlock) Meta() *BlockMeta {
	return pb.meta
}

// Index returns the block's index (may be nil if not loaded)
func (pb *ParquetBlock) Index() *index.Index {
	return pb.index
}

// HasIndex returns true if the block has an index loaded
func (pb *ParquetBlock) HasIndex() bool {
	return pb.index != nil
}

// Dir returns the directory path of this block
func (pb *ParquetBlock) Dir() string {
	return pb.dir
}

// GetSpanByID retrieves a span by ID using the index
// Uses page-level seeking via OffsetIndex (GenericRowGroupReader.SeekToRow uses it internally)
func (pb *ParquetBlock) GetSpanByID(spanID string) (*span.Span, error) {
	if !pb.HasIndex() {
		return nil, fmt.Errorf("block has no index")
	}

	ref, ok := pb.index.LookupSpanID(spanID)
	if !ok {
		return nil, fmt.Errorf("span %s not found", spanID)
	}

	return pb.readSpanAt(ref.RecordIndex, ref.RowIndex)
}

// readSpanAt reads a single span using PAGE-LEVEL access with OffsetIndex
// This is the most efficient way to read a single row from Parquet:
// 1. Create a reader for ONLY the specific row group (not the whole file)
// 2. SeekToRow uses OffsetIndex internally to jump to the right page in each column
// 3. Read exactly one row
func (pb *ParquetBlock) readSpanAt(rowGroupIdx, rowIdxInGroup int) (*span.Span, error) {
	rowGroups := pb.file.RowGroups()
	if rowGroupIdx >= len(rowGroups) {
		return nil, fmt.Errorf("invalid row group index: %d", rowGroupIdx)
	}

	// Get the specific row group
	rowGroup := rowGroups[rowGroupIdx]

	// Create a reader for ONLY this row group (not the whole file)
	// This is efficient because we only initialize page readers for one row group
	reader := parquet.NewGenericRowGroupReader[ParquetSpan](rowGroup)
	defer reader.Close()

	// Seek to the specific row within this row group
	// SeekToRow uses OffsetIndex internally to jump to the right page in each column
	if err := reader.SeekToRow(int64(rowIdxInGroup)); err != nil {
		return nil, fmt.Errorf("failed to seek to row: %w", err)
	}

	// Read exactly one row
	batch := make([]ParquetSpan, 1)
	n, err := reader.Read(batch)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n == 0 {
		return nil, fmt.Errorf("no data at row")
	}

	return parquetSpanToSpan(&batch[0]), nil
}

// ReadAll reads all spans from the Parquet block (for full scans)
// Memory efficient: Uses streaming reader, doesn't load all spans at once
func (pb *ParquetBlock) ReadAll() ([]*span.Span, error) {
	reader := parquet.NewGenericReader[ParquetSpan](pb.file)
	defer reader.Close()

	totalSpans := make([]*span.Span, 0, pb.file.NumRows())
	batch := make([]ParquetSpan, 1024) // Read in batches of 1024

	for {
		n, err := reader.Read(batch)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read spans: %w", err)
		}

		for i := range n {
			totalSpans = append(totalSpans, parquetSpanToSpan(&batch[i]))
		}

		if err == io.EOF {
			break
		}
	}

	return totalSpans, nil
}

// GetSpansBatch efficiently retrieves multiple spans by ID
// Optimized PAGE-LEVEL reads:
// 1. Group spans by row group
// 2. Create one reader per row group (minimizes memory)
// 3. Within each row group, seek to each row using OffsetIndex
// 4. Each SeekToRow jumps to the right page, not entire row group
func (pb *ParquetBlock) GetSpansBatch(spanIDs []string) ([]*span.Span, error) {
	if !pb.HasIndex() {
		return nil, fmt.Errorf("block has no index")
	}

	// Group lookups by row group
	type rowLookup struct {
		rowGroupIdx int
		rowIdx      int
	}

	lookupsByRowGroup := make(map[int][]rowLookup)
	for _, spanID := range spanIDs {
		ref, ok := pb.index.LookupSpanID(spanID)
		if !ok {
			continue // Skip spans not found
		}

		lookupsByRowGroup[ref.RecordIndex] = append(lookupsByRowGroup[ref.RecordIndex], rowLookup{
			rowGroupIdx: ref.RecordIndex,
			rowIdx:      ref.RowIndex,
		})
	}

	if len(lookupsByRowGroup) == 0 {
		return nil, nil
	}

	results := make([]*span.Span, 0, len(spanIDs))
	rowGroups := pb.file.RowGroups()

	// Process each row group separately
	for rgIdx, lookups := range lookupsByRowGroup {
		if rgIdx >= len(rowGroups) {
			continue
		}

		// Sort lookups within this row group by row index for sequential reads
		sort.Slice(lookups, func(i, j int) bool {
			return lookups[i].rowIdx < lookups[j].rowIdx
		})

		// Create a reader for ONLY this row group
		rowGroup := rowGroups[rgIdx]
		reader := parquet.NewGenericRowGroupReader[ParquetSpan](rowGroup)

		// Use anonymous function to ensure reader.Close() is called at end of each iteration
		// defer in a loop doesn't execute until function returns, causing resource leaks
		func() {
			defer reader.Close()

			// Read each span from this row group
			currentRow := int64(-1)
			batch := make([]ParquetSpan, 1)

			for _, lookup := range lookups {
				// Only seek if we're not already at the right position
				if currentRow != int64(lookup.rowIdx) {
					if err := reader.SeekToRow(int64(lookup.rowIdx)); err != nil {
						continue // Skip this span on seek error
					}
					currentRow = int64(lookup.rowIdx)
				}

				n, err := reader.Read(batch)
				if err != nil && err != io.EOF {
					continue // Skip this span on read error
				}
				if n == 0 {
					continue
				}

				results = append(results, parquetSpanToSpan(&batch[0]))
				currentRow++ // We've read one row
			}
		}()
	}

	return results, nil
}

// RowReference identifies a specific span by row group and row index
type RowReference struct {
	RowGroupIdx int
	RowIdx      int
}

// ScanMetadata scans using metadata projection and returns row references for matches
// This is much more efficient than ReadAll() for filtering on top-level fields
// filterFunc receives metadata and returns true if the row matches
func (pb *ParquetBlock) ScanMetadata(filterFunc func(*ParquetSpanMetadata) bool) ([]RowReference, error) {
	// Use metadata projection (excludes tags - faster!)
	reader := parquet.NewGenericReader[ParquetSpanMetadata](pb.file)
	defer reader.Close()

	var matches []RowReference
	batch := make([]ParquetSpanMetadata, 1024) // Read in batches of 1024

	rowGroups := pb.file.RowGroups()
	globalRowIdx := int64(0)

	for {
		n, err := reader.Read(batch)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read metadata: %w", err)
		}

		for i := range n {
			if filterFunc(&batch[i]) {
				// Find which row group this row belongs to
				rowGroupIdx, localRowIdx := pb.findRowGroup(globalRowIdx+int64(i), rowGroups)
				matches = append(matches, RowReference{
					RowGroupIdx: rowGroupIdx,
					RowIdx:      localRowIdx,
				})
			}
		}

		globalRowIdx += int64(n)

		if err == io.EOF {
			break
		}
	}

	return matches, nil
}

// findRowGroup finds the row group index and local row index for a global row index
func (pb *ParquetBlock) findRowGroup(globalRowIdx int64, rowGroups []parquet.RowGroup) (int, int) {
	currentRow := int64(0)
	for rgIdx, rg := range rowGroups {
		numRows := rg.NumRows()
		if globalRowIdx < currentRow+numRows {
			return rgIdx, int(globalRowIdx - currentRow)
		}
		currentRow += numRows
	}
	// Shouldn't happen, but return last row group
	lastRG := len(rowGroups) - 1
	return lastRG, int(globalRowIdx - currentRow)
}

// getAbsoluteRowNumber converts a row group index and local row index to an absolute row number
func (pb *ParquetBlock) getAbsoluteRowNumber(rowGroupIdx, rowIdx int) int64 {
	rowGroups := pb.file.RowGroups()
	if rowGroupIdx >= len(rowGroups) {
		return -1
	}

	absoluteRow := int64(0)
	for i := 0; i < rowGroupIdx; i++ {
		absoluteRow += rowGroups[i].NumRows()
	}
	absoluteRow += int64(rowIdx)
	return absoluteRow
}

// GetSpansByRowReferences efficiently fetches spans for a list of row references
// Optimized: Converts to absolute row numbers and uses direct seeks
func (pb *ParquetBlock) GetSpansByRowReferences(refs []RowReference) ([]*span.Span, error) {
	if len(refs) == 0 {
		return nil, nil
	}

	// Convert row references to absolute row numbers
	type absoluteLookup struct {
		absoluteRow int64
	}

	lookups := make([]absoluteLookup, 0, len(refs))
	for _, ref := range refs {
		absoluteRow := pb.getAbsoluteRowNumber(ref.RowGroupIdx, ref.RowIdx)
		if absoluteRow >= 0 {
			lookups = append(lookups, absoluteLookup{absoluteRow: absoluteRow})
		}
	}

	if len(lookups) == 0 {
		return nil, nil
	}

	// Sort by absolute row number for sequential reads
	sort.Slice(lookups, func(i, j int) bool {
		return lookups[i].absoluteRow < lookups[j].absoluteRow
	})

	// Use single reader and seek to each position
	reader := parquet.NewGenericReader[ParquetSpan](pb.file)
	defer reader.Close()

	results := make([]*span.Span, 0, len(lookups))
	currentRow := int64(-1)
	batch := make([]ParquetSpan, 1)

	for _, lookup := range lookups {
		if currentRow != lookup.absoluteRow {
			if err := reader.SeekToRow(lookup.absoluteRow); err != nil {
				continue
			}
			currentRow = lookup.absoluteRow
		}

		n, err := reader.Read(batch)
		if err != nil && err != io.EOF {
			continue
		}
		if n == 0 {
			continue
		}

		results = append(results, parquetSpanToSpan(&batch[0]))
		currentRow++
	}

	return results, nil
}

// Close releases resources held by this block
// This method is idempotent - it can be called multiple times safely
func (pb *ParquetBlock) Close() error {
	var closeErr error

	// Close the underlying OS file handle
	if pb.osFile != nil {
		closeErr = pb.osFile.Close()
		// Always nil out the fields to make Close() idempotent
		// and prevent file handle leaks even if Close() fails
		pb.osFile = nil
	}
	pb.file = nil

	if closeErr != nil {
		return fmt.Errorf("failed to close parquet file: %w", closeErr)
	}
	return nil
}

// GetTraceByID retrieves all spans for a given trace ID
// Uses index for efficient lookup
func (pb *ParquetBlock) GetTraceByID(traceID string) ([]*span.Span, error) {
	if !pb.HasIndex() {
		// Fallback to metadata scan
		return pb.scanByTraceID(traceID)
	}

	spanIDs := pb.index.LookupByTraceID(traceID)
	return pb.GetSpansBatch(spanIDs)
}

// GetSpansByTag retrieves all spans that have a specific tag key-value pair
// Uses index for efficient lookup
func (pb *ParquetBlock) GetSpansByTag(tagKey, tagValue string) ([]*span.Span, error) {
	if !pb.HasIndex() {
		// Fallback to full scan (tags require full span data)
		return pb.scanByTag(tagKey, tagValue)
	}

	spanIDs := pb.index.LookupByTag(tagKey, tagValue)
	return pb.GetSpansBatch(spanIDs)
}

// scanByTraceID is a fallback metadata scan when no index is available
func (pb *ParquetBlock) scanByTraceID(traceID string) ([]*span.Span, error) {
	// Parse the trace ID into hi/lo components
	traceIDHi, traceIDLo, err := span.ParseTraceID(traceID)
	if err != nil {
		return nil, fmt.Errorf("invalid trace ID: %w", err)
	}

	filterFunc := func(meta *ParquetSpanMetadata) bool {
		return meta.TraceIDHi == traceIDHi && meta.TraceIDLo == traceIDLo
	}

	refs, err := pb.ScanMetadata(filterFunc)
	if err != nil {
		return nil, err
	}

	return pb.GetSpansByRowReferences(refs)
}

// scanByTag is a fallback full scan when no index is available
// Note: This requires reading full span data since tags aren't in metadata projection
func (pb *ParquetBlock) scanByTag(tagKey, tagValue string) ([]*span.Span, error) {
	allSpans, err := pb.ReadAll()
	if err != nil {
		return nil, err
	}

	result := make([]*span.Span, 0)
	for _, sp := range allSpans {
		if sp.Tags != nil && sp.Tags[tagKey] == tagValue {
			result = append(result, sp)
		}
	}

	return result, nil
}

// spanToParquetSpan converts a Span to ParquetSpan
func spanToParquetSpan(s *span.Span) *ParquetSpan {
	// Parse trace ID into hi/lo components
	traceIDHi, traceIDLo, err := span.ParseTraceID(s.TraceID)
	if err != nil {
		// If parsing fails, use 0 values
		traceIDHi, traceIDLo = 0, 0
	}

	// Parse span ID
	spanID, err := span.ParseSpanID(s.SpanID)
	if err != nil {
		spanID = 0
	}

	// Parse parent span ID (0 if empty)
	parentSpanID := uint64(0)
	if s.ParentSpanID != "" {
		parentSpanID, err = span.ParseSpanID(s.ParentSpanID)
		if err != nil {
			parentSpanID = 0
		}
	}

	startUnixNano := s.StartTime.UnixNano()
	durationNs := s.GetDuration()

	// Calculate bucket1s: round down start time to the second
	const sec = int64(1_000_000_000)
	bucket := startUnixNano - (startUnixNano % sec)

	// Calculate duration bucket using exponential bucketing
	var durationBucketVal int32
	if durationNs <= 0 {
		durationBucketVal = 0
	} else {
		durationBucketVal = int32(bits.Len64(uint64(durationNs)) - 1)
	}

	return &ParquetSpan{
		TraceIDHi:      traceIDHi,
		TraceIDLo:      traceIDLo,
		SpanID:         spanID,
		ParentSpanID:   parentSpanID,
		Name:           s.Name,
		StartTime:      startUnixNano,
		EndTime:        s.EndTime.UnixNano(),
		Duration:       durationNs,
		ServiceName:    s.ServiceName,
		Tags:           s.Tags,
		Bucket1s:       bucket,
		DurationNs:     durationNs,
		DurationBucket: durationBucketVal,
	}
}

// parquetSpanToSpan converts a ParquetSpan to Span
func parquetSpanToSpan(ps *ParquetSpan) *span.Span {
	// Convert trace ID from hi/lo to hex string
	traceID := fmt.Sprintf("%016x%016x", ps.TraceIDHi, ps.TraceIDLo)

	// Convert span ID to hex string
	spanID := fmt.Sprintf("%016x", ps.SpanID)

	// Convert parent span ID to hex string (empty if 0)
	parentSpanID := ""
	if ps.ParentSpanID != 0 {
		parentSpanID = fmt.Sprintf("%016x", ps.ParentSpanID)
	}

	return &span.Span{
		TraceID:      traceID,
		SpanID:       spanID,
		ParentSpanID: parentSpanID,
		Name:         ps.Name,
		StartTime:    time.Unix(0, ps.StartTime),
		EndTime:      time.Unix(0, ps.EndTime),
		Duration:     ps.Duration,
		ServiceName:  ps.ServiceName,
		Tags:         ps.Tags,
	}
}

// GetEventsBySpanID retrieves all events for a given span ID from this block
func (pb *ParquetBlock) GetEventsBySpanID(spanID string) ([]*span.SpanEvent, error) {
	return GetEventsBySpanIDFromParquet(pb.dir, spanID)
}

// ReadAllEvents reads all events from this block
func (pb *ParquetBlock) ReadAllEvents() ([]*span.SpanEvent, error) {
	return ReadParquetEvents(pb.dir)
}

// GetLinksBySpanID retrieves all links for a given span ID from this block
func (pb *ParquetBlock) GetLinksBySpanID(spanID string) ([]*span.SpanLink, error) {
	return GetLinksBySpanIDFromParquet(pb.dir, spanID)
}

// ReadAllLinks reads all links from this block
func (pb *ParquetBlock) ReadAllLinks() ([]*span.SpanLink, error) {
	return ReadParquetLinks(pb.dir)
}

// WriteParquetBlock writes spans to a Parquet file with optimizations
// Uses atomic write with temporary directory to prevent corruption
func WriteParquetBlock(dir string, meta *BlockMeta, spans []*span.Span, idx *index.Index) error {
	// CRITICAL: Write to temporary directory first for atomicity
	tmpDir := dir + ".tmp"

	// Clean up any existing temp directory
	os.RemoveAll(tmpDir)

	// Create temporary block directory
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp block directory: %w", err)
	}

	metaPath := filepath.Join(tmpDir, metaFilename)
	metaData, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	if err := atomicWriteFile(metaPath, metaData); err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	if idx != nil {
		indexPath := filepath.Join(tmpDir, indexFilename)
		serialized := idx.Serialize()
		indexData, err := json.MarshalIndent(serialized, "", "  ")
		if err != nil {
			os.RemoveAll(tmpDir)
			return fmt.Errorf("failed to marshal index: %w", err)
		}
		if err := atomicWriteFile(indexPath, indexData); err != nil {
			os.RemoveAll(tmpDir)
			return fmt.Errorf("failed to write index: %w", err)
		}
	}

	parquetSpans := make([]ParquetSpan, len(spans))
	for i, s := range spans {
		parquetSpans[i] = *spanToParquetSpan(s)
	}

	dataPath := filepath.Join(tmpDir, parquetDataFilename)
	f, err := os.Create(dataPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to create parquet file: %w", err)
	}

	writer := parquet.NewGenericWriter[ParquetSpan](
		f,
		parquet.Compression(&snappy.Codec{}),
		parquet.PageBufferSize(100*1024*1024),
	)

	_, err = writer.Write(parquetSpans)
	if err != nil {
		writer.Close()
		f.Close()
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to write parquet data: %w", err)
	}

	if err := writer.Close(); err != nil {
		f.Close()
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to close writer: %w", err)
	}

	// CRITICAL: Fsync data file
	if err := f.Sync(); err != nil {
		f.Close()
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to sync parquet file: %w", err)
	}
	f.Close()

	// Fsync directory
	if err := fsyncDir(tmpDir); err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to sync temp directory: %w", err)
	}

	// ATOMIC: Rename to final location
	if err := os.Rename(tmpDir, dir); err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to rename block directory: %w", err)
	}

	// Fsync parent directory
	parentDir := filepath.Dir(dir)
	if err := fsyncDir(parentDir); err != nil {
		slog.Default().Warn("failed to sync parent directory",
			slog.String("error", err.Error()))
	}

	return nil
}
