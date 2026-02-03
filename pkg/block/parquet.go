package block

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
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
	TraceID      string            `parquet:"trace_id,dict"`
	SpanID       string            `parquet:"span_id"`
	ParentSpanID string            `parquet:"parent_span_id,optional,dict"`
	Name         string            `parquet:"name,dict"`
	StartTime    int64             `parquet:"start_time,delta"` // nanoseconds since epoch
	EndTime      int64             `parquet:"end_time,delta"`   // nanoseconds since epoch
	Duration     int64             `parquet:"duration,delta"`   // nanoseconds
	ServiceName  string            `parquet:"service_name,dict"`
	Tags         map[string]string `parquet:"tags,optional" parquet-key:"dict" parquet-value:"dict"`
}

// ParquetSpanMetadata is a metadata-only projection
// Useful for queries that only need span metadata without tags
type ParquetSpanMetadata struct {
	TraceID      string `parquet:"trace_id,dict"`
	SpanID       string `parquet:"span_id"`
	ParentSpanID string `parquet:"parent_span_id,optional,dict"`
	Name         string `parquet:"name,dict"`
	StartTime    int64  `parquet:"start_time,delta"`
	EndTime      int64  `parquet:"end_time,delta"`
	Duration     int64  `parquet:"duration,delta"`
	ServiceName  string `parquet:"service_name,dict"`
}

// ParquetBlock represents a block stored as Parquet (compacted data)
type ParquetBlock struct {
	meta          *BlockMeta
	dir           string
	file          *parquet.File // Parquet file handle for metadata and row group access
	osFile        *os.File      // Underlying OS file (must be closed)
	index         *index.Index
	rowGroupCache map[int][]*span.Span // Cache for row groups (key = row group index)
	cacheMu       sync.RWMutex         // Protects rowGroupCache
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

// ParquetSchema returns the Parquet schema
func (pb *ParquetBlock) ParquetSchema() *parquet.Schema {
	if pb.file != nil {
		return pb.file.Schema()
	}
	return nil
}

// Schema returns nil for Parquet blocks (Arrow-specific)
// Implements Block interface
func (pb *ParquetBlock) Schema() *arrow.Schema {
	return nil
}

// Records returns nil for Parquet blocks (Arrow-specific)
// Implements Block interface
func (pb *ParquetBlock) Records() []arrow.Record {
	return nil
}

// AsArrowRecords reads Parquet file and converts to Arrow records natively
// Uses Apache Arrow's pqarrow package to avoid double conversion (Parquet → Span → Arrow)
// This is the efficient path for FlightSQL queries on Parquet blocks
func (pb *ParquetBlock) AsArrowRecords() ([]arrow.Record, error) {
	dataPath := filepath.Join(pb.dir, parquetDataFilename)

	// Open Parquet file using Apache Arrow's reader
	rdr, err := file.OpenParquetFile(dataPath, false)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file with arrow: %w", err)
	}
	defer rdr.Close()

	// Create Arrow file reader using pqarrow
	mem := memory.NewGoAllocator()
	arrowReader, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader: %w", err)
	}

	// Read entire table
	table, err := arrowReader.ReadTable(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet as arrow table: %w", err)
	}
	defer table.Release()

	// Convert table to records
	// A table is a collection of chunked arrays, we need to convert to records
	tr := array.NewTableReader(table, -1) // -1 means read all rows at once
	defer tr.Release()

	var records []arrow.Record
	for tr.Next() {
		rec := tr.Record()
		rec.Retain() // Retain so it's not freed when reader is released
		records = append(records, rec)
	}

	if err := tr.Err(); err != nil {
		// Release any records we've collected
		for _, rec := range records {
			rec.Release()
		}
		return nil, fmt.Errorf("error reading table records: %w", err)
	}

	return records, nil
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

// readSpanAt reads a span from a specific row group and row index
// Uses row group caching for O(1) access after first read
func (pb *ParquetBlock) readSpanAt(rowGroupIdx, rowIdx int) (*span.Span, error) {
	pb.cacheMu.RLock()
	cached := pb.rowGroupCache[rowGroupIdx]
	pb.cacheMu.RUnlock()

	if cached != nil {
		// Cache hit
		if rowIdx < len(cached) {
			return cached[rowIdx], nil
		}
		return nil, fmt.Errorf("row index %d out of bounds for row group %d", rowIdx, rowGroupIdx)
	}

	// Cache miss - read entire row group at once
	spans, err := pb.readRowGroup(rowGroupIdx)
	if err != nil {
		return nil, err
	}

	// Cache the row group for future reads
	pb.cacheMu.Lock()
	if pb.rowGroupCache == nil {
		pb.rowGroupCache = make(map[int][]*span.Span)
	}
	pb.rowGroupCache[rowGroupIdx] = spans
	pb.cacheMu.Unlock()

	if rowIdx < len(spans) {
		return spans[rowIdx], nil
	}

	return nil, fmt.Errorf("row index %d out of bounds for row group %d", rowIdx, rowGroupIdx)
}

// readRowGroup reads an entire row group at once using batch reading
// This is much more efficient than reading rows one-by-one
func (pb *ParquetBlock) readRowGroup(rowGroupIdx int) ([]*span.Span, error) {
	rowGroups := pb.file.RowGroups()
	if rowGroupIdx >= len(rowGroups) {
		return nil, fmt.Errorf("invalid row group index: %d", rowGroupIdx)
	}

	rowGroup := rowGroups[rowGroupIdx]
	numRows := int(rowGroup.NumRows())

	// Use row group reader for efficient batch reading
	reader := parquet.NewRowGroupReader(rowGroup)
	defer reader.Close()

	spans := make([]*span.Span, 0, numRows)
	for {
		parquetSpan := ParquetSpan{}
		err := reader.Read(&parquetSpan)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read row: %w", err)
		}

		spans = append(spans, parquetSpanToSpan(&parquetSpan))
	}

	return spans, nil
}

// ReadAll reads all spans from the Parquet block (for full scans)
// Optimized to read row groups in batch
func (pb *ParquetBlock) ReadAll() ([]*span.Span, error) {
	rowGroups := pb.file.RowGroups()
	totalSpans := make([]*span.Span, 0, pb.file.NumRows())

	for i := range rowGroups {
		spans, err := pb.readRowGroup(i)
		if err != nil {
			return nil, fmt.Errorf("failed to read row group %d: %w", i, err)
		}
		totalSpans = append(totalSpans, spans...)
	}

	return totalSpans, nil
}

// GetSpansBatch efficiently retrieves multiple spans by ID
// Groups reads by row group to minimize I/O operations
func (pb *ParquetBlock) GetSpansBatch(spanIDs []string) ([]*span.Span, error) {
	if !pb.HasIndex() {
		return nil, fmt.Errorf("block has no index")
	}

	type lookup struct {
		rowGroupIdx int
		rowIdx      int
		spanID      string
	}

	lookups := make([]lookup, 0, len(spanIDs))
	for _, spanID := range spanIDs {
		ref, ok := pb.index.LookupSpanID(spanID)
		if !ok {
			continue // Skip spans not found
		}
		lookups = append(lookups, lookup{
			rowGroupIdx: ref.RecordIndex,
			rowIdx:      ref.RowIndex,
			spanID:      spanID,
		})
	}

	if len(lookups) == 0 {
		return nil, nil
	}

	// Sort by row group to read each row group only once
	sort.Slice(lookups, func(i, j int) bool {
		if lookups[i].rowGroupIdx != lookups[j].rowGroupIdx {
			return lookups[i].rowGroupIdx < lookups[j].rowGroupIdx
		}
		return lookups[i].rowIdx < lookups[j].rowIdx
	})

	// Read spans grouped by row group
	results := make([]*span.Span, 0, len(lookups))
	currentRowGroup := -1
	var currentSpans []*span.Span

	for _, lookup := range lookups {
		if lookup.rowGroupIdx != currentRowGroup {
			// Read new row group (will use cache if already loaded)
			var err error
			currentSpans, err = pb.readRowGroup(lookup.rowGroupIdx)
			if err != nil {
				return nil, fmt.Errorf("failed to read row group %d: %w", lookup.rowGroupIdx, err)
			}
			currentRowGroup = lookup.rowGroupIdx
		}

		if lookup.rowIdx < len(currentSpans) {
			results = append(results, currentSpans[lookup.rowIdx])
		}
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

// GetSpansByRowReferences efficiently fetches spans for a list of row references
// Groups reads by row group to minimize I/O
func (pb *ParquetBlock) GetSpansByRowReferences(refs []RowReference) ([]*span.Span, error) {
	if len(refs) == 0 {
		return nil, nil
	}

	sortedRefs := make([]RowReference, len(refs))
	copy(sortedRefs, refs)
	sort.Slice(sortedRefs, func(i, j int) bool {
		if sortedRefs[i].RowGroupIdx != sortedRefs[j].RowGroupIdx {
			return sortedRefs[i].RowGroupIdx < sortedRefs[j].RowGroupIdx
		}
		return sortedRefs[i].RowIdx < sortedRefs[j].RowIdx
	})

	results := make([]*span.Span, 0, len(sortedRefs))
	currentRowGroup := -1
	var currentSpans []*span.Span

	for _, ref := range sortedRefs {
		if ref.RowGroupIdx != currentRowGroup {
			// Read new row group (will use cache if already loaded)
			var err error
			currentSpans, err = pb.readRowGroup(ref.RowGroupIdx)
			if err != nil {
				return nil, fmt.Errorf("failed to read row group %d: %w", ref.RowGroupIdx, err)
			}
			currentRowGroup = ref.RowGroupIdx
		}

		if ref.RowIdx < len(currentSpans) {
			results = append(results, currentSpans[ref.RowIdx])
		}
	}

	return results, nil
}

// Close releases resources held by this block
func (pb *ParquetBlock) Close() error {
	// Close the underlying OS file handle
	if pb.osFile != nil {
		if err := pb.osFile.Close(); err != nil {
			return fmt.Errorf("failed to close parquet file: %w", err)
		}
		pb.osFile = nil
	}
	pb.file = nil
	pb.rowGroupCache = nil
	return nil
}

// spanToParquetSpan converts a Span to ParquetSpan
func spanToParquetSpan(s *span.Span) *ParquetSpan {
	return &ParquetSpan{
		TraceID:      s.TraceID,
		SpanID:       s.SpanID,
		ParentSpanID: s.ParentSpanID,
		Name:         s.Name,
		StartTime:    s.StartTime.UnixNano(),
		EndTime:      s.EndTime.UnixNano(),
		Duration:     s.GetDuration(),
		ServiceName:  s.ServiceName,
		Tags:         s.Tags,
	}
}

// parquetSpanToSpan converts a ParquetSpan to Span
func parquetSpanToSpan(ps *ParquetSpan) *span.Span {
	return &span.Span{
		TraceID:      ps.TraceID,
		SpanID:       ps.SpanID,
		ParentSpanID: ps.ParentSpanID,
		Name:         ps.Name,
		StartTime:    time.Unix(0, ps.StartTime),
		EndTime:      time.Unix(0, ps.EndTime),
		Duration:     ps.Duration,
		ServiceName:  ps.ServiceName,
		Tags:         ps.Tags,
	}
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
