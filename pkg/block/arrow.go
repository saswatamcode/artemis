package block

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/saswatamcode/artemis/pkg/index"
	"github.com/saswatamcode/artemis/pkg/span"
)

const (
	metaFilename  = "meta.json"
	dataFilename  = "spans.arrow"
	indexFilename = "index.json"
)

// ArrowBlock represents a disk-based Arrow IPC block (L0 blocks)
// These are created by flushing the in-memory HeadBlock to disk
//
// Characteristics:
// - Immutable: Read-only after creation
// - Disk-based: Loads Arrow IPC file from directory
// - Indexed: Has inverted index for fast lookups
// - L0: First level in the storage hierarchy (not compacted)
//
// Arrow IPC format benefits:
// - Fast to write (no compression/encoding needed)
// - Fast to read (zero-copy memory mapping)
// - Schema-preserving (maintains Arrow types)
// - Good for recently flushed data (will be compacted to Parquet soon)
//
// Compare to:
// - HeadBlock: In-memory, mutable, active ingestion
// - ParquetBlock: Parquet format, L1+, better compression, page-level reads
type ArrowBlock struct {
	meta        *BlockMeta
	dir         string
	records     []arrow.RecordBatch
	mem         memory.Allocator
	schema      *arrow.Schema
	index       *index.Index
	linkRecords []arrow.RecordBatch // Link records (optional)
	linkSchema  *arrow.Schema       // Link schema (optional)
}

// NewArrowBlock creates a new Arrow block from disk
func NewArrowBlock(dir string) (*ArrowBlock, error) {
	metaPath := filepath.Join(dir, metaFilename)
	metaData, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var meta BlockMeta
	if err := json.Unmarshal(metaData, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	ab := &ArrowBlock{
		meta: &meta,
		dir:  dir,
		mem:  memory.NewGoAllocator(),
	}

	if err := ab.loadRecords(); err != nil {
		return nil, fmt.Errorf("failed to load records: %w", err)
	}

	// Load link records if they exist
	if err := ab.loadLinkRecords(); err != nil {
		// If links file exists but can't be read, that's a real error
		return nil, fmt.Errorf("failed to load link records: %w", err)
	}

	if ab.linkRecords != nil && len(ab.linkRecords) > 0 {
		// Count total links
		totalLinks := int64(0)
		for _, rec := range ab.linkRecords {
			totalLinks += rec.NumRows()
		}
		slog.Default().Info("loaded link records for Arrow block",
			slog.String("block_dir", dir),
			slog.Int("num_records", len(ab.linkRecords)),
			slog.Int64("total_links", totalLinks))
	}

	// Load index if it exists
	if err := ab.loadIndex(); err != nil {
		// Index is optional - if it doesn't exist, queries will be slower but still work
		slog.Default().Warn("failed to load index for Arrow block",
			slog.String("block_dir", dir),
			slog.String("error", err.Error()))
	}

	return ab, nil
}

// loadRecords loads Arrow records from the IPC file
func (ab *ArrowBlock) loadRecords() error {
	dataPath := filepath.Join(ab.dir, dataFilename)
	f, err := os.Open(dataPath)
	if err != nil {
		return fmt.Errorf("failed to open data file: %w", err)
	}
	defer f.Close()

	reader, err := ipc.NewFileReader(f, ipc.WithAllocator(ab.mem))
	if err != nil {
		return fmt.Errorf("failed to create IPC reader: %w", err)
	}
	defer reader.Close()

	ab.schema = reader.Schema()
	records := make([]arrow.RecordBatch, 0, reader.NumRecords())

	// Load records with cleanup on error
	for i := 0; i < reader.NumRecords(); i++ {
		rec, err := reader.RecordBatch(i)
		if err != nil {
			// Release any records we've already retained
			for _, r := range records {
				r.Release()
			}
			return fmt.Errorf("failed to read record %d: %w", i, err)
		}
		rec.Retain() // Retain the record so it's not freed when reader closes
		records = append(records, rec)
	}

	ab.records = records
	return nil
}

// loadLinkRecords loads link records from the links IPC file if it exists
func (ab *ArrowBlock) loadLinkRecords() error {
	linkRecords, linkSchema, err := loadLinkRecords(ab.dir, ab.mem)
	if err != nil {
		return err
	}
	ab.linkRecords = linkRecords
	ab.linkSchema = linkSchema
	return nil
}

// loadIndex loads the index from disk
func (ab *ArrowBlock) loadIndex() error {
	indexPath := filepath.Join(ab.dir, indexFilename)
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

	ab.index = index.NewIndexFromSerialized(&serialized)
	return nil
}

// Meta returns the block metadata
func (ab *ArrowBlock) Meta() *BlockMeta {
	return ab.meta
}

// Records returns all Arrow records in this block
func (ab *ArrowBlock) Records() []arrow.RecordBatch {
	return ab.records
}

// Schema returns the Arrow schema
func (ab *ArrowBlock) Schema() *arrow.Schema {
	return ab.schema
}

// Index returns the block's index (may be nil if not loaded)
func (ab *ArrowBlock) Index() *index.Index {
	return ab.index
}

// HasIndex returns true if the block has an index loaded
func (ab *ArrowBlock) HasIndex() bool {
	return ab.index != nil
}

// Dir returns the directory path of this block
func (ab *ArrowBlock) Dir() string {
	return ab.dir
}

// Close releases resources held by this block
func (ab *ArrowBlock) Close() error {
	for _, rec := range ab.records {
		rec.Release()
	}
	ab.records = nil

	for _, rec := range ab.linkRecords {
		rec.Release()
	}
	ab.linkRecords = nil
	return nil
}

// GetSpanByID retrieves a single span by ID using the index
func (ab *ArrowBlock) GetSpanByID(spanID string) (*span.Span, error) {
	if !ab.HasIndex() {
		return nil, fmt.Errorf("block has no index")
	}

	ref, ok := ab.index.LookupSpanID(spanID)
	if !ok {
		return nil, fmt.Errorf("span %s not found", spanID)
	}

	if ref.RecordIndex >= len(ab.records) {
		return nil, fmt.Errorf("invalid record index %d", ref.RecordIndex)
	}

	return extractSpanFromArrowRecord(ab.records[ref.RecordIndex], ref.RowIndex)
}

// GetSpansBatch efficiently retrieves multiple spans by ID with their events and links
// Groups spans by record index to improve cache locality
// Fetches events and links in a single pass instead of N separate queries
func (ab *ArrowBlock) GetSpansBatch(spanIDs []string) ([]*span.Span, error) {
	if !ab.HasIndex() {
		return nil, fmt.Errorf("block has no index")
	}

	// Group span lookups by record index for better cache locality
	type spanLookup struct {
		recordIndex int
		rowIndex    int
	}
	lookups := make([]spanLookup, 0, len(spanIDs))

	for _, spanID := range spanIDs {
		ref, ok := ab.index.LookupSpanID(spanID)
		if !ok {
			continue // Skip spans not found
		}

		if ref.RecordIndex >= len(ab.records) {
			continue
		}

		lookups = append(lookups, spanLookup{
			recordIndex: ref.RecordIndex,
			rowIndex:    ref.RowIndex,
		})
	}

	// Process spans grouped by record for better cache performance
	result := make([]*span.Span, 0, len(lookups))
	recordGroups := make(map[int][]int) // recordIndex -> []rowIndex

	for _, lookup := range lookups {
		recordGroups[lookup.recordIndex] = append(recordGroups[lookup.recordIndex], lookup.rowIndex)
	}

	// Process each record once, extracting all needed spans
	for recordIdx, rowIndices := range recordGroups {
		record := ab.records[recordIdx]
		for _, rowIdx := range rowIndices {
			sp, err := extractSpanFromArrowRecord(record, rowIdx)
			if err != nil {
				continue
			}
			result = append(result, sp)
		}
	}

	// Batch fetch links if they exist
	linksMap, _ := ab.GetLinksBatch(spanIDs)
	if linksMap != nil {
		for _, sp := range result {
			if linkPtrs, found := linksMap[sp.SpanID]; found {
				// Convert []*SpanLink to []SpanLink
				sp.Links = make([]span.SpanLink, len(linkPtrs))
				for i, l := range linkPtrs {
					sp.Links[i] = *l
				}
			}
		}
	}

	return result, nil
}

// ReadAll reads all spans from the Arrow block
func (ab *ArrowBlock) ReadAll() ([]*span.Span, error) {
	result := make([]*span.Span, 0)

	for _, record := range ab.records {
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
func (ab *ArrowBlock) GetTraceByID(traceID string) ([]*span.Span, error) {
	if !ab.HasIndex() {
		// Fallback to full scan
		return ab.scanByTraceID(traceID)
	}

	spanIDs := ab.index.LookupByTraceID(traceID)
	// Fallback to scan if trace not found in index
	if spanIDs == nil || len(spanIDs) == 0 {
		return ab.scanByTraceID(traceID)
	}
	return ab.GetSpansBatch(spanIDs)
}

// GetSpansByTag retrieves all spans that have a specific tag key-value pair
func (ab *ArrowBlock) GetSpansByTag(tagKey, tagValue string) ([]*span.Span, error) {
	if !ab.HasIndex() {
		// Fallback to full scan
		return ab.scanByTag(tagKey, tagValue)
	}

	spanIDs := ab.index.LookupByTag(tagKey, tagValue)
	// Fallback to scan if tag not found in index (key/value not in symbol table)
	if spanIDs == nil || len(spanIDs) == 0 {
		return ab.scanByTag(tagKey, tagValue)
	}
	return ab.GetSpansBatch(spanIDs)
}

// scanByTraceID is a fallback full scan when no index is available
func (ab *ArrowBlock) scanByTraceID(traceID string) ([]*span.Span, error) {
	result := make([]*span.Span, 0)

	for _, record := range ab.records {
		for row := 0; row < int(record.NumRows()); row++ {
			sp, err := extractSpanFromArrowRecord(record, row)
			if err != nil {
				continue
			}
			if sp.TraceID == traceID {
				result = append(result, sp)
			}
		}
	}

	return result, nil
}

// scanByTag is a fallback full scan when no index is available
func (ab *ArrowBlock) scanByTag(tagKey, tagValue string) ([]*span.Span, error) {
	result := make([]*span.Span, 0)

	for _, record := range ab.records {
		for row := 0; row < int(record.NumRows()); row++ {
			sp, err := extractSpanFromArrowRecord(record, row)
			if err != nil {
				continue
			}
			if sp.Tags != nil && sp.Tags[tagKey] == tagValue {
				result = append(result, sp)
			}
		}
	}

	return result, nil
}

// hexDigits for fast hex encoding
const hexDigits = "0123456789abcdef"

// uint64ToHex converts a uint64 to a 16-character hex string without allocations
// Much faster than fmt.Sprintf("%016x", val)
func uint64ToHex(val uint64, buf []byte) {
	_ = buf[15] // bounds check hint
	for i := 15; i >= 0; i-- {
		buf[i] = hexDigits[val&0xf]
		val >>= 4
	}
}

// extractSpanFromArrowRecord extracts a span from an Arrow record
func extractSpanFromArrowRecord(record arrow.RecordBatch, rowIndex int) (*span.Span, error) {
	// Validate row index
	if rowIndex < 0 || rowIndex >= int(record.NumRows()) {
		return nil, fmt.Errorf("invalid row index %d (record has %d rows)", rowIndex, record.NumRows())
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

	// Extract fields with bounds checking
	// Read trace_id_hi and trace_id_lo and format as hex string
	traceIDHi := record.Column(0).(*array.Uint64).Value(rowIndex)
	traceIDLo := record.Column(1).(*array.Uint64).Value(rowIndex)
	var traceIDBuf [32]byte
	uint64ToHex(traceIDHi, traceIDBuf[:16])
	uint64ToHex(traceIDLo, traceIDBuf[16:])
	sp.TraceID = string(traceIDBuf[:])

	// Read span_id and format as hex string
	spanIDVal := record.Column(2).(*array.Uint64).Value(rowIndex)
	var spanIDBuf [16]byte
	uint64ToHex(spanIDVal, spanIDBuf[:])
	sp.SpanID = string(spanIDBuf[:])

	// Read parent_span_id and format as hex string (or empty if null)
	parentCol := record.Column(3).(*array.Uint64)
	if !parentCol.IsNull(rowIndex) {
		parentSpanIDVal := parentCol.Value(rowIndex)
		var parentIDBuf [16]byte
		uint64ToHex(parentSpanIDVal, parentIDBuf[:])
		sp.ParentSpanID = string(parentIDBuf[:])
	}

	sp.Name = record.Column(4).(*array.String).Value(rowIndex)

	sp.StartTime = time.Unix(0, record.Column(5).(*array.Int64).Value(rowIndex))

	sp.EndTime = time.Unix(0, record.Column(6).(*array.Int64).Value(rowIndex))

	sp.Duration = record.Column(7).(*array.Int64).Value(rowIndex)

	sp.ServiceName = record.Column(8).(*array.String).Value(rowIndex)

	tagsCol := record.Column(9).(*array.Map)
	if !tagsCol.IsNull(rowIndex) {
		sp.Tags = make(map[string]string)

		offsets := tagsCol.Offsets()
		// Validate offset bounds
		if rowIndex+1 >= len(offsets) {
			return nil, fmt.Errorf("invalid offset index %d for tags map (offsets length: %d)", rowIndex+1, len(offsets))
		}

		offset := offsets[rowIndex]
		nextOffset := offsets[rowIndex+1]

		keys := tagsCol.Keys().(*array.String)
		items := tagsCol.Items().(*array.String)

		for i := int(offset); i < int(nextOffset); i++ {
			// Return error on malformed data instead of silently breaking
			// This ensures callers know the data is corrupted rather than seeing incomplete tags
			if i >= keys.Len() || i >= items.Len() {
				return nil, fmt.Errorf("corrupted tags data at row %d: tag index %d exceeds bounds (keys: %d, items: %d)",
					rowIndex, i, keys.Len(), items.Len())
			}
			key := keys.Value(i)
			value := items.Value(i)
			sp.Tags[key] = value
		}
	}

	return sp, nil
}

// FlushBlock flushes an in-memory block to disk as Arrow IPC
// Uses atomic write with temporary directory to prevent corruption
func FlushBlock(dir string, meta *BlockMeta, records []arrow.RecordBatch, schema *arrow.Schema, idx *index.Index) error {
	// CRITICAL: Write to temporary directory first for atomicity
	// This prevents corruption if system crashes during write
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

	dataPath := filepath.Join(tmpDir, dataFilename)
	f, err := os.Create(dataPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to create data file: %w", err)
	}

	writer, err := ipc.NewFileWriter(f, ipc.WithSchema(schema))
	if err != nil {
		f.Close()
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to create IPC writer: %w", err)
	}

	for _, rec := range records {
		if err := writer.Write(rec); err != nil {
			writer.Close()
			f.Close()
			os.RemoveAll(tmpDir)
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		f.Close()
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to close writer: %w", err)
	}

	// CRITICAL: Fsync data file before closing
	if err := f.Sync(); err != nil {
		f.Close()
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to sync data file: %w", err)
	}
	f.Close()

	// Fsync the directory to ensure all files are persisted
	if err := fsyncDir(tmpDir); err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to sync temp directory: %w", err)
	}

	// ATOMIC: Rename temporary directory to final location
	if err := os.Rename(tmpDir, dir); err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to rename block directory: %w", err)
	}

	// Fsync parent directory for rename atomicity
	parentDir := filepath.Dir(dir)
	if err := fsyncDir(parentDir); err != nil {
		// Log but don't fail - rename is already complete
		slog.Default().Warn("failed to sync parent directory",
			slog.String("error", err.Error()))
	}

	return nil
}

// atomicWriteFile writes data to a file with fsync for durability
// AtomicWriteFile writes data to a file atomically with fsync
// Exported for use by compactor
func AtomicWriteFile(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(data); err != nil {
		return err
	}

	return f.Sync()
}

// atomicWriteFile is an internal alias for backwards compatibility
func atomicWriteFile(path string, data []byte) error {
	return AtomicWriteFile(path, data)
}

// FsyncDir fsyncs a directory
// Exported for use by compactor
func FsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// fsyncDir is an internal alias for backwards compatibility
func fsyncDir(dir string) error {
	return FsyncDir(dir)
}

// GetEventsBySpanID retrieves all events for a given span ID from this block
// GetLinksBySpanID retrieves all links for a given span ID from this block
func (ab *ArrowBlock) GetLinksBySpanID(spanID string) ([]*span.SpanLink, error) {
	if ab.linkRecords == nil || len(ab.linkRecords) == 0 {
		return nil, nil // No links in this block
	}
	return GetLinksBySpanIDFromArrow(ab.linkRecords, spanID)
}

// HasLinks returns true if the block has link records loaded
func (ab *ArrowBlock) HasLinks() bool {
	return ab.linkRecords != nil && len(ab.linkRecords) > 0
}

// LinkRecords returns all link records in this block
func (ab *ArrowBlock) LinkRecords() []arrow.RecordBatch {
	return ab.linkRecords
}

// ReadAllLinks reads all links from this block
func (ab *ArrowBlock) ReadAllLinks() ([]*span.SpanLink, error) {
	if ab.linkRecords == nil || len(ab.linkRecords) == 0 {
		return nil, nil // No links in this block
	}
	return ReadAllLinksFromArrow(ab.linkRecords)
}

// LinkSchema returns the link schema
func (ab *ArrowBlock) LinkSchema() *arrow.Schema {
	return ab.linkSchema
}

// DeleteBlock deletes a block directory and all its contents
func DeleteBlock(dir string) error {
	return os.RemoveAll(dir)
}
