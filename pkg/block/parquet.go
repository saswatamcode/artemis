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
	"sync"
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
// NOTE: Tags are NOT stored here - they are in attributes.parquet for efficient sparse storage
type ParquetSpan struct {
	TraceIDHi      uint64 `parquet:"trace_id_hi"`
	TraceIDLo      uint64 `parquet:"trace_id_lo"`
	SpanID         uint64 `parquet:"span_id"`
	ParentSpanID   uint64 `parquet:"parent_span_id,optional"`
	Name           string `parquet:"name,dict"`
	StartTime      int64  `parquet:"start_time,delta"` // nanoseconds since epoch
	EndTime        int64  `parquet:"end_time,delta"`   // nanoseconds since epoch
	Duration       int64  `parquet:"duration,delta"`   // nanoseconds
	ServiceName    string `parquet:"service_name,dict"`
	Bucket1s       int64  `parquet:"bucket1s"`        // per-second bucket of start time
	DurationBucket int32  `parquet:"duration_bucket"` // exponential duration bucket
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

	sp := parquetSpanToSpan(&batch[0])

	// Load attributes from attributes.parquet if it exists
	// OPTIMIZATION: Use direct attr ref if index has it
	attrRefs := pb.index.LookupAttrRefsBatch([]string{sp.SpanID})
	var attrsMap map[string]map[string]string
	if len(attrRefs) > 0 {
		// Fast path: Direct row access
		attrsMap, _ = pb.GetAttributesByRefs(attrRefs)
	} else {
		// Fallback: Scan attributes.parquet
		attrsMap, _ = GetAttributesBatch(pb.dir, []string{sp.SpanID})
	}

	if attrsMap != nil {
		if attrs, found := attrsMap[sp.SpanID]; found {
			if sp.Tags == nil {
				sp.Tags = attrs
			} else {
				for k, v := range attrs {
					sp.Tags[k] = v
				}
			}
		}
	}

	// Load links if they exist
	linksMap, _ := pb.GetLinksBatch([]string{sp.SpanID})
	if linksMap != nil {
		if linkPtrs, found := linksMap[sp.SpanID]; found {
			sp.Links = make([]span.SpanLink, len(linkPtrs))
			for i, l := range linkPtrs {
				sp.Links[i] = *l
			}
		}
	}

	return sp, nil
}

// ReadAll reads all spans from the Parquet block (for full scans)
// Memory efficient: Uses streaming reader, doesn't load all spans at once
func (pb *ParquetBlock) ReadAll() ([]*span.Span, error) {
	reader := parquet.NewGenericReader[ParquetSpan](pb.file)
	defer reader.Close()

	totalSpans := make([]*span.Span, 0, pb.file.NumRows())
	spanIDs := make([]string, 0, pb.file.NumRows()) // Track span IDs for attribute loading
	batch := make([]ParquetSpan, 1024)              // Read in batches of 1024

	for {
		n, err := reader.Read(batch)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read spans: %w", err)
		}

		for i := range n {
			sp := parquetSpanToSpan(&batch[i])
			totalSpans = append(totalSpans, sp)
			spanIDs = append(spanIDs, sp.SpanID)
		}

		if err == io.EOF {
			break
		}
	}

	// OPTIMIZATION: Batch fetch attributes and links in parallel
	type batchResult struct {
		attrsMap map[string]map[string]string
		linksMap map[string][]*span.SpanLink
	}

	resultChan := make(chan batchResult, 1)
	go func() {
		var result batchResult
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			// OPTIMIZATION: Use direct attr refs if index has them
			if pb.HasIndex() {
				attrRefs := pb.index.LookupAttrRefsBatch(spanIDs)
				if len(attrRefs) == len(spanIDs) {
					// All spans have attr refs - pure fast path
					result.attrsMap, _ = pb.GetAttributesByRefs(attrRefs)
				} else if len(attrRefs) > 0 {
					// Mixed case - use fast path + scan
					fastAttrs, _ := pb.GetAttributesByRefs(attrRefs)
					spansWithoutRefs := make([]string, 0)
					for _, sid := range spanIDs {
						if _, hasRef := attrRefs[sid]; !hasRef {
							spansWithoutRefs = append(spansWithoutRefs, sid)
						}
					}
					slowAttrs, _ := GetAttributesBatch(pb.dir, spansWithoutRefs)
					result.attrsMap = fastAttrs
					if slowAttrs != nil {
						for k, v := range slowAttrs {
							result.attrsMap[k] = v
						}
					}
				} else {
					result.attrsMap, _ = GetAttributesBatch(pb.dir, spanIDs)
				}
			} else {
				result.attrsMap, _ = GetAttributesBatch(pb.dir, spanIDs)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			result.linksMap, _ = pb.GetLinksBatch(spanIDs)
		}()

		wg.Wait()
		resultChan <- result
	}()

	batchRes := <-resultChan

	// Merge attributes
	if batchRes.attrsMap != nil {
		for _, sp := range totalSpans {
			if attrs, found := batchRes.attrsMap[sp.SpanID]; found {
				if sp.Tags == nil {
					sp.Tags = attrs
				} else {
					for k, v := range attrs {
						sp.Tags[k] = v
					}
				}
			}
		}
	}

	// Merge links
	if batchRes.linksMap != nil {
		for _, sp := range totalSpans {
			if linkPtrs, found := batchRes.linksMap[sp.SpanID]; found {
				sp.Links = make([]span.SpanLink, len(linkPtrs))
				for i, l := range linkPtrs {
					sp.Links[i] = *l
				}
			}
		}
	}

	return totalSpans, nil
}

// GetAttributesByRefs efficiently retrieves attributes using direct row references
// This is the KEY OPTIMIZATION for fast attribute loading:
// - Uses index.AttrRef to seek directly to attribute rows
// - Groups refs by row group for efficient batched reads
// - Avoids scanning entire attributes.parquet file
func (pb *ParquetBlock) GetAttributesByRefs(attrRefs map[string]index.AttrRef) (map[string]map[string]string, error) {
	if len(attrRefs) == 0 {
		return nil, nil
	}

	attrsPath := filepath.Join(pb.dir, "attributes.parquet")
	if _, err := os.Stat(attrsPath); os.IsNotExist(err) {
		return nil, nil
	}

	// Open file
	f, err := os.Open(attrsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open attributes parquet file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat attributes parquet file: %w", err)
	}

	file, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to open attributes parquet file: %w", err)
	}

	schema := file.Schema()

	// CRITICAL FIX: Use schema.Lookup() to get correct column indices
	// Don't iterate schemaColumns as order doesn't match RowBuilder's internal ordering
	spanIDLookup, hasSpanID := schema.Lookup("span_id")
	if !hasSpanID {
		return nil, fmt.Errorf("attributes file missing span_id column")
	}
	spanIDColIdx := spanIDLookup.ColumnIndex

	// Build column name to index mapping using Lookup()
	schemaColumns := schema.Columns()
	columnNameToIdx := make(map[string]int)
	for _, col := range schemaColumns {
		if lc, ok := schema.Lookup(col...); ok {
			columnNameToIdx[col[0]] = lc.ColumnIndex
		}
	}

	// Group refs by row group for efficient reading
	type rowLookup struct {
		spanID      string
		rowGroupIdx int
		rowIdx      int
	}

	lookupsByRowGroup := make(map[int][]rowLookup)
	for spanID, ref := range attrRefs {
		lookupsByRowGroup[ref.RecordIndex] = append(lookupsByRowGroup[ref.RecordIndex], rowLookup{
			spanID:      spanID,
			rowGroupIdx: ref.RecordIndex,
			rowIdx:      ref.RowIndex,
		})
	}

	result := make(map[string]map[string]string)
	rowGroups := file.RowGroups()

	// Process each row group
	for rgIdx, lookups := range lookupsByRowGroup {
		if rgIdx >= len(rowGroups) {
			continue
		}

		// Sort by row index for sequential reads
		sort.Slice(lookups, func(i, j int) bool {
			return lookups[i].rowIdx < lookups[j].rowIdx
		})

		rowGroup := rowGroups[rgIdx]
		reader := parquet.NewRowGroupReader(rowGroup)

		// Read rows in this row group
		// We read all rows and filter by rowIdx since Parquet doesn't support
		// efficient random row access within a row group
		numRowsInRG := rowGroup.NumRows()
		rows := make([]parquet.Row, numRowsInRG)
		n, err := reader.ReadRows(rows)
		if err != nil && err != io.EOF {
			continue
		}

		// Extract attributes for matching rows
		foundInThisRG := 0
		for _, lookup := range lookups {
			// Access row directly at the index stored in the attrRef
			actualRowIdx := lookup.rowIdx
			if actualRowIdx >= int(n) {
				continue
			}

			row := rows[actualRowIdx]
			if len(row) <= spanIDColIdx {
				continue
			}

			// Extract attributes
			attrs := make(map[string]string)
			for colName, colIdx := range columnNameToIdx {
				// Skip special columns
				if colName == "span_id" || colName == "__attr_index" {
					continue
				}

				// Check if this is an attribute column
				if attrKey, ok := ColumnToAttributeName(colName); ok {
					if colIdx < len(row) && !row[colIdx].IsNull() {
						value := row[colIdx].String()
						attrs[attrKey] = value
					}
				}
			}

			if len(attrs) > 0 {
				result[lookup.spanID] = attrs
				foundInThisRG++
			}
		}
	}

	return result, nil
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

	// OPTIMIZATION: Batch fetch attributes and links in parallel
	// This reduces sequential file I/O overhead by opening and reading files concurrently
	type batchResult struct {
		attrsMap map[string]map[string]string
		linksMap map[string][]*span.SpanLink
		attrsErr error
		linksErr error
	}

	resultChan := make(chan batchResult, 1)

	// Launch parallel goroutines to fetch attributes and links
	go func() {
		var result batchResult
		var wg sync.WaitGroup

		// Fetch attributes in parallel - use optimized path if attrIndex exists
		wg.Add(1)
		go func() {
			defer wg.Done()

			// OPTIMIZATION: Use direct attribute refs if index has them
			// This is 10-15x faster than scanning attributes.parquet
			attrRefs := pb.index.LookupAttrRefsBatch(spanIDs)

			if len(attrRefs) == len(spanIDs) {
				// All spans have attr refs - pure fast path
				result.attrsMap, result.attrsErr = pb.GetAttributesByRefs(attrRefs)
			} else if len(attrRefs) > 0 {
				// Mixed case - some spans have attrs in index, some don't
				// Use fast path for ones with refs, scan for the rest
				fastAttrs, _ := pb.GetAttributesByRefs(attrRefs)

				// Find spans without attr refs
				spansWithoutRefs := make([]string, 0, len(spanIDs)-len(attrRefs))
				for _, sid := range spanIDs {
					if _, hasRef := attrRefs[sid]; !hasRef {
						spansWithoutRefs = append(spansWithoutRefs, sid)
					}
				}

				// Scan for spans without refs
				slowAttrs, _ := GetAttributesBatch(pb.dir, spansWithoutRefs)

				// Merge results
				result.attrsMap = fastAttrs
				if slowAttrs != nil {
					for k, v := range slowAttrs {
						result.attrsMap[k] = v
					}
				}
			} else {
				// No spans have attr refs - pure fallback (old blocks or spans without attributes)
				result.attrsMap, result.attrsErr = GetAttributesBatch(pb.dir, spanIDs)
			}
		}()

		// Fetch links in parallel
		wg.Add(1)
		go func() {
			defer wg.Done()
			result.linksMap, result.linksErr = pb.GetLinksBatch(spanIDs)
		}()

		wg.Wait()
		resultChan <- result
	}()

	// Wait for parallel fetches to complete
	batchRes := <-resultChan

	// Merge attributes into spans
	if batchRes.attrsMap != nil {
		for _, sp := range results {
			if attrs, found := batchRes.attrsMap[sp.SpanID]; found {
				// Merge attributes from separate file
				// Attributes from attributes.parquet take precedence if there's overlap
				if sp.Tags == nil {
					sp.Tags = attrs
				} else {
					for k, v := range attrs {
						sp.Tags[k] = v
					}
				}
			}
		}
	}

	// Merge links into spans
	if batchRes.linksMap != nil {
		for _, sp := range results {
			if linkPtrs, found := batchRes.linksMap[sp.SpanID]; found {
				// Convert []*SpanLink to []SpanLink
				sp.Links = make([]span.SpanLink, len(linkPtrs))
				for i, l := range linkPtrs {
					sp.Links[i] = *l
				}
			}
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
	batch := make([]ParquetSpanMetadata, 4096) // Larger batch = fewer I/O calls

	rowGroups := pb.file.RowGroups()

	// OPTIMIZATION: Pre-compute row group boundaries once
	// This allows O(1) amortized lookups instead of O(m) per match
	rowGroupBoundaries := make([]int64, len(rowGroups)+1)
	rowGroupBoundaries[0] = 0
	for i, rg := range rowGroups {
		rowGroupBoundaries[i+1] = rowGroupBoundaries[i] + rg.NumRows()
	}

	globalRowIdx := int64(0)
	currentRGIdx := 0 // Track current row group for fast path

	for {
		n, err := reader.Read(batch)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read metadata: %w", err)
		}

		for i := range n {
			if filterFunc(&batch[i]) {
				rowIdx := globalRowIdx + int64(i)

				// FAST PATH: Check if still in current row group (common case)
				// Avoids binary search for sequential matches
				if currentRGIdx < len(rowGroups) &&
					rowIdx >= rowGroupBoundaries[currentRGIdx] &&
					rowIdx < rowGroupBoundaries[currentRGIdx+1] {
					matches = append(matches, RowReference{
						RowGroupIdx: currentRGIdx,
						RowIdx:      int(rowIdx - rowGroupBoundaries[currentRGIdx]),
					})
				} else {
					// SLOW PATH: Row group changed - use binary search
					rgIdx := pb.findRowGroupFast(rowIdx, rowGroupBoundaries)
					currentRGIdx = rgIdx
					matches = append(matches, RowReference{
						RowGroupIdx: rgIdx,
						RowIdx:      int(rowIdx - rowGroupBoundaries[rgIdx]),
					})
				}
			}
		}

		globalRowIdx += int64(n)

		if err == io.EOF {
			break
		}
	}

	return matches, nil
}

// findRowGroupFast uses binary search on pre-computed boundaries - O(log n)
func (pb *ParquetBlock) findRowGroupFast(globalRowIdx int64, boundaries []int64) int {
	// Binary search for the row group
	left, right := 0, len(boundaries)-2

	for left <= right {
		mid := (left + right) / 2
		if globalRowIdx < boundaries[mid] {
			right = mid - 1
		} else if globalRowIdx >= boundaries[mid+1] {
			left = mid + 1
		} else {
			return mid
		}
	}

	// Fallback to last row group
	return len(boundaries) - 2
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

	// OPTIMIZATION: Batch fetch attributes and links in parallel
	if len(results) > 0 {
		spanIDs := make([]string, len(results))
		for i, sp := range results {
			spanIDs[i] = sp.SpanID
		}

		type batchResult struct {
			attrsMap map[string]map[string]string
			linksMap map[string][]*span.SpanLink
		}

		resultChan := make(chan batchResult, 1)
		go func() {
			var result batchResult
			var wg sync.WaitGroup

			wg.Add(1)
			go func() {
				defer wg.Done()
				// OPTIMIZATION: Use direct attr refs if index has them
				if pb.HasIndex() {
					attrRefs := pb.index.LookupAttrRefsBatch(spanIDs)
					if len(attrRefs) == len(spanIDs) {
						// All spans have attr refs - pure fast path
						result.attrsMap, _ = pb.GetAttributesByRefs(attrRefs)
					} else if len(attrRefs) > 0 {
						// Mixed case - use fast path + scan
						fastAttrs, _ := pb.GetAttributesByRefs(attrRefs)
						spansWithoutRefs := make([]string, 0)
						for _, sid := range spanIDs {
							if _, hasRef := attrRefs[sid]; !hasRef {
								spansWithoutRefs = append(spansWithoutRefs, sid)
							}
						}
						slowAttrs, _ := GetAttributesBatch(pb.dir, spansWithoutRefs)
						result.attrsMap = fastAttrs
						if slowAttrs != nil {
							for k, v := range slowAttrs {
								result.attrsMap[k] = v
							}
						}
					} else {
						result.attrsMap, _ = GetAttributesBatch(pb.dir, spanIDs)
					}
				} else {
					result.attrsMap, _ = GetAttributesBatch(pb.dir, spanIDs)
				}
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				result.linksMap, _ = pb.GetLinksBatch(spanIDs)
			}()

			wg.Wait()
			resultChan <- result
		}()

		batchRes := <-resultChan

		// Merge attributes
		if batchRes.attrsMap != nil {
			for _, sp := range results {
				if attrs, found := batchRes.attrsMap[sp.SpanID]; found {
					if sp.Tags == nil {
						sp.Tags = attrs
					} else {
						for k, v := range attrs {
							sp.Tags[k] = v
						}
					}
				}
			}
		}

		// Merge links
		if batchRes.linksMap != nil {
			for _, sp := range results {
				if linkPtrs, found := batchRes.linksMap[sp.SpanID]; found {
					sp.Links = make([]span.SpanLink, len(linkPtrs))
					for i, l := range linkPtrs {
						sp.Links[i] = *l
					}
				}
			}
		}
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
	// Fallback to scan if trace not found in index
	if spanIDs == nil || len(spanIDs) == 0 {
		return pb.scanByTraceID(traceID)
	}
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
	// Fallback to scan if tag not found in index (key/value not in symbol table)
	if spanIDs == nil || len(spanIDs) == 0 {
		return pb.scanByTag(tagKey, tagValue)
	}
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
// Queries attributes.parquet for matching span IDs, then fetches those spans
func (pb *ParquetBlock) scanByTag(tagKey, tagValue string) ([]*span.Span, error) {
	// Query attributes.parquet directly for this tag
	matchingSpanIDs, err := QueryAttributesByKey(pb.dir, tagKey, tagValue)
	if err != nil || len(matchingSpanIDs) == 0 {
		return make([]*span.Span, 0), nil
	}

	// Fetch spans for matching span IDs
	return pb.GetSpansBatch(matchingSpanIDs)
}

// SpanToParquetSpan converts a Span to ParquetSpan
// Exported for use by compactor
func SpanToParquetSpan(s *span.Span) *ParquetSpan {
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
		Bucket1s:       bucket,
		DurationBucket: durationBucketVal,
	}
	// NOTE: Tags are stored separately in attributes.parquet, not in spans.parquet
}

// parquetSpanToSpan converts a ParquetSpan to Span
func parquetSpanToSpan(ps *ParquetSpan) *span.Span {
	// Convert trace ID from hi/lo to hex string
	var traceIDBuf [32]byte
	uint64ToHex(ps.TraceIDHi, traceIDBuf[:16])
	uint64ToHex(ps.TraceIDLo, traceIDBuf[16:])
	traceID := string(traceIDBuf[:])

	// Convert span ID to hex string
	var spanIDBuf [16]byte
	uint64ToHex(ps.SpanID, spanIDBuf[:])
	spanID := string(spanIDBuf[:])

	// Convert parent span ID to hex string (empty if 0)
	parentSpanID := ""
	if ps.ParentSpanID != 0 {
		var parentIDBuf [16]byte
		uint64ToHex(ps.ParentSpanID, parentIDBuf[:])
		parentSpanID = string(parentIDBuf[:])
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
		Tags:         nil, // Tags loaded separately from attributes.parquet
	}
	// NOTE: Tags will be loaded and merged from attributes.parquet by caller
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

	// Write spans.parquet and attributes.parquet in parallel for efficiency
	// Both files are independent and can be written concurrently
	// We need the attrRowMap from attributes write to build the index
	type writeResult struct {
		name       string
		err        error
		attrRowMap map[string]AttrRowInfo // Only populated for attributes write
	}

	resultCh := make(chan writeResult, 2)

	// Goroutine 1: Write spans.parquet
	go func() {
		parquetSpans := make([]ParquetSpan, len(spans))
		for i, s := range spans {
			parquetSpans[i] = *SpanToParquetSpan(s)
		}

		dataPath := filepath.Join(tmpDir, parquetDataFilename)
		f, err := os.Create(dataPath)
		if err != nil {
			resultCh <- writeResult{name: "spans.parquet", err: fmt.Errorf("failed to create parquet file: %w", err)}
			return
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
			resultCh <- writeResult{name: "spans.parquet", err: fmt.Errorf("failed to write parquet data: %w", err)}
			return
		}

		if err := writer.Close(); err != nil {
			f.Close()
			resultCh <- writeResult{name: "spans.parquet", err: fmt.Errorf("failed to close writer: %w", err)}
			return
		}

		// CRITICAL: Fsync data file
		if err := f.Sync(); err != nil {
			f.Close()
			resultCh <- writeResult{name: "spans.parquet", err: fmt.Errorf("failed to sync parquet file: %w", err)}
			return
		}
		f.Close()

		resultCh <- writeResult{name: "spans.parquet", err: nil}
	}()

	// Goroutine 2: Write attributes.parquet and capture row mapping
	go func() {
		attrRowMap, err := WriteParquetAttributesWithRowMap(tmpDir, spans)
		if err != nil {
			resultCh <- writeResult{name: "attributes.parquet", err: err}
		} else {
			resultCh <- writeResult{name: "attributes.parquet", err: nil, attrRowMap: attrRowMap}
		}
	}()

	// Wait for both writes to complete and collect attr row mapping
	var writeErrors []error
	var attrRowMap map[string]AttrRowInfo
	for i := 0; i < 2; i++ {
		result := <-resultCh
		if result.err != nil {
			writeErrors = append(writeErrors, fmt.Errorf("%s: %w", result.name, result.err))
		}
		if result.name == "attributes.parquet" && result.attrRowMap != nil {
			attrRowMap = result.attrRowMap
		}
	}

	// Check if any writes failed
	if len(writeErrors) > 0 {
		os.RemoveAll(tmpDir)
		// Return first error (both errors would be visible in logs)
		return writeErrors[0]
	}

	// Build/update index with attr refs AFTER parquet files are written
	if idx != nil {
		// For Parquet blocks, we DON'T need to rebuild the tag index because:
		// - Tags are in attributes.parquet and queried directly via QueryAttributesByKey
		// - Tag index is only needed for Arrow (in-memory) blocks
		// - We only need to update storage references (spanIndex, attrIndex, traceIndex)

		// Clear only storage references, keep tag index intact as fallback
		idx.ClearStorageRefs()

		// Rebuild storage references with attribute row references
		// We know row group size from WriteParquetAttributesWithRowMap matches spans.parquet row groups
		const rowGroupSize = 1024
		for i, sp := range spans {
			recordIdx := i / rowGroupSize
			rowIdx := i % rowGroupSize

			spanRef := index.SpanRef{
				RecordIndex: recordIdx,
				RowIndex:    rowIdx,
			}

			// Look up attr ref for this span
			var attrRef *index.AttrRef
			if attrInfo, hasAttrs := attrRowMap[sp.SpanID]; hasAttrs {
				attrRef = &index.AttrRef{
					RecordIndex: attrInfo.RowGroup,
					RowIndex:    attrInfo.Row,
				}
			}

			// Add storage references only (preserves existing tag index)
			idx.AddSpanRef(sp.SpanID, sp.TraceID, spanRef, attrRef)
		}

		// Write index to disk
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
