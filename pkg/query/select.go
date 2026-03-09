package query

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// SelectResult holds the results of a Select query
type SelectResult struct {
	Spans []*span.Span
}

// QueryOptions holds options for querying spans
type QueryOptions struct {
	// IncludeEvents controls whether span events should be loaded
	// If true, the Events field on each span will be populated
	IncludeEvents bool
}

// TimeRange specifies a time range for queries
type TimeRange struct {
	Start time.Time
	End   time.Time
}

// NewTimeRange creates a new time range
func NewTimeRange(start, end time.Time) *TimeRange {
	return &TimeRange{
		Start: start,
		End:   end,
	}
}

// Contains checks if a timestamp falls within the time range
func (tr *TimeRange) Contains(t time.Time) bool {
	return !t.Before(tr.Start) && !t.After(tr.End)
}

// Overlaps checks if a time range overlaps with another (using nanosecond timestamps)
func (tr *TimeRange) Overlaps(minTime, maxTime int64) bool {
	startNano := tr.Start.UnixNano()
	endNano := tr.End.UnixNano()
	return startNano <= maxTime && endNano >= minTime
}

// Select queries spans using matchers (similar to Prometheus Select)
// It uses indexes for efficient filtering by tags
func Select(storage *storage.ArrowStorage, matchers ...*Matcher) (*SelectResult, error) {
	ms := Matchers(matchers)

	if len(matchers) == 0 {
		return selectAll(storage)
	}

	// Try to use indexes for the first matcher with exact match
	var candidateSpanIDs []string
	var firstMatcherIndex = -1

	// Find the first matcher that can use an index (exact match)
	// Priority: trace_id > span_id > other fields
	for i, m := range matchers {
		if m.Type == MatchEqual {
			if m.Name == "trace_id" {
				candidateSpanIDs = storage.GetIndex().LookupByTraceID(m.Value)
				firstMatcherIndex = i
				break
			}

			// Check if this is a span ID lookup (unique - use spanIndex directly)
			if m.Name == "span_id" {
				if _, ok := storage.GetIndex().LookupSpanID(m.Value); ok {
					candidateSpanIDs = []string{m.Value}
					firstMatcherIndex = i
					break
				}
				continue
			}

			candidateSpanIDs = storage.GetIndex().LookupByTag(m.Name, m.Value)
			if len(candidateSpanIDs) > 0 {
				firstMatcherIndex = i
				break
			}
		}
	}

	// If we couldn't use an index, fall back to full scan
	if firstMatcherIndex == -1 {
		return selectWithScan(storage, ms)
	}

	result := &SelectResult{
		Spans: make([]*span.Span, 0, len(candidateSpanIDs)),
	}

	for _, spanID := range candidateSpanIDs {
		sp, err := storage.GetSpanByID(spanID)
		if err != nil {
			continue // Span not found, skip
		}

		if ms.Matches(sp) {
			result.Spans = append(result.Spans, sp)
		}
	}

	return result, nil
}

// selectAll returns all spans (expensive, use with caution)
func selectAll(storage *storage.ArrowStorage) (*SelectResult, error) {
	result := &SelectResult{
		Spans: make([]*span.Span, 0),
	}

	records := storage.GetRecords()
	for _, record := range records {
		for row := 0; row < int(record.NumRows()); row++ {
			sp, err := extractSpanFromRecord(record, row)
			if err != nil {
				continue
			}
			result.Spans = append(result.Spans, sp)
		}
	}

	return result, nil
}

// selectWithScan performs a full scan with matcher filtering
func selectWithScan(storage *storage.ArrowStorage, ms Matchers) (*SelectResult, error) {
	result := &SelectResult{
		Spans: make([]*span.Span, 0),
	}

	records := storage.GetRecords()
	for _, record := range records {
		for row := 0; row < int(record.NumRows()); row++ {
			sp, err := extractSpanFromRecord(record, row)
			if err != nil {
				continue
			}

			if ms.Matches(sp) {
				result.Spans = append(result.Spans, sp)
			}
		}
	}

	return result, nil
}

// SelectFromBlocks queries spans across both head block and persisted blocks
// This is the main query interface when using block management
// Works uniformly across HeadBlock (in-memory), ArrowBlock (L0), and ParquetBlock (L1+)
//
// The head block is treated as just another Block for uniform query interface
// Pass nil for head if querying only persisted blocks
func SelectFromBlocks(head block.Block, blocks []block.Block, matchers ...*Matcher) (*SelectResult, error) {
	return SelectFromBlocksWithTimeRange(head, blocks, nil, matchers...)
}

// SelectFromBlocksWithTimeRange queries spans across blocks within a specific time range
// Time range filtering provides significant performance benefits by skipping irrelevant blocks
// Works uniformly across HeadBlock (in-memory), ArrowBlock (L0), and ParquetBlock (L1+)
// TODO: Parallelize such fanout
func SelectFromBlocksWithTimeRange(head block.Block, blocks []block.Block, timeRange *TimeRange, matchers ...*Matcher) (*SelectResult, error) {
	ms := Matchers(matchers)
	result := &SelectResult{
		Spans: make([]*span.Span, 0),
	}

	// IMPORTANT: Query persisted blocks FIRST, then head block
	// This prevents missing spans during head block flush:
	// - If a span was just flushed from head to a block, we find it in the block
	// - If a span is still in head, we find it there
	// - Querying head last ensures we catch any new spans that arrived after flush

	// Query persisted blocks - skip blocks that don't overlap with time range
	for _, blk := range blocks {
		if timeRange != nil {
			meta := blk.Meta()
			if !timeRange.Overlaps(meta.MinTime, meta.MaxTime) {
				continue // Skip this block entirely
			}
		}

		blockSpans := QueryBlockWithTimeRange(blk, ms, timeRange)
		result.Spans = append(result.Spans, blockSpans...)
	}

	// Query head block last (most recent data)
	if head != nil {
		if timeRange == nil || timeRangeOverlapsBlock(head, timeRange) {
			headSpans := QueryBlockWithTimeRange(head, ms, timeRange)
			result.Spans = append(result.Spans, headSpans...)
		}
	}

	return result, nil
}

// timeRangeOverlapsBlock checks if a time range overlaps with a block's time range
func timeRangeOverlapsBlock(blk block.Block, timeRange *TimeRange) bool {
	meta := blk.Meta()
	if meta.MinTime == 0 && meta.MaxTime == 0 {
		return true // Empty block or no time tracking
	}
	return timeRange.Overlaps(meta.MinTime, meta.MaxTime)
}

// spanInTimeRange checks if a span overlaps with the time range
// A span overlaps if: span.endTime >= range.start AND span.startTime <= range.end
func spanInTimeRange(sp *span.Span, timeRange *TimeRange) bool {
	if timeRange == nil {
		return true
	}
	// Standard interval overlap check: spans overlap if they have any time in common
	return !sp.EndTime.Before(timeRange.Start) && !sp.StartTime.After(timeRange.End)
}

// isTopLevelField checks if a field name refers to a top-level span field
// rather than a tag in the Tags map
func isTopLevelField(name string) bool {
	switch name {
	case "trace_id", "span_id", "parent_span_id", "name", "service.name", "service_name":
		return true
	default:
		return false
	}
}

// createMetadataFilter creates a filter function for ParquetSpanMetadata
// This filters based on top-level fields and time range only (excludes tags)
func createMetadataFilter(ms Matchers, timeRange *TimeRange) func(*block.ParquetSpanMetadata) bool {
	return func(meta *block.ParquetSpanMetadata) bool {
		// Check time range first (quick rejection)
		// A span overlaps with the time range if: startNano <= rangeEnd && endNano >= rangeStart
		if timeRange != nil {
			startNano := timeRange.Start.UnixNano()
			endNano := timeRange.End.UnixNano()
			// Reject if span doesn't overlap with time range
			if meta.StartTime > endNano || meta.EndTime < startNano {
				return false
			}
		}

		// Check matchers on top-level fields only
		// Tag matchers will be checked later after fetching full span
		for _, m := range ms {
			// Skip tag matchers - we can't check them at metadata level
			if !isTopLevelField(m.Name) {
				continue
			}

			var val string
			var ok bool

			switch m.Name {
			case "trace_id":
				// Convert uint64 hi/lo to hex string
				val = fmt.Sprintf("%016x%016x", meta.TraceIDHi, meta.TraceIDLo)
				ok = val != ""
			case "span_id":
				// Convert uint64 to hex string
				val = fmt.Sprintf("%016x", meta.SpanID)
				ok = val != ""
			case "parent_span_id":
				// Convert uint64 to hex string (0 means no parent)
				if meta.ParentSpanID != 0 {
					val = fmt.Sprintf("%016x", meta.ParentSpanID)
					ok = true
				} else {
					val = ""
					ok = false
				}
			case "name":
				val = meta.Name
				ok = val != ""
			case "service.name", "service_name":
				val = meta.ServiceName
				ok = val != ""
			}

			if !m.MatchesValue(val, ok) {
				return false // Top-level field doesn't match
			}
		}

		return true // Passed all top-level checks
	}
}

// QueryBlockWithTimeRange queries a single block with matchers and time range
// Works with HeadBlock (in-memory), ArrowBlock (L0), and ParquetBlock (L1+)
// This is exported for use by the reduced PromQL query engine.
func QueryBlockWithTimeRange(blk block.Block, ms Matchers, timeRange *TimeRange) []*span.Span {
	spans := make([]*span.Span, 0)

	// Try HeadBlock first (in-memory)
	if hb, ok := blk.(*block.HeadBlock); ok {
		return queryHeadBlock(hb, ms, timeRange)
	}

	// Try Parquet block
	if pb, ok := blk.(*block.ParquetBlock); ok {
		return queryParquetBlock(pb, ms, timeRange)
	}

	// Try Arrow block
	if ab, ok := blk.(*block.ArrowBlock); ok {
		return queryArrowBlock(ab, ms, timeRange)
	}

	// Unknown block type - this indicates a bug or new block type not handled
	// Log warning to help with debugging
	meta := blk.Meta()
	if meta != nil {
		// Use fmt.Printf as a simple warning mechanism since we don't have logger context here
		// In production, this should be rare and indicates a code issue
		fmt.Printf("WARNING: Unknown block type encountered in query: ULID=%s, Level=%d, Dir=%s\n",
			meta.ULID, meta.Level(), blk.Dir())
	}
	return spans
}

// queryParquetBlock queries a Parquet block with matchers and time range
func queryParquetBlock(pb *block.ParquetBlock, ms Matchers, timeRange *TimeRange) []*span.Span {
	spans := make([]*span.Span, 0)

	// OPTIMIZATION: Try attribute-first query path for attribute matchers
	// This queries attributes.parquet FIRST to get matching span IDs, then fetches spans
	// Much faster than: fetch all spans → load attributes → filter
	if len(ms) > 0 {
		for _, m := range ms {
			if m.Type == MatchEqual {
				// Priority order for different matcher types:
				// 1. trace_id → use index (most selective)
				// 2. span_id → use index (unique)
				// 3. attribute (tag) → query attributes.parquet FIRST (new optimization!)
				// 4. other metadata fields → use index or metadata scan

				switch m.Name {
				case "trace_id":
					// Highest priority - use index
					if pb.HasIndex() {
						candidateSpanIDs := pb.Index().LookupByTraceID(m.Value)
						if len(candidateSpanIDs) > 0 {
							batchSpans, err := pb.GetSpansBatch(candidateSpanIDs)
							if err == nil {
								for _, sp := range batchSpans {
									if ms.Matches(sp) && (timeRange == nil || spanInTimeRange(sp, timeRange)) {
										spans = append(spans, sp)
									}
								}
							}
							return spans
						}
					}

				case "span_id":
					// Second priority - use index
					if pb.HasIndex() {
						if _, ok := pb.Index().LookupSpanID(m.Value); ok {
							candidateSpanIDs := []string{m.Value}
							batchSpans, err := pb.GetSpansBatch(candidateSpanIDs)
							if err == nil {
								for _, sp := range batchSpans {
									if ms.Matches(sp) && (timeRange == nil || spanInTimeRange(sp, timeRange)) {
										spans = append(spans, sp)
									}
								}
							}
							return spans
						}
					}

				case "service_name", "name", "parent_span_id":
					// Metadata fields - use index if available
					if pb.HasIndex() {
						candidateSpanIDs := pb.Index().LookupByTag(m.Name, m.Value)
						if len(candidateSpanIDs) > 0 {
							batchSpans, err := pb.GetSpansBatch(candidateSpanIDs)
							if err == nil {
								for _, sp := range batchSpans {
									if ms.Matches(sp) && (timeRange == nil || spanInTimeRange(sp, timeRange)) {
										spans = append(spans, sp)
									}
								}
							}
							return spans
						}
					}

				default:
					// ATTRIBUTE MATCHER - use attribute-first query path!
					// This is the key optimization for tag/attribute queries
					//
					// Old path: index → GetSpansBatch → load all span data → load attributes → filter
					// New path: QueryAttributesByKey → get matching span IDs → GetSpansBatch (with attributes already loaded)
					//
					// Benefits:
					// - Column projection on attributes.parquet (read only one attribute column)
					// - Row group statistics pruning on attribute values
					// - Only fetch spans that match the attribute filter
					matchingSpanIDs, err := block.QueryAttributesByKey(pb.Dir(), m.Name, m.Value)
					if err == nil && len(matchingSpanIDs) > 0 {
						// Fetch spans for matching span IDs
						batchSpans, err := pb.GetSpansBatch(matchingSpanIDs)
						if err == nil {
							for _, sp := range batchSpans {
								if ms.Matches(sp) && (timeRange == nil || spanInTimeRange(sp, timeRange)) {
									spans = append(spans, sp)
								}
							}
						}
						return spans
					}

					// If attribute query didn't find matches or attributes.parquet doesn't exist,
					// fall back to index-based tag lookup
					if pb.HasIndex() {
						candidateSpanIDs := pb.Index().LookupByTag(m.Name, m.Value)
						if len(candidateSpanIDs) > 0 {
							batchSpans, err := pb.GetSpansBatch(candidateSpanIDs)
							if err == nil {
								for _, sp := range batchSpans {
									if ms.Matches(sp) && (timeRange == nil || spanInTimeRange(sp, timeRange)) {
										spans = append(spans, sp)
									}
								}
							}
							return spans
						}
					}
				}
			}
		}
	}

	// Fall back to full scan (no index or no exact match matchers)
	// For Parquet blocks, use efficient column projection instead of ReadAll()
	// This only reads metadata columns (excludes tags), which is much faster
	filterFunc := createMetadataFilter(ms, timeRange)
	// Scan metadata columns only
	refs, err := pb.ScanMetadata(filterFunc)
	if err != nil {
		return spans
	}

	// Fetch full spans only for matching rows
	// Note: GetSpansByRowReferences now automatically loads attributes from attributes.parquet
	matchedSpans, err := pb.GetSpansByRowReferences(refs)
	if err != nil {
		return spans
	}

	// Final filtering with full matcher (includes tag checks)
	for _, sp := range matchedSpans {
		if ms.Matches(sp) && (timeRange == nil || spanInTimeRange(sp, timeRange)) {
			spans = append(spans, sp)
		}
	}

	return spans
}

// queryHeadBlock queries a HeadBlock (in-memory) with matchers and time range
// Uses Arrow columnar optimizations for better performance
func queryHeadBlock(hb *block.HeadBlock, ms Matchers, timeRange *TimeRange) []*span.Span {
	spans := make([]*span.Span, 0)

	// Get the underlying storage
	arrowStorage := hb.Storage()

	// MVCC Snapshot Isolation (Prometheus-style):
	// 1. Capture snapshot commit sequence (lightweight, releases lock immediately)
	// 2. Query executes without holding locks
	// 3. Filter results by snapshot visibility
	var snapshotSeq uint64
	var isolation *storage.IsolationCoordinator
	if ic := hb.IsolationCoordinator(); ic != nil {
		snapshotSeq = ic.BeginQuery()
		isolation = ic
	}

	if hb.HasIndex() && len(ms) > 0 {
		idx := hb.Index()

		// Find first exact match to use as index lookup
		// Priority: trace_id > span_id > other fields
		for _, m := range ms {
			if m.Type == MatchEqual {
				var candidateSpanIDs []string
				// Check if this is a trace ID lookup (highly selective!)
				switch m.Name {
				case "trace_id":
					candidateSpanIDs = idx.LookupByTraceID(m.Value)
				case "span_id":
					if _, ok := idx.LookupSpanID(m.Value); ok {
						candidateSpanIDs = []string{m.Value}
					}
				default:
					candidateSpanIDs = idx.LookupByTag(m.Name, m.Value)
				}

				if len(candidateSpanIDs) > 0 {
					// Head block: use GetSpansBatch for consistent interface usage
					batchSpans, err := hb.GetSpansBatch(candidateSpanIDs)
					if err == nil {
						for _, sp := range batchSpans {
							// Check MVCC visibility first (fast check using reverse index)
							if isolation != nil && !isolation.IsVisible(sp.SpanID, snapshotSeq) {
								continue // Span not visible at query snapshot
							}

							if ms.Matches(sp) && (timeRange == nil || spanInTimeRange(sp, timeRange)) {
								spans = append(spans, sp)
							}
						}
					}
					return spans
				}
			}
		}
	}

	// Fall back to full scan using two-phase columnar filtering
	// Phase 1: Build selection bitmap using columnar access (fast)
	// Phase 2: Extract only matching spans (avoids unnecessary allocations)
	records := arrowStorage.GetRecords()
	needTags := matchersNeedTags(ms)

	for _, record := range records {
		// Phase 1: Build selection bitmap using columnar filtering
		selection := buildSelectionBitmap(record, ms, timeRange)

		// Phase 2: Extract only selected spans
		for row := 0; row < int(record.NumRows()); row++ {
			if !selection[row] {
				continue
			}

			sp, err := extractSpanFromRecordWithOptions(record, row, needTags)
			if err != nil {
				continue
			}

			// Check MVCC visibility (lock-free check using reverse index)
			if isolation != nil && !isolation.IsVisible(sp.SpanID, snapshotSeq) {
				continue // Span not visible at query snapshot
			}

			// Final validation with full matchers (includes tag checks if needed)
			if ms.Matches(sp) && (timeRange == nil || spanInTimeRange(sp, timeRange)) {
				// If we didn't extract tags during filtering, extract them now for the final result
				if !needTags {
					sp, err = extractSpanFromRecordWithOptions(record, row, true)
					if err != nil {
						continue
					}
				}
				spans = append(spans, sp)
			}
		}
	}

	return spans
}

// matchersNeedTags checks if any matcher requires tags to be extracted
// This allows us to skip expensive tag extraction when only querying top-level fields
func matchersNeedTags(ms Matchers) bool {
	// If there are no matchers, we're returning all spans to the user, so extract tags
	if len(ms) == 0 {
		return true
	}

	for _, m := range ms {
		// If matcher is not a top-level field, it must be a tag matcher
		if !isTopLevelField(m.Name) {
			return true
		}
	}
	return false
}

// buildSelectionBitmap creates a selection bitmap using columnar filtering
// This leverages Arrow's columnar format for efficient filtering before materialization
func buildSelectionBitmap(record arrow.RecordBatch, ms Matchers, timeRange *TimeRange) []bool {
	numRows := int(record.NumRows())
	selection := make([]bool, numRows)

	// Start with all rows selected
	for i := range selection {
		selection[i] = true
	}

	// Validate schema has enough columns for time range filtering
	if timeRange != nil && record.NumCols() >= 7 {
		// Phase 1: Filter by time range using columnar access (very fast, SIMD-friendly)
		// New schema: trace_id_hi(0), trace_id_lo(1), span_id(2), parent_span_id(3), name(4), start_time(5), end_time(6)
		startTimeCol := record.Column(5).(*array.Int64)
		endTimeCol := record.Column(6).(*array.Int64)
		startNano := timeRange.Start.UnixNano()
		endNano := timeRange.End.UnixNano()

		for row := 0; row < numRows; row++ {
			if !selection[row] {
				continue
			}

			spanStart := startTimeCol.Value(row)
			spanEnd := endTimeCol.Value(row)

			// Check if span overlaps with time range
			// A span overlaps if: startNano <= spanEnd && endNano >= spanStart
			if startNano > spanEnd || endNano < spanStart {
				selection[row] = false
			}
		}
	}

	// Phase 2: Filter by top-level field matchers using columnar access
	// Skip tag matchers - they require full span materialization
	for _, m := range ms {
		if !isTopLevelField(m.Name) {
			continue // Tag matcher - skip for now, will check after extraction
		}

		// Apply matcher using columnar access
		applyMatcherColumnar(record, m, selection)
	}

	return selection
}

// applyMatcherColumnar applies a matcher to a selection bitmap using columnar access
// This is much faster than extracting individual spans
func applyMatcherColumnar(record arrow.RecordBatch, m *Matcher, selection []bool) {
	numRows := int(record.NumRows())
	numCols := int(record.NumCols())

	// Validate schema before accessing columns
	// New schema: trace_id_hi(0), trace_id_lo(1), span_id(2), parent_span_id(3), name(4),
	//             start_time(5), end_time(6), duration(7), service_name(8), tags(9)
	switch m.Name {
	case "trace_id":
		if numCols < 2 {
			return
		}
		hiCol := record.Column(0).(*array.Uint64)
		loCol := record.Column(1).(*array.Uint64)
		for row := 0; row < numRows; row++ {
			if !selection[row] {
				continue
			}
			// Format as hex string: "%016x%016x"
			traceID := fmt.Sprintf("%016x%016x", hiCol.Value(row), loCol.Value(row))
			if !m.MatchesValue(traceID, true) {
				selection[row] = false
			}
		}

	case "span_id":
		if numCols < 3 {
			return
		}
		col := record.Column(2).(*array.Uint64)
		for row := 0; row < numRows; row++ {
			if !selection[row] {
				continue
			}
			spanID := fmt.Sprintf("%016x", col.Value(row))
			if !m.MatchesValue(spanID, true) {
				selection[row] = false
			}
		}

	case "parent_span_id":
		if numCols < 4 {
			return
		}
		col := record.Column(3).(*array.Uint64)
		for row := 0; row < numRows; row++ {
			if !selection[row] {
				continue
			}
			isNull := col.IsNull(row)
			val := ""
			if !isNull {
				val = fmt.Sprintf("%016x", col.Value(row))
			}
			if !m.MatchesValue(val, !isNull) {
				selection[row] = false
			}
		}

	case "name":
		if numCols < 5 {
			return
		}
		col := record.Column(4).(*array.String)
		for row := 0; row < numRows; row++ {
			if !selection[row] {
				continue
			}
			if !m.MatchesValue(col.Value(row), true) {
				selection[row] = false
			}
		}

	case "service.name", "service_name":
		if numCols < 9 {
			return
		}
		col := record.Column(8).(*array.String)
		for row := 0; row < numRows; row++ {
			if !selection[row] {
				continue
			}
			if !m.MatchesValue(col.Value(row), true) {
				selection[row] = false
			}
		}
	}
}

// queryArrowBlock queries an Arrow block with matchers and time range
func queryArrowBlock(ab *block.ArrowBlock, ms Matchers, timeRange *TimeRange) []*span.Span {
	spans := make([]*span.Span, 0)

	if ab.HasIndex() && len(ms) > 0 {
		idx := ab.Index()

		// Find first exact match to use as index lookup
		// Priority: trace_id > span_id > other fields
		for _, m := range ms {
			if m.Type == MatchEqual {
				var candidateSpanIDs []string
				// Check if this is a trace ID lookup (highly selective!)
				switch m.Name {
				case "trace_id":
					candidateSpanIDs = idx.LookupByTraceID(m.Value)
				case "span_id":
					if _, ok := idx.LookupSpanID(m.Value); ok {
						candidateSpanIDs = []string{m.Value}
					}
				default:
					candidateSpanIDs = idx.LookupByTag(m.Name, m.Value)
				}

				if len(candidateSpanIDs) > 0 {
					// Arrow block: use GetSpansBatch for consistent interface usage
					batchSpans, err := ab.GetSpansBatch(candidateSpanIDs)
					if err == nil {
						for _, sp := range batchSpans {
							if ms.Matches(sp) && (timeRange == nil || spanInTimeRange(sp, timeRange)) {
								spans = append(spans, sp)
							}
						}
					}
					return spans
				}
			}
		}
	}

	// Fall back to full scan using two-phase columnar filtering
	// Phase 1: Build selection bitmap using columnar access (fast, SIMD-friendly)
	// Phase 2: Extract only matching spans (avoids unnecessary allocations)
	records := ab.Records()
	needTags := matchersNeedTags(ms)

	for _, record := range records {
		// Phase 1: Build selection bitmap using columnar filtering
		selection := buildSelectionBitmap(record, ms, timeRange)

		// Phase 2: Extract only selected spans
		for row := 0; row < int(record.NumRows()); row++ {
			if !selection[row] {
				continue
			}

			sp, err := extractSpanFromRecordWithOptions(record, row, needTags)
			if err != nil {
				continue
			}

			// Final validation with full matchers (includes tag checks if needed)
			if ms.Matches(sp) && (timeRange == nil || spanInTimeRange(sp, timeRange)) {
				// If we didn't extract tags during filtering, extract them now for the final result
				if !needTags {
					sp, err = extractSpanFromRecordWithOptions(record, row, true)
					if err != nil {
						continue
					}
				}
				spans = append(spans, sp)
			}
		}
	}

	return spans
}
