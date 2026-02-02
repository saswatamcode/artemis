package query

import (
	"time"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// SelectResult holds the results of a Select query
type SelectResult struct {
	Spans []*span.Span
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
// Works with both Arrow (L0) and Parquet (L1+) blocks
func SelectFromBlocks(head *storage.ArrowStorage, blocks []block.Block, matchers ...*Matcher) (*SelectResult, error) {
	return SelectFromBlocksWithTimeRange(head, blocks, nil, matchers...)
}

// SelectFromBlocksWithTimeRange queries spans across blocks within a specific time range
// Time range filtering provides significant performance benefits by skipping irrelevant blocks
// Works with both Arrow (L0) and Parquet (L1+) blocks
// TODO: Parallelize such fanout
func SelectFromBlocksWithTimeRange(head *storage.ArrowStorage, blocks []block.Block, timeRange *TimeRange, matchers ...*Matcher) (*SelectResult, error) {
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

		blockSpans := queryBlockWithTimeRange(blk, ms, timeRange)
		result.Spans = append(result.Spans, blockSpans...)
	}

	// Query head block last (most recent data)
	if head != nil {
		if timeRange == nil || headOverlapsTimeRange(head, timeRange) {
			headResult, err := Select(head, matchers...)
			if err == nil && headResult != nil {
				for _, sp := range headResult.Spans {
					if timeRange == nil || spanInTimeRange(sp, timeRange) {
						result.Spans = append(result.Spans, sp)
					}
				}
			}
		}
	}

	return result, nil
}

// headOverlapsTimeRange checks if the head block overlaps with a time range
func headOverlapsTimeRange(head *storage.ArrowStorage, timeRange *TimeRange) bool {
	minTime, maxTime := head.GetTimeRange()
	if minTime == 0 && maxTime == 0 {
		return true // Empty head block or no time tracking
	}
	return timeRange.Overlaps(minTime, maxTime)
}

// spanInTimeRange checks if a span falls within the time range
func spanInTimeRange(sp *span.Span, timeRange *TimeRange) bool {
	// A span is in range if either its start or end time is in the range
	// or if it completely encompasses the range
	return timeRange.Contains(sp.StartTime) ||
		timeRange.Contains(sp.EndTime) ||
		(sp.StartTime.Before(timeRange.Start) && sp.EndTime.After(timeRange.End))
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
		if timeRange != nil {
			startTime := time.Unix(0, meta.StartTime)
			endTime := time.Unix(0, meta.EndTime)
			if !timeRange.Contains(startTime) && !timeRange.Contains(endTime) &&
				(!startTime.Before(timeRange.Start) || !endTime.After(timeRange.End)) {
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
				val = meta.TraceID
				ok = val != ""
			case "span_id":
				val = meta.SpanID
				ok = val != ""
			case "parent_span_id":
				val = meta.ParentSpanID
				ok = val != ""
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

// queryBlockWithTimeRange queries a single block with matchers and time range
// Works with both Arrow (L0) and Parquet (L1+) blocks
func queryBlockWithTimeRange(blk block.Block, ms Matchers, timeRange *TimeRange) []*span.Span {
	spans := make([]*span.Span, 0)

	// Check if this is a Parquet block (Records() returns nil)
	isParquetBlock := blk.Records() == nil
	if blk.HasIndex() && len(ms) > 0 {
		idx := blk.Index()

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
					if isParquetBlock {
						// Parquet block: use GetSpansBatch for efficient batched reads
						// This groups I/O by row group instead of reading spans one-by-one
						if pb, ok := blk.(*block.ParquetBlock); ok {
							batchSpans, err := pb.GetSpansBatch(candidateSpanIDs)
							if err == nil {
								for _, sp := range batchSpans {
									if ms.Matches(sp) && (timeRange == nil || spanInTimeRange(sp, timeRange)) {
										spans = append(spans, sp)
									}
								}
							}
						}
					} else {
						records := blk.Records()
						for _, spanID := range candidateSpanIDs {
							ref, ok := idx.LookupSpanID(spanID)
							if !ok || ref.RecordIndex >= len(records) {
								continue
							}

							sp, err := extractSpanFromRecord(records[ref.RecordIndex], ref.RowIndex)
							if err != nil {
								continue
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

	// Fall back to full scan (no index or no exact match matchers)
	if isParquetBlock {
		// For Parquet blocks, use efficient column projection instead of ReadAll()
		// This only reads metadata columns (excludes tags), which is much faster
		if pb, ok := blk.(*block.ParquetBlock); ok {
			filterFunc := createMetadataFilter(ms, timeRange)
			// Scan metadata columns only
			refs, err := pb.ScanMetadata(filterFunc)
			if err != nil {
				return spans
			}

			// Fetch full spans only for matching rows
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
		}
		return spans
	}

	// Arrow block: full scan using Records()
	records := blk.Records()
	for _, record := range records {
		for row := 0; row < int(record.NumRows()); row++ {
			sp, err := extractSpanFromRecord(record, row)
			if err != nil {
				continue
			}

			if ms.Matches(sp) && (timeRange == nil || spanInTimeRange(sp, timeRange)) {
				spans = append(spans, sp)
			}
		}
	}

	return spans
}
