package block

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"

	"github.com/saswatamcode/artemis/pkg/span"
)

const (
	parquetEventsFilename = "events.parquet"
)

// ParquetSpanEvent is the Parquet-optimized representation of a span event
type ParquetSpanEvent struct {
	SpanID     uint64            `parquet:"span_id"`
	Name       string            `parquet:"name,dict"`
	Timestamp  int64             `parquet:"timestamp"`
	Attributes map[string]string `parquet:"attributes,optional" parquet-key:"dict" parquet-value:"dict"`
}

// spanEventToParquetEvent converts a SpanEvent to ParquetSpanEvent
func spanEventToParquetEvent(e *span.SpanEvent) *ParquetSpanEvent {
	// Parse span ID
	spanID, err := span.ParseSpanID(e.SpanID)
	if err != nil {
		spanID = 0
	}

	return &ParquetSpanEvent{
		SpanID:     spanID,
		Name:       e.Name,
		Timestamp:  e.Timestamp.UnixNano(),
		Attributes: e.Attributes,
	}
}

// parquetEventToSpanEvent converts a ParquetSpanEvent to SpanEvent
func parquetEventToSpanEvent(pe *ParquetSpanEvent) *span.SpanEvent {
	// Convert span ID to hex string
	spanID := fmt.Sprintf("%016x", pe.SpanID)

	return &span.SpanEvent{
		SpanID:     spanID,
		Name:       pe.Name,
		Timestamp:  time.Unix(0, pe.Timestamp),
		Attributes: pe.Attributes,
	}
}

// WriteParquetEvents writes span events to a Parquet file
// Events are sorted by span_id for optimal query performance and row group statistics
func WriteParquetEvents(dir string, events []*span.SpanEvent) error {
	if len(events) == 0 {
		return nil
	}

	parquetEvents := make([]ParquetSpanEvent, len(events))

	for i, e := range events {
		parquetEvents[i] = *spanEventToParquetEvent(e)
	}

	// CRITICAL: Sort events by span_id for optimal query performance
	// This clustering enables:
	// 1. Row group min/max statistics to skip irrelevant row groups
	// 2. Sequential reads instead of random seeks when fetching events for a span
	sort.Slice(parquetEvents, func(i, j int) bool {
		return parquetEvents[i].SpanID < parquetEvents[j].SpanID
	})

	eventsPath := filepath.Join(dir, parquetEventsFilename)
	f, err := os.Create(eventsPath)
	if err != nil {
		return fmt.Errorf("failed to create events parquet file: %w", err)
	}

	writer := parquet.NewGenericWriter[ParquetSpanEvent](
		f,
		parquet.Compression(&snappy.Codec{}),
		parquet.PageBufferSize(100*1024*1024),
	)

	_, err = writer.Write(parquetEvents)
	if err != nil {
		writer.Close()
		f.Close()
		return fmt.Errorf("failed to write parquet events data: %w", err)
	}

	if err := writer.Close(); err != nil {
		f.Close()
		return fmt.Errorf("failed to close events writer: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("failed to sync events parquet file: %w", err)
	}
	f.Close()

	return nil
}

// ReadParquetEvents reads all events from a Parquet events file
func ReadParquetEvents(dir string) ([]*span.SpanEvent, error) {
	eventsPath := filepath.Join(dir, parquetEventsFilename)

	// Check if events file exists
	if _, err := os.Stat(eventsPath); os.IsNotExist(err) {
		return nil, nil // No events file, return empty
	}

	f, err := os.Open(eventsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open events parquet file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat events parquet file: %w", err)
	}

	file, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to open events parquet file: %w", err)
	}

	reader := parquet.NewGenericReader[ParquetSpanEvent](file)
	defer reader.Close()

	totalEvents := make([]*span.SpanEvent, 0, file.NumRows())
	batch := make([]ParquetSpanEvent, 1024)

	for {
		n, err := reader.Read(batch)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read events: %w", err)
		}

		for i := range n {
			totalEvents = append(totalEvents, parquetEventToSpanEvent(&batch[i]))
		}

		if err == io.EOF {
			break
		}
	}

	return totalEvents, nil
}

// GetEventsBySpanIDFromParquet retrieves events for a specific span ID from a Parquet file
func GetEventsBySpanIDFromParquet(dir string, spanID string) ([]*span.SpanEvent, error) {
	eventsPath := filepath.Join(dir, parquetEventsFilename)

	// Check if events file exists
	if _, err := os.Stat(eventsPath); os.IsNotExist(err) {
		return nil, nil // No events file
	}

	// Parse the input span ID to int64 for comparison
	spanIDInt, err := span.ParseSpanID(spanID)
	if err != nil {
		return nil, fmt.Errorf("invalid span ID: %w", err)
	}

	f, err := os.Open(eventsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open events parquet file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat events parquet file: %w", err)
	}

	file, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to open events parquet file: %w", err)
	}

	reader := parquet.NewGenericReader[ParquetSpanEvent](file)
	defer reader.Close()

	result := make([]*span.SpanEvent, 0)
	batch := make([]ParquetSpanEvent, 1024)

	for {
		n, err := reader.Read(batch)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read events: %w", err)
		}

		for i := range n {
			if batch[i].SpanID == spanIDInt {
				result = append(result, parquetEventToSpanEvent(&batch[i]))
			}
		}

		if err == io.EOF {
			break
		}
	}

	return result, nil
}

// GetEventsBatch efficiently retrieves events for multiple span IDs
// Returns a map of spanID -> []SpanEvent
// Optimizations:
// 1. Skip row groups using span_id min/max statistics (requires sorted data)
// 2. Use column projection to read only span_id first
// 3. Seek to matching rows and read full event data
func (pb *ParquetBlock) GetEventsBatch(spanIDs []string) (map[string][]*span.SpanEvent, error) {
	if len(spanIDs) == 0 {
		return nil, nil
	}

	eventsPath := filepath.Join(pb.dir, parquetEventsFilename)

	// Check if events file exists
	if _, err := os.Stat(eventsPath); os.IsNotExist(err) {
		return nil, nil // No events file
	}

	// Parse all span IDs to uint64 and build set for fast lookup
	spanIDSet := make(map[uint64]string, len(spanIDs)) // uint64 -> original string
	minSpanID := uint64(^uint64(0))                    // max uint64
	maxSpanID := uint64(0)

	for _, sid := range spanIDs {
		sidInt, err := span.ParseSpanID(sid)
		if err != nil {
			continue // Skip invalid span IDs
		}
		spanIDSet[sidInt] = sid
		if sidInt < minSpanID {
			minSpanID = sidInt
		}
		if sidInt > maxSpanID {
			maxSpanID = sidInt
		}
	}

	if len(spanIDSet) == 0 {
		return nil, nil
	}

	f, err := os.Open(eventsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open events parquet file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat events parquet file: %w", err)
	}

	file, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to open events parquet file: %w", err)
	}

	result := make(map[string][]*span.SpanEvent)
	rowGroups := file.RowGroups()

	// Process each row group, skipping those that can't contain our span IDs
	for rgIdx, rg := range rowGroups {
		// OPTIMIZATION: Use row group statistics to skip irrelevant row groups
		// Since we sort events by span_id, each row group has a min/max span_id range
		columnChunks := rg.ColumnChunks()
		if len(columnChunks) == 0 {
			continue
		}

		spanIDColumn := columnChunks[0] // span_id is first column

		// Check column index for min/max values to potentially skip this row group
		columnIndex, err := spanIDColumn.ColumnIndex()
		canSkipRowGroup := false
		if err == nil && columnIndex != nil && columnIndex.NumPages() > 0 {
			// Check if ANY page in this row group could contain our target span IDs
			rowGroupHasMatch := false
			for pageIdx := 0; pageIdx < columnIndex.NumPages(); pageIdx++ {
				if columnIndex.NullPage(pageIdx) {
					continue
				}

				pageMin := columnIndex.MinValue(pageIdx)
				pageMax := columnIndex.MaxValue(pageIdx)

				if len(pageMin.Bytes()) == 8 && len(pageMax.Bytes()) == 8 {
					// Decode uint64 little-endian
					pageMinVal := binary.LittleEndian.Uint64(pageMin.Bytes())
					pageMaxVal := binary.LittleEndian.Uint64(pageMax.Bytes())

					// Check if our query range [minSpanID, maxSpanID] overlaps with page range
					if !(pageMaxVal < minSpanID || pageMinVal > maxSpanID) {
						rowGroupHasMatch = true
						break
					}
				} else {
					// Can't decode statistics, assume row group might have data
					rowGroupHasMatch = true
					break
				}
			}

			if !rowGroupHasMatch {
				canSkipRowGroup = true
			}
		}

		if canSkipRowGroup {
			continue // Skip this entire row group
		}

		// Row group might contain our data - scan it using column projection
		type spanIDOnly struct {
			SpanID uint64 `parquet:"span_id"`
		}

		projectedReader := parquet.NewGenericRowGroupReader[spanIDOnly](rg)

		// Find matching rows within this row group
		type matchInfo struct {
			rowIdx int64
		}
		matches := make([]matchInfo, 0)
		batch := make([]spanIDOnly, 1024)
		rowIdxInGroup := int64(0)

		for {
			n, err := projectedReader.Read(batch)
			if err != nil && err != io.EOF {
				projectedReader.Close()
				break
			}

			for i := range n {
				if _, found := spanIDSet[batch[i].SpanID]; found {
					matches = append(matches, matchInfo{
						rowIdx: rowIdxInGroup + int64(i),
					})
				}
			}

			rowIdxInGroup += int64(n)

			if err == io.EOF {
				break
			}
		}
		projectedReader.Close()

		if len(matches) == 0 {
			continue // No matches in this row group
		}

		// Read full event data for matching rows
		fullReader := parquet.NewGenericRowGroupReader[ParquetSpanEvent](rg)
		currentRow := int64(-1)
		eventBatch := make([]ParquetSpanEvent, 1)

		for _, match := range matches {
			if currentRow != match.rowIdx {
				if err := fullReader.SeekToRow(match.rowIdx); err != nil {
					continue
				}
				currentRow = match.rowIdx
			}

			n, err := fullReader.Read(eventBatch)
			if err != nil && err != io.EOF {
				continue
			}
			if n == 0 {
				continue
			}

			if originalSid, found := spanIDSet[eventBatch[0].SpanID]; found {
				result[originalSid] = append(result[originalSid], parquetEventToSpanEvent(&eventBatch[0]))
			}

			currentRow++
		}
		fullReader.Close()

		// Early exit optimization: if we've found events for all requested span IDs, stop
		if len(result) == len(spanIDSet) {
			// Check if each span has at least one event (might have more)
			allFound := true
			for spanID := range spanIDSet {
				if _, found := result[spanIDSet[spanID]]; !found {
					allFound = false
					break
				}
			}
			if allFound {
				// We could break here, but spans might have multiple events across row groups
				// Continue processing to get all events
			}
		}

		_ = rgIdx // Suppress unused variable warning
	}

	return result, nil
}
