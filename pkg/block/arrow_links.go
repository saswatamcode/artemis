package block

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/saswatamcode/artemis/pkg/span"
)

const (
	linksFilename = "links.arrow"
)

// FlushLinksBlock writes link records to disk as Arrow IPC
func FlushLinksBlock(dir string, records []arrow.RecordBatch, schema *arrow.Schema) error {
	if len(records) == 0 {
		return nil // No links to write
	}

	linksPath := filepath.Join(dir, linksFilename)
	f, err := os.Create(linksPath)
	if err != nil {
		return fmt.Errorf("failed to create links file: %w", err)
	}

	writer, err := ipc.NewFileWriter(f, ipc.WithSchema(schema))
	if err != nil {
		f.Close()
		return fmt.Errorf("failed to create IPC writer: %w", err)
	}

	for _, rec := range records {
		if err := writer.Write(rec); err != nil {
			writer.Close()
			f.Close()
			return fmt.Errorf("failed to write link record: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		f.Close()
		return fmt.Errorf("failed to close writer: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("failed to sync links file: %w", err)
	}
	f.Close()

	return nil
}

// loadLinkRecords loads Arrow link records from the IPC file
func loadLinkRecords(dir string, mem memory.Allocator) ([]arrow.RecordBatch, *arrow.Schema, error) {
	linksPath := filepath.Join(dir, linksFilename)

	// Check if links file exists
	if _, err := os.Stat(linksPath); os.IsNotExist(err) {
		return nil, nil, nil // No links file
	}

	f, err := os.Open(linksPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open links file: %w", err)
	}
	defer f.Close()

	reader, err := ipc.NewFileReader(f, ipc.WithAllocator(mem))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create IPC reader: %w", err)
	}
	defer reader.Close()

	schema := reader.Schema()
	records := make([]arrow.RecordBatch, 0, reader.NumRecords())

	for i := 0; i < reader.NumRecords(); i++ {
		rec, err := reader.RecordBatch(i)
		if err != nil {
			// Release any records we've already retained
			for _, r := range records {
				r.Release()
			}
			return nil, nil, fmt.Errorf("failed to read link record %d: %w", i, err)
		}
		rec.Retain()
		records = append(records, rec)
	}

	return records, schema, nil
}

// extractLinkFromArrowRecord extracts a span link from an Arrow record
func extractLinkFromArrowRecord(record arrow.RecordBatch, rowIndex int) (*span.SpanLink, error) {
	if rowIndex < 0 || rowIndex >= int(record.NumRows()) {
		return nil, fmt.Errorf("invalid row index %d (record has %d rows)", rowIndex, record.NumRows())
	}

	expectedColumns := 5 // span_id, linked_trace_id_hi, linked_trace_id_lo, linked_span_id, attributes
	if record.NumCols() < int64(expectedColumns) {
		return nil, fmt.Errorf("invalid schema: expected at least %d columns, got %d", expectedColumns, record.NumCols())
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

		offsets := attrsCol.Offsets()
		if rowIndex+1 >= len(offsets) {
			return nil, fmt.Errorf("invalid offset index %d for attributes map (offsets length: %d)", rowIndex+1, len(offsets))
		}

		offset := offsets[rowIndex]
		nextOffset := offsets[rowIndex+1]

		keys := attrsCol.Keys().(*array.String)
		items := attrsCol.Items().(*array.String)

		for i := int(offset); i < int(nextOffset); i++ {
			if i >= keys.Len() || i >= items.Len() {
				return nil, fmt.Errorf("corrupted attributes data at row %d: index %d exceeds bounds (keys: %d, items: %d)",
					rowIndex, i, keys.Len(), items.Len())
			}
			key := keys.Value(i)
			value := items.Value(i)
			l.Attributes[key] = value
		}
	}

	return l, nil
}

// GetLinksBySpanIDFromArrow retrieves links for a specific span ID from Arrow records
func GetLinksBySpanIDFromArrow(records []arrow.RecordBatch, spanID string) ([]*span.SpanLink, error) {
	result := make([]*span.SpanLink, 0)

	for _, record := range records {
		for row := 0; row < int(record.NumRows()); row++ {
			l, err := extractLinkFromArrowRecord(record, row)
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

// ReadAllLinksFromArrow reads all links from Arrow records
func ReadAllLinksFromArrow(records []arrow.RecordBatch) ([]*span.SpanLink, error) {
	result := make([]*span.SpanLink, 0)

	for _, record := range records {
		for row := 0; row < int(record.NumRows()); row++ {
			l, err := extractLinkFromArrowRecord(record, row)
			if err != nil {
				return nil, fmt.Errorf("failed to extract link at row %d: %w", row, err)
			}
			result = append(result, l)
		}
	}

	return result, nil
}

// GetLinksBatch efficiently retrieves links for multiple span IDs
// Returns a map of spanID -> []SpanLink
// Single pass through all link records instead of N passes
func (ab *ArrowBlock) GetLinksBatch(spanIDs []string) (map[string][]*span.SpanLink, error) {
	if len(spanIDs) == 0 {
		return nil, nil
	}

	// Build set of span IDs we're looking for
	spanIDSet := make(map[string]struct{}, len(spanIDs))
	for _, sid := range spanIDs {
		spanIDSet[sid] = struct{}{}
	}

	// Single pass through all link records
	result := make(map[string][]*span.SpanLink)

	for _, record := range ab.linkRecords {
		for row := 0; row < int(record.NumRows()); row++ {
			l, err := extractLinkFromArrowRecord(record, row)
			if err != nil {
				continue
			}
			// Only collect links for span IDs we care about
			if _, found := spanIDSet[l.SpanID]; found {
				result[l.SpanID] = append(result[l.SpanID], l)
			}
		}
	}

	return result, nil
}
