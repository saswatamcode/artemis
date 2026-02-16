package block

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/saswatamcode/artemis/pkg/span"
)

const (
	eventsFilename = "events.arrow"
)

// FlushEventsBlock writes event records to disk as Arrow IPC
func FlushEventsBlock(dir string, records []arrow.RecordBatch, schema *arrow.Schema) error {
	if len(records) == 0 {
		return nil // No events to write
	}

	eventsPath := filepath.Join(dir, eventsFilename)
	f, err := os.Create(eventsPath)
	if err != nil {
		return fmt.Errorf("failed to create events file: %w", err)
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
			return fmt.Errorf("failed to write event record: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		f.Close()
		return fmt.Errorf("failed to close writer: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("failed to sync events file: %w", err)
	}
	f.Close()

	return nil
}

// loadEventRecords loads Arrow event records from the IPC file
func loadEventRecords(dir string, mem memory.Allocator) ([]arrow.RecordBatch, *arrow.Schema, error) {
	eventsPath := filepath.Join(dir, eventsFilename)

	// Check if events file exists
	if _, err := os.Stat(eventsPath); os.IsNotExist(err) {
		return nil, nil, nil // No events file
	}

	f, err := os.Open(eventsPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open events file: %w", err)
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
			return nil, nil, fmt.Errorf("failed to read event record %d: %w", i, err)
		}
		rec.Retain()
		records = append(records, rec)
	}

	return records, schema, nil
}

// extractEventFromArrowRecord extracts a span event from an Arrow record
func extractEventFromArrowRecord(record arrow.RecordBatch, rowIndex int) (*span.SpanEvent, error) {
	if rowIndex < 0 || rowIndex >= int(record.NumRows()) {
		return nil, fmt.Errorf("invalid row index %d (record has %d rows)", rowIndex, record.NumRows())
	}

	expectedColumns := 4 // span_id, name, timestamp, attributes
	if record.NumCols() < int64(expectedColumns) {
		return nil, fmt.Errorf("invalid schema: expected at least %d columns, got %d", expectedColumns, record.NumCols())
	}

	e := &span.SpanEvent{}

	// Read span_id and format as hex string
	spanIDVal := record.Column(0).(*array.Uint64).Value(rowIndex)
	e.SpanID = fmt.Sprintf("%016x", spanIDVal)

	e.Name = record.Column(1).(*array.String).Value(rowIndex)
	e.Timestamp = time.Unix(0, record.Column(2).(*array.Int64).Value(rowIndex))

	attrsCol := record.Column(3).(*array.Map)
	if !attrsCol.IsNull(rowIndex) {
		e.Attributes = make(map[string]string)

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
			e.Attributes[key] = value
		}
	}

	return e, nil
}

// GetEventsBySpanIDFromArrow retrieves events for a specific span ID from Arrow records
func GetEventsBySpanIDFromArrow(records []arrow.RecordBatch, spanID string) ([]*span.SpanEvent, error) {
	result := make([]*span.SpanEvent, 0)

	for _, record := range records {
		for row := 0; row < int(record.NumRows()); row++ {
			e, err := extractEventFromArrowRecord(record, row)
			if err != nil {
				continue
			}
			if e.SpanID == spanID {
				result = append(result, e)
			}
		}
	}

	return result, nil
}

// ReadAllEventsFromArrow reads all events from Arrow records
func ReadAllEventsFromArrow(records []arrow.RecordBatch) ([]*span.SpanEvent, error) {
	result := make([]*span.SpanEvent, 0)

	for _, record := range records {
		for row := 0; row < int(record.NumRows()); row++ {
			e, err := extractEventFromArrowRecord(record, row)
			if err != nil {
				return nil, fmt.Errorf("failed to extract event at row %d: %w", row, err)
			}
			result = append(result, e)
		}
	}

	return result, nil
}

// GetEventsBatch efficiently retrieves events for multiple span IDs
// Returns a map of spanID -> []SpanEvent
// Single pass through all event records instead of N passes
func (ab *ArrowBlock) GetEventsBatch(spanIDs []string) (map[string][]*span.SpanEvent, error) {
	if len(spanIDs) == 0 {
		return nil, nil
	}

	// Build set of span IDs we're looking for
	spanIDSet := make(map[string]struct{}, len(spanIDs))
	for _, sid := range spanIDs {
		spanIDSet[sid] = struct{}{}
	}

	// Single pass through all event records
	result := make(map[string][]*span.SpanEvent)

	for _, record := range ab.eventRecords {
		for row := 0; row < int(record.NumRows()); row++ {
			e, err := extractEventFromArrowRecord(record, row)
			if err != nil {
				continue
			}
			// Only collect events for span IDs we care about
			if _, found := spanIDSet[e.SpanID]; found {
				result[e.SpanID] = append(result[e.SpanID], e)
			}
		}
	}

	return result, nil
}
