package block

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
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
func WriteParquetEvents(dir string, events []*span.SpanEvent) error {
	if len(events) == 0 {
		return nil
	}

	parquetEvents := make([]ParquetSpanEvent, len(events))
	for i, e := range events {
		parquetEvents[i] = *spanEventToParquetEvent(e)
	}

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
// OPTIMIZATION: Single pass through parquet file instead of N passes
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
	for _, sid := range spanIDs {
		sidInt, err := span.ParseSpanID(sid)
		if err != nil {
			continue // Skip invalid span IDs
		}
		spanIDSet[sidInt] = sid
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

	reader := parquet.NewGenericReader[ParquetSpanEvent](file)
	defer reader.Close()

	result := make(map[string][]*span.SpanEvent)
	batch := make([]ParquetSpanEvent, 1024)

	// Single pass through all events
	for {
		n, err := reader.Read(batch)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read events: %w", err)
		}

		for i := range n {
			// Only collect events for span IDs we care about
			if originalSid, found := spanIDSet[batch[i].SpanID]; found {
				result[originalSid] = append(result[originalSid], parquetEventToSpanEvent(&batch[i]))
			}
		}

		if err == io.EOF {
			break
		}
	}

	return result, nil
}
