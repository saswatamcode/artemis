package block

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/saswatamcode/artemis/pkg/span"
)

const (
	linksFilename = "links.arrow"
)

// FlushLinksBlock writes link records to disk as Arrow IPC.
// For backward compatibility, this delegates to FlushLinksBlockContext with a background context.
func FlushLinksBlock(dir string, records []arrow.RecordBatch, schema *arrow.Schema) error {
	return FlushLinksBlockContext(context.Background(), dir, records, schema)
}

// FlushLinksBlockContext writes link records to disk as Arrow IPC with context support.
func FlushLinksBlockContext(ctx context.Context, dir string, records []arrow.RecordBatch, schema *arrow.Schema) error {
	// Check context before starting
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled before links flush: %w", err)
	}
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

	// Write records with context checks every 10 records
	for i, rec := range records {
		if i%10 == 0 {
			if err := ctx.Err(); err != nil {
				writer.Close()
				f.Close()
				return fmt.Errorf("context cancelled during link write (wrote %d/%d records): %w", i, len(records), err)
			}
		}

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
// extractLinkFromArrowRecord extracts a link from an Arrow record.
// DEPRECATED: Use span.ExtractLinkFromRecord instead. This wrapper is kept for backward compatibility.
func extractLinkFromArrowRecord(record arrow.RecordBatch, rowIndex int) (*span.SpanLink, error) {
	return span.ExtractLinkFromRecord(record, rowIndex)
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
