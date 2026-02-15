package block

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"

	"github.com/saswatamcode/artemis/pkg/span"
)

const (
	parquetLinksFilename = "links.parquet"
)

// ParquetSpanLink is the Parquet-optimized representation of a span link
type ParquetSpanLink struct {
	SpanID          uint64            `parquet:"span_id"`
	LinkedTraceIDHi uint64            `parquet:"linked_trace_id_hi"`
	LinkedTraceIDLo uint64            `parquet:"linked_trace_id_lo"`
	LinkedSpanID    uint64            `parquet:"linked_span_id"`
	Attributes      map[string]string `parquet:"attributes,optional" parquet-key:"dict" parquet-value:"dict"`
}

// spanLinkToParquetLink converts a SpanLink to ParquetSpanLink
func spanLinkToParquetLink(l *span.SpanLink) *ParquetSpanLink {
	// Parse span ID
	spanID, err := span.ParseSpanID(l.SpanID)
	if err != nil {
		spanID = 0
	}

	// Parse linked trace ID into hi/lo components
	linkedTraceIDHi, linkedTraceIDLo, err := span.ParseTraceID(l.LinkedTraceID)
	if err != nil {
		linkedTraceIDHi, linkedTraceIDLo = 0, 0
	}

	// Parse linked span ID
	linkedSpanID, err := span.ParseSpanID(l.LinkedSpanID)
	if err != nil {
		linkedSpanID = 0
	}

	return &ParquetSpanLink{
		SpanID:          spanID,
		LinkedTraceIDHi: linkedTraceIDHi,
		LinkedTraceIDLo: linkedTraceIDLo,
		LinkedSpanID:    linkedSpanID,
		Attributes:      l.Attributes,
	}
}

// parquetLinkToSpanLink converts a ParquetSpanLink to SpanLink
func parquetLinkToSpanLink(pl *ParquetSpanLink) *span.SpanLink {
	// Convert span ID to hex string
	spanID := fmt.Sprintf("%016x", pl.SpanID)

	// Convert linked trace ID from hi/lo to hex string
	linkedTraceID := fmt.Sprintf("%016x%016x", pl.LinkedTraceIDHi, pl.LinkedTraceIDLo)

	// Convert linked span ID to hex string
	linkedSpanID := fmt.Sprintf("%016x", pl.LinkedSpanID)

	return &span.SpanLink{
		SpanID:        spanID,
		LinkedTraceID: linkedTraceID,
		LinkedSpanID:  linkedSpanID,
		Attributes:    pl.Attributes,
	}
}

// WriteParquetLinks writes span links to a Parquet file
func WriteParquetLinks(dir string, links []*span.SpanLink) error {
	if len(links) == 0 {
		return nil
	}

	parquetLinks := make([]ParquetSpanLink, len(links))
	for i, l := range links {
		parquetLinks[i] = *spanLinkToParquetLink(l)
	}

	linksPath := filepath.Join(dir, parquetLinksFilename)
	f, err := os.Create(linksPath)
	if err != nil {
		return fmt.Errorf("failed to create links parquet file: %w", err)
	}

	writer := parquet.NewGenericWriter[ParquetSpanLink](
		f,
		parquet.Compression(&snappy.Codec{}),
		parquet.PageBufferSize(100*1024*1024),
	)

	_, err = writer.Write(parquetLinks)
	if err != nil {
		writer.Close()
		f.Close()
		return fmt.Errorf("failed to write parquet links data: %w", err)
	}

	if err := writer.Close(); err != nil {
		f.Close()
		return fmt.Errorf("failed to close links writer: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("failed to sync links parquet file: %w", err)
	}
	f.Close()

	return nil
}

// ReadParquetLinks reads all links from a Parquet links file
func ReadParquetLinks(dir string) ([]*span.SpanLink, error) {
	linksPath := filepath.Join(dir, parquetLinksFilename)

	// Check if links file exists
	if _, err := os.Stat(linksPath); os.IsNotExist(err) {
		return nil, nil // No links file, return empty
	}

	f, err := os.Open(linksPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open links parquet file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat links parquet file: %w", err)
	}

	file, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to open links parquet file: %w", err)
	}

	reader := parquet.NewGenericReader[ParquetSpanLink](file)
	defer reader.Close()

	totalLinks := make([]*span.SpanLink, 0, file.NumRows())
	batch := make([]ParquetSpanLink, 1024)

	for {
		n, err := reader.Read(batch)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read links: %w", err)
		}

		for i := range n {
			totalLinks = append(totalLinks, parquetLinkToSpanLink(&batch[i]))
		}

		if err == io.EOF {
			break
		}
	}

	return totalLinks, nil
}

// GetLinksBySpanIDFromParquet retrieves links for a specific span ID from a Parquet file
func GetLinksBySpanIDFromParquet(dir string, spanID string) ([]*span.SpanLink, error) {
	linksPath := filepath.Join(dir, parquetLinksFilename)

	// Check if links file exists
	if _, err := os.Stat(linksPath); os.IsNotExist(err) {
		return nil, nil // No links file
	}

	// Parse the input span ID to int64 for comparison
	spanIDInt, err := span.ParseSpanID(spanID)
	if err != nil {
		return nil, fmt.Errorf("invalid span ID: %w", err)
	}

	f, err := os.Open(linksPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open links parquet file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat links parquet file: %w", err)
	}

	file, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to open links parquet file: %w", err)
	}

	reader := parquet.NewGenericReader[ParquetSpanLink](file)
	defer reader.Close()

	result := make([]*span.SpanLink, 0)
	batch := make([]ParquetSpanLink, 1024)

	for {
		n, err := reader.Read(batch)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read links: %w", err)
		}

		for i := range n {
			if batch[i].SpanID == spanIDInt {
				result = append(result, parquetLinkToSpanLink(&batch[i]))
			}
		}

		if err == io.EOF {
			break
		}
	}

	return result, nil
}

// GetLinksBatch efficiently retrieves links for multiple span IDs
// Returns a map of spanID -> []SpanLink
// OPTIMIZATION: Single pass through parquet file instead of N passes
func (pb *ParquetBlock) GetLinksBatch(spanIDs []string) (map[string][]*span.SpanLink, error) {
	if len(spanIDs) == 0 {
		return nil, nil
	}

	linksPath := filepath.Join(pb.dir, parquetLinksFilename)

	// Check if links file exists
	if _, err := os.Stat(linksPath); os.IsNotExist(err) {
		return nil, nil // No links file
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

	f, err := os.Open(linksPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open links parquet file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat links parquet file: %w", err)
	}

	file, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to open links parquet file: %w", err)
	}

	reader := parquet.NewGenericReader[ParquetSpanLink](file)
	defer reader.Close()

	result := make(map[string][]*span.SpanLink)
	batch := make([]ParquetSpanLink, 1024)

	// Single pass through all links
	for {
		n, err := reader.Read(batch)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read links: %w", err)
		}

		for i := range n {
			// Only collect links for span IDs we care about
			if originalSid, found := spanIDSet[batch[i].SpanID]; found {
				result[originalSid] = append(result[originalSid], parquetLinkToSpanLink(&batch[i]))
			}
		}

		if err == io.EOF {
			break
		}
	}

	return result, nil
}
