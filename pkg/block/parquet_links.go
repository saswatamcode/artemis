package block

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

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
// Links are sorted by span_id for optimal query performance and row group statistics
func WriteParquetLinks(dir string, links []*span.SpanLink) error {
	if len(links) == 0 {
		return nil
	}

	parquetLinks := make([]ParquetSpanLink, len(links))

	for i, l := range links {
		parquetLinks[i] = *spanLinkToParquetLink(l)
	}

	// CRITICAL: Sort links by span_id for optimal query performance
	// This clustering enables:
	// 1. Row group min/max statistics to skip irrelevant row groups
	// 2. Sequential reads instead of random seeks when fetching links for a span
	sort.Slice(parquetLinks, func(i, j int) bool {
		return parquetLinks[i].SpanID < parquetLinks[j].SpanID
	})

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
// Optimizations:
// 1. Skip row groups using span_id min/max statistics (requires sorted data)
// 2. Use column projection to read only span_id first
// 3. Seek to matching rows and read full link data
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

	result := make(map[string][]*span.SpanLink)
	rowGroups := file.RowGroups()

	// Process each row group, skipping those that can't contain our span IDs
	for rgIdx, rg := range rowGroups {
		// OPTIMIZATION: Use row group statistics to skip irrelevant row groups
		// Since we sort links by span_id, each row group has a min/max span_id range
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

		// Read full link data for matching rows
		fullReader := parquet.NewGenericRowGroupReader[ParquetSpanLink](rg)
		currentRow := int64(-1)
		linkBatch := make([]ParquetSpanLink, 1)

		for _, match := range matches {
			if currentRow != match.rowIdx {
				if err := fullReader.SeekToRow(match.rowIdx); err != nil {
					continue
				}
				currentRow = match.rowIdx
			}

			n, err := fullReader.Read(linkBatch)
			if err != nil && err != io.EOF {
				continue
			}
			if n == 0 {
				continue
			}

			if originalSid, found := spanIDSet[linkBatch[0].SpanID]; found {
				result[originalSid] = append(result[originalSid], parquetLinkToSpanLink(&linkBatch[0]))
			}

			currentRow++
		}
		fullReader.Close()

		_ = rgIdx // Suppress unused variable warning
	}

	return result, nil
}
