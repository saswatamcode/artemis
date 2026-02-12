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
	SpanID        string            `parquet:"span_id,dict"`
	LinkedTraceID string            `parquet:"linked_trace_id,dict"`
	LinkedSpanID  string            `parquet:"linked_span_id,dict"`
	Attributes    map[string]string `parquet:"attributes,optional" parquet-key:"dict" parquet-value:"dict"`
}

// spanLinkToParquetLink converts a SpanLink to ParquetSpanLink
func spanLinkToParquetLink(l *span.SpanLink) *ParquetSpanLink {
	return &ParquetSpanLink{
		SpanID:        l.SpanID,
		LinkedTraceID: l.LinkedTraceID,
		LinkedSpanID:  l.LinkedSpanID,
		Attributes:    l.Attributes,
	}
}

// parquetLinkToSpanLink converts a ParquetSpanLink to SpanLink
func parquetLinkToSpanLink(pl *ParquetSpanLink) *span.SpanLink {
	return &span.SpanLink{
		SpanID:        pl.SpanID,
		LinkedTraceID: pl.LinkedTraceID,
		LinkedSpanID:  pl.LinkedSpanID,
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
			if batch[i].SpanID == spanID {
				result = append(result, parquetLinkToSpanLink(&batch[i]))
			}
		}

		if err == io.EOF {
			break
		}
	}

	return result, nil
}
