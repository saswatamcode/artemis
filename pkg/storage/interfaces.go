package storage

import "github.com/saswatamcode/artemis/pkg/span"

// SpanStorage provides storage operations for spans
// This interface enables better testability and allows for different storage implementations
type SpanStorage interface {
	// AddSpan adds a span to the storage
	AddSpan(s *span.Span) error

	// GetSpanByID retrieves a single span by its ID
	GetSpanByID(spanID string) (*span.Span, error)

	// RowCount returns the total number of spans stored
	RowCount() int64

	// Reset clears all data in the storage
	Reset()
}

// LinkStorage provides storage operations for span links
// This interface enables better testability and allows for different storage implementations
type LinkStorage interface {
	// AddLink adds a single span link to the storage
	AddLink(link *span.SpanLink) error

	// AddLinks adds multiple span links to the storage in a single operation
	AddLinks(links []*span.SpanLink) error

	// GetLinksBySpanID retrieves all links for a given span ID
	GetLinksBySpanID(spanID string) ([]*span.SpanLink, error)

	// GetLinksBatch efficiently retrieves links for multiple span IDs
	// Returns a map of spanID -> links
	GetLinksBatch(spanIDs []string) (map[string][]*span.SpanLink, error)

	// RowCount returns the total number of links stored
	RowCount() int64

	// Reset clears all data in the link storage
	Reset()
}

// Compile-time interface checks
// These ensure that our implementations satisfy the interfaces
var _ SpanStorage = (*ArrowStorage)(nil)
var _ LinkStorage = (*ArrowLinkStorage)(nil)
