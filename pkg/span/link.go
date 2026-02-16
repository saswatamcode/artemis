package span

// SpanLink represents a link from one span to another span
// Links are used to associate spans that are not in a parent-child relationship
// For example: batch processing, async operations, or cross-service dependencies
type SpanLink struct {
	SpanID        string            // The span that owns this link
	LinkedTraceID string            // Trace ID of the linked span
	LinkedSpanID  string            // Span ID of the linked span
	Attributes    map[string]string // Link attributes
}
