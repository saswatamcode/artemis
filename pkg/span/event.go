package span

import "time"

// SpanEvent represents an event that occurred during a span's lifetime
// Events are stored separately from spans in their own table/file
type SpanEvent struct {
	SpanID     string            // Reference to parent span
	Name       string            // Event name
	Timestamp  time.Time         // Event timestamp
	Attributes map[string]string // Event attributes
}
