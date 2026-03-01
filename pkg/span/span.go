package span

import "time"

// Span represents a distributed trace span
type Span struct {
	TraceID      string            // Unique trace identifier
	SpanID       string            // Unique span identifier
	ParentSpanID string            // Parent span ID (empty for root spans)
	Name         string            // Operation name
	StartTime    time.Time         // Span start timestamp
	EndTime      time.Time         // Span end timestamp
	Duration     int64             // Duration in nanoseconds
	Tags         map[string]string // Span tags/attributes
	ServiceName  string            // Service that created this span
	Links        []SpanLink        // Span links (nil by default, populated on request)
}

// Duration returns the span duration in nanoseconds
func (s *Span) GetDuration() int64 {
	if s.Duration != 0 {
		return s.Duration
	}
	return s.EndTime.Sub(s.StartTime).Nanoseconds()
}
