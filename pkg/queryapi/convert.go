package queryapi

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/saswatamcode/artemis/pkg/span"
)

// parseTimestamp parses a timestamp in various formats
// Supports:
//   - Unix epoch seconds (10 digits)
//   - Unix epoch nanoseconds (19 digits)
//   - RFC3339 strings
func parseTimestamp(s string) (time.Time, error) {
	// Try parsing as integer (unix timestamp)
	if ts, err := strconv.ParseInt(s, 10, 64); err == nil {
		// Determine if seconds or nanoseconds based on magnitude
		if ts < 10000000000 {
			// Seconds (10 digits)
			return time.Unix(ts, 0), nil
		}
		// Nanoseconds (19 digits)
		return time.Unix(0, ts), nil
	}

	// Try parsing as RFC3339
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}

	// Try parsing as RFC3339Nano
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	return time.Time{}, fmt.Errorf("invalid timestamp format: %s", s)
}

// parseTimeRange parses start and end time parameters
// Supports:
//   - Unix epoch seconds (10 digits)
//   - Unix epoch nanoseconds (19 digits)
//   - RFC3339 strings
func parseTimeRange(startStr, endStr string) (time.Time, time.Time, error) {
	var start, end time.Time

	// Default to last hour if not specified
	if startStr == "" && endStr == "" {
		end = time.Now()
		start = end.Add(-1 * time.Hour)
		return start, end, nil
	}

	// Parse start time
	if startStr != "" {
		var err error
		start, err = parseTimestamp(startStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid start time: %w", err)
		}
	} else {
		start = time.Now().Add(-1 * time.Hour)
	}

	// Parse end time
	if endStr != "" {
		var err error
		end, err = parseTimestamp(endStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid end time: %w", err)
		}
	} else {
		end = time.Now()
	}

	return start, end, nil
}

// respondJSON writes a JSON response with proper headers
func respondJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// respondError writes an error response in JSON format
func respondError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "error",
		"error":  message,
	})
}

// calculateDefaultStep calculates a reasonable default step based on time range
func calculateDefaultStep(start, end time.Time) string {
	duration := end.Sub(start)

	switch {
	case duration <= 5*time.Minute:
		return "5s"
	case duration <= 30*time.Minute:
		return "15s"
	case duration <= 2*time.Hour:
		return "30s"
	case duration <= 6*time.Hour:
		return "1m"
	case duration <= 24*time.Hour:
		return "5m"
	case duration <= 7*24*time.Hour:
		return "15m"
	default:
		return "1h"
	}
}

// parseDuration parses a duration string (e.g., "15s", "1m", "5m")
func parseDuration(s string) (time.Duration, error) {
	return time.ParseDuration(s)
}

// selectExemplars applies strategy to select N spans
func selectExemplars(spans []*span.Span, maxCount int, strategy string) []Exemplar {
	if len(spans) == 0 || maxCount <= 0 {
		return nil
	}

	// Sort based on strategy with stable secondary sort by TraceID for determinism
	switch strategy {
	case "slowest":
		sort.Slice(spans, func(i, j int) bool {
			if spans[i].Duration != spans[j].Duration {
				return spans[i].Duration > spans[j].Duration
			}
			// Stable sort: use TraceID as tiebreaker
			return spans[i].TraceID < spans[j].TraceID
		})
	case "fastest":
		sort.Slice(spans, func(i, j int) bool {
			if spans[i].Duration != spans[j].Duration {
				return spans[i].Duration < spans[j].Duration
			}
			return spans[i].TraceID < spans[j].TraceID
		})
	case "random":
		rand.Shuffle(len(spans), func(i, j int) {
			spans[i], spans[j] = spans[j], spans[i]
		})
	default:
		// Default to slowest
		sort.Slice(spans, func(i, j int) bool {
			if spans[i].Duration != spans[j].Duration {
				return spans[i].Duration > spans[j].Duration
			}
			return spans[i].TraceID < spans[j].TraceID
		})
	}

	// Take top N
	count := maxCount
	if len(spans) < count {
		count = len(spans)
	}

	exemplars := make([]Exemplar, count)
	for i := 0; i < count; i++ {
		sp := spans[i]
		exemplars[i] = Exemplar{
			Timestamp: sp.StartTime.Unix(),
			TraceID:   sp.TraceID,
			SpanID:    sp.SpanID,
			Duration:  sp.Duration,
			Labels:    sp.Tags,
		}
	}

	return exemplars
}

// convertSpanToDetail converts a span to SpanDetail format
func convertSpanToDetail(sp *span.Span) SpanDetail {
	return SpanDetail{
		SpanID:       sp.SpanID,
		Name:         sp.Name,
		StartTime:    sp.StartTime.UnixNano(),
		EndTime:      sp.EndTime.UnixNano(),
		Duration:     sp.Duration,
		ServiceName:  sp.ServiceName,
		Attributes:   sp.Tags,
		ParentSpanID: sp.ParentSpanID,
	}
}
