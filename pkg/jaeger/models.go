package jaeger

import (
	"strconv"
	"strings"
	"time"

	"github.com/saswatamcode/artemis/pkg/span"
)

// Jaeger API models - compatible with Grafana's Jaeger datasource

// Trace represents a complete trace with all its spans
type Trace struct {
	TraceID   string             `json:"traceID"`
	Spans     []Span             `json:"spans"`
	Processes map[string]Process `json:"processes"`
}

// Span represents a single span in Jaeger format
type Span struct {
	TraceID       string      `json:"traceID"`
	SpanID        string      `json:"spanID"`
	OperationName string      `json:"operationName"`
	References    []Reference `json:"references"` // Always include, even if empty
	StartTime     int64       `json:"startTime"`  // microseconds since epoch
	Duration      int64       `json:"duration"`   // microseconds
	Tags          []Tag       `json:"tags"`       // Always include, even if empty
	Logs          []Log       `json:"logs"`       // Always include, even if empty
	ProcessID     string      `json:"processID"`
	Warnings      []string    `json:"warnings,omitempty"`
}

// Log represents a span log entry
type Log struct {
	Timestamp int64 `json:"timestamp"` // microseconds since epoch
	Fields    []Tag `json:"fields"`
}

// Reference represents a reference to another span (parent, follows, etc)
type Reference struct {
	RefType string `json:"refType"` // CHILD_OF or FOLLOWS_FROM
	TraceID string `json:"traceID"`
	SpanID  string `json:"spanID"`
}

// Tag represents a span tag/attribute
type Tag struct {
	Key   string `json:"key"`
	Type  string `json:"type"` // string, bool, int64, float64, binary
	Value any    `json:"value"`
}

// Process represents service information
type Process struct {
	ServiceName string `json:"serviceName"`
	Tags        []Tag  `json:"tags"` // Always include, even if empty
}

// TraceQueryResponse is the response for trace search
type TraceQueryResponse struct {
	Data   []Trace `json:"data"`
	Total  int     `json:"total"`
	Limit  int     `json:"limit"`
	Offset int     `json:"offset"`
	Errors []any   `json:"errors,omitempty"`
}

// ServicesResponse is the response for services list
type ServicesResponse struct {
	Data []string `json:"data"`
}

// OperationsResponse is the response for operations list
type OperationsResponse struct {
	Data []string `json:"data"`
}

// convertSpanToJaeger converts internal span to Jaeger format
func convertSpanToJaeger(s *span.Span, processID string) Span {
	// Convert timestamps to microseconds
	startTimeMicros := s.StartTime.UnixNano() / 1000
	durationMicros := s.Duration / 1000

	// Convert tags to Jaeger format, excluding resource attributes and internal fields
	// (resource attributes are in Process.tags, internal fields are in span structure)
	tags := make([]Tag, 0, len(s.Tags))
	for k, v := range s.Tags {
		if isResourceAttribute(k) {
			continue
		}
		if k == "trace_id" || k == "span_id" || k == "parent_span_id" || k == "name" {
			continue
		}

		tag := stringToTypedTag(k, v)
		tags = append(tags, tag)
	}

	references := []Reference{}
	if s.ParentSpanID != "" {
		references = append(references, Reference{
			RefType: "CHILD_OF",
			TraceID: s.TraceID,
			SpanID:  s.ParentSpanID,
		})
	}

	jaegerSpan := Span{
		TraceID:       s.TraceID,
		SpanID:        s.SpanID,
		OperationName: s.Name,
		References:    references,
		StartTime:     startTimeMicros,
		Duration:      durationMicros,
		Tags:          tags,
		Logs:          []Log{}, // Initialize as empty array
		ProcessID:     processID,
	}

	return jaegerSpan
}

// stringToTypedTag converts a string value to a typed Tag based on the string content
func stringToTypedTag(key, value string) Tag {
	// Try bool
	if b, err := strconv.ParseBool(value); err == nil {
		return Tag{Key: key, Type: "bool", Value: b}
	}

	// Try int64
	if i, err := strconv.ParseInt(value, 10, 64); err == nil {
		return Tag{Key: key, Type: "int64", Value: i}
	}

	// Try float64 (only if it contains a decimal point to avoid false positives)
	if strings.Contains(value, ".") {
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return Tag{Key: key, Type: "float64", Value: f}
		}
	}

	// Default to string
	return Tag{Key: key, Type: "string", Value: value}
}

// Resource attribute prefixes - these should go in Process.tags, not Span.tags
var resourceAttributePrefixes = []string{
	"service.",
	"telemetry.sdk.",
	"process.",
	"deployment.",
	"host.",
	"container.",
	"k8s.",
	"cloud.",
}

// isResourceAttribute checks if an attribute key is a resource-level attribute
func isResourceAttribute(key string) bool {
	for _, prefix := range resourceAttributePrefixes {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

// ConvertTraceToJaeger converts a list of spans (all same trace) to Jaeger trace format
func ConvertTraceToJaeger(spans []*span.Span) *Trace {
	if len(spans) == 0 {
		return nil
	}

	traceID := spans[0].TraceID

	processes := make(map[string]Process)
	processIDs := make(map[string]string) // service name -> process ID

	for _, s := range spans {
		if _, exists := processIDs[s.ServiceName]; !exists {
			processID := "p" + strconv.Itoa(len(processes)+1)

			resourceTags := make([]Tag, 0)
			for k, v := range s.Tags {
				if isResourceAttribute(k) {
					resourceTags = append(resourceTags, Tag{
						Key:   k,
						Type:  "string",
						Value: v,
					})
				}
			}

			processes[processID] = Process{
				ServiceName: s.ServiceName,
				Tags:        resourceTags,
			}
			processIDs[s.ServiceName] = processID
		}
	}

	// Convert all spans
	jaegerSpans := make([]Span, len(spans))
	for i, s := range spans {
		processID := processIDs[s.ServiceName]
		jaegerSpans[i] = convertSpanToJaeger(s, processID)
	}

	return &Trace{
		TraceID:   traceID,
		Spans:     jaegerSpans,
		Processes: processes,
	}
}

// TimeRange represents a time range for queries
type TimeRange struct {
	Start time.Time
	End   time.Time
}

// TraceQueryParams represents query parameters for trace search
type TraceQueryParams struct {
	Service      string
	Operation    string
	Tags         map[string]string
	StartTime    time.Time
	EndTime      time.Time
	MinDuration  time.Duration
	MaxDuration  time.Duration
	Limit        int
	LookbackDays int
}

// SQLQueryRequest represents a SQL query request
type SQLQueryRequest struct {
	Query string `json:"query"`
}

// SQLQueryResponse represents a SQL query response
type SQLQueryResponse struct {
	Success  bool                     `json:"success"`
	Columns  []string                 `json:"columns"`
	RowCount int                      `json:"row_count"`
	Traces   []Trace                  `json:"traces,omitempty"` // If query returns full spans
	Rows     []map[string]interface{} `json:"rows,omitempty"`   // If query returns aggregations/projections
	Error    string                   `json:"error,omitempty"`
}
