package tempo

import (
	"time"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/saswatamcode/artemis/pkg/span"
)

// SearchResponse is the response for trace search
type SearchResponse struct {
	Traces  []TraceSearchMetadata `json:"traces"`
	Metrics SearchMetrics         `json:"metrics"`
}

// TraceSearchMetadata represents trace metadata in search results
type TraceSearchMetadata struct {
	TraceID           string                 `json:"traceID"`
	RootServiceName   string                 `json:"rootServiceName"`
	RootTraceName     string                 `json:"rootTraceName"`
	StartTimeUnixNano uint64                 `json:"startTimeUnixNano"`
	DurationMs        uint64                 `json:"durationMs"`
	SpanSets          []SpanSet              `json:"spanSets,omitempty"`
	ServiceStats      map[string]ServiceStat `json:"serviceStats,omitempty"`
}

// SpanSet represents a set of matching spans
type SpanSet struct {
	Spans   []SpanMetadata `json:"spans"`
	Matched int            `json:"matched"`
}

// SpanMetadata represents span metadata
type SpanMetadata struct {
	SpanID            string            `json:"spanID"`
	Name              string            `json:"name"`
	StartTimeUnixNano uint64            `json:"startTimeUnixNano"`
	DurationNanos     uint64            `json:"durationNanos"`
	Attributes        map[string]string `json:"attributes,omitempty"`
}

// ServiceStat represents service statistics
type ServiceStat struct {
	SpanCount  int `json:"spanCount"`
	ErrorCount int `json:"errorCount"`
}

// SearchMetrics represents search metrics
type SearchMetrics struct {
	InspectedTraces int `json:"inspectedTraces"`
	InspectedBlocks int `json:"inspectedBlocks"`
	InspectedBytes  int `json:"inspectedBytes"`
	TotalBlocks     int `json:"totalBlocks"`
}

// TagsResponse is the response for tag names
type TagsResponse struct {
	TagNames []string `json:"tagNames"`
}

// TagValuesResponse is the response for tag values
type TagValuesResponse struct {
	TagValues []TagValue `json:"tagValues"`
}

// TagValue represents a tag value
type TagValue struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// ConvertSpansToOTLP converts internal spans to OTLP ResourceSpans (one per service)
func ConvertSpansToOTLP(spans []*span.Span) []*tracev1.ResourceSpans {
	if len(spans) == 0 {
		return nil
	}

	serviceSpans := make(map[string][]*span.Span)
	for _, s := range spans {
		serviceSpans[s.ServiceName] = append(serviceSpans[s.ServiceName], s)
	}

	resourceSpansList := make([]*tracev1.ResourceSpans, 0, len(serviceSpans))
	for _, svcSpans := range serviceSpans {
		if len(svcSpans) == 0 {
			continue
		}

		resourceAttrs := extractResourceAttributes(svcSpans[0])
		otlpSpans := make([]*tracev1.Span, 0, len(svcSpans))
		for _, s := range svcSpans {
			otlpSpans = append(otlpSpans, convertSpanToOTLP(s))
		}

		resourceSpansList = append(resourceSpansList, &tracev1.ResourceSpans{
			Resource: &resourcev1.Resource{
				Attributes: resourceAttrs,
			},
			ScopeSpans: []*tracev1.ScopeSpans{
				{
					Scope: &commonv1.InstrumentationScope{
						Name:    "artemis",
						Version: "1.0.0",
					},
					Spans: otlpSpans,
				},
			},
		})
	}

	return resourceSpansList
}

// extractResourceAttributes extracts resource-level attributes from span tags
func extractResourceAttributes(s *span.Span) []*commonv1.KeyValue {
	attrs := make([]*commonv1.KeyValue, 0)

	resourcePrefixes := []string{
		"service.", "telemetry.sdk.", "process.", "deployment.",
		"host.", "container.", "k8s.", "cloud.",
	}

	for k, v := range s.Tags {
		for _, prefix := range resourcePrefixes {
			if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
				attrs = append(attrs, &commonv1.KeyValue{
					Key:   k,
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}},
				})
				break
			}
		}
	}

	return attrs
}

// convertSpanToOTLP converts an internal span to OTLP format
func convertSpanToOTLP(s *span.Span) *tracev1.Span {
	traceID := hexToBytes(s.TraceID)
	spanID := hexToBytes(s.SpanID)
	parentSpanID := hexToBytes(s.ParentSpanID)

	attrs := make([]*commonv1.KeyValue, 0)
	resourcePrefixes := []string{
		"service.", "telemetry.sdk.", "process.", "deployment.",
		"host.", "container.", "k8s.", "cloud.",
		"trace_id", "span_id", "parent_span_id", "name",
	}

	for k, v := range s.Tags {
		isResource := false
		for _, prefix := range resourcePrefixes {
			if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
				isResource = true
				break
			}
		}
		if !isResource {
			attrs = append(attrs, &commonv1.KeyValue{
				Key:   k,
				Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}},
			})
		}
	}

	spanKind := tracev1.Span_SPAN_KIND_INTERNAL
	if kindStr, ok := s.Tags["span.kind"]; ok {
		switch kindStr {
		case "server":
			spanKind = tracev1.Span_SPAN_KIND_SERVER
		case "client":
			spanKind = tracev1.Span_SPAN_KIND_CLIENT
		case "producer":
			spanKind = tracev1.Span_SPAN_KIND_PRODUCER
		case "consumer":
			spanKind = tracev1.Span_SPAN_KIND_CONSUMER
		}
	}

	// Convert span events to OTLP events
	var otlpEvents []*tracev1.Span_Event
	if len(s.Events) > 0 {
		otlpEvents = make([]*tracev1.Span_Event, 0, len(s.Events))
		for _, evt := range s.Events {
			eventAttrs := make([]*commonv1.KeyValue, 0, len(evt.Attributes))
			for k, v := range evt.Attributes {
				eventAttrs = append(eventAttrs, &commonv1.KeyValue{
					Key:   k,
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}},
				})
			}

			otlpEvents = append(otlpEvents, &tracev1.Span_Event{
				TimeUnixNano: uint64(evt.Timestamp.UnixNano()),
				Name:         evt.Name,
				Attributes:   eventAttrs,
			})
		}
	}

	// Convert span links to OTLP links
	var otlpLinks []*tracev1.Span_Link
	if len(s.Links) > 0 {
		otlpLinks = make([]*tracev1.Span_Link, 0, len(s.Links))
		for _, link := range s.Links {
			linkAttrs := make([]*commonv1.KeyValue, 0, len(link.Attributes))
			for k, v := range link.Attributes {
				linkAttrs = append(linkAttrs, &commonv1.KeyValue{
					Key:   k,
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}},
				})
			}

			otlpLinks = append(otlpLinks, &tracev1.Span_Link{
				TraceId:    hexToBytes(link.LinkedTraceID),
				SpanId:     hexToBytes(link.LinkedSpanID),
				Attributes: linkAttrs,
			})
		}
	}

	return &tracev1.Span{
		TraceId:           traceID,
		SpanId:            spanID,
		ParentSpanId:      parentSpanID,
		Name:              s.Name,
		Kind:              spanKind,
		StartTimeUnixNano: uint64(s.StartTime.UnixNano()),
		EndTimeUnixNano:   uint64(s.EndTime.UnixNano()),
		Attributes:        attrs,
		Events:            otlpEvents,
		Links:             otlpLinks,
		Status: &tracev1.Status{
			Code: tracev1.Status_STATUS_CODE_UNSET,
		},
	}
}

// hexToBytes converts hex string to bytes
func hexToBytes(hexStr string) []byte {
	if hexStr == "" {
		return nil
	}

	b := make([]byte, len(hexStr)/2)
	for i := 0; i < len(hexStr); i += 2 {
		b[i/2] = hexCharToByte(hexStr[i])<<4 | hexCharToByte(hexStr[i+1])
	}
	return b
}

func hexCharToByte(c byte) byte {
	if c >= '0' && c <= '9' {
		return c - '0'
	}
	if c >= 'a' && c <= 'f' {
		return c - 'a' + 10
	}
	if c >= 'A' && c <= 'F' {
		return c - 'A' + 10
	}
	return 0
}

// ConvertSpansToSearchMetadata converts spans to trace search metadata
func ConvertSpansToSearchMetadata(traceSpans map[string][]*span.Span) []TraceSearchMetadata {
	results := make([]TraceSearchMetadata, 0, len(traceSpans))

	for traceID, spans := range traceSpans {
		if len(spans) == 0 {
			continue
		}

		var rootSpan *span.Span
		minStartTime := time.Now()
		maxEndTime := time.Time{}

		serviceStats := make(map[string]*ServiceStat)

		for _, s := range spans {
			if s.ParentSpanID == "" {
				rootSpan = s
			}
			if s.StartTime.Before(minStartTime) {
				minStartTime = s.StartTime
			}
			if s.EndTime.After(maxEndTime) {
				maxEndTime = s.EndTime
			}

			if _, exists := serviceStats[s.ServiceName]; !exists {
				serviceStats[s.ServiceName] = &ServiceStat{}
			}
			serviceStats[s.ServiceName].SpanCount++

			if statusCode, ok := s.Tags["status.code"]; ok && statusCode == "error" {
				serviceStats[s.ServiceName].ErrorCount++
			}
		}

		rootServiceName := "unknown"
		rootTraceName := "unknown"
		if rootSpan != nil {
			rootServiceName = rootSpan.ServiceName
			rootTraceName = rootSpan.Name
		} else if len(spans) > 0 {
			// Use first span if no root found
			rootServiceName = spans[0].ServiceName
			rootTraceName = spans[0].Name
		}

		duration := maxEndTime.Sub(minStartTime)

		svcStats := make(map[string]ServiceStat)
		for svc, stats := range serviceStats {
			svcStats[svc] = *stats
		}

		results = append(results, TraceSearchMetadata{
			TraceID:           traceID,
			RootServiceName:   rootServiceName,
			RootTraceName:     rootTraceName,
			StartTimeUnixNano: uint64(minStartTime.UnixNano()),
			DurationMs:        uint64(duration.Milliseconds()),
			ServiceStats:      svcStats,
		})
	}

	return results
}
