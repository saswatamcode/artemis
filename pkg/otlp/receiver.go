package otlp

import (
	"context"
	"fmt"
	"maps"
	"time"

	coltracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/tracedb"
)

// Receiver handles OTLP trace ingestion
type Receiver struct {
	db *tracedb.DB
}

// NewReceiver creates a new OTLP receiver
func NewReceiver(db *tracedb.DB) *Receiver {
	return &Receiver{
		db: db,
	}
}

// Export handles the OTLP trace export request
func (r *Receiver) Export(ctx context.Context, req *coltracev1.ExportTraceServiceRequest) (*coltracev1.ExportTraceServiceResponse, error) {
	if req == nil || len(req.ResourceSpans) == 0 {
		return &coltracev1.ExportTraceServiceResponse{}, nil
	}

	// Pre-allocate slice to collect all spans for bulk ingestion
	// Estimate capacity based on typical OTLP batch sizes
	spans := make([]*span.Span, 0, 100)
	for _, resourceSpans := range req.ResourceSpans {
		resourceAttrs := extractAttributes(resourceSpans.Resource)
		scopeAttrs := make(map[string]string)
		for _, scopeSpans := range resourceSpans.ScopeSpans {
			if scopeSpans.Scope != nil {
				scopeAttrs = extractScopeAttributes(scopeSpans.Scope)
			}

			for _, otlpSpan := range scopeSpans.Spans {
				internalSpan := convertOTLPSpan(otlpSpan, resourceAttrs, scopeAttrs)
				spans = append(spans, internalSpan)
			}
		}
	}

	if len(spans) > 0 {
		if err := r.db.WriteSpans(spans); err != nil {
			return nil, fmt.Errorf("failed to write spans: %w", err)
		}
	}

	return &coltracev1.ExportTraceServiceResponse{
		PartialSuccess: &coltracev1.ExportTracePartialSuccess{
			RejectedSpans: 0,
		},
	}, nil
}

// convertOTLPSpan converts an OTLP span to our internal span format
func convertOTLPSpan(otlpSpan *tracev1.Span, resourceAttrs, scopeAttrs map[string]string) *span.Span {
	traceID := fmt.Sprintf("%x", otlpSpan.TraceId)
	spanID := fmt.Sprintf("%x", otlpSpan.SpanId)

	parentSpanID := ""
	if len(otlpSpan.ParentSpanId) > 0 {
		parentSpanID = fmt.Sprintf("%x", otlpSpan.ParentSpanId)
	}

	startTime := time.Unix(0, int64(otlpSpan.StartTimeUnixNano))
	endTime := time.Unix(0, int64(otlpSpan.EndTimeUnixNano))
	duration := int64(otlpSpan.EndTimeUnixNano - otlpSpan.StartTimeUnixNano)

	tags := make(map[string]string)

	maps.Copy(tags, resourceAttrs)

	for k, v := range scopeAttrs {
		tags["scope."+k] = v
	}

	for _, attr := range otlpSpan.Attributes {
		tags[attr.Key] = attributeValueToString(attr.Value)
	}

	tags["span.kind"] = spanKindToString(otlpSpan.Kind)

	if otlpSpan.Status != nil {
		tags["status.code"] = statusCodeToString(otlpSpan.Status.Code)
		if otlpSpan.Status.Message != "" {
			tags["status.message"] = otlpSpan.Status.Message
		}
	}

	serviceName := tags["service.name"]
	if serviceName == "" {
		// Add default service name to tags so queries can find it
		serviceName = "unknown-service"
		tags["service.name"] = serviceName
	}

	// Add some structural fields to tags for efficient indexed queries
	// trace_id and span_id have dedicated indexes, so we don't duplicate them here
	// name and parent_span_id use the tag index for efficient querying
	tags["name"] = otlpSpan.Name
	if parentSpanID != "" {
		tags["parent_span_id"] = parentSpanID
	}

	// Extract span links
	var links []span.SpanLink
	if len(otlpSpan.Links) > 0 {
		links = make([]span.SpanLink, 0, len(otlpSpan.Links))
		for _, otlpLink := range otlpSpan.Links {
			link := span.SpanLink{
				SpanID:        spanID,
				LinkedTraceID: fmt.Sprintf("%x", otlpLink.TraceId),
				LinkedSpanID:  fmt.Sprintf("%x", otlpLink.SpanId),
				Attributes:    make(map[string]string),
			}

			// Extract link attributes
			for _, attr := range otlpLink.Attributes {
				link.Attributes[attr.Key] = attributeValueToString(attr.Value)
			}

			links = append(links, link)
		}
	}

	return &span.Span{
		TraceID:      traceID,
		SpanID:       spanID,
		ParentSpanID: parentSpanID,
		Name:         otlpSpan.Name,
		StartTime:    startTime,
		EndTime:      endTime,
		Duration:     duration,
		ServiceName:  serviceName,
		Tags:         tags,
		Links:        links,
	}
}

// extractScopeAttributes extracts attributes from an instrumentation scope
func extractScopeAttributes(scope *commonv1.InstrumentationScope) map[string]string {
	if scope == nil {
		return make(map[string]string)
	}

	attrs := make(map[string]string)
	if scope.Name != "" {
		attrs["name"] = scope.Name
	}
	if scope.Version != "" {
		attrs["version"] = scope.Version
	}
	for _, attr := range scope.Attributes {
		attrs[attr.Key] = attributeValueToString(attr.Value)
	}
	return attrs
}

// extractAttributes extracts attributes from a resource
func extractAttributes(resource *resourcev1.Resource) map[string]string {
	if resource == nil {
		return make(map[string]string)
	}

	attrs := make(map[string]string)
	for _, attr := range resource.Attributes {
		attrs[attr.Key] = attributeValueToString(attr.Value)
	}
	return attrs
}

// attributeValueToString converts an OTLP attribute value to string
func attributeValueToString(value *commonv1.AnyValue) string {
	if value == nil {
		return ""
	}

	switch v := value.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		return v.StringValue
	case *commonv1.AnyValue_BoolValue:
		if v.BoolValue {
			return "true"
		}
		return "false"
	case *commonv1.AnyValue_IntValue:
		return fmt.Sprintf("%d", v.IntValue)
	case *commonv1.AnyValue_DoubleValue:
		return fmt.Sprintf("%f", v.DoubleValue)
	case *commonv1.AnyValue_ArrayValue:
		return "[array]" // Simplified - could expand arrays
	case *commonv1.AnyValue_KvlistValue:
		return "[map]" // Simplified - could expand maps
	case *commonv1.AnyValue_BytesValue:
		return fmt.Sprintf("[%d bytes]", len(v.BytesValue))
	default:
		return ""
	}
}

// spanKindToString converts OTLP span kind to string
func spanKindToString(kind tracev1.Span_SpanKind) string {
	switch kind {
	case tracev1.Span_SPAN_KIND_INTERNAL:
		return "internal"
	case tracev1.Span_SPAN_KIND_SERVER:
		return "server"
	case tracev1.Span_SPAN_KIND_CLIENT:
		return "client"
	case tracev1.Span_SPAN_KIND_PRODUCER:
		return "producer"
	case tracev1.Span_SPAN_KIND_CONSUMER:
		return "consumer"
	default:
		return "unspecified"
	}
}

// statusCodeToString converts OTLP status code to string
func statusCodeToString(code tracev1.Status_StatusCode) string {
	switch code {
	case tracev1.Status_STATUS_CODE_UNSET:
		return "unset"
	case tracev1.Status_STATUS_CODE_OK:
		return "ok"
	case tracev1.Status_STATUS_CODE_ERROR:
		return "error"
	default:
		return "unknown"
	}
}
