package otlp

import (
	"context"
	"testing"
	"time"

	coltracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/saswatamcode/artemis/pkg/tracedb"
)

func TestNewReceiver(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &tracedb.Config{
		WALDir:          tmpDir + "/wal",
		CompactInterval: 0, // Disable background compaction for tests
	}
	db, err := tracedb.New(cfg)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	receiver := NewReceiver(db)
	if receiver == nil {
		t.Fatal("NewReceiver() should not return nil")
	}

	if receiver.db == nil {
		t.Error("Receiver db should not be nil")
	}
}

func TestReceiver_Export_Empty(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &tracedb.Config{
		WALDir:          tmpDir + "/wal",
		CompactInterval: 0, // Disable background compaction for tests
	}
	db, err := tracedb.New(cfg)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	receiver := NewReceiver(db)

	// Test with nil request
	resp, err := receiver.Export(context.Background(), nil)
	if err != nil {
		t.Errorf("Export(nil) should not error, got %v", err)
	}
	if resp == nil {
		t.Error("Response should not be nil")
	}

	// Test with empty request
	resp, err = receiver.Export(context.Background(), &coltracev1.ExportTraceServiceRequest{})
	if err != nil {
		t.Errorf("Export(empty) should not error, got %v", err)
	}
	if resp == nil {
		t.Error("Response should not be nil")
	}
}

func TestReceiver_Export_Success(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &tracedb.Config{
		WALDir:          tmpDir + "/wal",
		CompactInterval: 0, // Disable background compaction for tests
	}
	db, err := tracedb.New(cfg)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	receiver := NewReceiver(db)

	// Create OTLP request with a span
	req := &coltracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{
							Key:   "service.name",
							Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "test-service"}},
						},
					},
				},
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Scope: &commonv1.InstrumentationScope{
							Name:    "test-scope",
							Version: "1.0.0",
						},
						Spans: []*tracev1.Span{
							{
								TraceId:           []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
								SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
								ParentSpanId:      []byte{0, 0, 0, 0, 0, 0, 0, 1},
								Name:              "test-operation",
								Kind:              tracev1.Span_SPAN_KIND_SERVER,
								StartTimeUnixNano: uint64(time.Now().UnixNano()),
								EndTimeUnixNano:   uint64(time.Now().Add(time.Millisecond).UnixNano()),
								Attributes: []*commonv1.KeyValue{
									{
										Key:   "http.method",
										Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "GET"}},
									},
									{
										Key:   "http.status_code",
										Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 200}},
									},
								},
								Status: &tracev1.Status{
									Code:    tracev1.Status_STATUS_CODE_OK,
									Message: "success",
								},
							},
						},
					},
				},
			},
		},
	}

	resp, err := receiver.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}

	if resp.PartialSuccess.RejectedSpans != 0 {
		t.Errorf("RejectedSpans = %d, want 0", resp.PartialSuccess.RejectedSpans)
	}
}

func TestConvertOTLPSpan(t *testing.T) {
	now := time.Now()
	otlpSpan := &tracev1.Span{
		TraceId:           []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
		ParentSpanId:      []byte{0, 0, 0, 0, 0, 0, 0, 1},
		Name:              "test-operation",
		Kind:              tracev1.Span_SPAN_KIND_CLIENT,
		StartTimeUnixNano: uint64(now.UnixNano()),
		EndTimeUnixNano:   uint64(now.Add(time.Millisecond).UnixNano()),
		Attributes: []*commonv1.KeyValue{
			{
				Key:   "test.key",
				Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "test-value"}},
			},
		},
		Status: &tracev1.Status{
			Code:    tracev1.Status_STATUS_CODE_ERROR,
			Message: "test error",
		},
	}

	resourceAttrs := map[string]string{"service.name": "test-service"}
	scopeAttrs := map[string]string{"scope.name": "test-scope"}

	internalSpan := convertOTLPSpan(otlpSpan, resourceAttrs, scopeAttrs)

	// Verify trace ID
	expectedTraceID := "0102030405060708090a0b0c0d0e0f10"
	if internalSpan.TraceID != expectedTraceID {
		t.Errorf("TraceID = %s, want %s", internalSpan.TraceID, expectedTraceID)
	}

	// Verify span ID
	expectedSpanID := "0102030405060708"
	if internalSpan.SpanID != expectedSpanID {
		t.Errorf("SpanID = %s, want %s", internalSpan.SpanID, expectedSpanID)
	}

	// Verify parent span ID
	expectedParentSpanID := "0000000000000001"
	if internalSpan.ParentSpanID != expectedParentSpanID {
		t.Errorf("ParentSpanID = %s, want %s", internalSpan.ParentSpanID, expectedParentSpanID)
	}

	// Verify name
	if internalSpan.Name != "test-operation" {
		t.Errorf("Name = %s, want test-operation", internalSpan.Name)
	}

	// Verify service name
	if internalSpan.ServiceName != "test-service" {
		t.Errorf("ServiceName = %s, want test-service", internalSpan.ServiceName)
	}

	// Verify tags contain resource, scope, and span attributes
	if internalSpan.Tags["service.name"] != "test-service" {
		t.Error("Tags should contain resource attributes")
	}
	if internalSpan.Tags["scope.scope.name"] != "test-scope" {
		t.Error("Tags should contain scope attributes with scope. prefix")
	}
	if internalSpan.Tags["test.key"] != "test-value" {
		t.Error("Tags should contain span attributes")
	}

	// Verify span kind tag
	if internalSpan.Tags["span.kind"] != "client" {
		t.Errorf("span.kind = %s, want client", internalSpan.Tags["span.kind"])
	}

	// Verify status tags
	if internalSpan.Tags["status.code"] != "error" {
		t.Errorf("status.code = %s, want error", internalSpan.Tags["status.code"])
	}
	if internalSpan.Tags["status.message"] != "test error" {
		t.Errorf("status.message = %s, want test error", internalSpan.Tags["status.message"])
	}
}

func TestConvertOTLPSpan_NoParent(t *testing.T) {
	otlpSpan := &tracev1.Span{
		TraceId:           []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
		ParentSpanId:      nil, // No parent
		Name:              "root-span",
		StartTimeUnixNano: uint64(time.Now().UnixNano()),
		EndTimeUnixNano:   uint64(time.Now().Add(time.Millisecond).UnixNano()),
	}

	internalSpan := convertOTLPSpan(otlpSpan, make(map[string]string), make(map[string]string))

	if internalSpan.ParentSpanID != "" {
		t.Errorf("ParentSpanID = %s, want empty string", internalSpan.ParentSpanID)
	}

	// Verify service name defaults to unknown-service
	if internalSpan.ServiceName != "unknown-service" {
		t.Errorf("ServiceName = %s, want unknown-service", internalSpan.ServiceName)
	}
}

func TestExtractAttributes(t *testing.T) {
	resource := &resourcev1.Resource{
		Attributes: []*commonv1.KeyValue{
			{
				Key:   "service.name",
				Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "my-service"}},
			},
			{
				Key:   "service.version",
				Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "1.0.0"}},
			},
		},
	}

	attrs := extractAttributes(resource)

	if len(attrs) != 2 {
		t.Errorf("extractAttributes() returned %d attributes, want 2", len(attrs))
	}

	if attrs["service.name"] != "my-service" {
		t.Errorf("service.name = %s, want my-service", attrs["service.name"])
	}

	if attrs["service.version"] != "1.0.0" {
		t.Errorf("service.version = %s, want 1.0.0", attrs["service.version"])
	}

	// Test with nil resource
	attrs = extractAttributes(nil)
	if len(attrs) != 0 {
		t.Errorf("extractAttributes(nil) should return empty map, got %d attrs", len(attrs))
	}
}

func TestExtractScopeAttributes(t *testing.T) {
	scope := &commonv1.InstrumentationScope{
		Name:    "my-instrumentation",
		Version: "2.0.0",
		Attributes: []*commonv1.KeyValue{
			{
				Key:   "custom.key",
				Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "custom-value"}},
			},
		},
	}

	attrs := extractScopeAttributes(scope)

	if len(attrs) != 3 {
		t.Errorf("extractScopeAttributes() returned %d attributes, want 3", len(attrs))
	}

	if attrs["name"] != "my-instrumentation" {
		t.Errorf("name = %s, want my-instrumentation", attrs["name"])
	}

	if attrs["version"] != "2.0.0" {
		t.Errorf("version = %s, want 2.0.0", attrs["version"])
	}

	if attrs["custom.key"] != "custom-value" {
		t.Errorf("custom.key = %s, want custom-value", attrs["custom.key"])
	}

	// Test with nil scope
	attrs = extractScopeAttributes(nil)
	if len(attrs) != 0 {
		t.Errorf("extractScopeAttributes(nil) should return empty map, got %d attrs", len(attrs))
	}
}

func TestAttributeValueToString(t *testing.T) {
	tests := []struct {
		name  string
		value *commonv1.AnyValue
		want  string
	}{
		{
			"string value",
			&commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "test"}},
			"test",
		},
		{
			"bool true",
			&commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: true}},
			"true",
		},
		{
			"bool false",
			&commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: false}},
			"false",
		},
		{
			"int value",
			&commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 42}},
			"42",
		},
		{
			"double value",
			&commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: 3.14}},
			"3.140000",
		},
		{
			"array value",
			&commonv1.AnyValue{Value: &commonv1.AnyValue_ArrayValue{}},
			"[array]",
		},
		{
			"map value",
			&commonv1.AnyValue{Value: &commonv1.AnyValue_KvlistValue{}},
			"[map]",
		},
		{
			"bytes value",
			&commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: []byte{1, 2, 3}}},
			"[3 bytes]",
		},
		{
			"nil value",
			nil,
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := attributeValueToString(tt.value)
			if got != tt.want {
				t.Errorf("attributeValueToString() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestSpanKindToString(t *testing.T) {
	tests := []struct {
		kind tracev1.Span_SpanKind
		want string
	}{
		{tracev1.Span_SPAN_KIND_UNSPECIFIED, "unspecified"},
		{tracev1.Span_SPAN_KIND_INTERNAL, "internal"},
		{tracev1.Span_SPAN_KIND_SERVER, "server"},
		{tracev1.Span_SPAN_KIND_CLIENT, "client"},
		{tracev1.Span_SPAN_KIND_PRODUCER, "producer"},
		{tracev1.Span_SPAN_KIND_CONSUMER, "consumer"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := spanKindToString(tt.kind)
			if got != tt.want {
				t.Errorf("spanKindToString() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestStatusCodeToString(t *testing.T) {
	tests := []struct {
		code tracev1.Status_StatusCode
		want string
	}{
		{tracev1.Status_STATUS_CODE_UNSET, "unset"},
		{tracev1.Status_STATUS_CODE_OK, "ok"},
		{tracev1.Status_STATUS_CODE_ERROR, "error"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := statusCodeToString(tt.code)
			if got != tt.want {
				t.Errorf("statusCodeToString() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestNewServer(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &tracedb.Config{
		WALDir:          tmpDir + "/wal",
		CompactInterval: 0, // Disable background compaction for tests
	}
	db, err := tracedb.New(cfg)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Use port 0 to get a random free port
	server, err := NewServer(db, "localhost:0", nil, nil, nil)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	defer server.Stop()

	if server == nil {
		t.Fatal("NewServer() should not return nil")
	}

	if server.receiver == nil {
		t.Error("Server receiver should not be nil")
	}

	if server.grpcServer == nil {
		t.Error("Server grpcServer should not be nil")
	}

	if server.listener == nil {
		t.Error("Server listener should not be nil")
	}

	if server.logger == nil {
		t.Error("Server logger should not be nil")
	}

	// Verify address
	addr := server.Addr()
	if addr == "" {
		t.Error("Server address should not be empty")
	}
}

func TestServer_Export(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &tracedb.Config{
		WALDir:          tmpDir + "/wal",
		CompactInterval: 0, // Disable background compaction for tests
	}
	db, err := tracedb.New(cfg)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	server, err := NewServer(db, "localhost:0", nil, nil, nil)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	defer server.Stop()

	// Test Export through server
	req := &coltracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{
							Key:   "service.name",
							Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "test-service"}},
						},
					},
				},
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Spans: []*tracev1.Span{
							{
								TraceId:           []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
								SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
								Name:              "test-span",
								StartTimeUnixNano: uint64(time.Now().UnixNano()),
								EndTimeUnixNano:   uint64(time.Now().Add(time.Millisecond).UnixNano()),
							},
						},
					},
				},
			},
		},
	}

	resp, err := server.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("Server.Export() error = %v", err)
	}

	if resp == nil {
		t.Error("Response should not be nil")
	}

	if resp.PartialSuccess.RejectedSpans != 0 {
		t.Errorf("RejectedSpans = %d, want 0", resp.PartialSuccess.RejectedSpans)
	}
}
