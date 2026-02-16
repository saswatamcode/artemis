package span

import (
	"testing"
	"time"
)

func TestSpan_MarshalUnmarshal(t *testing.T) {
	original := &Span{
		TraceID:      "0123456789abcdef0123456789abcdef", // 32 hex chars (128-bit)
		SpanID:       "fedcba9876543210",                 // 16 hex chars (64-bit)
		ParentSpanID: "1111222233334444",                 // 16 hex chars (64-bit)
		Name:         "test-span",
		ServiceName:  "test-service",
		StartTime:    time.Unix(0, 1000000000),
		EndTime:      time.Unix(0, 2000000000),
		Duration:     1000000000,
		Tags: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}

	// Marshal
	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}

	// Unmarshal
	var decoded Span
	if err := decoded.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary() error = %v", err)
	}

	// Compare
	if decoded.TraceID != original.TraceID {
		t.Errorf("TraceID mismatch: got %q, want %q", decoded.TraceID, original.TraceID)
	}
	if decoded.SpanID != original.SpanID {
		t.Errorf("SpanID mismatch: got %q, want %q", decoded.SpanID, original.SpanID)
	}
	if decoded.ParentSpanID != original.ParentSpanID {
		t.Errorf("ParentSpanID mismatch: got %q, want %q", decoded.ParentSpanID, original.ParentSpanID)
	}
	if decoded.Name != original.Name {
		t.Errorf("Name mismatch: got %q, want %q", decoded.Name, original.Name)
	}
	if decoded.ServiceName != original.ServiceName {
		t.Errorf("ServiceName mismatch: got %q, want %q", decoded.ServiceName, original.ServiceName)
	}
	if !decoded.StartTime.Equal(original.StartTime) {
		t.Errorf("StartTime mismatch: got %v, want %v", decoded.StartTime, original.StartTime)
	}
	if !decoded.EndTime.Equal(original.EndTime) {
		t.Errorf("EndTime mismatch: got %v, want %v", decoded.EndTime, original.EndTime)
	}
	if decoded.Duration != original.Duration {
		t.Errorf("Duration mismatch: got %d, want %d", decoded.Duration, original.Duration)
	}
	if len(decoded.Tags) != len(original.Tags) {
		t.Errorf("Tags length mismatch: got %d, want %d", len(decoded.Tags), len(original.Tags))
	}
	for k, v := range original.Tags {
		if decoded.Tags[k] != v {
			t.Errorf("Tag %q mismatch: got %q, want %q", k, decoded.Tags[k], v)
		}
	}
}

func TestSpan_RootSpan(t *testing.T) {
	original := &Span{
		TraceID:      "0123456789abcdef0123456789abcdef",
		SpanID:       "fedcba9876543210",
		ParentSpanID: "", // Root span - no parent
		Name:         "root-span",
		ServiceName:  "test-service",
		StartTime:    time.Unix(0, 1000000000),
		EndTime:      time.Unix(0, 2000000000),
		Duration:     1000000000,
		Tags:         map[string]string{},
	}

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}

	var decoded Span
	if err := decoded.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary() error = %v", err)
	}

	if decoded.ParentSpanID != "" {
		t.Errorf("ParentSpanID should be empty for root span, got %q", decoded.ParentSpanID)
	}
}

func TestSpanEvent_MarshalUnmarshal(t *testing.T) {
	original := &SpanEvent{
		SpanID:    "fedcba9876543210",
		Name:      "test-event",
		Timestamp: time.Unix(0, 1500000000),
		Attributes: map[string]string{
			"attr1": "val1",
			"attr2": "val2",
		},
	}

	// Marshal
	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}

	// Unmarshal
	var decoded SpanEvent
	if err := decoded.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary() error = %v", err)
	}

	// Compare
	if decoded.SpanID != original.SpanID {
		t.Errorf("SpanID mismatch: got %q, want %q", decoded.SpanID, original.SpanID)
	}
	if decoded.Name != original.Name {
		t.Errorf("Name mismatch: got %q, want %q", decoded.Name, original.Name)
	}
	if !decoded.Timestamp.Equal(original.Timestamp) {
		t.Errorf("Timestamp mismatch: got %v, want %v", decoded.Timestamp, original.Timestamp)
	}
	if len(decoded.Attributes) != len(original.Attributes) {
		t.Errorf("Attributes length mismatch: got %d, want %d", len(decoded.Attributes), len(original.Attributes))
	}
	for k, v := range original.Attributes {
		if decoded.Attributes[k] != v {
			t.Errorf("Attribute %q mismatch: got %q, want %q", k, decoded.Attributes[k], v)
		}
	}
}

func TestSpanLink_MarshalUnmarshal(t *testing.T) {
	original := &SpanLink{
		SpanID:        "fedcba9876543210",
		LinkedTraceID: "0123456789abcdef0123456789abcdef",
		LinkedSpanID:  "1111222233334444",
		Attributes: map[string]string{
			"link.type": "async",
			"link.name": "background-job",
		},
	}

	// Marshal
	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}

	// Unmarshal
	var decoded SpanLink
	if err := decoded.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary() error = %v", err)
	}

	// Compare
	if decoded.SpanID != original.SpanID {
		t.Errorf("SpanID mismatch: got %q, want %q", decoded.SpanID, original.SpanID)
	}
	if decoded.LinkedTraceID != original.LinkedTraceID {
		t.Errorf("LinkedTraceID mismatch: got %q, want %q", decoded.LinkedTraceID, original.LinkedTraceID)
	}
	if decoded.LinkedSpanID != original.LinkedSpanID {
		t.Errorf("LinkedSpanID mismatch: got %q, want %q", decoded.LinkedSpanID, original.LinkedSpanID)
	}
	if len(decoded.Attributes) != len(original.Attributes) {
		t.Errorf("Attributes length mismatch: got %d, want %d", len(decoded.Attributes), len(original.Attributes))
	}
	for k, v := range original.Attributes {
		if decoded.Attributes[k] != v {
			t.Errorf("Attribute %q mismatch: got %q, want %q", k, decoded.Attributes[k], v)
		}
	}
}

func TestSpan_EmptyFields(t *testing.T) {
	original := &Span{
		TraceID:      "0123456789abcdef0123456789abcdef",
		SpanID:       "fedcba9876543210",
		ParentSpanID: "",
		Name:         "",
		ServiceName:  "",
		StartTime:    time.Unix(0, 0),
		EndTime:      time.Unix(0, 0),
		Duration:     0,
		Tags:         map[string]string{},
	}

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}

	var decoded Span
	if err := decoded.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary() error = %v", err)
	}

	if decoded.Name != "" {
		t.Errorf("Name should be empty, got %q", decoded.Name)
	}
	if decoded.ServiceName != "" {
		t.Errorf("ServiceName should be empty, got %q", decoded.ServiceName)
	}
	if len(decoded.Tags) != 0 {
		t.Errorf("Tags should be empty, got %d entries", len(decoded.Tags))
	}
}

func TestSpan_DeterministicEncoding(t *testing.T) {
	// Test that encoding is deterministic - same input produces same output
	original := &Span{
		TraceID:      "0123456789abcdef0123456789abcdef",
		SpanID:       "fedcba9876543210",
		ParentSpanID: "1111222233334444",
		Name:         "test-span",
		ServiceName:  "test-service",
		StartTime:    time.Unix(0, 1000000000),
		EndTime:      time.Unix(0, 2000000000),
		Duration:     1000000000,
		Tags: map[string]string{
			"zebra":  "last",
			"alpha":  "first",
			"middle": "center",
			"beta":   "second",
		},
	}

	// Encode multiple times
	data1, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() #1 error = %v", err)
	}

	data2, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() #2 error = %v", err)
	}

	data3, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() #3 error = %v", err)
	}

	// All encodings should be identical
	if len(data1) != len(data2) || len(data1) != len(data3) {
		t.Errorf("Encoded lengths differ: %d, %d, %d", len(data1), len(data2), len(data3))
	}

	for i := range data1 {
		if data1[i] != data2[i] {
			t.Errorf("Byte %d differs between encoding #1 and #2: %x vs %x", i, data1[i], data2[i])
			break
		}
		if data1[i] != data3[i] {
			t.Errorf("Byte %d differs between encoding #1 and #3: %x vs %x", i, data1[i], data3[i])
			break
		}
	}
}

func TestSpanEvent_DeterministicEncoding(t *testing.T) {
	original := &SpanEvent{
		SpanID:    "fedcba9876543210",
		Name:      "test-event",
		Timestamp: time.Unix(0, 1500000000),
		Attributes: map[string]string{
			"z_attr": "last",
			"a_attr": "first",
			"m_attr": "middle",
		},
	}

	data1, _ := original.MarshalBinary()
	data2, _ := original.MarshalBinary()

	if len(data1) != len(data2) {
		t.Errorf("Encoded lengths differ: %d vs %d", len(data1), len(data2))
	}

	for i := range data1 {
		if data1[i] != data2[i] {
			t.Errorf("Byte %d differs: %x vs %x", i, data1[i], data2[i])
			break
		}
	}
}

func TestSpanLink_DeterministicEncoding(t *testing.T) {
	original := &SpanLink{
		SpanID:        "fedcba9876543210",
		LinkedTraceID: "0123456789abcdef0123456789abcdef",
		LinkedSpanID:  "1111222233334444",
		Attributes: map[string]string{
			"link_z": "last",
			"link_a": "first",
		},
	}

	data1, _ := original.MarshalBinary()
	data2, _ := original.MarshalBinary()

	if len(data1) != len(data2) {
		t.Errorf("Encoded lengths differ: %d vs %d", len(data1), len(data2))
	}

	for i := range data1 {
		if data1[i] != data2[i] {
			t.Errorf("Byte %d differs: %x vs %x", i, data1[i], data2[i])
			break
		}
	}
}
