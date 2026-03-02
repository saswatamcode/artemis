package span

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"time"
)

// MarshalBinary encodes a span into binary format following the schema:
// u64be    trace_id_hi
// u64be    trace_id_lo
// u64be    span_id
// u64be    parent_span_id   // 0 if root
// uvarint  name_len
// bytes    name
// uvarint  service_len
// bytes    service
// svarint  start_unix_nano
// svarint  end_unix_nano
// svarint  duration_nano
// uvarint  tags_count
// repeat:
//
//	uvarint key_len
//	bytes   key
//	uvarint val_len
//	bytes   val
func (s *Span) MarshalBinary() ([]byte, error) {
	// Estimate buffer size to reduce allocations
	estimatedSize := 8*4 + // Fixed u64 fields
		10*5 + // Max varint sizes
		len(s.Name) + len(s.ServiceName) +
		len(s.Tags)*20 // Rough estimate for tags

	buf := make([]byte, 0, estimatedSize)

	// Parse trace ID (assume 128-bit hex string, split into hi/lo)
	traceIDHi, traceIDLo, err := ParseTraceID(s.TraceID)
	if err != nil {
		return nil, fmt.Errorf("invalid trace ID: %w", err)
	}

	// Parse span ID (64-bit hex string)
	spanID, err := parseSpanID(s.SpanID)
	if err != nil {
		return nil, fmt.Errorf("invalid span ID: %w", err)
	}

	// Parse parent span ID (64-bit hex string, 0 if empty)
	parentSpanID := uint64(0)
	if s.ParentSpanID != "" {
		parentSpanID, err = parseSpanID(s.ParentSpanID)
		if err != nil {
			return nil, fmt.Errorf("invalid parent span ID: %w", err)
		}
	}

	// Write fixed u64 fields (big-endian)
	buf = appendUint64BE(buf, traceIDHi)
	buf = appendUint64BE(buf, traceIDLo)
	buf = appendUint64BE(buf, spanID)
	buf = appendUint64BE(buf, parentSpanID)

	// Write name
	buf = appendString(buf, s.Name)

	// Write service
	buf = appendString(buf, s.ServiceName)

	// Write timestamps and duration as svarint
	buf = appendSvarint(buf, s.StartTime.UnixNano())
	buf = appendSvarint(buf, s.EndTime.UnixNano())
	buf = appendSvarint(buf, s.GetDuration())

	// Write tags count
	buf = appendUvarint(buf, uint64(len(s.Tags)))

	// Write tags (sorted by key for deterministic encoding)
	if len(s.Tags) > 0 {
		keys := make([]string, 0, len(s.Tags))
		for k := range s.Tags {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, key := range keys {
			buf = appendString(buf, key)
			buf = appendString(buf, s.Tags[key])
		}
	}

	return buf, nil
}

// UnmarshalBinary decodes a span from binary format
func (s *Span) UnmarshalBinary(data []byte) error {
	offset := 0

	// Read fixed u64 fields (big-endian)
	if len(data) < 32 {
		return fmt.Errorf("data too short for header")
	}

	traceIDHi := binary.BigEndian.Uint64(data[offset:])
	offset += 8
	traceIDLo := binary.BigEndian.Uint64(data[offset:])
	offset += 8
	spanID := binary.BigEndian.Uint64(data[offset:])
	offset += 8
	parentSpanID := binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// Format trace ID as hex string
	s.TraceID = fmt.Sprintf("%016x%016x", traceIDHi, traceIDLo)
	s.SpanID = fmt.Sprintf("%016x", spanID)
	if parentSpanID != 0 {
		s.ParentSpanID = fmt.Sprintf("%016x", parentSpanID)
	} else {
		s.ParentSpanID = ""
	}

	// Read name
	var err error
	s.Name, offset, err = readString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to read name: %w", err)
	}

	// Read service
	s.ServiceName, offset, err = readString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to read service: %w", err)
	}

	// Read timestamps and duration
	var startNano, endNano, duration int64
	startNano, offset, err = readSvarint(data, offset)
	if err != nil {
		return fmt.Errorf("failed to read start time: %w", err)
	}

	endNano, offset, err = readSvarint(data, offset)
	if err != nil {
		return fmt.Errorf("failed to read end time: %w", err)
	}

	duration, offset, err = readSvarint(data, offset)
	if err != nil {
		return fmt.Errorf("failed to read duration: %w", err)
	}

	s.StartTime = unixNanoToTime(startNano)
	s.EndTime = unixNanoToTime(endNano)
	s.Duration = duration

	// Read tags count
	var tagsCount uint64
	tagsCount, offset, err = readUvarint(data, offset)
	if err != nil {
		return fmt.Errorf("failed to read tags count: %w", err)
	}

	// Read tags
	s.Tags = make(map[string]string, tagsCount)
	for i := uint64(0); i < tagsCount; i++ {
		var key, val string
		key, offset, err = readString(data, offset)
		if err != nil {
			return fmt.Errorf("failed to read tag key: %w", err)
		}

		val, offset, err = readString(data, offset)
		if err != nil {
			return fmt.Errorf("failed to read tag value: %w", err)
		}

		s.Tags[key] = val
	}

	return nil
}

// Helper functions

func appendUint64BE(buf []byte, v uint64) []byte {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], v)
	return append(buf, tmp[:]...)
}

func appendUvarint(buf []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(buf, tmp[:n]...)
}

func appendSvarint(buf []byte, v int64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutVarint(tmp[:], v)
	return append(buf, tmp[:n]...)
}

func appendString(buf []byte, s string) []byte {
	buf = appendUvarint(buf, uint64(len(s)))
	return append(buf, []byte(s)...)
}

func readUvarint(data []byte, offset int) (uint64, int, error) {
	v, n := binary.Uvarint(data[offset:])
	if n <= 0 {
		return 0, offset, fmt.Errorf("failed to read uvarint")
	}
	return v, offset + n, nil
}

func readSvarint(data []byte, offset int) (int64, int, error) {
	v, n := binary.Varint(data[offset:])
	if n <= 0 {
		return 0, offset, fmt.Errorf("failed to read svarint")
	}
	return v, offset + n, nil
}

func readString(data []byte, offset int) (string, int, error) {
	length, newOffset, err := readUvarint(data, offset)
	if err != nil {
		return "", offset, err
	}

	if newOffset+int(length) > len(data) {
		return "", offset, fmt.Errorf("string length exceeds data bounds")
	}

	str := string(data[newOffset : newOffset+int(length)])
	return str, newOffset + int(length), nil
}

func ParseTraceID(traceID string) (uint64, uint64, error) {
	// Handle 128-bit trace ID (32 hex chars) or 64-bit (16 hex chars)
	if len(traceID) == 32 {
		// 128-bit: split into hi and lo
		hi, err := strconv.ParseUint(traceID[:16], 16, 64)
		if err != nil {
			return 0, 0, err
		}
		lo, err := strconv.ParseUint(traceID[16:], 16, 64)
		if err != nil {
			return 0, 0, err
		}
		return hi, lo, nil
	} else if len(traceID) == 16 {
		// 64-bit: hi is 0
		lo, err := strconv.ParseUint(traceID, 16, 64)
		if err != nil {
			return 0, 0, err
		}
		return 0, lo, nil
	}
	return 0, 0, fmt.Errorf("invalid trace ID length: %d", len(traceID))
}

func ParseSpanID(spanID string) (uint64, error) {
	// Expect 16 hex chars (64-bit)
	if len(spanID) != 16 {
		return 0, fmt.Errorf("invalid span ID length: %d", len(spanID))
	}
	return strconv.ParseUint(spanID, 16, 64)
}

// Keep private version for backwards compatibility
func parseSpanID(spanID string) (uint64, error) {
	return ParseSpanID(spanID)
}

func unixNanoToTime(nano int64) time.Time {
	return time.Unix(0, nano)
}

// MarshalBinary encodes a span link into binary format following the schema:
// u64be    span_id
// u64be    linked_trace_id_hi
// u64be    linked_trace_id_lo
// u64be    linked_span_id
// uvarint  attributes_count
// repeat:
//
//	uvarint key_len
//	bytes   key
//	uvarint val_len
//	bytes   val
func (l *SpanLink) MarshalBinary() ([]byte, error) {
	// Estimate buffer size
	estimatedSize := 8*4 + // Fixed u64 fields
		10 + len(l.Attributes)*20 // attributes

	buf := make([]byte, 0, estimatedSize)

	// Parse span ID
	spanID, err := parseSpanID(l.SpanID)
	if err != nil {
		return nil, fmt.Errorf("invalid span ID: %w", err)
	}

	// Parse linked trace ID
	linkedTraceIDHi, linkedTraceIDLo, err := ParseTraceID(l.LinkedTraceID)
	if err != nil {
		return nil, fmt.Errorf("invalid linked trace ID: %w", err)
	}

	// Parse linked span ID
	linkedSpanID, err := parseSpanID(l.LinkedSpanID)
	if err != nil {
		return nil, fmt.Errorf("invalid linked span ID: %w", err)
	}

	// Write fixed u64 fields
	buf = appendUint64BE(buf, spanID)
	buf = appendUint64BE(buf, linkedTraceIDHi)
	buf = appendUint64BE(buf, linkedTraceIDLo)
	buf = appendUint64BE(buf, linkedSpanID)

	// Write attributes count
	buf = appendUvarint(buf, uint64(len(l.Attributes)))

	// Write attributes (sorted by key for deterministic encoding)
	if len(l.Attributes) > 0 {
		keys := make([]string, 0, len(l.Attributes))
		for k := range l.Attributes {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, key := range keys {
			buf = appendString(buf, key)
			buf = appendString(buf, l.Attributes[key])
		}
	}

	return buf, nil
}

// UnmarshalBinary decodes a span link from binary format
func (l *SpanLink) UnmarshalBinary(data []byte) error {
	offset := 0

	// Read fixed u64 fields
	if len(data) < 32 {
		return fmt.Errorf("data too short for header")
	}

	spanID := binary.BigEndian.Uint64(data[offset:])
	offset += 8
	linkedTraceIDHi := binary.BigEndian.Uint64(data[offset:])
	offset += 8
	linkedTraceIDLo := binary.BigEndian.Uint64(data[offset:])
	offset += 8
	linkedSpanID := binary.BigEndian.Uint64(data[offset:])
	offset += 8

	l.SpanID = fmt.Sprintf("%016x", spanID)
	l.LinkedTraceID = fmt.Sprintf("%016x%016x", linkedTraceIDHi, linkedTraceIDLo)
	l.LinkedSpanID = fmt.Sprintf("%016x", linkedSpanID)

	// Read attributes count
	var attributesCount uint64
	var err error
	attributesCount, offset, err = readUvarint(data, offset)
	if err != nil {
		return fmt.Errorf("failed to read attributes count: %w", err)
	}

	// Read attributes
	l.Attributes = make(map[string]string, attributesCount)
	for i := uint64(0); i < attributesCount; i++ {
		var key, val string
		key, offset, err = readString(data, offset)
		if err != nil {
			return fmt.Errorf("failed to read attribute key: %w", err)
		}

		val, offset, err = readString(data, offset)
		if err != nil {
			return fmt.Errorf("failed to read attribute value: %w", err)
		}

		l.Attributes[key] = val
	}

	return nil
}
