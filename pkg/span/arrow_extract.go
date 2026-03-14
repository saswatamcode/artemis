package span

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ExtractSpanFromRecord extracts a span from an Arrow record at the given row.
// This is the canonical implementation used across storage and block packages.
//
// The record must have the standard span schema with 12 columns:
// trace_id_hi, trace_id_lo, span_id, parent_span_id, name, start_time, end_time,
// duration, service_name, tags, bucket1s, duration_bucket
//
// Returns error if the row index is invalid or the schema is incorrect.
func ExtractSpanFromRecord(record arrow.RecordBatch, rowIndex int) (*Span, error) {
	// Validate row index
	if rowIndex < 0 || rowIndex >= int(record.NumRows()) {
		return nil, fmt.Errorf("invalid row index %d (record has %d rows)", rowIndex, record.NumRows())
	}

	// Validate schema has expected number of columns
	// We have 12 columns (10 original + 2 indexing fields: bucket1s, duration_bucket)
	// The indexing fields don't need to be extracted into the Span struct
	expectedColumns := 12
	if record.NumCols() < int64(expectedColumns) {
		return nil, fmt.Errorf("invalid schema: expected at least %d columns, got %d", expectedColumns, record.NumCols())
	}

	sp := &Span{}

	// Extract trace ID (columns 0-1: trace_id_hi, trace_id_lo)
	traceIDHi := record.Column(0).(*array.Uint64).Value(rowIndex)
	traceIDLo := record.Column(1).(*array.Uint64).Value(rowIndex)
	sp.TraceID = FormatTraceID(traceIDHi, traceIDLo)

	// Extract span ID (column 2)
	spanIDVal := record.Column(2).(*array.Uint64).Value(rowIndex)
	sp.SpanID = FormatSpanID(spanIDVal)

	// Extract parent span ID (column 3, nullable)
	parentCol := record.Column(3).(*array.Uint64)
	if !parentCol.IsNull(rowIndex) {
		parentSpanIDVal := parentCol.Value(rowIndex)
		sp.ParentSpanID = FormatSpanID(parentSpanIDVal)
	}

	// Extract name (column 4)
	sp.Name = record.Column(4).(*array.String).Value(rowIndex)

	// Extract timestamps (columns 5-6)
	sp.StartTime = time.Unix(0, record.Column(5).(*array.Int64).Value(rowIndex))
	sp.EndTime = time.Unix(0, record.Column(6).(*array.Int64).Value(rowIndex))

	// Extract duration (column 7)
	sp.Duration = record.Column(7).(*array.Int64).Value(rowIndex)

	// Extract service name (column 8)
	sp.ServiceName = record.Column(8).(*array.String).Value(rowIndex)

	// Extract tags map (column 9, nullable)
	tagsCol := record.Column(9).(*array.Map)
	if !tagsCol.IsNull(rowIndex) {
		sp.Tags = make(map[string]string)

		offsets := tagsCol.Offsets()
		// Validate offset bounds
		if rowIndex+1 >= len(offsets) {
			return nil, fmt.Errorf("invalid offset index %d for tags map (offsets length: %d)", rowIndex+1, len(offsets))
		}

		offset := offsets[rowIndex]
		nextOffset := offsets[rowIndex+1]

		keys := tagsCol.Keys().(*array.String)
		items := tagsCol.Items().(*array.String)

		for i := int(offset); i < int(nextOffset); i++ {
			// Validate bounds to detect corrupted data
			if i >= keys.Len() || i >= items.Len() {
				return nil, fmt.Errorf("corrupted tags data at row %d: tag index %d exceeds bounds (keys: %d, items: %d)",
					rowIndex, i, keys.Len(), items.Len())
			}
			key := keys.Value(i)
			value := items.Value(i)
			sp.Tags[key] = value
		}
	}

	return sp, nil
}

// ExtractLinkFromRecord extracts a span link from an Arrow record at the given row.
// This is the canonical implementation used across storage and block packages.
//
// The record must have the standard link schema with 5 columns:
// span_id, linked_trace_id_hi, linked_trace_id_lo, linked_span_id, attributes
//
// Returns error if the row index is invalid or the schema is incorrect.
func ExtractLinkFromRecord(record arrow.RecordBatch, rowIndex int) (*SpanLink, error) {
	// Validate row index
	if rowIndex < 0 || rowIndex >= int(record.NumRows()) {
		return nil, fmt.Errorf("invalid row index %d (record has %d rows)", rowIndex, record.NumRows())
	}

	// Validate schema
	expectedColumns := 5 // span_id, linked_trace_id_hi, linked_trace_id_lo, linked_span_id, attributes
	if record.NumCols() < int64(expectedColumns) {
		return nil, fmt.Errorf("invalid schema: expected at least %d columns, got %d", expectedColumns, record.NumCols())
	}

	l := &SpanLink{}

	// Extract span ID (column 0)
	spanIDVal := record.Column(0).(*array.Uint64).Value(rowIndex)
	l.SpanID = FormatSpanID(spanIDVal)

	// Extract linked trace ID (columns 1-2: linked_trace_id_hi, linked_trace_id_lo)
	linkedTraceIDHi := record.Column(1).(*array.Uint64).Value(rowIndex)
	linkedTraceIDLo := record.Column(2).(*array.Uint64).Value(rowIndex)
	l.LinkedTraceID = FormatTraceID(linkedTraceIDHi, linkedTraceIDLo)

	// Extract linked span ID (column 3)
	linkedSpanIDVal := record.Column(3).(*array.Uint64).Value(rowIndex)
	l.LinkedSpanID = FormatSpanID(linkedSpanIDVal)

	// Extract attributes map (column 4, nullable)
	attrsCol := record.Column(4).(*array.Map)
	if !attrsCol.IsNull(rowIndex) {
		l.Attributes = make(map[string]string)

		offsets := attrsCol.Offsets()
		// Validate offset bounds
		if rowIndex+1 >= len(offsets) {
			return nil, fmt.Errorf("invalid offset index %d for attributes map (offsets length: %d)", rowIndex+1, len(offsets))
		}

		offset := offsets[rowIndex]
		nextOffset := offsets[rowIndex+1]

		keys := attrsCol.Keys().(*array.String)
		items := attrsCol.Items().(*array.String)

		for i := int(offset); i < int(nextOffset); i++ {
			// Validate bounds to detect corrupted data
			if i >= keys.Len() || i >= items.Len() {
				return nil, fmt.Errorf("corrupted attributes data at row %d: index %d exceeds bounds (keys: %d, items: %d)",
					rowIndex, i, keys.Len(), items.Len())
			}
			key := keys.Value(i)
			value := items.Value(i)
			l.Attributes[key] = value
		}
	}

	return l, nil
}
