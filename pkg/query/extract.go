package query

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/saswatamcode/artemis/pkg/span"
)

// extractSpanFromRecord extracts a span from an Arrow record at the given row
// This is a duplicate of the storage method, but needed here for query access
// Set extractTags=false to skip tag extraction for better performance when tags aren't needed
func extractSpanFromRecord(record arrow.RecordBatch, rowIndex int) (*span.Span, error) {
	return extractSpanFromRecordWithOptions(record, rowIndex, true)
}

// extractSpanFromRecordWithOptions extracts a span with control over what to extract
// extractTags controls whether to materialize the tags map (expensive operation)
func extractSpanFromRecordWithOptions(record arrow.RecordBatch, rowIndex int, extractTags bool) (*span.Span, error) {
	if rowIndex >= int(record.NumRows()) {
		return nil, fmt.Errorf("invalid row index %d", rowIndex)
	}

	sp := &span.Span{}

	// New schema: trace_id_hi(0), trace_id_lo(1), span_id(2), parent_span_id(3), name(4),
	//             start_time(5), end_time(6), duration(7), service_name(8), tags(9)

	// Read trace_id as two uint64 values and format as hex string
	traceIDHi := record.Column(0).(*array.Uint64).Value(rowIndex)
	traceIDLo := record.Column(1).(*array.Uint64).Value(rowIndex)
	sp.TraceID = fmt.Sprintf("%016x%016x", traceIDHi, traceIDLo)

	// Read span_id and format as hex string
	spanIDVal := record.Column(2).(*array.Uint64).Value(rowIndex)
	sp.SpanID = fmt.Sprintf("%016x", spanIDVal)

	// Read parent_span_id and format as hex string (or empty if null)
	parentCol := record.Column(3).(*array.Uint64)
	if !parentCol.IsNull(rowIndex) {
		parentSpanIDVal := parentCol.Value(rowIndex)
		sp.ParentSpanID = fmt.Sprintf("%016x", parentSpanIDVal)
	}

	sp.Name = record.Column(4).(*array.String).Value(rowIndex)

	sp.StartTime = time.Unix(0, record.Column(5).(*array.Int64).Value(rowIndex))

	sp.EndTime = time.Unix(0, record.Column(6).(*array.Int64).Value(rowIndex))

	sp.Duration = record.Column(7).(*array.Int64).Value(rowIndex)

	sp.ServiceName = record.Column(8).(*array.String).Value(rowIndex)

	// Only extract tags if needed (expensive operation)
	if extractTags {
		tagsCol := record.Column(9).(*array.Map)
		if !tagsCol.IsNull(rowIndex) {
			sp.Tags = make(map[string]string)

			// Get the offset for this map entry
			offsets := tagsCol.Offsets()
			// Bounds check: ensure rowIndex+1 is within offsets array
			if rowIndex+1 >= len(offsets) {
				return nil, fmt.Errorf("invalid row index %d for tag extraction (offsets length: %d)", rowIndex, len(offsets))
			}

			offset := offsets[rowIndex]
			nextOffset := offsets[rowIndex+1]

			keys := tagsCol.Keys().(*array.String)
			items := tagsCol.Items().(*array.String)

			for i := int(offset); i < int(nextOffset); i++ {
				// Bounds check for keys and items arrays
				if i >= keys.Len() || i >= items.Len() {
					break
				}
				key := keys.Value(i)
				value := items.Value(i)
				sp.Tags[key] = value
			}
		}
	}

	return sp, nil
}
