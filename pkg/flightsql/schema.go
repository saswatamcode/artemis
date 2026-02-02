package flightsql

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// GetSpansSchema returns the Arrow schema for spans
// This matches the schema used internally in pkg/storage/arrow.go
func GetSpansSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "trace_id", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "span_id", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "parent_span_id", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "start_time", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "end_time", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "duration", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "service_name", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "tags", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), Nullable: true},
		},
		nil,
	)
}
