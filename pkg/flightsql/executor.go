package flightsql

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
	"github.com/saswatamcode/artemis/pkg/tracedb"
)

// SQLExecutor executes SQL queries against Artemis
type SQLExecutor struct {
	db     *tracedb.DB
	logger *slog.Logger
}

// NewSQLExecutor creates a new SQL executor
func NewSQLExecutor(db *tracedb.DB, logger *slog.Logger) *SQLExecutor {
	if logger == nil {
		logger = slog.Default()
	}
	return &SQLExecutor{
		db:     db,
		logger: logger,
	}
}

// Execute executes a SQL query and returns Arrow records
func (e *SQLExecutor) Execute(sqlQuery string) (arrow.Record, error) {
	// Parse SQL
	query, err := ParseSQL(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	e.logger.Debug("executing sql query",
		"columns", query.Columns,
		"matchers", len(query.Matchers),
		"has_time_range", query.TimeRange != nil,
		"limit", query.Limit,
	)

	// Get querier
	querier := e.db.GetQuerier()

	// Execute query with or without time range
	var spans []*span.Span

	if query.TimeRange != nil {
		selectResult, err := querier.SelectWithTimeRange(query.TimeRange, query.Matchers...)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query with time range: %w", err)
		}
		spans = selectResult.Spans
	} else {
		selectResult, err := querier.Select(query.Matchers...)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query: %w", err)
		}
		spans = selectResult.Spans
	}

	// Apply ORDER BY
	if len(query.OrderBy) > 0 {
		e.applyOrderBy(spans, query.OrderBy)
	}

	// Apply LIMIT
	if query.Limit > 0 && query.Limit < len(spans) {
		spans = spans[:query.Limit]
	}

	e.logger.Debug("query executed", "spans_returned", len(spans))

	// Convert spans to Arrow record
	record, err := ConvertSpansToArrowRecord(spans, query.Columns)
	if err != nil {
		return nil, fmt.Errorf("failed to convert spans to Arrow: %w", err)
	}

	return record, nil
}

// applyOrderBy sorts spans based on ORDER BY clauses
func (e *SQLExecutor) applyOrderBy(spans []*span.Span, orderBy []OrderByClause) {
	if len(orderBy) == 0 {
		return
	}

	// For now, support only first ORDER BY clause
	clause := orderBy[0]

	sort.Slice(spans, func(i, j int) bool {
		var vi, vj any

		switch clause.Column {
		case "trace_id":
			vi, vj = spans[i].TraceID, spans[j].TraceID
		case "span_id":
			vi, vj = spans[i].SpanID, spans[j].SpanID
		case "name":
			vi, vj = spans[i].Name, spans[j].Name
		case "service_name":
			vi, vj = spans[i].ServiceName, spans[j].ServiceName
		case "start_time":
			vi, vj = spans[i].StartTime.UnixNano(), spans[j].StartTime.UnixNano()
		case "end_time":
			vi, vj = spans[i].EndTime.UnixNano(), spans[j].EndTime.UnixNano()
		case "duration":
			vi, vj = spans[i].GetDuration(), spans[j].GetDuration()
		default:
			return false
		}

		// Compare based on type
		switch v := vi.(type) {
		case string:
			if clause.Descending {
				return v > vj.(string)
			}
			return v < vj.(string)
		case int64:
			if clause.Descending {
				return v > vj.(int64)
			}
			return v < vj.(int64)
		}

		return false
	})
}

// ConvertSpansToArrowRecord converts spans to an Arrow record
// Reuses SpanRecordBuilder from pkg/storage/arrow.go
func ConvertSpansToArrowRecord(spans []*span.Span, columns []string) (arrow.Record, error) {
	if len(spans) == 0 {
		// Return empty record with schema
		mem := memory.NewGoAllocator()
		schema := GetSpansSchema()
		builder := storage.NewSpanRecordBuilder(mem, schema)
		defer builder.Release()

		// Build empty record
		record := builder.NewRecord()
		if record == nil {
			// Create truly empty record
			emptyArrays := make([]arrow.Array, len(schema.Fields()))
			for i, field := range schema.Fields() {
				switch field.Type {
				case arrow.BinaryTypes.String:
					b := array.NewStringBuilder(mem)
					emptyArrays[i] = b.NewStringArray()
					b.Release()
				case arrow.PrimitiveTypes.Int64:
					b := array.NewInt64Builder(mem)
					emptyArrays[i] = b.NewInt64Array()
					b.Release()
				default:
					if field.Name == "tags" {
						b := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
						emptyArrays[i] = b.NewMapArray()
						b.Release()
					}
				}
			}
			record = array.NewRecord(schema, emptyArrays, 0)
		}
		return record, nil
	}

	mem := memory.NewGoAllocator()
	schema := GetSpansSchema()
	builder := storage.NewSpanRecordBuilder(mem, schema)
	defer builder.Release()

	for _, sp := range spans {
		builder.Append(sp)
	}

	record := builder.NewRecord()
	if record == nil {
		return nil, fmt.Errorf("failed to build Arrow record")
	}

	return record, nil
}
