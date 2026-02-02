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

	// Execute query and get Arrow records directly (no Span conversion!)
	var record arrow.Record

	if query.TimeRange != nil {
		record, err = querier.SelectAsArrowWithTimeRange(query.TimeRange, query.Matchers...)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query with time range: %w", err)
		}
	} else {
		record, err = querier.SelectAsArrow(query.Matchers...)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query: %w", err)
		}
	}

	// TODO: Apply ORDER BY on Arrow record (not implemented yet)
	// For now, ORDER BY is skipped when using Arrow path

	// Apply LIMIT on Arrow record
	if query.Limit > 0 && record.NumRows() > int64(query.Limit) {
		record = sliceRecord(record, 0, query.Limit)
	}

	// Apply column projection if not SELECT *
	if len(query.Columns) > 0 && query.Columns[0] != "*" {
		record = projectColumns(record, query.Columns)
	}

	e.logger.Debug("query executed", "rows_returned", record.NumRows())
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

// projectColumns filters an Arrow record to only include specified columns
func projectColumns(record arrow.Record, columns []string) arrow.Record {
	schema := record.Schema()

	// Find indices and build new schema
	var indices []int
	var fields []arrow.Field

	for _, colName := range columns {
		idx := schema.FieldIndices(colName)
		if len(idx) > 0 {
			indices = append(indices, idx[0])
			fields = append(fields, schema.Field(idx[0]))
		}
	}

	if len(indices) == 0 {
		// No valid columns, return empty record
		return createEmptyProjectedRecord(columns)
	}

	// Extract columns
	projectedArrays := make([]arrow.Array, len(indices))
	for i, idx := range indices {
		projectedArrays[i] = record.Column(idx)
	}

	newSchema := arrow.NewSchema(fields, nil)
	return array.NewRecord(newSchema, projectedArrays, record.NumRows())
}

// createEmptyProjectedRecord creates an empty record with the requested column names
func createEmptyProjectedRecord(columns []string) arrow.Record {
	mem := memory.NewGoAllocator()

	// Create fields with string type (default)
	fields := make([]arrow.Field, len(columns))
	emptyArrays := make([]arrow.Array, len(columns))

	for i, colName := range columns {
		fields[i] = arrow.Field{Name: colName, Type: arrow.BinaryTypes.String, Nullable: true}
		builder := array.NewStringBuilder(mem)
		emptyArrays[i] = builder.NewStringArray()
		builder.Release()
	}

	schema := arrow.NewSchema(fields, nil)
	return array.NewRecord(schema, emptyArrays, 0)
}

// sliceRecord returns a subset of an Arrow record
func sliceRecord(record arrow.Record, offset, length int) arrow.Record {
	if offset >= int(record.NumRows()) {
		// Return empty record
		emptyArrays := make([]arrow.Array, record.NumCols())
		for i := range emptyArrays {
			emptyArrays[i] = array.NewSlice(record.Column(i), 0, 0)
		}
		return array.NewRecord(record.Schema(), emptyArrays, 0)
	}

	end := offset + length
	if end > int(record.NumRows()) {
		end = int(record.NumRows())
	}

	slicedArrays := make([]arrow.Array, record.NumCols())
	for i := 0; i < int(record.NumCols()); i++ {
		slicedArrays[i] = array.NewSlice(record.Column(i), int64(offset), int64(end))
	}

	return array.NewRecord(record.Schema(), slicedArrays, int64(end-offset))
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
