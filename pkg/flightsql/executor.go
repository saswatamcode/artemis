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

	// Apply ORDER BY using Arrow-based sorting
	if len(query.OrderBy) > 0 {
		record, err = sortRecord(record, query.OrderBy)
		if err != nil {
			e.logger.Warn("failed to sort record, returning unsorted", "error", err)
		}
	}

	// Apply LIMIT and OFFSET on Arrow record
	if query.Limit > 0 || query.Offset > 0 {
		limit := query.Limit
		if limit < 0 {
			limit = int(record.NumRows()) // No limit means all rows
		}
		record = sliceRecord(record, query.Offset, limit)
	}

	// Apply column projection if not SELECT *
	if len(query.Columns) > 0 && query.Columns[0] != "*" {
		record = projectColumns(record, query.Columns)
	}

	e.logger.Debug("query executed", "rows_returned", record.NumRows())
	return record, nil
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

// sortRecord sorts an Arrow record based on ORDER BY clauses
// Uses Go's sort package with Arrow array accessors
func sortRecord(record arrow.Record, orderBy []OrderByClause) (arrow.Record, error) {
	if len(orderBy) == 0 || record.NumRows() == 0 {
		return record, nil
	}

	// For MVP, support only the first ORDER BY clause
	clause := orderBy[0]
	schema := record.Schema()
	colIdx := schema.FieldIndices(clause.Column)
	if len(colIdx) == 0 {
		return record, fmt.Errorf("column %s not found in schema", clause.Column)
	}

	// Get the column array
	column := record.Column(colIdx[0])
	numRows := int(record.NumRows())

	// Create indices array
	indices := make([]int, numRows)
	for i := range indices {
		indices[i] = i
	}

	// Sort indices based on column values and type
	switch arr := column.(type) {
	case *array.String:
		sort.Slice(indices, func(i, j int) bool {
			vi, vj := arr.Value(indices[i]), arr.Value(indices[j])
			if clause.Descending {
				return vi > vj
			}
			return vi < vj
		})
	case *array.Int64:
		sort.Slice(indices, func(i, j int) bool {
			vi, vj := arr.Value(indices[i]), arr.Value(indices[j])
			if clause.Descending {
				return vi > vj
			}
			return vi < vj
		})
	default:
		return record, fmt.Errorf("unsupported column type for sorting: %T", arr)
	}

	// Use Take to reorder all columns based on sorted indices
	return takeRecord(record, indices)
}

// takeRecord reorders an Arrow record based on an array of indices
func takeRecord(record arrow.Record, indices []int) (arrow.Record, error) {
	mem := memory.NewGoAllocator()
	numCols := int(record.NumCols())
	reorderedArrays := make([]arrow.Array, numCols)

	// Build index array for Arrow Take operation
	indexBuilder := array.NewInt64Builder(mem)
	defer indexBuilder.Release()

	for _, idx := range indices {
		indexBuilder.Append(int64(idx))
	}
	indexArray := indexBuilder.NewInt64Array()
	defer indexArray.Release()

	// Reorder each column
	for i := 0; i < numCols; i++ {
		column := record.Column(i)

		// Manually reorder based on indices (Arrow compute Take may not be available)
		reordered, err := reorderArray(column, indices, mem)
		if err != nil {
			return nil, fmt.Errorf("failed to reorder column %d: %w", i, err)
		}
		reorderedArrays[i] = reordered
	}

	return array.NewRecord(record.Schema(), reorderedArrays, int64(len(indices))), nil
}

// reorderArray reorders an Arrow array based on indices
func reorderArray(arr arrow.Array, indices []int, mem memory.Allocator) (arrow.Array, error) {
	switch typed := arr.(type) {
	case *array.String:
		builder := array.NewStringBuilder(mem)
		defer builder.Release()
		for _, idx := range indices {
			if typed.IsNull(idx) {
				builder.AppendNull()
			} else {
				builder.Append(typed.Value(idx))
			}
		}
		return builder.NewStringArray(), nil

	case *array.Int64:
		builder := array.NewInt64Builder(mem)
		defer builder.Release()
		for _, idx := range indices {
			if typed.IsNull(idx) {
				builder.AppendNull()
			} else {
				builder.Append(typed.Value(idx))
			}
		}
		return builder.NewInt64Array(), nil

	case *array.Map:
		// For map arrays (tags), we need special handling
		builder := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
		defer builder.Release()

		for _, idx := range indices {
			if typed.IsNull(idx) {
				builder.AppendNull()
			} else {
				// Get the map value at this index
				start, end := typed.ValueOffsets(idx)
				builder.Append(true)

				// Get the keys and values arrays
				keys := typed.Keys().(*array.String)
				values := typed.Items().(*array.String)

				keyBuilder := builder.KeyBuilder().(*array.StringBuilder)
				valueBuilder := builder.ItemBuilder().(*array.StringBuilder)

				// Copy all key-value pairs for this map entry
				for j := int(start); j < int(end); j++ {
					keyBuilder.Append(keys.Value(j))
					valueBuilder.Append(values.Value(j))
				}
			}
		}
		return builder.NewMapArray(), nil

	default:
		return nil, fmt.Errorf("unsupported array type for reordering: %T", typed)
	}
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
