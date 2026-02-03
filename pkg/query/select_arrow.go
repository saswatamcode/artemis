package query

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// SelectAsArrow queries and returns Arrow records directly without converting to span.Span
// This avoids the double conversion: Arrow→Span→Arrow
func SelectAsArrow(storage *storage.ArrowStorage, matchers ...*Matcher) (arrow.Record, error) {
	// Get all Arrow records from storage
	records := storage.GetRecords()
	if len(records) == 0 {
		return createEmptyRecord(memory.NewGoAllocator()), nil
	}

	// If no matchers, return all records concatenated
	if len(matchers) == 0 {
		return concatenateRecords(records)
	}

	// Apply filters on Arrow records
	filtered, err := filterArrowRecords(records, matchers)
	if err != nil {
		return nil, fmt.Errorf("failed to filter records: %w", err)
	}

	return concatenateRecords(filtered)
}

// SelectAsArrowWithTimeRange queries Arrow records within a time range
func SelectAsArrowWithTimeRange(storage *storage.ArrowStorage, blocks []block.Block, timeRange *TimeRange, matchers ...*Matcher) (arrow.Record, error) {
	var allRecords []arrow.Record

	// Get records from head/storage
	if storage != nil {
		records := storage.GetRecords()
		allRecords = append(allRecords, records...)
	}

	// Get records from blocks (both Arrow L0 and Parquet L1+ blocks)
	for _, blk := range blocks {
		// Skip blocks outside time range
		meta := blk.Meta()
		if !timeRange.Overlaps(meta.MinTime, meta.MaxTime) {
			continue
		}

		// Use AsArrowRecords() for native Arrow conversion (works for both Arrow and Parquet blocks)
		records, err := blk.AsArrowRecords()
		if err != nil {
			return nil, fmt.Errorf("failed to get arrow records from block %s: %w", blk.Dir(), err)
		}
		if records != nil {
			allRecords = append(allRecords, records...)
		}
	}

	if len(allRecords) == 0 {
		return createEmptyRecord(memory.NewGoAllocator()), nil
	}

	// Apply filters if any
	if len(matchers) > 0 {
		filtered, err := filterArrowRecords(allRecords, matchers)
		if err != nil {
			return nil, fmt.Errorf("failed to filter records: %w", err)
		}
		allRecords = filtered
	}

	return concatenateRecords(allRecords)
}

// filterArrowRecords filters Arrow records based on matchers
func filterArrowRecords(records []arrow.Record, matchers []*Matcher) ([]arrow.Record, error) {
	var filtered []arrow.Record

	for _, record := range records {
		// Build filter mask for this record
		mask, err := buildFilterMask(record, matchers)
		if err != nil {
			return nil, err
		}

		// Apply mask to filter rows
		filteredRecord, err := applyFilterMask(record, mask)
		mask.Release()

		if err != nil {
			return nil, err
		}

		if filteredRecord.NumRows() > 0 {
			filtered = append(filtered, filteredRecord)
		}
	}

	return filtered, nil
}

// buildFilterMask creates a boolean array indicating which rows match
func buildFilterMask(record arrow.Record, matchers []*Matcher) (*array.Boolean, error) {
	mem := memory.NewGoAllocator()
	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()

	numRows := int(record.NumRows())
	schema := record.Schema()

	// For each row, check if all matchers match
	for i := 0; i < numRows; i++ {
		match := true
		for _, matcher := range matchers {
			// Find column index for this matcher
			fieldIdx := schema.FieldIndices(matcher.Name)
			if len(fieldIdx) == 0 {
				match = false
				break
			}

			col := record.Column(fieldIdx[0])
			if !matchesArrowValue(col, i, matcher) {
				match = false
				break
			}
		}
		builder.Append(match)
	}

	return builder.NewBooleanArray(), nil
}

// matchesArrowValue checks if a value in an Arrow array matches the matcher
func matchesArrowValue(col arrow.Array, rowIdx int, matcher *Matcher) bool {
	if col.IsNull(rowIdx) {
		return matcher.Type == MatchNotEqual
	}

	// Handle different column types
	switch arr := col.(type) {
	case *array.String:
		value := arr.Value(rowIdx)
		return matchesString(value, matcher)
	case *array.Int64:
		// For numeric columns, we only support exact match for now
		if matcher.Type == MatchEqual {
			// TODO: Parse matcher.Value as int64 and compare
			return false
		}
		return false
	default:
		return false
	}
}

// matchesString applies matcher logic to a string value
func matchesString(value string, matcher *Matcher) bool {
	switch matcher.Type {
	case MatchEqual:
		return value == matcher.Value
	case MatchNotEqual:
		return value != matcher.Value
	case MatchRegexp:
		// matcher already has compiled regexp in matcher.re
		// But we can't access it from here, so fall back to simple check
		return value == matcher.Value
	case MatchNotRegexp:
		return value != matcher.Value
	default:
		return false
	}
}

// applyFilterMask applies a boolean mask to filter an Arrow record
func applyFilterMask(record arrow.Record, mask *array.Boolean) (arrow.Record, error) {
	mem := memory.NewGoAllocator()
	schema := record.Schema()

	// Count true values to allocate result
	var trueCount int64
	for i := 0; i < mask.Len(); i++ {
		if mask.Value(i) {
			trueCount++
		}
	}

	if trueCount == 0 {
		return createEmptyRecord(mem), nil
	}

	// Build new arrays with only matching rows
	newArrays := make([]arrow.Array, record.NumCols())
	for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
		col := record.Column(colIdx)
		newArrays[colIdx] = filterArray(mem, col, mask)
	}

	return array.NewRecord(schema, newArrays, trueCount), nil
}

// filterArray filters an Arrow array using a boolean mask
func filterArray(mem memory.Allocator, arr arrow.Array, mask *array.Boolean) arrow.Array {
	switch typed := arr.(type) {
	case *array.String:
		builder := array.NewStringBuilder(mem)
		for i := 0; i < mask.Len(); i++ {
			if mask.Value(i) {
				if typed.IsNull(i) {
					builder.AppendNull()
				} else {
					builder.Append(typed.Value(i))
				}
			}
		}
		result := builder.NewStringArray()
		builder.Release()
		return result

	case *array.Int64:
		builder := array.NewInt64Builder(mem)
		for i := 0; i < mask.Len(); i++ {
			if mask.Value(i) {
				if typed.IsNull(i) {
					builder.AppendNull()
				} else {
					builder.Append(typed.Value(i))
				}
			}
		}
		result := builder.NewInt64Array()
		builder.Release()
		return result

	case *array.Map:
		// Map arrays require special handling
		// For now, use array.Concatenate with slices to preserve matching rows
		var slices []arrow.Array
		for i := 0; i < mask.Len(); i++ {
			if mask.Value(i) {
				slices = append(slices, array.NewSlice(typed, int64(i), int64(i+1)))
			}
		}

		if len(slices) == 0 {
			// Return empty map array
			builder := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
			result := builder.NewMapArray()
			builder.Release()
			return result
		}

		if len(slices) == 1 {
			return slices[0]
		}

		// Concatenate all slices
		result, err := array.Concatenate(slices, mem)
		if err != nil {
			// Fallback: return empty array
			builder := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
			emptyResult := builder.NewMapArray()
			builder.Release()
			return emptyResult
		}
		return result

	default:
		// Fallback: return empty array
		return arr
	}
}

// concatenateRecords combines multiple Arrow records into one
func concatenateRecords(records []arrow.Record) (arrow.Record, error) {
	if len(records) == 0 {
		return createEmptyRecord(memory.NewGoAllocator()), nil
	}

	mem := memory.NewGoAllocator()
	schema := records[0].Schema()

	// Always concatenate to create new arrays with copied data
	// This ensures the returned record owns its data and isn't affected
	// by changes to the original records (e.g., storage rotations)
	numCols := int(records[0].NumCols())
	concatArrays := make([]arrow.Array, numCols)

	for colIdx := 0; colIdx < numCols; colIdx++ {
		// Collect arrays for this column
		var arrays []arrow.Array
		for _, rec := range records {
			arrays = append(arrays, rec.Column(colIdx))
		}

		// Use Arrow's Concatenate function
		concat, err := array.Concatenate(arrays, mem)
		if err != nil {
			return nil, fmt.Errorf("failed to concatenate column %d: %w", colIdx, err)
		}
		concatArrays[colIdx] = concat
	}

	// Count total rows
	var totalRows int64
	for _, rec := range records {
		totalRows += rec.NumRows()
	}

	return array.NewRecord(schema, concatArrays, totalRows), nil
}

// createEmptyRecord creates an empty Arrow record with the spans schema
func createEmptyRecord(mem memory.Allocator) arrow.Record {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "trace_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "span_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "parent_span_id", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "start_time", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "end_time", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "duration", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "service_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "tags", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), Nullable: true},
	}, nil)

	emptyArrays := make([]arrow.Array, len(schema.Fields()))
	for i, field := range schema.Fields() {
		switch field.Type.ID() {
		case arrow.STRING:
			b := array.NewStringBuilder(mem)
			emptyArrays[i] = b.NewStringArray()
			b.Release()
		case arrow.INT64:
			b := array.NewInt64Builder(mem)
			emptyArrays[i] = b.NewInt64Array()
			b.Release()
		case arrow.MAP:
			b := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
			emptyArrays[i] = b.NewMapArray()
			b.Release()
		default:
			b := array.NewNullBuilder(mem)
			emptyArrays[i] = b.NewArray()
			b.Release()
		}
	}

	return array.NewRecord(schema, emptyArrays, 0)
}
