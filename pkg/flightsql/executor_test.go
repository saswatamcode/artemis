package flightsql

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestSortRecord tests Arrow-based record sorting
func TestSortRecord(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create a test schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "duration", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)

	// Build test data
	nameBuilder := array.NewStringBuilder(mem)
	defer nameBuilder.Release()
	nameBuilder.AppendValues([]string{"span3", "span1", "span2"}, nil)
	names := nameBuilder.NewStringArray()
	defer names.Release()

	durationBuilder := array.NewInt64Builder(mem)
	defer durationBuilder.Release()
	durationBuilder.AppendValues([]int64{300, 100, 200}, nil)
	durations := durationBuilder.NewInt64Array()
	defer durations.Release()

	record := array.NewRecord(schema, []arrow.Array{names, durations}, 3)
	defer record.Release()

	t.Run("sort by string column ascending", func(t *testing.T) {
		orderBy := []OrderByClause{{Column: "name", Descending: false}}
		sorted, err := sortRecord(record, orderBy)
		if err != nil {
			t.Fatalf("sortRecord() error = %v", err)
		}
		defer sorted.Release()

		nameCol := sorted.Column(0).(*array.String)
		if nameCol.Value(0) != "span1" || nameCol.Value(1) != "span2" || nameCol.Value(2) != "span3" {
			t.Errorf("sortRecord() names not sorted correctly: %v, %v, %v",
				nameCol.Value(0), nameCol.Value(1), nameCol.Value(2))
		}

		// Check that durations were reordered correctly too
		durationCol := sorted.Column(1).(*array.Int64)
		if durationCol.Value(0) != 100 || durationCol.Value(1) != 200 || durationCol.Value(2) != 300 {
			t.Errorf("sortRecord() durations not reordered correctly: %v, %v, %v",
				durationCol.Value(0), durationCol.Value(1), durationCol.Value(2))
		}
	})

	t.Run("sort by int64 column descending", func(t *testing.T) {
		orderBy := []OrderByClause{{Column: "duration", Descending: true}}
		sorted, err := sortRecord(record, orderBy)
		if err != nil {
			t.Fatalf("sortRecord() error = %v", err)
		}
		defer sorted.Release()

		durationCol := sorted.Column(1).(*array.Int64)
		if durationCol.Value(0) != 300 || durationCol.Value(1) != 200 || durationCol.Value(2) != 100 {
			t.Errorf("sortRecord() durations not sorted correctly: %v, %v, %v",
				durationCol.Value(0), durationCol.Value(1), durationCol.Value(2))
		}

		// Check that names were reordered correctly too
		nameCol := sorted.Column(0).(*array.String)
		if nameCol.Value(0) != "span3" || nameCol.Value(1) != "span2" || nameCol.Value(2) != "span1" {
			t.Errorf("sortRecord() names not reordered correctly: %v, %v, %v",
				nameCol.Value(0), nameCol.Value(1), nameCol.Value(2))
		}
	})

	t.Run("sort by non-existent column", func(t *testing.T) {
		orderBy := []OrderByClause{{Column: "nonexistent", Descending: false}}
		_, err := sortRecord(record, orderBy)
		if err == nil {
			t.Error("sortRecord() expected error for non-existent column")
		}
	})

	t.Run("sort empty record", func(t *testing.T) {
		emptyNames := array.NewSlice(names, 0, 0)
		defer emptyNames.Release()
		emptyDurations := array.NewSlice(durations, 0, 0)
		defer emptyDurations.Release()

		emptyRecord := array.NewRecord(schema, []arrow.Array{emptyNames, emptyDurations}, 0)
		defer emptyRecord.Release()

		orderBy := []OrderByClause{{Column: "name", Descending: false}}
		sorted, err := sortRecord(emptyRecord, orderBy)
		if err != nil {
			t.Fatalf("sortRecord() error = %v", err)
		}
		defer sorted.Release()

		if sorted.NumRows() != 0 {
			t.Errorf("sortRecord() expected 0 rows, got %d", sorted.NumRows())
		}
	})
}

// TestSliceRecord tests Arrow record slicing with offset and limit
func TestSliceRecord(t *testing.T) {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)

	// Build test data with 10 rows
	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	for i := 0; i < 10; i++ {
		builder.Append(int64(i))
	}
	ids := builder.NewInt64Array()
	defer ids.Release()

	record := array.NewRecord(schema, []arrow.Array{ids}, 10)
	defer record.Release()

	t.Run("slice with offset and limit", func(t *testing.T) {
		sliced := sliceRecord(record, 3, 4)
		defer sliced.Release()

		if sliced.NumRows() != 4 {
			t.Errorf("sliceRecord() expected 4 rows, got %d", sliced.NumRows())
		}

		idCol := sliced.Column(0).(*array.Int64)
		for i := 0; i < int(sliced.NumRows()); i++ {
			expected := int64(i + 3)
			if idCol.Value(i) != expected {
				t.Errorf("sliceRecord() row %d = %v, want %v", i, idCol.Value(i), expected)
			}
		}
	})

	t.Run("slice with offset beyond end", func(t *testing.T) {
		sliced := sliceRecord(record, 15, 5)
		defer sliced.Release()

		if sliced.NumRows() != 0 {
			t.Errorf("sliceRecord() expected 0 rows, got %d", sliced.NumRows())
		}
	})

	t.Run("slice with limit beyond end", func(t *testing.T) {
		sliced := sliceRecord(record, 5, 10)
		defer sliced.Release()

		if sliced.NumRows() != 5 {
			t.Errorf("sliceRecord() expected 5 rows, got %d", sliced.NumRows())
		}

		idCol := sliced.Column(0).(*array.Int64)
		for i := 0; i < int(sliced.NumRows()); i++ {
			expected := int64(i + 5)
			if idCol.Value(i) != expected {
				t.Errorf("sliceRecord() row %d = %v, want %v", i, idCol.Value(i), expected)
			}
		}
	})

	t.Run("slice with zero offset", func(t *testing.T) {
		sliced := sliceRecord(record, 0, 3)
		defer sliced.Release()

		if sliced.NumRows() != 3 {
			t.Errorf("sliceRecord() expected 3 rows, got %d", sliced.NumRows())
		}
	})
}

// TestProjectColumns tests column projection
func TestProjectColumns(t *testing.T) {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "col1", Type: arrow.BinaryTypes.String},
			{Name: "col2", Type: arrow.PrimitiveTypes.Int64},
			{Name: "col3", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	// Build test data
	col1Builder := array.NewStringBuilder(mem)
	defer col1Builder.Release()
	col1Builder.AppendValues([]string{"a", "b"}, nil)
	col1 := col1Builder.NewStringArray()
	defer col1.Release()

	col2Builder := array.NewInt64Builder(mem)
	defer col2Builder.Release()
	col2Builder.AppendValues([]int64{1, 2}, nil)
	col2 := col2Builder.NewInt64Array()
	defer col2.Release()

	col3Builder := array.NewStringBuilder(mem)
	defer col3Builder.Release()
	col3Builder.AppendValues([]string{"x", "y"}, nil)
	col3 := col3Builder.NewStringArray()
	defer col3.Release()

	record := array.NewRecord(schema, []arrow.Array{col1, col2, col3}, 2)
	defer record.Release()

	t.Run("project subset of columns", func(t *testing.T) {
		projected := projectColumns(record, []string{"col1", "col3"})
		defer projected.Release()

		if projected.NumCols() != 2 {
			t.Errorf("projectColumns() expected 2 columns, got %d", projected.NumCols())
		}

		if projected.Schema().Field(0).Name != "col1" {
			t.Errorf("projectColumns() first column name = %s, want col1", projected.Schema().Field(0).Name)
		}

		if projected.Schema().Field(1).Name != "col3" {
			t.Errorf("projectColumns() second column name = %s, want col3", projected.Schema().Field(1).Name)
		}
	})

	t.Run("project non-existent columns", func(t *testing.T) {
		projected := projectColumns(record, []string{"nonexistent"})
		defer projected.Release()

		// Should return empty record but with the requested column names
		if projected.NumRows() != 0 {
			t.Errorf("projectColumns() expected 0 rows, got %d", projected.NumRows())
		}
	})

	t.Run("project all columns", func(t *testing.T) {
		projected := projectColumns(record, []string{"col1", "col2", "col3"})
		defer projected.Release()

		if projected.NumCols() != 3 {
			t.Errorf("projectColumns() expected 3 columns, got %d", projected.NumCols())
		}
	})
}

// TestReorderArrayWithMap tests reordering arrays including map types
func TestReorderArrayWithMap(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("reorder string array", func(t *testing.T) {
		builder := array.NewStringBuilder(mem)
		defer builder.Release()
		builder.AppendValues([]string{"c", "a", "b"}, nil)
		arr := builder.NewStringArray()
		defer arr.Release()

		indices := []int{1, 2, 0}
		reordered, err := reorderArray(arr, indices, mem)
		if err != nil {
			t.Fatalf("reorderArray() error = %v", err)
		}
		defer reordered.Release()

		strArr := reordered.(*array.String)
		if strArr.Value(0) != "a" || strArr.Value(1) != "b" || strArr.Value(2) != "c" {
			t.Errorf("reorderArray() values not reordered correctly: %v, %v, %v",
				strArr.Value(0), strArr.Value(1), strArr.Value(2))
		}
	})

	t.Run("reorder int64 array", func(t *testing.T) {
		builder := array.NewInt64Builder(mem)
		defer builder.Release()
		builder.AppendValues([]int64{30, 10, 20}, nil)
		arr := builder.NewInt64Array()
		defer arr.Release()

		indices := []int{1, 2, 0}
		reordered, err := reorderArray(arr, indices, mem)
		if err != nil {
			t.Fatalf("reorderArray() error = %v", err)
		}
		defer reordered.Release()

		intArr := reordered.(*array.Int64)
		if intArr.Value(0) != 10 || intArr.Value(1) != 20 || intArr.Value(2) != 30 {
			t.Errorf("reorderArray() values not reordered correctly: %v, %v, %v",
				intArr.Value(0), intArr.Value(1), intArr.Value(2))
		}
	})

	t.Run("reorder map array", func(t *testing.T) {
		mapBuilder := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
		defer mapBuilder.Release()

		keyBuilder := mapBuilder.KeyBuilder().(*array.StringBuilder)
		valueBuilder := mapBuilder.ItemBuilder().(*array.StringBuilder)

		// First map: {"key1": "val1"}
		mapBuilder.Append(true)
		keyBuilder.Append("key1")
		valueBuilder.Append("val1")

		// Second map: {"key2": "val2"}
		mapBuilder.Append(true)
		keyBuilder.Append("key2")
		valueBuilder.Append("val2")

		// Third map: {"key3": "val3"}
		mapBuilder.Append(true)
		keyBuilder.Append("key3")
		valueBuilder.Append("val3")

		arr := mapBuilder.NewMapArray()
		defer arr.Release()

		// Reorder: swap first and last
		indices := []int{2, 1, 0}
		reordered, err := reorderArray(arr, indices, mem)
		if err != nil {
			t.Fatalf("reorderArray() error = %v", err)
		}
		defer reordered.Release()

		mapArr := reordered.(*array.Map)
		if mapArr.Len() != 3 {
			t.Errorf("reorderArray() expected 3 map entries, got %d", mapArr.Len())
		}

		// Check first entry (should be key3)
		start, _ := mapArr.ValueOffsets(0)
		keys := mapArr.Keys().(*array.String)
		if keys.Value(int(start)) != "key3" {
			t.Errorf("reorderArray() first map key = %s, want key3", keys.Value(int(start)))
		}
	})
}
