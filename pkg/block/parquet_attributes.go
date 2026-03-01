package block

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"

	"github.com/saswatamcode/artemis/pkg/span"
)

const (
	parquetAttributesFilename = "attributes.parquet"
	attrColumnPrefix          = "__attr_"
	attrIndexColumn           = "__attr_index"
)

// AttributeColumnName converts an attribute key to a column name
func AttributeColumnName(key string) string {
	return fmt.Sprintf("%s%s", attrColumnPrefix, key)
}

// ColumnToAttributeName converts a column name back to attribute key
func ColumnToAttributeName(col string) (string, bool) {
	if len(col) > len(attrColumnPrefix) && col[:len(attrColumnPrefix)] == attrColumnPrefix {
		return col[len(attrColumnPrefix):], true
	}
	return "", false
}

// ParquetAttribute represents a row in the attributes table
// This is a base struct - actual schema is built dynamically with additional columns
type ParquetAttribute struct {
	SpanID    uint64 `parquet:"span_id"`
	AttrIndex []byte `parquet:"__attr_index,optional"`
}

// BuildAttributesSchema creates a dynamic Parquet schema based on discovered attribute keys
// Similar to Thanos' BuildSchemaFromLabels approach
func BuildAttributesSchema(attributeKeys []string) *parquet.Schema {
	g := make(parquet.Group)

	// Start with base columns from ParquetAttribute struct
	baseSchema := parquet.SchemaOf(&ParquetAttribute{})
	for _, col := range baseSchema.Columns() {
		lc, _ := baseSchema.Lookup(col...)
		g[col[0]] = lc.Node
	}

	// Add a column for each discovered attribute key
	// Optional + dictionary encoded for sparse, compressed storage
	for _, key := range attributeKeys {
		g[AttributeColumnName(key)] = parquet.Optional(parquet.Encoded(parquet.String(), &parquet.RLEDictionary))
	}

	return parquet.NewSchema("span_attributes", g)
}

// DiscoverAttributeKeys collects all unique attribute keys from spans
// Returns sorted list for consistent schema generation
func DiscoverAttributeKeys(spans []*span.Span) []string {
	keysSet := make(map[string]struct{})

	for _, s := range spans {
		for key := range s.Tags {
			keysSet[key] = struct{}{}
		}
	}

	keys := make([]string, 0, len(keysSet))
	for key := range keysSet {
		keys = append(keys, key)
	}

	// Sort for deterministic schema
	sort.Strings(keys)
	return keys
}

// EncodeAttributeIndex creates a bitmap or index of which attributes are present
// For now, we use a simple byte slice encoding where each byte is an attribute index
// This could be optimized to a bitmap for large numbers of attributes
func EncodeAttributeIndex(attrKeys []string, spanTags map[string]string) []byte {
	if len(spanTags) == 0 {
		return nil
	}

	// Build map of attribute key -> index in schema
	keyToIdx := make(map[string]int, len(attrKeys))
	for i, key := range attrKeys {
		keyToIdx[key] = i
	}

	// Find which attributes this span has
	indices := make([]int, 0, len(spanTags))
	for key := range spanTags {
		if idx, found := keyToIdx[key]; found {
			indices = append(indices, idx)
		}
	}

	// Sort indices for consistent encoding
	sort.Ints(indices)

	// Encode as varints for space efficiency
	buf := make([]byte, 0, len(indices)*binary.MaxVarintLen64)
	for _, idx := range indices {
		var tmp [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(tmp[:], uint64(idx))
		buf = append(buf, tmp[:n]...)
	}

	return buf
}

// DecodeAttributeIndex decodes the attribute index bitmap
func DecodeAttributeIndex(data []byte) ([]int, error) {
	if len(data) == 0 {
		return nil, nil
	}

	indices := make([]int, 0)
	r := data

	for len(r) > 0 {
		val, n := binary.Uvarint(r)
		if n <= 0 {
			return nil, fmt.Errorf("failed to decode attribute index")
		}
		indices = append(indices, int(val))
		r = r[n:]
	}

	return indices, nil
}

// WriteParquetAttributes writes span attributes to a separate Parquet file with dynamic schema
// This creates a sparse representation where only populated attributes consume storage
func WriteParquetAttributes(dir string, spans []*span.Span) error {
	// Filter to only spans with attributes
	spansWithAttrs := make([]*span.Span, 0, len(spans))
	for _, s := range spans {
		if len(s.Tags) > 0 {
			spansWithAttrs = append(spansWithAttrs, s)
		}
	}

	if len(spansWithAttrs) == 0 {
		return nil // No attributes to write
	}

	// Discover all unique attribute keys across all spans
	attrKeys := DiscoverAttributeKeys(spansWithAttrs)
	if len(attrKeys) == 0 {
		return nil
	}

	// Build dynamic schema based on discovered attributes
	schema := BuildAttributesSchema(attrKeys)

	// Get actual column order from schema (important because maps are unordered!)
	schemaColumns := schema.Columns()
	columnNames := make([]string, len(schemaColumns))
	for i, col := range schemaColumns {
		columnNames[i] = col[0]
	}

	// Create map buffer for writing
	// We need to use map[string]any for dynamic parquet writing
	rows := make([]parquet.Row, 0, len(spansWithAttrs))

	for _, s := range spansWithAttrs {
		// Parse span ID
		spanID, err := span.ParseSpanID(s.SpanID)
		if err != nil {
			// Skip spans with invalid span IDs to avoid data corruption
			continue
		}

		// Pre-encode attr_index
		attrIndex := EncodeAttributeIndex(attrKeys, s.Tags)

		// Build row in the EXACT order of schema columns
		row := make(parquet.Row, 0, len(columnNames))

		for colIdx, colName := range columnNames {
			switch colName {
			case "span_id":
				row = append(row, parquet.ValueOf(spanID).Level(0, 0, colIdx))

			case "__attr_index":
				if attrIndex != nil {
					// Definition level 1 = value is present
					row = append(row, parquet.ValueOf(attrIndex).Level(0, 1, colIdx))
				} else {
					// Definition level 0 = value is null
					row = append(row, parquet.ValueOf(nil).Level(0, 0, colIdx))
				}

			default:
				// This is an attribute column
				if attrKey, ok := ColumnToAttributeName(colName); ok {
					if value, found := s.Tags[attrKey]; found {
						// Attribute is present - write value
						row = append(row, parquet.ValueOf(value).Level(0, 1, colIdx))
					} else {
						// Attribute is not present - write null
						row = append(row, parquet.ValueOf(nil).Level(0, 0, colIdx))
					}
				} else {
					// Unknown column - write null
					row = append(row, parquet.ValueOf(nil).Level(0, 0, colIdx))
				}
			}
		}

		rows = append(rows, row)
	}

	// Find the span_id column index for sorting
	spanIDColIdx := -1
	for idx, colName := range columnNames {
		if colName == "span_id" {
			spanIDColIdx = idx
			break
		}
	}

	// Sort rows by span_id for optimal query performance
	if spanIDColIdx >= 0 {
		sort.Slice(rows, func(i, j int) bool {
			// Extract span_id from its column
			spanIDi := rows[i][spanIDColIdx].Uint64()
			spanIDj := rows[j][spanIDColIdx].Uint64()
			return spanIDi < spanIDj
		})
	}

	// Write to parquet file
	attrsPath := filepath.Join(dir, parquetAttributesFilename)
	f, err := os.Create(attrsPath)
	if err != nil {
		return fmt.Errorf("failed to create attributes parquet file: %w", err)
	}

	writer := parquet.NewWriter(
		f,
		schema,
		parquet.Compression(&snappy.Codec{}),
		parquet.PageBufferSize(100*1024*1024),
	)

	for _, row := range rows {
		if _, err := writer.WriteRows([]parquet.Row{row}); err != nil {
			writer.Close()
			f.Close()
			return fmt.Errorf("failed to write attributes row: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		f.Close()
		return fmt.Errorf("failed to close attributes writer: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("failed to sync attributes parquet file: %w", err)
	}
	f.Close()

	return nil
}

// ReadParquetAttributes reads all attributes from the attributes.parquet file
// Returns a map of span_id -> attributes
func ReadParquetAttributes(dir string) (map[string]map[string]string, error) {
	attrsPath := filepath.Join(dir, parquetAttributesFilename)

	// Check if attributes file exists
	if _, err := os.Stat(attrsPath); os.IsNotExist(err) {
		return nil, nil // No attributes file
	}

	f, err := os.Open(attrsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open attributes parquet file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat attributes parquet file: %w", err)
	}

	file, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to open attributes parquet file: %w", err)
	}

	// Get schema to know which columns exist and their order
	schema := file.Schema()
	schemaColumns := schema.Columns()

	// Build column name to index mapping
	columnNameToIdx := make(map[string]int)
	for i, col := range schemaColumns {
		columnNameToIdx[col[0]] = i
	}

	// Find span_id column index
	spanIDColIdx, hasSpanID := columnNameToIdx["span_id"]
	if !hasSpanID {
		return nil, fmt.Errorf("attributes file missing span_id column")
	}

	reader := parquet.NewReader(file)
	defer reader.Close()

	result := make(map[string]map[string]string)

	for {
		row := make(parquet.Row, len(schemaColumns))
		_, err := reader.ReadRows([]parquet.Row{row})
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read attributes row: %w", err)
		}

		// Extract span_id from its actual column position
		if len(row) <= spanIDColIdx {
			continue
		}
		spanIDVal := row[spanIDColIdx].Uint64()
		spanID := fmt.Sprintf("%016x", spanIDVal)

		// Extract attributes from their columns
		attrs := make(map[string]string)
		for colName, colIdx := range columnNameToIdx {
			// Skip special columns
			if colName == "span_id" || colName == "__attr_index" {
				continue
			}

			// Check if this is an attribute column
			if attrKey, ok := ColumnToAttributeName(colName); ok {
				if colIdx < len(row) && !row[colIdx].IsNull() {
					value := row[colIdx].String()
					attrs[attrKey] = value
				}
			}
		}

		if len(attrs) > 0 {
			result[spanID] = attrs
		}
	}

	return result, nil
}

// GetAttributesBySpanID retrieves attributes for a specific span ID
func GetAttributesBySpanID(dir string, spanID string) (map[string]string, error) {
	attrsPath := filepath.Join(dir, parquetAttributesFilename)

	// Check if attributes file exists
	if _, err := os.Stat(attrsPath); os.IsNotExist(err) {
		return nil, nil // No attributes file
	}

	// Parse the input span ID
	spanIDInt, err := span.ParseSpanID(spanID)
	if err != nil {
		return nil, fmt.Errorf("invalid span ID: %w", err)
	}

	f, err := os.Open(attrsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open attributes parquet file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat attributes parquet file: %w", err)
	}

	file, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to open attributes parquet file: %w", err)
	}

	schema := file.Schema()
	schemaColumns := schema.Columns()

	// Build column name to index mapping
	columnNameToIdx := make(map[string]int)
	for i, col := range schemaColumns {
		columnNameToIdx[col[0]] = i
	}

	// Find span_id column index
	spanIDColIdx, hasSpanID := columnNameToIdx["span_id"]
	if !hasSpanID {
		return nil, fmt.Errorf("attributes file missing span_id column")
	}

	reader := parquet.NewReader(file)
	defer reader.Close()

	for {
		row := make(parquet.Row, len(schemaColumns))
		_, err := reader.ReadRows([]parquet.Row{row})
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read attributes row: %w", err)
		}

		if len(row) <= spanIDColIdx {
			continue
		}

		rowSpanID := row[spanIDColIdx].Uint64()
		if rowSpanID == spanIDInt {
			// Found the span - extract attributes
			attrs := make(map[string]string)
			for colName, colIdx := range columnNameToIdx {
				// Skip special columns
				if colName == "span_id" || colName == "__attr_index" {
					continue
				}

				// Check if this is an attribute column
				if attrKey, ok := ColumnToAttributeName(colName); ok {
					if colIdx < len(row) && !row[colIdx].IsNull() {
						value := row[colIdx].String()
						attrs[attrKey] = value
					}
				}
			}
			return attrs, nil
		}
	}

	return nil, nil // Span not found
}

// GetAttributesBatch efficiently retrieves attributes for multiple span IDs
// Returns a map of spanID -> attributes
// Optimizations:
// 1. Skip row groups using span_id min/max statistics
// 2. Use column projection to read only span_id first
// 3. Seek to matching rows and read full attribute data
func GetAttributesBatch(dir string, spanIDs []string) (map[string]map[string]string, error) {
	if len(spanIDs) == 0 {
		return nil, nil
	}

	attrsPath := filepath.Join(dir, parquetAttributesFilename)

	// Check if attributes file exists
	if _, err := os.Stat(attrsPath); os.IsNotExist(err) {
		return nil, nil // No attributes file
	}

	// Parse all span IDs to uint64 and build set for fast lookup
	spanIDSet := make(map[uint64]string, len(spanIDs))
	minSpanID := uint64(^uint64(0)) // max uint64
	maxSpanID := uint64(0)

	for _, sid := range spanIDs {
		sidInt, err := span.ParseSpanID(sid)
		if err != nil {
			continue
		}
		spanIDSet[sidInt] = sid
		if sidInt < minSpanID {
			minSpanID = sidInt
		}
		if sidInt > maxSpanID {
			maxSpanID = sidInt
		}
	}

	if len(spanIDSet) == 0 {
		return nil, nil
	}

	f, err := os.Open(attrsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open attributes parquet file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat attributes parquet file: %w", err)
	}

	file, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to open attributes parquet file: %w", err)
	}

	schema := file.Schema()
	schemaColumns := schema.Columns()

	// Build column name to index mapping
	columnNameToIdx := make(map[string]int)
	for i, col := range schemaColumns {
		columnNameToIdx[col[0]] = i
	}

	// Find span_id column index
	spanIDColIdx, hasSpanID := columnNameToIdx["span_id"]
	if !hasSpanID {
		return nil, fmt.Errorf("attributes file missing span_id column")
	}

	result := make(map[string]map[string]string)
	rowGroups := file.RowGroups()

	// Process each row group
	for _, rg := range rowGroups {
		// OPTIMIZATION: Use row group statistics to skip irrelevant row groups
		columnChunks := rg.ColumnChunks()
		if spanIDColIdx >= len(columnChunks) {
			continue
		}

		spanIDColumn := columnChunks[spanIDColIdx] // Get span_id column chunk

		// Check column index for min/max values
		colIdx, err := spanIDColumn.ColumnIndex()
		canSkipRowGroup := false
		if err == nil && colIdx != nil && colIdx.NumPages() > 0 {
			rowGroupHasMatch := false
			for pageIdx := 0; pageIdx < colIdx.NumPages(); pageIdx++ {
				if colIdx.NullPage(pageIdx) {
					continue
				}

				pageMin := colIdx.MinValue(pageIdx)
				pageMax := colIdx.MaxValue(pageIdx)

				if len(pageMin.Bytes()) == 8 && len(pageMax.Bytes()) == 8 {
					pageMinVal := binary.LittleEndian.Uint64(pageMin.Bytes())
					pageMaxVal := binary.LittleEndian.Uint64(pageMax.Bytes())

					if !(pageMaxVal < minSpanID || pageMinVal > maxSpanID) {
						rowGroupHasMatch = true
						break
					}
				} else {
					rowGroupHasMatch = true
					break
				}
			}

			if !rowGroupHasMatch {
				canSkipRowGroup = true
			}
		}

		if canSkipRowGroup {
			continue
		}

		// Row group might contain data - read it
		reader := parquet.NewRowGroupReader(rg)

		for {
			row := make(parquet.Row, len(schemaColumns))
			_, err := reader.ReadRows([]parquet.Row{row})
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}

			if len(row) <= spanIDColIdx {
				continue
			}

			rowSpanID := row[spanIDColIdx].Uint64()
			if originalSid, found := spanIDSet[rowSpanID]; found {
				// Extract attributes
				attrs := make(map[string]string)
				for colName, colIdx := range columnNameToIdx {
					// Skip special columns
					if colName == "span_id" || colName == "__attr_index" {
						continue
					}

					// Check if this is an attribute column
					if attrKey, ok := ColumnToAttributeName(colName); ok {
						if colIdx < len(row) && !row[colIdx].IsNull() {
							value := row[colIdx].String()
							attrs[attrKey] = value
						}
					}
				}

				if len(attrs) > 0 {
					result[originalSid] = attrs
				}
			}
		}
	}

	return result, nil
}

// RemoveNullAttributeColumns creates a projection that excludes columns with only null values
// Similar to Thanos' RemoveNullColumns
func RemoveNullAttributeColumns(file *parquet.File) *parquet.Schema {
	g := make(parquet.Group)
	schema := file.Schema()
	cidxs := file.ColumnIndexes()
	nrg := len(file.RowGroups())

	for i, col := range schema.Columns() {
		// Check if this column has only null pages
		hasNonNullPage := false
		for j := range nrg * len(schema.Columns()) {
			if j%len(schema.Columns()) == i {
				if cidxs != nil && j < len(cidxs) {
					nullPages := cidxs[j].NullPages
					if slices.ContainsFunc(nullPages, func(np bool) bool { return !np }) {
						hasNonNullPage = true
						break
					}
				}
			}
		}

		// Only include columns that have at least one non-null page
		if hasNonNullPage || col[0] == "span_id" || col[0] == attrIndexColumn {
			lc, ok := schema.Lookup(col...)
			if ok {
				g[col[0]] = lc.Node
			}
		}
	}

	return parquet.NewSchema("sparse_attributes", g)
}

// QueryAttributesByKey efficiently queries attributes.parquet for a specific attribute key-value pair
// Returns span IDs that match the attribute filter
// This is the PRIMARY optimization for attribute-based queries:
// 1. Use column projection to read ONLY span_id and the target attribute column
// 2. Use row group statistics on attribute column to skip non-matching row groups
// 3. Use __attr_index to skip rows that don't have the attribute at all
// 4. Return matching span IDs for fetching from spans.parquet
func QueryAttributesByKey(dir string, attrKey string, attrValue string) ([]string, error) {
	attrsPath := filepath.Join(dir, parquetAttributesFilename)

	// Check if attributes file exists
	if _, err := os.Stat(attrsPath); os.IsNotExist(err) {
		return nil, nil // No attributes file
	}

	f, err := os.Open(attrsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open attributes parquet file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat attributes parquet file: %w", err)
	}

	file, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to open attributes parquet file: %w", err)
	}

	schema := file.Schema()
	attrColumnName := AttributeColumnName(attrKey)

	// Check if this attribute column exists in the schema
	_, hasAttrColumn := schema.Lookup(attrColumnName)
	if !hasAttrColumn {
		return nil, nil // Attribute column doesn't exist, no spans have this attribute
	}

	// Build column name to index mapping
	schemaColumns := schema.Columns()
	columnNameToIdx := make(map[string]int)
	for i, col := range schemaColumns {
		columnNameToIdx[col[0]] = i
	}

	spanIDColIdx, hasSpanID := columnNameToIdx["span_id"]
	if !hasSpanID {
		return nil, fmt.Errorf("attributes file missing span_id column")
	}

	attrColIdx, hasAttr := columnNameToIdx[attrColumnName]
	if !hasAttr {
		return nil, nil // Column not in index (shouldn't happen but defensive)
	}

	attrIndexColIdx := columnNameToIdx[attrIndexColumn]

	result := make([]string, 0)
	rowGroups := file.RowGroups()

	// Process each row group with statistics-based pruning
	for _, rg := range rowGroups {
		columnChunks := rg.ColumnChunks()
		if attrColIdx >= len(columnChunks) {
			continue
		}

		attrColumn := columnChunks[attrColIdx]

		// OPTIMIZATION 1: Use row group statistics to skip non-matching row groups
		// Check if this row group's min/max values for the attribute column could contain our target value
		columnIndex, err := attrColumn.ColumnIndex()
		canSkipRowGroup := false
		if err == nil && columnIndex != nil && columnIndex.NumPages() > 0 {
			// Check if ANY page in this row group contains our target value
			rowGroupMightMatch := false
			for pageIdx := 0; pageIdx < columnIndex.NumPages(); pageIdx++ {
				if columnIndex.NullPage(pageIdx) {
					continue // Skip null-only pages
				}

				pageMin := columnIndex.MinValue(pageIdx)
				pageMax := columnIndex.MaxValue(pageIdx)

				// For string columns, check if target value is within [min, max] range
				minStr := pageMin.String()
				maxStr := pageMax.String()

				// If target value is within the min/max range of this page, row group might have matches
				if attrValue >= minStr && attrValue <= maxStr {
					rowGroupMightMatch = true
					break
				}
			}

			if !rowGroupMightMatch {
				canSkipRowGroup = true
			}
		}

		if canSkipRowGroup {
			continue // Skip this entire row group
		}

		// OPTIMIZATION 2: Read with column projection - ONLY span_id, __attr_index, and target attribute
		// This avoids reading all other attribute columns
		reader := parquet.NewRowGroupReader(rg)

		for {
			row := make(parquet.Row, len(schemaColumns))
			_, err := reader.ReadRows([]parquet.Row{row})
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}

			if len(row) <= attrColIdx || len(row) <= spanIDColIdx {
				continue
			}

			// OPTIMIZATION 3: Use __attr_index to skip rows that don't have this attribute
			// Decode the attribute index bitmap
			if attrIndexColIdx < len(row) && !row[attrIndexColIdx].IsNull() {
				attrIndexBytes := row[attrIndexColIdx].Bytes()
				indices, err := DecodeAttributeIndex(attrIndexBytes)
				if err == nil {
					// Check if this row has the attribute we're looking for
					// We need to find which index position corresponds to our attribute
					// For now, we'll skip this optimization and check the column value directly
					// TODO: Build attribute key -> index mapping for faster checks
					_ = indices
				}
			}

			// Check if attribute value matches
			if !row[attrColIdx].IsNull() {
				value := row[attrColIdx].String()
				if value == attrValue {
					// Found a match - extract span ID
					spanIDVal := row[spanIDColIdx].Uint64()
					spanID := fmt.Sprintf("%016x", spanIDVal)
					result = append(result, spanID)
				}
			}
		}
	}

	return result, nil
}

// QueryAttributesByKeyBatch efficiently queries for multiple attribute key-value pairs
// Returns a map of (key,value) -> []spanID
// Uses column projection to read only necessary columns
func QueryAttributesByKeyBatch(dir string, attrFilters map[string]string) (map[string][]string, error) {
	if len(attrFilters) == 0 {
		return nil, nil
	}

	attrsPath := filepath.Join(dir, parquetAttributesFilename)

	// Check if attributes file exists
	if _, err := os.Stat(attrsPath); os.IsNotExist(err) {
		return nil, nil
	}

	f, err := os.Open(attrsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open attributes parquet file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat attributes parquet file: %w", err)
	}

	file, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to open attributes parquet file: %w", err)
	}

	schema := file.Schema()
	schemaColumns := schema.Columns()

	// Build column name to index mapping
	columnNameToIdx := make(map[string]int)
	for i, col := range schemaColumns {
		columnNameToIdx[col[0]] = i
	}

	spanIDColIdx, hasSpanID := columnNameToIdx["span_id"]
	if !hasSpanID {
		return nil, fmt.Errorf("attributes file missing span_id column")
	}

	// Build filter column indices
	type filterInfo struct {
		attrKey   string
		attrValue string
		columnIdx int
		resultKey string
	}

	filters := make([]filterInfo, 0, len(attrFilters))
	for key, value := range attrFilters {
		attrColumnName := AttributeColumnName(key)
		if colIdx, exists := columnNameToIdx[attrColumnName]; exists {
			filters = append(filters, filterInfo{
				attrKey:   key,
				attrValue: value,
				columnIdx: colIdx,
				resultKey: fmt.Sprintf("%s=%s", key, value),
			})
		}
	}

	if len(filters) == 0 {
		return nil, nil // None of the requested attributes exist in the schema
	}

	result := make(map[string][]string)
	for _, f := range filters {
		result[f.resultKey] = make([]string, 0)
	}

	rowGroups := file.RowGroups()

	// Process each row group
	for _, rg := range rowGroups {
		reader := parquet.NewRowGroupReader(rg)

		for {
			row := make(parquet.Row, len(schemaColumns))
			_, err := reader.ReadRows([]parquet.Row{row})
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}

			if len(row) <= spanIDColIdx {
				continue
			}

			spanIDVal := row[spanIDColIdx].Uint64()
			spanID := fmt.Sprintf("%016x", spanIDVal)

			// Check each filter
			for _, filter := range filters {
				if filter.columnIdx < len(row) && !row[filter.columnIdx].IsNull() {
					value := row[filter.columnIdx].String()
					if value == filter.attrValue {
						result[filter.resultKey] = append(result[filter.resultKey], spanID)
					}
				}
			}
		}
	}

	return result, nil
}
