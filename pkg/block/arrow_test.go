package block

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// TestArrowBlock_RecordsIteration tests iterating over Arrow records
func TestArrowBlock_RecordsIteration(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "arrow-records-block")

	// Create test spans
	now := time.Now()
	spans := []*span.Span{
		{
			TraceID:      "trace-1",
			SpanID:       "span-1",
			ParentSpanID: "",
			Name:         "root-span",
			StartTime:    now,
			EndTime:      now.Add(10 * time.Millisecond),
			Duration:     10_000_000,
			ServiceName:  "api-gateway",
			Tags: map[string]string{
				"http.method": "GET",
				"http.path":   "/api/users",
			},
		},
		{
			TraceID:      "trace-1",
			SpanID:       "span-2",
			ParentSpanID: "span-1",
			Name:         "db-query",
			StartTime:    now.Add(2 * time.Millisecond),
			EndTime:      now.Add(8 * time.Millisecond),
			Duration:     6_000_000,
			ServiceName:  "database",
			Tags: map[string]string{
				"db.type":      "postgres",
				"db.statement": "SELECT * FROM users",
			},
		},
		{
			TraceID:      "trace-2",
			SpanID:       "span-3",
			ParentSpanID: "",
			Name:         "cache-lookup",
			StartTime:    now.Add(15 * time.Millisecond),
			EndTime:      now.Add(16 * time.Millisecond),
			Duration:     1_000_000,
			ServiceName:  "redis",
			Tags: map[string]string{
				"cache.key": "user:123",
			},
		},
	}

	// Create Arrow block
	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	for _, sp := range spans {
		arrowStorage.AddSpan(sp)
	}
	arrowStorage.Flush()

	meta := &BlockMeta{
		ULID:      ulid.Make(),
		MinTime:   now.UnixNano(),
		MaxTime:   now.Add(16 * time.Millisecond).UnixNano(),
		SpanCount: int64(len(spans)),
		Version:   1,
		CreatedAt: now,
	}

	err := FlushBlock(blockDir, meta, arrowStorage.GetRecords(), arrowStorage.Schema(), arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("FlushBlock() error = %v", err)
	}

	// Load Arrow block
	ab, err := NewArrowBlock(blockDir)
	if err != nil {
		t.Fatalf("NewArrowBlock() error = %v", err)
	}
	defer ab.Close()

	// Test Records()
	records := ab.Records()
	if len(records) == 0 {
		t.Fatal("Records() should return at least one record")
	}

	// Iterate over records and extract all spans
	var extractedSpans []*span.Span
	for _, record := range records {
		for row := 0; row < int(record.NumRows()); row++ {
			sp, err := extractSpanFromArrowRecord(record, row)
			if err != nil {
				t.Errorf("extractSpanFromArrowRecord() error = %v", err)
				continue
			}
			extractedSpans = append(extractedSpans, sp)
		}
	}

	// Verify we got all spans
	if len(extractedSpans) != len(spans) {
		t.Errorf("Extracted %d spans, want %d", len(extractedSpans), len(spans))
	}

	// Verify span data integrity
	for i, original := range spans {
		extracted := extractedSpans[i]

		if extracted.TraceID != original.TraceID {
			t.Errorf("Span %d: TraceID = %s, want %s", i, extracted.TraceID, original.TraceID)
		}
		if extracted.SpanID != original.SpanID {
			t.Errorf("Span %d: SpanID = %s, want %s", i, extracted.SpanID, original.SpanID)
		}
		if extracted.ParentSpanID != original.ParentSpanID {
			t.Errorf("Span %d: ParentSpanID = %s, want %s", i, extracted.ParentSpanID, original.ParentSpanID)
		}
		if extracted.Name != original.Name {
			t.Errorf("Span %d: Name = %s, want %s", i, extracted.Name, original.Name)
		}
		if extracted.ServiceName != original.ServiceName {
			t.Errorf("Span %d: ServiceName = %s, want %s", i, extracted.ServiceName, original.ServiceName)
		}
		if extracted.Duration != original.Duration {
			t.Errorf("Span %d: Duration = %d, want %d", i, extracted.Duration, original.Duration)
		}

		// Verify tags
		if len(extracted.Tags) != len(original.Tags) {
			t.Errorf("Span %d: Tags count = %d, want %d", i, len(extracted.Tags), len(original.Tags))
		}
		for key, expectedVal := range original.Tags {
			if actualVal, ok := extracted.Tags[key]; !ok {
				t.Errorf("Span %d: Missing tag %s", i, key)
			} else if actualVal != expectedVal {
				t.Errorf("Span %d: Tag[%s] = %s, want %s", i, key, actualVal, expectedVal)
			}
		}
	}
}

// TestArrowBlock_Schema tests Arrow schema access
func TestArrowBlock_Schema(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "arrow-schema-block")

	now := time.Now()
	spans := []*span.Span{
		{
			TraceID:     "trace-1",
			SpanID:      "span-1",
			Name:        "test-op",
			StartTime:   now,
			EndTime:     now.Add(time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "test-service",
			Tags:        map[string]string{"key": "value"},
		},
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	arrowStorage.AddSpan(spans[0])
	arrowStorage.Flush()

	meta := &BlockMeta{
		ULID:      ulid.Make(),
		MinTime:   now.UnixNano(),
		MaxTime:   now.Add(time.Millisecond).UnixNano(),
		SpanCount: 1,
		Version:   1,
		CreatedAt: now,
	}

	err := FlushBlock(blockDir, meta, arrowStorage.GetRecords(), arrowStorage.Schema(), arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("FlushBlock() error = %v", err)
	}

	ab, err := NewArrowBlock(blockDir)
	if err != nil {
		t.Fatalf("NewArrowBlock() error = %v", err)
	}
	defer ab.Close()

	// Test Schema()
	schema := ab.Schema()
	if schema == nil {
		t.Fatal("Schema() should not return nil for Arrow blocks")
	}

	// Verify schema has expected fields
	expectedFields := []string{
		"trace_id",
		"span_id",
		"parent_span_id",
		"name",
		"start_time",
		"end_time",
		"duration",
		"service_name",
		"tags",
	}

	if schema.NumFields() != len(expectedFields) {
		t.Errorf("Schema has %d fields, want %d", schema.NumFields(), len(expectedFields))
	}

	for i, expectedName := range expectedFields {
		field := schema.Field(i)
		if field.Name != expectedName {
			t.Errorf("Field %d: name = %s, want %s", i, field.Name, expectedName)
		}
	}
}

// TestArrowBlock_IndexLookup tests using the index to find spans
func TestArrowBlock_IndexLookup(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "arrow-index-block")

	now := time.Now()
	spans := []*span.Span{
		{
			TraceID:     "trace-alpha",
			SpanID:      "span-a1",
			Name:        "operation-1",
			StartTime:   now,
			EndTime:     now.Add(time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "service-1",
			Tags:        map[string]string{"env": "prod"},
		},
		{
			TraceID:     "trace-alpha",
			SpanID:      "span-a2",
			Name:        "operation-2",
			StartTime:   now.Add(time.Millisecond),
			EndTime:     now.Add(2 * time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "service-2",
			Tags:        map[string]string{"env": "prod"},
		},
		{
			TraceID:     "trace-beta",
			SpanID:      "span-b1",
			Name:        "operation-3",
			StartTime:   now.Add(2 * time.Millisecond),
			EndTime:     now.Add(3 * time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "service-1",
			Tags:        map[string]string{"env": "dev"},
		},
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	for _, sp := range spans {
		arrowStorage.AddSpan(sp)
	}
	arrowStorage.Flush()

	meta := &BlockMeta{
		ULID:      ulid.Make(),
		MinTime:   now.UnixNano(),
		MaxTime:   now.Add(3 * time.Millisecond).UnixNano(),
		SpanCount: int64(len(spans)),
		Version:   1,
		CreatedAt: now,
	}

	err := FlushBlock(blockDir, meta, arrowStorage.GetRecords(), arrowStorage.Schema(), arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("FlushBlock() error = %v", err)
	}

	ab, err := NewArrowBlock(blockDir)
	if err != nil {
		t.Fatalf("NewArrowBlock() error = %v", err)
	}
	defer ab.Close()

	// Test index access
	idx := ab.Index()
	if idx == nil {
		t.Fatal("Index() should not return nil")
	}

	if !ab.HasIndex() {
		t.Error("HasIndex() should return true")
	}

	// Test span ID lookups
	for _, sp := range spans {
		ref, ok := idx.LookupSpanID(sp.SpanID)
		if !ok {
			t.Errorf("LookupSpanID(%s) should find span", sp.SpanID)
			continue
		}

		// Use the reference to extract the span
		records := ab.Records()
		if ref.RecordIndex >= len(records) {
			t.Errorf("Invalid record index %d for span %s", ref.RecordIndex, sp.SpanID)
			continue
		}

		extractedSpan, err := extractSpanFromArrowRecord(records[ref.RecordIndex], ref.RowIndex)
		if err != nil {
			t.Errorf("extractSpanFromArrowRecord() error = %v", err)
			continue
		}

		if extractedSpan.SpanID != sp.SpanID {
			t.Errorf("Extracted wrong span: got %s, want %s", extractedSpan.SpanID, sp.SpanID)
		}
	}

	// Test trace ID lookup
	traceAlphaSpans := idx.LookupByTraceID("trace-alpha")
	if len(traceAlphaSpans) != 2 {
		t.Errorf("LookupByTraceID(trace-alpha) returned %d spans, want 2", len(traceAlphaSpans))
	}

	traceBetaSpans := idx.LookupByTraceID("trace-beta")
	if len(traceBetaSpans) != 1 {
		t.Errorf("LookupByTraceID(trace-beta) returned %d spans, want 1", len(traceBetaSpans))
	}

	// Test tag lookup
	envProdSpans := idx.LookupByTag("env", "prod")
	if len(envProdSpans) != 2 {
		t.Errorf("LookupByTag(env, prod) returned %d spans, want 2", len(envProdSpans))
	}

	envDevSpans := idx.LookupByTag("env", "dev")
	if len(envDevSpans) != 1 {
		t.Errorf("LookupByTag(env, dev) returned %d spans, want 1", len(envDevSpans))
	}
}

// TestArrowBlock_MultipleRecords tests blocks with multiple record batches
func TestArrowBlock_MultipleRecords(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "arrow-multi-record-block")

	// Create enough spans to trigger multiple record batches (batch size is 1024)
	now := time.Now()
	numSpans := 2500 // This will create at least 2 record batches
	spans := make([]*span.Span, numSpans)

	for i := range numSpans {
		spans[i] = &span.Span{
			TraceID:     "trace-bulk",
			SpanID:      ulid.Make().String(),
			Name:        "bulk-operation",
			StartTime:   now.Add(time.Duration(i) * time.Microsecond),
			EndTime:     now.Add(time.Duration(i+1) * time.Microsecond),
			Duration:    1000,
			ServiceName: "bulk-service",
			Tags: map[string]string{
				"batch": "test",
				"index": string(rune(i % 256)),
			},
		}
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	for _, sp := range spans {
		arrowStorage.AddSpan(sp)
	}
	arrowStorage.Flush()

	meta := &BlockMeta{
		ULID:      ulid.Make(),
		MinTime:   now.UnixNano(),
		MaxTime:   now.Add(time.Duration(numSpans) * time.Microsecond).UnixNano(),
		SpanCount: int64(numSpans),
		Version:   1,
		CreatedAt: now,
	}

	err := FlushBlock(blockDir, meta, arrowStorage.GetRecords(), arrowStorage.Schema(), arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("FlushBlock() error = %v", err)
	}

	ab, err := NewArrowBlock(blockDir)
	if err != nil {
		t.Fatalf("NewArrowBlock() error = %v", err)
	}
	defer ab.Close()

	// Verify we have multiple records
	records := ab.Records()
	if len(records) < 2 {
		t.Errorf("Expected at least 2 record batches for %d spans, got %d", numSpans, len(records))
	}

	// Count total rows across all records
	totalRows := 0
	for _, record := range records {
		totalRows += int(record.NumRows())
	}

	if totalRows != numSpans {
		t.Errorf("Total rows = %d, want %d", totalRows, numSpans)
	}

	// Verify index can find spans across different record batches
	idx := ab.Index()
	foundCount := 0
	for _, sp := range spans {
		if _, ok := idx.LookupSpanID(sp.SpanID); ok {
			foundCount++
		}
	}

	if foundCount != numSpans {
		t.Errorf("Index found %d spans, want %d", foundCount, numSpans)
	}
}

// TestArrowBlock_EmptyTags tests handling of spans with empty or nil tags
func TestArrowBlock_EmptyTags(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "arrow-empty-tags-block")

	now := time.Now()
	spans := []*span.Span{
		{
			TraceID:     "trace-1",
			SpanID:      "span-1",
			Name:        "no-tags",
			StartTime:   now,
			EndTime:     now.Add(time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "service",
			Tags:        map[string]string{}, // Empty tags
		},
		{
			TraceID:     "trace-1",
			SpanID:      "span-2",
			Name:        "nil-tags",
			StartTime:   now,
			EndTime:     now.Add(time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "service",
			Tags:        nil, // Nil tags
		},
		{
			TraceID:     "trace-1",
			SpanID:      "span-3",
			Name:        "with-tags",
			StartTime:   now,
			EndTime:     now.Add(time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "service",
			Tags:        map[string]string{"key": "value"},
		},
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	for _, sp := range spans {
		arrowStorage.AddSpan(sp)
	}
	arrowStorage.Flush()

	meta := &BlockMeta{
		ULID:      ulid.Make(),
		MinTime:   now.UnixNano(),
		MaxTime:   now.Add(time.Millisecond).UnixNano(),
		SpanCount: 3,
		Version:   1,
		CreatedAt: now,
	}

	err := FlushBlock(blockDir, meta, arrowStorage.GetRecords(), arrowStorage.Schema(), arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("FlushBlock() error = %v", err)
	}

	ab, err := NewArrowBlock(blockDir)
	if err != nil {
		t.Fatalf("NewArrowBlock() error = %v", err)
	}
	defer ab.Close()

	// Extract all spans
	records := ab.Records()
	var extractedSpans []*span.Span
	for _, record := range records {
		for row := 0; row < int(record.NumRows()); row++ {
			sp, err := extractSpanFromArrowRecord(record, row)
			if err != nil {
				t.Fatalf("extractSpanFromArrowRecord() error = %v", err)
			}
			extractedSpans = append(extractedSpans, sp)
		}
	}

	if len(extractedSpans) != 3 {
		t.Fatalf("Expected 3 spans, got %d", len(extractedSpans))
	}

	// Verify empty/nil tags are handled correctly
	for i, sp := range extractedSpans {
		if i < 2 {
			// First two spans should have no tags (nil or empty)
			if len(sp.Tags) > 0 {
				t.Errorf("Span %d should have empty/nil tags, got %d tags", i, len(sp.Tags))
			}
		} else {
			// Third span should have one tag
			if len(sp.Tags) != 1 {
				t.Errorf("Span %d should have 1 tag, got %d", i, len(sp.Tags))
			}
		}
	}
}

// TestArrowBlock_Close tests resource cleanup
func TestArrowBlock_Close(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "arrow-close-block")

	now := time.Now()
	spans := []*span.Span{
		{
			TraceID:     "trace-1",
			SpanID:      "span-1",
			Name:        "test",
			StartTime:   now,
			EndTime:     now.Add(time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "service",
		},
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	arrowStorage.AddSpan(spans[0])
	arrowStorage.Flush()

	meta := &BlockMeta{
		ULID:      ulid.Make(),
		MinTime:   now.UnixNano(),
		MaxTime:   now.Add(time.Millisecond).UnixNano(),
		SpanCount: 1,
		Version:   1,
		CreatedAt: now,
	}

	err := FlushBlock(blockDir, meta, arrowStorage.GetRecords(), arrowStorage.Schema(), arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("FlushBlock() error = %v", err)
	}

	ab, err := NewArrowBlock(blockDir)
	if err != nil {
		t.Fatalf("NewArrowBlock() error = %v", err)
	}

	// Verify block works before closing
	records := ab.Records()
	if len(records) == 0 {
		t.Error("Should have records before Close()")
	}

	// Close the block
	err = ab.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// After close, records should be nil
	records = ab.Records()
	if records != nil {
		t.Error("Records() should return nil after Close()")
	}
}

// TestArrowBlock_Metadata tests block metadata access
func TestArrowBlock_Metadata(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "arrow-meta-block")

	now := time.Now()
	blockID := ulid.Make()
	spans := []*span.Span{
		{
			TraceID:     "trace-1",
			SpanID:      "span-1",
			Name:        "test",
			StartTime:   now,
			EndTime:     now.Add(5 * time.Millisecond),
			Duration:    5_000_000,
			ServiceName: "service",
		},
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	arrowStorage.AddSpan(spans[0])
	arrowStorage.Flush()

	meta := &BlockMeta{
		ULID:      blockID,
		MinTime:   now.UnixNano(),
		MaxTime:   now.Add(5 * time.Millisecond).UnixNano(),
		SpanCount: 1,
		Version:   1,
		CreatedAt: now,
	}

	err := FlushBlock(blockDir, meta, arrowStorage.GetRecords(), arrowStorage.Schema(), arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("FlushBlock() error = %v", err)
	}

	ab, err := NewArrowBlock(blockDir)
	if err != nil {
		t.Fatalf("NewArrowBlock() error = %v", err)
	}
	defer ab.Close()

	// Test Meta()
	loadedMeta := ab.Meta()
	if loadedMeta == nil {
		t.Fatal("Meta() should not return nil")
	}

	if loadedMeta.ULID != blockID {
		t.Errorf("ULID = %s, want %s", loadedMeta.ULID, blockID)
	}

	if loadedMeta.SpanCount != 1 {
		t.Errorf("SpanCount = %d, want 1", loadedMeta.SpanCount)
	}

	if loadedMeta.Version != 1 {
		t.Errorf("Version = %d, want 1", loadedMeta.Version)
	}

	if loadedMeta.Level() != 0 {
		t.Errorf("Level() = %d, want 0 (L0 block)", loadedMeta.Level())
	}

	// Test Dir()
	if ab.Dir() != blockDir {
		t.Errorf("Dir() = %s, want %s", ab.Dir(), blockDir)
	}
}
