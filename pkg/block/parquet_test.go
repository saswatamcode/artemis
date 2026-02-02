package block

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// TestParquetBlock_GetSpansBatch tests efficient batch retrieval of spans
func TestParquetBlock_GetSpansBatch(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "batch-block")

	// Create test spans across multiple row groups
	now := time.Now()
	spans := make([]*span.Span, 50)
	for i := range 50 {
		spans[i] = &span.Span{
			TraceID:     "trace-1",
			SpanID:      ulid.Make().String(),
			Name:        "operation-" + string(rune(i)),
			StartTime:   now.Add(time.Duration(i) * time.Millisecond),
			EndTime:     now.Add(time.Duration(i+1) * time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "service-" + string(rune(i%3)),
			Tags: map[string]string{
				"index": string(rune(i)),
				"group": string(rune(i / 10)),
			},
		}
	}

	// Write Parquet block
	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	for _, sp := range spans {
		arrowStorage.AddSpan(sp)
	}

	meta := &BlockMeta{
		ULID:       ulid.Make(),
		MinTime:    now.UnixNano(),
		MaxTime:    now.Add(50 * time.Millisecond).UnixNano(),
		SpanCount:  int64(len(spans)),
		Version:    1,
		CreatedAt:  now,
		Compaction: &CompactionMeta{Level: 1},
	}

	err := WriteParquetBlock(blockDir, meta, spans, arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("WriteParquetBlock() error = %v", err)
	}

	pb, err := NewParquetBlock(blockDir)
	if err != nil {
		t.Fatalf("NewParquetBlock() error = %v", err)
	}
	defer pb.Close()

	tests := []struct {
		name    string
		spanIDs []string
		wantLen int
		wantErr bool
	}{
		{
			name:    "retrieve multiple spans",
			spanIDs: []string{spans[5].SpanID, spans[15].SpanID, spans[25].SpanID, spans[35].SpanID},
			wantLen: 4,
			wantErr: false,
		},
		{
			name:    "retrieve single span",
			spanIDs: []string{spans[10].SpanID},
			wantLen: 1,
			wantErr: false,
		},
		{
			name:    "retrieve all spans",
			spanIDs: extractSpanIDs(spans),
			wantLen: 50,
			wantErr: false,
		},
		{
			name:    "empty span list",
			spanIDs: []string{},
			wantLen: 0,
			wantErr: false,
		},
		{
			name:    "mix of existing and non-existing spans",
			spanIDs: []string{spans[0].SpanID, "non-existent-1", spans[10].SpanID, "non-existent-2"},
			wantLen: 2, // Only existing spans returned
			wantErr: false,
		},
		{
			name:    "only non-existing spans",
			spanIDs: []string{"non-existent-1", "non-existent-2"},
			wantLen: 0,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := pb.GetSpansBatch(tt.spanIDs)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetSpansBatch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(result) != tt.wantLen {
				t.Errorf("GetSpansBatch() returned %d spans, want %d", len(result), tt.wantLen)
				return
			}

			// Verify all returned spans are in the requested list
			requestedIDs := make(map[string]bool)
			for _, id := range tt.spanIDs {
				requestedIDs[id] = true
			}

			for _, sp := range result {
				if !requestedIDs[sp.SpanID] {
					t.Errorf("GetSpansBatch() returned unexpected span %s", sp.SpanID)
				}
			}
		})
	}
}

// TestParquetBlock_GetSpansBatch_NoIndex tests batch retrieval without index
func TestParquetBlock_GetSpansBatch_NoIndex(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "no-index-block")

	now := time.Now()
	spans := []*span.Span{
		{
			TraceID:     "trace-1",
			SpanID:      "span-1",
			Name:        "op-1",
			StartTime:   now,
			EndTime:     now.Add(time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "service",
		},
	}

	meta := &BlockMeta{
		ULID:       ulid.Make(),
		MinTime:    now.UnixNano(),
		MaxTime:    now.Add(time.Millisecond).UnixNano(),
		SpanCount:  1,
		Version:    1,
		CreatedAt:  now,
		Compaction: &CompactionMeta{Level: 1},
	}

	// Write block WITHOUT index
	err := WriteParquetBlock(blockDir, meta, spans, nil)
	if err != nil {
		t.Fatalf("WriteParquetBlock() error = %v", err)
	}

	pb, err := NewParquetBlock(blockDir)
	if err != nil {
		t.Fatalf("NewParquetBlock() error = %v", err)
	}
	defer pb.Close()

	// Should error when trying to use GetSpansBatch without index
	_, err = pb.GetSpansBatch([]string{"span-1"})
	if err == nil {
		t.Error("GetSpansBatch() should error when block has no index")
	}
}

// TestParquetBlock_ScanMetadata tests metadata-only scanning
func TestParquetBlock_ScanMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "scan-block")

	// Create test spans with varied metadata
	now := time.Now()
	spans := []*span.Span{
		{
			TraceID:     "trace-1",
			SpanID:      "span-1",
			Name:        "GET /api/users",
			StartTime:   now,
			EndTime:     now.Add(10 * time.Millisecond),
			Duration:    10_000_000,
			ServiceName: "api-gateway",
			Tags:        map[string]string{"http.method": "GET", "http.status": "200"},
		},
		{
			TraceID:     "trace-1",
			SpanID:      "span-2",
			Name:        "SELECT * FROM users",
			StartTime:   now.Add(2 * time.Millisecond),
			EndTime:     now.Add(8 * time.Millisecond),
			Duration:    6_000_000,
			ServiceName: "database",
			Tags:        map[string]string{"db.type": "postgres"},
		},
		{
			TraceID:     "trace-2",
			SpanID:      "span-3",
			Name:        "POST /api/orders",
			StartTime:   now.Add(20 * time.Millisecond),
			EndTime:     now.Add(50 * time.Millisecond),
			Duration:    30_000_000,
			ServiceName: "api-gateway",
			Tags:        map[string]string{"http.method": "POST"},
		},
		{
			TraceID:     "trace-2",
			SpanID:      "span-4",
			Name:        "cache.get",
			StartTime:   now.Add(22 * time.Millisecond),
			EndTime:     now.Add(24 * time.Millisecond),
			Duration:    2_000_000,
			ServiceName: "redis",
			Tags:        map[string]string{"cache.key": "user:123"},
		},
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	for _, sp := range spans {
		arrowStorage.AddSpan(sp)
	}

	meta := &BlockMeta{
		ULID:       ulid.Make(),
		MinTime:    now.UnixNano(),
		MaxTime:    now.Add(50 * time.Millisecond).UnixNano(),
		SpanCount:  int64(len(spans)),
		Version:    1,
		CreatedAt:  now,
		Compaction: &CompactionMeta{Level: 1},
	}

	err := WriteParquetBlock(blockDir, meta, spans, arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("WriteParquetBlock() error = %v", err)
	}

	pb, err := NewParquetBlock(blockDir)
	if err != nil {
		t.Fatalf("NewParquetBlock() error = %v", err)
	}
	defer pb.Close()

	tests := []struct {
		name       string
		filterFunc func(*ParquetSpanMetadata) bool
		wantCount  int
	}{
		{
			name: "filter by trace_id",
			filterFunc: func(meta *ParquetSpanMetadata) bool {
				return meta.TraceID == "trace-1"
			},
			wantCount: 2,
		},
		{
			name: "filter by service_name",
			filterFunc: func(meta *ParquetSpanMetadata) bool {
				return meta.ServiceName == "api-gateway"
			},
			wantCount: 2,
		},
		{
			name: "filter by name prefix",
			filterFunc: func(meta *ParquetSpanMetadata) bool {
				return len(meta.Name) > 0 && meta.Name[0] == 'P'
			},
			wantCount: 1,
		},
		{
			name: "filter by duration > 5ms",
			filterFunc: func(meta *ParquetSpanMetadata) bool {
				return meta.Duration > 5_000_000
			},
			wantCount: 3,
		},
		{
			name: "filter by time range",
			filterFunc: func(meta *ParquetSpanMetadata) bool {
				return meta.StartTime >= now.Add(20*time.Millisecond).UnixNano()
			},
			wantCount: 2,
		},
		{
			name: "no matches",
			filterFunc: func(meta *ParquetSpanMetadata) bool {
				return meta.ServiceName == "non-existent"
			},
			wantCount: 0,
		},
		{
			name: "match all",
			filterFunc: func(meta *ParquetSpanMetadata) bool {
				return true
			},
			wantCount: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			refs, err := pb.ScanMetadata(tt.filterFunc)
			if err != nil {
				t.Fatalf("ScanMetadata() error = %v", err)
			}

			if len(refs) != tt.wantCount {
				t.Errorf("ScanMetadata() returned %d refs, want %d", len(refs), tt.wantCount)
			}

			// Verify references are valid
			for _, ref := range refs {
				if ref.RowGroupIdx < 0 {
					t.Errorf("Invalid RowGroupIdx: %d", ref.RowGroupIdx)
				}
				if ref.RowIdx < 0 {
					t.Errorf("Invalid RowIdx: %d", ref.RowIdx)
				}
			}
		})
	}
}

// TestParquetBlock_GetSpansByRowReferences tests fetching spans by row references
func TestParquetBlock_GetSpansByRowReferences(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "refs-block")

	now := time.Now()
	spans := make([]*span.Span, 20)
	for i := range 20 {
		spans[i] = &span.Span{
			TraceID:     "trace-" + string(rune(i/5)),
			SpanID:      "span-" + string(rune(i)),
			Name:        "operation-" + string(rune(i)),
			StartTime:   now.Add(time.Duration(i) * time.Millisecond),
			EndTime:     now.Add(time.Duration(i+1) * time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "service-" + string(rune(i%3)),
			Tags:        map[string]string{"index": string(rune(i))},
		}
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	for _, sp := range spans {
		arrowStorage.AddSpan(sp)
	}

	meta := &BlockMeta{
		ULID:       ulid.Make(),
		MinTime:    now.UnixNano(),
		MaxTime:    now.Add(20 * time.Millisecond).UnixNano(),
		SpanCount:  int64(len(spans)),
		Version:    1,
		CreatedAt:  now,
		Compaction: &CompactionMeta{Level: 1},
	}

	err := WriteParquetBlock(blockDir, meta, spans, arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("WriteParquetBlock() error = %v", err)
	}

	pb, err := NewParquetBlock(blockDir)
	if err != nil {
		t.Fatalf("NewParquetBlock() error = %v", err)
	}
	defer pb.Close()

	// First use ScanMetadata to get references
	refs, err := pb.ScanMetadata(func(meta *ParquetSpanMetadata) bool {
		return meta.TraceID == "trace-\x01" // spans 5-9
	})
	if err != nil {
		t.Fatalf("ScanMetadata() error = %v", err)
	}

	if len(refs) != 5 {
		t.Fatalf("ScanMetadata() returned %d refs, want 5", len(refs))
	}

	// Now fetch those spans
	result, err := pb.GetSpansByRowReferences(refs)
	if err != nil {
		t.Fatalf("GetSpansByRowReferences() error = %v", err)
	}

	if len(result) != 5 {
		t.Errorf("GetSpansByRowReferences() returned %d spans, want 5", len(result))
	}

	// Verify all returned spans have correct trace_id
	for _, sp := range result {
		if sp.TraceID != "trace-\x01" {
			t.Errorf("GetSpansByRowReferences() returned span with trace_id %s, want trace-\\x01", sp.TraceID)
		}
	}

	// Test with empty refs
	emptyResult, err := pb.GetSpansByRowReferences([]RowReference{})
	if err != nil {
		t.Errorf("GetSpansByRowReferences() with empty refs error = %v", err)
	}
	if len(emptyResult) != 0 {
		t.Errorf("GetSpansByRowReferences() with empty refs returned %d spans, want 0", len(emptyResult))
	}
}

// TestParquetBlock_ScanAndFetch tests the complete scan->fetch pipeline
func TestParquetBlock_ScanAndFetch(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "pipeline-block")

	now := time.Now()
	spans := []*span.Span{
		{
			TraceID:     "trace-prod",
			SpanID:      "span-1",
			Name:        "GET /health",
			StartTime:   now,
			EndTime:     now.Add(time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "api",
			Tags:        map[string]string{"env": "prod"},
		},
		{
			TraceID:     "trace-dev",
			SpanID:      "span-2",
			Name:        "GET /health",
			StartTime:   now,
			EndTime:     now.Add(time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "api",
			Tags:        map[string]string{"env": "dev"},
		},
		{
			TraceID:     "trace-prod",
			SpanID:      "span-3",
			Name:        "POST /api/data",
			StartTime:   now,
			EndTime:     now.Add(10 * time.Millisecond),
			Duration:    10_000_000,
			ServiceName: "api",
			Tags:        map[string]string{"env": "prod", "method": "POST"},
		},
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	for _, sp := range spans {
		arrowStorage.AddSpan(sp)
	}

	meta := &BlockMeta{
		ULID:       ulid.Make(),
		MinTime:    now.UnixNano(),
		MaxTime:    now.Add(10 * time.Millisecond).UnixNano(),
		SpanCount:  3,
		Version:    1,
		CreatedAt:  now,
		Compaction: &CompactionMeta{Level: 1},
	}

	err := WriteParquetBlock(blockDir, meta, spans, arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("WriteParquetBlock() error = %v", err)
	}

	pb, err := NewParquetBlock(blockDir)
	if err != nil {
		t.Fatalf("NewParquetBlock() error = %v", err)
	}
	defer pb.Close()

	// Step 1: Scan metadata to find prod traces
	refs, err := pb.ScanMetadata(func(meta *ParquetSpanMetadata) bool {
		return meta.TraceID == "trace-prod"
	})
	if err != nil {
		t.Fatalf("ScanMetadata() error = %v", err)
	}

	if len(refs) != 2 {
		t.Fatalf("Expected 2 prod spans, got %d", len(refs))
	}

	// Step 2: Fetch full spans
	prodSpans, err := pb.GetSpansByRowReferences(refs)
	if err != nil {
		t.Fatalf("GetSpansByRowReferences() error = %v", err)
	}

	if len(prodSpans) != 2 {
		t.Fatalf("Expected 2 prod spans, got %d", len(prodSpans))
	}

	// Step 3: Verify we can now check tags (which aren't in metadata)
	foundPOST := false
	for _, sp := range prodSpans {
		if sp.Tags["method"] == "POST" {
			foundPOST = true
		}
		if sp.Tags["env"] != "prod" {
			t.Errorf("Expected env=prod, got %s", sp.Tags["env"])
		}
	}

	if !foundPOST {
		t.Error("Should have found POST span in prod traces")
	}
}

// extractSpanIDs is a helper to extract span IDs from a slice of spans
func extractSpanIDs(spans []*span.Span) []string {
	ids := make([]string, len(spans))
	for i, sp := range spans {
		ids[i] = sp.SpanID
	}
	return ids
}
