package query

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// Type aliases for convenience
type (
	Block          = block.Block
	BlockMeta      = block.BlockMeta
	CompactionMeta = block.CompactionMeta
)

func TestNewMatcher(t *testing.T) {
	tests := []struct {
		name      string
		matchType MatchType
		key       string
		value     string
		wantError bool
	}{
		{"valid equal", MatchEqual, "key", "value", false},
		{"valid not equal", MatchNotEqual, "key", "value", false},
		{"valid regexp", MatchRegexp, "key", "val.*", false},
		{"invalid regexp", MatchRegexp, "key", "[invalid", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewMatcher(tt.matchType, tt.key, tt.value)
			if (err != nil) != tt.wantError {
				t.Errorf("NewMatcher() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}

func TestMatcher_Matches(t *testing.T) {
	tests := []struct {
		name       string
		matchType  MatchType
		matchKey   string
		matchValue string
		spanTags   map[string]string
		want       bool
	}{
		{
			"equal match",
			MatchEqual, "env", "prod",
			map[string]string{"env": "prod"},
			true,
		},
		{
			"equal no match",
			MatchEqual, "env", "prod",
			map[string]string{"env": "dev"},
			false,
		},
		{
			"not equal match",
			MatchNotEqual, "env", "prod",
			map[string]string{"env": "dev"},
			true,
		},
		{
			"not equal no match",
			MatchNotEqual, "env", "prod",
			map[string]string{"env": "prod"},
			false,
		},
		{
			"regexp match",
			MatchRegexp, "status", "5.*",
			map[string]string{"status": "500"},
			true,
		},
		{
			"regexp no match",
			MatchRegexp, "status", "5.*",
			map[string]string{"status": "200"},
			false,
		},
		{
			"missing key",
			MatchEqual, "missing", "value",
			map[string]string{"env": "prod"},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher, err := NewMatcher(tt.matchType, tt.matchKey, tt.matchValue)
			if err != nil {
				t.Fatalf("NewMatcher() error = %v", err)
			}

			testSpan := &span.Span{
				SpanID:      "0000000000000101",
				StartTime:   time.Now(),
				EndTime:     time.Now().Add(time.Millisecond),
				ServiceName: "service",
				Tags:        tt.spanTags,
			}

			got := matcher.Matches(testSpan)
			if got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMatcher_TopLevelFields(t *testing.T) {
	tests := []struct {
		name       string
		matchKey   string
		matchValue string
		span       *span.Span
		want       bool
	}{
		{
			"match trace_id",
			"trace_id", "trace-123",
			&span.Span{TraceID: "trace-123", SpanID: "0000000000000102"},
			true,
		},
		{
			"no match trace_id",
			"trace_id", "trace-123",
			&span.Span{TraceID: "trace-456", SpanID: "0000000000000102"},
			false,
		},
		{
			"match span_id",
			"span_id", "0000000000000199",
			&span.Span{TraceID: "trace-1", SpanID: "0000000000000199"},
			true,
		},
		{
			"match parent_span_id",
			"parent_span_id", "0000000000000108",
			&span.Span{TraceID: "trace-1", SpanID: "0000000000000102", ParentSpanID: "0000000000000108"},
			true,
		},
		{
			"match name",
			"name", "GET /api/users",
			&span.Span{TraceID: "trace-1", SpanID: "0000000000000102", Name: "GET /api/users"},
			true,
		},
		{
			"match service.name",
			"service.name", "payment-service",
			&span.Span{TraceID: "trace-1", SpanID: "0000000000000102", ServiceName: "payment-service"},
			true,
		},
		{
			"match service_name",
			"service_name", "auth-service",
			&span.Span{TraceID: "trace-1", SpanID: "0000000000000102", ServiceName: "auth-service"},
			true,
		},
		{
			"fallback to tags",
			"custom_tag", "custom_value",
			&span.Span{
				TraceID:     "trace-1",
				SpanID:      "0000000000000102",
				ServiceName: "service",
				Tags:        map[string]string{"custom_tag": "custom_value"},
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher, err := NewMatcher(MatchEqual, tt.matchKey, tt.matchValue)
			if err != nil {
				t.Fatalf("NewMatcher() error = %v", err)
			}

			got := matcher.Matches(tt.span)
			if got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSelect(t *testing.T) {
	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()

	// Add test spans
	spans := []*span.Span{
		{
			SpanID:      "0000000000000102",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service-a",
			Tags:        map[string]string{"env": "prod", "version": "1.0"},
		},
		{
			SpanID:      "0000000000000103",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service-b",
			Tags:        map[string]string{"env": "dev", "version": "2.0"},
		},
		{
			SpanID:      "0000000000000104",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service-a",
			Tags:        map[string]string{"env": "prod", "version": "2.0"},
		},
	}

	for _, sp := range spans {
		arrowStorage.AddSpan(sp)
	}
	arrowStorage.Flush()

	// Test single matcher
	matcher, _ := NewMatcher(MatchEqual, "env", "prod")
	results, err := Select(arrowStorage, matcher)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}

	if len(results.Spans) != 2 {
		t.Errorf("Select() returned %d results, want 2", len(results.Spans))
	}

	// Test multiple matchers
	m1, _ := NewMatcher(MatchEqual, "env", "prod")
	m2, _ := NewMatcher(MatchEqual, "version", "2.0")
	results, err = Select(arrowStorage, m1, m2)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}

	if len(results.Spans) != 1 {
		t.Errorf("Select() with multiple matchers returned %d results, want 1", len(results.Spans))
	}
	if results.Spans[0].SpanID != "0000000000000104" {
		t.Errorf("Expected 0000000000000104, got %s", results.Spans[0].SpanID)
	}
}

func TestNewTimeRange(t *testing.T) {
	start := time.Unix(0, 0)
	end := time.Unix(0, 1000)

	tr := NewTimeRange(start, end)

	if tr.Start != start {
		t.Errorf("TimeRange.Start = %v, want %v", tr.Start, start)
	}
	if tr.End != end {
		t.Errorf("TimeRange.End = %v, want %v", tr.End, end)
	}
}

func TestSelectFromBlocksWithTimeRange(t *testing.T) {
	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()

	now := time.Now()

	// Add spans at different times
	spans := []*span.Span{
		{
			SpanID:    "0000000000000105",
			StartTime: now.Add(-2 * time.Hour),
			EndTime:   now.Add(-2 * time.Hour).Add(time.Millisecond),
			Tags:      map[string]string{"env": "prod"},
		},
		{
			SpanID:    "0000000000000106",
			StartTime: now.Add(-5 * time.Minute),
			EndTime:   now.Add(-5 * time.Minute).Add(time.Millisecond),
			Tags:      map[string]string{"env": "prod"},
		},
		{
			SpanID:    "0000000000000107",
			StartTime: now.Add(-1 * time.Minute),
			EndTime:   now.Add(-1 * time.Minute).Add(time.Millisecond),
			Tags:      map[string]string{"env": "prod"},
		},
	}

	for _, sp := range spans {
		arrowStorage.AddSpan(sp)
	}
	arrowStorage.Flush()

	// Query last 10 minutes
	timeRange := NewTimeRange(now.Add(-10*time.Minute), now)
	matcher, _ := NewMatcher(MatchEqual, "env", "prod")

	results, err := SelectFromBlocksWithTimeRange(block.NewHeadBlock(arrowStorage), nil, timeRange, matcher)
	if err != nil {
		t.Fatalf("SelectFromBlocksWithTimeRange() error = %v", err)
	}

	// Should only get recent and new spans
	if len(results.Spans) != 2 {
		t.Errorf("SelectFromBlocksWithTimeRange() returned %d results, want 2", len(results.Spans))
	}
}

// TestQuerier is covered comprehensively in querier_comprehensive_test.go
// This basic test remains for backward compatibility
func TestQuerier(t *testing.T) {
	tmpDir := t.TempDir()

	headStorage := storage.NewArrowStorage()
	defer headStorage.Release()

	baseTime := time.Now().Add(-2 * time.Hour)

	headSpans := createTestSpans(t, "head", baseTime.Add(1*time.Hour), 50, "service-head", "prod")
	for _, sp := range headSpans {
		headStorage.AddSpan(sp)
	}
	headStorage.Flush()

	testBlock := createTestBlockWithCustomSpans(t, tmpDir, 1, baseTime,
		createTestSpans(t, "block", baseTime, 100, "service-block", "prod"))
	defer testBlock.Close()

	querier := NewBlockQuerier(block.NewHeadBlock(headStorage), []Block{testBlock})

	matcher, _ := NewMatcher(MatchEqual, "env", "prod")
	results, err := querier.Select(matcher)
	if err != nil {
		t.Fatalf("Querier.Select() error = %v", err)
	}

	// Should get all spans (50 head + 100 block)
	if len(results.Spans) != 150 {
		t.Errorf("Querier.Select() returned %d spans, want 150", len(results.Spans))
	}
}

func Benchmark_Select(b *testing.B) {
	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()

	// Add 1000 test spans
	for i := range 1000 {
		sp := &span.Span{
			SpanID:      formatSpanID(uint64(i)),
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
			Tags:        map[string]string{"env": "prod", "index": string(rune(i % 10))},
		}
		arrowStorage.AddSpan(sp)
	}
	arrowStorage.Flush()

	matcher, _ := NewMatcher(MatchEqual, "env", "prod")

	for b.Loop() {
		Select(arrowStorage, matcher)
	}
}

// TestSelectFromMixedBlocks tests the legacy SelectFromBlocks function
// Comprehensive coverage in querier_comprehensive_test.go
func TestSelectFromMixedBlocks(t *testing.T) {
	tmpDir := t.TempDir()

	headStorage := storage.NewArrowStorage()
	defer headStorage.Release()

	baseTime := time.Now().Add(-3 * time.Hour)

	headSpans := createTestSpans(t, "head", baseTime.Add(2*time.Hour), 50, "service-head", "prod")
	for _, sp := range headSpans {
		headStorage.AddSpan(sp)
	}
	headStorage.Flush()

	l0Block := createTestBlockWithCustomSpans(t, tmpDir, 0, baseTime.Add(1*time.Hour),
		createTestSpans(t, "l0", baseTime.Add(1*time.Hour), 100, "service-l0", "prod"))
	defer l0Block.Close()

	l1Block := createTestBlockWithCustomSpans(t, tmpDir, 1, baseTime,
		createTestSpans(t, "l1", baseTime, 150, "service-l1", "prod"))
	defer l1Block.Close()

	blocks := []Block{l0Block, l1Block}

	matcher, _ := NewMatcher(MatchEqual, "env", "prod")
	results, err := SelectFromBlocks(block.NewHeadBlock(headStorage), blocks, matcher)
	if err != nil {
		t.Fatalf("SelectFromBlocks() error = %v", err)
	}

	// Should get all spans (50 from head + 100 from L0 + 150 from L1)
	expectedCount := 300
	if len(results.Spans) != expectedCount {
		t.Errorf("SelectFromBlocks() returned %d spans, want %d", len(results.Spans), expectedCount)
	}
}

// TestSelectFromMixedBlocksWithTimeRange tests time-based queries
// Comprehensive coverage in querier_comprehensive_test.go
func TestSelectFromMixedBlocksWithTimeRange(t *testing.T) {
	tmpDir := t.TempDir()

	headStorage := storage.NewArrowStorage()
	defer headStorage.Release()

	baseTime := time.Now().Add(-3 * time.Hour)

	headSpans := createTestSpans(t, "head", baseTime.Add(2*time.Hour+30*time.Minute), 50, "service-head", "prod")
	for _, sp := range headSpans {
		headStorage.AddSpan(sp)
	}
	headStorage.Flush()

	l0Block := createTestBlockWithCustomSpans(t, tmpDir, 0, baseTime.Add(1*time.Hour),
		createTestSpans(t, "l0", baseTime.Add(1*time.Hour), 100, "service-l0", "prod"))
	defer l0Block.Close()

	blocks := []Block{l0Block}

	// Query last hour - should only get head spans
	timeRange := NewTimeRange(baseTime.Add(2*time.Hour), baseTime.Add(3*time.Hour))
	matcher, _ := NewMatcher(MatchEqual, "env", "prod")

	results, err := SelectFromBlocksWithTimeRange(block.NewHeadBlock(headStorage), blocks, timeRange, matcher)
	if err != nil {
		t.Fatalf("SelectFromBlocksWithTimeRange() error = %v", err)
	}

	if len(results.Spans) != 50 {
		t.Errorf("Query for last hour returned %d spans, want 50", len(results.Spans))
	}
}

// Helper functions

func createTestSpans(t *testing.T, prefix string, baseTime time.Time, count int, serviceName, env string) []*span.Span {
	t.Helper()

	spans := make([]*span.Span, count)
	for i := range count {
		// Generate valid 16-character hex span IDs
		// Use prefix hash + counter to create unique IDs
		var prefixHash uint64
		for _, c := range prefix {
			prefixHash = prefixHash*31 + uint64(c)
		}
		spanIDVal := (prefixHash << 32) | uint64(i)

		spans[i] = &span.Span{
			TraceID:     "trace-" + prefix + "-" + string(rune(i)),
			SpanID:      formatSpanID(spanIDVal),
			Name:        "operation-" + prefix,
			StartTime:   baseTime.Add(time.Duration(i) * time.Millisecond),
			EndTime:     baseTime.Add(time.Duration(i+1) * time.Millisecond),
			Duration:    1000000, // 1ms
			ServiceName: serviceName,
			Tags: map[string]string{
				"env":     env,
				"version": "2.0",
				"index":   string(rune(i % 10)),
			},
		}
	}
	return spans
}

// formatSpanID formats a uint64 as a 16-character hex string
func formatSpanID(id uint64) string {
	return fmt.Sprintf("%016x", id)
}

func createTestBlockWithCustomSpans(t *testing.T, baseDir string, level int, createdAt time.Time, spans []*span.Span) Block {
	t.Helper()

	// We need to import the necessary packages
	// Use storage to write the spans properly
	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()

	var minTime, maxTime int64
	for i, sp := range spans {
		arrowStorage.AddSpan(sp)
		startNano := sp.StartTime.UnixNano()
		endNano := sp.EndTime.UnixNano()

		if i == 0 {
			minTime = startNano
			maxTime = endNano
		} else {
			if startNano < minTime {
				minTime = startNano
			}
			if endNano > maxTime {
				maxTime = endNano
			}
		}
	}
	arrowStorage.Flush()

	// Create block metadata
	blockID := ulid.Make()
	var compactionMeta *CompactionMeta
	if level > 0 {
		compactionMeta = &CompactionMeta{
			Level:       level,
			Sources:     []ulid.ULID{},
			CompactedAt: createdAt,
		}
	}

	meta := &BlockMeta{
		ULID:       blockID,
		MinTime:    minTime,
		MaxTime:    maxTime,
		SpanCount:  int64(len(spans)),
		Version:    1,
		CreatedAt:  createdAt,
		Compaction: compactionMeta,
	}

	blockDir := filepath.Join(baseDir, blockID.String())

	var err error
	if level == 0 {
		// Write as Arrow IPC
		err = block.FlushBlock(blockDir, meta, arrowStorage.GetRecords(), arrowStorage.Schema(), arrowStorage.GetIndex())
	} else {
		// Write as Parquet
		err = block.WriteParquetBlock(blockDir, meta, spans, arrowStorage.GetIndex())
	}

	if err != nil {
		t.Fatalf("Failed to create test block: %v", err)
	}

	// Load and return the block
	blk, err := block.LoadBlock(blockDir)
	if err != nil {
		t.Fatalf("Failed to load test block: %v", err)
	}

	return blk
}
