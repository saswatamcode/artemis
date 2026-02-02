package query

import (
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// TestIndexLookupPaths tests all index lookup paths documented in README:
// 1. trace_id: traceIndex → spanIndex → storage (3-hop)
// 2. span_id: spanIndex → storage (2-hop, fastest)
// 3. tag: tagIndex → spanIndex → storage (3-hop)
// 4. parent_span_id: tagIndex → spanIndex → storage (3-hop, reverse lookup)
// 5. name: tagIndex → spanIndex → storage (3-hop)
//
// Tests each path on all three block types:
// - Head block (in-memory Arrow)
// - L0 block (Arrow IPC on disk)
// - L1 block (Parquet on disk)
func TestIndexLookupPaths(t *testing.T) {
	tmpDir := t.TempDir()
	baseTime := time.Now().Add(-2 * time.Hour)

	// Create test data with relationships
	testSpans := []*span.Span{
		{
			TraceID:      "trace-123",
			SpanID:       "span-parent",
			ParentSpanID: "",
			Name:         "GET /api/users",
			ServiceName:  "api-gateway",
			StartTime:    baseTime,
			EndTime:      baseTime.Add(100 * time.Millisecond),
			Duration:     100_000_000,
			Tags: map[string]string{
				"http.method": "GET",
				"http.status": "200",
				"name":        "GET /api/users",
			},
		},
		{
			TraceID:      "trace-123",
			SpanID:       "span-child-1",
			ParentSpanID: "span-parent",
			Name:         "SELECT * FROM users",
			ServiceName:  "database",
			StartTime:    baseTime.Add(10 * time.Millisecond),
			EndTime:      baseTime.Add(50 * time.Millisecond),
			Duration:     40_000_000,
			Tags: map[string]string{
				"db.system":      "postgres",
				"parent_span_id": "span-parent",
				"name":           "SELECT * FROM users",
			},
		},
		{
			TraceID:      "trace-123",
			SpanID:       "span-child-2",
			ParentSpanID: "span-parent",
			Name:         "Cache lookup",
			ServiceName:  "redis",
			StartTime:    baseTime.Add(5 * time.Millisecond),
			EndTime:      baseTime.Add(8 * time.Millisecond),
			Duration:     3_000_000,
			Tags: map[string]string{
				"cache.hit":      "true",
				"parent_span_id": "span-parent",
				"name":           "Cache lookup",
			},
		},
		{
			TraceID:      "trace-456",
			SpanID:       "span-other",
			ParentSpanID: "",
			Name:         "POST /api/orders",
			ServiceName:  "api-gateway",
			StartTime:    baseTime.Add(1 * time.Hour),
			EndTime:      baseTime.Add(1 * time.Hour).Add(200 * time.Millisecond),
			Duration:     200_000_000,
			Tags: map[string]string{
				"http.method": "POST",
				"http.status": "201",
				"name":        "POST /api/orders",
			},
		},
	}

	// Create head block
	headStorage := storage.NewArrowStorage()
	defer headStorage.Release()
	for _, sp := range testSpans {
		headStorage.AddSpan(sp)
	}
	headStorage.Flush()

	// Create L0 Arrow IPC block
	l0Block := createTestBlockWithCustomSpans(t, tmpDir, 0, baseTime, testSpans)
	defer l0Block.Close()

	// Create L1 Parquet block
	l1Block := createTestBlockWithCustomSpans(t, tmpDir, 1, baseTime, testSpans)
	defer l1Block.Close()

	// Test each query path on each block type
	testCases := []struct {
		name           string
		matcher        func() *Matcher
		expectedCount  int
		expectedSpanID string
		description    string
	}{
		{
			name: "trace_id query (3-hop: traceIndex → spanIndex → storage)",
			matcher: func() *Matcher {
				m, _ := NewMatcher(MatchEqual, "trace_id", "trace-123")
				return m
			},
			expectedCount: 3,
			description:   "Should use traceIndex to get all spans in trace-123",
		},
		{
			name: "span_id query (2-hop: spanIndex → storage, FASTEST)",
			matcher: func() *Matcher {
				m, _ := NewMatcher(MatchEqual, "span_id", "span-child-1")
				return m
			},
			expectedCount:  1,
			expectedSpanID: "span-child-1",
			description:    "Should use spanIndex directly (no intermediate lookup)",
		},
		{
			name: "tag query (3-hop: tagIndex → spanIndex → storage)",
			matcher: func() *Matcher {
				m, _ := NewMatcher(MatchEqual, "http.method", "GET")
				return m
			},
			expectedCount: 1,
			description:   "Should use tagIndex for custom tag",
		},
		{
			name: "parent_span_id query (3-hop: tagIndex → spanIndex → storage)",
			matcher: func() *Matcher {
				m, _ := NewMatcher(MatchEqual, "parent_span_id", "span-parent")
				return m
			},
			expectedCount: 2,
			description:   "Should use tagIndex to find all children (reverse lookup)",
		},
		{
			name: "name query (3-hop: tagIndex → spanIndex → storage)",
			matcher: func() *Matcher {
				m, _ := NewMatcher(MatchEqual, "name", "GET /api/users")
				return m
			},
			expectedCount:  1,
			expectedSpanID: "span-parent",
			description:    "Should use tagIndex for operation name",
		},
	}

	blockTypes := []struct {
		name    string
		storage *storage.ArrowStorage
		blocks  []block.Block
	}{
		{"Head (in-memory Arrow)", headStorage, nil},
		{"L0 (Arrow IPC on disk)", nil, []block.Block{l0Block}},
		{"L1 (Parquet on disk)", nil, []block.Block{l1Block}},
	}

	for _, blockType := range blockTypes {
		t.Run(blockType.name, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					matcher := tc.matcher()

					var results *SelectResult
					var err error

					if blockType.storage != nil {
						// Query head block
						results, err = Select(blockType.storage, matcher)
					} else {
						// Query persisted blocks
						emptyHead := storage.NewArrowStorage()
						defer emptyHead.Release()
						emptyHead.Flush()
						results, err = SelectFromBlocks(emptyHead, blockType.blocks, matcher)
					}

					if err != nil {
						t.Fatalf("%s: query error = %v", tc.description, err)
					}

					if len(results.Spans) != tc.expectedCount {
						t.Errorf("%s: got %d spans, want %d",
							tc.description, len(results.Spans), tc.expectedCount)
					}

					if tc.expectedSpanID != "" && len(results.Spans) > 0 {
						found := false
						for _, sp := range results.Spans {
							if sp.SpanID == tc.expectedSpanID {
								found = true
								break
							}
						}
						if !found {
							t.Errorf("%s: expected to find span %s, but didn't",
								tc.description, tc.expectedSpanID)
						}
					}
				})
			}
		})
	}
}

// TestTraceIDIndexPath specifically tests the trace_id index path
func TestTraceIDIndexPath(t *testing.T) {
	tmpDir := t.TempDir()
	baseTime := time.Now()

	// Create a multi-span trace
	traceSpans := []*span.Span{
		{
			TraceID:     "trace-multi",
			SpanID:      "root",
			Name:        "root operation",
			ServiceName: "frontend",
			StartTime:   baseTime,
			EndTime:     baseTime.Add(100 * time.Millisecond),
			Duration:    100_000_000,
			Tags:        map[string]string{"name": "root operation"},
		},
		{
			TraceID:      "trace-multi",
			SpanID:       "child-1",
			ParentSpanID: "root",
			Name:         "auth check",
			ServiceName:  "auth-service",
			StartTime:    baseTime.Add(5 * time.Millisecond),
			EndTime:      baseTime.Add(10 * time.Millisecond),
			Duration:     5_000_000,
			Tags: map[string]string{
				"parent_span_id": "root",
				"name":           "auth check",
			},
		},
		{
			TraceID:      "trace-multi",
			SpanID:       "child-2",
			ParentSpanID: "root",
			Name:         "db query",
			ServiceName:  "database",
			StartTime:    baseTime.Add(15 * time.Millisecond),
			EndTime:      baseTime.Add(90 * time.Millisecond),
			Duration:     75_000_000,
			Tags: map[string]string{
				"parent_span_id": "root",
				"name":           "db query",
			},
		},
	}

	// Test on head block
	t.Run("Head block trace lookup", func(t *testing.T) {
		headStorage := storage.NewArrowStorage()
		defer headStorage.Release()

		for _, sp := range traceSpans {
			headStorage.AddSpan(sp)
		}
		headStorage.Flush()

		// Verify index has the trace
		if headStorage.GetIndex() == nil {
			t.Fatal("Head block should have index")
		}

		spanIDs := headStorage.GetIndex().LookupByTraceID("trace-multi")
		if len(spanIDs) != 3 {
			t.Errorf("traceIndex should return 3 span IDs, got %d", len(spanIDs))
		}

		// Query by trace_id
		matcher, _ := NewMatcher(MatchEqual, "trace_id", "trace-multi")
		results, err := Select(headStorage, matcher)
		if err != nil {
			t.Fatalf("Select error: %v", err)
		}

		if len(results.Spans) != 3 {
			t.Errorf("Should get all 3 spans in trace, got %d", len(results.Spans))
		}

		// Verify all spans belong to the same trace
		for _, sp := range results.Spans {
			if sp.TraceID != "trace-multi" {
				t.Errorf("Got span from trace %s, want trace-multi", sp.TraceID)
			}
		}
	})

	// Test on Parquet block
	t.Run("Parquet block trace lookup", func(t *testing.T) {
		l1Block := createTestBlockWithCustomSpans(t, tmpDir, 1, baseTime, traceSpans)
		defer l1Block.Close()

		// Verify block has index
		if !l1Block.HasIndex() {
			t.Fatal("Parquet block should have index")
		}

		spanIDs := l1Block.Index().LookupByTraceID("trace-multi")
		if len(spanIDs) != 3 {
			t.Errorf("traceIndex should return 3 span IDs, got %d", len(spanIDs))
		}

		// Query by trace_id
		matcher, _ := NewMatcher(MatchEqual, "trace_id", "trace-multi")
		emptyHead := storage.NewArrowStorage()
		defer emptyHead.Release()
		emptyHead.Flush()

		results, err := SelectFromBlocks(emptyHead, []block.Block{l1Block}, matcher)
		if err != nil {
			t.Fatalf("SelectFromBlocks error: %v", err)
		}

		if len(results.Spans) != 3 {
			t.Errorf("Should get all 3 spans in trace, got %d", len(results.Spans))
		}
	})
}

// TestParentSpanIDReverseLookup specifically tests the parent_span_id reverse lookup
func TestParentSpanIDReverseLookup(t *testing.T) {
	tmpDir := t.TempDir()
	baseTime := time.Now()

	// Create parent with multiple children
	spans := []*span.Span{
		{
			TraceID:     "trace-1",
			SpanID:      "parent-span",
			Name:        "parent operation",
			ServiceName: "service-a",
			StartTime:   baseTime,
			EndTime:     baseTime.Add(100 * time.Millisecond),
			Duration:    100_000_000,
			Tags:        map[string]string{"name": "parent operation"},
		},
		{
			TraceID:      "trace-1",
			SpanID:       "child-a",
			ParentSpanID: "parent-span",
			Name:         "child A",
			ServiceName:  "service-b",
			StartTime:    baseTime.Add(10 * time.Millisecond),
			EndTime:      baseTime.Add(30 * time.Millisecond),
			Duration:     20_000_000,
			Tags: map[string]string{
				"parent_span_id": "parent-span",
				"name":           "child A",
			},
		},
		{
			TraceID:      "trace-1",
			SpanID:       "child-b",
			ParentSpanID: "parent-span",
			Name:         "child B",
			ServiceName:  "service-c",
			StartTime:    baseTime.Add(40 * time.Millisecond),
			EndTime:      baseTime.Add(80 * time.Millisecond),
			Duration:     40_000_000,
			Tags: map[string]string{
				"parent_span_id": "parent-span",
				"name":           "child B",
			},
		},
		{
			TraceID:      "trace-1",
			SpanID:       "child-c",
			ParentSpanID: "parent-span",
			Name:         "child C",
			ServiceName:  "service-d",
			StartTime:    baseTime.Add(35 * time.Millisecond),
			EndTime:      baseTime.Add(60 * time.Millisecond),
			Duration:     25_000_000,
			Tags: map[string]string{
				"parent_span_id": "parent-span",
				"name":           "child C",
			},
		},
	}

	t.Run("Find all children via parent_span_id", func(t *testing.T) {
		headStorage := storage.NewArrowStorage()
		defer headStorage.Release()

		for _, sp := range spans {
			headStorage.AddSpan(sp)
		}
		headStorage.Flush()

		// Query for all children of parent-span
		matcher, _ := NewMatcher(MatchEqual, "parent_span_id", "parent-span")
		results, err := Select(headStorage, matcher)
		if err != nil {
			t.Fatalf("Select error: %v", err)
		}

		// Should get 3 children (not the parent itself)
		if len(results.Spans) != 3 {
			t.Errorf("Should get 3 children, got %d", len(results.Spans))
		}

		// Verify none of the results is the parent
		for _, sp := range results.Spans {
			if sp.SpanID == "parent-span" {
				t.Error("Should not return the parent span itself")
			}
			if sp.ParentSpanID != "parent-span" {
				t.Errorf("Span %s has wrong parent %s", sp.SpanID, sp.ParentSpanID)
			}
		}

		// Verify we got all expected children
		childIDs := make(map[string]bool)
		for _, sp := range results.Spans {
			childIDs[sp.SpanID] = true
		}

		expectedChildren := []string{"child-a", "child-b", "child-c"}
		for _, childID := range expectedChildren {
			if !childIDs[childID] {
				t.Errorf("Missing expected child %s", childID)
			}
		}
	})

	t.Run("Parquet block parent_span_id lookup", func(t *testing.T) {
		l1Block := createTestBlockWithCustomSpans(t, tmpDir, 1, baseTime, spans)
		defer l1Block.Close()

		// Verify tagIndex can find children
		if !l1Block.HasIndex() {
			t.Fatal("Parquet block should have index")
		}

		childSpanIDs := l1Block.Index().LookupByTag("parent_span_id", "parent-span")
		if len(childSpanIDs) != 3 {
			t.Errorf("tagIndex should return 3 child span IDs, got %d", len(childSpanIDs))
		}

		// Query via SelectFromBlocks
		matcher, _ := NewMatcher(MatchEqual, "parent_span_id", "parent-span")
		emptyHead := storage.NewArrowStorage()
		defer emptyHead.Release()
		emptyHead.Flush()

		results, err := SelectFromBlocks(emptyHead, []block.Block{l1Block}, matcher)
		if err != nil {
			t.Fatalf("SelectFromBlocks error: %v", err)
		}

		if len(results.Spans) != 3 {
			t.Errorf("Should get 3 children from Parquet block, got %d", len(results.Spans))
		}
	})
}

// TestNameQueryIndexPath tests querying by operation name
func TestNameQueryIndexPath(t *testing.T) {
	tmpDir := t.TempDir()
	baseTime := time.Now()

	// Create spans with different names
	spans := []*span.Span{
		{
			TraceID:     "trace-1",
			SpanID:      "span-1",
			Name:        "GET /api/users",
			ServiceName: "api",
			StartTime:   baseTime,
			EndTime:     baseTime.Add(50 * time.Millisecond),
			Duration:    50_000_000,
			Tags: map[string]string{
				"http.method": "GET",
				"name":        "GET /api/users",
			},
		},
		{
			TraceID:     "trace-2",
			SpanID:      "span-2",
			Name:        "GET /api/users",
			ServiceName: "api",
			StartTime:   baseTime.Add(100 * time.Millisecond),
			EndTime:     baseTime.Add(150 * time.Millisecond),
			Duration:    50_000_000,
			Tags: map[string]string{
				"http.method": "GET",
				"name":        "GET /api/users",
			},
		},
		{
			TraceID:     "trace-3",
			SpanID:      "span-3",
			Name:        "POST /api/orders",
			ServiceName: "api",
			StartTime:   baseTime.Add(200 * time.Millisecond),
			EndTime:     baseTime.Add(300 * time.Millisecond),
			Duration:    100_000_000,
			Tags: map[string]string{
				"http.method": "POST",
				"name":        "POST /api/orders",
			},
		},
	}

	t.Run("Query by name in head block", func(t *testing.T) {
		headStorage := storage.NewArrowStorage()
		defer headStorage.Release()

		for _, sp := range spans {
			headStorage.AddSpan(sp)
		}
		headStorage.Flush()

		// Query for all "GET /api/users" operations
		matcher, _ := NewMatcher(MatchEqual, "name", "GET /api/users")
		results, err := Select(headStorage, matcher)
		if err != nil {
			t.Fatalf("Select error: %v", err)
		}

		if len(results.Spans) != 2 {
			t.Errorf("Should get 2 spans with name 'GET /api/users', got %d", len(results.Spans))
		}

		for _, sp := range results.Spans {
			if sp.Name != "GET /api/users" {
				t.Errorf("Got span with name %s, want 'GET /api/users'", sp.Name)
			}
		}
	})

	t.Run("Query by name in Parquet block", func(t *testing.T) {
		l1Block := createTestBlockWithCustomSpans(t, tmpDir, 1, baseTime, spans)
		defer l1Block.Close()

		// Verify tagIndex has name entries
		if !l1Block.HasIndex() {
			t.Fatal("Parquet block should have index")
		}

		spanIDs := l1Block.Index().LookupByTag("name", "GET /api/users")
		if len(spanIDs) != 2 {
			t.Errorf("tagIndex should return 2 span IDs for name, got %d", len(spanIDs))
		}

		// Query via SelectFromBlocks
		matcher, _ := NewMatcher(MatchEqual, "name", "GET /api/users")
		emptyHead := storage.NewArrowStorage()
		defer emptyHead.Release()
		emptyHead.Flush()

		results, err := SelectFromBlocks(emptyHead, []block.Block{l1Block}, matcher)
		if err != nil {
			t.Fatalf("SelectFromBlocks error: %v", err)
		}

		if len(results.Spans) != 2 {
			t.Errorf("Should get 2 spans from Parquet block, got %d", len(results.Spans))
		}
	})
}

// TestSpanIDDirectLookup verifies span_id queries use spanIndex directly (2-hop, fastest)
func TestSpanIDDirectLookup(t *testing.T) {
	tmpDir := t.TempDir()
	baseTime := time.Now()

	spans := []*span.Span{
		{
			TraceID:     "trace-1",
			SpanID:      "target-span",
			Name:        "operation",
			ServiceName: "service",
			StartTime:   baseTime,
			EndTime:     baseTime.Add(10 * time.Millisecond),
			Duration:    10_000_000,
			Tags:        map[string]string{"name": "operation"},
		},
		{
			TraceID:     "trace-1",
			SpanID:      "other-span-1",
			Name:        "operation",
			ServiceName: "service",
			StartTime:   baseTime.Add(20 * time.Millisecond),
			EndTime:     baseTime.Add(30 * time.Millisecond),
			Duration:    10_000_000,
			Tags:        map[string]string{"name": "operation"},
		},
		{
			TraceID:     "trace-1",
			SpanID:      "other-span-2",
			Name:        "operation",
			ServiceName: "service",
			StartTime:   baseTime.Add(40 * time.Millisecond),
			EndTime:     baseTime.Add(50 * time.Millisecond),
			Duration:    10_000_000,
			Tags:        map[string]string{"name": "operation"},
		},
	}

	t.Run("span_id direct lookup in head", func(t *testing.T) {
		headStorage := storage.NewArrowStorage()
		defer headStorage.Release()

		for _, sp := range spans {
			headStorage.AddSpan(sp)
		}
		headStorage.Flush()

		// Verify spanIndex has direct entry
		if headStorage.GetIndex() == nil {
			t.Fatal("Head should have index")
		}

		ref, ok := headStorage.GetIndex().LookupSpanID("target-span")
		if !ok {
			t.Fatal("spanIndex should have target-span")
		}
		if ref.RecordIndex < 0 || ref.RowIndex < 0 {
			t.Errorf("Invalid span reference: %+v", ref)
		}

		// Query by span_id (should be fastest - 2-hop lookup)
		matcher, _ := NewMatcher(MatchEqual, "span_id", "target-span")
		results, err := Select(headStorage, matcher)
		if err != nil {
			t.Fatalf("Select error: %v", err)
		}

		if len(results.Spans) != 1 {
			t.Errorf("Should get exactly 1 span, got %d", len(results.Spans))
		}

		if results.Spans[0].SpanID != "target-span" {
			t.Errorf("Got span %s, want target-span", results.Spans[0].SpanID)
		}
	})

	t.Run("span_id direct lookup in Parquet", func(t *testing.T) {
		l1Block := createTestBlockWithCustomSpans(t, tmpDir, 1, baseTime, spans)
		defer l1Block.Close()

		// Verify spanIndex has direct entry
		if !l1Block.HasIndex() {
			t.Fatal("Parquet block should have index")
		}

		ref, ok := l1Block.Index().LookupSpanID("target-span")
		if !ok {
			t.Fatal("spanIndex should have target-span")
		}
		if ref.RecordIndex < 0 || ref.RowIndex < 0 {
			t.Errorf("Invalid span reference: %+v", ref)
		}

		// Query by span_id
		matcher, _ := NewMatcher(MatchEqual, "span_id", "target-span")
		emptyHead := storage.NewArrowStorage()
		defer emptyHead.Release()
		emptyHead.Flush()

		results, err := SelectFromBlocks(emptyHead, []block.Block{l1Block}, matcher)
		if err != nil {
			t.Fatalf("SelectFromBlocks error: %v", err)
		}

		if len(results.Spans) != 1 {
			t.Errorf("Should get exactly 1 span from Parquet, got %d", len(results.Spans))
		}

		if results.Spans[0].SpanID != "target-span" {
			t.Errorf("Got span %s, want target-span", results.Spans[0].SpanID)
		}
	})
}
