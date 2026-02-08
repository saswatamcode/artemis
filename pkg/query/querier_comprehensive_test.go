package query

import (
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// TestFanoutQuerier_EmptyQueriers tests fanout with no queriers
func TestFanoutQuerier_EmptyQueriers(t *testing.T) {
	querier := NewFanoutQuerier()

	result, err := querier.Select()
	if err != nil {
		t.Fatalf("FanoutQuerier.Select() error = %v", err)
	}

	if len(result.Spans) != 0 {
		t.Errorf("Expected 0 spans, got %d", len(result.Spans))
	}
}

// TestFanoutQuerier_SingleQuerier tests fanout with a single querier
func TestFanoutQuerier_SingleQuerier(t *testing.T) {
	headStorage := storage.NewArrowStorage()
	defer headStorage.Release()

	spans := createTestSpans(t, "test", time.Now(), 10, "service", "prod")
	for _, sp := range spans {
		headStorage.AddSpan(sp)
	}
	headStorage.Flush()

	headQuerier := NewHeadBlockQuerier(block.NewHeadBlock(headStorage))
	fanoutQuerier := NewFanoutQuerier(headQuerier)

	result, err := fanoutQuerier.Select()
	if err != nil {
		t.Fatalf("FanoutQuerier.Select() error = %v", err)
	}

	if len(result.Spans) != 10 {
		t.Errorf("Expected 10 spans, got %d", len(result.Spans))
	}
}

// TestFanoutQuerier_MultipleQueriers tests fanout with multiple queriers
func TestFanoutQuerier_MultipleQueriers(t *testing.T) {
	tmpDir := t.TempDir()
	baseTime := time.Now().Add(-2 * time.Hour)

	// Create head storage
	headStorage := storage.NewArrowStorage()
	defer headStorage.Release()

	headSpans := createTestSpans(t, "head", baseTime.Add(1*time.Hour), 50, "service-head", "prod")
	for _, sp := range headSpans {
		headStorage.AddSpan(sp)
	}
	headStorage.Flush()

	// Create persisted blocks
	block1 := createTestBlockWithCustomSpans(t, tmpDir, 0, baseTime,
		createTestSpans(t, "block1", baseTime, 100, "service-block1", "prod"))
	defer block1.Close()

	block2 := createTestBlockWithCustomSpans(t, tmpDir, 1, baseTime.Add(-1*time.Hour),
		createTestSpans(t, "block2", baseTime.Add(-1*time.Hour), 75, "service-block2", "prod"))
	defer block2.Close()

	// Create queriers
	headQuerier := NewHeadBlockQuerier(block.NewHeadBlock(headStorage))
	persistedQuerier := NewPersistedBlockQuerier([]block.Block{block1, block2})
	fanoutQuerier := NewFanoutQuerier(persistedQuerier, headQuerier)

	t.Run("query all without filters", func(t *testing.T) {
		result, err := fanoutQuerier.Select()
		if err != nil {
			t.Fatalf("FanoutQuerier.Select() error = %v", err)
		}

		// Should get 50 + 100 + 75 = 225 spans
		if len(result.Spans) != 225 {
			t.Errorf("Expected 225 spans, got %d", len(result.Spans))
		}
	})

	t.Run("query with time range", func(t *testing.T) {
		// Query only the last hour (should get only head spans)
		timeRange := NewTimeRange(baseTime.Add(1*time.Hour), baseTime.Add(2*time.Hour))
		matcher, _ := NewMatcher(MatchEqual, "env", "prod")

		result, err := fanoutQuerier.SelectWithTimeRange(timeRange, matcher)
		if err != nil {
			t.Fatalf("FanoutQuerier.SelectWithTimeRange() error = %v", err)
		}

		if len(result.Spans) != 50 {
			t.Errorf("Expected 50 spans, got %d", len(result.Spans))
		}
	})

	t.Run("query with service filter", func(t *testing.T) {
		matcher, _ := NewMatcher(MatchEqual, "service.name", "service-block1")
		result, err := fanoutQuerier.Select(matcher)
		if err != nil {
			t.Fatalf("FanoutQuerier.Select() error = %v", err)
		}

		// Should only get block1 spans
		if len(result.Spans) != 100 {
			t.Errorf("Expected 100 spans, got %d", len(result.Spans))
		}
	})
}

// TestHeadBlockQuerier_EmptyHead tests querying an empty head block
func TestHeadBlockQuerier_EmptyHead(t *testing.T) {
	headStorage := storage.NewArrowStorage()
	defer headStorage.Release()
	headStorage.Flush()

	querier := NewHeadBlockQuerier(block.NewHeadBlock(headStorage))

	result, err := querier.Select()
	if err != nil {
		t.Fatalf("HeadBlockQuerier.Select() error = %v", err)
	}

	if len(result.Spans) != 0 {
		t.Errorf("Expected 0 spans, got %d", len(result.Spans))
	}
}

// TestHeadBlockQuerier_NilHead tests querying with a nil head block
func TestHeadBlockQuerier_NilHead(t *testing.T) {
	querier := NewHeadBlockQuerier(nil)

	result, err := querier.Select()
	if err != nil {
		t.Fatalf("HeadBlockQuerier.Select() error = %v", err)
	}

	if len(result.Spans) != 0 {
		t.Errorf("Expected 0 spans, got %d", len(result.Spans))
	}
}

// TestHeadBlockQuerier_WithTimeRangeFilter tests time range filtering on head block
func TestHeadBlockQuerier_WithTimeRangeFilter(t *testing.T) {
	headStorage := storage.NewArrowStorage()
	defer headStorage.Release()

	now := time.Now()
	spans := []*span.Span{
		{
			SpanID:      "span-old",
			StartTime:   now.Add(-2 * time.Hour),
			EndTime:     now.Add(-2 * time.Hour).Add(time.Millisecond),
			ServiceName: "service",
			Tags:        map[string]string{"env": "prod"},
		},
		{
			SpanID:      "span-recent",
			StartTime:   now.Add(-30 * time.Minute),
			EndTime:     now.Add(-30 * time.Minute).Add(time.Millisecond),
			ServiceName: "service",
			Tags:        map[string]string{"env": "prod"},
		},
	}

	for _, sp := range spans {
		headStorage.AddSpan(sp)
	}
	headStorage.Flush()

	querier := NewHeadBlockQuerier(block.NewHeadBlock(headStorage))

	t.Run("time range matches recent span only", func(t *testing.T) {
		timeRange := NewTimeRange(now.Add(-1*time.Hour), now)
		matcher, _ := NewMatcher(MatchEqual, "env", "prod")

		result, err := querier.SelectWithTimeRange(timeRange, matcher)
		if err != nil {
			t.Fatalf("HeadBlockQuerier.SelectWithTimeRange() error = %v", err)
		}

		if len(result.Spans) != 1 {
			t.Errorf("Expected 1 span, got %d", len(result.Spans))
		}
		if len(result.Spans) > 0 && result.Spans[0].SpanID != "span-recent" {
			t.Errorf("Expected span-recent, got %s", result.Spans[0].SpanID)
		}
	})

	t.Run("time range doesn't overlap", func(t *testing.T) {
		timeRange := NewTimeRange(now.Add(-5*time.Hour), now.Add(-3*time.Hour))
		matcher, _ := NewMatcher(MatchEqual, "env", "prod")

		result, err := querier.SelectWithTimeRange(timeRange, matcher)
		if err != nil {
			t.Fatalf("HeadBlockQuerier.SelectWithTimeRange() error = %v", err)
		}

		if len(result.Spans) != 0 {
			t.Errorf("Expected 0 spans, got %d", len(result.Spans))
		}
	})
}

// TestPersistedBlockQuerier_EmptyBlocks tests querying with no blocks
func TestPersistedBlockQuerier_EmptyBlocks(t *testing.T) {
	querier := NewPersistedBlockQuerier([]block.Block{})

	result, err := querier.Select()
	if err != nil {
		t.Fatalf("PersistedBlockQuerier.Select() error = %v", err)
	}

	if len(result.Spans) != 0 {
		t.Errorf("Expected 0 spans, got %d", len(result.Spans))
	}
}

// TestPersistedBlockQuerier_SingleBlock tests querying a single block
func TestPersistedBlockQuerier_SingleBlock(t *testing.T) {
	tmpDir := t.TempDir()
	baseTime := time.Now()

	blk := createTestBlockWithCustomSpans(t, tmpDir, 0, baseTime,
		createTestSpans(t, "test", baseTime, 50, "service", "prod"))
	defer blk.Close()

	querier := NewPersistedBlockQuerier([]block.Block{blk})

	result, err := querier.Select()
	if err != nil {
		t.Fatalf("PersistedBlockQuerier.Select() error = %v", err)
	}

	if len(result.Spans) != 50 {
		t.Errorf("Expected 50 spans, got %d", len(result.Spans))
	}
}

// TestPersistedBlockQuerier_MultipleBlocksParallel tests parallel querying of multiple blocks
func TestPersistedBlockQuerier_MultipleBlocksParallel(t *testing.T) {
	tmpDir := t.TempDir()
	baseTime := time.Now()

	// Create multiple blocks
	blocks := make([]block.Block, 0, 5)
	for i := range 5 {
		blk := createTestBlockWithCustomSpans(t, tmpDir, i%2, baseTime.Add(time.Duration(-i)*time.Hour),
			createTestSpans(t, "block"+string(rune(i)), baseTime.Add(time.Duration(-i)*time.Hour), 20, "service-"+string(rune(i)), "prod"))
		defer blk.Close()
		blocks = append(blocks, blk)
	}

	querier := NewPersistedBlockQuerier(blocks)

	t.Run("query all blocks", func(t *testing.T) {
		result, err := querier.Select()
		if err != nil {
			t.Fatalf("PersistedBlockQuerier.Select() error = %v", err)
		}

		// Should get 5 blocks * 20 spans = 100 spans
		if len(result.Spans) != 100 {
			t.Errorf("Expected 100 spans, got %d", len(result.Spans))
		}
	})

	t.Run("query with time range filters blocks", func(t *testing.T) {
		// Only query the first 2 hours (should skip last 3 blocks)
		timeRange := NewTimeRange(baseTime.Add(-2*time.Hour), baseTime.Add(1*time.Hour))

		result, err := querier.SelectWithTimeRange(timeRange)
		if err != nil {
			t.Fatalf("PersistedBlockQuerier.SelectWithTimeRange() error = %v", err)
		}

		// Should get spans from blocks 0, 1, 2 (60 spans total)
		if len(result.Spans) != 60 {
			t.Errorf("Expected 60 spans, got %d", len(result.Spans))
		}
	})
}

// TestPersistedBlockQuerier_MixedBlockTypes tests querying Arrow and Parquet blocks together
func TestPersistedBlockQuerier_MixedBlockTypes(t *testing.T) {
	tmpDir := t.TempDir()
	baseTime := time.Now()

	// Create L0 Arrow block
	arrowBlock := createTestBlockWithCustomSpans(t, tmpDir, 0, baseTime,
		createTestSpans(t, "arrow", baseTime, 50, "service-arrow", "prod"))
	defer arrowBlock.Close()

	// Create L1 Parquet block
	parquetBlock := createTestBlockWithCustomSpans(t, tmpDir, 1, baseTime.Add(-1*time.Hour),
		createTestSpans(t, "parquet", baseTime.Add(-1*time.Hour), 75, "service-parquet", "prod"))
	defer parquetBlock.Close()

	querier := NewPersistedBlockQuerier([]block.Block{arrowBlock, parquetBlock})

	result, err := querier.Select()
	if err != nil {
		t.Fatalf("PersistedBlockQuerier.Select() error = %v", err)
	}

	// Should get 50 + 75 = 125 spans
	if len(result.Spans) != 125 {
		t.Errorf("Expected 125 spans, got %d", len(result.Spans))
	}

	// Verify we got spans from both block types
	services := make(map[string]int)
	for _, sp := range result.Spans {
		services[sp.ServiceName]++
	}

	if services["service-arrow"] != 50 {
		t.Errorf("Expected 50 spans from Arrow block, got %d", services["service-arrow"])
	}
	if services["service-parquet"] != 75 {
		t.Errorf("Expected 75 spans from Parquet block, got %d", services["service-parquet"])
	}
}

// TestBlockQuerier_NilHeadWithBlocks tests BlockQuerier with nil head but valid blocks
func TestBlockQuerier_NilHeadWithBlocks(t *testing.T) {
	tmpDir := t.TempDir()
	baseTime := time.Now()

	blk := createTestBlockWithCustomSpans(t, tmpDir, 0, baseTime,
		createTestSpans(t, "test", baseTime, 50, "service", "prod"))
	defer blk.Close()

	querier := NewBlockQuerier(nil, []block.Block{blk})

	result, err := querier.Select()
	if err != nil {
		t.Fatalf("BlockQuerier.Select() error = %v", err)
	}

	if len(result.Spans) != 50 {
		t.Errorf("Expected 50 spans, got %d", len(result.Spans))
	}
}

// TestBlockQuerier_HeadWithoutBlocks tests BlockQuerier with head but no persisted blocks
func TestBlockQuerier_HeadWithoutBlocks(t *testing.T) {
	headStorage := storage.NewArrowStorage()
	defer headStorage.Release()

	spans := createTestSpans(t, "test", time.Now(), 30, "service", "prod")
	for _, sp := range spans {
		headStorage.AddSpan(sp)
	}
	headStorage.Flush()

	querier := NewBlockQuerier(block.NewHeadBlock(headStorage), []block.Block{})

	result, err := querier.Select()
	if err != nil {
		t.Fatalf("BlockQuerier.Select() error = %v", err)
	}

	if len(result.Spans) != 30 {
		t.Errorf("Expected 30 spans, got %d", len(result.Spans))
	}
}

// TestBlockQuerier_EmptyHeadAndBlocks tests BlockQuerier with empty head and no blocks
func TestBlockQuerier_EmptyHeadAndBlocks(t *testing.T) {
	headStorage := storage.NewArrowStorage()
	defer headStorage.Release()
	headStorage.Flush()

	querier := NewBlockQuerier(block.NewHeadBlock(headStorage), []block.Block{})

	result, err := querier.Select()
	if err != nil {
		t.Fatalf("BlockQuerier.Select() error = %v", err)
	}

	if len(result.Spans) != 0 {
		t.Errorf("Expected 0 spans, got %d", len(result.Spans))
	}
}

// TestBlockQuerier_ComplexTimeRangeScenarios tests various time range scenarios
func TestBlockQuerier_ComplexTimeRangeScenarios(t *testing.T) {
	tmpDir := t.TempDir()
	baseTime := time.Now().Add(-4 * time.Hour)

	headStorage := storage.NewArrowStorage()
	defer headStorage.Release()

	// Head: last 30 minutes
	headSpans := createTestSpans(t, "head", baseTime.Add(3*time.Hour+30*time.Minute), 20, "service", "prod")
	for _, sp := range headSpans {
		headStorage.AddSpan(sp)
	}
	headStorage.Flush()

	// Block 1: 3-4 hours ago
	block1 := createTestBlockWithCustomSpans(t, tmpDir, 0, baseTime,
		createTestSpans(t, "b1", baseTime, 30, "service", "prod"))
	defer block1.Close()

	// Block 2: 2-3 hours ago
	block2 := createTestBlockWithCustomSpans(t, tmpDir, 0, baseTime.Add(1*time.Hour),
		createTestSpans(t, "b2", baseTime.Add(1*time.Hour), 40, "service", "prod"))
	defer block2.Close()

	// Block 3: 1-2 hours ago
	block3 := createTestBlockWithCustomSpans(t, tmpDir, 1, baseTime.Add(2*time.Hour),
		createTestSpans(t, "b3", baseTime.Add(2*time.Hour), 50, "service", "prod"))
	defer block3.Close()

	querier := NewBlockQuerier(block.NewHeadBlock(headStorage), []block.Block{block1, block2, block3})

	t.Run("query exact block boundary", func(t *testing.T) {
		// Query 1-2 hours ago window to get block2
		// Since createTestSpans creates spans at baseTime + millisecond intervals,
		// we query the full hour to ensure we capture all block2 spans
		timeRange := NewTimeRange(baseTime.Add(1*time.Hour), baseTime.Add(1*time.Hour+100*time.Millisecond))

		result, err := querier.SelectWithTimeRange(timeRange)
		if err != nil {
			t.Fatalf("BlockQuerier.SelectWithTimeRange() error = %v", err)
		}

		// Should get block2 spans (40 spans created at baseTime.Add(1h) + [0-40]ms)
		if len(result.Spans) != 40 {
			t.Errorf("Expected 40 spans from block2, got %d", len(result.Spans))
		}
	})

	t.Run("query spanning multiple blocks", func(t *testing.T) {
		// Query covering block1 and block2 with buffer
		timeRange := NewTimeRange(baseTime.Add(-time.Second), baseTime.Add(2*time.Hour))

		result, err := querier.SelectWithTimeRange(timeRange)
		if err != nil {
			t.Fatalf("BlockQuerier.SelectWithTimeRange() error = %v", err)
		}

		// Should get block1 + block2 (around 70 spans, allowing for some overlap)
		if len(result.Spans) < 60 || len(result.Spans) > 80 {
			t.Errorf("Expected ~70 spans, got %d", len(result.Spans))
		}
	})

	t.Run("query recent data only", func(t *testing.T) {
		// Query last hour (should get head + block3)
		timeRange := NewTimeRange(baseTime.Add(2*time.Hour), baseTime.Add(4*time.Hour))

		result, err := querier.SelectWithTimeRange(timeRange)
		if err != nil {
			t.Fatalf("BlockQuerier.SelectWithTimeRange() error = %v", err)
		}

		expected := 70 // 50 from block3 + 20 from head
		if len(result.Spans) != expected {
			t.Errorf("Expected %d spans, got %d", expected, len(result.Spans))
		}
	})

	t.Run("query all time", func(t *testing.T) {
		// Query everything
		timeRange := NewTimeRange(baseTime.Add(-1*time.Hour), baseTime.Add(5*time.Hour))

		result, err := querier.SelectWithTimeRange(timeRange)
		if err != nil {
			t.Fatalf("BlockQuerier.SelectWithTimeRange() error = %v", err)
		}

		expected := 140 // 30 + 40 + 50 + 20
		if len(result.Spans) != expected {
			t.Errorf("Expected %d spans, got %d", expected, len(result.Spans))
		}
	})

	t.Run("query non-overlapping time range", func(t *testing.T) {
		// Query future (should get nothing)
		timeRange := NewTimeRange(baseTime.Add(5*time.Hour), baseTime.Add(6*time.Hour))

		result, err := querier.SelectWithTimeRange(timeRange)
		if err != nil {
			t.Fatalf("BlockQuerier.SelectWithTimeRange() error = %v", err)
		}

		if len(result.Spans) != 0 {
			t.Errorf("Expected 0 spans, got %d", len(result.Spans))
		}
	})
}

// TestBlockQuerier_MatcherCombinations tests various matcher combinations
func TestBlockQuerier_MatcherCombinations(t *testing.T) {
	baseTime := time.Now()

	headStorage := storage.NewArrowStorage()
	defer headStorage.Release()

	// Create diverse spans
	spans := []*span.Span{
		{
			TraceID:     "trace-1",
			SpanID:      "span-1",
			Name:        "GET /api/users",
			StartTime:   baseTime,
			EndTime:     baseTime.Add(time.Millisecond),
			ServiceName: "api-service",
			Tags:        map[string]string{"env": "prod", "version": "1.0", "status": "200"},
		},
		{
			TraceID:     "trace-1",
			SpanID:      "span-2",
			Name:        "POST /api/users",
			StartTime:   baseTime,
			EndTime:     baseTime.Add(time.Millisecond),
			ServiceName: "api-service",
			Tags:        map[string]string{"env": "prod", "version": "2.0", "status": "201"},
		},
		{
			TraceID:     "trace-2",
			SpanID:      "span-3",
			Name:        "GET /api/orders",
			StartTime:   baseTime,
			EndTime:     baseTime.Add(time.Millisecond),
			ServiceName: "order-service",
			Tags:        map[string]string{"env": "dev", "version": "1.0", "status": "500"},
		},
	}

	for _, sp := range spans {
		headStorage.AddSpan(sp)
	}
	headStorage.Flush()

	querier := NewBlockQuerier(block.NewHeadBlock(headStorage), []block.Block{})

	t.Run("single matcher", func(t *testing.T) {
		matcher, _ := NewMatcher(MatchEqual, "env", "prod")
		result, err := querier.Select(matcher)
		if err != nil {
			t.Fatalf("BlockQuerier.Select() error = %v", err)
		}

		if len(result.Spans) != 2 {
			t.Errorf("Expected 2 spans, got %d", len(result.Spans))
		}
	})

	t.Run("multiple matchers - AND logic", func(t *testing.T) {
		m1, _ := NewMatcher(MatchEqual, "env", "prod")
		m2, _ := NewMatcher(MatchEqual, "version", "2.0")

		result, err := querier.Select(m1, m2)
		if err != nil {
			t.Fatalf("BlockQuerier.Select() error = %v", err)
		}

		if len(result.Spans) != 1 {
			t.Errorf("Expected 1 span, got %d", len(result.Spans))
		}
		if len(result.Spans) > 0 && result.Spans[0].SpanID != "span-2" {
			t.Errorf("Expected span-2, got %s", result.Spans[0].SpanID)
		}
	})

	t.Run("regexp matcher", func(t *testing.T) {
		matcher, _ := NewMatcher(MatchRegexp, "name", "GET.*")
		result, err := querier.Select(matcher)
		if err != nil {
			t.Fatalf("BlockQuerier.Select() error = %v", err)
		}

		if len(result.Spans) != 2 {
			t.Errorf("Expected 2 spans (both GETs), got %d", len(result.Spans))
		}
	})

	t.Run("not equal matcher", func(t *testing.T) {
		matcher, _ := NewMatcher(MatchNotEqual, "env", "prod")
		result, err := querier.Select(matcher)
		if err != nil {
			t.Fatalf("BlockQuerier.Select() error = %v", err)
		}

		if len(result.Spans) != 1 {
			t.Errorf("Expected 1 span (env=dev), got %d", len(result.Spans))
		}
	})

	t.Run("trace_id matcher", func(t *testing.T) {
		matcher, _ := NewMatcher(MatchEqual, "trace_id", "trace-1")
		result, err := querier.Select(matcher)
		if err != nil {
			t.Fatalf("BlockQuerier.Select() error = %v", err)
		}

		if len(result.Spans) != 2 {
			t.Errorf("Expected 2 spans for trace-1, got %d", len(result.Spans))
		}
	})
}
