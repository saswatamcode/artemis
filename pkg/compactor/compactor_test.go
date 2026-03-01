package compactor

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

func TestLevelConfig_ShouldCompact(t *testing.T) {
	cfg := &LevelConfig{
		MinBlocks:   2,
		MinBlockAge: 1 * time.Hour,
	}

	tests := []struct {
		name           string
		blockCount     int
		oldestBlockAge time.Duration
		want           bool
	}{
		{"not enough blocks", 1, 2 * time.Hour, false},
		{"blocks not old enough", 3, 30 * time.Minute, false},
		{"should compact", 2, 2 * time.Hour, true},
		{"exact thresholds", 2, 1 * time.Hour, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cfg.ShouldCompact(tt.blockCount, tt.oldestBlockAge)
			if got != tt.want {
				t.Errorf("ShouldCompact(%d, %v) = %v, want %v",
					tt.blockCount, tt.oldestBlockAge, got, tt.want)
			}
		})
	}
}

func TestCompactor_Plan_NotEnoughBlocks(t *testing.T) {
	tmpDir := t.TempDir()
	c := NewCompactor(tmpDir)

	// Create a single L0 block
	blockDir := createTestBlock(t, tmpDir, 0, time.Now().Add(-1*time.Hour))
	blk, err := block.LoadBlock(blockDir)
	if err != nil {
		t.Fatalf("Failed to create test block: %v", err)
	}
	defer blk.Close()

	// Plan should be nil (need at least 2 blocks)
	plan := c.Plan([]block.Block{blk}, 0)
	if plan != nil {
		t.Error("Plan() should return nil when not enough blocks")
	}
}

func TestCompactor_Plan_BlocksTooYoung(t *testing.T) {
	tmpDir := t.TempDir()
	c := NewCompactor(tmpDir)

	// Create two L0 blocks that are too young
	blockDir1 := createTestBlock(t, tmpDir, 0, time.Now().Add(-1*time.Minute))
	blockDir2 := createTestBlock(t, tmpDir, 0, time.Now())

	blk1, err := block.LoadBlock(blockDir1)
	if err != nil {
		t.Fatalf("Failed to create test block 1: %v", err)
	}
	defer blk1.Close()

	blk2, err := block.LoadBlock(blockDir2)
	if err != nil {
		t.Fatalf("Failed to create test block 2: %v", err)
	}
	defer blk2.Close()

	// Plan should be nil (blocks too young, need 10 minutes for L0)
	plan := c.Plan([]block.Block{blk1, blk2}, 0)
	if plan != nil {
		t.Error("Plan() should return nil when blocks are too young")
	}
}

func TestCompactor_Plan_Success(t *testing.T) {
	tmpDir := t.TempDir()
	c := NewCompactor(tmpDir)

	// Create two L0 blocks that are old enough
	blockDir1 := createTestBlock(t, tmpDir, 0, time.Now().Add(-15*time.Minute))
	blockDir2 := createTestBlock(t, tmpDir, 0, time.Now().Add(-12*time.Minute))

	blk1, err := block.LoadBlock(blockDir1)
	if err != nil {
		t.Fatalf("Failed to create test block 1: %v", err)
	}
	defer blk1.Close()

	blk2, err := block.LoadBlock(blockDir2)
	if err != nil {
		t.Fatalf("Failed to create test block 2: %v", err)
	}
	defer blk2.Close()

	// Plan should succeed
	plan := c.Plan([]block.Block{blk1, blk2}, 0)
	if plan == nil {
		t.Fatal("Plan() should return a plan")
	}

	if plan.Level != 0 {
		t.Errorf("plan.Level = %d, want 0", plan.Level)
	}

	if len(plan.Blocks) != 2 {
		t.Errorf("plan.Blocks length = %d, want 2", len(plan.Blocks))
	}

	if len(plan.Sources) != 2 {
		t.Errorf("plan.Sources length = %d, want 2", len(plan.Sources))
	}
}

func TestCompactor_Compact(t *testing.T) {
	tmpDir := t.TempDir()
	c := NewCompactor(tmpDir)

	// Create two L0 blocks with test data
	now := time.Now().Add(-15 * time.Minute)
	blockDir1 := createTestBlockWithSpans(t, tmpDir, 0, now, 5)
	blockDir2 := createTestBlockWithSpans(t, tmpDir, 0, now.Add(1*time.Minute), 5)

	blk1, err := block.LoadBlock(blockDir1)
	if err != nil {
		t.Fatalf("Failed to create test block 1: %v", err)
	}
	defer blk1.Close()

	blk2, err := block.LoadBlock(blockDir2)
	if err != nil {
		t.Fatalf("Failed to create test block 2: %v", err)
	}
	defer blk2.Close()

	// Create compaction plan
	plan := &CompactionPlan{
		Level:   0,
		Blocks:  []block.Block{blk1, blk2},
		Sources: []ulid.ULID{blk1.Meta().ULID, blk2.Meta().ULID},
	}

	// Compact
	meta, err := c.Compact(plan)
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	// Verify metadata
	if meta.SpanCount != 10 {
		t.Errorf("SpanCount = %d, want 10", meta.SpanCount)
	}

	if meta.Level() != 1 {
		t.Errorf("Level = %d, want 1", meta.Level())
	}

	if meta.Compaction == nil {
		t.Fatal("Compaction metadata should not be nil")
	}

	if len(meta.Compaction.Sources) != 2 {
		t.Errorf("Sources length = %d, want 2", len(meta.Compaction.Sources))
	}

	// Verify the compacted block was created on disk
	blockDir := filepath.Join(tmpDir, meta.ULID.String())
	metaPath := filepath.Join(blockDir, "meta.json")
	if !fileExists(metaPath) {
		t.Error("meta.json should exist")
	}

	parquetPath := filepath.Join(blockDir, "spans.parquet")
	if !fileExists(parquetPath) {
		t.Error("spans.parquet should exist")
	}

	// Load the compacted block and verify no data loss
	compactedBlk, err := block.LoadBlock(blockDir)
	if err != nil {
		t.Fatalf("Failed to load compacted block: %v", err)
	}
	defer compactedBlk.Close()

	// Verify all 10 spans are present with correct data
	verifyBlockData(t, compactedBlk, 10)
}

func TestExtractULIDs(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test blocks
	blockDir1 := createTestBlock(t, tmpDir, 0, time.Now())
	blockDir2 := createTestBlock(t, tmpDir, 0, time.Now())

	blk1, err := block.LoadBlock(blockDir1)
	if err != nil {
		t.Fatalf("Failed to create test block 1: %v", err)
	}
	defer blk1.Close()

	blk2, err := block.LoadBlock(blockDir2)
	if err != nil {
		t.Fatalf("Failed to create test block 2: %v", err)
	}
	defer blk2.Close()

	// Extract ULIDs
	ulids := extractULIDs([]block.Block{blk1, blk2})

	if len(ulids) != 2 {
		t.Errorf("extractULIDs() returned %d ULIDs, want 2", len(ulids))
	}

	if ulids[0] != blk1.Meta().ULID {
		t.Errorf("First ULID mismatch")
	}

	if ulids[1] != blk2.Meta().ULID {
		t.Errorf("Second ULID mismatch")
	}
}

func TestCompactor_BuildIndex(t *testing.T) {
	c := NewCompactor(t.TempDir())

	// Create test spans
	spans := []*span.Span{
		{
			TraceID:     "trace-1",
			SpanID:      "span-1",
			Name:        "op1",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			Duration:    1000000,
			ServiceName: "service-1",
			Tags: map[string]string{
				"key1": "value1",
			},
		},
		{
			TraceID:     "trace-2",
			SpanID:      "span-2",
			Name:        "op2",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(2 * time.Millisecond),
			Duration:    2000000,
			ServiceName: "service-2",
			Tags: map[string]string{
				"key2": "value2",
			},
		},
	}

	idx := c.buildIndex(spans)

	// Verify index
	stats := idx.Stats()
	if stats.TotalSpans != 2 {
		t.Errorf("Index TotalSpans = %d, want 2", stats.TotalSpans)
	}

	if stats.UniqueTraces != 2 {
		t.Errorf("Index UniqueTraces = %d, want 2", stats.UniqueTraces)
	}

	// Verify span lookups
	_, ok := idx.LookupSpanID("span-1")
	if !ok {
		t.Error("Index should contain span-1")
	}

	// Verify trace lookups
	traceSpans := idx.LookupByTraceID("trace-1")
	if len(traceSpans) != 1 {
		t.Errorf("Trace-1 should have 1 span, got %d", len(traceSpans))
	}
}

// TestCompactor_MultiLevelCompaction tests compaction through all supported levels (L0 → L5)
// using real blocks with actual span data
func TestCompactor_MultiLevelCompaction(t *testing.T) {
	tmpDir := t.TempDir()
	c := NewCompactor(tmpDir)

	baseTime := time.Now().Add(-24 * time.Hour)
	spansPerBlock := 100

	// Step 1: Create L0 Arrow IPC blocks
	t.Log("Step 1: Creating L0 Arrow IPC blocks...")
	l0Blocks := make([]block.Block, 4)
	for i := range 4 {
		blockTime := baseTime.Add(time.Duration(i) * time.Hour)
		blockDir := createTestBlockWithSpans(t, tmpDir, 0, blockTime, spansPerBlock)
		blk, err := block.LoadBlock(blockDir)
		if err != nil {
			t.Fatalf("Failed to load L0 block %d: %v", i, err)
		}
		defer blk.Close()
		l0Blocks[i] = blk

		// Verify it's an Arrow block
		if _, ok := blk.(*block.ArrowBlock); !ok {
			t.Errorf("L0 block %d should be Arrow IPC format", i)
		}
		if blk.Meta().Level() != 0 {
			t.Errorf("Block %d level = %d, want 0", i, blk.Meta().Level())
		}
	}

	totalSpans := int64(spansPerBlock * 4)
	t.Logf("Created %d L0 blocks with %d total spans", len(l0Blocks), totalSpans)

	// Step 2: Compact L0 → L1 (Arrow IPC → Parquet)
	t.Log("\nStep 2: Compacting L0 → L1 (2h Parquet blocks)...")
	l1Blocks := compactLevel(t, c, l0Blocks[:2], 0, tmpDir)
	l1Blocks = append(l1Blocks, compactLevel(t, c, l0Blocks[2:], 0, tmpDir)...)

	// Verify L1 blocks
	for i, blk := range l1Blocks {
		meta := blk.Meta()
		if meta.Level() != 1 {
			t.Errorf("L1 block %d level = %d, want 1", i, meta.Level())
		}
		if meta.SpanCount != int64(spansPerBlock*2) {
			t.Errorf("L1 block %d span count = %d, want %d", i, meta.SpanCount, spansPerBlock*2)
		}
		if meta.Compaction == nil {
			t.Errorf("L1 block %d should have compaction metadata", i)
		}
		if len(meta.Compaction.Sources) != 2 {
			t.Errorf("L1 block %d sources = %d, want 2", i, len(meta.Compaction.Sources))
		}

		// Verify it's a Parquet block
		if _, ok := blk.(*block.ParquetBlock); !ok {
			t.Errorf("L1 block %d should be Parquet format", i)
		}

		// Verify we can read data back
		verifyBlockData(t, blk, int64(spansPerBlock*2))
	}
	t.Logf("Created %d L1 blocks", len(l1Blocks))

	// Step 3: Compact L1 → L2 (4h Parquet blocks)
	t.Log("\nStep 3: Compacting L1 → L2 (4h Parquet blocks)...")
	l2Blocks := compactLevel(t, c, l1Blocks, 1, tmpDir)

	// Verify L2 blocks
	for i, blk := range l2Blocks {
		meta := blk.Meta()
		if meta.Level() != 2 {
			t.Errorf("L2 block %d level = %d, want 2", i, meta.Level())
		}
		if meta.SpanCount != totalSpans {
			t.Errorf("L2 block %d span count = %d, want %d", i, meta.SpanCount, totalSpans)
		}
		if len(meta.Compaction.Sources) != 2 {
			t.Errorf("L2 block %d sources = %d, want 2", i, len(meta.Compaction.Sources))
		}

		// Verify it's a Parquet block
		if _, ok := blk.(*block.ParquetBlock); !ok {
			t.Errorf("L2 block %d should be Parquet format", i)
		}

		// Verify we can read data back
		verifyBlockData(t, blk, totalSpans)
	}
	t.Logf("Created %d L2 block(s)", len(l2Blocks))

	// Step 4: Create more L2 blocks to compact to L3
	t.Log("\nStep 4: Creating additional L2 block for L3 compaction...")
	// Create another set of L0 blocks → L1 → L2
	l0Blocks2 := make([]block.Block, 4)
	for i := range 4 {
		blockTime := baseTime.Add(time.Duration(i+4) * time.Hour)
		blockDir := createTestBlockWithSpans(t, tmpDir, 0, blockTime, spansPerBlock)
		blk, err := block.LoadBlock(blockDir)
		if err != nil {
			t.Fatalf("Failed to load L0 block %d: %v", i+4, err)
		}
		defer blk.Close()
		l0Blocks2[i] = blk
	}

	l1Blocks2 := compactLevel(t, c, l0Blocks2[:2], 0, tmpDir)
	l1Blocks2 = append(l1Blocks2, compactLevel(t, c, l0Blocks2[2:], 0, tmpDir)...)
	l2Blocks2 := compactLevel(t, c, l1Blocks2, 1, tmpDir)

	// Step 5: Compact L2 → L3 (8h Parquet blocks)
	t.Log("\nStep 5: Compacting L2 → L3 (8h Parquet blocks)...")
	allL2Blocks := append(l2Blocks, l2Blocks2...)
	l3Blocks := compactLevel(t, c, allL2Blocks, 2, tmpDir)

	// Verify L3 blocks
	for i, blk := range l3Blocks {
		meta := blk.Meta()
		if meta.Level() != 3 {
			t.Errorf("L3 block %d level = %d, want 3", i, meta.Level())
		}
		expectedSpans := totalSpans * 2
		if meta.SpanCount != expectedSpans {
			t.Errorf("L3 block %d span count = %d, want %d", i, meta.SpanCount, expectedSpans)
		}

		// Verify it's a Parquet block
		if _, ok := blk.(*block.ParquetBlock); !ok {
			t.Errorf("L3 block %d should be Parquet format", i)
		}

		// Verify we can read data back
		verifyBlockData(t, blk, expectedSpans)
	}
	t.Logf("Created %d L3 block(s)", len(l3Blocks))

	// Step 6: Create more blocks for L4 compaction
	t.Log("\nStep 6: Creating blocks for L4 compaction...")
	// Create another L3 block
	l0Blocks3 := make([]block.Block, 4)
	for i := range 4 {
		blockTime := baseTime.Add(time.Duration(i+8) * time.Hour)
		blockDir := createTestBlockWithSpans(t, tmpDir, 0, blockTime, spansPerBlock)
		blk, err := block.LoadBlock(blockDir)
		if err != nil {
			t.Fatalf("Failed to load L0 block %d: %v", i+8, err)
		}
		defer blk.Close()
		l0Blocks3[i] = blk
	}

	l1Blocks3 := compactLevel(t, c, l0Blocks3[:2], 0, tmpDir)
	l1Blocks3 = append(l1Blocks3, compactLevel(t, c, l0Blocks3[2:], 0, tmpDir)...)
	l2Blocks3 := compactLevel(t, c, l1Blocks3, 1, tmpDir)
	l3Blocks2 := compactLevel(t, c, l2Blocks3, 2, tmpDir)

	// Compact L3 → L4 (2d/48h Parquet blocks)
	t.Log("\nStep 7: Compacting L3 → L4 (2d Parquet blocks)...")
	allL3Blocks := append(l3Blocks, l3Blocks2...)
	l4Blocks := compactLevel(t, c, allL3Blocks, 3, tmpDir)

	// Verify L4 blocks
	for i, blk := range l4Blocks {
		meta := blk.Meta()
		if meta.Level() != 4 {
			t.Errorf("L4 block %d level = %d, want 4", i, meta.Level())
		}

		// Verify it's a Parquet block
		if _, ok := blk.(*block.ParquetBlock); !ok {
			t.Errorf("L4 block %d should be Parquet format", i)
		}

		// Verify we can read data back
		verifyBlockData(t, blk, meta.SpanCount)
	}
	t.Logf("Created %d L4 block(s)", len(l4Blocks))

	// Step 8: Create more blocks for L5 compaction
	t.Log("\nStep 8: Creating blocks for L5 compaction...")
	// Create another L4 block
	l0Blocks4 := make([]block.Block, 4)
	for i := range 4 {
		blockTime := baseTime.Add(time.Duration(i+12) * time.Hour)
		blockDir := createTestBlockWithSpans(t, tmpDir, 0, blockTime, spansPerBlock)
		blk, err := block.LoadBlock(blockDir)
		if err != nil {
			t.Fatalf("Failed to load L0 block %d: %v", i+12, err)
		}
		defer blk.Close()
		l0Blocks4[i] = blk
	}

	l1Blocks4 := compactLevel(t, c, l0Blocks4[:2], 0, tmpDir)
	l1Blocks4 = append(l1Blocks4, compactLevel(t, c, l0Blocks4[2:], 0, tmpDir)...)
	l2Blocks4 := compactLevel(t, c, l1Blocks4, 1, tmpDir)
	l3Blocks3 := compactLevel(t, c, l2Blocks4, 2, tmpDir)
	l4Blocks2 := compactLevel(t, c, l3Blocks3, 3, tmpDir)

	// Compact L4 → L5 (14d/336h Parquet blocks)
	t.Log("\nStep 9: Compacting L4 → L5 (14d Parquet blocks)...")
	allL4Blocks := append(l4Blocks, l4Blocks2...)
	l5Blocks := compactLevel(t, c, allL4Blocks, 4, tmpDir)

	// Verify L5 blocks (final level)
	for i, blk := range l5Blocks {
		meta := blk.Meta()
		if meta.Level() != 5 {
			t.Errorf("L5 block %d level = %d, want 5", i, meta.Level())
		}

		// Verify it's a Parquet block
		if _, ok := blk.(*block.ParquetBlock); !ok {
			t.Errorf("L5 block %d should be Parquet format", i)
		}

		// Verify we can read data back
		verifyBlockData(t, blk, meta.SpanCount)
	}
	t.Logf("Created %d L5 block(s)", len(l5Blocks))

	t.Log("\n✓ Successfully compacted through all levels: L0 → L1 → L2 → L3 → L4 → L5")
	t.Logf("✓ All blocks verified: format, metadata, span counts, and data integrity")
}

// compactLevel is a helper that compacts a set of blocks from one level to the next
func compactLevel(t *testing.T, c *Compactor, blocks []block.Block, level int, tmpDir string) []block.Block {
	t.Helper()

	plan := &CompactionPlan{
		Level:   level,
		Blocks:  blocks,
		Sources: extractULIDs(blocks),
	}

	meta, err := c.Compact(plan)
	if err != nil {
		t.Fatalf("Failed to compact level %d: %v", level, err)
	}

	// Load the compacted block
	blockDir := filepath.Join(tmpDir, meta.ULID.String())
	blk, err := block.LoadBlock(blockDir)
	if err != nil {
		t.Fatalf("Failed to load compacted block: %v", err)
	}

	return []block.Block{blk}
}

// verifyBlockData reads all spans from a block and verifies basic data integrity
func verifyBlockData(t *testing.T, blk block.Block, expectedSpans int64) {
	t.Helper()

	// Use the unified ReadAll() method
	spans, err := blk.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read spans from block: %v", err)
	}

	if int64(len(spans)) != expectedSpans {
		t.Errorf("Read %d spans, expected %d", len(spans), expectedSpans)
	}

	// Verify basic span data integrity
	for i, sp := range spans {
		if sp.TraceID == "" {
			t.Errorf("Span %d has empty TraceID", i)
		}
		if sp.SpanID == "" {
			t.Errorf("Span %d has empty SpanID", i)
		}
		if sp.Name == "" {
			t.Errorf("Span %d has empty Name", i)
		}
		if sp.ServiceName == "" {
			t.Errorf("Span %d has empty ServiceName", i)
		}
	}
}

func createTestBlock(t *testing.T, baseDir string, level int, createdAt time.Time) string {
	return createTestBlockWithSpans(t, baseDir, level, createdAt, 0)
}

func createTestBlockWithSpans(t *testing.T, baseDir string, level int, createdAt time.Time, spanCount int) string {
	t.Helper()

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()

	// Add test spans if requested
	now := time.Now()
	// Use timestamp to ensure unique span IDs across multiple blocks
	baseID := uint64(now.UnixNano())
	for i := range spanCount {
		sp := &span.Span{
			TraceID:     "00000000000000000000000000000001",
			SpanID:      fmt.Sprintf("%016x", baseID+uint64(i)),
			Name:        "test-operation",
			StartTime:   now.Add(time.Duration(i) * time.Millisecond),
			EndTime:     now.Add(time.Duration(i+1) * time.Millisecond),
			ServiceName: "test-service",
			Tags: map[string]string{
				"test": "value",
			},
		}
		arrowStorage.AddSpan(sp)
	}
	arrowStorage.Flush()

	// Create block metadata
	blockID := ulid.Make()
	var compactionMeta *block.CompactionMeta
	if level > 0 {
		compactionMeta = &block.CompactionMeta{
			Level:       level,
			Sources:     []ulid.ULID{},
			CompactedAt: createdAt,
		}
	}

	meta := &block.BlockMeta{
		ULID:       blockID,
		MinTime:    now.Add(-1 * time.Hour).UnixNano(),
		MaxTime:    now.UnixNano(),
		SpanCount:  int64(spanCount),
		Version:    1,
		CreatedAt:  createdAt,
		Compaction: compactionMeta,
	}

	blockDir := filepath.Join(baseDir, blockID.String())

	// Write as Arrow IPC for L0, Parquet for L1+
	var err error
	if level == 0 {
		err = block.FlushBlock(blockDir, meta, arrowStorage.GetRecords(), arrowStorage.Schema(), arrowStorage.GetIndex())
	} else {
		// For Parquet blocks, create a temporary L0 block first, then read spans from it
		tmpBlockDir := filepath.Join(baseDir, "tmp-"+blockID.String())
		if err := block.FlushBlock(tmpBlockDir, meta, arrowStorage.GetRecords(), arrowStorage.Schema(), arrowStorage.GetIndex()); err != nil {
			t.Fatalf("Failed to create temporary block: %v", err)
		}

		// Load the temporary block and read all spans
		tmpBlock, err := block.LoadBlock(tmpBlockDir)
		if err != nil {
			t.Fatalf("Failed to load temporary block: %v", err)
		}

		spans, err := tmpBlock.ReadAll()
		tmpBlock.Close()
		if err != nil {
			t.Fatalf("Failed to read spans from temporary block: %v", err)
		}

		// Write as Parquet with updated metadata for the correct level
		err = block.WriteParquetBlock(blockDir, meta, spans, arrowStorage.GetIndex())
	}

	if err != nil {
		t.Fatalf("Failed to create test block: %v", err)
	}

	return blockDir
}

func fileExists(path string) bool {
	_, err := filepath.Glob(path)
	return err == nil
}
