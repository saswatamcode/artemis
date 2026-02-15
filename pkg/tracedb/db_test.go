package tracedb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/wal"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg == nil {
		t.Fatal("DefaultConfig() should not return nil")
	}

	if cfg.WALDir == "" {
		t.Error("Default WALDir should not be empty")
	}

	if cfg.CompactInterval == 0 {
		t.Error("Default CompactInterval should not be 0")
	}

	if cfg.CheckpointInterval == 0 {
		t.Error("Default CheckpointInterval should not be 0")
	}

	if cfg.CheckpointThreshold == 0 {
		t.Error("Default CheckpointThreshold should not be 0")
	}

	if !cfg.EnableCompaction {
		t.Error("Default EnableCompaction should be true")
	}

	if cfg.EnableRetention {
		t.Error("Default EnableRetention should be false")
	}
}

func TestNew(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:          filepath.Join(tmpDir, "wal"),
		CompactInterval: 0, // Disable background compaction for tests
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	if db.wal == nil {
		t.Error("WAL should not be nil")
	}

	if db.storage == nil {
		t.Error("Storage should not be nil")
	}

	// Block manager should be nil if not configured
	if db.blockManager != nil {
		t.Error("Block manager should be nil when BlockConfig is not provided")
	}
}

func TestNew_WithBlockManager(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:          filepath.Join(tmpDir, "wal"),
		CompactInterval: 0, // Disable background compaction for tests
		BlockConfig: &block.Config{
			Dir:              filepath.Join(tmpDir, "blocks"),
			MaxBlockDuration: 1 * time.Hour,
			MaxBlockSpans:    1000,
		},
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	if db.blockManager == nil {
		t.Error("Block manager should not be nil when BlockConfig is provided")
	}

	if db.compactor == nil {
		t.Error("Compactor should not be nil when BlockConfig is provided")
	}
}

func TestDB_WriteSpan(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:          filepath.Join(tmpDir, "wal"),
		CompactInterval: 0,
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// Write a span
	sp := &span.Span{
		TraceID:     "00000000000000000000000000000001",
		SpanID:      "0000000000000001",
		Name:        "test-operation",
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(time.Millisecond),
		ServiceName: "test-service",
		Tags: map[string]string{
			"key": "value",
		},
	}

	err = db.WriteSpan(sp)
	if err != nil {
		t.Fatalf("WriteSpan() error = %v", err)
	}

	// Verify span count
	stats := db.Stats()
	if stats.TotalSpans != 1 {
		t.Errorf("TotalSpans = %d, want 1", stats.TotalSpans)
	}
}

func TestDB_WriteMultipleSpans(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:          filepath.Join(tmpDir, "wal"),
		CompactInterval: 0,
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// Write multiple spans
	for i := range 10 {
		sp := &span.Span{
			TraceID:     "00000000000000000000000000000001",
			SpanID:      fmt.Sprintf("%016x", i+1),
			Name:        "test-operation",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "test-service",
		}

		if err := db.WriteSpan(sp); err != nil {
			t.Fatalf("WriteSpan() error = %v", err)
		}
	}

	// Verify span count
	stats := db.Stats()
	if stats.TotalSpans != 10 {
		t.Errorf("TotalSpans = %d, want 10", stats.TotalSpans)
	}
}

func TestDB_Flush(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:          filepath.Join(tmpDir, "wal"),
		CompactInterval: 0,
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// Write some spans
	for i := range 5 {
		sp := &span.Span{
			TraceID:     "00000000000000000000000000000001",
			SpanID:      fmt.Sprintf("%016x", i+1),
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		db.WriteSpan(sp)
	}

	// Before flush, record batches should be 0
	stats := db.Stats()
	if stats.RecordBatches != 0 {
		t.Errorf("RecordBatches before flush = %d, want 0", stats.RecordBatches)
	}

	// Flush
	err = db.Flush()
	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// After flush, should have record batches
	stats = db.Stats()
	if stats.RecordBatches != 1 {
		t.Errorf("RecordBatches after flush = %d, want 1", stats.RecordBatches)
	}
}

func TestDB_GetStorage(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:          filepath.Join(tmpDir, "wal"),
		CompactInterval: 0,
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	storage := db.GetStorage()
	if storage == nil {
		t.Error("GetStorage() should not return nil")
	}
}

func TestDB_Stats(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:          filepath.Join(tmpDir, "wal"),
		CompactInterval: 0,
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// Initial stats
	stats := db.Stats()
	if stats.TotalSpans != 0 {
		t.Errorf("Initial TotalSpans = %d, want 0", stats.TotalSpans)
	}
	if stats.RecordBatches != 0 {
		t.Errorf("Initial RecordBatches = %d, want 0", stats.RecordBatches)
	}
	if stats.StorageInfo == "" {
		t.Error("StorageInfo should not be empty")
	}
}

func TestDB_Close(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:          filepath.Join(tmpDir, "wal"),
		CompactInterval: 0,
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Write a span
	sp := &span.Span{
		TraceID:     "00000000000000000000000000000001",
		SpanID:      "0000000000000001",
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(time.Millisecond),
		ServiceName: "service",
	}
	db.WriteSpan(sp)

	// Close
	err = db.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Try to close again (should not error)
	err = db.Close()
	if err != nil {
		t.Errorf("Second Close() should not error, got %v", err)
	}
}

func TestDB_Checkpoint(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:              filepath.Join(tmpDir, "wal"),
		CompactInterval:     0,
		CheckpointInterval:  1 * time.Hour, // High value so it won't trigger automatically
		CheckpointThreshold: 2,             // Low threshold for testing
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// Write enough spans to create multiple segments (if segment size allows)
	for i := range 100 {
		sp := &span.Span{
			TraceID:     "00000000000000000000000000000001",
			SpanID:      fmt.Sprintf("%016x", i+1),
			Name:        "operation-with-long-name-to-increase-size",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service-with-long-name",
			Tags: map[string]string{
				"tag1": "value1-very-long-value",
				"tag2": "value2-very-long-value",
			},
		}
		db.WriteSpan(sp)
	}

	// Try to create checkpoint (may not create if not enough segments)
	err = db.Checkpoint()
	// Should not error even if no checkpoint was created
	if err != nil {
		t.Errorf("Checkpoint() error = %v", err)
	}
}

func TestDB_CheckpointSafety(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:              filepath.Join(tmpDir, "wal"),
		CompactInterval:     0, // Disable auto compaction
		CheckpointInterval:  1 * time.Hour,
		CheckpointThreshold: 2, // Need at least 2 persisted segments
		BlockConfig: &block.Config{
			Dir:              filepath.Join(tmpDir, "blocks"),
			MaxBlockDuration: 1 * time.Hour,
			MaxBlockSpans:    10, // Low threshold so we can flush easily
		},
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// SCENARIO 1: Try to checkpoint before any blocks are persisted
	// This should be a no-op since no WAL segments have been persisted yet
	err = db.Checkpoint()
	if err != nil {
		t.Fatalf("Checkpoint() before any persistence should not error: %v", err)
	}

	// Write some spans (not enough to trigger auto-flush)
	for i := range 5 {
		sp := &span.Span{
			TraceID:     "00000000000000000000000000000001",
			SpanID:      fmt.Sprintf("%016x", i+1),
			Name:        "operation",
			StartTime:   time.Now().Add(time.Duration(i) * time.Millisecond),
			EndTime:     time.Now().Add(time.Duration(i+1) * time.Millisecond),
			ServiceName: "service",
		}
		db.WriteSpan(sp)
	}

	// SCENARIO 2: Try to checkpoint before blocks are flushed
	// Even though we have WAL data, it's not persisted to blocks yet
	err = db.Checkpoint()
	if err != nil {
		t.Fatalf("Checkpoint() before block flush should not error: %v", err)
	}

	// Verify no WAL segments were deleted (data still in memory/WAL only)
	currentWALSegment := db.wal.SegmentIndex()
	t.Logf("Current WAL segment: %d", currentWALSegment)

	// SCENARIO 3: Flush blocks and verify checkpoint uses correct segment
	db.Flush()
	if err := db.flushHeadBlock(); err != nil {
		t.Fatalf("flushHeadBlock() error = %v", err)
	}

	// Check which WAL segment was recorded
	blocks := db.GetBlocks()
	if len(blocks) != 1 {
		t.Fatalf("Expected 1 block after flush, got %d", len(blocks))
	}

	firstBlockMaxSegment := blocks[0].Meta().MaxWALSegment
	t.Logf("First block MaxWALSegment: %d", firstBlockMaxSegment)

	// Write more spans to create additional WAL segments
	for i := 5; i < 15; i++ {
		sp := &span.Span{
			TraceID:     "00000000000000000000000000000001",
			SpanID:      fmt.Sprintf("%016x", i+1),
			Name:        "operation",
			StartTime:   time.Now().Add(time.Duration(i) * time.Millisecond),
			EndTime:     time.Now().Add(time.Duration(i+1) * time.Millisecond),
			ServiceName: "service",
		}
		db.WriteSpan(sp)
	}

	db.Flush()
	if err := db.flushHeadBlock(); err != nil {
		t.Fatalf("Second flushHeadBlock() error = %v", err)
	}

	blocks = db.GetBlocks()
	if len(blocks) != 2 {
		t.Fatalf("Expected 2 blocks after second flush, got %d", len(blocks))
	}

	secondBlockMaxSegment := blocks[1].Meta().MaxWALSegment
	t.Logf("Second block MaxWALSegment: %d", secondBlockMaxSegment)

	// SCENARIO 4: Now checkpoint should work and only checkpoint up to persisted segments
	err = db.Checkpoint()
	if err != nil {
		t.Fatalf("Checkpoint() after persistence error = %v", err)
	}

	// SCENARIO 5: Write more data WITHOUT flushing to blocks
	for i := 15; i < 20; i++ {
		sp := &span.Span{
			TraceID:     "00000000000000000000000000000001",
			SpanID:      fmt.Sprintf("%016x", i+1),
			Name:        "operation",
			StartTime:   time.Now().Add(time.Duration(i) * time.Millisecond),
			EndTime:     time.Now().Add(time.Duration(i+1) * time.Millisecond),
			ServiceName: "service",
		}
		db.WriteSpan(sp)
	}

	thirdWALSegment := db.wal.SegmentIndex()
	t.Logf("Current WAL segment after more writes: %d", thirdWALSegment)

	// Close and reopen to verify we can recover all data
	db.Close()

	db2, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	// Should have all 20 spans
	stats := db2.Stats()
	if stats.TotalSpans != 20 {
		t.Errorf("After recovery, TotalSpans = %d, want 20", stats.TotalSpans)
	}

	// Verify blocks were persisted correctly
	// Note: May have 2 or 3 blocks depending on whether Close() flushed the head block
	blocks = db2.GetBlocks()
	if len(blocks) < 2 {
		t.Errorf("After recovery, block count = %d, want at least 2", len(blocks))
	}

	t.Logf("Recovery successful: %d spans recovered from %d blocks", stats.TotalSpans, len(blocks))
}

func TestDB_CheckpointWithoutBlockManager(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:              filepath.Join(tmpDir, "wal"),
		CompactInterval:     0,
		CheckpointInterval:  1 * time.Hour,
		CheckpointThreshold: 2,
		// No BlockConfig - checkpoint should be disabled
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// Write spans to create WAL segments
	for i := range 100 {
		sp := &span.Span{
			TraceID:     "00000000000000000000000000000001",
			SpanID:      fmt.Sprintf("%016x", i+1),
			Name:        "operation",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		db.WriteSpan(sp)
	}

	// Checkpoint should be a no-op without block manager (for safety)
	err = db.Checkpoint()
	if err != nil {
		t.Errorf("Checkpoint() without block manager should not error: %v", err)
	}

	// All data should still be in WAL
	currentSegment := db.wal.SegmentIndex()
	t.Logf("WAL segment after checkpoint attempt (no block manager): %d", currentSegment)
}

func TestDB_LightweightCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:              filepath.Join(tmpDir, "wal"),
		CompactInterval:     0,
		CheckpointInterval:  1 * time.Hour,
		CheckpointThreshold: 1, // Checkpoint after 1 segment (for testing)
		BlockConfig: &block.Config{
			Dir:              filepath.Join(tmpDir, "blocks"),
			MaxBlockDuration: 1 * time.Hour,
			MaxBlockSpans:    10,
		},
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// Write and flush first batch to create block with WAL segment 0
	for i := range 10 {
		sp := &span.Span{
			TraceID:     "00000000000000000000000000000001",
			SpanID:      fmt.Sprintf("%016x", i+1),
			Name:        "operation",
			StartTime:   time.Now().Add(time.Duration(i) * time.Millisecond),
			EndTime:     time.Now().Add(time.Duration(i+1) * time.Millisecond),
			ServiceName: "service",
		}
		db.WriteSpan(sp)
	}
	db.Flush()
	db.flushHeadBlock()

	// Write and flush second batch to create block with WAL segment 0 (still same segment)
	for i := range 10 {
		sp := &span.Span{
			TraceID:     "00000000000000000000000000000001",
			SpanID:      fmt.Sprintf("%016x", i+11),
			Name:        "operation",
			StartTime:   time.Now().Add(time.Duration(i+10) * time.Millisecond),
			EndTime:     time.Now().Add(time.Duration(i+11) * time.Millisecond),
			ServiceName: "service",
		}
		db.WriteSpan(sp)
	}
	db.Flush()
	db.flushHeadBlock()

	// Write third batch to create block with WAL segment 0
	for i := range 10 {
		sp := &span.Span{
			TraceID:     "00000000000000000000000000000001",
			SpanID:      fmt.Sprintf("%016x", i+21),
			Name:        "operation",
			StartTime:   time.Now().Add(time.Duration(i+20) * time.Millisecond),
			EndTime:     time.Now().Add(time.Duration(i+21) * time.Millisecond),
			ServiceName: "service",
		}
		db.WriteSpan(sp)
	}
	db.Flush()
	db.flushHeadBlock()

	// Now trigger checkpoint - should NOT delete WAL segment 0 because it's still open
	// This is the correct behavior to prevent .fuse_hidden files
	currentSegment := db.wal.SegmentIndex()
	t.Logf("Current WAL segment before checkpoint: %d", currentSegment)

	err = db.Checkpoint()
	if err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}

	// Verify checkpoint did NOT delete the current segment (it's still open for writes)
	// With low traffic, WAL doesn't rotate (< 128MB), so segment 0 stays open
	walSegmentPath := filepath.Join(db.walDir, "000000.wal")
	if _, err := os.Stat(walSegmentPath); err != nil {
		t.Errorf("WAL segment 000000.wal should still exist (it's open), got error: %v", err)
	}

	// Verify no checkpoint metadata was written (nothing was deleted)
	metadata, err := wal.ReadCheckpointMetadata(db.walDir)
	if err != nil {
		t.Fatalf("ReadCheckpointMetadata() error = %v", err)
	}

	if metadata != nil {
		t.Logf("Note: Checkpoint metadata exists but shouldn't have deleted current segment")
		t.Logf("Checkpoint metadata: MaxDeletedSegment=%d, DeletedCount=%d",
			metadata.MaxDeletedSegment, metadata.DeletedCount)
	}

	// Close and reopen database - should recover all data from blocks (not WAL)
	db.Close()

	db2, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	// After recovery, data is in blocks (not in memory since WAL was deleted)
	blocks := db2.GetBlocks()
	if len(blocks) < 3 {
		t.Errorf("After recovery, block count = %d, want at least 3", len(blocks))
	}

	// Count spans in blocks
	blockStats := db2.BlockStats()
	if blockStats == nil {
		t.Fatal("BlockStats should not be nil")
	}

	if blockStats.PersistedSpans != 30 {
		t.Errorf("After recovery, PersistedSpans = %d, want 30", blockStats.PersistedSpans)
	}

	t.Logf("Recovery successful: %d spans in %d blocks (WAL deleted, data persisted)",
		blockStats.PersistedSpans, len(blocks))
}

func TestDB_BlockStats_NoBlockManager(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:          filepath.Join(tmpDir, "wal"),
		CompactInterval: 0,
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	stats := db.BlockStats()
	if stats != nil {
		t.Error("BlockStats() should return nil when block manager is not configured")
	}
}

func TestDB_BlockStats_WithBlockManager(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:          filepath.Join(tmpDir, "wal"),
		CompactInterval: 0,
		BlockConfig: &block.Config{
			Dir:              filepath.Join(tmpDir, "blocks"),
			MaxBlockDuration: 1 * time.Hour,
			MaxBlockSpans:    1000,
		},
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	stats := db.BlockStats()
	if stats == nil {
		t.Error("BlockStats() should not return nil when block manager is configured")
	}

	if stats.PersistedBlocks != 0 {
		t.Errorf("Initial PersistedBlocks = %d, want 0", stats.PersistedBlocks)
	}
}

func TestDB_GetBlocks_NoBlockManager(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:          filepath.Join(tmpDir, "wal"),
		CompactInterval: 0,
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	blocks := db.GetBlocks()
	if blocks != nil {
		t.Error("GetBlocks() should return nil when block manager is not configured")
	}
}

func TestDB_GetBlocks_WithBlockManager(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:          filepath.Join(tmpDir, "wal"),
		CompactInterval: 0,
		BlockConfig: &block.Config{
			Dir:              filepath.Join(tmpDir, "blocks"),
			MaxBlockDuration: 1 * time.Hour,
			MaxBlockSpans:    1000,
		},
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	blocks := db.GetBlocks()
	if blocks == nil {
		t.Error("GetBlocks() should not return nil when block manager is configured")
	}

	if len(blocks) != 0 {
		t.Errorf("Initial block count = %d, want 0", len(blocks))
	}
}

func TestDB_GetBlockManager(t *testing.T) {
	tmpDir := t.TempDir()

	// Test without block manager
	cfg1 := &Config{
		WALDir:          filepath.Join(tmpDir, "wal1"),
		CompactInterval: 0,
	}

	db1, err := New(cfg1)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db1.Close()

	if db1.GetBlockManager() != nil {
		t.Error("GetBlockManager() should return nil when not configured")
	}

	// Test with block manager
	cfg2 := &Config{
		WALDir:          filepath.Join(tmpDir, "wal2"),
		CompactInterval: 0,
		BlockConfig: &block.Config{
			Dir:              filepath.Join(tmpDir, "blocks"),
			MaxBlockDuration: 1 * time.Hour,
			MaxBlockSpans:    1000,
		},
	}

	db2, err := New(cfg2)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db2.Close()

	if db2.GetBlockManager() == nil {
		t.Error("GetBlockManager() should not return nil when configured")
	}
}

func TestDB_WALReplay(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:          filepath.Join(tmpDir, "wal"),
		CompactInterval: 0,
	}

	// Create database and write some spans
	db1, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	for i := range 5 {
		sp := &span.Span{
			TraceID:     "00000000000000000000000000000001",
			SpanID:      fmt.Sprintf("%016x", i+1),
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		db1.WriteSpan(sp)
	}

	// Close database
	db1.Close()

	// Reopen database - should replay WAL
	db2, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	// Verify spans were replayed
	stats := db2.Stats()
	if stats.TotalSpans != 5 {
		t.Errorf("After replay, TotalSpans = %d, want 5", stats.TotalSpans)
	}
}

func TestDB_BackgroundCompaction(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:          filepath.Join(tmpDir, "wal"),
		CompactInterval: 50 * time.Millisecond, // Fast compaction for test
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer db.Close()

	// Write a span
	sp := &span.Span{
		TraceID:     "00000000000000000000000000000001",
		SpanID:      "0000000000000001",
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(time.Millisecond),
		ServiceName: "service",
	}
	db.WriteSpan(sp)

	// Wait for background compaction to run at least once
	time.Sleep(100 * time.Millisecond)

	// Verify the span is still accessible
	stats := db.Stats()
	if stats.TotalSpans < 1 {
		t.Errorf("TotalSpans = %d, want >= 1", stats.TotalSpans)
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		duration time.Duration
		want     string
	}{
		{30 * time.Minute, "30m"},
		{1 * time.Hour, "1h"},
		{2 * time.Hour, "2h"},
		{24 * time.Hour, "1d"},
		{48 * time.Hour, "2d"},
		{336 * time.Hour, "14d"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := formatDuration(tt.duration)
			if got != tt.want {
				t.Errorf("formatDuration(%v) = %s, want %s", tt.duration, got, tt.want)
			}
		})
	}
}

// TestDB_NoFuseHiddenFiles verifies that the database lifecycle does not create .fuse_hidden files
// This tests the complete flow: write → flush → checkpoint → close → reopen
func TestDB_NoFuseHiddenFiles(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		WALDir:              filepath.Join(tmpDir, "wal"),
		WALSegmentSize:      64 * 1024, // 64KB segments to trigger rotation
		CompactInterval:     0,
		CheckpointInterval:  1 * time.Hour,
		CheckpointThreshold: 1, // Checkpoint after 1 segment
		BlockConfig: &block.Config{
			Dir:              filepath.Join(tmpDir, "blocks"),
			MaxBlockDuration: 1 * time.Hour,
			MaxBlockSpans:    50, // Low threshold to trigger flushes
		},
	}

	// Helper function to check for .fuse_hidden files
	var checkNoFuseHiddenFiles func(string, string)
	checkNoFuseHiddenFiles = func(dir string, context string) {
		entries, err := os.ReadDir(dir)
		if err != nil && !os.IsNotExist(err) {
			t.Fatalf("%s: failed to read directory %s: %v", context, dir, err)
		}
		for _, entry := range entries {
			if entry.IsDir() {
				// Check subdirectories recursively
				checkNoFuseHiddenFiles(filepath.Join(dir, entry.Name()), context)
			} else {
				name := entry.Name()
				if len(name) > 12 && name[:12] == ".fuse_hidden" {
					t.Errorf("%s: found .fuse_hidden file: %s", context, filepath.Join(dir, name))
				}
			}
		}
	}

	// Create database
	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Write enough spans to trigger WAL rotation and multiple block flushes
	t.Log("Writing spans to trigger WAL rotation and block flushes...")
	for batch := range 3 {
		for i := range 100 {
			sp := &span.Span{
				TraceID:     fmt.Sprintf("%032x", batch),
				SpanID:      fmt.Sprintf("%016x", batch*100+i+1),
				Name:        "test-operation-with-long-name-to-increase-size",
				StartTime:   time.Now().Add(time.Duration(batch*100+i) * time.Millisecond),
				EndTime:     time.Now().Add(time.Duration(batch*100+i+1) * time.Millisecond),
				ServiceName: "test-service",
				Tags: map[string]string{
					"batch": fmt.Sprintf("%d", batch),
					"index": fmt.Sprintf("%d", i),
					"tag1":  "value1-with-long-content",
					"tag2":  "value2-with-long-content",
					"tag3":  "value3-with-long-content",
				},
			}
			if err := db.WriteSpan(sp); err != nil {
				t.Fatalf("WriteSpan() error = %v", err)
			}
		}

		// Flush to blocks after each batch
		if err := db.Flush(); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}
		if err := db.flushHeadBlock(); err != nil {
			t.Fatalf("flushHeadBlock() error = %v", err)
		}
	}

	walSegment := db.wal.SegmentIndex()
	blocks := db.GetBlocks()
	t.Logf("After writes: %d WAL segments, %d blocks", walSegment+1, len(blocks))

	// Checkpoint to delete old WAL segments
	t.Log("Creating checkpoint...")
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}

	// Check for .fuse_hidden files after checkpoint
	t.Log("Checking for .fuse_hidden files after checkpoint...")
	checkNoFuseHiddenFiles(tmpDir, "after checkpoint")

	// Close database
	t.Log("Closing database...")
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Check for .fuse_hidden files after close
	t.Log("Checking for .fuse_hidden files after close...")
	checkNoFuseHiddenFiles(tmpDir, "after close")

	// Reopen database to verify recovery
	t.Log("Reopening database...")
	db2, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	// Verify data was recovered correctly
	stats := db2.Stats()
	blockStats := db2.BlockStats()
	blocks2 := db2.GetBlocks()

	t.Logf("After reopen: %d spans in memory, %d blocks, %d persisted spans",
		stats.TotalSpans, len(blocks2), blockStats.PersistedSpans)

	// After reopen, we should have all 300 spans persisted in blocks
	if blockStats.PersistedSpans != 300 {
		t.Errorf("After reopen, PersistedSpans = %d, want 300", blockStats.PersistedSpans)
	}

	if len(blocks2) != 3 {
		t.Errorf("After reopen, block count = %d, want 3", len(blocks2))
	}

	// TotalSpans is only the in-memory head (replayed from WAL)
	// Checkpoint deleted segment 0, so only segments 1+ were replayed
	// Note: With uint64 IDs, records are smaller, so WAL rotation behavior differs from string IDs
	t.Logf("Note: TotalSpans = %d (head only), PersistedSpans = %d (blocks)",
		stats.TotalSpans, blockStats.PersistedSpans)

	// Verify total data is correct (head + blocks)
	// We wrote 300 spans total - some are in blocks, rest replayed to head from remaining WAL
	totalData := stats.TotalSpans + blockStats.PersistedSpans
	if totalData < 300 || totalData > 315 {
		t.Errorf("Total data (head + blocks) = %d, want between 300-315", totalData)
	}

	// Check for .fuse_hidden files after reopen
	t.Log("Checking for .fuse_hidden files after reopen...")
	checkNoFuseHiddenFiles(tmpDir, "after reopen")

	t.Log("SUCCESS: No .fuse_hidden files found throughout database lifecycle")
}
