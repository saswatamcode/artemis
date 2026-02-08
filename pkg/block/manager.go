package block

import (
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/saswatamcode/artemis/pkg/storage"
)

// Manager manages the lifecycle of blocks (head and persisted)
type Manager struct {
	mu              sync.RWMutex
	dir             string
	head            *storage.ArrowStorage
	persistedBlocks []Block
	logger          *slog.Logger

	maxBlockDuration time.Duration // Flush head when it covers this much time
	maxBlockSpans    int64         // Flush head when it contains this many spans

	headMinTime int64
	headMaxTime int64
}

// Config holds block manager configuration
type Config struct {
	Dir              string        // Directory for persisted blocks
	MaxBlockDuration time.Duration // Maximum time range for a block (e.g., 2 hours)
	MaxBlockSpans    int64         // Maximum spans in head before flush (e.g., 1M)
	Logger           *slog.Logger  // Logger for block operations (defaults to slog.Default())
}

// DefaultConfig returns default block manager configuration
func DefaultConfig() *Config {
	return &Config{
		Dir:              "./data/blocks",
		MaxBlockDuration: 2 * time.Hour,
		MaxBlockSpans:    1000000, // 1M spans
	}
}

// NewManager creates a new block manager
func NewManager(cfg *Config, head *storage.ArrowStorage) (*Manager, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create blocks directory: %w", err)
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	bm := &Manager{
		dir:              cfg.Dir,
		head:             head,
		persistedBlocks:  make([]Block, 0),
		maxBlockDuration: cfg.MaxBlockDuration,
		maxBlockSpans:    cfg.MaxBlockSpans,
		headMinTime:      0,
		headMaxTime:      0,
		logger:           logger,
	}

	if err := bm.loadPersistedBlocks(); err != nil {
		return nil, fmt.Errorf("failed to load persisted blocks: %w", err)
	}

	return bm, nil
}

// loadPersistedBlocks loads all persisted blocks from disk
// Automatically detects and loads Arrow (L0) or Parquet (L1+) blocks
func (bm *Manager) loadPersistedBlocks() error {
	entries, err := os.ReadDir(bm.dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		blockDir := filepath.Join(bm.dir, entry.Name())
		block, err := LoadBlock(blockDir)
		if err != nil {
			bm.logger.Warn("failed to load block",
				slog.String("block_name", entry.Name()),
				slog.String("error", err.Error()))
			continue
		}

		bm.persistedBlocks = append(bm.persistedBlocks, block)
	}

	// Sort blocks by min time
	sort.Slice(bm.persistedBlocks, func(i, j int) bool {
		return bm.persistedBlocks[i].Meta().MinTime < bm.persistedBlocks[j].Meta().MinTime
	})

	bm.logger.Info("loaded persisted blocks",
		slog.Int("block_count", len(bm.persistedBlocks)))
	return nil
}

// UpdateHeadTimeRange updates the time range tracked for the head block
func (bm *Manager) UpdateHeadTimeRange(minTime, maxTime int64) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if bm.headMinTime == 0 || minTime < bm.headMinTime {
		bm.headMinTime = minTime
	}
	if maxTime > bm.headMaxTime {
		bm.headMaxTime = maxTime
	}
}

// ShouldFlush returns true if the head block should be flushed
func (bm *Manager) ShouldFlush() bool {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	// Check span count
	if bm.head.RowCount() >= bm.maxBlockSpans {
		return true
	}

	// Check time duration
	if bm.headMinTime > 0 && bm.headMaxTime > 0 {
		duration := time.Duration(bm.headMaxTime - bm.headMinTime)
		if duration >= bm.maxBlockDuration {
			return true
		}
	}

	return false
}

// FlushHead flushes the head block to disk
// minWALSegment and maxWALSegment are the WAL segment index range whose data is included in this block
// NOTE: Caller should reset the head storage after this returns successfully to avoid data loss
func (bm *Manager) FlushHead(minWALSegment, maxWALSegment int) (*BlockMeta, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	// Snapshot the current head to avoid race conditions
	// This prevents concurrent writes from modifying data while we're flushing
	oldHead := bm.head
	if oldHead == nil {
		return nil, fmt.Errorf("head is nil, cannot flush")
	}

	// CRITICAL FIX: Flush builder state before snapshot to materialize pending records
	// Without this, any spans in the builder (not yet in a record) would be lost
	if err := oldHead.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush builder: %w", err)
	}

	entropy := ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
	blockID := ulid.MustNew(ulid.Timestamp(time.Now()), entropy)

	meta := &BlockMeta{
		ULID:          blockID,
		MinTime:       bm.headMinTime,
		MaxTime:       bm.headMaxTime,
		SpanCount:     oldHead.RowCount(),
		Version:       1,
		CreatedAt:     time.Now(),
		MinWALSegment: minWALSegment,
		MaxWALSegment: maxWALSegment,
	}

	blockDir := filepath.Join(bm.dir, blockID.String())

	// Get data from the old head snapshot
	records := oldHead.GetRecords()
	schema := oldHead.Schema()
	idx := oldHead.GetIndex()

	// Flush to disk
	if err := FlushBlock(blockDir, meta, records, schema, idx); err != nil {
		return nil, fmt.Errorf("failed to flush block: %w", err)
	}

	// Load the newly created block
	block, err := LoadBlock(blockDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load flushed block: %w", err)
	}

	// Add to persisted blocks
	bm.persistedBlocks = append(bm.persistedBlocks, block)

	// Sort blocks by min time
	sort.Slice(bm.persistedBlocks, func(i, j int) bool {
		return bm.persistedBlocks[i].Meta().MinTime < bm.persistedBlocks[j].Meta().MinTime
	})

	bm.logger.Info("flushed head block",
		slog.String("block_id", meta.ULID.String()),
		slog.Int64("span_count", meta.SpanCount),
		slog.Int("level", meta.Level()))

	// Reset head time tracking
	bm.headMinTime = 0
	bm.headMaxTime = 0

	return meta, nil
}

// GetBlocks returns all persisted blocks
func (bm *Manager) GetBlocks() []Block {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	blocks := make([]Block, len(bm.persistedBlocks))
	copy(blocks, bm.persistedBlocks)
	return blocks
}

// GetHead returns the head block's underlying ArrowStorage
// Use this for write operations (AddSpan, etc.)
func (bm *Manager) GetHead() *storage.ArrowStorage {
	return bm.head
}

// GetHeadAsBlock returns the head block as a Block interface
// Use this for read/query operations to get uniform access across all block types
func (bm *Manager) GetHeadAsBlock() Block {
	return NewHeadBlock(bm.head)
}

// RemoveBlock removes a block from the manager's list
// This should be called after deleting a block from disk (e.g., after compaction)
func (bm *Manager) RemoveBlock(blockID string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for i, blk := range bm.persistedBlocks {
		if blk.Meta().ULID.String() == blockID {
			// Close the block before removing
			if err := blk.Close(); err != nil {
				return fmt.Errorf("failed to close block %s: %w", blockID, err)
			}

			bm.persistedBlocks = append(bm.persistedBlocks[:i], bm.persistedBlocks[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("block %s not found in manager", blockID)
}

// AddBlock adds a block to the manager
// This should be called after creating a new block (e.g., from compaction)
func (bm *Manager) AddBlock(blockDir string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	block, err := LoadBlock(blockDir)
	if err != nil {
		return fmt.Errorf("failed to load block: %w", err)
	}

	bm.persistedBlocks = append(bm.persistedBlocks, block)

	// Re-sort by min time
	sort.Slice(bm.persistedBlocks, func(i, j int) bool {
		return bm.persistedBlocks[i].Meta().MinTime < bm.persistedBlocks[j].Meta().MinTime
	})

	return nil
}

// Close closes all blocks
func (bm *Manager) Close() error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for _, block := range bm.persistedBlocks {
		if err := block.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Stats returns statistics about all blocks
func (bm *Manager) Stats() ManagerStats {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	stats := ManagerStats{
		HeadSpans:         bm.head.RowCount(),
		HeadRecordBatches: bm.head.RecordCount(),
		PersistedBlocks:   len(bm.persistedBlocks),
	}

	for _, block := range bm.persistedBlocks {
		stats.PersistedSpans += block.Meta().SpanCount
	}

	stats.TotalSpans = stats.HeadSpans + stats.PersistedSpans

	return stats
}

// ManagerStats holds statistics about the block manager
type ManagerStats struct {
	HeadSpans         int64
	HeadRecordBatches int
	PersistedBlocks   int
	PersistedSpans    int64
	TotalSpans        int64
}
