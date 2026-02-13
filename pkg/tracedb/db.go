package tracedb

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/compactor"
	"github.com/saswatamcode/artemis/pkg/query"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
	"github.com/saswatamcode/artemis/pkg/wal"
)

// DB is the main trace database that combines WAL and Arrow storage
type DB struct {
	wal          *wal.WAL
	storage      *storage.ArrowStorage
	eventStorage *storage.ArrowEventStorage // Event storage
	linkStorage  *storage.ArrowLinkStorage  // Link storage
	blockManager *block.Manager
	compactor    *compactor.Compactor
	walDir       string
	blocksDir    string // Base directory for blocks (cached for deleteBlock)
	logger       *slog.Logger

	// Background compaction and checkpointing
	compactInterval          time.Duration
	checkpointInterval       time.Duration
	blockCompactionInterval  time.Duration
	retentionPeriod          time.Duration
	lastCheckpointTime       time.Time
	lastBlockCompactionTime  time.Time
	lastRetentionCleanupTime time.Time
	checkpointThreshold      int // Number of segments before creating checkpoint
	stopCh                   chan struct{}
	wg                       sync.WaitGroup
	mu                       sync.Mutex
	closed                   bool

	// Query/flush synchronization
	// Queries take read lock, flushes take write lock
	// This prevents head block from being reset while queries are in progress
	queryMu sync.RWMutex
}

// Config holds database configuration
type Config struct {
	WALDir                  string
	WALSegmentSize          int64                          // Maximum WAL segment size before rotation (0 = 128MB default)
	CompactInterval         time.Duration                  // How often to compact WAL to Arrow storage
	CheckpointInterval      time.Duration                  // How often to create WAL checkpoints
	CheckpointThreshold     int                            // Create checkpoint after N segments
	BlockCompactionInterval time.Duration                  // How often to run block compaction (L0→L1, L1→L2)
	RetentionPeriod         time.Duration                  // Delete blocks older than this (0 = no retention)
	ReplayProgressCallback  wal.ReplayCallback             // Optional callback for WAL replay progress
	BlockConfig             *block.Config                  // Block management configuration (optional)
	CompactionLevels        map[int]*compactor.LevelConfig // Custom compaction level configs (optional)
	EnableCompaction        bool                           // Enable automatic block compaction
	EnableRetention         bool                           // Enable automatic retention cleanup
	Logger                  *slog.Logger                   // Logger for structured logging
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		WALDir:                  "./data/wal",
		CompactInterval:         10 * time.Second,
		CheckpointInterval:      60 * time.Second,
		CheckpointThreshold:     5,               // Checkpoint after 5 segments
		BlockCompactionInterval: 5 * time.Minute, // Run compaction every 5 minutes
		RetentionPeriod:         0,               // No retention by default
		EnableCompaction:        true,            // Enable compaction by default
		EnableRetention:         false,           // Disable retention by default
	}
}

// New creates a new trace database
func New(cfg *Config) (*DB, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	var walLog *wal.WAL
	var err error
	if cfg.WALSegmentSize > 0 {
		walLog, err = wal.NewWALWithSegmentSize(cfg.WALDir, cfg.WALSegmentSize, logger)
	} else {
		walLog, err = wal.NewWAL(cfg.WALDir, logger)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	arrowStorage := storage.NewArrowStorage()
	eventStorage := storage.NewArrowEventStorage()
	linkStorage := storage.NewArrowLinkStorage()

	var blockMgr *block.Manager
	var comp *compactor.Compactor
	var blocksDir string
	if cfg.BlockConfig != nil {
		blockMgr, err = block.NewManager(cfg.BlockConfig, arrowStorage)
		if err != nil {
			return nil, fmt.Errorf("failed to create block manager: %w", err)
		}
		// Set block manager on storage for time tracking
		arrowStorage.SetBlockManager(blockMgr)

		if cfg.CompactionLevels != nil {
			comp = compactor.NewCompactorWithConfigs(cfg.BlockConfig.Dir, cfg.CompactionLevels)
		} else {
			comp = compactor.NewCompactor(cfg.BlockConfig.Dir)
		}
		blocksDir = cfg.BlockConfig.Dir
	}

	db := &DB{
		wal:                      walLog,
		storage:                  arrowStorage,
		eventStorage:             eventStorage,
		linkStorage:              linkStorage,
		blockManager:             blockMgr,
		compactor:                comp,
		walDir:                   cfg.WALDir,
		blocksDir:                blocksDir,
		logger:                   logger,
		compactInterval:          cfg.CompactInterval,
		checkpointInterval:       cfg.CheckpointInterval,
		blockCompactionInterval:  cfg.BlockCompactionInterval,
		retentionPeriod:          cfg.RetentionPeriod,
		checkpointThreshold:      cfg.CheckpointThreshold,
		lastCheckpointTime:       time.Now(),
		lastBlockCompactionTime:  time.Now(),
		lastRetentionCleanupTime: time.Now(),
		stopCh:                   make(chan struct{}),
	}

	// Load existing WAL data into Arrow storage with progress tracking
	logger.Info("replaying wal", "dir", cfg.WALDir)
	if err := db.replayWAL(cfg.ReplayProgressCallback); err != nil {
		return nil, fmt.Errorf("failed to load from WAL: %w", err)
	}

	// Start background compaction
	if cfg.CompactInterval > 0 {
		db.wg.Add(1)
		go db.compactionLoop()
	}

	return db, nil
}

// WriteSpan writes a span to the database
// First writes to WAL for durability, then adds to in-memory storage
func (db *DB) WriteSpan(s *span.Span) error {
	// CRITICAL: Check if closed while holding mu lock, then acquire queryMu
	// This prevents TOCTOU race where Close() runs between check and queryMu acquisition
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return fmt.Errorf("database is closed")
	}

	// CRITICAL: Acquire read lock to prevent head block flush during write
	// This prevents data loss when flushHeadBlock() resets storage
	// We must acquire queryMu WHILE holding mu to prevent Close() from running
	db.queryMu.RLock()
	db.mu.Unlock() // Release mu now that we have queryMu
	defer db.queryMu.RUnlock()

	// Write to WAL first for durability and get the actual segment index
	// WAL now returns the segment index where the span was written
	// This eliminates the race condition where rotation could happen between
	// getting the segment index and writing the span
	actualSegment, err := db.wal.WriteSpan(s)
	if err != nil {
		return fmt.Errorf("failed to write span to WAL: %w", err)
	}

	// CRITICAL: Track the ACTUAL segment that contains this span
	// This ensures checkpoint only deletes WAL segments that have been flushed to blocks
	db.storage.UpdateWALSegment(actualSegment)

	if err := db.storage.AddSpan(s); err != nil {
		return fmt.Errorf("failed to add span to storage: %w", err)
	}

	return nil
}

// WriteSpans writes multiple spans to the database in bulk
// This is more efficient than calling WriteSpan repeatedly for batch ingestion
// First writes to WAL for durability, then adds to in-memory storage
func (db *DB) WriteSpans(spans []*span.Span) error {
	if len(spans) == 0 {
		return nil
	}

	// CRITICAL: Check if closed while holding mu lock, then acquire queryMu
	// This prevents TOCTOU race where Close() runs between check and queryMu acquisition
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return fmt.Errorf("database is closed")
	}

	// CRITICAL: Acquire read lock to prevent head block flush during write
	// This prevents data loss when flushHeadBlock() resets storage
	// We must acquire queryMu WHILE holding mu to prevent Close() from running
	db.queryMu.RLock()
	db.mu.Unlock() // Release mu now that we have queryMu
	defer db.queryMu.RUnlock()

	// Track which segments we actually write to
	segmentSet := make(map[int]bool)

	// Collect events from all spans
	var allEvents []*span.SpanEvent
	for _, s := range spans {
		if len(s.Events) > 0 {
			// Convert []SpanEvent to []*SpanEvent for WriteEvents
			for i := range s.Events {
				allEvents = append(allEvents, &s.Events[i])
			}
		}
	}

	// Collect links from all spans
	var allLinks []*span.SpanLink
	for _, s := range spans {
		if len(s.Links) > 0 {
			// Convert []SpanLink to []*SpanLink for WriteLinks
			for i := range s.Links {
				allLinks = append(allLinks, &s.Links[i])
			}
		}
	}

	// Write to WAL first for durability (one by one as WAL doesn't have bulk API)
	// FIX: Collect actual segment indices from each write
	for _, s := range spans {
		actualSegment, err := db.wal.WriteSpan(s)
		if err != nil {
			return fmt.Errorf("failed to write span to WAL: %w", err)
		}
		segmentSet[actualSegment] = true
	}

	// Write events to WAL if we have any
	if len(allEvents) > 0 {
		_, err := db.wal.WriteEvents(allEvents)
		if err != nil {
			return fmt.Errorf("failed to write events to WAL: %w", err)
		}
	}

	// Write links to WAL if we have any
	if len(allLinks) > 0 {
		_, err := db.wal.WriteLinks(allLinks)
		if err != nil {
			return fmt.Errorf("failed to write links to WAL: %w", err)
		}
	}

	// Update WAL segment tracking for all segments that were actually written to
	// This is precise - we only track segments that contain data
	for seg := range segmentSet {
		db.storage.UpdateWALSegment(seg)
	}

	// Add to in-memory Arrow storage in bulk for better performance
	if err := db.storage.AddSpans(spans); err != nil {
		return fmt.Errorf("failed to add spans to storage: %w", err)
	}

	// Add events to in-memory event storage
	if len(allEvents) > 0 {
		if err := db.eventStorage.AddEvents(allEvents); err != nil {
			return fmt.Errorf("failed to add events to storage: %w", err)
		}
	}

	// Add links to in-memory link storage
	if len(allLinks) > 0 {
		if err := db.linkStorage.AddLinks(allLinks); err != nil {
			return fmt.Errorf("failed to add links to storage: %w", err)
		}
	}

	return nil
}

// WriteEvent writes a span event to the database
// First writes to WAL for durability, then adds to in-memory event storage
func (db *DB) WriteEvent(e *span.SpanEvent) error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return fmt.Errorf("database is closed")
	}

	db.queryMu.RLock()
	db.mu.Unlock()
	defer db.queryMu.RUnlock()

	// Write to WAL first for durability
	_, err := db.wal.WriteEvent(e)
	if err != nil {
		return fmt.Errorf("failed to write event to WAL: %w", err)
	}

	if err := db.eventStorage.AddEvent(e); err != nil {
		return fmt.Errorf("failed to add event to storage: %w", err)
	}

	return nil
}

// WriteEvents writes multiple span events to the database in bulk
// More efficient than calling WriteEvent repeatedly for batch ingestion
func (db *DB) WriteEvents(events []*span.SpanEvent) error {
	if len(events) == 0 {
		return nil
	}

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return fmt.Errorf("database is closed")
	}

	db.queryMu.RLock()
	db.mu.Unlock()
	defer db.queryMu.RUnlock()

	// Write to WAL first for durability
	_, err := db.wal.WriteEvents(events)
	if err != nil {
		return fmt.Errorf("failed to write events to WAL: %w", err)
	}

	// Add to in-memory Arrow event storage in bulk
	if err := db.eventStorage.AddEvents(events); err != nil {
		return fmt.Errorf("failed to add events to storage: %w", err)
	}

	return nil
}

// replayWAL replays the WAL with optional progress callback
// Skips WAL segments that are already persisted in blocks
func (db *DB) replayWAL(progressCallback wal.ReplayCallback) error {
	reader := wal.NewReader(db.walDir, db.logger)

	// IMPORTANT: Check if blocks exist on disk and determine which WAL segments
	// are already persisted, even if no checkpoint exists (crash recovery scenario)
	minPersistedWALSegment := -1
	if db.blockManager != nil {
		blocks := db.blockManager.GetBlocks()
		if len(blocks) > 0 {
			// Find the minimum MinWALSegment across all blocks
			// All segments BEFORE this are fully persisted
			for _, blk := range blocks {
				meta := blk.Meta()
				if minPersistedWALSegment == -1 || meta.MinWALSegment < minPersistedWALSegment {
					minPersistedWALSegment = meta.MinWALSegment
				}
			}

			// If we have blocks with WAL segment tracking, create a synthetic checkpoint
			// so WAL replay knows to skip those segments
			if minPersistedWALSegment > 0 {
				db.logger.Info("found persisted blocks, adjusting replay start",
					"block_count", len(blocks),
					"min_wal_segment", minPersistedWALSegment)

				// Check if a real checkpoint exists
				existingCheckpoint, err := wal.ReadCheckpointMetadata(db.walDir)
				if err == nil && existingCheckpoint != nil {
					// Real checkpoint exists, use it
					db.logger.Info("using existing checkpoint",
						"max_deleted_segment", existingCheckpoint.MaxDeletedSegment)
				} else {
					// No checkpoint but we have blocks - create one to avoid duplicate replay
					// We can safely mark segments 0 through (minPersistedWALSegment - 1) as deletable
					if minPersistedWALSegment > 0 {
						maxSafeToDelete := minPersistedWALSegment - 1
						db.logger.Info("creating checkpoint marker to skip already persisted segments",
							"max_safe_to_delete", maxSafeToDelete)
						if err := wal.WriteCheckpointMetadata(db.walDir, maxSafeToDelete, maxSafeToDelete+1); err != nil {
							db.logger.Warn("failed to write checkpoint metadata", "error", err)
						}
					}
				}
			}
		}
	}

	// Replay spans, events, and links from WAL
	var err error
	err = reader.ReadAllWithEventsAndLinks(
		func(s *span.Span) error {
			return db.storage.AddSpan(s)
		},
		func(e *span.SpanEvent) error {
			return db.eventStorage.AddEvent(e)
		},
		func(l *span.SpanLink) error {
			return db.linkStorage.AddLink(l)
		},
	)

	if err != nil {
		return fmt.Errorf("WAL replay failed: %w", err)
	}

	db.logger.Info("wal replay complete",
		"span_count", db.storage.RowCount(),
		"event_count", db.eventStorage.RowCount(),
		"link_count", db.linkStorage.RowCount())

	return nil
}

// Flush flushes pending spans, events, and links to Arrow record batches
func (db *DB) Flush() error {
	if err := db.storage.Flush(); err != nil {
		return err
	}
	if err := db.eventStorage.Flush(); err != nil {
		return err
	}
	return db.linkStorage.Flush()
}

// Stats returns database statistics
func (db *DB) Stats() Stats {
	return Stats{
		TotalSpans:    db.storage.RowCount(),
		RecordBatches: db.storage.RecordCount(),
		StorageInfo:   db.storage.PrintStats(),
	}
}

// Stats holds database statistics
type Stats struct {
	TotalSpans    int64
	RecordBatches int
	StorageInfo   string
}

// ReplayStats returns the last WAL replay statistics
type ReplayStats = wal.ReplayStats

// compactionLoop runs periodic compaction and checkpointing
func (db *DB) compactionLoop() {
	defer db.wg.Done()

	ticker := time.NewTicker(db.compactInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Flush any pending spans to record batches
			if err := db.Flush(); err != nil {
				db.logger.Error("compaction flush error", "error", err)
			}

			// Check if head block should be flushed to disk
			if db.blockManager != nil && db.blockManager.ShouldFlush() {
				if err := db.flushHeadBlock(); err != nil {
					db.logger.Error("head block flush error", "error", err)
				}
			}

			// Check if we should create a checkpoint
			db.mu.Lock()
			shouldCheckpoint := time.Since(db.lastCheckpointTime) >= db.checkpointInterval
			db.mu.Unlock()

			if shouldCheckpoint {
				if err := db.createCheckpoint(); err != nil {
					db.logger.Error("checkpoint creation error", "error", err)
				} else {
					db.mu.Lock()
					db.lastCheckpointTime = time.Now()
					db.mu.Unlock()
				}
			}

			// Check if we should run block compaction
			db.mu.Lock()
			shouldCompactBlocks := time.Since(db.lastBlockCompactionTime) >= db.blockCompactionInterval
			db.mu.Unlock()

			if shouldCompactBlocks {
				if err := db.compactBlocks(); err != nil {
					db.logger.Error("block compaction error", "error", err)
				} else {
					db.mu.Lock()
					db.lastBlockCompactionTime = time.Now()
					db.mu.Unlock()
				}
			}

			// Check if we should run retention cleanup
			db.mu.Lock()
			shouldCleanup := db.retentionPeriod > 0 && time.Since(db.lastRetentionCleanupTime) >= 1*time.Hour
			db.mu.Unlock()

			if shouldCleanup {
				if err := db.cleanupOldBlocks(); err != nil {
					db.logger.Error("retention cleanup error", "error", err)
				} else {
					db.mu.Lock()
					db.lastRetentionCleanupTime = time.Now()
					db.mu.Unlock()
				}
			}
		case <-db.stopCh:
			db.logger.Info("compaction loop shutting down")
			return
		}
	}
}

// Close closes the database and releases resources
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.mu.Unlock()

	db.logger.Info("database shutting down")

	// Stop compaction loop if running
	if db.compactInterval > 0 {
		close(db.stopCh)
		db.wg.Wait()
	}

	// Flush final data
	db.logger.Info("flushing final data")
	if err := db.Flush(); err != nil {
		db.logger.Error("failed to flush final data", "error", err)
		return err
	}

	// Flush head block if block manager is enabled and has data
	if db.blockManager != nil {
		// Use queryMu to safely check row count (prevents race with concurrent writes)
		db.queryMu.RLock()
		rowCount := db.storage.RowCount()
		db.queryMu.RUnlock()

		if rowCount > 0 {
			db.logger.Info("flushing head block", "span_count", rowCount)
			if err := db.flushHeadBlock(); err != nil {
				db.logger.Error("failed to flush head block on close", "error", err)
			}
		}
	}

	// Close block manager
	if db.blockManager != nil {
		if err := db.blockManager.Close(); err != nil {
			return err
		}
	}

	// Close WAL
	if err := db.wal.Close(); err != nil {
		return err
	}

	// Release Arrow storage
	db.storage.Release()
	db.eventStorage.Release()
	db.linkStorage.Release()

	db.logger.Info("database shutdown complete")

	return nil
}

// GetStorage returns the underlying Arrow storage for querying
func (db *DB) GetStorage() *storage.ArrowStorage {
	return db.storage
}

// createCheckpoint deletes WAL segments that have been persisted to blocks
// and writes checkpoint metadata tracking what was deleted
func (db *DB) createCheckpoint() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// SAFETY: Only delete WAL segments that have been persisted to disk blocks
	// If we don't have block persistence, we can't safely delete WAL segments
	if db.blockManager == nil {
		return nil
	}

	// Find the highest WAL segment that's been persisted to disk
	maxPersistedSegment := db.findMaxPersistedWALSegment()
	if maxPersistedSegment < 0 {
		// No blocks have been persisted yet, can't delete any WAL segments
		return nil
	}

	// Read existing checkpoint to see what's already been deleted
	existingCheckpoint, err := wal.ReadCheckpointMetadata(db.walDir)
	if err != nil {
		return fmt.Errorf("failed to read checkpoint metadata: %w", err)
	}

	// Determine which segments to delete
	deleteFrom := 0
	if existingCheckpoint != nil {
		// Start deleting from the segment after the last deleted one
		deleteFrom = existingCheckpoint.MaxDeletedSegment + 1
	}

	deleteTo := maxPersistedSegment

	// Don't do anything if there are no new segments to delete
	if deleteTo < deleteFrom {
		return nil
	}

	// Number of segments being deleted in this checkpoint operation
	deleteCountIncremental := deleteTo - deleteFrom + 1

	// Don't checkpoint if we don't have enough segments to delete
	if deleteCountIncremental < db.checkpointThreshold {
		return nil
	}

	// CRITICAL: Re-check current WAL segment right before deletion
	// This prevents deleting a segment that became active between
	// findMaxPersistedWALSegment() and now (WAL rotation race)
	currentWALSegment := db.wal.SegmentIndex()
	if deleteTo >= currentWALSegment {
		// The current segment is in the deletion range
		// Only delete up to the segment before the current one
		if currentWALSegment > 0 {
			deleteTo = currentWALSegment - 1
			deleteCountIncremental = deleteTo - deleteFrom + 1
		} else {
			// Current segment is 0, can't delete anything
			return nil
		}
	}

	// Recheck threshold after adjustment
	if deleteCountIncremental < db.checkpointThreshold {
		return nil
	}

	// Total number of segments deleted (0 through deleteTo, inclusive)
	// This is what the checkpoint metadata validation expects
	deleteCountTotal := deleteTo + 1

	// CRITICAL: Flush current WAL page to ensure all data is on disk
	// This must happen BEFORE writing checkpoint metadata to prevent data loss
	// If we checkpoint without flushing, a crash could lose unflushed data
	if err := db.wal.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL before checkpoint: %w", err)
	}

	// CRITICAL: Write checkpoint metadata FIRST, THEN delete segments
	// This ensures if system crashes during deletion, checkpoint metadata
	// correctly reflects what should be deleted (next checkpoint can retry)
	if err := wal.WriteCheckpointMetadata(db.walDir, deleteTo, deleteCountTotal); err != nil {
		return fmt.Errorf("failed to write checkpoint metadata: %w", err)
	}

	// Delete WAL segments (data is safely in blocks now)
	// If this fails, next checkpoint will retry (metadata already written)
	if err := wal.DeleteWALSegments(db.walDir, deleteTo); err != nil {
		return fmt.Errorf("failed to delete old WAL segments: %w", err)
	}

	db.logger.Info("checkpoint created",
		"deleted_from", deleteFrom,
		"deleted_to", deleteTo,
		"segment_count_incremental", deleteCountIncremental,
		"segment_count_total", deleteCountTotal)

	return nil
}

// Checkpoint manually triggers a checkpoint (useful for testing or manual maintenance)
func (db *DB) Checkpoint() error {
	return db.createCheckpoint()
}

// findMaxPersistedWALSegment returns the highest WAL segment index that can be safely deleted
// This is determined by finding the maximum MaxWALSegment across all persisted blocks
// For example, if blocks have MaxWALSegment values [2, 5, 8], we can delete segments 0-8
// Returns -1 if no blocks have been persisted or segments cannot be safely deleted
func (db *DB) findMaxPersistedWALSegment() int {
	blocks := db.blockManager.GetBlocks()
	if len(blocks) == 0 {
		return -1
	}

	// Find the maximum MaxWALSegment across all persisted blocks
	// This represents the highest segment that's been fully persisted to disk
	maxMaxWALSegment := -1
	for _, block := range blocks {
		meta := block.Meta()
		if meta.MaxWALSegment > maxMaxWALSegment {
			maxMaxWALSegment = meta.MaxWALSegment
		}
	}

	// CRITICAL: Get current WAL segment to ensure we NEVER delete the actively written file
	// WAL segments only rotate when they hit 128MB. With low traffic, the same segment
	// file stays open for writes even after head block flushes.
	// Example: Head flushes at 50 spans (< 128MB), WAL is still writing to segment 0
	currentWALSegment := db.wal.SegmentIndex()

	// CRITICAL: Acquire read lock to prevent concurrent writes from modifying head state
	// This prevents reading stale WAL segment range while writes are in progress
	db.queryMu.RLock()
	headMinWAL, _ := db.storage.GetWALSegmentRange()
	db.queryMu.RUnlock()

	if headMinWAL != -1 {
		// Head has unpersisted data. We can only delete segments that are:
		// 1. BEFORE the head's range (so head doesn't reference them)
		// 2. AND are covered by persisted blocks (no gaps!)
		if headMinWAL > 0 {
			// Head starts at segment N (N > 0), so we can delete segments 0 through N-1
			// But ONLY if ALL those segments are covered by persisted blocks
			maxSafeToDelete := headMinWAL - 1

			// CRITICAL: Check that blocks cover ALL segments from 0 to maxSafeToDelete
			// If there's a gap, we can't safely delete anything beyond the gap
			if maxMaxWALSegment >= maxSafeToDelete {
				// All segments up to head are covered by blocks
				return maxSafeToDelete
			}
			// SAFETY: Blocks don't cover all segments up to head's start
			// Example: head starts at 5, blocks only cover 0-3, segments 4 would be lost
			// We can only delete up to maxMaxWALSegment, but there's a gap
			// Return -1 to avoid deleting anything (safest option)
			return -1
		}
		// Head starts at segment 0, can't delete anything (head still needs segment 0)
		return -1
	}

	// Head is empty (just flushed), but we CANNOT delete the current WAL segment
	// because it's still open for writes.
	//
	// Example scenario (demo mode: head flush every 5s, checkpoint every 10s):
	//   T=0s:  WAL writing to segment 0
	//   T=5s:  Head flushes → block with maxWALSegment=0, head becomes empty
	//   T=10s: Checkpoint runs, head is empty, but WAL STILL writing to segment 0
	//          Without this check, we'd try to delete the open file → .fuse_hidden
	//
	// SAFETY: Only delete segments strictly less than currentWALSegment
	if maxMaxWALSegment >= currentWALSegment {
		// Blocks cover up to or past the current WAL segment
		// Can only delete segments before the current one
		if currentWALSegment > 0 {
			return currentWALSegment - 1
		}
		// Current segment is 0, can't delete anything
		return -1
	}

	// Blocks only cover segments before current WAL segment, safe to delete all
	return maxMaxWALSegment
}

// flushHeadBlock flushes the head block to disk and resets the head
func (db *DB) flushHeadBlock() error {
	if db.blockManager == nil {
		return fmt.Errorf("block manager not configured")
	}

	// CRITICAL: Acquire write lock to prevent queries from accessing head while it's being reset
	// Queries hold read locks, so this will wait for all ongoing queries to complete
	db.queryMu.Lock()
	defer db.queryMu.Unlock()

	// Flush pending record batches first
	if err := db.Flush(); err != nil {
		return fmt.Errorf("failed to flush pending batches: %w", err)
	}

	// Get WAL segment range from head block
	// This tells us which WAL segments contributed data to this block
	minWALSegment, maxWALSegment := db.storage.GetWALSegmentRange()

	// SAFETY: If no WAL segments tracked (empty head), skip flush entirely
	// Creating a block with no data would pollute the block metadata
	if minWALSegment == -1 {
		db.logger.Debug("skipping empty head block flush")
		return nil
	}

	// Flush head block to disk with WAL segment range tracking
	meta, err := db.blockManager.FlushHead(minWALSegment, maxWALSegment)
	if err != nil {
		return fmt.Errorf("failed to flush head block: %w", err)
	}

	if meta != nil {
		blockDir := filepath.Join(db.blocksDir, meta.ULID.String())

		// Flush event records to disk if we have any
		eventRecords := db.eventStorage.GetRecords()
		if len(eventRecords) > 0 {
			if err := block.FlushEventsBlock(blockDir, eventRecords, db.eventStorage.Schema()); err != nil {
				return fmt.Errorf("failed to flush events block: %w", err)
			}
		}

		// Flush link records to disk if we have any
		linkRecords := db.linkStorage.GetRecords()
		if len(linkRecords) > 0 {
			if err := block.FlushLinksBlock(blockDir, linkRecords, db.linkStorage.Schema()); err != nil {
				return fmt.Errorf("failed to flush links block: %w", err)
			}
		}

		// Now add the block to the manager (this loads it with events and links)
		if err := db.blockManager.AddFlushedBlock(blockDir); err != nil {
			return fmt.Errorf("failed to add flushed block: %w", err)
		}

		db.logger.Info("head block flushed",
			"block_id", meta.ULID,
			"min_wal_segment", meta.MinWALSegment,
			"max_wal_segment", meta.MaxWALSegment,
			"span_count", meta.SpanCount,
			"event_count", db.eventStorage.RowCount(),
			"link_count", db.linkStorage.RowCount(),
			"level", meta.Level())

		// Reset the head block, event storage, and link storage
		db.storage.Reset()
		db.eventStorage.Reset()
		db.linkStorage.Reset()
	}

	return nil
}

// GetBlocks returns all persisted blocks
func (db *DB) GetBlocks() []block.Block {
	if db.blockManager == nil {
		return nil
	}
	return db.blockManager.GetBlocks()
}

// GetQuerier creates a new querier for the current database state
// This provides a clean interface for querying spans across head and persisted blocks
// A new querier is created each time to ensure it has the latest block list
//
// IMPORTANT: This method is DEPRECATED in favor of QueryWithLock().
// The returned querier may reference blocks that could be deleted by compaction.
// For safe querying, use QueryWithLock() which holds queryMu during the entire query.
//
// If you must use this method, ensure the query completes quickly before compaction runs.
func (db *DB) GetQuerier() query.Querier {
	// Flush any pending data to ensure queries see all data
	db.storage.Flush()

	var blocks []block.Block
	if db.blockManager != nil {
		blocks = db.blockManager.GetBlocks()
	}

	headBlock := block.NewHeadBlock(db.storage)
	return query.NewBlockQuerier(headBlock, blocks)
}

// GetBlockManager returns the block manager (nil if not configured)
func (db *DB) GetBlockManager() *block.Manager {
	return db.blockManager
}

// BlockStats returns block manager statistics
func (db *DB) BlockStats() *block.ManagerStats {
	if db.blockManager == nil {
		return nil
	}
	stats := db.blockManager.Stats()
	return &stats
}

// compactBlocks runs Prometheus-style multi-level block compaction
// Tries to compact blocks at each level: L0→L1 (2h), L1→L2 (4h), L2→L3 (8h), etc.
func (db *DB) compactBlocks() error {
	if db.compactor == nil || db.blockManager == nil {
		return nil
	}

	// Get current block list without holding mutex during I/O
	blocks := db.blockManager.GetBlocks()
	if len(blocks) == 0 {
		return nil
	}

	// Try compaction at each level (0→1, 1→2, 2→3, 3→4, 4→5)
	// Level 0 = Arrow IPC from head
	// Level 1 = 2h Parquet
	// Level 2 = 4h Parquet
	// Level 3 = 8h Parquet
	// Level 4 = 2d Parquet
	// Level 5 = 14d Parquet
	for level := range 5 {
		plan := db.compactor.Plan(blocks, level)
		if plan != nil {
			db.logger.Info("compacting blocks",
				"block_count", len(plan.Blocks),
				"from_level", level,
				"to_level", level+1)

			// Perform compaction WITHOUT holding db.mu (expensive I/O)
			newMeta, err := db.compactor.Compact(plan)
			if err != nil {
				return fmt.Errorf("L%d→L%d compaction failed: %w", level, level+1, err)
			}

			db.logger.Info("created compacted block",
				"level", newMeta.Level(),
				"block_id", newMeta.ULID,
				"span_count", newMeta.SpanCount,
				"duration", formatDuration(newMeta.Duration()))

			// Add new compacted block to manager (no lock needed - manager has internal locking)
			newBlockDir := filepath.Join(db.compactor.GetBaseDir(), newMeta.ULID.String())
			if err := db.blockManager.AddBlock(newBlockDir); err != nil {
				return fmt.Errorf("failed to add compacted block to manager: %w", err)
			}

			// CRITICAL: Acquire write lock to ensure no queries are accessing these blocks
			// Queries hold read locks, so this will wait for ongoing queries to complete
			db.queryMu.Lock()

			// CRITICAL: Close file handles before deletion to prevent .fuse_hidden files
			// Order is important:
			// 1. Remove blocks from manager (stops new queries from accessing them)
			// 2. Explicitly close the blocks (releases file handles)
			// 3. Delete the files from disk
			sourceBlockIDs := make([]string, len(plan.Blocks))
			for i, blk := range plan.Blocks {
				sourceBlockIDs[i] = blk.Meta().ULID.String()
				if err := db.blockManager.RemoveBlock(sourceBlockIDs[i]); err != nil {
					db.logger.Warn("failed to remove source block from manager",
						"block_id", sourceBlockIDs[i],
						"error", err)
				}
			}

			// Explicitly close the blocks to release file handles
			// plan.Blocks may still hold references even after RemoveBlock()
			for _, blk := range plan.Blocks {
				if err := blk.Close(); err != nil {
					db.logger.Warn("failed to close source block",
						"block_id", blk.Meta().ULID.String(),
						"error", err)
				}
			}

			// Delete source blocks after closing file handles
			for _, blockID := range sourceBlockIDs {
				if err := db.deleteBlockFiles(blockID); err != nil {
					db.logger.Warn("failed to delete source block",
						"block_id", blockID,
						"error", err)
				}
			}

			// Release write lock
			db.queryMu.Unlock()

			// Get updated block list
			blocks = db.blockManager.GetBlocks()
		}
	}

	return nil
}

// formatDuration formats a duration in a human-readable way
func formatDuration(d time.Duration) string {
	hours := d.Hours()
	if hours < 1 {
		return fmt.Sprintf("%.0fm", d.Minutes())
	} else if hours < 24 {
		return fmt.Sprintf("%.0fh", hours)
	} else {
		days := hours / 24
		return fmt.Sprintf("%.0fd", days)
	}
}

// deleteBlock deletes a block directory and removes it from the manager
// Acquires queryMu write lock to ensure no queries are accessing the block
func (db *DB) deleteBlock(blockID string) error {
	if db.blockManager == nil {
		return nil
	}

	// CRITICAL: Acquire write lock to prevent queries from accessing block during deletion
	// This ensures all ongoing queries complete before we delete the block
	db.queryMu.Lock()
	defer db.queryMu.Unlock()

	// Remove from manager (closes the block)
	if err := db.blockManager.RemoveBlock(blockID); err != nil {
		// Block might already be removed, log but continue with deletion
		db.logger.Warn("block not in manager",
			"block_id", blockID,
			"error", err)
	}

	// Delete from disk
	return db.deleteBlockFiles(blockID)
}

// deleteBlockFiles deletes block files from disk
// Assumes block has already been removed from manager and file handles closed
func (db *DB) deleteBlockFiles(blockID string) error {
	// Construct full path using cached base directory
	fullPath := filepath.Join(db.blocksDir, blockID)

	// Delete from disk
	return block.DeleteBlock(fullPath)
}

// cleanupOldBlocks deletes blocks older than the retention period
func (db *DB) cleanupOldBlocks() error {
	if db.blockManager == nil || db.retentionPeriod == 0 {
		return nil
	}

	// Get blocks without holding mutex during I/O
	blocks := db.blockManager.GetBlocks()
	cutoffTime := time.Now().Add(-db.retentionPeriod)

	deletedCount := 0
	for _, blk := range blocks {
		meta := blk.Meta()
		blockEndTime := time.Unix(0, meta.MaxTime)

		if blockEndTime.Before(cutoffTime) {
			db.logger.Info("deleting old block",
				"block_id", meta.ULID,
				"ended_at", blockEndTime.Format(time.RFC3339))
			// deleteBlock acquires queryMu internally for safety
			if err := db.deleteBlock(meta.ULID.String()); err != nil {
				db.logger.Warn("failed to delete old block",
					"block_id", meta.ULID,
					"error", err)
			} else {
				deletedCount++
			}
		}
	}

	if deletedCount > 0 {
		db.logger.Info("retention cleanup complete",
			"deleted_count", deletedCount,
			"retention_period", db.retentionPeriod)
	}

	return nil
}

// GetStorageForQuery returns the head storage with query synchronization
// This should be used by query operations to ensure the head isn't flushed during queries
//
// DEPRECATED: This API requires manual lock management which is error-prone.
// Use QueryWithLock() instead, which automatically handles locking.
//
// WARNING: You MUST call ReleaseQueryLock() when done, or deadlock will occur.
// Prefer using QueryWithLock() which handles this automatically via defer.
func (db *DB) GetStorageForQuery() *storage.ArrowStorage {
	db.queryMu.RLock()
	return db.storage
}

// ReleaseQueryLock releases the query read lock
// Should be called after GetStorageForQuery() and querying is complete
//
// DEPRECATED: This API requires manual lock management which is error-prone.
// Use QueryWithLock() instead, which automatically handles locking.
//
// WARNING: Calling this without a prior GetStorageForQuery() will panic.
// Always use defer immediately after GetStorageForQuery(), or use QueryWithLock().
func (db *DB) ReleaseQueryLock() {
	db.queryMu.RUnlock()
}

// GetBlocksForQuery returns persisted blocks with query synchronization
// This should be used together with GetStorageForQuery()
//
// DEPRECATED: This API assumes queryMu is already held, but doesn't enforce it.
// Use QueryWithLock() instead, which provides both storage and blocks safely.
//
// WARNING: This method assumes the caller holds queryMu.RLock().
// Calling without the lock causes race conditions. Use QueryWithLock() instead.
func (db *DB) GetBlocksForQuery() []block.Block {
	if db.blockManager == nil {
		return nil
	}
	return db.blockManager.GetBlocks()
}

// QueryWithLock performs a query operation with proper synchronization
// The callback receives head storage and blocks, and should perform the query
// This ensures head block is not flushed during the query
func (db *DB) QueryWithLock(fn func(head *storage.ArrowStorage, blocks []block.Block) error) error {
	// Take read lock to prevent head flush during query
	db.queryMu.RLock()
	defer db.queryMu.RUnlock()

	// Flush any pending spans to record batches before querying
	if err := db.storage.Flush(); err != nil {
		return err
	}

	// Get blocks while holding the lock
	var blocks []block.Block
	if db.blockManager != nil {
		blocks = db.blockManager.GetBlocks()
	}

	// Execute the query callback
	return fn(db.storage, blocks)
}

// QuerySpansWithEvents queries spans and optionally loads their events
// This is a convenience method that wraps GetQuerier() and handles event loading
// If includeEvents is true, the Events field on each span will be populated
func (db *DB) QuerySpansWithEvents(includeEvents bool, matchers ...*query.Matcher) ([]*span.Span, error) {
	// Get querier and query spans
	querier := db.GetQuerier()
	result, err := querier.Select(matchers...)
	if err != nil {
		return nil, err
	}

	// If events are not requested, return spans as-is
	if !includeEvents {
		return result.Spans, nil
	}

	// Load events for each span
	for _, s := range result.Spans {
		if s != nil {
			events, err := db.GetEventsForSpan(s.SpanID)
			if err != nil {
				return nil, fmt.Errorf("failed to load events for span %s: %w", s.SpanID, err)
			}
			s.Events = events
		}
	}

	return result.Spans, nil
}

// GetEventsForSpan retrieves all events for a specific span
// Queries both in-memory event storage and persisted blocks
func (db *DB) GetEventsForSpan(spanID string) ([]span.SpanEvent, error) {
	db.queryMu.RLock()
	defer db.queryMu.RUnlock()

	// Flush any pending events to record batches before querying
	if err := db.eventStorage.Flush(); err != nil {
		return nil, err
	}

	// Query in-memory event storage (head)
	headEvents, err := db.eventStorage.GetEventsBySpanID(spanID)
	if err != nil {
		return nil, fmt.Errorf("failed to query head events: %w", err)
	}

	// Convert pointers to values
	result := make([]span.SpanEvent, 0, len(headEvents))
	for _, e := range headEvents {
		if e != nil {
			result = append(result, *e)
		}
	}

	// Query persisted blocks if block manager is enabled
	if db.blockManager != nil {
		blocks := db.blockManager.GetBlocks()
		for _, blk := range blocks {
			// Try to get events from this block
			// ArrowBlock and ParquetBlock both implement GetEventsBySpanID if they have events
			var blockEvents []*span.SpanEvent
			var err error

			// Check block type and call appropriate method
			switch b := blk.(type) {
			case *block.ArrowBlock:
				blockEvents, err = b.GetEventsBySpanID(spanID)
			case *block.ParquetBlock:
				blockEvents, err = b.GetEventsBySpanID(spanID)
			default:
				// Block type doesn't support events, skip
				continue
			}

			if err != nil {
				return nil, fmt.Errorf("failed to query events from block %s: %w", blk.Meta().ULID, err)
			}

			// Add block events to result
			for _, e := range blockEvents {
				if e != nil {
					result = append(result, *e)
				}
			}
		}
	}

	return result, nil
}

// GetEventsForTrace retrieves all events for all spans in a trace
// Returns a map of spanID -> events
// This method first queries all spans for the trace, then fetches events for each span
func (db *DB) GetEventsForTrace(traceID string) (map[string][]span.SpanEvent, error) {
	// First, find all spans in this trace using the querier
	matcher, err := query.NewMatcher(query.MatchEqual, "trace_id", traceID)
	if err != nil {
		return nil, fmt.Errorf("failed to create matcher: %w", err)
	}

	querier := db.GetQuerier()
	result, err := querier.Select(matcher)
	if err != nil {
		return nil, fmt.Errorf("failed to query spans for trace: %w", err)
	}

	// Now fetch events for each span
	eventsMap := make(map[string][]span.SpanEvent)
	for _, s := range result.Spans {
		if s != nil {
			events, err := db.GetEventsForSpan(s.SpanID)
			if err != nil {
				return nil, fmt.Errorf("failed to get events for span %s: %w", s.SpanID, err)
			}
			if len(events) > 0 {
				eventsMap[s.SpanID] = events
			}
		}
	}

	return eventsMap, nil
}

// GetLinksForSpan retrieves all links for a specific span
// Queries both in-memory link storage and persisted blocks
func (db *DB) GetLinksForSpan(spanID string) ([]span.SpanLink, error) {
	db.queryMu.RLock()
	defer db.queryMu.RUnlock()

	// Flush any pending links to record batches before querying
	if err := db.linkStorage.Flush(); err != nil {
		return nil, err
	}

	// Query in-memory link storage (head)
	headLinks, err := db.linkStorage.GetLinksBySpanID(spanID)
	if err != nil {
		return nil, fmt.Errorf("failed to query head links: %w", err)
	}

	// Convert pointers to values
	result := make([]span.SpanLink, 0, len(headLinks))
	for _, l := range headLinks {
		if l != nil {
			result = append(result, *l)
		}
	}

	// Query persisted blocks if block manager is enabled
	if db.blockManager != nil {
		blocks := db.blockManager.GetBlocks()
		for _, blk := range blocks {
			// Try to get links from this block
			// ArrowBlock and ParquetBlock both implement GetLinksBySpanID if they have links
			var blockLinks []*span.SpanLink
			var err error

			// Check block type and call appropriate method
			switch b := blk.(type) {
			case *block.ArrowBlock:
				blockLinks, err = b.GetLinksBySpanID(spanID)
			case *block.ParquetBlock:
				blockLinks, err = b.GetLinksBySpanID(spanID)
			default:
				// Block type doesn't support links, skip
				continue
			}

			if err != nil {
				return nil, fmt.Errorf("failed to query links from block %s: %w", blk.Meta().ULID, err)
			}

			// Add block links to result
			for _, l := range blockLinks {
				if l != nil {
					result = append(result, *l)
				}
			}
		}
	}

	return result, nil
}

// GetLinksForTrace retrieves all links for all spans in a trace
// Returns a map of spanID -> links
// This method first queries all spans for the trace, then fetches links for each span
func (db *DB) GetLinksForTrace(traceID string) (map[string][]span.SpanLink, error) {
	// First, find all spans in this trace using the querier
	matcher, err := query.NewMatcher(query.MatchEqual, "trace_id", traceID)
	if err != nil {
		return nil, fmt.Errorf("failed to create matcher: %w", err)
	}

	querier := db.GetQuerier()
	result, err := querier.Select(matcher)
	if err != nil {
		return nil, fmt.Errorf("failed to query spans for trace: %w", err)
	}

	// Now fetch links for each span
	linksMap := make(map[string][]span.SpanLink)
	for _, s := range result.Spans {
		if s != nil {
			links, err := db.GetLinksForSpan(s.SpanID)
			if err != nil {
				return nil, fmt.Errorf("failed to get links for span %s: %w", s.SpanID, err)
			}
			if len(links) > 0 {
				linksMap[s.SpanID] = links
			}
		}
	}

	return linksMap, nil
}
